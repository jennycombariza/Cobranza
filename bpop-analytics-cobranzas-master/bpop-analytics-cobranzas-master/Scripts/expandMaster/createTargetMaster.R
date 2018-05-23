#' createTargetMaster
#' 
#' @description Funcion que sirve para identificar si un cliente/obligacion 
#' contenido en la caseBase pasará al siguiente tramo de mora (>30 días en mora) 
#' 
#' @param period dia en formato "YYYYMMdd"
#' @return identificador unico de entrada en mora junto a las variables target 
#' pertinentes y el maximo de dias en mora al cabo de 40 dias
#' @examples
#' createTargetMaster("201700801")

createTargetMaster <- function(period, saveFile = TRUE){
  cat("Processing Target for", period, fill = TRUE)
  
  # Seleccion de archivos de caseBase de mora guardados en 1.Data/PreparedData
  # con la etiqueta "caseBase" para el periodo seleccionado
  
  customerBaseDt <- getProcessedTableByPeriod(dataSource = "caseBase",
                                              periods = period,
                                              path = customerBaseExpandedPath,
                                              colClasses = "character",
                                              na.strings = c("", NA))[, .(ID_CONTRATO)]
  # Además del periodo que pasamos en la ejecucion cargaremos las 40 
  # fechas posteriores para calcular la target
  
  periodsToSelect <- c(0:40)
  periodsToSelect <- dayAddInteger(period, periodsToSelect)
  
  # Seleccion de archivos procesados de mora guardados en 1.Data/PreparedData
  # con la etiqueta "processedMora" para las fechas contenidas en el vector
  # periodsToSelect
  
  targetDt <- getProcessedTableByPeriod(dataSource = "processedMora",
                                        periods = periodsToSelect,
                                        path = moraProcessedPath,
                                        colClasses = "character",
                                        na.strings = c("", NA))
  
  # Hacemos un left join con la tabla de customerBase para quedarnos solo con 
  # el historico de mora de los clientes/obligaciones que forman parte de ella
  
  targetDt <- merge(customerBaseDt,
                    targetDt[, .(ID_CONTRATO, YEAR_MONTH_DAY, DIAS)],
                    by = c("ID_CONTRATO"),
                    all.x = TRUE,
                    all.y = FALSE)
  
  # Nos aseguramos que la variable DIAS sea numerica
  targetDt[, DIAS := as.numeric(DIAS)]
  
  # Calculamos el maximo de dias que ha estado cada una de las obligaciones en
  # mora para saber si superan o no los 30 dias
  
  targetDt[, MAX_DIAS_MORA := max(DIAS, na.rm = TRUE),
           by = c("ID_CONTRATO")]
  
  # Creamos la variable target binaria marcando a "1" aquellas obligaciones que 
  # superan los 30 dias y a "0" el resto
  targetDt[, TARGET_BINARY := 0]
  targetDt[MAX_DIAS_MORA > 30, TARGET_BINARY := 1]
  
  # Nos quedamos solo con las columnas imprescendibles
  
  targetDt <- targetDt[YEAR_MONTH_DAY == period, 
                       .(ID_CONTRATO,
                         YEAR_MONTH_DAY,
                         TARGET_BINARY,
                         MAX_DIAS_MORA)]
  
  # Guardar el archivo como caseBase para el periodo seleccionado
  saveProcessedTableByPeriod(targetDt,
                             path= targetExpandedPath,
                             source = "target",
                             periodIDvarname = "YEAR_MONTH_DAY")
}

