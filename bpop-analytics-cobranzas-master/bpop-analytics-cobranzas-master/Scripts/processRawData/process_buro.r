#' process_buro
#' 
#' @description Funcion de limpieza de datos de la tabla de Buró. Esta funcion
#' recibe los datos del S3 segun requerimientos de extraccion y los adapta para
#' ser usado por el resto de funciones
#' 
#' @param period Dia para el cual se va a ejecutar la funcion. Formato YYYYMM
#' @return Datos de clientes a nivel mensual preparados para ser utilizados por el resto de funciones
#' @examples
#' process_buro("201702")


processBuro <- function(period, saveSummary = T){
  cat("Processing Buro for " %+% paste(period, collapse = ", ") %+% "...\n")
  buro <- getProcessedTableByPeriod(dataSource = "buro",
                                    periods = period,
                                    path = buroRawPath,
                                    colClasses = "character",
                                    na.strings = c("", NA))
  buro[, YEAR_MONTH := period]
  # Estandarizamos los nombres de las columnas
  setnames(buro, names(buro), namesBuro)
  
  # Coercionamos las fechas a un formato concreto
  buro[, FECHA_MAS_ANTIGUA_TDC := ymd(paste0(FECHA_MAS_ANTIGUA_TDC, "-01"))]

  # Coercinamos las variables numericas y quitamos los separadores de miles
  buro[, (numericColumnsBuro) := lapply(.SD, function(x){as.numeric(str_replace_all(x, ",", ""))}), .SDcols=numericColumnsBuro]
  # Si saveSummary es igual a TRUE se guarda un archivo resumen de data quality
  
  if (saveSummary){
    saveTableSummariesByPeriod(buro, path = dataQualityAnalysisPath,
                               source = "buro", periodIDvarname = "YEAR_MONTH")  
  }
  
  # Asumimos que las variables que vienen a missing toman valor 0
  # Reemplazamos los "NA" por 0 
  for(var in numericColumnsBuro){
    cat("Limpiando na's de la variable", var, fill = TRUE)
    set(x = buro,
        j = var,
        i = buro[, which(is.nan(get(var)) | is.na(get(var)))],
        value = 0)
    
  }
  
  # identificamos las variables caracter
  characterColumns <- setdiff(names(buro),
                              c("FECHA_MAS_ANTIGUA_TDC", numericColumnsBuro))
  
  # Aplicamos la funcion trim para eliminar los espacios en blanco que pueden existir
  buro[, (characterColumns) := lapply(.SD, FUN = str_trim, side = 'both'), .SDcols = characterColumns]
  
  saveProcessedTableByPeriod(buro,
                             path = buroProcessedPath,
                             source = "processedBuro",
                             periodIDvarname = "YEAR_MONTH")
  
}



