#' process_mora
#' 
#' @description Funcion de limpieza de datos de la tabla de Mora. Esta funcion
#' recibe los datos del S3 segun requerimientos de extraccion y los adapta para
#' ser usado por el resto de funciones
#' 
#' @param periodo Dia para el cual se va a ejecutar la funcion. Formato YYYYMMDD
#' @return Datos de mora a nivel diario preparados para ser utilizados por el resto de funciones
#' @examples
#' process_mora("20170201")


processMora <- function(period, saveSummary = T){
  # Cargamos el archivo diario de mora agregado a nivel mensual (Cambia en la implementacion)
  cat("Processing Mora for " %+% paste(period, collapse = ", ") %+% "...\n")
  mora <- getProcessedTableByPeriod(dataSource = "mora",
                                    periods = period,
                                    path = moraRawPath,
                                    colClasses = "character",
                                    na.string = c("", NA))
  mora[, YEAR_MONTH := str_sub(period, 1, 6)]
  # Cambiamos los nombres de las variables por otros nombres estandarizados
  setnames(mora, setdiff(names(mora), "VLR_CUOTA"), namesMora)
  mora <- mora[, -c("VLR_CUOTA"), with = F]
  mora[, (numColsMora) := lapply(.SD, function(x){as.numeric(str_replace(x, ",", "."))}),
       .SDcols = numColsMora]
  # Estandarizamos los valores del campo TIPO_DE_PRODUCTO
  mora[, LINEA := toupper(str_replace_all(str_replace_all(iconv(LINEA,
                                                                from = "latin1",
                                                                to='ASCII//TRANSLIT'),
                                                          "[ -]+","_"),
                                          "\\.", ""))]

  # Creamos la variable auxiliar YEAR_MONTH_DAY
  mora[, YEAR_MONTH_DAY := period]
  # Quitamos las filas con la variable ID_CONTRATO a missing 
  mora <- unique(mora[!is.na(ID_CONTRATO) & ID_CONTRATO != ""])
  # Nos quedamos con las variables relevantes para el modelo
  mora <- mora[LINEA %in% productsToIncludeMora, colsToSelectMora, with = F]
  
  # Si saveSummary es igual a TRUE se guarda un archivo resumen de data quality
  if (saveSummary){
    saveTableSummariesByPeriod(mora, path = dataQualityAnalysisPath,
                               source = "mora", periodIDvarname = "YEAR_MONTH_DAY")  
  }
  
  saveProcessedTableByPeriod(mora,
                             path = moraProcessedPath,
                             source = "processedMora",
                             periodIDvarname = "YEAR_MONTH_DAY")
  
}
