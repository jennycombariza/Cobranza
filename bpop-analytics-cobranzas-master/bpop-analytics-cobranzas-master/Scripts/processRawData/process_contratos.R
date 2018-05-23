#' process_contratos
#' 
#' @description Funcion procesar los datos de contratos, estandarizando los formatos y nombres de variables
#' 
#' @param period dia en formato "YYYYMM"
#' @return Guarda la tabla de contratos estandarizada con el prefijo "processedContratos"
#' @examples
#' process_contratos("201708")

processContratos <- function(period, saveSummary = T){
  
  cat("Processing contratos for " %+% paste(period, collapse = ", ") %+% "...\n")
  # Cargamos el fichero de contatos para el periodo seleccionado
  contratos <- getProcessedTableByPeriod(dataSource = "contratos",
                                         periods = period,
                                         path = contratosRawPath,
                                         colClasses = "character",
                                         na.strings = c("", NA))
  capitalizeNames(contratos)
  contratos[, YEAR_MONTH := period]
  
  # Editamos los nombres de algunas columnas que vienen con parentesis
  setnames(contratos, names(contratos), str_replace_all(names(contratos), "_\\(SI\\/NO\\)", ""))
  setnames(contratos, c("CONTRATO", "ID_PERSONA"), c("ID_CONTRATO", "ID_CLIENTE"))
  # Coercionamos las variables númericas, binomiales y de fechas al formato deseado
  contratos[, (numColsContratos) := lapply(.SD, function(x){as.numeric(str_replace(x, ",", "."))}), .SDcols = numColsContratos]
  contratos[, (binomColsContratos) := lapply(.SD, function(x){ifelse(x == "SI", 1, 0)}), .SDcols = binomColsContratos]
  contratos[, (dateColsContratos) := lapply(.SD, function(x){as.Date(x, "%Y%m%d")}), .SDcols = dateColsContratos]
  
  # Corregimos los valores del campo TIPO_DE_PRODUCTO
  contratos[, TIPO_DE_PRODUCTO := toupper(str_replace_all(str_replace_all(iconv(TIPO_DE_PRODUCTO,
                                                                                from = "latin1",
                                                                                to='ASCII//TRANSLIT'),
                                                                          "[ -]+","_"),
                                                          "\\.", ""))]
  
  # Si saveSummary es igual a TRUE se guardara un data quality report en la carpeta de analisis
  if (saveSummary){
    saveTableSummariesByPeriod(contratos, path = dataQualityAnalysisPath, source = "contratos", periodIDvarname = "YEAR_MONTH")  
  }
  
  # Seleccionamos solo aquellas columnas que necesitamos
  contratos <- contratos[, colsToSelectContratos, with = F]
  
  saveProcessedTableByPeriod(contratos,
                             path = contratosProcessedPath,
                             source = "processedContratos",
                             periodIDvarname = "YEAR_MONTH")
}