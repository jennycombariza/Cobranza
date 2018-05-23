#' process_clientes
#' 
#' @description Funcion procesar los datos de clientes, estandarizando los formatos y nombres de variables
#' 
#' @param period dia en formato "YYYYMM"
#' @return Guarda la tabla de contratos estandarizada con el prefijo "processedClientes"
#' @examples
#' process_clientes("201708")

processClientes <- function(period, saveSummary = T){
  cat("Procesando clientes para " %+% paste(period, collapse = ", ") %+% "...\n")
  clientes <- getProcessedTableByPeriod(dataSource = "clientes",
                                        periods = period,
                                        path = clientesRawPath,
                                        colClasses = "character",
                                        na.strings = c("", NA))
  clientes[, YEAR_MONTH := period]
  # Seleccionamos variables atemporales porque actualmente no tenemos fotos mensuales del fichero clientes
  clientes <- clientes[, varsToSelectClientes, with = FALSE]
  setnames(clientes, c("AA_NIT_ENCRIPTADO"), c("ID_CLIENTE"))
  # Si saveSummary es igual a TRUE se guarda un archivo resumen de data quality
  if (saveSummary){
    saveTableSummariesByPeriod(clientes, path = dataQualityAnalysisPath,
                               source = "clientes", periodIDvarname = "YEAR_MONTH")  
  }
  # Filtramos filas con ID_CLIENTE sin informar
  clientes <- clientes[!is.na(ID_CLIENTE), ]
  # Cambiamos el formato de la fecha
  clientes[, FECHA_NACIMIENTO := dmy(FECHA_NACIMIENTO)]
  # Calculamos la EDAD del cliente a la fecha del periodo analizado
  clientes[, EDAD := as.numeric(as.Date(paste0(YEAR_MONTH, "01"), "%Y%M%d") - FECHA_NACIMIENTO)/365]
  # Dado el formato incial de la fecha de nacimiento donde no se informa el siglo hacemos un ajuste
  # cuando el calculo de edad sale negativo
  clientes[, EDAD := round(ifelse(EDAD < 0, 100 + EDAD, EDAD),0)]
  clientes[, FECHA_NACIMIENTO := NULL]
  # Cambiamos la variable GENERO a numerica
  clientes[GENERO == "M", GENERO := 0]
  clientes[GENERO == "F", GENERO := 1]
  clientes[, GENERO := as.numeric(GENERO)]
  clientes[!GENERO %in% c(0,1), GENERO := NA]
  
  # Nos quedamos con una unica linea por cliente en caso de que presente informacion contradictoria
  clientes[, EDAD := max(EDAD, na.rm = T), by = .(ID_CLIENTE)]
  clientes[, GENERO := max(GENERO, na.rm = T), by = .(ID_CLIENTE)]
  
  for(var in c("EDAD", "GENERO")){
    cat("Limpiando infinitos de la variable", var, fill = TRUE)
    set(x = clientes,
        j = var,
        i = clientes[, which(is.nan(get(var)) | is.infinite(get(var)))],
        value = NA)
    
  }
  
  clientes <- unique(clientes)
  saveProcessedTableByPeriod(clientes,
                             path = clientesProcessedPath,
                             source = "processedClientes",
                             periodIDvarname = "YEAR_MONTH")
}