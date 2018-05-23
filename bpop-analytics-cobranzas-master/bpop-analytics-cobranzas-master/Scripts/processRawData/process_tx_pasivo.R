#' process_transactions
#' 
#' @description Funcion de limpieza de datos de la tabla de transaccional pasivo. Esta funcion
#' recibe los datos del S3 segun requerimientos de extraccion y los adapta para
#' ser usado por el resto de funciones
#' 
#' @param periodo Mes para el cual se va a ejecutar la funcion. Formato YYYYMM
#' @return Datos de transacciones de cuentas corrientes
#' preparados para ser utilizados por el resto de funciones
#' @examples
#' process_transactions("201702")


processTxPasivo <- function(period, saveSummary = T){
  cat("Procesando Transacciones de Pasivo para " %+% paste(period, collapse = ", ") %+% "...\n")
  dt.transactions <- getProcessedTableByPeriod(dataSource = "tx_pasivo",
                                               periods = period,
                                               path = txPasivoRawPath,
                                               colClasses = "character",
                                               na.strings = c("", NA))
  dt.transactions[, YEAR_MONTH := period]
  # Ponemos nombres homogeneizados a las variables KEY
  cambiarNombre <- names(dt.transactions) %in% "CTA_ENCRIPTADO" %>% sum()
  if (cambiarNombre > 0){
    setnames(dt.transactions, c("CTA_ENCRIPTADO"), c("ID_CONTRATO"))  
  }
  
  # Nos aseguramos de que el formato de las columnas numericas sea correcto
  dt.transactions[, (numColsTx_pasivo) := lapply(.SD, function(x){as.numeric(x)}), .SDcols = numColsTx_pasivo]
  # Si saveSummary es igual a TRUE se guardara un data quality report en la carpeta de analisis
  if (saveSummary){
    saveTableSummariesByPeriod(dt.transactions, path = dataQualityAnalysisPath, source = "tx_pasivo", periodIDvarname = "YEAR_MONTH")  
  }
  # Para las operaciones de debito (ej.: retirada de efectivo) cambiamos de signo para facilitar las operaciones
  dt.transactions[SIGNO_TX == "+", VALOR_TX := VALOR_TX * -1]
  
  print(paste0("Saving transactions for period...",  period))
  saveProcessedTableByPeriod(dt.transactions,
                             path = txPasivoProcessedPath,
                             source = "processedTX_pasivo",
                             periodIDvarname = "YEAR_MONTH")
}


  
 
