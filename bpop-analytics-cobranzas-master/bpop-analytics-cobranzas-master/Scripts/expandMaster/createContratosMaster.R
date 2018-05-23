#' expand_contratos
#' 
#' @description Funcion que sirve para agregar profundidad historica a la informacion
#' contenida en la tabla de contratos#' 
#' @param period dia en formato "YYYYMM"
#' @return Guarda la tabla de contratos expandida con info historica con el 
#' prefijo "expandedContratos"
#' @examples
#' expand_contratos("201708")

createContratosMaster <- function(period){
  
  fileSource <- "expandedContratos"
  cat("Agregando historico al fichero de contratos...", period, fill = TRUE)
  # Seleccionamos la profudidad histórica deseada (3 meses)
  maxMonthsBack <- 3
  monthsRequired <- seqMonth(monthAddInteger(min(period), -maxMonthsBack), max(period))
  # Cargamos los datos para el periodo seleccionado + los datos de 3 meses
  # anteriores
  dt.contratosExpanded <- getProcessedTableByPeriod(dataSource = "processedContratos",
                                                    path = contratosProcessedPath,
                                                    periods = monthsRequired,
                                                    colClasses = "character",
                                                    na.strings = c("", NA))
  
  dt.contratosExpanded[, (numColsContratos) := lapply(.SD, as.numeric), .SDcols = numColsContratos]
  # Calculamos la deuda incial y el saldo de capital y el numero de productos
  # a nivel cliente
  dt.contratosExpanded[, DEUDA_INICIAL_CLIENTE := sum(DEUDA_TOTAL_INICIAL, na.rm = T),
                       by = .(ID_CLIENTE, YEAR_MONTH)]
  
  dt.contratosExpanded[, SAL_CAPITAL_CLIENTE := sum(SAL_CAPITA, na.rm = T),
                       by = .(ID_CLIENTE, YEAR_MONTH)]
  
  dt.contratosExpanded[, NUM_PRODUCTOS := .N, by = .(ID_CLIENTE, YEAR_MONTH)]
  
  # Calculamos el numero de productos de tipo tarjeta de credito o libranzas de
  # cada cliente
  dt.contratosExpanded[TIPO_DE_PRODUCTO %in% c("TARJETA_DE_CREDITO","LIBRANZAS"), NUM_LIBRANZAS_TARJETAS := as.numeric(.N), by = .(ID_CLIENTE, YEAR_MONTH)]
  dt.contratosExpanded[, NUM_LIBRANZAS_TARJETAS := max(NUM_LIBRANZAS_TARJETAS, na.rm = T), by = .(ID_CLIENTE, YEAR_MONTH)]
  dt.contratosExpanded[, NUM_LIBRANZAS_TARJETAS := ifelse(NUM_LIBRANZAS_TARJETAS>=0, NUM_LIBRANZAS_TARJETAS, 0)]

  # Binarizamos la variable TIPO_DE_PRODUCTO
  dcast_productType <-  dcast.data.table(dt.contratosExpanded,
                                         ID_CONTRATO + YEAR_MONTH ~ paste0("PT_",
                                                                           TIPO_DE_PRODUCTO),
                                         fun.aggregate = length,
                                         value.var = "ID_CONTRATO")
  # Quitamos la variable TIPO_DE_PRODUCTO porque ya disponemos de las columnas
  # binarizadas
  dt.contratosExpanded[, TIPO_DE_PRODUCTO := NULL]
  
  # Pegamos las variables binarizadas a la tabla original
  dt.contratosExpanded <- merge(dt.contratosExpanded,
                                dcast_productType,
                                by = c("ID_CONTRATO","YEAR_MONTH"),
                                all.x = T)
  # Coercionamos las variables de fechas para asegurar que están en formato correcto
  dt.contratosExpanded[, (dateColsContratos) := lapply(.SD, ymd), .SDcols = dateColsContratos]
  # Creamos la variable de duracion de contrato en meses
  dt.contratosExpanded[, CONTRACT_DURATION := ymd(FECHA_DE_FINALIZACION) - ymd(FECHA_DE_INICIO)]
  
  # Creamos la variable de tiempo hasta el final de contrato en meses
  dt.contratosExpanded[, TIME_TO_CONTRACT_EXP := FECHA_DE_FINALIZACION - as.Date(paste0(YEAR_MONTH, "01"), '%Y%m%d')]
  
  # Creamos la variable de antiguedad de contrato
  dt.contratosExpanded[, CONTRACT_SENIORITY := as.Date(paste0(YEAR_MONTH, "01"), '%Y%m%d') - FECHA_DE_INICIO]
  # Seleccionamos las variables para mirar el historico
  varsToExpand <- c("SAL_CAPITAL_CLIENTE", "VLR_CUOTA")
  # Seleccionamos las variables para sacar ratios respecto al periodo actual
  varsToRatio <- c("SAL_CAPITAL_CLIENTE", "VLR_CUOTA", "NUM_PRODUCTOS", "NUM_LIBRANZAS_TARJETAS")
  # Seleccionamos los metodos de expansion de los datos
  expandMethods <- c("sd", "shift")
  dt.contratosExpanded <- expandByPeriods(dt.contratosExpanded,
                                          colsToExpand = varsToExpand,
                                          nPeriods = maxMonthsBack,
                                          timeSeriesIDcolNames = "ID_CLIENTE",
                                          periodColName = "YEAR_MONTH",
                                          expandMethods = expandMethods,
                                          includeCurrentPeriod = TRUE,
                                          colsToRatio = varsToRatio,
                                          doGrid = TRUE,
                                          doSort = TRUE,
                                          verbose = TRUE)
  
  # La funcion expandByPeriods crea variables de ratio entre la desviacion estandar
  # y el valor actual de la variable (estos ratios no se tendran en cuenta)
  ratioSDcols <- names(dt.contratosExpanded) %gv% "_sdPrevRatio"
  dt.contratosExpanded[, (ratioSDcols) := NULL]
  # Arreglamos los valores de -Inf o NaN que puede generan expandByPeriods
  createdCols <- names(dt.contratosExpanded) %gv%   "_sd|_pRatio|Prev[0-9]{2,3}|p[0-9]{1}"
  # Reemplazamos los "-Inf" por NA 
  for(var in createdCols){
    cat("Limpiando infinitos de la variable", var, fill = TRUE)
    set(x = dt.contratosExpanded,
        j = var,
        i = dt.contratosExpanded[, which(is.nan(get(var)) | is.infinite(get(var)))],
        value = NA)
    
  }
  # Nos quedamos con los datos del periodo analizado
  dt.contratosExpanded <- dt.contratosExpanded[YEAR_MONTH  == period & !is.na(ID_CONTRATO)]
  saveProcessedTableByPeriod(dt.contratosExpanded,
                             path = contratosExpandedPath,
                             source = fileSource,
                             periodIDvarname = "YEAR_MONTH")
} 


