#' createBuroMaster
#' 
#' @description Funcion encargada de crear las variables sinteticas de buro
#' 
#' @param period mes en formato "YYYYMM"
#' @return Tabla a nivel cliente con los datos a la última fecha disponible 
#' @examples
#' createBuroMaster("201708")

createBuroMaster <- function(period) {
  
  print(glue::glue('expanding bureau data for {period}'))
  
  maxMonthsBack <- 3
  previousPeriod <- monthAddInteger(min(period), -maxMonthsBack)
  
  # Cargamos el archivo de buro para el periodo analizado y el archivo del
  # trimestre anterior 
  
  # Cargamos los ficheros por separado y eliminamos los duplicados de ID existentes
  buroCurrent <- getProcessedTableByPeriod(dataSource = 'processedBuro',
                                           path=buroProcessedPath,
                                           periods = period,
                                           colClasses = "character",
                                           na.strings = c("", NA)
  )
  
  buroCurrent <- buroCurrent[order(buroCurrent[, ID_CLIENTE])]
  buroCurrent <- buroCurrent[!duplicated(buroCurrent[, ID_CLIENTE]),]
  
  
  buroPrevious <- getProcessedTableByPeriod(dataSource = 'processedBuro',
                                            path=buroProcessedPath,
                                            periods = previousPeriod,
                                            colClasses = "character",
                                            na.strings = c("", NA)
  )
  
  buroPrevious <- buroPrevious[order(buroPrevious[, ID_CLIENTE])]
  buroPrevious <- buroPrevious[!duplicated(buroPrevious[, ID_CLIENTE]),]
  
  
  buro <- rbind(buroPrevious, buroCurrent)
  
  print(glue::glue('generating synthetic variables'))
  
  # Generamos variables sinteticas
  # Cambiamos la variable caracter de rango aproximado edad por la media del intervalo
  buro[,  MEDIA_RANGO_APROXIMADO_EDAD := mean(as.numeric(str_extract_all(RANGO_APROXIMADO_EDAD, "[0-9]+")[[1]])),
       by = .(ID_CLIENTE, YEAR_MONTH)]
  buro[, (numericColumnsBuro) := lapply(.SD, as.numeric), .SDcols = numericColumnsBuro]
  # Quitamos la variable original de porcentaje de utilizacion de las TDC y la calculamos de nuevo
  buro[, PORC_UTILIZACION_TDC := NULL]
  buro[, PORC_UTILIZACION_TDC := VALOR_UTILIZADO_TDC / VALOR_CUPOS_TDC ]
  buro[, SALDO_TOTAL_SISTEMA:= VALOR_SALDO_CB + VALOR_SALDO_CV + VALOR_SALDO_OCF + VALOR_UTILIZADO_TDC + VALOR_SALDO_SR + VALOR_SALDO_COOP + VALOR_SALDO_COD]
  buro[, VALOR_INICIAL_TOTAL_SISTEMA := VALOR_INICIAL_CB + VALOR_INICIAL_CV + VALOR_INICIAL_OCF + VALOR_INICIAL_SR + VALOR_INICIAL_COOP ]
  buro[, MORA_TOTAL_SISTEMA := VALOR_MORA_CB + VALOR_MORA_CV + VALOR_MORA_OCF + VALOR_MORA_TDC + VALOR_MORA_SR + VALOR_MORA_COOP + VALOR_MORA_COD]
  buro[, CUOTAS_TOTAL_SISTEMA := VALOR_CUOTAS_CB + VALOR_CUOTAS_CV + VALOR_CUOTAS_OCF + VALOR_CUOTAS_TDC + VALOR_CUOTAS_SR + VALOR_CUOTAS_COOP + VALOR_CUOTAS_COD]

  # Binarizamos la variable genero
  buro[GENERO == "F", GENERO := 1]
  buro[GENERO == "M", GENERO := 0]
  buro[, GENERO := as.numeric(GENERO)]

  # Obtenemos el ratio de saldo en mora enter el total de saldo por ciudad
  
  buro[, PCT_MORA_CIUDAD := sum(MORA_TOTAL_SISTEMA, na.rm = T) / sum(SALDO_TOTAL_SISTEMA, na.rm = T), .(CIUDAD_EXPEDICION, YEAR_MONTH)]
  
  divisionNA <- function(X, Y, replaceNAValue = Inf) {
    
    ifelse(Y == 0, replaceNAValue, X / Y)
    
  }
  # Creamos variables sinteticas de ratios adicionales a nivel cliente
  buro[,`:=`(
    PCT_SALDO_EN_MORA                            = divisionNA(MORA_TOTAL_SISTEMA, SALDO_TOTAL_SISTEMA, replaceNAValue = 0),
    PCT_PRODUCTOS_EN_MORA                        = divisionNA((NUM_OBLIG_ACTIV - NUM_OBLIG_AL_DIA), NUM_OBLIG_ACTIV, replaceNAValue = 0),
    MORA_POR_PRODUCTO_ACTIVO                     = divisionNA(MORA_TOTAL_SISTEMA, NUM_OBLIG_ACTIV, replaceNAValue = 0),
    PCT_MORA_COMO_CODEUDOR                       = divisionNA(VALOR_MORA_COD, MORA_TOTAL_SISTEMA, replaceNAValue = 0),
    YEAR_MONTH                                   = as.character(YEAR_MONTH)
  )]
  

  

  print(glue::glue('expanding by periods'))


  colsToRatio <- c("MORA_TOTAL_SISTEMA",
                   "SALDO_TOTAL_SISTEMA",
                   "PCT_PRODUCTOS_EN_MORA",
                   "NUM_OBLIG_ACTIV",
                   "NUM_OBLIG_AL_DIA", 
                   "PCT_MORA_CIUDAD",
                   "VALOR_UTILIZADO_TDC",
                   "PORC_UTILIZACION_TDC",
                   "VALOR_MORA_TDC",
                   "ACIERTA_A_FINANCIERO",
                   "QUANTO3")

  buroExpanded <- expandByPeriods(dt = buro,
                                 colsToExpand = NULL,
                                 nPeriods = 1,
                                 timeSeriesIDcolNames = "ID_CLIENTE",
                                 periodColName = "YEAR_MONTH",
                                 expandMethods = "shift",
                                 colsToRatio = colsToRatio,
                                 includeCurrentPeriod = TRUE,suffixRatios = "_RATIO_T",
                                 doGrid = TRUE,
                                 doSort = TRUE,
                                 verbose = TRUE
  )
  
  # Definimos un vector con los nombres de las variables sinteticas creadas
  createdCols <- names(buroExpanded) %gv% "RATIO_T1"
  # Reemplazamos los "-Inf|NaN" por 0
  for(var in createdCols){
    cat("Limpiando infinitos de la variable", var, fill = TRUE)
    set(x = buroExpanded,
        j = var,
        i = buroExpanded[, which(is.nan(get(var)) | is.infinite(get(var)))],
        value = 0)
    
  }
  

  # Nos quedamos solo con el periodo analizado
  
  finalOutput <- buroExpanded[YEAR_MONTH == period]
  
  finalOutput[is.na(PEOR_CALIFICACION_T1), PEOR_CALIFICACION_T1 := "SIN_PUNTUAR"]
  finalOutput[is.na(PEOR_CALIFICACION_T2), PEOR_CALIFICACION_T2 := "SIN_PUNTUAR"]
  
  dcast_peor_calificacion_1 <- dcast.data.table(finalOutput,
                                                ID_CLIENTE ~ paste0("CALIFICACION_T1_",
                                                                    PEOR_CALIFICACION_T1),
                                                value.var = "YEAR_MONTH",
                                                fun.aggregate = length)
  
  # Hacemos un left join con la tabla final de las variables binarizadas
  finalOutput <- merge(finalOutput,
                       dcast_peor_calificacion_1,
                       by = "ID_CLIENTE",
                       all.x = T)
  
  dcast_peor_calificacion_2 <- dcast.data.table(finalOutput,
                                                ID_CLIENTE ~ paste0("CALIFICACION_T2_",
                                                                    PEOR_CALIFICACION_T2),
                                                value.var = "YEAR_MONTH",
                                                fun.aggregate = length)
  
  # Hacemos un left join con la tabla final de las variables binarizadas
  finalOutput <- merge(finalOutput,
                       dcast_peor_calificacion_2,
                       by = "ID_CLIENTE",
                       all.x = T)
  
  saveProcessedTableByPeriod(dt = finalOutput,
                             path = buroExpandedPath,
                             source = 'expandedBuro',
                             periodIDvarname = 'YEAR_MONTH'
  )
  
}

