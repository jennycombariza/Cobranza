#' createMoraMaster
#' 
#' @description Funcion encargada de crear las variables sinteticas de mora 
#' 
#' @param period dia en formato "YYYYMMdd"
#' @return identificador unico de entrada en mora junto a las variables 
#' sinteticas para la fecha seleccionada
#' @examples
#' createMoraMaster("20170801")

createMoraMaster <- function(period){
  # Definimos la profundidad historica para la creacion de variables sinteticas 
  # temporales
  fileSource <- "expandedMora"
  maxPeriodsBack <- 120
  cat("Expanding Mora for " %+% paste(period, collapse = ", ") %+% "...\n")
  periodsRequired <- seqDays(dayAddInteger(min(period), -maxPeriodsBack), max(period))
  
  # Seleccion de archivos de caseBase de mora guardados en 1.Data/PreparedData
  # con la etiqueta "caseBase" para el periodo seleccionado
  caseBase <- getProcessedTableByPeriod(dataSource = "caseBase",
                                        path = customerBaseExpandedPath,
                                        periods = period,
                                        colClasses = "character",
                                        na.strings = c("", NA))
 
  # Seleccion de archivos procesados de mora guardados en 1.Data/PreparedData
  # con la etiqueta "processedMora" para las fechas contenidas en el vector
  # periodsRequired
  moraDt <- getProcessedTableByPeriod(dataSource = "processedMora",
                                      path = moraProcessedPath,
                                      periods = periodsRequired,
                                      colClasses = "character",
                                      na.strings = c("", NA))
  
  auxCaseBase <- getProcessedTableByPeriod(dataSource = "caseBase",
                                           path = customerBaseExpandedPath,
                                           periods = periodsRequired,
                                           colClasses = "character",
                                           na.strings = c("", NA))
  auxCaseBase[, ENTRADA_MORA := 1]
  

  # obtenemos un vector de obligaciones contenidos en la caseBase
  listIDs <- unique(caseBase[, ID_CONTRATO])
  
  # Hacemos el grid de la lista de obligaciones y las fechas necesarias para 
  # hacer el expand
  moraDt_forExpand <- moraDt[ID_CONTRATO %in% listIDs, .(ID_CONTRATO,
                                                         YEAR_MONTH_DAY,
                                                         DIAS,
                                                         SALDO_CAPITAL,
                                                         EN_MORA)]
  dt.grid <- CJ(listIDs, periodsRequired)
  names(dt.grid) <- c("ID_CONTRATO", "YEAR_MONTH_DAY")
  
  # Creamos una tabla auxiliar realizando un left join de la tabla 
  # obligacion-fecha con la informacion contenida en la tabla de mora
  auxDt <- merge(dt.grid,
                            moraDt_forExpand,
                            by= c("ID_CONTRATO", "YEAR_MONTH_DAY"),
                            all.x = T)
  # Seleccionamos las columnas sobre las que se crearan las variables sinteticas
  colsToExpand <- c("DIAS", "SALDO_CAPITAL", "EN_MORA")
  # Nos aseguramos de que se interpreten como numericas
  auxDt[, (colsToExpand) := lapply(.SD, as.numeric), .SDcols = colsToExpand]
  # Seleccionamos los metodos aritmeticos con los que crearemos las variables
  expandMethods <- c("mean", "max", "sum", "sd")
  # Creamos las variables sinteticas para los tramos 30, 60, 90 y 120 dias
  aux <- expandByPeriods(auxDt,
                         colsToExpand = colsToExpand,
                         nPeriods = seq(30, maxPeriodsBack, by = 30),
                         timeSeriesIDcolNames = "ID_CONTRATO",
                         periodColName = "YEAR_MONTH_DAY",
                         expandMethods = expandMethods,
                         includeCurrentPeriod = FALSE,
                         colsToRatio = c(),
                         doGrid = FALSE,
                         doSort = TRUE,
                         verbose = TRUE)
  
  # Hacemos un inner join con la tabla de caseBase para quedarnos con una unica
  # linea para la obligacion y fecha seleccionada
  aux <- merge(aux, 
               caseBase,
               by = c("ID_CONTRATO", "YEAR_MONTH_DAY"),
               all.x = F,
               all.y = F)
  
  # Obtenemos las variables iniciales para el periodo analizado
  moraDt <- merge(aux[, -c("DIAS",
                           "SALDO_CAPITAL",
                           "EN_MORA"), with = F],
                  moraDt,
                  by = c("ID_CONTRATO", "YEAR_MONTH_DAY"))
  
  # Definimos un vector con los nombres de las variables sinteticas creadas
  createdCols <- names(moraDt) %gv% "Prev[0-9]{2,3}"
  # Reemplazamos los "-Inf" por NA 
  for(var in createdCols){
    cat("Limpiando infinitos de la variable", var, fill = TRUE)
    set(x = moraDt,
        j = var,
        i = moraDt[, which(is.nan(get(var)) | is.infinite(get(var)))],
        value = NA)
    
  }
  
  # En bucle creamos las variables de evolucion de las variables creadas con el
  # expand; Evolucion definida con respecto a los valores de dia -30
  # (variable_Prev_30 / variable_Prev_30+N) -1
  for (bucket in seq(30, maxPeriodsBack-30, by = 30)){
    for (col in colsToExpand){
      for (type in expandMethods){
        moraDt[, ("delta_" %+% type %+% "_" %+% col %+% "_" %+% bucket %+% "_"  %+% (bucket + 30)) :=
                 (get(col %+% "_" %+% type %+% "Prev" %+% bucket) /
                    get(col %+% "_" %+% type %+% "Prev" %+% (bucket + 30))) -1]
      }
    }
  }
  
  # Creamos la variable auxiliar de LINEA para su binarizacion posterior
  moraDt[, LINEA_AUX := ifelse(LINEA %in% productsToIncludeMora, LINEA, "OTHER") ]
  dcastMora_linea <- dcast.data.table(moraDt,
                                      ID_CONTRATO ~ paste0("LINEA_", LINEA_AUX),
                                      value.var = "YEAR_MONTH",
                                      fun.aggregate = length)
  
  # Hacemos un left join con la tabla final de las variables binarizadas
  moraDt <- merge(moraDt,
                  dcastMora_linea,
                  by = "ID_CONTRATO",
                  all.x = T)
  
  # Creamos la variable auxiliar de RIESGO a partir de los campos que contengan un niveld e riesgo explicito
  # para su binarizacion posterior
  explicitRiesgo <- moraDt[!is.na(RIESGO), unique(RIESGO)][str_detect(moraDt[!is.na(RIESGO), unique(RIESGO)], "BAJO|MEDIO|ALTO|MUYALTO")]
  moraDt[, RIESGO_AUX := ifelse(RIESGO %in% explicitRiesgo, str_extract(RIESGO,"BAJO|MEDIO|ALTO|MUYALTO"), "OTHER")]
  
  dcastMora_riesgo <- dcast.data.table(moraDt,
                                       ID_CONTRATO ~ paste0("RIESGO_",
                                                            toupper(str_replace_all(iconv(RIESGO_AUX,
                                                                                          to='ASCII//TRANSLIT'),
                                                                                    "[ -]+","_"))),
                                       value.var = "YEAR_MONTH",
                                       fun.aggregate = length)
  
  # Hacemos un left join con la tabla final de las variables binarizadas
  moraDt <- merge(moraDt,
                  dcastMora_riesgo,
                  by = "ID_CONTRATO",
                  all.x = T)
  
  # Creacion de variables a nivel trayecto de mora
  auxDt[, YEAR_MONTH_DAY := ymd(YEAR_MONTH_DAY)]
  auxCaseBase[, YEAR_MONTH_DAY := ymd(YEAR_MONTH_DAY)]
  
  # Contrato
  for(i in seq(30, maxPeriodsBack, by = 30)){
    auxMora <- auxDt[ymd(period) - YEAR_MONTH_DAY < i,
                     .(ID_CONTRATO, YEAR_MONTH_DAY, DIAS)]
    
    auxMora <- merge(auxMora,
                     auxCaseBase[, .(ID_CONTRATO, YEAR_MONTH_DAY, ENTRADA_MORA)],
                     by = c("ID_CONTRATO", "YEAR_MONTH_DAY"),
                     all.x = TRUE,
                     all.y = FALSE)
    
    
    auxMora[is.na(ENTRADA_MORA), ENTRADA_MORA := 0]
    auxMora[, aux := 1:.N, by = ID_CONTRATO]
    auxMora[aux == 1 & DIAS > 0, ENTRADA_MORA := 1]
    auxMora[, TRAYECTO_MORA := cumsum(ENTRADA_MORA), by = ID_CONTRATO]
    auxMora[DIAS == 0, TRAYECTO_MORA := 0]
    auxMora[, MAX_TRAYECTO_MORA := max(TRAYECTO_MORA), by = ID_CONTRATO]
    auxMora[MAX_TRAYECTO_MORA == TRAYECTO_MORA, TRAYECTO_MORA := NA]
    auxMora[, MAX_TRAYECTO_MORA := MAX_TRAYECTO_MORA - 1]
    auxMora[TRAYECTO_MORA == 0, TRAYECTO_MORA := NA]
    auxMora[, MAX_DIAS_TRAYECTO_MORA := max(DIAS, na.rm = TRUE),
            by = .(ID_CONTRATO, TRAYECTO_MORA)]
    
    auxMora <- auxMora[!is.na(TRAYECTO_MORA), 
                       lapply(.SD, first),
                       by = .(ID_CONTRATO, TRAYECTO_MORA),
                       .SDcols = c("MAX_TRAYECTO_MORA", "MAX_DIAS_TRAYECTO_MORA")]
    auxMora <- auxMora[, .(MEAN_DIAS_TRAYECTO_MORA = mean(MAX_DIAS_TRAYECTO_MORA),
                           SUM_DIAS_TRAYECTO_MORA = sum(MAX_DIAS_TRAYECTO_MORA),
                           SD_DIAS_TRAYECTO_MORA = sd(MAX_DIAS_TRAYECTO_MORA),
                           MIN_DIAS_TRAYECTO_MORA = min(MAX_DIAS_TRAYECTO_MORA),
                           NUMERO_TRAYECTO_MORA = MAX_TRAYECTO_MORA),
                       by = .(ID_CONTRATO)][, lapply(.SD, first), by = ID_CONTRATO]
    auxNames <- c("MEAN_DIAS_TRAYECTO_MORA", "SUM_DIAS_TRAYECTO_MORA", 
                  "SD_DIAS_TRAYECTO_MORA", "MIN_DIAS_TRAYECTO_MORA",
                  "NUMERO_TRAYECTO_MORA")
    setnames(auxMora, auxNames, paste0(auxNames, "_LAST", i))
    
    if(i == 30){
      auxFinal <- merge(caseBase,
                        auxMora,
                        by = c("ID_CONTRATO"),
                        all.x = TRUE)
    } else {
      auxFinal <- merge(auxFinal,
                        auxMora,
                        by = c("ID_CONTRATO"),
                        all.x = TRUE)
    }
  }
  
  moraDt <- merge(moraDt,
                      auxFinal[,-c("DIAS_MORA_PREVIO_ENTRADA"), with = F],
                      by = c("ID_CONTRATO", "YEAR_MONTH_DAY"),
                      all.x = TRUE,
                      all.y = FALSE)
  moraDt <- capitalizeNames(moraDt)
  saveProcessedTableByPeriod(moraDt, 
                             path = moraExpandedPath,
                             source = "expandedMora",
                             periodIDvarname = "YEAR_MONTH_DAY")
}

