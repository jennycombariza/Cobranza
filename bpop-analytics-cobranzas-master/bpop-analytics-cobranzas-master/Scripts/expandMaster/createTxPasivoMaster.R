#' createTxPasivoMaster
#' 
#' @description Funcion encargada de crear las variables sinteticas de 
#' transacciones de pasivo 
#' 
#' @param period dia en formato "YYYYMMdd"
#' @return fichero resumen, tanto en numero como en cantidades, de las 
#' transacciones en cuentas de pasivo realizadas en el mes, así como la evolucion
#' de las principales metricas a lo largo de los ultimos 3 meses
#' @examples
#' createTxPasivoMaster("201708")

createTxPasivoMaster <- function(period){
gc()
cat("Expandiendo el transaccional para el periodo " %+% paste(period, collapse = ", ") %+% "...\n")
# Cargamos los datos de transacciones procesados para el periodo seleccionado
maxMonthsBack <- 3
monthsRequired <- seqMonth(monthAddInteger(min(period), -maxMonthsBack), max(period))
dt.transactionsExpanded <- getProcessedTableByPeriod(dataSource = "processedTX_pasivo",
                                                     path = txPasivoProcessedPath,
                                                     periods = monthsRequired,
                                                     colClasses = "character",
                                                     na.strings = c("", NA))

# Calculamos el día de cada transaccion
dt.transactionsExpanded[, TRANS_DAY := as.numeric(str_sub(FECHA_TX, 7,8))]
# Coercionamos la variable valor_tx a numerica para poder realizar operaciones
dt.transactionsExpanded[, VALOR_TX := as.numeric(VALOR_TX)]
# Definimos el periodo del mes durante el cual tuvo lugar la transaccion
dt.transactionsExpanded[TRANS_DAY > 20, MONTH_PHASE := "LAST_MONTH_PHASE"]
dt.transactionsExpanded[TRANS_DAY <= 20, MONTH_PHASE := "MIDDLE_MONTH_PHASE"]
dt.transactionsExpanded[TRANS_DAY <= 10, MONTH_PHASE := "FIRST_MONTH_PHASE"]

# Creamos una lista de clientes por mes
clients_month <- unique(dt.transactionsExpanded[, .(ID_CLIENTE, YEAR_MONTH)])

# Definimos un campo auxiliar indicando el signo de la transaccion
dt.transactionsExpanded[SIGNO_TX == "-", TRANS_SIGN := "POS"]
dt.transactionsExpanded[SIGNO_TX == "+", TRANS_SIGN := "NEG"]

output <- data.table()
# Calculamos el valor medio de las transacciones por signo de operacion y mes
dcast_avg_trans_sign <-  dcast.data.table(dt.transactionsExpanded, ID_CLIENTE + YEAR_MONTH ~ paste0("AVG_",TRANS_SIGN, "_TRAN")  ,
                                          fun.aggregate = mean,
                                          value.var = "VALOR_TX")
cols_avg_trans_sign <- setdiff(names(dcast_avg_trans_sign),
                               c("ID_CLIENTE", "YEAR_MONTH"))
dcast_avg_trans_sign[, (cols_avg_trans_sign) := lapply(.SD,
                                                     function(x){ifelse(is.nan(x)|is.na(x), 0, x)}),
                     .SDcols = cols_avg_trans_sign]

output <- merge(clients_month, dcast_avg_trans_sign, by = c("ID_CLIENTE", "YEAR_MONTH"), all.x = T)


# Calculamos el numero de transacciones por signo y mes
dcast_num_trans_sign <-  dcast.data.table(dt.transactionsExpanded,
                                          ID_CLIENTE + YEAR_MONTH ~ paste0("NT_",TRANS_SIGN),
                                          value.var = "VALOR_TX",
                                          fun.aggregate = length)
cols_num_trans_sign <- setdiff(names(dcast_num_trans_sign),
                               c("ID_CLIENTE", "YEAR_MONTH"))
output <- merge(output, dcast_num_trans_sign, by = c("ID_CLIENTE", "YEAR_MONTH"), all.x = T)

print(paste0("Casting total transaction amount per sign..."))
# Calculamos la suma de las transacciones por signo y mes
dcast_sum_trans_sign <-  dcast.data.table(dt.transactionsExpanded, ID_CLIENTE + YEAR_MONTH ~ paste0("SUM_",TRANS_SIGN, "_TRAN")  ,
                                          fun.aggregate = sum,
                                          value.var = "VALOR_TX")
dcast_sum_trans_sign <- dcast_sum_trans_sign[, NET_AMNT_TRANS := SUM_POS_TRAN + SUM_NEG_TRAN]
output <- merge(output, dcast_sum_trans_sign, by = c("ID_CLIENTE", "YEAR_MONTH"), all.x = T)

print(paste0("Casting total transaction amount per sign and the phase of the month..."))
# Calculamos la suma de las transacciones por signo y fase del mes en la que tuvieron lugar
dcast_sum_month_phase_trans_sign <-  dcast.data.table(dt.transactionsExpanded, ID_CLIENTE + YEAR_MONTH ~ paste0("SUM_",TRANS_SIGN, "_TRAN") + MONTH_PHASE  ,
                                                      fun.aggregate = sum,
                                                      value.var = "VALOR_TX")
output <- merge(output, dcast_sum_month_phase_trans_sign, by = c("ID_CLIENTE", "YEAR_MONTH"), all.x = T)

# Calculamos el valor de la transaccion maxima por signo y mes
dt.transactionsExpanded[TRANS_SIGN == "POS", MAX_POS_TRANS :=  max(VALOR_TX),
                        by = .(ID_CLIENTE, YEAR_MONTH)]
dt.transactionsExpanded[TRANS_SIGN == "NEG", MAX_NEG_TRANS :=  min(VALOR_TX), by = .(ID_CLIENTE, YEAR_MONTH)]

print(paste0("Calculando el dia del ingreso maximo..."))
DAY_MAX_POS_TRANS <- dt.transactionsExpanded[MAX_POS_TRANS == VALOR_TX, .(ID_CLIENTE, 
                                                                  MAX_POS_TRANS,
                                                                  DAY_MAX_POS_TRANS = TRANS_DAY,
                                                                  MIN_DAY_MAX_POS_TRANS = min(TRANS_DAY)), by = .(ID_CLIENTE, YEAR_MONTH)]

DAY_MAX_POS_TRANS <- DAY_MAX_POS_TRANS[MIN_DAY_MAX_POS_TRANS == DAY_MAX_POS_TRANS, .(ID_CLIENTE, YEAR_MONTH, MAX_POS_TRANS, DAY_MAX_POS_TRANS)] %>% unique()
output <- merge(output, DAY_MAX_POS_TRANS, by = c("ID_CLIENTE", "YEAR_MONTH"), all.x = T)

print(paste0("Calculando el dia del gasto maximo..."))
DAY_MAX_NEG_TRANS <- dt.transactionsExpanded[MAX_NEG_TRANS == VALOR_TX, .(ID_CLIENTE, 
                                                                  MAX_NEG_TRANS,
                                                                  DAY_MAX_NEG_TRANS = TRANS_DAY,
                                                                  MIN_DAY_MAX_NEG_TRANS = min(TRANS_DAY)), by = .(ID_CLIENTE, YEAR_MONTH)]

DAY_MAX_NEG_TRANS <- DAY_MAX_NEG_TRANS[MIN_DAY_MAX_NEG_TRANS == DAY_MAX_NEG_TRANS, .(ID_CLIENTE, YEAR_MONTH, MAX_NEG_TRANS, DAY_MAX_NEG_TRANS)] %>% unique()
output <- merge(output, DAY_MAX_NEG_TRANS, by = c("ID_CLIENTE", "YEAR_MONTH"), all.x = T)

output[, c("MAX_POS_TRANS", "MAX_NEG_TRANS") := lapply(.SD,
                                                       function(x){ifelse(is.nan(x)|is.na(x), 0, x)}),
       .SDcols = c("MAX_POS_TRANS", "MAX_NEG_TRANS")]

setorder(dt.transactionsExpanded, ID_CLIENTE, YEAR_MONTH, TRANS_SIGN, -VALOR_TX)

dt.transactionsExpanded[TRANS_SIGN == "NEG", RANK_NEG_TRANS := 1:.N , by = .(ID_CLIENTE, YEAR_MONTH)]
dt.transactionsExpanded[TRANS_SIGN == "POS", RANK_POS_TRANS := .N:1, by = .(ID_CLIENTE, YEAR_MONTH)]

print(paste0("Calculando la suma de los dos ingresos mas altos del mes..."))

dt.transactionsExpanded[!TRANS_SIGN %in% c("NEG") & RANK_POS_TRANS %in% c(1,2), SUM_2_LARGEST_POS_TRANS := sum(VALOR_TX), by = .(ID_CLIENTE, YEAR_MONTH)] 

SUM_2_LARGEST_POS_TRANS <- dt.transactionsExpanded[ !TRANS_SIGN %in% c("NEG") & RANK_POS_TRANS %in% c(1,2), .(ID_CLIENTE, YEAR_MONTH, SUM_2_LARGEST_POS_TRANS)] %>% unique()

output <- merge(output, SUM_2_LARGEST_POS_TRANS, by =  c("ID_CLIENTE", "YEAR_MONTH"), all.x = T)

print(paste0("Calculando la suma de los dos gastos mas altos del mes..."))

dt.transactionsExpanded[!TRANS_SIGN %in% c("POS") & RANK_NEG_TRANS %in% c(1,2), SUM_2_LARGEST_NEG_TRANS := sum(VALOR_TX), by = .(ID_CLIENTE, YEAR_MONTH)]

SUM_2_LARGEST_NEG_TRANS <- dt.transactionsExpanded[ !TRANS_SIGN %in% c("POS") & RANK_NEG_TRANS %in% c(1,2), .(ID_CLIENTE, YEAR_MONTH, SUM_2_LARGEST_NEG_TRANS)] %>% unique()

output <- merge(output, SUM_2_LARGEST_NEG_TRANS, by =  c("ID_CLIENTE", "YEAR_MONTH"), all.x = T)

# Calculando la proporcion de los ingresos / gastos por fase del mes respecto al total de ingresos / gastos
output[, NEG_TRAN_FIRST_MONTH_PHASE_VS_TOT := SUM_NEG_TRAN_FIRST_MONTH_PHASE / SUM_NEG_TRAN]
output[, NEG_TRAN_MIDDLE_MONTH_PHASE_VS_TOT := SUM_NEG_TRAN_MIDDLE_MONTH_PHASE / SUM_NEG_TRAN]
output[, NEG_TRAN_LAST_MONTH_PHASE_VS_TOT := SUM_NEG_TRAN_LAST_MONTH_PHASE / SUM_NEG_TRAN]
output[, POS_TRAN_FIRST_MONTH_PHASE_VS_TOT := SUM_POS_TRAN_FIRST_MONTH_PHASE / SUM_POS_TRAN]
output[, POS_TRAN_MIDDLE_MONTH_PHASE_VS_TOT := SUM_POS_TRAN_MIDDLE_MONTH_PHASE / SUM_POS_TRAN]
output[, POS_TRAN_LAST_MONTH_PHASE_VS_TOT := SUM_POS_TRAN_LAST_MONTH_PHASE / SUM_POS_TRAN]


# Detectando las categorias mas comunes
manejoEfectivo <- unique(dt.transactionsExpanded[, CONCEPTO]) %gv% "EFECTIVO|RETIRO|RET\\. EF|AVANCE TARJET"
intereses <- unique(dt.transactionsExpanded[, CONCEPTO]) %gv% "INTERESES|LIQUID\\. INT|PAGO RENDIM|DEBITO INTER"
impuestos <- unique(dt.transactionsExpanded[, CONCEPTO]) %gv% "IMPUESTO|IVA|RECAUDO IMP|RECAUDO NAC|IMPTO|EMBARG|IMP\\.|\\. GMF|PLANILLA|CONTRIBUC|RETENCION|REC-PLAN"
comisiones <- unique(dt.transactionsExpanded[, CONCEPTO]) %gv% "USO CAJERO|PORTES TELE|TIMBRES CHEQ|MANEJO TAR|Gestion y Contac|Extracto FAX|COSTO PIN|COSTO CONS|REEXPED|ADMON PDTE|COBRO DE CHEQ|COBRO DE TAL|COBRO TELEX|COM\\.|COMISION|COMIS|COM-"
transferencia <- unique(dt.transactionsExpanded[, CONCEPTO]) %gv% "TRANSF"
nomina <- unique(dt.transactionsExpanded[, CONCEPTO]) %gv% "NOMINA"

# Creando variable resumen por categorias de concepto de transacciones
dt.transactionsExpanded[CONCEPTO %in% comisiones, CONCEPTO_CAT := "COMISIONES"]
dt.transactionsExpanded[is.na(CONCEPTO_CAT) & CONCEPTO %in% manejoEfectivo, CONCEPTO_CAT := "MANEJO_EFECTIVO"]
dt.transactionsExpanded[is.na(CONCEPTO_CAT) & CONCEPTO %in% intereses, CONCEPTO_CAT := "INTERESES"]
dt.transactionsExpanded[is.na(CONCEPTO_CAT) & CONCEPTO %in% impuestos, CONCEPTO_CAT := "IMPUESTOS"]
dt.transactionsExpanded[is.na(CONCEPTO_CAT) & CONCEPTO %in% transferencia, CONCEPTO_CAT := "TRANSFERENCIAS"]
dt.transactionsExpanded[is.na(CONCEPTO_CAT) & CONCEPTO %in% nomina, CONCEPTO_CAT := "NOMINA"]
dt.transactionsExpanded[is.na(CONCEPTO_CAT), CONCEPTO_CAT := "OTROS"]

# Calculo de monto neto por categoria de transaccion
dcast_net_amount_per_type <- dcast.data.table(dt.transactionsExpanded, ID_CLIENTE + YEAR_MONTH ~ paste0(CONCEPTO_CAT, "_NET_AMOUNT"),
                                              value.var = "VALOR_TX",
                                              fun.aggregate = sum)

output <- merge(output, dcast_net_amount_per_type, by = c("ID_CLIENTE", "YEAR_MONTH"), all.x = T)

# Calculo de numero de transacciones por categoria
dcast_tx_per_type <- dcast.data.table(dt.transactionsExpanded, ID_CLIENTE + YEAR_MONTH ~ paste0(CONCEPTO_CAT, "_NUM_TX"),
                                      value.var = "YEAR_MONTH",
                                      fun.aggregate = length)

output <- merge(output, dcast_tx_per_type, by = c("ID_CLIENTE", "YEAR_MONTH"), all.x = T)

rm(dt.transactionsExpanded)
gc()

colsToExpand <- c("NT_POS",
                  "NT_NEG",
                  "SUM_POS_TRAN",
                  "SUM_NEG_TRAN",
                  "NET_AMNT_TRANS",
                  "NOMINA_NET_AMOUNT")

colsToRatio <- c("NT_POS",
                 "NT_NEG",
                 "SUM_POS_TRAN",
                 "SUM_NEG_TRAN",
                 "NET_AMNT_TRANS",
                 "NOMINA_NET_AMOUNT")

expandMethods <- c("mean", "max", "sd", "sum")

# Calculamos variables estadisticas para los ultimos meses siempre incluyendo el periodo de analisis
stats <- expandByPeriods(output,
                       colsToExpand = colsToExpand,
                       nPeriods = seq(2, maxMonthsBack + 1),
                       timeSeriesIDcolNames = "ID_CLIENTE",
                       periodColName = "YEAR_MONTH",
                       expandMethods = expandMethods,
                       includeCurrentPeriod = TRUE,
                       doGrid = TRUE,
                       doSort = TRUE,
                       verbose = TRUE)

# Calculamos los ratios de las variables de los meses previos respecto a su valor en el periodo de analisis
ratios <- expandByPeriods(output,
                       colsToExpand,
                       nPeriods = seq(1, maxMonthsBack),
                       timeSeriesIDcolNames = "ID_CLIENTE",
                       periodColName = "YEAR_MONTH",
                       expandMethods = "shift",
                       colsToRatio = colsToRatio,
                       includeCurrentPeriod = FALSE,
                       doGrid = TRUE,
                       doSort = TRUE,
                       verbose = TRUE)


# Nos quedamos con el periodo analizado
dt.transactionsExpandedF <- stats[YEAR_MONTH %in% period]
dt.transactionsExpandedF <- merge(dt.transactionsExpandedF,
                                  ratios[, c("ID_CLIENTE", "YEAR_MONTH", names(ratios) %gv% "Ratio"), with = F],
                                  by = c("ID_CLIENTE", "YEAR_MONTH"),
                                  all.x = T)
expandedCols <- setdiff(names(dt.transactionsExpandedF), names(output))
rm(output)
gc()
# Reemplazamos los "-Inf" por NA 
for(var in expandedCols){
  cat("Limpiando infinitos de la variable", var, fill = TRUE)
  set(x = dt.transactionsExpandedF,
      j = var,
      i = dt.transactionsExpandedF[, which(is.nan(get(var)) | is.infinite(get(var)))],
      value = NA)
  
}

print(paste0("Saving transactions for period...",  period))
saveProcessedTableByPeriod(dt.transactionsExpandedF,
                           path = txPasivoExpandedPath,
                           source = "expandedTxPasivo",
                           periodIDvarname = "YEAR_MONTH")
rm(dt.transactionsExpandedF)
gc()
}
