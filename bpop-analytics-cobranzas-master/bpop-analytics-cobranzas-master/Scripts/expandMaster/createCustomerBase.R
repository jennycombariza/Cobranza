#' createCustomerBase
#' 
#' @description Funcion que identifica la base de obligaciones/clientes que
#' entran en mora en en el periodo analizado
#' 
#' @param period dia en formato "YYYYMMdd"
#' @return Un identificador unico de entrada en mora para cada fecha analizada
#' @examples
#' createCustomerBase("20170801")

createCustomerBase <- function(period){
cat("Creating customer base for period", period, fill = TRUE)
# Seleccion de archivos procesados de mora guardados en 1.Data/PreparedData
# con la etiqueta "processedMora"
# Ademas del periodo que pasamos en la ejecucion cargaremos los 5 
# periodos anteriores para asegurar que tenemos el dia anterior evitando fines
# de semana y festivos

periodsToSelect <- c(0:-5) 
periodsToSelect <- dayAddInteger(period, periodsToSelect)

# Cargamos los datos utilizando la funcion generica getProcessedTableByPeriod

moraDt <- getProcessedTableByPeriod(dataSource = "processedMora",
                                    path = moraProcessedPath,
                                    periods = periodsToSelect,
                                    colClasses = "character",
                                    na.strings = c("", NA))

# Eliminamos campos con ID_CONTRATO missing
moraDt <- moraDt[!is.na(ID_CONTRATO)]

# Selecionamos solo los productos y las variables relevantes para el proceso
# Los productos a seleccionar se pueden encontrar en el script constants.R

moraDt <- moraDt[LINEA %in% productsToIncludeMora, .(ID_CONTRATO,
                                                     YEAR_MONTH_DAY,
                                                     DIAS,
                                                     LINEA)]
# Hacemos el shift de la columna DIAS para tener el dato de dias de mora
# en la misma linea
moraDt[, DIAS := as.numeric(DIAS)]
moraExp <- expandByPeriods(moraDt,
                           colsToExpand = c("DIAS"),
                           nPeriods = 1,
                           timeSeriesIDcolNames = "ID_CONTRATO",
                           periodColName = "YEAR_MONTH_DAY",
                           expandMethods = "shift",
                           includeCurrentPeriod = TRUE,
                           colsToRatio = c(),
                           doGrid = TRUE,
                           doSort = TRUE,
                           verbose = TRUE)

# Reemplazamos los valores missing por 0 dado que suponemos que un contrato que 
# no aparecia el dia anterior y para ese dia si existen datos de contratos morosos
# debia estar a 0 dias de mora
moraExp[is.na(DIAS), DIAS := 0]
moraExp[is.na(DIAS_p1), DIAS_p1 := 0]
# Calculamos la diferencia de los dias en mora con respecto al dia anterior
moraExp[, diffDias := DIAS_p1 - DIAS]
# Seleccionamos las obligaciones que formaran parte de la customer base:

# (1) Obligaciones que tenian mas dias en mora el dia anterior y que hoy tengan
# entre 1 y 6 dias y que la diferencia sea mayor a 4. Ademas el numero de dias
# en la fecha anterior tiene que ser inferior a 40 o esta a missing
moraExp[,base1 := NULL]
moraExp[(DIAS_p1 > DIAS & (DIAS > 0 & DIAS <= 6) & diffDias > 4), base1 := 1]

# (2) Obligaciones que el dia anterior no estan informadas o que tienen el 
# numero de dias en mora a 0 y que hoy tienen mas de 0 dias en mora.

moraExp[((is.na(DIAS_p1) | DIAS_p1 == 0) & DIAS > 0), base1 := 1]

# (3) Excluimos aquellos casos que entren con mas de 7 dias de mora
moraExp[DIAS >= 7, base1 := 0]

base <- moraExp[base1 == 1 & YEAR_MONTH_DAY == period, .(ID_CONTRATO, YEAR_MONTH_DAY, DIAS_MORA_PREVIO_ENTRADA = DIAS_p1)]

saveProcessedTableByPeriod(base,
                           path = customerBaseExpandedPath,
                           source = "caseBase" ,
                           periodIDvarname = "YEAR_MONTH_DAY")
}
