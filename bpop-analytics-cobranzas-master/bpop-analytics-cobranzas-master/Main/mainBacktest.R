args = commandArgs(trailingOnly=TRUE)

source("/home/mck/bpop-analytics-cobranzas/configuration/configEnv.R")
configEnvironment()
auxJson <- jsonlite::read_json(paste0(rootPath,"configuration/configJson.json"))

inicioBacktest <<- "20180109"
finBacktest <<- "20180109"

# Comprobar que se haya recibido al menos, un argumento. En caso contrario, devolver un error
if (length(args)!=2) {
  stop("Es necesario el argumento: Periodo a predecir", call.=FALSE)
} else {
  inicioBacktest = args[1]
  finBacktest = args[2]
}

# Leer funciones a ejecutar del JSON de configuracion
processFunctionsDaily <- auxJson[["DataPreparationConfig"]][["processFunctionsDaily"]]
processFunctionsMonthly <- auxJson[["DataPreparationConfig"]][["processFunctionsMonthly"]]
expandFunctionsDaily <- auxJson[["DataPreparationConfig"]][["expandFunctionsDaily"]] %>% unlist
expandFunctionsMonthly <- auxJson[["DataPreparationConfig"]][["expandFunctionsMonthly"]] %>% unlist

# TBD - Validar existencia de archivos ------------------------------------


dailyPeriodsToExpand <- seqDays(inicioBacktest, finBacktest)

dailyPeriodsToProcess <- seqDays(dayAddInteger(min(dailyPeriodsToExpand), -120),
                                 dayAddInteger(max(dailyPeriodsToExpand), 40))

monthlyPeriodsToProcess <- unique(str_sub(seqMonth(dayAddInteger(min(dailyPeriodsToExpand), -120),
                                                   dayAddInteger(max(dailyPeriodsToExpand), -30)), 1, 6))
monthlyPeriodsToExpand <- c(unique(monthAddInteger(str_sub(dailyPeriodsToExpand, 1, 6), -2)),
                            unique(monthAddInteger(str_sub(dailyPeriodsToExpand, 1, 6), -1)))


# Procesando tablas -------------------------------------------------

# Ejecutar los scripts de procesamiento de datos originales

for(auxFunction in processFunctionsMonthly){
  cat("Ejecutando funcion de proceso de datos:", auxFunction, fill = TRUE)
  nonGeneratedPeriods <- returnNonExistentFilesS3(fun = auxFunction, periods = monthlyPeriodsToProcess, json = auxJson)
  for(auxPeriodo in nonGeneratedPeriods){
    tryCatch(do.call(eval(auxFunction), list(period = auxPeriodo)),
             error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  }
}
for(auxFunction in processFunctionsDaily){
  cat("Ejecutando funcion de proceso de datos:", auxFunction, fill = TRUE)
  nonGeneratedPeriods <- returnNonExistentFilesS3(fun = auxFunction, periods = dailyPeriodsToProcess, json = auxJson)
  for(auxPeriodo in nonGeneratedPeriods){
    tryCatch(do.call(eval(auxFunction), list(period = auxPeriodo)),
             error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  }
}
cat("Finalizando procesamiento de datos", fill = TRUE)

# Expand Functions --------------------------------------------------
# Ejecutar los scripts de creacion de variable sinteticas

for(auxFunction in expandFunctionsMonthly){
  cat("Ejecutando funcion de creacion de sinteticas:", auxFunction, fill = TRUE)
  nonGeneratedPeriods <- returnNonExistentFilesS3(fun = auxFunction, periods = monthlyPeriodsToExpand, json = auxJson)
  for(auxPeriodo in nonGeneratedPeriods){
    tryCatch(do.call(eval(auxFunction), list(period = auxPeriodo)),
             error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  }
}
for(auxFunction in expandFunctionsDaily){
  cat("Ejecutando funcion de creacion de sinteticas:", auxFunction, fill = TRUE)
  nonGeneratedPeriods <- returnNonExistentFilesS3(fun = auxFunction, periods = dailyPeriodsToExpand, json = auxJson)
  for(auxPeriodo in nonGeneratedPeriods){
    tryCatch(do.call(eval(auxFunction), list(period = auxPeriodo)),
             error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  }
}



cat("Finalizando creacion de variables sinteticas", fill = TRUE)

cat("Creando dataset", fill = TRUE)
dataset <- createDataset(periodsToSelect = seq(inicioBacktest, finBacktest))


# Seleccionamos variables que no van a entrar al modelo
# Estas variables o bien son la target, o pueden contener informacion de futuro
varsTarget <- names(dataset)[str_detect(names(dataset), "TARG")]

# Seleccionando columnas para no incluir en el backtest (tiene que ser consistente con el train)
varsToRemove <- unique(c(varsTarget,
                         "YEAR_MONTH",
                         "ID_CLIENTE",
                         "ID_CONTRATO",
                         "YEAR_MONTH_DAY",
                         colnames(dataset)[dataset[, lapply(.SD, class)]== "character"]))

# Ejecutamos el backtest con los datos anteriores
backtestModel(dataset = dataset, 
             modelName = "Collections_vPrueba",
             varsToRemove = varsToRemove, 
             productIDColName = "ID_CONTRATO",
             clientIDColName = "ID_CLIENTE",
             target = "TARGET_BINARY_TARG",
             periodColName = "YEAR_MONTH_DAY",
             inicioBacktest = inicioBacktest,
             finBacktest = finBacktest)

