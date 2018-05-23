args = commandArgs(trailingOnly=TRUE)

source("/home/mck/bpop-analytics-cobranzas/configuration/configEnv.R")
configEnvironment()
auxJson <- jsonlite::read_json(paste0(rootPath,"configuration/configJson.json"))

predictPeriodo <<- "20180207"
# Comprobar que se haya recibido al menos, un argumento. En caso contrario, devolver un error
if (length(args)!=1) {
  stop("Es necesario el argumento: Periodo a predecir", call.=FALSE)
} else {
  predictPeriodo = args[1]
}

# Leer funciones a ejecutar del JSON de configuracion
processFunctionsDaily <- auxJson[["DataPreparationConfig"]][["processFunctionsDaily"]]
processFunctionsMonthly <- auxJson[["DataPreparationConfig"]][["processFunctionsMonthly"]]
expandFunctionsDaily <- auxJson[["DataPreparationConfig"]][["expandFunctionsDaily"]] %>% unlist
expandFunctionsMonthly <- auxJson[["DataPreparationConfig"]][["expandFunctionsMonthly"]] %>% unlist
expandFunctionsDaily <- setdiff(expandFunctionsDaily, "createTargetMaster")

# TBD - Validar existencia de archivos ------------------------------------

dailyPeriodToExpand <- predictPeriodo

dailyPeriodsToProcess <- seqDays(dayAddInteger(min(dailyPeriodToExpand), -120), dailyPeriodToExpand)

monthlyPeriodsToProcess <- unique(str_sub(seqMonth(dayAddInteger(min(dailyPeriodToExpand), -120),
                                                   dayAddInteger(max(dailyPeriodToExpand), -30)), 1, 6))
monthlyPeriodsToExpand <- c(unique(monthAddInteger(str_sub(dailyPeriodToExpand, 1, 6), -2)),
                            unique(monthAddInteger(str_sub(dailyPeriodToExpand, 1, 6), -1)))


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
    cat("Ejecutando funcion de proceso de datos:", auxFunction, fill = TRUE)
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
  nonGeneratedPeriods <- returnNonExistentFilesS3(fun = auxFunction, periods = dailyPeriodToExpand, json = auxJson)
  for(auxPeriodo in nonGeneratedPeriods){
    tryCatch(do.call(eval(auxFunction), list(period = auxPeriodo)),
             error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  }
}
cat("Finalizando creacion de variables sinteticas", fill = TRUE)

cat("Creando dataset", fill = TRUE)
dataset <- createDataset(periodsToSelect = predictPeriodo)

# Save dataset, in case there was a dataset already in the folder, move old dataset 
# to control version folder, with the date of creation of the dataset
datasetFile <- osPathJoin(datasetPreparedPath, "Dataset_predict.csv")
if(file.exists_s3(datasetFile) == TRUE){
  auxDataset <- fread_s3(datasetFile)
  timeStamp <- file.info_s3(datasetFile)
  fwrite_s3(auxDataset, osPathJoin(oldDatasetPreparedPath, paste0("Dataset_predict_", timeStamp, ".csv")))
}
fwrite_s3(dataset, datasetFile)


# Entrenamos Modelo
# Seleccionamos variables que no van a entrar al modelo
# Estas variables o bien son la target, o pueden contener informacion de futuro
varsTarget <- names(dataset)[str_detect(names(dataset), "TARG")]

# Seleccionando columnas para no incluir en la modelizacion
varsToRemove <- unique(c(varsTarget,
                         "YEAR_MONTH",
                         "ID_CLIENTE",
                         "ID_CONTRATO",
                         "YEAR_MONTH_DAY",
                         colnames(dataset)[dataset[, lapply(.SD, class)]== "character"]))

# Ejecutamos el modelo con los datos anteriores
predictModel(dataset = dataset, 
             modelName = "Collections_vPrueba",
             varsToRemove = varsToRemove, 
             productIDColName = "ID_CONTRATO",
             clientIDColName = "ID_CLIENTE",
             periodColName = "YEAR_MONTH_DAY",
             periodo = predictPeriodo)

# TBD - Output validation -------------------------------------------------


# TBD - Model Performance para el nuevo mes -------------------------------


