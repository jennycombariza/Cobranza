args = commandArgs(trailingOnly=TRUE)

source("/home/mck/bpop-analytics-cobranzas/configuration/configEnv.R")
configEnvironment()
auxJson <- jsonlite::read_json(paste0(rootPath,"configuration/configJson.json"))

beginPeriodoTrain = "20170511"
endPeriodoTrain = "20170511"
beginPeriodoTest = "20170513"
endPeriodoTest = "20170514"

# # Comprobar que se haya recibido al menos, un argumento. En caso contrario, devolver un error
if (length(args)!=4) {
  stop("Es necesario los argumentos: InicioPeriodoTrain FinalPeriodoTrain InicioPeriodoTest FinalPeriodoTest", call.=FALSE)
} else{
  beginPeriodoTrain = args[1]
  endPeriodoTrain = args[2]
  beginPeriodoTest = args[3]
  endPeriodoTest = args[4]
}

# Leer funciones a ejecutar del JSON de configuracion
processFunctionsDaily <- auxJson[["DataPreparationConfig"]][["processFunctionsDaily"]]
processFunctionsMonthly <- auxJson[["DataPreparationConfig"]][["processFunctionsMonthly"]]
expandFunctionsDaily <- auxJson[["DataPreparationConfig"]][["expandFunctionsDaily"]] %>% unlist
expandFunctionsMonthly <- auxJson[["DataPreparationConfig"]][["expandFunctionsMonthly"]] %>% unlist

# TBD - Validar existencia de archivos ------------------------------------
trainingPeriod <- seqDays(beginPeriodoTrain, endPeriodoTrain)
testingPeriod <- seqDays(beginPeriodoTest, endPeriodoTest)

dailyPeriodsToExpand <- c(trainingPeriod, testingPeriod)

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
  nonGeneratedPeriods <- returnNonExistentFilesS3(fun = auxFunction, periods = dailyPeriodsToExpand, json = auxJson)
  for(auxPeriodo in nonGeneratedPeriods){
    tryCatch(do.call(eval(auxFunction), list(period = auxPeriodo)),
             error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  }
}
cat("Finalizando creacion de variables sinteticas", fill = TRUE)

cat("Creando dataset", fill = TRUE)
dataset <- createDataset(periodsToSelect = seqDays("20170511", "20180228"))

# Save dataset, in case there was a dataset already in the folder, move old dataset
# to control version folder, with the date of creation of the dataset

datasetFile <- osPathJoin(datasetPreparedPath, "Dataset_train.csv")
if(file.exists_s3(datasetFile) == TRUE){
  auxDataset <- fread_s3(datasetFile)
  timeStamp <- file.info_s3(datasetFile)
  fwrite_s3(auxDataset, osPathJoin(oldDatasetPreparedPath,
                                   paste0("Dataset_train_", timeStamp, ".csv")))
}
fwrite_s3(dataset, datasetFile)


# TBD - Input Validation --------------------------------------------------


# Entrenamos Modelo
# Seleccionamos variables que no van a entrar al modelo
# Estas variables o bien son la target, o pueden contener informacion de futuro
varsTarget <- names(dataset)[str_detect(names(dataset), "_TARG")]

#Coercionando las fechas a formato apropiado
dataset[, (fechasMasterTable) := lapply(.SD, ymd), .SDcols = fechasMasterTable]

# Seleccionando columnas para no incluir en la modelizacion
varsToRemove <- unique(c(varsTarget,
                         "YEAR_MONTH",
                         "ID_CLIENTE",
                         "ID_CONTRATO",
                         "YEAR_MONTH_DAY",
                         # names(dataset)[str_detect(names(dataset), "RIESGO_")],
                         # names(dataset)[str_detect(names(dataset), "_BURO")],
                         colnames(dataset)[dataset[, lapply(.SD, class)]== "character"]))


parameters <- auxJson[["Modelling"]][['parameters']]

# Ejecutamos el modelo con los datos anteriores
test <- trainModel(dataset = dataset,
                   modelName = "Collections_vPrueba",
                   varsToRemove = varsToRemove,
                   outputName = outputsPath,
                   target = "TARGET_BINARY_TARG",
                   trainPeriods = seqDays("20170511", "20171231"),
                   testPeriods = seqDays("20180101", "20180221"),
                   daysForDevelopment = 30,
                   productIDColName = "ID_CONTRATO",
                   clientIDColName = "ID_CLIENTE",
                   periodColName = "YEAR_MONTH_DAY",
                   parametersConfig = parameters,
                   nRounds = 5,
                   doUndersampling = F)


dataset = dataset
modelName = "Collections_vPrueba"
varsToRemove = varsToRemove
outputName = outputsPath
target = "TARGET_BINARY_TARG"
trainPeriods = seqDays("20170511", "20171231")
testPeriods = seqDays("20180101", "20180221")
daysForDevelopment = 30
productIDColName = "ID_CONTRATO"
clientIDColName = "ID_CLIENTE"
periodColName = "YEAR_MONTH_DAY"
parametersConfig = parameters
nRounds = 5
doUndersampling = F
