rm(list=ls())
gc()

args = commandArgs(trailingOnly=TRUE)

modelType <<- "" 
rootPath <<- "/srv/ocf115/"
# test if there is at least two argument: if not, return an error
if (length(args)!=2 & length(args)!=6) {
  stop("You need to provide two or six arguments: Model_Type and Root_Path.n", call.=FALSE)
} else{
  #### select Model Type from Config File
  modelType = args[1]
  # Provide rootPath
  rootPath = args[2]
  if (length(args)==6){
    #### select Model Type from Config File
    trainPeriods_Begin = args[3]
    # Provide rootPath
    trainPeriods_End = args[4]
    testPeriods_Begin = args[5]
    testPeriods_End = args[6]
  }
}

# Load utility funcions
source(paste0(rootPath,"4.Models/genericUtilities.R"))
# Load utility funcions for s3 file management
source(paste0(rootPath,"4.Models/genericUtilitiesS3.R"))

setWorkspace(rootPath = rootPath,
             modelType = "")
librariesRequired <- setdiff(librariesRequired, "mlr")
loadLibraries(librariesRequired)

# Read configuratin for storage devices
auxJsonStorage <- jsonlite::read_json(paste0(rootPath,"4.Models/configStorage.json"))
awsS3BucketID <- auxJsonStorage[["S3_Configuration"]][["AWS_BUCKET_ID"]]
awsCliPath <- auxJsonStorage[["S3_Configuration"]][["AWS_CLI_PATH"]]
ebsRootPath <- auxJsonStorage[["S3_Configuration"]][["EBS_ROOT_PATH"]]
configure_s3(aws_cli_path = awsCliPath,
             aws_S3_Bucket_ID = awsS3BucketID, 
             localEBSPath = ebsRootPath)
cat("My assinged temp_path is: ", tempPath, fill = T)

# Cargamos las funciones de proyecto especificas
purrr::walk(.x = list.files(paste0(rootPath,"4.Models/Scripts"), full.names = T, recursive = T), .f = source)

dailyPeriodsToProcess <- seqDays("20171001", "20171005")
monthlyPeriodsToProcess <- monthAddInteger(unique(str_sub(dailyPeriodsToProcess, 1,6)), -1)

# (1) Procesamos las tablas de caracter diario
for (period in dailyPeriodsToProcess){
  process_mora(period)
  createCustomerBase(period)
}
# (1) Procesamos las tablas de caracter mensual

for (period in monthlyPeriodsToProcess){
  process_obligaciones(period)
}

# (2) Creamos las variables sinteticas para tablas de caracter diario + target
for (period in dailyPeriodsToProcess){
  tryCatch(createTargetMaster(period), error = function(err){})
  tryCatch(createMoraMaster(period), error = function(err){})
}

# (2) Creamos las variables sinteticas para las tablas de caracter mensual
for (period in monthlyPeriodsToProcess){
  createObligacionesMaster(period)
}

# (4) Creamos la master table 
for (period in periods){
  tryCatch(createMasterTable(period), error = function(err){})
}

################# Cargamos el dataset a utilizar en la modelizacion #################
dailyPeriodsForModelling <- seqDays("20170401", "20171231")

# (5) Apilamos las distintas Master Tables diarias para crear el dataset
masterDt <- createDataset(dailyPeriodsForModelling)

# Seleccionamos variables que no van a entrar al modelo
# Estas variables o bien son la target, o pueden contener informacion de futuro
varsTarget <- names(masterDt)[str_detect(names(masterDt), "TARGET")]

varsToRemove <- unique(c(varsTarget,
                         "YEAR_MONTH",
                         "ID_CLIENTE",
                         "ID_CONTRATO",
                         "YEAR_MONTH_DAY",
                         "MAX_DIAS_MORA",
                         "TARGET_BINARY"))

firstDayTest <- c("20171101")

# Seleccionamos periodos de train y test
trainingPeriod <- unique(masterDt[!YEAR_MONTH_DAY %in% seqDays(firstDayTest, max(masterDt[, YEAR_MONTH_DAY])), YEAR_MONTH_DAY])
testingPeriod <- unique(masterDt[YEAR_MONTH_DAY %in% seqDays(firstDayTest, max(masterDt[, YEAR_MONTH_DAY])), YEAR_MONTH_DAY])

# Seleccionamos los hiperparametros del modelo
# Los hiperparametros optimos se seleccionan mediante validacion cruzada
# con el set de development
parameters <-  list(booster           = "gbtree",
                    objective         = "binary:logistic",
                    eval_metric       = "auc",
                    tree_method       = "exact",
                    max_depth         = 4,
                    gamma             = 8,
                    colsample_by_tree = 0.9,
                    subsample         = 0.9)

# Ejecutamos el modelo con los datos anteriores
test <- trainModel(dataset = masterDt,
                   modelName = "prueba_training",
                   varsToRemove = varsToRemove,
                   outputName = outputsPath,
                   target = "TARGET_BINARY",
                   trainPeriods = trainingPeriod,
                   testPeriods = testingPeriod,
                   daysForDevelopment = 15,
                   productIDColName = "ID_CONTRATO",
                   clientIDColName = "ID_CLIENTE",
                   periodColName = "YEAR_MONTH_DAY",
                   parametersConfig = parameters,
                   nRounds = 100,
                   doUndersampling = F)
