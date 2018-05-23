rm(list=ls())
gc()
source("/Drive/_Projects/201802_ZZZ000_GrupoAval_ABIERTO/generic_scripts/generic_utilities.R")
setWorkspace(rootPath = "/Drive/_Projects/201802_ZZZ000_GrupoAval_ABIERTO/",
             modelType = "BOCC")
setwd(analysisPath)
librariesRequired <- setdiff(librariesRequired, "mlr")
loadLibraries(librariesRequired)

# Cargamos las funciones de proyecto especificas
source("/Drive/_Projects/201802_ZZZ000_GrupoAval_ABIERTO/Training/processData/process_obligaciones.R")
source("/Drive/_Projects/201802_ZZZ000_GrupoAval_ABIERTO/Training/processData/process_Mora.R")
source("/Drive/_Projects/201802_ZZZ000_GrupoAval_ABIERTO/Training/createMaster/createCustomerBase.R")
source("/Drive/_Projects/201802_ZZZ000_GrupoAval_ABIERTO/Training/createMaster/createTargetMaster.R")
source("/Drive/_Projects/201802_ZZZ000_GrupoAval_ABIERTO/Training/createMaster/createMoraMaster.R")
source("/Drive/_Projects/201802_ZZZ000_GrupoAval_ABIERTO/Training/createMaster/createObligacionesMaster.R")
source("/Drive/_Projects/201802_ZZZ000_GrupoAval_ABIERTO/Training/createMaster/createMasterTable_v2.R")
source("/Drive/_Projects/201802_ZZZ000_GrupoAval_ABIERTO/Training/createMaster/createDataset.R")
source("/Drive/_Projects/201802_ZZZ000_GrupoAval_ABIERTO/Training/modellingFunctions/trainModelV2.R")
source("/Drive/_Projects/201802_ZZZ000_GrupoAval_ABIERTO/BOCC/4.Models/Scripts/constants.R")



dailyPeriodsToProcess <- seqDays("20171001", "20171231")
monthlyPeriodsToProcess <- monthAddInteger(201612, c(0:12))

# (1) Procesamos las tablas de caracter diario
for (period in dailyPeriodsToProcess){
  # process_mora(period)
  # createCustomerBase(period)
  # tryCatch(createMoraMaster(period), error = function(err){})
  tryCatch(createExposicionMaster(period), error = function(err){})
}
# (1) Procesamos las tablas de caracter mensual

for (period in monthlyPeriodsToProcess){
  # process_obligaciones(period)
  process_garantias(period)
}

# (2) Creamos las variables sinteticas para tablas de caracter diario + target
for (period in dailyPeriodsToProcess){
  # tryCatch(createTargetMaster(period), error = function(err){})
  # tryCatch(createMoraMaster(period), error = function(err){})
}

# (2) Creamos las variables sinteticas para las tablas de caracter mensual
for (period in monthlyPeriodsToProcess){
  # createObligacionesMaster(period)
  tryCatch(createGarantiasMaster(period), error = function(err){})
}

# (4) Creamos la master table 
for (period in dailyPeriodsToProcess){
  tryCatch(createMasterTable(period), error = function(err){})
}

library(doParallel)
cl <- parallel::makePSOCKcluster(12)
registerDoParallel(cl)

stopCluster(cl)
closeAllConnections()

foreach(i = 1:length(dailyPeriodsToProcess), .packages = librariesRequired) %dopar% {
  # createMasterTable(periodoDia = dailyPeriodsToProcess[i])
  tryCatch(createExposicionMaster(periodoDia = dailyPeriodsToProcess[i]),
           error = function(err){})
  # tryCatch(createMasterTable(periodoDia = dailyPeriodsToProcess[i]),
  #          error = function(err){})
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

firstDayTest <- c("20171001")

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
                    max_depth         = 8,
                    gamma             = 2,
                    colsample_by_tree = 0.5,
                    subsample         = 0.2)

# Ejecutamos el modelo con los datos anteriores
test <- trainModel(dataset = masterDt,
                   modelName = "Modelo_Binary_v11",
                   varsToRemove = varsToRemove,
                   outputName = outputsPath,
                   target = "TARGET_BINARY",
                   trainPeriods = trainingPeriod,
                   testPeriods = testingPeriod,
                   daysForDevelopment = 30,
                   productIDColName = "ID_CONTRATO",
                   clientIDColName = "ID_CLIENTE",
                   periodColName = "YEAR_MONTH_DAY",
                   parametersConfig = parameters,
                   nRounds = 40,
                   doUndersampling = F)
