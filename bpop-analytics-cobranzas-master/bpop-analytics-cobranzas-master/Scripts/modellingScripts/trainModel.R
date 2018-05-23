#' saveModel
#' 
#' @description Funcion de guardado automatico del modelo entrenado de XGBoost
#' en el almacenamiento S3. Esta funcion mueve cualquier modelo anterior a un
#' repositorio para asegurar la trazabilidad de los resultados.
#' 
#' @param modelXGBoostBinary Modelo entrenado de XGBoost a ser guardado
#' @param modelName Nombre del modelo a guardar
#' @return Modelo de XGBoost pre-entrenado guardado en S3
#' @examples
#' saveModel(modelXGBoostBinary = modeloEntrenado, modelName = "Cobranzas")

saveModel <- function(modelXGBoostBinary, modelName){
  modelFile <- osPathJoin(modelsPath, paste0("ModelBinary_", modelName, ".RDS"))
  if(file.exists_s3(modelFile) == TRUE){
    cat("Read RDS modelFile", fill = TRUE)
    auxModel <- readRDS_s3(modelFile)
    timeStamp <- str_split(file.info_s3(modelFile), " ")
    timeStamp <- paste0(timeStamp[1])
    cat("saveRDS RDS", modelName, fill = TRUE)
    saveRDS_s3(auxModel, 
               osPathJoin(oldModelsPath, 
                          paste0("ModelBinary_", modelName,"_", timeStamp, ".RDS")))
  }
  cat("saveRDS RDS", modelName, fill = TRUE)
  saveRDS_s3(modelXGBoostBinary,
             osPathJoin(modelsPath,
                        paste0("ModelBinary_", modelName, ".RDS")))
}

#' saveVariables
#' 
#' @description Funcion de guardado automatico en el almacenamiento S3 de las variables 
#' utilizadas para entrenar el modelo. Esta funcion mueve las variables anteriores a un
#' repositorio para asegurar la trazabilidad de los resultados.
#' 
#' @param dataset Dataset utilizado para entrenar el modelo
#' @param varsToRemove Variables a ser eliminadas del dataset
#' @param modelName Nombre del modelo a guardar
#' @return Vector del nombre de variables utilizado para entrenar el modelo guardado en S3
#' @examples
#' saveVariables(dataset = dataset, varsToRemove = varsToRemove, modelName = "Cobranzas")
#' 
saveVariables <- function(dataset, varsToRemove, modelName){
  variablesFile <- osPathJoin(modelsPath, paste0("variablesModeloXGBoost_", modelName, ".RDS"))
  if(file.exists_s3(variablesFile) == TRUE){
    cat("readRDS RDS", variablesFile, fill = TRUE)
    auxVariables <- readRDS_s3(variablesFile)
    timeStamp <- str_split(file.info_s3(variablesFile), " ")
    timeStamp <- paste0(timeStamp[1])
    cat("saveRDS RDS variables", modelName, fill = TRUE)
    saveRDS_s3(auxVariables,
               osPathJoin(oldVariablesPath,
                          paste0("variablesModeloXGBoost_", modelName,"_", timeStamp, ".RDS")))
  }
  
  cat("saveRDS RDS variables", modelName, fill = TRUE)
  saveRDS_s3(names(dataset[, -varsToRemove, with = FALSE]),
             osPathJoin(variablesPath,
                        paste0("variablesModeloXGBoost_", modelName,".RDS")))
  
}

#' saveMostImportantVariables
#' 
#' @description Funcion de guardado automatico en el almacenamiento S3 de la importancia
#' de variables al entrenar el modelo.
#' 
#' @param xtrain Dataset de train utilizado para entrenar el modelo
#' @param varsToRemove Variables a ser eliminadas del dataset
#' @param modelXGBoostBinary Modelo entrenado de XGBoost a ser guardado
#' @param outputFileName Nombre del modelo a guardar
#' @return Tablas con la importancia por variable guardada en S3.
#' @examples
#' saveMostImportantVariables(xtrain = xtrain, varsToRemove = varsToRemove, 
#'                            modelXGBoostBinary = modelXGBoostBinary , outputFileName = "Cobranzas")

saveMostImportantVariables <- function(xtrain, varsToRemove, modelXGBoostBinary, outputFileName){
  # Crear las variables mas importantes del modelo
  OutputVarImp <- xgb.importance(feature_names = names(xtrain[, -varsToRemove, with = FALSE]),
                                 model = modelXGBoostBinary) 
  OutputVarImp[, SOURCE := getSource(Feature), by = Feature]
  OutputVarImp[, SOURCE_GAIN := sum(Gain)/sum(OutputVarImp[, Gain]), by = .(SOURCE)]
  
  setorder(OutputVarImp,-Gain)
  
  cat("Guardando outputVarImp", fill = TRUE)
  fwrite_s3(OutputVarImp, paste0(varImpPath, "/Important_Vars_", outputFileName,".csv"))
  
  return(OutputVarImp)
  
}

savePredBreakdown <- function(model = modelXGBoostBinary, matrix = testMatrix,
                              predXGB, productIDs, outputFileName, path = predBreakdownPath){
  
  pred.breakdown <- as.data.table(predict(model,
                                          matrix,
                                          predcontrib = TRUE))
  
  pred.breakdown[, logit := rowSums(.SD, na.rm = TRUE), .SDcols = names(pred.breakdown)]
  pred.breakdown[, pred := logit.prob(logit)]
  pred.breakdown[, ID_CONTRATO := productIDs]
  # Guardar vision a nivel producto
  fwrite_s3(pred.breakdown, paste0(path, "/predBreakdown_", outputFileName, ".csv"))
}

saveBoundsLogExposurePred <- function(model = modelXGBoostBinary, matrix = devMatrix,
                                      dt = xdev, clientIDColName, periodColName,
                                      outputFileName, target = target){
  # Obtenemos las predicciones para el archivo de development
  predXGB <- predict(model, matrix, missing = c(NA, NaN))

  dt[, predxgb := predXGB]
  # Procesamos la tabla de development para crear las variables necesarias para clasificar a los clientes
  dt[, MAX_PRED := max(predxgb), by = c(clientIDColName, periodColName)]
  dt[, MIN_PRED := min(predxgb), by = c(clientIDColName, periodColName)]
  dt[, MAX_TARGET := max(get(target)), by = c(clientIDColName, periodColName)]
  dt[, SUM_TARGET := sum(get(target)), by = c(clientIDColName, periodColName)]
  dt[, CAPITAL_ENTRA_EN_MORA_CLIENTE := sum(SALDO_CAPITAL_MORA, na.rm = T), by = c(clientIDColName, periodColName)]
  dt[, CAPITAL_ENTRA_EN_MORA_CLIENTE_CONT := sum(SAL_CAPITA_CONT, na.rm = T), by = c(clientIDColName, periodColName)]
  dt[is.na(SAL_CAPITAL_CLIENTE_CONT), SAL_CAPITAL_CLIENTE_CONT := 0]
  dt[, SALDO_TOTAL_CLIENTE := SAL_CAPITAL_CLIENTE_CONT - CAPITAL_ENTRA_EN_MORA_CLIENTE_CONT + CAPITAL_ENTRA_EN_MORA_CLIENTE,
        by = c(clientIDColName, periodColName)]
  
  clientVision <- unique(dt[, c(clientIDColName,
                                   periodColName,
                                   "SALDO_TOTAL_CLIENTE",
                                   "MAX_PRED",
                                   "MIN_PRED",
                                   "MAX_TARGET",
                                   "SUM_TARGET"), with = F],
                         by = c(clientIDColName, periodColName))
  
  clientVision[, LOG_EXPO := log(SALDO_TOTAL_CLIENTE + 1)]
  clientVision[, LOG_EXPO_MAX_PRED := MAX_PRED*LOG_EXPO]
  clientVision[, LOG_EXPO_MIN_PRED := MIN_PRED*LOG_EXPO]
  clientVision[, MIN_RANK_PRED := frank(-MIN_PRED)/.N]
  clientVision[, MAX_RANK_PRED := frank(-MAX_PRED)/.N]
  clientVision[, LOG_EXPO_MAX_PRED_RANK := frank(-LOG_EXPO_MAX_PRED)/.N]
  clientVision[, LOG_EXPO_MIN_PRED_RANK := frank(-LOG_EXPO_MIN_PRED)/.N]
  
  vectorDeciles <- c(-Inf,seq(0.1, 1, by = 0.1))
  clientVision[, riskDecile := cut(LOG_EXPO_MAX_PRED_RANK,
                                   breaks = vectorDeciles,
                                   labels = paste0("DECILE_", vectorDeciles[2:length(vectorDeciles)]*10))]
  clientVision[, ROLLED_CAPITAL := SALDO_TOTAL_CLIENTE * MAX_TARGET]
  riskPerDecile <- clientVision[, .(num = .N,
                                    min_logExpPred = min(LOG_EXPO_MAX_PRED),
                                    max_logExpPred = max(LOG_EXPO_MAX_PRED),
                                    min_pred = min(MAX_PRED),
                                    max_pred = max(MAX_PRED),
                                    min_saldoCapital = min(SALDO_TOTAL_CLIENTE),
                                    max_saldoCapital = max(SALDO_TOTAL_CLIENTE),
                                    sum_rolledSaldoCapital = sum(ROLLED_CAPITAL)), by = .(riskDecile)][order(-riskDecile)]
  
  boundRiskFile <- osPathJoin(boundsLogExposurePredPath,  "riskPerDecileMaster.csv")
  
  if(file.exists_s3(boundRiskFile) == TRUE){
    auxDataset <- fread_s3(boundRiskFile)
    timeStamp <- file.info_s3(boundRiskFile)
    fwrite_s3(riskPerDecile, paste0(oldBoundsLogExposurePredPath,
                                    "/riskPerDecileMaster_", outputFileName, ".csv"))
  }
  
  fwrite_s3(riskPerDecile, boundRiskFile)
  
  return(riskPerDecile)
}
  


saveIDScore <- function(clientVision, productVision, outputFileName){
  # # Generation of the IDs and scores of the validation set
  cat("fwriteS3 IDScore", fill = TRUE)
  # Guardar vision a nivel cliente
  fwrite_s3(clientVision, paste0(trainingClientVisionPath, "/ID_Score_ClientVision_", outputFileName, ".csv"))
  # Guardar vision a nivel producto
  fwrite_s3(productVision, paste0(trainingProductVisionPath, "/ID_Score_ProductVision_", outputFileName, ".csv"))
}



trainModel <- function(dataset, modelName, varsToRemove, outputName, target, trainPeriods, testPeriods, daysForDevelopment = 30,
                       clientIDColName, productIDColName, periodColName, parametersConfig, nRounds, 
                       doUndersampling = F){
  
  # Seleccionamos periodos de train
  dataset[get(periodColName) %in% trainPeriods, train := 1]
  
  # Seleccionamos periodos de test
  dataset[get(periodColName) %in% testPeriods, train := 0]
  
  # Creacion de variables para la generacion del nombre del output
  modelingDay <- format(Sys.Date(), "%Y%m%d")
  outputFileName <- modelingDay %+% "_"  %+% modelName
  
  # Periodos para desarrollo, calculados a partir de la fecha final del train
  periodsForDevelopment <- seqDays(dayAddInteger(max(dataset[train == 1, YEAR_MONTH_DAY]), -daysForDevelopment), max(dataset[train == 1, YEAR_MONTH_DAY]))
  
  # Creaccion del train
  dataset[get(target) < 0, eval(target) := 0]
  xtrain <- dataset[train == 1 & !YEAR_MONTH_DAY %in% periodsForDevelopment, ]
  
  # En caso de que se necesite reducir el numero de 0s
  # Este procedimiento es recomendable para el caso de target muy baja
  # Considerese como un hiperparametro adicional por optimizar
  if(doUndersampling){
    xtrainZeros <- xtrain[get(target) == 0, ]
    set.seed(19610412)
    sampleZeros <- sample_n(xtrainZeros, round(0.6*NROW(xtrainZeros), 0))
    xtrain <- rbind(xtrain[get(target)==1, ], sampleZeros)
  }
  # Creacion del set de development
  xdev <- dataset[train == 1 & YEAR_MONTH_DAY %in% periodsForDevelopment, ]
  # Creacion del set de test
  xtest <- dataset[train == 0, ]
  
  cat("\nExecuting model: ", modelName,
      "\nRunning model for target", target,
      "\nTrain: from ", min(xtrain[,
                                   get(periodColName)]), "to",
      max(xtrain[,
                 get(periodColName)]),
      nrow(xtrain[,]), "x", ncol(xtrain[,-varsToRemove, with = FALSE]),
      "Target mean =", xtrain[, mean(get(target), na.rm=T)],
      "\nDevelopment: from ", min(xdev[,
                                       get(periodColName)]), "to",
      max(xdev[,
               get(periodColName)]),
      nrow(xdev[,]), "x", ncol(xdev[,-varsToRemove, with = FALSE]),
      "Target mean =", xdev[, mean(get(target), na.rm=T)],
      "\nTest: from ", min(xtest[,
                                 get(periodColName)]), "to",
      max(xtest[,
                get(periodColName)]),
      nrow(xtest[,]), "x", ncol(xtest[,-varsToRemove, with = FALSE]),
      "\n")
  
  # Creacion de los objetos xgb.DMatrix requeridos por XGBoost
  # Este proceso transforma el dataset en dos matrices numericas que el algoritmo
  # entiende
  trainMatrix <- xgb.DMatrix(data = data.matrix(xtrain[, -varsToRemove, with = FALSE]),
                             label = unlist(xtrain[, target, with = FALSE]),
                             missing = c(NA, NaN))
  devMatrix <- xgb.DMatrix(data = data.matrix(xdev[, -varsToRemove, with = FALSE]),
                           label = unlist(xdev[, target, with = FALSE]),
                           missing = c(NA, NaN))
  testMatrix <- xgb.DMatrix(data = data.matrix(xtest[, -varsToRemove, with = FALSE]),
                            label = unlist(xtest[, target, with = FALSE]),
                            missing = c(NA, NaN))
  gc()
  
  # Entrenamiento del algoritmo
  # data -> matriz de train en formato xgb.DMatrix
  # watchlist -> lista de matrices xgb.DMatrix que el algoritmo usa para testeo
  # Por defecto, el algoritmo entrena con train y valida con la ultima entrada
  # params -> hiperparametros del algoritmo
  # nrounds -> numero de arboles
  # nthread -> numero de CPUs utilizadas
  # missing -> tratamiento de valores missing
  # verbose -> nivel de informacion que retorna el modelo durante ejecucion
  # maximize -> TRUE en caso de que el algoritmo tenga que maximizar la eval_metric
  
  cat("Entrenando modelo XGBoost para clasificacion binaria de", target, fill = TRUE)
  set.seed(19610412)
  modelXGBoostBinary <- xgb.train(data = trainMatrix,
                                  watchlist = list(train = trainMatrix, 
                                                   test = testMatrix, 
                                                   development = devMatrix),
                                  params = parametersConfig,
                                  early_stopping_rounds = 30,
                                  nrounds = nRounds,
                                  nthread = 12,
                                  missing = NA,
                                  verbose = TRUE,
                                  maximize = TRUE)
  
  # Crear predicciones del modelo
  cat("Creando predicciones", fill = TRUE)
  
  xgb.pred <- predict(modelXGBoostBinary,
                      testMatrix,
                      missing = NA)

  # Calcular el impacto de cada variable en cada uno de los contratos
  savePredBreakdown(model = modelXGBoostBinary, matrix = testMatrix, 
                    predXGB = xgb.pred, productIDs =xtest[, get(productIDColName)],
                    outputFileName)
  
  # Procesamiento de los resultados a nivel cliente y producto
  dt.res <- data.table()
  dt.imp <- data.table()
  
  xtest[, predxgb := xgb.pred]
  
  xtest[, MAX_PRED := max(predxgb), by = c(clientIDColName, periodColName)]
  xtest[, MIN_PRED := min(predxgb), by = c(clientIDColName, periodColName)]
  xtest[, MAX_TARGET := max(get(target)), by = c(clientIDColName, periodColName)]
  xtest[, SUM_TARGET := sum(get(target)), by = c(clientIDColName, periodColName)]
  xtest[, CAPITAL_ENTRA_EN_MORA_CLIENTE := sum(SALDO_CAPITAL_MORA, na.rm = T), by = c(clientIDColName, periodColName)]
  xtest[, CAPITAL_ENTRA_EN_MORA_CLIENTE_CONT := sum(SAL_CAPITA_CONT, na.rm = T), by = c(clientIDColName, periodColName)]
  xtest[is.na(SAL_CAPITAL_CLIENTE_CONT), SAL_CAPITAL_CLIENTE_CONT := 0]
  xtest[, SALDO_TOTAL_CLIENTE := SAL_CAPITAL_CLIENTE_CONT - CAPITAL_ENTRA_EN_MORA_CLIENTE_CONT + CAPITAL_ENTRA_EN_MORA_CLIENTE,
        by = c(clientIDColName, periodColName)]
  
  xtest[is.na(LINEA_TARJETA_DE_CREDITO_CREDIBANCO_MORA), LINEA_TARJETA_DE_CREDITO_CREDIBANCO_MORA := 0]
  xtest[is.na(LINEA_TARJETA_DE_CREDITO_EXPRESS_MORA), LINEA_TARJETA_DE_CREDITO_EXPRESS_MORA := 0]
  xtest[, NUM_TARJ_EXPRESS := sum(LINEA_TARJETA_DE_CREDITO_EXPRESS_MORA, na.rm = T), by = c(clientIDColName, periodColName) ]
  xtest[, NUM_TARJ_CREDIBANCO := sum(LINEA_TARJETA_DE_CREDITO_CREDIBANCO_MORA, na.rm = T), by = c(clientIDColName, periodColName) ]
  xtest <- xtest[!is.na(get(clientIDColName)) & !is.na(get(productIDColName))]
  
  xtest[, NUM_TARJ_EXPRESS_ROLL := LINEA_TARJETA_DE_CREDITO_EXPRESS_MORA * get(target)]
  xtest[, NUM_TARJ_CREDIBANCO_ROLL := LINEA_TARJETA_DE_CREDITO_CREDIBANCO_MORA * get(target)]
  
  xtest[, NUM_TARJ_CREDIBANCO_ROLL := sum(NUM_TARJ_CREDIBANCO_ROLL), by = c(clientIDColName, periodColName)]
  xtest[, NUM_TARJ_EXPRESS_ROLL := sum(NUM_TARJ_EXPRESS_ROLL), by = c(clientIDColName, periodColName)]
  
  clientVision <- unique(xtest[, c(clientIDColName,
                                   periodColName,
                                   "SALDO_TOTAL_CLIENTE",
                                   "MAX_PRED",
                                   "MIN_PRED",
                                   "MAX_TARGET",
                                   "SUM_TARGET",
                                   "NUM_TARJ_EXPRESS", 
                                   "NUM_TARJ_CREDIBANCO",
                                   "NUM_TARJ_EXPRESS_ROLL",
                                   "NUM_TARJ_CREDIBANCO_ROLL"), with = F],
                         by = c(clientIDColName, periodColName))
  
  clientVision[, LOG_EXPO := log(SALDO_TOTAL_CLIENTE + 1)]
  clientVision[, LOG_EXPO_MAX_PRED := MAX_PRED*LOG_EXPO]
  clientVision[, LOG_EXPO_MIN_PRED := MIN_PRED*LOG_EXPO]
  clientVision[, MIN_RANK_PRED := frank(-MIN_PRED)/.N]
  clientVision[, MAX_RANK_PRED := frank(-MAX_PRED)/.N]
  clientVision[, LOG_EXPO_MAX_PRED_RANK := frank(-LOG_EXPO_MAX_PRED)/.N]
  clientVision[, LOG_EXPO_MIN_PRED_RANK := frank(-LOG_EXPO_MIN_PRED)/.N]
  
  # Guardar los bounds de riesgo de la variable LOG_EXPOSURE_PRED
  
  riskPerDecile <- saveBoundsLogExposurePred(model = modelXGBoostBinary, matrix = devMatrix,
                                             dt = xdev, clientIDColName, periodColName,
                                             outputFileName, target)
  
  
  clientVision[, riskDecile :=cut(LOG_EXPO_MAX_PRED, 
                                  breaks = c(-Inf, riskPerDecile[, max_logExpPred]),
                                  labels = riskPerDecile[, riskDecile],
                                  right = T)]
  
  ModelPerformanceByPercentileCollections(dir = modelPerformanceByPercentilePath, validationSet = clientVision,
                                          target = "MAX_TARGET", capitalCol = "SALDO_TOTAL_CLIENTE",
                                          scores = "LOG_EXPO_MAX_PRED", step = 1,
                                          title = paste0(outputFileName, "_client_vision_logexpoRanking"),
                                          sep = ";", dec = ".")
  
  ModelPerformanceByPercentileCollections(dir = modelPerformanceByPercentilePath, validationSet = clientVision,
                                          target = "MAX_TARGET", capitalCol = "SALDO_TOTAL_CLIENTE",
                                          scores = "MAX_PRED", step = 1,
                                          title = paste0(outputFileName, "_client_vision_maxPredRanking"),
                                          sep = ";", dec = ".")
  
  print("#3")
  # Procesamiento a nivel producto
  productVision <- xtest[, c(clientIDColName, 
                             productIDColName, 
                             periodColName,
                             "predxgb",
                             target,
                             "SALDO_CAPITAL_MORA", 
                             "LINEA_MORA"), 
                         with = F]
  
  productVision[, LOG_EXPO := log(SALDO_CAPITAL_MORA + 1)]
  productVision[, LOG_EXPO_PRED := predxgb*LOG_EXPO]
  productVision[, RANK_PRED := frank(-predxgb)/.N, by = .(LINEA_MORA)]
  productVision[, LOG_EXPO_PRED_RANK := frank(-LOG_EXPO_PRED)/.N, by = .(LINEA_MORA)]
  
  #Create outputs
  
  #Performance metrics
  for (product in unique(productVision[, LINEA_MORA])){
    ModelPerformanceByPercentileCollections(dir = modelPerformanceByPercentilePath, validationSet = productVision[LINEA_MORA == product],
                                            target = "TARGET_BINARY_TARG",  capitalCol = "SALDO_CAPITAL_MORA",
                                            scores = "LOG_EXPO_PRED", step = 1, title = outputFileName %+% "_logExpScore_" %+% product ,
                                            sep = ";", dec = ".")
    ModelPerformanceByPercentileCollections(dir = modelPerformanceByPercentilePath, validationSet = productVision[LINEA_MORA == product],
                                            target = "TARGET_BINARY_TARG", capitalCol = "SALDO_CAPITAL_MORA",
                                            scores = "predxgb", step = 1, title = outputFileName %+% "_score_" %+% product ,
                                            sep = ";", dec = ".")
  }
  
  
  OutputVarImp <- saveMostImportantVariables(xtrain = xtrain, 
                                             varsToRemove = varsToRemove, 
                                             modelXGBoostBinary = modelXGBoostBinary, 
                                             outputFileName = outputFileName)
  # Comportamiento de las variables mas predictivas por decil de riesgo
  createVariableSummaryPerScoreTranche(dt = xtest, 
                                       OutputVarImp, outputName = outputFileName, 
                                       outputDir = variableSummaryByTranchePath)
  
  # Guardar curva ROC
  DrawROCCurve(dir = rocCurvePath,validationSet =  productVision,
               target = target, scores = productVision$predxgb,
               colours = c("#EC0D06", "#000000"), 
               title = outputFileName, maxSize = 750000)
  
  # Guardar modelo pre-entrenado
  saveModel(modelXGBoostBinary = modelXGBoostBinary, modelName = modelName)
  saveVariables(dataset = dataset, 
                varsToRemove = varsToRemove, 
                modelName = modelName)
  # saveBestResult()
  saveIDScore(clientVision = clientVision,
              productVision = productVision,
              outputFileName = outputFileName)
  
}

