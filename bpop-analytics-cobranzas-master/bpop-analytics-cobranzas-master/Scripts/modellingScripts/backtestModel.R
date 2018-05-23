#' readModel
#' 
#' @description Funcion encargada de cargar el modelo entrenado de XGBoost que 
#' se encuentra guardado en el almacenamiento s3
#' @param modelName Nombre del modelo que se utiliza para generar las predicciones
#' @return modelo en formato xgb.Booster para ser utilizado por el resto de
#' funciones dependientes
#' @examples
#' modelXGBoostBinary <- readModel(modelName = "Cobranzas_V1")
#'      

readModel <- function(modelName){
  modelFile <- osPathJoin(modelsPath, paste0("ModelBinary_", modelName, ".RDS"))
  cat("ReadRDS RDS", modelName, fill = TRUE)
  modelXGBoostBinary <- readRDS_s3(osPathJoin(modelsPath,
                                              paste0("ModelBinary_", modelName, ".RDS")))
  return(modelXGBoostBinary)
}

#' readVariables
#' 
#' @description Funcion encargada de cargar las variables utilizadas durante
#' el entrenamiento del modelo desde la ruta especifica en el almacenamiento s3.
#' @param modelName Nombre del modelo que se utiliza para generar las predicciones
#' @return Vector de variables utilizadas durante el entrenamiento del modelo
#' seleccionado
#' @examples
#' varsToSelect <- readVariables(modelName = "Cobranzas_V1")
#'                   

readVariables <- function(modelName){
  cat("ReadRDS RDS variables", modelName, fill = TRUE)
  variablesFile <- readRDS_s3(osPathJoin(variablesPath,
                                         paste0("variablesModeloXGBoost_", modelName,".RDS")))
  return(variablesFile)
}


#' saveIDScoreBacktest
#' 
#' @description Funcion de guardado automatico en el almacenamiento S3 de las predicciones
#' del modelo sobre los datos del backtest
#' 
#' @param clientVision Tabla resumen de las predicciones a nivel cliente
#' @param productVision Tabla resumen de las predicciones a nivel contrato
#' @param outputFileName Nombre del modelo a guardar
#' @return Tablas con la importancia por variable guardada en S3.
#' @examples
#' saveIDScoreBacktest(clientVision = clientVision, productVision = productVision, outputFileName = "Cobranzas")

saveIDScoreBacktest <- function(clientVision, productVision, outputFileName){
  # # Generation of the IDs and scores of the validation set
  cat("fwriteS3 IDScore", fill = TRUE)
  # Guardar vision a nivel cliente
  fwrite_s3(clientVision, paste0(backtetingClientVisionPath, "/ID_Score_ClientVision_", outputFileName, ".csv"))
  # Guardar vision a nivel producto
  fwrite_s3(productVision, paste0(backtetingProductVisionPath, "/ID_Score_ProductVision_", outputFileName, ".csv"))
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


backtestModel <- function(dataset, modelName, varsToRemove,
                          clientIDColName, productIDColName, 
                          target, periodColName, inicioBacktest, 
                          finBacktest){
  xtest <- dataset
  # Creacion de variables para la generacion del nombre del output

  outputFileName <- inicioBacktest %+% "_" %+% finBacktest %+% "_"  %+% modelName
  
  # Cargamos el modelo guardado y los nombres de las variables utilizadas
  # durante el entrenamiento
  modelXGBoostBinary <- readModel(modelName = modelName)
  varsToSelect <- readVariables(modelName = modelName)
  
  # Creacion del set de backtest
  cat("\nUsing model: ", modelName,
      "\nBacktesting model for target", target,
      "\nDataset: from ", min(xtest[,
                                   get(periodColName)]), "to",
      max(xtest[,
                 get(periodColName)]),
      nrow(xtest[,]), "x", ncol(xtest[,-varsToRemove, with = FALSE]),
      "Target mean =", xtest[, mean(get(target), na.rm=T)], "\n")
      
  # Creacion de los objetos xgb.DMatrix requeridos por XGBoost
  # Este proceso transforma el dataset en matrices numericas que el algoritmo
  # entiende
  xtest[, train := 0]
  varsMissing <- setdiff(varsToSelect, names(xtest))
  xtest[, (varsMissing) := NA]
  testMatrix <- xgb.DMatrix(data = data.matrix(xtest[, varsToSelect, with = FALSE]),
                            missing = NA)
  gc()
  
  # Crear predicciones del modelo
  cat("Creando predicciones", fill = TRUE)
  
  xgb.pred <- predict(modelXGBoostBinary,
                      testMatrix,
                      missing = NA)
  
  # Calcular el impacto de cada variable en cada uno de los contratos
  savePredBreakdown(model = modelXGBoostBinary, matrix = testMatrix, 
                    predXGB = xgb.pred, productIDs =xtest[, get(productIDColName)],
                    outputFileName, path = backtetingPredBreakdownPath)
  
  # Procesamiento de los resultados a nivel cliente y producto
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
  
  # Cargamos los ultimos bounds de riesgo de la variable LOG_EXPOSURE_PRED
  
  riskPerDecile <- fread_s3(osPathJoin(boundsLogExposurePredPath, "riskPerDecileMaster.csv"))
  
  
  clientVision[, riskDecile :=cut(LOG_EXPO_MAX_PRED, 
                                  breaks = c(-Inf, riskPerDecile[, max_logExpPred]),
                                  labels = riskPerDecile[, riskDecile],
                                  right = T)]
  
  ModelPerformanceByPercentileCollections(dir = backtetingModelPerformanceByPercentilePath, validationSet = clientVision,
                                          target = "MAX_TARGET", capitalCol = "SALDO_TOTAL_CLIENTE",
                                          scores = "LOG_EXPO_MAX_PRED", step = 1,
                                          title = paste0(outputFileName, "_client_vision_logexpoRanking"),
                                          sep = ";", dec = ".")
  
  ModelPerformanceByPercentileCollections(dir = backtetingModelPerformanceByPercentilePath, validationSet = clientVision,
                                          target = "MAX_TARGET", capitalCol = "SALDO_TOTAL_CLIENTE",
                                          scores = "MAX_PRED", step = 1,
                                          title = paste0(outputFileName, "_client_vision_maxPredRanking"),
                                          sep = ";", dec = ".")

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
    ModelPerformanceByPercentileCollections(dir = backtetingModelPerformanceByPercentilePath, validationSet = productVision[LINEA_MORA == product],
                                            target = "TARGET_BINARY_TARG",  capitalCol = "SALDO_CAPITAL_MORA",
                                            scores = "LOG_EXPO_PRED", step = 1, title = outputFileName %+% "_logExpScore_" %+% product ,
                                            sep = ";", dec = ".")
    ModelPerformanceByPercentileCollections(dir = backtetingModelPerformanceByPercentilePath, validationSet = productVision[LINEA_MORA == product],
                                            target = "TARGET_BINARY_TARG", capitalCol = "SALDO_CAPITAL_MORA",
                                            scores = "predxgb", step = 1, title = outputFileName %+% "_score_" %+% product ,
                                            sep = ";", dec = ".")
  }
  
  # Cargamos las variables mas importantes del modelo
  OutputVarImp <- xgb.importance(feature_names = names(xtest[, -varsToRemove, with = FALSE]),
                                 model = modelXGBoostBinary) 
  OutputVarImp[, SOURCE := getSource(Feature), by = Feature]
  OutputVarImp[, SOURCE_GAIN := sum(Gain)/sum(OutputVarImp[, Gain]), by = .(SOURCE)]
  
  setorder(OutputVarImp,-Gain)

  # Comportamiento de las variables mas predictivas por decil de riesgo
  createVariableSummaryPerScoreTranche(dt = xtest, 
                                       OutputVarImp, outputName = outputFileName, 
                                       outputDir = backtestinVariableSummaryByTranchePath)
  
  # Guardar curva ROC
  DrawROCCurve(dir = backtestingRocCurvePath, validationSet =  productVision,
               target = target, scores = productVision$predxgb,
               colours = c("#EC0D06", "#000000"), 
               title = outputFileName, maxSize = 750000)
  
  # saveBestResult()
  saveIDScoreBacktest(clientVision = clientVision,
              productVision = productVision,
              outputFileName = outputFileName)
  
}