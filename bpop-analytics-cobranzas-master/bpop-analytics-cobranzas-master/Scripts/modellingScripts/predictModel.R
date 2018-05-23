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

#' savePredictions
#' 
#' @description Funcion de guardado automatico de las predicciones
#' del ultimo modelo entrenado de XGBoost disponible que se encuentra
#' almacenado en S3. Tanto la versión a nivel cliente oomo a nivel
#' producto 
#' 
#' @param clientVision Tabla con el resumen de las predicciones a nivel cliente
#' @param productVision Tabla con el resumen de las predicciones a nivel producto
#' @param outputFileName nombre generico del archivo a guardar
#' @param periodo periodo para el que se han generado las predicciones
#' @return Predicciones del modelo guardadas en S3
#' @examples
#' savePredictions(clientVision = clientVision,
#'                 productVision = productVision,
#'                 outputFileName = outputFileName,
#'                 periodo = "20170101")
#'                   

savePredictions <- function(clientVision, productVision, outputFileName, periodo){
  # # Generation of the IDs and scores of the validation set
  cat("fwriteS3 IDScore", fill = TRUE)
  # Guardar vision a nivel cliente
  fwrite_s3(clientVision, paste0(dailyClientVisionPath, "/Predictions_ClientVision_", periodo, ".csv"))
  # Guardar vision a nivel producto
  fwrite_s3(productVision, paste0(dailyProductVisionPath, "/Predictions_ProductVision_", periodo, ".csv"))
}

#' saveDailyPredBreakdown
#' 
#' @description Funcion encargada de guardar el desglose, por variable, del score que se atribuye
#' a cada contrato en formato logodds
#' @param model Modelo con el cual se han generado las predicciones
#' @param matrix Tabla en formato xgb.DMatrix sobre la que se han generado las predicciones
#' @param predXGB vector con las predicciones generadas para cada contrato
#' @param productIDs vector de identificadores unicos de los contratos para los que
#' se han generado las predicciones
#' @param periodo periodo para el que se han generado las predicciones
#' @return Predicciones del modelo guardadas en S3
#' @examples
#' saveDailyPredBreakdown(model = modelXGBoostBinary, matrix = testMatrix, 
#'                       predXGB = xgb.pred, productIDs = productIDs,
#'                       periodo = "20170101")
#'      

saveDailyPredBreakdown <- function(model = modelXGBoostBinary, matrix = testMatrix, predXGB, productIDs, periodo){
  
  pred.breakdown <- as.data.table(predict(model,
                                          matrix,
                                          predcontrib = TRUE))
  
  pred.breakdown[, logit := rowSums(.SD, na.rm = TRUE), .SDcols = names(pred.breakdown)]
  pred.breakdown[, pred := logit.prob(logit)]
  pred.breakdown[, ID_CONTRATO := productIDs]
  # Guardar vision a nivel producto
  fwrite_s3(pred.breakdown, paste0(dailyPredBreakdownPath, "/predBreakdown_", periodo, ".csv"))
}


#' predictModel
#' 
#' @description Funcion encargada de generar las predicciones para los productos
#' y clientes que entran en mora, a partir del modelo y otros metadatos guardados
#' en el almaenamiento s3
#' @param dataset tabla que contiene la master table para los productos a predecir
#' @param modelName Nombre del modelo que se utiliza para generar las predicciones
#' @param varsToRemove Vector que contiene las variables que no queremos incluir
#' para generar la prediccion
#' @param clientIDColName Nombre de la columna que contiene el ID de cliente
#' @param productIDColName Nombre de la columna que contiene el ID de producto
#' @param periodColName Nombre de la columna que contiene el identificador de periodo
#' @param periodo Periodo a predecir en formato "YYYYMMDD"
#' @return Predicciones para el periodo seleccionado a nivel de cliente y contrato
#' @examples
#' test <- predictModel(dataset = dataset, 
#'                      modelName = "Cobranzas_V1",
#'                      varsToRemove = varsToRemove, 
#'                      productIDColName = "ID_CONTRATO",
#'                      clientIDColName = "ID_CLIENTE",
#'                      periodColName = "YEAR_MONTH_DAY",
#'                      periodo = "20170101")
#'                     
   
predictModel <- function(dataset, modelName, varsToRemove,
                       clientIDColName, productIDColName, periodColName, periodo = predictPeriodo){
  xtest <- dataset
  # Creacion de variables para la generacion del nombre del output
  modelingDay <- format(Sys.Date(), "%Y%m%d")
  outputFileName <- modelingDay %+% "_"  %+% modelName
  
  # Cargamos el modelo guardado y los nombres de las variables utilizadas
  # durante el entrenamiento
  modelXGBoostBinary <- readModel(modelName = modelName)
  varsToSelect <- readVariables(modelName = modelName)
  
  # Creacion del set de test
  periodo <- xtest[, unique(get(periodColName))]
  cat("\nExecuting model: ", modelName,
      "\nTest: Periodo ", periodo,
      nrow(xtest[,]), "x", length(varsToSelect),
      "\n")
  
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
  
  # Creamos las predicciones para el periodo selecionado
  xgb.pred <- predict(modelXGBoostBinary,
                      testMatrix,
                      missing = NA)
  
  # Calcular el impacto de cada variable en la prediccion para cada contrato
  saveDailyPredBreakdown(model = modelXGBoostBinary, matrix = testMatrix, 
                         predXGB = xgb.pred, productIDs =xtest[, get(productIDColName)],
                         periodo)
  
  # Procesamiento de los resultados a nivel cliente y producto
  xtest[, predxgb := xgb.pred]
  
  xtest[, MAX_PRED := max(predxgb), by = c(clientIDColName, periodColName)]
  xtest[, MIN_PRED := min(predxgb), by = c(clientIDColName, periodColName)]
  xtest[, CAPITAL_ENTRA_EN_MORA_CLIENTE := sum(SALDO_CAPITAL_MORA, na.rm = T), by = c(clientIDColName, periodColName)]
  xtest[, CAPITAL_ENTRA_EN_MORA_CLIENTE_CONT := sum(SAL_CAPITA_CONT, na.rm = T), by = c(clientIDColName, periodColName)]
  xtest[is.na(SAL_CAPITAL_CLIENTE_CONT), SAL_CAPITAL_CLIENTE_CONT := 0]
  xtest[, SALDO_TOTAL_CLIENTE := SAL_CAPITAL_CLIENTE_CONT - CAPITAL_ENTRA_EN_MORA_CLIENTE_CONT + CAPITAL_ENTRA_EN_MORA_CLIENTE,
        by = c(clientIDColName, periodColName)]
  
  # Procesamiento a nivel cliente
  clientVision <- unique(xtest[, c(clientIDColName,
                                   periodColName,
                                   "SALDO_TOTAL_CLIENTE",
                                   "MAX_PRED",
                                   "MIN_PRED"), with = F],
                         by = clientIDColName)
  
  clientVision[, LOG_EXPO := log(SALDO_TOTAL_CLIENTE + 1)]
  clientVision[, LOG_EXPO_MAX_PRED := MAX_PRED*LOG_EXPO]
  clientVision[, LOG_EXPO_MIN_PRED := MIN_PRED*LOG_EXPO]
  clientVision[, MIN_RANK_PRED := frank(-MIN_PRED)/.N]
  clientVision[, MAX_RANK_PRED := frank(-MAX_PRED)/.N]
  clientVision[, LOG_EXPO_MAX_PRED_RANK := frank(-LOG_EXPO_MAX_PRED)/.N]
  clientVision[, LOG_EXPO_MIN_PRED_RANK := frank(-LOG_EXPO_MIN_PRED)/.N]
  
  # Cargamos los ultimos bounds de riesgo de la variable LOG_EXPOSURE_PRED
  riskPerDecile <- fread_s3(osPathJoin(boundsLogExposurePredPath, "riskPerDecileMaster.csv"))
  
  # Incluimos la variable riskDecile con los limites de LOG_EXPO_MAX_PRED obtenidos durante el 
  # entrenamiento
  clientVision[, riskDecile :=cut(LOG_EXPO_MAX_PRED, 
                                  breaks = c(-Inf, riskPerDecile[, max_logExpPred]),
                                  labels = riskPerDecile[, riskDecile],
                                  right = T)]
  # Procesamiento a nivel producto
  productVision <- xtest[, c(clientIDColName, 
                             productIDColName, 
                             periodColName,
                             "predxgb",
                             "SALDO_CAPITAL_MORA", 
                             "LINEA_MORA"), 
                         with = F]
  
  productVision[, LOG_EXPO := log(SALDO_CAPITAL_MORA + 1)]
  productVision[, LOG_EXPO_PRED := predxgb*LOG_EXPO]
  productVision[, RANK_PRED := frank(-predxgb)/.N, by = .(LINEA_MORA)]
  productVision[, LOG_EXPO_PRED_RANK := frank(-LOG_EXPO_PRED)/.N, by = .(LINEA_MORA)]
  
  # Guardamos las predicciones
  savePredictions(clientVision = clientVision,
              productVision = productVision,
              outputFileName = outputFileName,
              periodo = periodo)
  
}

