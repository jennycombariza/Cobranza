trainModel <- function(dataset, modelName, varsToRemove, outputName, target, trainPeriods, testPeriods, daysForDevelopment = 30,
                       clientIDColName, productIDColName, periodColName, parametersConfig, nRounds, 
                       doUndersampling = F){
  

  dataset[get(periodColName) %in% trainPeriods, train := 1]
  dataset[get(periodColName) %in% testPeriods, train := 0]
  
  modelingDay <- format(Sys.Date(), "%Y%m%d")
  modelResults.path <-  outputsPath %+% "/" %+% modelingDay %+%  "_" %+% modelName %+%  "/"
  dateOutput <- str_extract(basename(modelResults.path), "\\d{8}")
  outputFileName <- dateOutput %+% "_"  %+% target
  #Create outputs
  resFileName <- modelResults.path %+% "resModel_" %+% outputFileName %+% ".csv"
  varImpFileName <-  modelResults.path %+% "modelvarImp_" %+% outputFileName %+% ".csv"
  clientVisionFileName <- modelResults.path %+% "Model_clientVision" %+% outputFileName %+% ".csv"
  productVisionFileName <- modelResults.path %+% "Model_productVision" %+% outputFileName %+% ".csv"
  probabilityDistributionFileName <- modelResults.path %+% "probabilityDistrib_" %+% outputFileName
  logOddsDistributionFolder <- modelResults.path %+% "logOddsDistribution/"
  
  if(!dir.exists(modelResults.path)){
    dir.create(modelResults.path, recursive = TRUE)
    dir.create(logOddsDistributionFolder, recursive = TRUE)
  }
  periodsForDevelopment <- seqDays(dayAddInteger(max(dataset[train == 1, YEAR_MONTH_DAY]), -daysForDevelopment), max(dataset[train == 1, YEAR_MONTH_DAY]))
  
  dataset[get(target) < 0, eval(target) := 0]
  xtrain <- dataset[train == 1 & !YEAR_MONTH_DAY %in% periodsForDevelopment, ]
  if(doUndersampling){
    xtrainZeros <- xtrain[get(target) == 0, ]
    set.seed(1804)
    sampleZeros <- sample_n(xtrainZeros, round(0.6*NROW(xtrainZeros), 0))
    xtrain <- rbind(xtrain[get(target)==1, ], sampleZeros)
  }
  xdev <- dataset[train == 1 & YEAR_MONTH_DAY %in% periodsForDevelopment, ]
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
  # Creation of the xgb.DMatrix objects required by xgboost
  # This step transforms the dataset into two numeric matrices that the algorithm requires as input
  
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
  # Training the algorithm itself
  
  cat("Training XGBoost model for binary classification of", target, fill = TRUE)
  # parametersConfig <-  list(booster   = "gbtree",
  #                     objective       = "binary:logistic",
  #                     eval_metric     = "auc",
  #                     tree_method     = "exact",
  #                     max.depth       = 12,
  #                     eta             = 0.05,
  #                     gamma           = 12,
  #                     alpha           = 6,
  #                     lambda          = 6,
  #                     subsample       = 0.9,
  #                     colsample_bytree= 0.9
  # )
  # nRounds <- 200
  set.seed(1804)
  modelXGBoostBinary <- xgb.train(data = trainMatrix,
                                  watchlist = list(train = trainMatrix, test = testMatrix, development = devMatrix),
                                  params = parametersConfig,
                                  nrounds = nRounds,
                                  nthread = 12,
                                  missing = NA,
                                  verbose = TRUE,
                                  early_stopping_rounds = 20,
                                  maximize = TRUE)
  
  # Creation of the predictions for the validation set
  cat("Creating predictions", fill = TRUE)
  
  xgb.pred <- predict(modelXGBoostBinary,
                      testMatrix,
                      missing = NA)
  

  # xgb.pred[sample(1:length(xgb.pred), length(xgb.pred)*0.07)] <- sample(xgb.pred, length(xgb.pred)*0.07)
  xgb.predDev <- predict(modelXGBoostBinary,
                         devMatrix,
                         missing = NA)
  # xgb.predDev[sample(1:length(xgb.predDev), length(xgb.predDev)*0.07)] <- sample(xgb.predDev, length(xgb.predDev)*0.07)
  
  # Saving the most important variables of the trained model
  OutputVarImp <- xgb.importance(feature_names = names(xtest[, -varsToRemove, with = FALSE]),
                                 model = modelXGBoostBinary) 
  mostImportantVars <- OutputVarImp[1:min(15,length(OutputVarImp[, Gain])), Feature]
  
  OutputVarImp[, SOURCE := getSource(Feature), by = Feature]
  OutputVarImp[, SOURCE_GAIN := sum(Gain)/sum(OutputVarImp[, Gain]), by = .(SOURCE)]
  cat("fwriteS3 outputVarImp", fill = TRUE)
  fwrite(OutputVarImp, varImpFileName)
  
  # Creamos los graficos de la distribucion de los logodds para el valor de las variables contenidas en
  # las 25 variables mas importantes del modelo
  
  pred_contr <- predict(modelXGBoostBinary,
                        testMatrix,
                        missing = NA,
                        predcontrib = T)
  for (i in seq(1:length(mostImportantVars))){
    var <- mostImportantVars[i]
    saveLogOddsPlot(var, i, path = logOddsDistributionFolder, pred_contr, xtest)
  }
  
  xtest[, predxgb := xgb.pred]
  xdev[, predxgb := xgb.predDev]
  
  min_score <- min(xdev[, predxgb])
  max_score <- max(xdev[, predxgb])
  
  score_vector <- c(0, seq(min_score, max_score, by = (max_score - min_score)/(100-1)), 1)
  xdev[, bucket_pred := as.numeric(as.character(cut(predxgb, breaks = score_vector, labels = round(score_vector[2:102], 8))))]
  xdev[, PROB_BUCKET_PRED :=  as.numeric(as.character(mean(get(target)))), by = bucket_pred]
  risk_vector <- seq(0, 1, by = 0.01)
  xdev[, riskPerc := frank(predxgb)/.N]
  xdev[, bucket_risk := as.numeric(as.character(cut(riskPerc, breaks = risk_vector, labels = risk_vector[2:length(risk_vector)])))]
  xdev[, PROB_BUCKET_RISK := as.numeric(as.character(mean(get(target)))), by = bucket_risk]
  
  probToFit <- unique(xdev[, .(cases = .N), by = .(bucket_pred, PROB_BUCKET_PRED)][order(bucket_pred)])
  probToFit[, perc := cases/sum(cases)]
  probToFit[, cumPerc := cumsum(perc)]
  probToFitRisk <- unique(xdev[, .(bucket_risk, PROB_BUCKET_RISK)][order(bucket_risk)])
  
  modelRisk <- glm(PROB_BUCKET_RISK ~ bucket_risk,data = probToFitRisk)
  modelPredWeighted <- glm(PROB_BUCKET_PRED ~ bucket_pred, data = probToFit, weights = perc)
  modelPred <- glm(PROB_BUCKET_PRED ~ bucket_pred, data = probToFit)
  probToFit[, fittedProbWeighted := predict(modelPredWeighted, list(bucket_pred = probToFit$bucket_pred))]
  probToFit[, fittedProb := predict(modelPred, list(bucket_pred = probToFit$bucket_pred))]
  probToFitRisk[, fittedProb := predict(modelRisk, list(bucket_risk = probToFitRisk$bucket_risk))]
  
  probabilityDistributionScoreBased <- plot_ly(data = probToFit, x = ~bucket_pred, y = ~PROB_BUCKET_PRED, type = 'scatter', name = "Original probability") %>% 
    add_trace(y = ~fittedProbWeighted, mode = 'lines', name = "Population Weighted") %>%
    add_trace(y = ~fittedProb, mode = 'lines', name = "Non-weighted") %>%
    add_lines(data = probToFit, x = ~bucket_pred, y = ~perc, type = 'bar', yaxis = "y2", name = "population %") %>% 
    layout(yaxis = list(range = c(0,1.01),
                        title = "Probability"),
           yaxis2 = list(overlaying= "y",
                         side = "right",
                         title = "population density",
                         range = c(0, max(probToFit[, perc]) + 0.05)))
  
  probabilityDistributionRiskPerc <- plot_ly(data = probToFitRisk, x = ~bucket_risk, y = ~PROB_BUCKET_RISK, type = 'scatter', name = "Original probability") %>% 
    add_trace(y = ~fittedProb, mode = 'lines', name = "Fitted curve") %>%
    layout(yaxis = list(range = c(0,1.01),
                        title = "Probability"))
  
  # saveWidget(as_widget(probabilityDistributionScoreBased), file = probabilityDistributionFileName %+% "_ScoreBased.html")
  # fwrite(probToFit, probabilityDistributionFileName %+% "_ScoreBased.csv")
  # saveWidget(as_widget(probabilityDistributionRiskPerc), file = probabilityDistributionFileName %+% "_RiskPercentile.html")
  # fwrite(probToFitRisk, probabilityDistributionFileName %+% "_RiskPercentile.csv")
  

  xtest[is.na(SAL_CAPITA_CONTR), SAL_CAPITA_CONTR := 0 ]
  xtest[is.na(SAL_CAPITAL_CLIENTE_CONTR), SAL_CAPITAL_CLIENTE_CONTR := 0 ]
  xtest[, probabilityScoreWeighted := predict(modelPredWeighted, list(bucket_pred = xtest$predxgb))]
  xtest[probabilityScoreWeighted > 0.99,  probabilityScoreWeighted := 0.99]
  xtest[, probabilityScore := predict(modelPred, list(bucket_pred = xtest$predxgb))]
  xtest[probabilityScore > 0.99,  probabilityScore := 0.99]
  xtest[, riskPerc := frank(predxgb)/.N]
  xtest[, bucket_risk := as.numeric(as.character(cut(riskPerc, breaks = risk_vector, labels = risk_vector[2:length(risk_vector)])))]
  xtest[, probabilityRiskPerc := predict(modelRisk, list(bucket_risk = xtest$bucket_risk))]
  
  xtest[, MAX_PRED := max(predxgb), by = c(clientIDColName, periodColName)]
  xtest[, MIN_PRED := min(predxgb), by = c(clientIDColName, periodColName)]
  xtest[, MAX_PROB := max(probabilityScore), by = c(clientIDColName, periodColName)]
  xtest[, MIN_PROB := min(probabilityScore), by = c(clientIDColName, periodColName)]
  xtest[, MAX_TARGET := max(get(target)), by = c(clientIDColName, periodColName)]
  xtest[, SUM_TARGET := sum(get(target)), by = c(clientIDColName, periodColName)]
  xtest[, SALDO_ENTRA_MORA_CLIENTE := sum(SALDO_CAPITAL_MR, na.rm = T), by = c(clientIDColName, periodColName)]
  clientVision <- unique(xtest[, c(clientIDColName,
                                   periodColName,
                                   "SAL_CAPITAL_CLIENTE_CONTR",
                                   "SALDO_ENTRA_MORA_CLIENTE",
                                   "MAX_PRED",
                                   "MAX_PROB",
                                   "MAX_TARGET",
                                   "SUM_TARGET"), with = F])
  
  clientVision[, LOG_EXPO := log(SAL_CAPITAL_CLIENTE_CONTR + 1)]
  clientVision[, LOG_EXPO_MAX_PRED := MAX_PRED*LOG_EXPO]
  # clientVision[, MIN_EXP_LOSS := SAL_CAPITAL_CLIENTE_CONTR * MAX_PROB]
  # clientVision[, MAX_EXP_LOSS := SAL_CAPITAL_CLIENTE_CONTR * MIN_PROB]
  clientVision[, MAX_RANK_PRED := frank(-MAX_PRED)/.N]
  clientVision[, LOG_EXPO_MAX_PRED_RANK := frank(-LOG_EXPO_MAX_PRED)/.N]
  fwrite(clientVision, analysisPath %+% "/PresentaciónModelo/clientVisionExample.csv", sep = ";" )
  # clientVision[, MIN_EXP_LOSS_RANK := frank(-MIN_EXP_LOSS)/.N]
  # clientVision[, MAX_EXP_LOSS_RANK := frank(-MAX_EXP_LOSS)/.N]
  
  
  ModelPerformanceByPercentileCollections(dir = modelResults.path, validationSet = clientVision,
                                          target = "MAX_TARGET", capitalCol = "SAL_CAPITAL_CLIENTE_CONTR",
                                          scores = "LOG_EXPO_MAX_PRED", step = 10, title = "uplift_client_vision_logexpoRanking",
                                          sep = ";", dec = ".")
  
  ModelPerformanceByPercentileCollections(dir = modelResults.path, validationSet = clientVision,
                                          target = "MAX_TARGET", capitalCol = "SAL_CAPITAL_CLIENTE_CONTR",
                                          scores = "MAX_PRED", step = 10, title = "uplift_client_vision_maxPredRanking",
                                          sep = ";", dec = ".")
  
  productVision <- xtest[, c(clientIDColName, productIDColName, periodColName, "predxgb", "TARGET_BINARY", "SAL_CAPITAL_CLIENTE_CONTR", "probabilityScore", names(xtest) %gv% "LINEA"), 
                         with = F]
  productVision[, LOG_EXPO :=  log(SAL_CAPITAL_CLIENTE_CONTR + 1)]
  productVision[, LOG_EXPO_PRED := predxgb*LOG_EXPO]
  # productVision[, EXP_LOSS := SAL_CAPITAL_CLIENTE_CONTR * probabilityScore]
  productVision[, RANK_PRED := frank(-predxgb)/.N, by = .(LINEA_AUX_MR)]
  productVision[, LOG_EXPO_PRED_RANK := frank(-LOG_EXPO_PRED)/.N, by = .(LINEA_AUX_MR)]
  # productVision[, EXP_LOSS_RANK := frank(-EXP_LOSS)/.N, by = .(LINEA_AUX_MR)]
  
  for (product in unique(productVision[, LINEA_MR])){
    ModelPerformanceByPercentileCollections(dir = modelResults.path, validationSet = productVision[LINEA_MR == product],
                                            target = "TARGET_BINARY",  capitalCol = "SAL_CAPITAL_CLIENTE_CONTR",
                                            scores = "LOG_EXPO_PRED", step = 10, title = outputFileName %+% "_logExpScore_" %+% product ,
                                            sep = ";", dec = ".")
    ModelPerformanceByPercentileCollections(dir = modelResults.path, validationSet = productVision[LINEA_MR == product],
                                            target = "TARGET_BINARY", capitalCol = "SAL_CAPITAL_CLIENTE_CONTR",
                                            scores = "predxgb", step = 10, title = outputFileName %+% "_score_" %+% product ,
                                            sep = ";", dec = ".")
  }
  
  
  #Most important variables behavior on top percentiles
  createVariableSummaryPerScoreTranche(dt = xtest, OutputVarImp, outputName = outputFileName, outputDir = modelResults.path)
  
  #Plot ROC curve
  DrawROCCurve(dir = modelResults.path, productVision, target = "TARGET_BINARY", scores = productVision$predxgb, colours = c("#EC0D06", "#000000"), 
               title = outputFileName, maxSize = 750000)
  
  #Save client vision
  fwrite(clientVision, clientVisionFileName)
  #Save product vision
  fwrite(productVision, productVisionFileName)
  
  return(rbind(productVision, xdev, fill = T))
}
