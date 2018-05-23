# This script contains all the auxiliary functions used on the rest of scripts
options(encoding = "latin-9",
        scipen = 999) 

osPathJoin <- function(...){
  # Combines directory path adding a forward slash. Alternative to paste0
  normalizePath(file.path(...), winslash = "/", mustWork = FALSE)
}

setWorkspace <- function(rootPath = "/ws_storage/disco_kolmogorov/GrupoAval",
                         modelType){
  # Creates all the working directories used to reference file locations
  cat("Creating workspace paths", fill = TRUE)
  
  stopifnot(modelType %in% c("BOCC",
                             "BPOP",
                             "BAV"))
  
  basePath <<- osPathJoin(rootPath, modelType)
  
  # Data paths
  dataPath <<- osPathJoin(basePath, "1.Data")
  originalDataPath <<- osPathJoin(dataPath, "RawData")
  originalDailyDataPath <<- osPathJoin(dataPath, "DailyData")
  preparedDataPath <<- osPathJoin(dataPath, "PreparedData")
  # File directories of every prepared monthly table
  targetPreparedPath <<- osPathJoin(preparedDataPath, "TARGET_MASTER")
  transaccionalPreparedPath <<- osPathJoin(preparedDataPath, "TRANSACCIONAL_MASTER")
  mergedTablePreparedPath <<- osPathJoin(preparedDataPath, "MERGED_TABLE_MASTER")
  datasetPreparedPath <<- osPathJoin(preparedDataPath, "DATASET_MASTER")
  
  # Analysis paths
  analysisPath <<- osPathJoin("/Drive/_Projects/201802_ZZZ000_GrupoAval_ABIERTO/BPOP/2.Analysis/")
  dataQualityAnalysisPath <<- osPathJoin(analysisPath, "dataQuality")
  # Models paths
  modelsPath <<- osPathJoin(basePath, "4.Models")
  scriptsPath <<- osPathJoin(modelsPath, "Scripts")
  processFunctionsPath <<- osPathJoin(scriptsPath, "processData")
  expandFunctionsPath <<- osPathJoin(scriptsPath, "expandMaster")
  outputsPath <<- osPathJoin(modelsPath, "Outputs")
  
}

loadFunctions <- function(rootPath = "/ws_storage/disco_kolmogorov/GrupoAval"){
  # Loads into memory all the createMasterFunctions that transform the original monthly raw table
  # into a prepared monthly table
  cat("Loading auxiliary functions", fill = TRUE)
  directories <- c(paste0(rootPath,"/generic_scripts/"))
  for(auxDirectory in directories){
    setwd(osPathJoin(auxDirectory))
    files <- dir()
    files <- files[str_detect(files, ".R")]
    for(script in files){
      cat("Loading", script, fill = TRUE)
      source(script)
    }
    
  }
  
  # setwd(rootPath)
}


loadProjectFunctions <- function(){
  # Loads into memory all the createMasterFunctions that transform the original monthly raw table
  # into a prepared monthly table
  cat("Loading auxiliary functions", fill = TRUE)
  processFunctions <- list.files(processFunctionsPath, full.names = T, pattern = ".R|.r")
  expandFunctions <- list.files(expandFunctionsPath, full.names = T, pattern = ".R|.r")
  for(script in c(processFunctions, expandFunctions)){
    cat("Loading", script, fill = TRUE)
    source(script)
    }
  }


librariesRequired <- c("bit64", "data.table", "tidyr", "stringr", 
                       "lubridate",
                       "xgboost","matrixStats", 
                       "plyr", "dplyr", 
                       "parallel", "lazyeval",
                       "jsonlite", "readxl", "rJava", "xlsx", 
                       # "xgboostExplainer",
                       "tictoc", "mlr", "ROCR", "png", "plotly", "htmlwidgets", "testthat"
                       )

loadLibraries <- function(librariesRequired){
  # Loads all the required libraries used in the model  
  for(aux in librariesRequired){
    cat(aux, fill = TRUE)
    tryCatch(do.call("library", list(aux)),
             error = function(err){
               cat(aux, "not installed. Installing and reloading", fill = TRUE)
               if(aux == "data.table"){
                 install.packages("data.table", type = "source",
                                  repos = "http://Rdatatable.github.io/data.table")
               } else {
                 install.packages(aux)    
               }
               do.call("library", list(aux))
             }
    )
  }
}

capitalizeNames <- function(dataset){
  auxNames <- names(dataset) %>% toupper %>% str_replace_all(" ", "_")
  auxNames <- iconv(auxNames, from = "UTF-8", to = "ASCII//TRANSLIT")
  setnames(dataset, auxNames)
}


getFuturePeriod <- function(PERIOD, numberOfPeriods){
  
  aux <- PERIOD
  aux <- paste0(str_sub(aux, 1, 4), "-W",
                str_sub(aux, 5, 6), "-1")
  stopifnot(is.numeric(numberOfPeriods))
  
  aux <- date2ISOweek(ISOweek2date(aux) + lubridate::weeks(numberOfPeriods))
  aux <- paste0(str_sub(aux, 1, 4),
                str_sub(aux, 7, 8))
  
  return(aux)
}

getPastPeriod <- function(PERIOD, numberOfPeriods){
  
  aux <- PERIOD
  aux <- paste0(str_sub(aux, 1, 4), "-W",
                str_sub(aux, 5, 6), "-1")
  stopifnot(is.numeric(numberOfPeriods))
  
  aux <- date2ISOweek(ISOweek2date(aux) - lubridate::weeks(numberOfPeriods))
  aux <- paste0(str_sub(aux, 1, 4),
                str_sub(aux, 7, 8))
  
  return(aux)
}


getPrettyWeek <- function(YEAR_WEEK){
  paste0(str_sub(YEAR_WEEK, 1, 4), "-W",
         str_sub(YEAR_WEEK, 5, 6), "-1")  %>% 
    ISOweek2date %>% return
}

getStartEndWeek <- function(week, numberDays = 6){
  dateStart <- getPrettyWeek(week)
  dateEnd <- dateStart + days(numberDays)
  return(c(dateStart, dateEnd))
}

getYearWeek <- function(date){
  date <- ISOweek(date)
  paste0(str_sub(date,1,4),
         str_pad(str_sub(date, 7, 9),width = 2, pad = "0", side = "left"))
}

getYearMonth <- function(date){
  return(paste0(str_sub(date, 1, 4), str_sub(date, 6, 7)))
}

sumNA <- function(...){
  return(sum(..., na.rm = TRUE))
}

meanNA <- function(...){
  return(mean(..., na.rm = TRUE))
}

str_detect_names <- function(dataset, pattern){
  auxVar <- names(dataset)[stringr::str_detect(names(dataset), pattern)]
  return(auxVar)
}


CJ.dt <- function(X, Y){
  k <- NULL
  X <- X[, c(k = 1, .SD)]
  setkey(X, k)
  Y <- Y[, c(k = 1, .SD)]
  setkey(Y, NULL)
  X[Y, allow.cartesian = TRUE][, `:=`(k, NULL)]
}

#---------------------------------------------------------------------------------------

`%gn%` <- function(x, y) {
  grep(y, ignore.case = T, x = names(x))
}

#' Wrapper for grepping values. Is NOT case sensitive
#'
#' @param x String vector
#' @param y Pattern
#'
#' @return Elements of string that fit the pattern
#' @export
#'
#' @examples
#' 
#' c("hola", "adios", "cocacola") %gv% "ola"
#' 
`%gv%` <- function(x, y) {
  grep(y, ignore.case = T, x = x, value = T)
}

`%g%` <- function(x, y) {
  grep(y, ignore.case = T, x = x)
}

#' Wrapper for paste0. Easy, simple and fast way to concatenate two strings.
#'
#' @param x
#' @param y
#'
#' @return 
#' @export
#'
#' @examples
`%+%` <- function(x, y) {
  paste0(x,y)
}

#' Conditional cat
#'
#' @param condFlag If TRUE, message in printed, otherwise not
#' @param ... Message
#'
#' @return
#' @export
#'
#' @examples
cond_cat <- function(condFlag = TRUE, ...){
  if(condFlag){
    cat(...)
  }
}

#Preliminary: load dependencies manually
needed <- c("RcppRoll", "optiRum")
for(x in needed){
  if(!x %in% installed.packages()){
    print(x)
    install.packages(x)  
  }
  library(x, character.only = T)
}


#' Expand by periods function
#' Adds to a data table columns that represent the past value, rolling past average or ratios of other columns.
#' IMPORTANT NOTE: please notice that if doGrid == TRUE (which is by default), the outputted data table will have more rows that the input one and may be required to filter them afterwards.
#'
#' @param dt data.table to expand. The function will modify it
#' @param colsToExpand Columns that will be expanded
#' @param nPeriods Number of periods that will be expanded (admits several values on a vector on a single call)
#'                 Positive values represent past expansions and negative values represent future expansions
#' @param timeSeriesIDcolNames Name of the column (or columns) that contains the time series identifier (e.g client ID). 
#' @param periodColName Name of the column that contains the period. The natural R order of this column should be chronological (large values the latest)
#' @param expandMethods How the columns will be expanded, admits one or more of: "shift", "mean", "median", "min", "max", "prod", "sum", "sd" and "var"
#' @param includeCurrentPeriod Whether the expanded aggregatons will include info about the current period or not. 
#'                             Does not apply for the "shift" expanding method
#' @param colsToRatio Columns of which the ratio will be computed. Ratio is currentValue/expandedValue
#' @param methodsToRatio For which expandMethods the ratios should be computed. 
#' @param periodsToRatio For which periods the ratios should be computed. 
#' @param doGrid If TRUE, synthetic rows will be created with all combinations of client IDs and periods. This way, if a period is missing for a client the aggregation will be performed with an NA for that period.
#'               If FALSE, the n most recent periods will be expanded independently of any temporal gaps.
#'               IMPORTANT NOTE: please notice that if set to TRUE, the outputted data table will have more rows that the input one and may be required to filter them afterwards.
#' @param doSort The data.table must be sorted by (timeSeriesIDcolNames, periodColName) for the expanding to work. If the data is already sorted this can be called with FALSE and save some time. 
#' @param suffixNewVars Suffix that will be used to name expanded variables, the default values are internally handled to support negative periods and should be fine in most cases
#' @param suffixRatios Suffix that will be used to name the ratios
#' @param verbose If TRUE, prints some information about its execution
#' @param ... Other parameters to pass to aggregating functions (e.g na.rm)
#'
#' @return Returns the provided data table with the added expanded columns
#' @export
#'
#' @examples
#' 
#' 
#'testDT <- data.table(clientID = c(rep("Client1", 5), rep("Client2", 4)),
#'                     month_id = c(seqMonth("201701", "201705"), setdiff(seqMonth("201701", "201705"), "201704")),
#'                     consumption = c(c(0,1,1,2,0), c(8,7,2,1)),
#'                     billing = c(c(10,12,7,NA,0), c(0,0,0,1))
#')
#'
#'print(expandByPeriods(testDT,
#'                colsToExpand = c("consumption", "billing"),
#'                nPeriods = c(2,3),
#'                timeSeriesIDcolNames = "clientID",
#'                periodColName = "month_id",
#'                colsToRatio = c("consumption", "billing"),
#'                na.rm = T
#'))
#' 
#'testDT <- data.table(clientID = c(rep("Client1", 5), rep("Client2", 4)),
#'                     prodID = c(rep("Prod1", 3), rep("Prod2", 6)),
#'                     month_id = c(seqMonth("201701", "201705"), setdiff(seqMonth("201701", "201705"), "201704")),
#'                     consumption = c(c(0,1,1,2,0), c(8,7,2,1)),
#'                     billing = c(c(10,12,7,NA,0), c(0,0,0,1))
#')
#'
#'print(expandByPeriods(testDT,
#'                      colsToExpand = c("consumption", "billing"),
#'                      nPeriods = c(2,3),
#'                      timeSeriesIDcolNames = c("clientID", "prodID"),
#'                      periodColName = "month_id",
#'                      colsToRatio = c("consumption", "billing"),
#'                      na.rm = T
#'))  
#' 
#' 
expandByPeriods <- function(dt,
                            colsToExpand,
                            nPeriods,
                            timeSeriesIDcolNames,
                            periodColName = "month_id",
                            expandMethods = "mean",
                            includeCurrentPeriod = TRUE,
                            colsToRatio = c(),
                            methodsToRatio = expandMethods,
                            periodsToRatio = nPeriods,
                            doGrid = TRUE,
                            doSort = TRUE,
                            suffixNewVars = ifelse(expandMethods == "shift", "_p", "_" %+% expandMethods %+% "Prev"),
                            suffixRatios = ifelse(methodsToRatio == "shift", "_p", "_" %+% methodsToRatio %+% "Prev") %+% "Ratio",
                            verbose = TRUE,
                            ...){
  
  #Error control
  if(doGrid == TRUE & doSort == FALSE){
    warning("doSort should not be FALSE when doGrid = TRUE because doing the grid may alter the table order. doSort will be set to TRUE")
    doSort <- TRUE
  }
  
  possibleMethods <- c("shift", "mean", "median", "min", "max", "prod", "sum", "sd", "var")
  badMethods <- setdiff(expandMethods, possibleMethods)
  if(length(badMethods) != 0){
    stop("expandMethods " %+% paste(badMethods, collapse = ", ") %+% " not recognized")  
  }
  
  if(length(setdiff(methodsToRatio, expandMethods)) != 0){
    warning("Ratios won't be computed for methods that are not in expandMethods, please add them in expandMethods")
  }
  if(length(setdiff(periodsToRatio, nPeriods)) != 0){
    warning("Ratios won't be computed for periods that are not in nPeriods, please add them in nPeriods")
  }
  
  #Create a grid of all cilent and period combinations to fill time gaps in the data
  if(doGrid){
    cond_cat(verbose, "Creating grid...\n")
    tic()
    
    #There are two methods to do the grid
    if(length(timeSeriesIDcolNames) == 1){
      #CJ function does a faster grid but can add too many extra rows when there are more than one column to identify the time series.
      #That is because it takes all possible combinations even if they dont exist beforehand 
      #This is done with do.call because that way it works for more than 1 time series ID column even though this section is only run when there's 1 column
      dt.grid <- do.call(CJ, lapply(c(timeSeriesIDcolNames, periodColName),
                                    function(colName){ dt[, unique(get(colName))]}))
      
    }else{
      #CJ.dt is slower (30% more time in preliminary tests) but allows us to consider only time series IDs columns combinations that exist on the dataset
      dt.grid <- CJ.dt(unique(dt[, timeSeriesIDcolNames, with=FALSE]),
                       unique(dt[, .(get(periodColName))]))
    }
    names(dt.grid) <- c(timeSeriesIDcolNames, periodColName) 
    
    dt <- dt[dt.grid, on = c(timeSeriesIDcolNames, periodColName)]
    toc(quiet = !verbose)
  }
  
  #Sort the table so that we expand the correct periods of time
  if(doSort){
    cond_cat(verbose, "Sorting table...\n")
    tic()
    setorderv(dt,
              c(timeSeriesIDcolNames, periodColName),
              c(rep(1, length(timeSeriesIDcolNames)), 1))
    toc(quiet = !verbose)
  }else{
    warning("Calling expandByPeriods with doSort = FALSE will generate a corrupt output if the data was not sorted beforehand")
  } 
  
  cond_cat(verbose, "Expanding periods...\n")
  tic()
  for(currentMethod in expandMethods){
    #Get the name of the function we will use to expand the periods
    if(currentMethod == "shift"){
      #Use shift function if we just want to shift a column
      expandFunName <- "shift" 
    }else{
      #For rolling aggregates, use the corresponding function from the RcppRoll package
      expandFunName <- "roll_" %+% currentMethod 
    }
    
    for(currentPeriod in nPeriods){
      #Names on the columns we will create. We need to expand to compute ratios as well 
      currentSuffix <- suffixNewVars[currentMethod == expandMethods]
      #If the default suffixes are being used and the expand is negative (adding forward info), change the suffixes to reflect that they're forward
      if(all(suffixNewVars == ifelse(expandMethods == "shift", "_p", "_" %+% expandMethods %+% "Prev")) & currentPeriod < 0){
        currentSuffix <- str_replace_all(currentSuffix, "Prev", "Fwd")
        currentSuffix <- str_replace_all(currentSuffix, "_p", "_f")
      }
      
      #We need to expand also columns of which we only need the ratio in order to compute the ratio
      colsToExpand_all <- unique(c(colsToExpand, colsToRatio))
      expandedColsNames <- colsToExpand_all %+% currentSuffix %+% abs(currentPeriod)
      
      #Expand the periods
      if(expandFunName == "shift"){
        dt[, (expandedColsNames) := lapply(.SD, expandFunName, n = abs(currentPeriod), type = ifelse(currentPeriod >= 0 , "lag", "lead")), .SDcols = colsToExpand_all]
      }else{
        dt[, (expandedColsNames) := lapply(.SD, expandFunName, n = abs(currentPeriod), fill = NA, na.rm = TRUE, align = ifelse(currentPeriod >= 0 , "right", "left"), ...), .SDcols = colsToExpand_all]
      }
      
      #Shift the newly created vars if the current period should not be included 
      if(!includeCurrentPeriod & expandFunName != "shift"){
        dt[, (expandedColsNames) := lapply(.SD, shift, n = 1, type = ifelse(currentPeriod >= 0 , "lag", "lead")), .SDcols = expandedColsNames]
      }
      
      #Set rows that dont have enough past periods to expand them to NA.
      #If there are several columns that identify the time series ID create an auxiliary ID on a single column
      #This makes it easier to put the NAs on the new columns when any of the time series ID columns changes
      if(length(timeSeriesIDcolNames) > 1){
        dt[, auxTimeSeriesID_EBP := Reduce(function(x,y){paste(x, y, sep = "__EBPsep__")}, .SD), .SDcols = timeSeriesIDcolNames]
      }else{
        dt[, auxTimeSeriesID_EBP := get(timeSeriesIDcolNames)]
      }
      
      dt[auxTimeSeriesID_EBP != shift(auxTimeSeriesID_EBP,
                                      n = abs(currentPeriod) - ifelse(expandFunName == "shift" | !includeCurrentPeriod, 0, 1),
                                      type = ifelse(currentPeriod >= 0 , "lag", "lead"))
         , (expandedColsNames) := NA]
      
      #Delete the auxiliary time series ID that we created
      dt[, auxTimeSeriesID_EBP := NULL]
      
      #Compute the ratios when corresponds
      if(currentMethod %in% methodsToRatio & currentPeriod %in% periodsToRatio){
        currentSuffixRatios <- suffixRatios[currentMethod == methodsToRatio]
        #If the default suffixes are being used and the expand is negative (adding forward info), change the suffixes to reflect that they're forward
        if(all(suffixRatios == ifelse(methodsToRatio == "shift", "_p", "_" %+% methodsToRatio %+% "Prev") %+% "Ratio") & currentPeriod < 0){
          currentSuffixRatios <- str_replace_all(currentSuffixRatios, "Prev", "Fwd")
          currentSuffixRatios <- str_replace_all(currentSuffixRatios, "_p", "_f")
        }
        
        for(colName in colsToRatio){
          ratioColName <- colName %+% currentSuffixRatios %+% abs(currentPeriod)
          dt[, (ratioColName) := get(colName) / get(colName %+% currentSuffix %+% abs(currentPeriod))]
          
          #If for that variable only the ratio and not the expand was required, remove the expand
          if(!colName %in% colsToExpand){
            dt[, (colName %+% currentSuffix %+% abs(currentPeriod)) := NULL]
          }
        }
      }
    }
  }
  toc(quiet = !verbose)
  
  return(dt)
}

#' Creates a sequence of months in format yyyymm similar to seq for integers
#'
#' @param from starting month of the sequence
#' @param to last month of the sequence
#' @param by number of months between each sequence element
#'
#' @return a sequence of months in the format yyyymm
#' @export
#'
#' @examples
seqMonth <- function(from, to, by = 1){
  fromDate <- as.Date(from %+% "01", "%Y%m%d")
  toDate <- as.Date(to %+% "01", "%Y%m%d")
  s <- seq(fromDate, toDate, by = by %+% " months")
  return(format(s, "%Y%m"))
}

seqDays <- function(from, to, by = 1){
  fromDate <- as.Date(as.character(from), "%Y%m%d")
  toDate <- as.Date(as.character(to), "%Y%m%d")
  s <- seq(fromDate, toDate, by = by %+% " days")
  newDates <- str_replace_all(s, "-", "")
  return(newDates)
}

#' Calculate most important model metrics performance for a categorical model 
#' @param target a numeric vector that contains the target
#' @param scores a numeric vector with the model scores
#' @param step granularity of the percentiles that will be computed (a value of 1 computes all 100 percentiles)
#' @param file name of the file where the output will be saved
#' @param outputFormat either "xlsx" or "csv"
#' @param ... other parameters to pass to the writing function
#' @return Returns the report of results
#'
#' @author McKinsey Analytics Madrid Hub
#'
#' @examples
#' 
#' @export

modelPerformanceByPercentile <- function(target, 
                                         scores,
                                         step = 0.1,
                                         file = NULL,
                                         outputFormat = ifelse(file %like% "xlsx", "xlsx", "csv"),
                                         ...
){
  
  dt <- data.table(predicted = scores, target = target)
  
  setorder(dt, -predicted)
  totalObservations <- nrow(dt)
  totalPositiveCases <- sum(dt$target == 1)
  meanChurn <- mean(dt$target)
  
  dt$ID <- (1:totalObservations)/totalObservations * 100
  Percentile <- seq(step, 100, by = step)
  
  if (max(Percentile) < 100) {
    Percentile <- c(Percentile, 100)
  }
  
  dt$Bin <- findInterval(dt$ID, Percentile, rightmost.closed = F, left.open = T)
  
  dt <- dt[, .(Clientes = .N, PositiveCases = sum(target == 1),
               SegmentObservations = .N,
               SegmentPositiveCases = sum(target == 1),
               SegmentUplift = mean(target)/meanChurn,
               SegmentRecall = sum(target == 1)/totalPositiveCases,
               SegmentPrecision = mean(target)),
           by = .(Bin)]  
  
  dt <- cbind(dt, Percentile)
  dt$Bin <- NULL
  
  dt$AccumulatedObservations <- dt$Clientes %>% cumsum
  dt$AccumulatedPositiveCases <- dt$PositiveCases %>% cumsum
  dt$AccumulatedRecall <- dt$AccumulatedPositiveCases/totalPositiveCases
  
  dt$AccumulatedPrecision <- dt$AccumulatedPositiveCases/dt$AccumulatedObservations
  dt$AccumulatedUplift <- dt$AccumulatedPrecision/meanChurn
  
  dt$GINI <- gini(target = target, scores)
  
  #Set order columns
  names.order <- c("Percentile", "AccumulatedObservations", "AccumulatedPositiveCases", 
                   "AccumulatedPrecision", "AccumulatedRecall", "AccumulatedUplift",
                   "SegmentObservations", "SegmentPositiveCases", "SegmentPrecision", 
                   "SegmentRecall", "SegmentUplift", "GINI")
  dt <- dt[, names.order, with = F]
  
  if(!is.null(file)){
    if(outputFormat %like% "excel|xlsx"){
      write.xlsx2(dt, file = file, row.names = FALSE, ...)
    }else{
      fwrite(dt, file = file, ...)
    }
  }
  
  return(invisible(dt))
}


#' Computes the gini metric for a set of predictions for a categorigal target
#'
#'
#' @param target 0s or 1s to predict
#' @param pred prediction scores
#'
#' @return the gini metric scaled from 0 to 100
#' @export
#'
#' @examples
gini <- function(target, pred){
  # if(any(is.na(pred))){
  #   warning("found NAs in pred. Ignoring those values.")
  #   target <- target[!is.na(pred)]
  #   pred <- pred[!is.na(pred)]
  # }
  # if(!all(sort(unique(target)) %in% c(0,1))) {
  #   return(as.numeric(NA))
  # }
  # if(uniqueN(target) != 2) {
  #   return(as.numeric(NA))
  # }
  
  auc <- unlist(performance(prediction(pred, target),'auc')@y.values)
  
  return(100 * (auc-0.5)*2)
}


mae <- function(obs, pred, na.rm=FALSE){
  mean(abs(obs - pred), na.rm = na.rm)
}


#' Eval metric to be passed to xgboost as a parameters so that it displays the mae while training
#' Should not be called manually
#' 
#' @param preds 
#' @param dtrain 
#'
#' @return 
#' @export
#'
#' @examples
#' 
#'  parameters <-  list(booster         = "gbtree",
#'                      objective       = "reg:linear",
#'                      eval_metric     = mae_evalxgb)
mae_evalxgb <- function(preds, dtrain) {
  labels <- getinfo(dtrain, "label")
  return(list(metric = "mae", value = mae(labels, preds)))
}


#' Eval metric to be passed to xgboost as a parameters so that it displays the uplift while training
#' The percentile for which the uplift will be computed can be set by changing the global variable uplift_evalxgb_quantileGlobal
#' Should not be called manually
#' 
#' @param preds 
#' @param dtrain 
#'
#' @return 
#' @export
#'
#' @examples
#' 
#'  uplift_evalxgb_quantileGlobal <<- 0.01
#'  parameters <-  list(booster         = "gbtree",
#'                      objective       = "reg:linear",
#'                      eval_metric     = uplift_evalxgb)
#'                      
# uplift_evalxgb_quantileGlobal <<- 0.01

uplift_evalxgb <- function(preds, dtrain) {
  labels <- getinfo(dtrain, "label")
  results <- data.table(preds, labels)
  setorder(results, -preds)
  topPercent <- results[1:(round(nrow(results)*uplift_evalxgb_quantileGlobal, 0)), ]
  percentageChurnersTop <- mean(topPercent$labels)
  percentageChurners <- mean(labels)
  uplift <- percentageChurnersTop/percentageChurners
  percentage <- 100*uplift_evalxgb_quantileGlobal
  return(list(metric = "Uplift-Top" %+% percentage %+% "perc... ", value = uplift))
}
substrRight <- function(x, n){
  substr(x, nchar(x)-n+1, nchar(x))
}

comparativeMerge <- function(dt1, dt2, table1Name, table2Name,  id_1, id_2){
  dt1_unique <- uniqueN(dt1[, get(id_1)])
  dt2_unique <- uniqueN(dt2[, get(id_2)])
  dt1_in_dt2 <- uniqueN(dt2[get(id_2) %in% unique(dt1[, get(id_1)]), get(id_2)])
  dt2_in_dt1 <- uniqueN(dt1[get(id_1) %in% unique(dt2[, get(id_2)]), get(id_1)])
  summaryTable <- data.table(table1 = table1Name, colID1 = id_1, table2 = table2Name, colID2 = id_2,
                             dt_1_unique = dt1_unique, dt_2_unique = dt2_unique,
                             dt1_in_2 = dt1_in_dt2, dt2_in_1 = dt2_in_dt1)
  return(summaryTable)
}

asignarTramoMora <- function(dt, numTramos = 7, stepTramo = 30 , colMora = "DIAS"){
  tramos <- seq(0, numTramos*stepTramo - stepTramo, by = stepTramo)
  dt[, tramoMora := cut(as.numeric(get(colMora)), breaks = tramos, labels = paste0("<= ", tramos[2:length(tramos)]), include.lowest = F, right = T)]
  dt[as.numeric(get(colMora)) <= 0 , tramoMora := "0"]
  dt[as.numeric(get(colMora)) > max(tramos), tramoMora := paste0("> ", max(tramos))]
  return(dt)
}

analisisSeleccionTarget <- function(dt, colProductos = "NOMBRE_PRODUCTO",
                                    colDiasMora = "maxDiasMora",
                                    stepTramo = 1,
                                    maxDiasMora = 120,
                                    colExposure = "maxCAPITALMora",
                                    plot = T#todav?a sin implementar
){
  
  escalaTargets <- seq(stepTramo, maxDiasMora, by = stepTramo)
  resumenImpactoTarget <- data.table()
  periods <- unique(dt[,PERIODO])
  for (period in periods){
    print(period)
    for (producto in unique(dt[, get(colProductos)])){
      print(producto)
      for (i in escalaTargets){
        print("Target: " %+% i)
        numMora <- dt[get(colProductos) == producto & PERIODO == period, .N]
        capitalMora <- dt[get(colProductos) == producto & PERIODO == period, sum(get(colExposure))]
        dt[get(colProductos) == producto & PERIODO == period, target := ifelse(maxDiasEnMora >= i, 1, 0)]
        numTarget <- dt[get(colProductos) == producto & PERIODO == period, sum(target)]
        capitalMoraTarget <- dt[get(colProductos) == producto & target == 1 & PERIODO == period, sum(get(colExposure))]
        temp <- data.table(period = period,
                           target = i, PROD = producto, sumTarget = numTarget,
                           percTotal = round(numTarget/numMora*100, 2),
                           capitalEnMora = capitalMoraTarget,
                           percCapital = round(capitalMoraTarget/capitalMora*100, 2))
        resumenImpactoTarget <- rbind(resumenImpactoTarget, temp)
      }
    }
    for (i in escalaTargets){
      numMora <- dt[PERIODO == period, .N]
      capitalMora <- dt[PERIODO == period, sum(get(colExposure))]
      dt[PERIODO == period, target := ifelse(maxDiasEnMora >= i, 1, 0)]
      numTarget <- dt[PERIODO == period, sum(target)]
      capitalMoraTarget <- dt[target == 1 & PERIODO == period, sum(get(colExposure))]
      temp <- data.table(period = period,
                         target = i, PROD = "TODOS", sumTarget = numTarget,
                         percTotal = round(numTarget/numMora*100, 2),
                         capitalEnMora = capitalMoraTarget,
                         percCapital = round(capitalMoraTarget/capitalMora*100, 2))
      
      resumenImpactoTarget <- rbind(resumenImpactoTarget, temp)
    }
  }
  return(resumenImpactoTarget)
}


#' Subtract dates in format yyyymm
#'
#' @param d1 
#' @param d2 
#'
#' @return The difference in months between d1 and d2
#' @export
#'
#' @examples
monthSub <- function(d1, d2) {
  m1 <- as.numeric(substr(as.character(d1), 5, 6))
  m2 <- as.numeric(substr(as.character(d2), 5, 6))
  y1 <- as.numeric(substr(as.character(d1), 1, 4))
  y2 <- as.numeric(substr(as.character(d2), 1, 4))
  return(12 * (y1-y2) + (m1-m2))
}


#' Adds a number of months from a date in format yyyymm
#'
#' @param d Date to be subtracted
#' @param n Number of months to add (admits negative values)
#'
#' @return A vactor of months in the format yyyymm
#' @export
#'
#' @examples
monthAddInteger <- function(d, n) {
  year <- as.numeric(str_sub(d, 1, 4))
  mon <- as.numeric(str_sub(d, 5, 6))
  
  yearsPassed <- floor((n + mon - 1) / 12)
  endMonth <- ((n + mon - 1) %% 12) + 1 
  return((year + yearsPassed) %+% str_pad(endMonth, width = 2, side = "left", pad = "0"))
}

dayAddInteger <- function(d, n) {
  date <- as.Date(as.character(d), "%Y%m%d")
  newDate <- date + n
  stringNewDate <- str_replace_all(newDate, "-", "")
  return(stringNewDate)
}


cleanNumeric <- function(x){
  return(x %>% str_replace_all(",", ".") %>% as.numeric())
}

saveProcessedTableByPeriod <- function(dt, path = preparedDataPath, source, periodIDvarname = "YEAR_MONTH"){
  for(mon in unique(dt[, get(periodIDvarname)])){
    outputFilename <-source %+% "_" %+% mon %+% ".rds"
    saveRDS(dt[get(periodIDvarname) == mon], path %+% "/" %+% outputFilename)
  }
}

getProcessedTableByPeriod <- function(dataSource, periods = "all", path = preparedDataPath){
  if(length(periods) == 0){
    fileNames <- c()
  }else if(any(periods == "all")){
    fileNames <- list.files(path, dataSource %+% "_" %+% "\\d{6,8}" %+% ".rds", full.names = TRUE)
  }else{
    fileNames <- path %+% "/" %+% dataSource %+% "_" %+% periods %+% ".rds"
  }
  dt <- lapply(fileNames, function(fileName){
    if(file.exists(fileName)){
      return(readRDS(fileName))  
    }else{
      warning("Trying to read file: " %+% fileName %+% " but it doesn't exist.")
      return(NULL)
    }
  })
  dt <- rbindlist(dt, use.names = TRUE, fill = TRUE)
  return(invisible(dt))
}

getFuturePeriod <- function(PERIOD, numberOfPeriods){
  # Auxiliar function that given a PERIOD, returns the numberOfPeriods future
  # For example: if PERIOD is 201611 and numberOfPeriods is 2, this function will return
  # the period 201701
  # This function recursively calls getNextPeriod()
  aux <- PERIOD
  
  stopifnot(is.numeric(numberOfPeriods))
  
  while(numberOfPeriods != 0){
    aux <- getNextPeriod(aux)
    numberOfPeriods <- numberOfPeriods - 1
  }
  
  return(aux)
}

getNextPeriod <- function(PERIOD){
  # Auxiliar function that given a PERIOD, returns the next period
  # For example: if PERIOD is 201611 this function will return 201612
  
  year <- substr(PERIOD, 1, 4) %>% as.numeric
  month <- substr(PERIOD, 5, 6) %>% as.numeric
  
  nextMonth <- ifelse(month == 12, 1, month + 1)
  nextMonth <- str_pad(nextMonth, 2, side = "left", pad = "0")
  
  nextYear <- ifelse(month == 12, year + 1, year)
  
  nextPERIOD <- paste0(nextYear, nextMonth)
  return(nextPERIOD)
}

getPreviousPeriod <- function(PERIOD){
  # Auxiliar function that given a PERIOD, returns the previous period
  # For example: if PERIOD is 201611 this function will return 201610
  
  year <- substr(PERIOD, 1, 4) %>% as.numeric
  month <- substr(PERIOD, 5, 6) %>% as.numeric
  
  previousMonth <- ifelse(month == 1, 12, month - 1)
  previousMonth <- str_pad(previousMonth, 2, side = "left", pad = "0")
  
  previousYear <- ifelse(month == 1, year - 1, year)
  
  previousPERIOD <- paste0(previousYear, previousMonth)
  return(previousPERIOD)
}

getPastPeriod <- function(PERIOD, numberOfPeriods){
  # Auxiliar function that given a PERIOD, returns the numberOfPeriods past
  # For example: if PERIOD is 201611 and numberOfPeriods is 2, this function will return
  # the period 201609
  # This function recursively calls getPreviousPeriod()
  
  aux <- PERIOD
  
  stopifnot(is.numeric(numberOfPeriods))
  
  while(numberOfPeriods != 0){
    aux <- getPreviousPeriod(aux)
    numberOfPeriods <- numberOfPeriods - 1
  }
  
  return(aux)
}



saveTableSummariesByPeriod <- function(dt, path = dataQualityAnalysisPath, source, periodIDvarname = "YEAR_MONTH"){
  for(mon in unique(dt[, get(periodIDvarname)])){
    outputFilename <- source %+% "_" %+% mon %+% ".csv"
    SummariseTable(dt[get(periodIDvarname) == mon], write = TRUE, outputName = outputFilename, outputDir = path)
  }
}

#' Calculate most important model metrics performance for a collections model
#' @param dir string that gives the path where the final csv file will be saved.
#' @param validationSet data.frame or data.table object with the target
#'   variable and the associated scores (scores are optional).
#' @param target a string with the name of the column target variable inside 
#'   the validationSet
#' @param scores if a string, gives the name of the scores inside 
#'   validationSet. If numeric, vector with the scores associated to the
#'   registers in validationSet (has to be in the same order).
#' @param step increment to apply each iteration (numeric)
#' @param title title string of the plot and the name of the csv file.
#' @param sep separator between columns in the csv file.
#' @param dec decimal separator in the csv file
#' @param capitalCol column with containing the capital captured
#' @return Silently saves a csv file with the generated output results.
#'
#' @author McKinsey Analytics Hub
#'
#' @examples
#' ModelPerformanceByPercentileCollections(dir = "Reports/", validationSet = test, 
#' target = "TARGET", scores = "PREDICTION", step = 0.1, title = "Example")
#' @export

ModelPerformanceByPercentileCollections <- function(dir = "~", validationSet, target,
                                                    capitalCol, 
                                                    scores, step = 0.1, title = "test",
                                                    sep = ";", dec = ","){
  
  neededLibraries <- c("data.table","dplyr")
  for( x in neededLibraries){
    library(x, character.only = T)
  }
  
  tarPos <- which(names(validationSet) == target)
  if (class(scores) == "character"){
    scoPos <- which(names(validationSet) == scores)
    dt <- data.table(predicted = validationSet[[scoPos]], target = validationSet[[tarPos]], capital = validationSet[[capitalCol]])
  } else if (class(scores) == "numeric") {
    dt <- data.table(predicted = scores, target = validationSet[[tarPos]], capital = validationSet[[capitalCol]])
  } else {
    stop(paste0("Cannot recognise the class of the argument scores.",
                "Currently only character and numeric supported."))
  }
  
  setorder(dt, -predicted)
  totalObservations <- nrow(dt)
  totalPositiveCases <- sum(dt$target == 1)
  dt[, rolledCapital := target*capital] 
  totalRolledCapital <- dt[, sum(rolledCapital, na.rm = T)]
  meanChurn <- mean(dt$target)
  
  dt$ID <- (1:totalObservations)/totalObservations * 100
  Percentile <- seq(step, 100, by = step)
  
  if (max(Percentile) < 100) {
    Percentile <- c(Percentile, 100)
  }
  
  dt$Bin <- findInterval(dt$ID, Percentile, rightmost.closed = T, left.open = F)
  
  dt <- dt[, .(Clientes = .N, 
               PositiveCases = sum(target == 1),
               SegmentRolledCapital = sum(rolledCapital, na.rm = T),
               SegmentObservations = .N,
               SegmentPositiveCases = sum(target == 1),
               SegmentUplift = mean(target)/meanChurn,
               SegmentRecall = sum(target == 1)/totalPositiveCases,
               SegmentRolledCapitalPrec = sum(rolledCapital, na.rm = T)/totalRolledCapital,
               SegmentPrecision = mean(target)),
           by = .(Bin)]  
  
  dt <- cbind(dt, Percentile)
  dt$Bin <- NULL
  
  dt$AccumulatedObservations <- dt$Clientes %>% cumsum
  dt$AccumulatedPositiveCases <- dt$PositiveCases %>% cumsum
  dt$AccumulatedRolledCapital <- dt$SegmentRolledCapital %>% cumsum
  dt$AccumulatedRecall <- dt$AccumulatedPositiveCases/totalPositiveCases
  
  
  dt$AccumulatedPrecision <- dt$AccumulatedPositiveCases/dt$AccumulatedObservations
  dt$AccumulatedUplift <- dt$AccumulatedPrecision/meanChurn
  dt$AccumulatedRolledCapitalPrec <- dt$AccumulatedRolledCapital/totalRolledCapital
  dt$GINI <- gini(target = validationSet[[tarPos]], validationSet[[scoPos]])
  #Set order columns
  names.order <- c("Percentile", "AccumulatedObservations", "AccumulatedPositiveCases", 
                   "AccumulatedRolledCapital", "AccumulatedRolledCapitalPrec",
                   "AccumulatedPrecision", "AccumulatedRecall", "AccumulatedUplift",
                   "SegmentObservations", "SegmentPositiveCases", "SegmentRolledCapital",
                   "SegmentRolledCapitalPrec",
                   "SegmentPrecision", 
                   "SegmentRecall", "SegmentUplift", "GINI")
  dt <- dt[, names.order, with = F]
  
  if(dir.exists(dir)){
    fwrite(dt, file = file.path(dir, paste0("Uplift_", title, ".csv")), row.names = FALSE, sep = sep, dec = dec)
  }else{
    stop(paste0("Cannot write the output on disk. Please, review the provided dir path"))
  }
}

#' Draw ROC curve including a logo
#' 
#' \code{DrawROCCurve} Saves a ROC curve with a logo in a png file
#' 
#' @param dir string that gives the path where the png file will be saved.
#' @param validationSet data.frame or data.table object with the target
#'   variable and the associated scores (scores are optional).
#' @param target a string with the name of the column target variable inside 
#'   the validationSet
#' @param scores if a string, gives the name of the scores inside 
#'   validationSet. If numeric, vector with the scores associated to the
#'   registers in validationSet (has to be in the same order).
#' @param colours strings that set the colours to be used in the plot for, in
#'   this order, ROC curve and line of random model.
#' @param title string to add to the title of the plot and the name of the png
#'   file.
#' @param maxSize integer that sets the maximum number of points to be
#'   considered when plotting. This is needed because ggplot2 takes a
#'   huge amount of time for big data sets.
#' @param logo string that contains the path to the png file with the logo.
#' @param ... Other parameters passed on to \code{ggsave}.
#' 
#' @return Silently saves a png file with the generated ROC curve.
#'
#' @author Juan Rosco, \email{Juan_Rosco@@mckinsey.com}
#'
#' @examples
#' set.seed(2529)
#' nR <- 2e+05
#' D.ex <- rbinom(nR, size = 1, prob = 0.5)
#' M1 <- rnorm(nR, mean = D.ex, sd = 0.65)
#' M2 <- rnorm(nR, mean = D.ex, sd = 1.5)
#' test <- data.frame(D = D.ex, D.str = c("Healthy", "Ill")[D.ex + 1], M1 = M1,
#'                    M2 = M2, straingsAsFactors = FALSE)
#' DrawROCCurve(validationSet = test, target = "D", scores = "M1",
#'                title = "example", logo = system.file("img", "Rlogo.png",
#'                                                     package="png"),
#'                height = 7, width = 8)
#' @export

DrawROCCurve <- function(dir = NULL, validationSet, target, scores, colours = c("#EC0D06", "#000000"), 
                         title = "test", maxSize = 750000, logo = NULL, ...){
  
  neededLibraries <- c("ggplot2", "data.table", "ROCR", "png","grid")
  for( x in neededLibraries){
    library(x, character.only = T)
  }
  
  if(!is.null(logo)){
    img <- readPNG(logo)
  }else{
    img <- NULL
  }
  
  tarPos <- which(names(validationSet) == target)
  if (class(scores) == "character"){
    scoPos <- which(names(validationSet) == scores)
    perf <- performance(pred <- prediction(validationSet[[scoPos]], validationSet[[tarPos]]),
                        'tpr', 'fpr')
  } else if (class(scores) == "numeric") {
    perf <- performance(pred <- prediction(scores, validationSet[[tarPos]]),
                        'tpr', 'fpr')
  } else {
    stop(paste0("Cannot recognise the class of the argument scores. ",
                "Currently only character and numeric supported."))
  }
  
  gini <- (unlist(performance(pred, measure = 'auc')@y.values) - 0.5) * 2
  x <- unlist(perf@y.values)
  y <- unlist(perf@x.values)
  ks <- max (x - y)
  DT <- data.table(False_positive_rate = unlist(perf@x.values),
                   True_positive_rate = unlist(perf@y.values))
  
  if (DT[, .N] > 1e+06){
    set.seed(38476)
    filter_ <- sample(x = 1:DT[, .N], size = maxSize)
    DT <- DT[filter_]
  }
  
  suppressWarnings(
    p <- ggplot(DT, aes(x = False_positive_rate, y = True_positive_rate)) + 
      geom_abline(slope = 1, intercept = 0, color = colours[[2]],
                  linetype = "twodash") +
      geom_line(color = colours[[1]]) + 
      scale_x_continuous(limits = c(0, 1), expand = c(0, 0.00),
                         breaks = seq(0, 1, by = 0.1)) +
      scale_y_continuous(limits = c(0, 1), expand = c(0, 0.00),
                         breaks = seq(0, 1, by = 0.1)) + 
      theme_bw() +
      theme(panel.border = element_rect(fill = NA),
            plot.margin = unit(c(0.5, 0.5, 0.5, 0.5), "cm"),
            plot.caption = element_text(family = "Calibri", face = "italic",
                                        color = "darkgrey", size = 5)) +
      geom_rect(xmin = 0.8, xmax = Inf, ymin = -Inf, ymax = 0.15,
                fill = "white", color = "black", size = 0.25) +
      labs(title = paste0(title, " - ROC Curve"), x = "False positive rate",
           y = "True positive rate",
           caption = "Source: McKinsey Advanced Analytics - Madrid Hub") +
      annotate("text", label = paste0("Gini: ", round(gini*100, 1),"%"), x = 0.9,
               y = 0.1, size = 5, fontface = "bold", family = "Ubuntu") +
      annotate("text", label = paste0("KS: ", round(ks*100, 1),"%"), x = 0.9, 
               y = 0.05, size = 5, fontface = "bold", family = "Ubuntu") +
      
      if(!is.null(img)){
        annotation_raster(img, xmin = 0.8, xmax = 1, ymax = 0.151, ymin = 0.35)
      }else{
        NULL
      }
  )
  
  if (is.null(dir)){
    print(p)
  } else {
    ggsave(plot = p, filename = file.path(dir, paste0("ROC_model_", title, ".png")), ...)  
  }
}

getSource <- function(x){
  str_split(x, pattern = "_")[[1]]  %>% tail(1)
}



saveLogOddsPlot <- function(variable, i, path, pred_contr, xtest){
  logOdds <- as.data.table(pred_contr)[, get(variable)]
  value <- xtest[, get(variable)]
  dt <- data.table(logOdds = logOdds, value = value)
  p <- ggplot(data = dt, aes(value, logOdds, alpha = 0.01)) + geom_point(color="#009E73") +
    ggtitle(paste0("logOdds distribution per ", tolower(variable), " value")) +
    geom_smooth()
  ggsave(plot = p, filename = paste0(path, "/", i, "_",variable, ".png"))  
}
