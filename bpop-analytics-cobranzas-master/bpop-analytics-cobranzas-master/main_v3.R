rm(list=ls())
gc()

rootPath <<- "/home/mck/BPO_CollectionsLibrary/"

# Load utility funcions
source(paste0(rootPath,"genericUtilities.R"))
source(paste0(rootPath,"telegramLibrary.R"))
source(paste0(rootPath,"configEnv.R"))
# Load utility funcions for s3 file management
source(paste0(rootPath,"genericUtilitiesS3.R"))

setWorkspace(rootPath = rootPath)
librariesRequired <- setdiff(librariesRequired, "mlr")
loadLibraries(librariesRequired)

# Read configuratin for storage devices
auxJsonStorage <- jsonlite::read_json(paste0(rootPath,"configStorage.json"))
auxJson <- jsonlite::read_json(paste0(rootPath,"configJson.json"))
awsS3BucketID <- getBucket_S3()
awsCliPath <- auxJsonStorage[["S3_Configuration"]][["AWS_CLI_PATH"]]
ebsRootPath <- auxJsonStorage[["S3_Configuration"]][["EBS_ROOT_PATH"]]
configure_s3(aws_cli_path = awsCliPath,
             aws_S3_Bucket_ID = awsS3BucketID, 
             localEBSPath = ebsRootPath)
auxJson <- jsonlite::read_json(paste0(rootPath,"configJson.json"))
cat("My assinged temp_path is: ", tempPath, fill = T)

# Cargamos las funciones de proyecto especificas
purrr::walk(.x = list.files(paste0(rootPath,"/Scripts"), full.names = T, recursive = T), .f = source)

dailyPeriodsToProcess <- seqDays("20170101", "20180331")
dailyPeriodsToExpand <- seqDays("20170501", "20180331")
monthlyPeriodsToProcess <- seqMonth(201701, 201803)
monthlyPeriodsToExpand <- seqMonth(201704, 201801)

# (1) Procesamos las tablas de caracter diario

library(parallel)
library(doParallel)
cl <- makeCluster(8, type = "PSOCK")
clusterExport(cl, "configEnvironment")
clusterEvalQ(cl, configEnvironment())

clusterExport(cl, "awsCliPath")
clusterExport(cl, "awsS3BucketID")
clusterExport(cl, "ebsRootPath")
clusterExport(cl, "dailyPeriodsToProcess")
clusterExport(cl, "monthlyPeriodsToExpand")
clusterExport(cl, "auxJson")

auxFunction_one <- function(period){
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID,
               localEBSPath = ebsRootPath)
  # messageTelegram(bot, paste0("Iterando periodo ", period))
  tryCatch(processMora(period), error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  
}
auxFunction_two <- function(period){
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID,
               localEBSPath = ebsRootPath)
  # messageTelegram(bot, paste0("Iterando periodo ", period))
  tryCatch(createCustomerBase(period),
           error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
}

auxFunction_two_1 <- function(period){
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID,
               localEBSPath = ebsRootPath)
  # messageTelegram(bot, paste0("Iterando periodo ", period))
  tryCatch(createTargetMaster(period),
           error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
}

auxFunction_two_2 <- function(period){
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID,
               localEBSPath = ebsRootPath)
  # messageTelegram(bot, paste0("Iterando periodo ", period))
  tryCatch(createMoraMaster(period),
           error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
}


auxFunction_three <- function(period){
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID,
               localEBSPath = ebsRootPath)
    # messageTelegram(bot, paste0("Iterando periodo ", period))
    # tryCatch(process_obligaciones(period),
    #          error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
    tryCatch(processTxPasivo(period), 
             error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
    # tryCatch(process_buro(period), 
    #          error = function(err){cat("Error: ", err[['message']], fill = TRUE)})

}
auxFunction_four <- function(period){
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID,
               localEBSPath = ebsRootPath)
  messageTelegram(bot, paste0("Iterando periodo ", period))
    tryCatch(createObligacionesMaster(period),
             error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
    # tryCatch(createTransaccionesMaster(period), 
    #          error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
}
auxFunction_five <- function(period){
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID,
               localEBSPath = ebsRootPath)
  # messageTelegram(bot, paste0("Iterando periodo ", period))
  tryCatch(processBuro(period),
           error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  # tryCatch(createTransaccionesMaster(period), 
  #          error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
}
auxFunction_six <- function(period){
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID,
               localEBSPath = ebsRootPath)
  # messageTelegram(bot, paste0("Iterando periodo ", period))
  tryCatch(processClientes(period),
           error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  # tryCatch(createTransaccionesMaster(period), 
  #          error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
}
auxFunction_seven <- function(period){
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID,
               localEBSPath = ebsRootPath)
  # messageTelegram(bot, paste0("Iterando periodo ", period))
  tryCatch(processContratos(period),
           error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  # tryCatch(createTransaccionesMaster(period), 
  #          error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
}
auxFunction_eight <- function(period){
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID,
               localEBSPath = ebsRootPath)
  # messageTelegram(bot, paste0("Iterando periodo ", period))
  tryCatch(createContratosMaster(period),
           error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  # tryCatch(createTransaccionesMaster(period), 
  #          error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
}
auxFunction_nine <- function(period){
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID,
               localEBSPath = ebsRootPath)
  # messageTelegram(bot, paste0("Iterando periodo ", period))
  tryCatch(createBuroMaster(period),
           error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  # tryCatch(createTransaccionesMaster(period), 
  #          error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
}
auxFunction_ten <- function(period){
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID,
               localEBSPath = ebsRootPath)
  # messageTelegram(bot, paste0("Iterando periodo ", period))
  tryCatch(createTxPasivoMaster(period),
           error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  # tryCatch(createTransaccionesMaster(period), 
  #          error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
}
auxFunction_eleven <- function(period){
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID,
               localEBSPath = ebsRootPath)
  # messageTelegram(bot, paste0("Iterando periodo ", period))
  tryCatch(createMasterTable(period),
           error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
  # tryCatch(createTransaccionesMaster(period), 
  #          error = function(err){cat("Error: ", err[['message']], fill = TRUE)})
}





# parLapply(cl, X = seqDays("20180228", "20180430"), fun = auxFunction_one)
# parLapply(cl, X = seqDays("20180228", "20180430"), fun = auxFunction_two)
# parLapply(cl, X = seqDays("20180228", "20180430"), fun = auxFunction_two_1)
# parLapply(cl, X = seqDays("20180228", "20180430"), fun = auxFunction_two_2)
# parLapply(cl, X = "201804", fun = auxFunction_three)
# parLapply(cl, X = monthlyPeriodsToProcess, fun = auxFunction_four)
# parLapply(cl, X = "201803", fun = auxFunction_five)
# parLapply(cl, X = "201804", fun = auxFunction_six)
# parLapply(cl, X = "201804", fun = auxFunction_seven)
# parLapply(cl, X = "201804", fun = auxFunction_eight)
# parLapply(cl, X = "201803", fun = auxFunction_nine)
# parLapply(cl, X = c("201804"), fun = auxFunction_ten)
parLapply(cl, X = seqDays("20180228", "20180331"), fun = auxFunction_eleven)

# clusterApplyLB(cl, x = monthlyPeriodsToProcess, fun = auxFunction_three)
# clusterApplyLB(cl, x = monthlyPeriodsToExpand, fun = auxFunction_four)

closeAllConnections()
stopCluster(cl)

dir_s3(obligacionesProcessedPath) 
dir_s3(obligacionesExpandedPath) 
