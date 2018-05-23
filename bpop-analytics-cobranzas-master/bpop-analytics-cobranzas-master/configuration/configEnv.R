configEnvironment <- function(){
  .libPaths(c("/home/mck/R/x86_64-redhat-linux-gnu-library/3.4",
              "/usr/lib64/R/library",
              "/usr/share/R/library"))
  rootPath <<- "/home/mck/bpop-analytics-cobranzas/"
  
  # Load utility funcions
  source(paste0(rootPath,"configuration/genericUtilities.R"))
  
  # Load utility funcions for s3 file management
  source(paste0(rootPath,"configuration/genericUtilitiesS3.R"))
  
  setWorkspace(rootPath = rootPath)
  librariesRequired <- setdiff(librariesRequired, "mlr")
  loadLibraries(librariesRequired)
  
  # Read configuratin for storage devices
  auxJsonStorage <- jsonlite::read_json(paste0(rootPath,"configuration/configStorage.json"))
  awsS3BucketID <<- getBucket_S3()
  awsCliPath <- auxJsonStorage[["S3_Configuration"]][["AWS_CLI_PATH"]]
  ebsRootPath <- auxJsonStorage[["S3_Configuration"]][["EBS_ROOT_PATH"]]
  configure_s3(aws_cli_path = awsCliPath,
               aws_S3_Bucket_ID = awsS3BucketID, 
               localEBSPath = ebsRootPath)
  cat("Mi temp_path asignado es: ", tempPath, fill = T)
  
  # Cargamos las funciones de proyecto especificas
  purrr::walk(.x = list.files(osPathJoin(rootPath,"Scripts"), full.names = T, recursive = T), .f = source, encoding = "UTF-8")
  cat("Mi rootpath es:", rootPath, fill = TRUE)
  auxJson <<- jsonlite::read_json(paste0(rootPath,"configuration/configJson.json"))
  
}
