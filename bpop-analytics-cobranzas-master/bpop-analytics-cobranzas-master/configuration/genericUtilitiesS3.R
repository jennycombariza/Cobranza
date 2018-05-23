#' configure_s3
#'
#' @description Configuration function that sets up the environment variables required
#' for the rest of the S3 utilities package to work properly. Must be run at the beginning
#' of the script to enable the rest of the functions.
#'
#' @param aws_cli_path File path address of the aws cli application in Linux
#' @param aws_S3_Bucket_ID ID of the S3 bucket
#' @param rootPath File path address of the location where the temporary files are stored
#' @param AWS_ACCESS_KEY_ID Access key of the S3 bucket (provided by AWS Identity and Access Management through aws configure CLI)
#' @param AWS_SECRET_ACCESS_KEY Secret key of the S3 bucket (provided by AWS Identity and Access Management through aws configure CLI)
#'
#' @return Set up the environment variables for the rest of S3 utilities functions to work
#' @examples
#'configure_s3(aws_cli_path = "/home/user/.local/bin/aws",
#'             aws_S3_Bucket_ID = "bucket-id",
#'             rootPath = "//mnt/shared/")


# To Do: Update documentation, remove access keys

configure_s3 <- function(aws_cli_path, aws_S3_Bucket_ID, localEBSPath = "/dev/shm/"){
  assign("aws_cli", aws_cli_path, envir = as.environment(.GlobalEnv))
  assign("auxBucket", aws_S3_Bucket_ID, envir = as.environment(.GlobalEnv))
  assign("tempPath", paste0(localEBSPath,
                                paste0(system("whoami", intern = TRUE), "_", system("hostname", intern = TRUE), "_", sample(1:1000, 1),  ".temp")),
         envir = as.environment(.GlobalEnv))
}


#' get_key_s3
#'
#' @description Auxiliar function to retrieve the absolute S3 key of a file given
#' it's path and S3 bucket id
#'
#' @param filename Path of the file within the desired S3 bucket
#' @param bucket ID of the S3 bucket. Should be auxBucket defined by default from configure_s3 function
#'
#' @return Absolute S3 key to the filename to be used on other functions
#' @examples
#' get_key_s3(filename = "/PreparedData/Prepared_table_master_201701.csv",
#'            bucket = auxBucket)

get_key_s3 <- function(filename, bucket){
  return(paste0("s3://", bucket, filename))
}


#' fread_s3
#'
#' @description Adaptation of fread function from data.table package to work with S3 buckets
#'
#' @param file Path of the file within the desired S3 bucket
#' @param ... List of additional parameters sent to fread
#'
#' @return A data.table by default. A data.frame when argument data.table=FALSE; e.g. options(datatable.fread.datatable=FALSE)
#' @examples
#' fread_s3(filename = "/PreparedData/Prepared_table_master_201701.csv")
#' fread_s3(filename = "/PreparedData/Prepared_table_master_201701.csv",
#'          colClasses = "character",
#'          nrows = 1000)

fread_s3 <- function(file, ...){
  fileKey <- paste0("s3://", auxBucket, file)
  auxDt <- tryCatch(fread(paste0(aws_cli, " s3 cp ",fileKey, " -"), encoding = "Latin-1",...),
                    error = function(err){})
  return(auxDt)
}

#' readRDS_s3
#'
#' @description Adaptation of readRDS function from base package to work with S3 buckets
#'
#' @param file Path of the file within the desired S3 bucket
#' @param ... List of additional parameters sent to fread
#'
#' @return A R object by default
#' @examples
#' readRDS_s3(file = "/PreparedData/Prepared_table_master_201701.RDS")
#' 

readRDS_s3 <- function(file, ...){
  fileKey <- paste0("s3://", auxBucket, file)
  system(paste0(aws_cli, " s3 cp ", fileKey, " ", tempPath))
  auxDt <- readRDS(tempPath, ...)
  return(auxDt)
}

#' fwrite_s3
#'
#' @description Adaptation of fwrite function from data.table package to work with S3 buckets.
#'
#' @param file Object to be written
#' @param filePath Name and path of file to store the data within the S3 bucket
#' @param ... List of additional parameters sent to fwrite
#'
#' @details The standard solution to write files from R to S3 uses a HTTP PUT request.
#' The issue with this approach is the 5GB limit on part size that AWS S3 imposes.
#' To overcome this limitation, the file is written locally in a temporary location, then moved
#' to S3 using AWS CLI and finally deleted.
#' By using AWS CLI it's also possible to control all the aspects of the copy, such as server side encryption
#'
#' @examples
#' fwrite_s3(file = preparedTableMasterDt,
#'           filepath = "/PreparedData/Prepared_table_master_201701.csv")
#' fwrite_s3(file = preparedTableMasterDt,
#'           filepath = "/PreparedData/Prepared_table_master_201701.csv",
#'           nThread = 64, sep = "|")

fwrite_s3 <- function(file, filePath, ...){
  if(nrow(file) > 0){
    fwrite(file, tempPath, nThread = detectCores() - 1,...)
    system(paste0(aws_cli, " s3 cp --sse aws:kms ",
                  tempPath, " ",
                  get_key_s3(osPathJoin(filePath), auxBucket)))
    system(paste0("rm ", tempPath))
  }
  else(print("Empty file, not saved"))
}


#' saveRDS_s3
#'
#' @description Adaptation of saveRDS function from base package to work with S3 buckets.
#'
#' @param file Object to be written
#' @param filePath Name and path of file to store the data within the S3 bucket
#' @param ... List of additional parameters sent to saveRDS
#'
#' @details The standard solution to write files from R to S3 uses a HTTP PUT request.
#' The issue with this approach is the 5GB limit on part size that AWS S3 imposes together with server-side encryption.
#' To overcome this limitation, the file is written locally in a temporary location, then moved
#' to S3 using AWS CLI and finally deleted.
#' By using AWS CLI it's also possible to control all the aspects of the copy, such as server side encryption
#'
#' @examples
#' saveRDS_s3(file = preparedTableMasterDt,
#'            filepath = "/PreparedData/Prepared_table_master_201701.RDS")

saveRDS_s3 <- function(file, filePath, ...){
  saveRDS(file, tempPath, ...)
  system(paste0(aws_cli, " s3 cp --sse aws:kms ",
                tempPath, " ",
                get_key_s3(osPathJoin(filePath), auxBucket)))
  system(paste0("rm ", tempPath))
}


#' dir_s3
#'
#' @description Adaptation of dir function from base R to work with S3 buckets.
#'
#' @param path Path within the S3 bucket to list files
#'
#' @details This function uses AWS CLI ls command to write a temporary file that is then
#' read from R. The location of the temporary file is defined in configure_s3 creating a unique
#' name based on the whoami and hostname of the machine.
#'
#' @examples
#' dir_s3(path = "/PreparedData")

dir_s3 <- function(path, directory = FALSE){
  if(directory == TRUE){
    system(paste0(aws_cli, " s3 ls ", get_key_s3(path, auxBucket),
                  "/ | cat > ", tempPath))
    if(file.info(tempPath)$size == 0){
      return(character(0))
    }
    auxDt <- readLines(tempPath)
    auxDt <- auxDt[str_detect(auxDt, "/")]
    writeLines(text = auxDt, con = tempPath)
    auxDt <- fread(tempPath, sep = " ", header = FALSE) 
    return(auxDt[, V2])
  } else {
    system(paste0(aws_cli, " s3 ls ", get_key_s3(path, auxBucket),
                  "/ | cat > ", tempPath))
    if(file.info(tempPath)$size == 0){
      return(character(0))
    }
    auxDt <- readLines(tempPath)
    auxDt <- auxDt[!str_detect(auxDt, "/")]
    writeLines(text = auxDt, con = tempPath)
    auxDt <- fread(tempPath, sep = " ") 
    return(auxDt[, V4])
  }
  
}

#' file.info_s3
#'
#' @description This function returns the time stamp of creation of the provided S3 key
#'
#' @param filePath Path within the S3 bucket to list files
#'
#' @details This function uses AWS CLI ls command to write a temporary file that is then
#' read from R. The location of the temporary file is defined in configure_s3 creating a unique
#' name based on the whoami and hostname of the machine.
#'
#' @examples
#' file.info_s3(filePath = "/PreparedData/Prepared_table_master_201701.RDS")


file.info_s3 <- function(filePath){
  options(warn=-1)
  filename <- str_split(filePath, "/") %>% sapply(last)
  directory <- str_sub(filePath, 1, nchar(filePath) - nchar(filename) - 1)
  system(paste0(aws_cli, " s3 ls ", get_key_s3(directory, auxBucket),
                "/ | cat > ", tempPath))
  auxDt <- fread(tempPath, sep = " ")
  auxDt <- auxDt[V4 == filename]
  timeStamp <- paste0(auxDt[, c("V1"), with = FALSE],"_", auxDt[, c("V2"), with = FALSE])
  options(warn=0)
  return(timeStamp)
}


#' fread_s3_parallel
#'
#' @description Adaptation of fread function from data.table package to work with S3 buckets in
#' a parallel fashion (multiple files downloaded and read in parallel)
#'
#' @param filename List of the path of files within the desired S3 bucket
#' @param numberOfCores Number of parallel R sessions to be created (always leave at least
#' one CPU free on your machine to avoid bottlenecks)
#' @param ... List of additional parameters sent to rbindlist
#'
#' @details In order to speed up reading multiple files (e.g: all the periods of transaction)
#' a cluster is created with parallel R sessions, each of them will execute a call to fread_s3
#' Each node returns their input as an element of a list, which is then rbindlist-ed
#'
#' @examples
#' week <- 201701
#' periodsToSelect <- c(0:104)
#' weeksToSelect <- getPastPeriod(week, periodsToSelect)
#' filename <- osPathJoin(originalWeeklyDataPath, paste0(auxFilename, weeksToSelect, ".csv"))
#' auxDataDt <- fread_s3_parallel(filename, ...)
#'

fread_s3_parallel <- function(filename,
                              numberOfCores = min(52, detectCores() - 1),
                              ...){
  library(parallel)
  cl <- makeCluster(numberOfCores)
  clusterExport(cl, "auxBucket")
  clusterExport(cl, "aws_cli")
  clusterEvalQ(cl, library("data.table"))
  clusterEvalQ(cl, library("bit64"))
  
  auxDataDt <- rbindlist(parLapply(cl, filename, ... ,fread_s3), fill = T)
  stopCluster(cl)

  return(auxDataDt)
}

#' verboseFread
#'
#' @description Alternative version of fread_s3 that shows on console the file being loaded
#'
#' @param x Path of the file within the desired S3 bucket
#' @param ... List of additional parameters sent to fwrite_s3
#'
#'
#' @examples
#' verboseFread(x = "/PreparedData/Prepared_table_master_201701.csv")


verboseFread <- function(x, ...){
  cat("Loading", x, "\n")
  auxDt <- fread_s3(x)
  return(auxDt)
}

#' file.exists_s3
#'
#' @description Adaptation of file.exists function from base package to work with S3 buckets
#'
#' @param filePath Absolute path of the file
#'
#' @examples
#' setWorkspace("16.Model_3_MX_TT_HomeMarket", "MX")
#' directory <- targetPreparedPath
#' filename <- "TARGET_MASTER_PVP_ICP_PRODUCT_ID_201748.csv"
#' filePath <- osPathJoin(directory, filename)
#'
#' file.exists_s3(filePath) #TRUE
#'

file.exists_s3 <- function(filePath){
  filename <- str_split(filePath, "/") %>% sapply(last)
  directory <- str_sub(filePath, 1, nchar(filePath) - nchar(filename) - 1)
  filename %in% dir_s3(directory)
}



getBucket_S3 <- function(env = 'gb-analytics-dev',
                         name = 's3-bucket-name-rnode-data',
                         region = 'us-east-2'){
  cat(env, fill = TRUE)
  name <- osPathJoin(env, name)
  name <- paste0("/", name)
  auxDt <- system(glue::glue("aws ssm get-parameter --name {name} --region {region}"), intern = TRUE) %>%
    paste(collapse = "\n") %>%
    rjson::fromJSON()
  auxDt$Parameter$Value
}

#' returnNonExistentFilesS3
#'
#' @description Verifica si los archivos a producir por determinada funcion
#' ya existen en el bucket s3 y devuelve la lista de fechas para las que esa
#' funcion no ha producido ningun fichero todavia. Se evita sobreescribir ficheros
#' ya existentes
#'
#' @param fun funcion que se va a lanzar
#' @param periods lista de fechas para las que se va a lanzar la funcion "fun"
#' @param json fichero json predeterminado cargado en memoria con la configuracion de
#' entorno pre-definida
#'
#' @examples
#' returnNonExistentFilesS3(fun = "processMora", periods = c("20170101"), json = auxJson) 
#'

returnNonExistentFilesS3 <- function(fun, periods, json = auxJson){
  filepath <- get(json[["OutputFileNames"]][[fun]][["path"]])
  listDates <- str_extract_all(dir_s3(filepath),"[\\d]+") %>% unlist
  nonExistentFiles <- setdiff(periods, listDates)
  if (length(nonExistentFiles) == 0){
    cat("Todos los periodos solicitados ya fueron generados con anterioridad...\n")
  } else {
    return(nonExistentFiles)
  }
}