load_data <- function(inPath = originalDataPath, periodsToRead, source){
  dt <- data.table()
  for (period in periodsToRead){
    file <- list.files(inPath %+% "/" %+% period, full.names = T) %gv% source
    if (length(file) > 0){
      
      temp <- fread(file, 
                    na.strings = c("NA", ""), colClasses = "character",
                    encoding = "Latin-1", nThread = detectCores()- 1) %>% capitalizeNames
      
      temp[, YEAR_MONTH := period]
      
      dt <- rbind(dt, temp)
    } else {
      warning("Requested " %+%  source %+% " " %+% period %+%  " file does not exists and it was skipped")
      next
    }
  }
  return(dt)
}