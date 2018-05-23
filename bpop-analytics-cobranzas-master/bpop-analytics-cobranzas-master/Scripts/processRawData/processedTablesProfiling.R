###########################################################################
#  ------------------------------------------------------------------------
#  Functions to summarise tables
#
#  @author: Garage
#  @version: 2017/01/23
#
#  ------------------------------------------------------------------------
###########################################################################

UniqueValuesInTable <- function(database) {
  
  uniques <- database[,lapply(.SD, uniqueN)]
  uniques <- t(uniques)
  
  uniques <- cbind(rownames(uniques), uniques)
  
  colnames(uniques) <- c("Variables", "Unique")
  
  uniques <- data.table(uniques)
  uniques <- uniques[Variables != "V1"]
  
  return(uniques)
  
}

CountNonNAs <- function(database, percFormat = TRUE) {
  
  countNonNAs <- as.data.frame(colMeans(!is.na(database)))
  
  countNonNAs <- cbind(rownames(countNonNAs), countNonNAs)
  
  colnames(countNonNAs) <- c("Variables", "NonNAs")
  
  if(!percFormat) {
    countNonNAs$NonNAs <- countNonNAs$NonNAs*nrow(database)
  }
  
  return(countNonNAs)
  
}

SummariseNumericColumns <- function(database) {
  
  nums <- colnames(database)[sapply(database, is.numeric)]
  
  if(length(nums) < 1) {
    return(NA)
  }
  
  temp <- data.frame()
  
  for(column in nums) {
    temp <- rbind(temp,
                  cbind("Variables" = column, database %>% summarise_(Min = interp(~min(var, na.rm = TRUE), var = as.name(column)),
                                                                      q1 = interp(~quantile(var,probs = 0.25, na.rm = TRUE), var = as.name(column)),
                                                                      Mean = interp(~mean(var, na.rm = TRUE), var = as.name(column)),
                                                                      Median = interp(~median(var, na.rm = TRUE), var = as.name(column)),
                                                                      q3 = interp(~quantile(var,probs = 0.75, na.rm = TRUE), var = as.name(column)),
                                                                      Max = interp(~max(var, na.rm = TRUE), var = as.name(column))
                                                                      
                  )
                  )
    )
  }
  
  cols <- as.data.frame(colnames(database))
  colnames(cols) <- "Variables"
  
  cols <- merge(cols, temp, by.x = "Variables", by.y = "Variables", all.x = TRUE, all.y = TRUE)

  return(cols)
  
}

GetDistinctValues <- function(database, uniques = NA, numCases = 20) {
  
  if(is.na(uniques) | any(grepl("Unique", colnames(uniques)))) {
    uniques <- database[,lapply(.SD, uniqueN)]
    uniques <- t(uniques)
    
    uniques <- cbind(rownames(uniques), uniques)
    
    colnames(uniques) <- c("Variables", "Unique")
    
    uniques <- data.table(uniques)
    uniques <- uniques[Variables != "V1"]
    uniques[,Unique := as.numeric(Unique)]
  }
  
  filtered <- uniques[Unique <= numCases]
  samples <- uniques[Unique > numCases]
  
  database_distinct <- database %>% select_(.dots = unique(as.character(filtered$Variables)))
  
  database_sample <- database %>% select_(.dots = unique(as.character(samples$Variables)))
  
  distinctValues <- as.data.frame(apply(database_distinct, 2, function(x)paste(unique(x), collapse = ", ")))
  sampleValues <- as.data.frame(apply(database_sample, 2, function(x)paste(sample(x, numCases), collapse = ", ")))
  
  distinctValues <- cbind(rownames(distinctValues), distinctValues)
  colnames(distinctValues) <- c("Variables", "Values")
  
  sampleValues <- cbind(rownames(sampleValues), sampleValues)
  colnames(sampleValues) <- c("Variables", "Values")
  
  distinctValues <- rbind(distinctValues, sampleValues)
  
  distinctValues$Values <- as.character(distinctValues$Values)
  
  distinctValues[is.na(distinctValues)] <- " - "

  return(distinctValues)
  
}

GetColumnType <- function(database) {
  
  columntypes <- t(as.data.frame(lapply(database, class)))
  
  columntypes <- cbind(rownames(columntypes), columntypes)
  
  colnames(columntypes) <- c("Variables", "Type")
  
  columntypes <- as.data.frame(columntypes)
  
  return(columntypes)
  
}

SummariseTable <- function(database, filename = NULL) {
  
  
  rows <- nrow(database)
  
  print("Column types...")
  summaryTable <- GetColumnType(database)
  
  summaryTable$Rows <- rows
  
  summaryTable$RowNum <- seq(1:nrow(summaryTable))
  
  print("Non NAs...")
  temp <- CountNonNAs(database)
  
  if(!is.na(temp)) {
    summaryTable <- merge(summaryTable, temp, by.x = "Variables",
                          by.y = "Variables", all.x = TRUE, all.y = TRUE)
  }
  
  print("Unique values...")
  temp <- UniqueValuesInTable(database)
  
  if(!is.na(temp)) {
    summaryTable <- merge(summaryTable, temp, by.x = "Variables",
                          by.y = "Variables", all.x = TRUE, all.y = TRUE)
  }
  
  print("Summarising numeric columns...")
  temp <- SummariseNumericColumns(database)
  
  if(!is.na(temp)) {
    summaryTable <- merge(summaryTable, temp, by.x = "Variables",
                          by.y = "Variables", all.x = TRUE, all.y = TRUE)
  }
  
  print("Getting distinct values...")
  temp <- GetDistinctValues(database)
  
  if(!is.na(temp)) {
    summaryTable <- merge(summaryTable, temp, by.x = "Variables",
                          by.y = "Variables", all.x = TRUE, all.y = TRUE)
  }
  
  summaryTable <- summaryTable %>% arrange(RowNum) %>% select(-RowNum)
  
  fwrite_s3(file = summaryTable, filePath = filename)
}
