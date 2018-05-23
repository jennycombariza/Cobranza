
createDataset <- function(periodsToSelect){
  
  dataset <- getProcessedTableByPeriod(dataSource = "masterTable",
                                       periods = periodsToSelect,
                                       path = masterTableExpandedPath)
  dataset <- unique(dataset, by = c("ID_CLIENTE", "ID_CONTRATO", "YEAR_MONTH_DAY"))
  
  # Limpiamos los NAs generados por el rbind de la union
  
  binarized_cols <- names(dataset) %gv% "LINEA_|PT_|RIESGO_"
  # Reemplazamos los "-Inf" por NA 
  for(var in binarized_cols){
    cat("Limpiando infinitos de la variable", var, fill = TRUE)
    set(x = dataset,
        j = var,
        i = dataset[, which(is.nan(get(var)) | is.infinite(get(var)))],
        value = NA)
    
  }
  
  dataset <- unique(dataset, by = c("ID_CLIENTE", "ID_CONTRATO", "YEAR_MONTH_DAY"))
  
  return(dataset)
}

