#' createMasterTable
#' 
#' @description Funcion de creaccion de variable en la que se unen todas las
#' tablas de variables sinteticas para crear la vision unica por cliente-producto-dia
#' que entra al modelo 
#' 
#' @param periodo Dia de ejecucion del modelo, en formato YYYYMMDD
#' @return Master Table para el dia de ejecucion con toda la informacion a nivel cliente-producto-dia
#' el dia que se scorea al cliente
#' @examples
#' createMasterTable(20180417)

createMasterTable <- function(periodo, tarea = "train"){
  
  # Creaccion de variables auxiliares para seleccionar los datos mensuales
  # Solo la tabla de Mora incorpora informacion diaria, el resto son tablas a cierre de mes
  # La tabla de Buro funciona de manera especial al reportarse de manera bimensual
  # con un mes de lag
  # Faltaria por introducir el lag existente en las tablas mensuales
  
  # Crear el masterDt para realizar la prediccion
  tablesDataset <- auxJson[["DataPreparationConfig"]][["Tables"]]
  # Comenzando por la tabla de customer base, se realiza una serie de left-joins
  table <- tablesDataset[["CustomerBase"]]
  path <- get(table$path)
  filename <- table$filename
  masterDt <- getProcessedTableByPeriod(dataSource = filename,
                                        periods = periodo,
                                        path = path,
                                        colClasses = "character",
                                        na.strings = c("", NA))[, 
                                                                c(auxJson$varProducto, auxJson$varPeriodo),
                                                                with = F]
  testthat::expect_true(!is.null(masterDt), 
                        info = paste("Ningun cliente entro en mora en el periodo:", periodo))
  
  tablesDataset[["CustomerBase"]] <- NULL
  # Target will not be read for the prediction period, since it does not exist
  if(tarea != "train"){
    tablesDataset[["Target"]] <- NULL
  }
  
  
  lagDataBuroPasivo <- 10
  lagDataContratos <- 4
  periodoMesPasivo <- monthAddInteger(str_sub(periodo, 1, 6), -1)
  periodoMesBuro <- monthAddInteger(str_sub(periodo, 1, 6), -1)
  periodoMesContratos <- monthAddInteger(str_sub(periodo, 1, 6), -1)
  
  if(day(ymd(periodo)) < lagDataBuroPasivo){
    periodoMesPasivo <- monthAddInteger(periodoMesPasivo, -1)
  }
  if(day(ymd(periodo)) < lagDataBuroPasivo){
    periodoMesBuro <- monthAddInteger(periodoMesBuro, -1)
    # Identificamos el ultimo archivo de buro actualizado para mergearlo con la master
    listExpandedBuroFiles <- unlist(str_extract_all(dir_s3(buroExpandedPath), "[0-9]+"))
    periodoMesBuro <- max(listExpandedBuroFiles[listExpandedBuroFiles <= periodoMesBuro])
  } else {
    # Identificamos el ultimo archivo de buro actualizado para mergearlo con la master
    listExpandedBuroFiles <- unlist(str_extract_all(dir_s3(buroExpandedPath), "[0-9]+"))
    periodoMesBuro <- max(listExpandedBuroFiles[listExpandedBuroFiles <= periodoMesBuro])
  }
  if(day(ymd(periodo)) < lagDataContratos){
    periodoMesContratos <- monthAddInteger(periodoMesContratos, -1)
  }
  
  
  # Para cada una de las tablas a mergear se realiza el mismo proceso
  # a) Lectura de tabla
  # b) Limpieza de variables (añadir sufijo de origen de las tablas)
  # c) Fijar el key de mergeo (variables por las cual se uniran las tablas)
  
  # Las tablas introducidas en el modelo son:
  # 1) Customer Base
  # 2) Mora
  # 3) Contratos
  # 4) TxPasivo
  # 5) Buro
  # 6) Clientes
  # 7) Target
  
  
  for(listNames in names(tablesDataset)){
    cat("Mergeando tabla", listNames, "para el periodo", periodo, fill = TRUE)
    table <- tablesDataset[[listNames]]
    path <- get(table$path)
    filename <- table$filename
    tag <- table$tag
    keys <- unlist(table$keys)
    # Logica del dia a mergear
    if(listNames %in% c("CustomerBase", "Mora", "Target")){
      auxPeriodToSelect <- periodo
    } else if(listNames %in% c("Buro")){
      auxPeriodToSelect <- periodoMesBuro
    } else if(listNames %in% c("txPasivo")){
      auxPeriodToSelect <- periodoMesPasivo
    } else if(listNames %in% c("Contratos", "Clientes")){
      auxPeriodToSelect <- periodoMesContratos
    }
    
    auxFiles <- getProcessedTableByPeriod(dataSource = filename,
                                          periods = auxPeriodToSelect,
                                          path = path,
                                          colClasses = "character",
                                          na.strings = c("", NA))
    masterDt[, (keys) := lapply(.SD, as.character), .SDcols = keys]
    auxFiles[, (keys) := lapply(.SD, as.character), .SDcols = keys]
    auxFiles <- unique(auxFiles)
    
    auxFiles[, YEAR_MONTH_DAY := NULL]
    auxFiles[, YEAR_MONTH := NULL]
    
    namesToChange <- setdiff(names(auxFiles), c(auxJson$varProducto,
                                                auxJson$varCliente,
                                                auxJson$varPeriodo))
    setnames(auxFiles, 
             namesToChange,
             paste0(namesToChange, "_", tag))
    
    masterDt <- merge(masterDt,
                      auxFiles,
                      by = keys,
                      all.x = TRUE,
                      all.y = FALSE)
  }

  # Creamos variables sinteticas adicionales
  masterDt[, DIA_ENTRA_MORA := as.numeric(str_sub(YEAR_MONTH_DAY, 7,8))]
  masterDt[, SIN_HISTORICO_MORA := ifelse(!is.na(DIAS_MAXPREV120_MORA), 1, 0)]
  
  # Guardar el archivo procesado como MASTER_TABLE_YYYYMMDD en el directorio
  # 1.Data/PreparedData
  print("Dimension tabla master: " %+% masterDt[, .N])
  
  saveProcessedTableByPeriod(masterDt,
                             source = "masterTable" ,
                             periodIDvarname = "YEAR_MONTH_DAY",
                             path = masterTableExpandedPath)
}
