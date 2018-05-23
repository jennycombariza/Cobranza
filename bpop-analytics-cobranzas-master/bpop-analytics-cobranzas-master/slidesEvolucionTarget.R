dataset[, YEAR_MONTH := str_sub(YEAR_MONTH_DAY, 1, 6)]
dataset[is.na(SAL_CAPITAL_CLIENTE_CONT), SAL_CAPITAL_CLIENTE_CONT := 0]
dataset[, rolledCapital := SAL_CAPITAL_CLIENTE_CONT * TARGET_BINARY_TARG]
dataset[, rolledCapitalContrato := SALDO_CAPITAL_MORA * TARGET_BINARY_TARG]
dataset[, MAX_TARGET := min(max(TARGET_BINARY_TARG, na.rm = T), 1), by = .(ID_CLIENTE, YEAR_MONTH, LINEA_MORA)]

clientVision <- unique(dataset[,.(ID_CLIENTE, YEAR_MONTH, MAX_TARGET)])
summaryClientesRoll <- clientVision[, .(clientes = .N,
                                        clientesRoll = sum(MAX_TARGET)), by = .(YEAR_MONTH)]

summary <- dataset[, .(num_contratos = .N,
                       clientes = uniqueN(ID_CLIENTE),
                       sum_target = sum(TARGET_BINARY_TARG, na.rm = T),
                       meanTarget = mean(TARGET_BINARY_TARG),
                       sum_capital = sum(SAL_CAPITAL_CLIENTE_CONT, na.rm = T),
                       sum_rolledCapital = sum(rolledCapital, na.rm = T),
                       sum_capital_contrato = sum(SALDO_CAPITAL_MORA, na.rm = T),
                       sum_rolledCapital = sum(rolledCapitalContrato, na.rm = T)
                       ), by = .(LINEA_MORA, YEAR_MONTH)]

fwrite(summary, "BPO_CollectionsLibrary/visionProducto.csv", sep = ";")
fwrite(summaryClientesRoll, "BPO_CollectionsLibrary/visionCliente.csv", sep = ";")
