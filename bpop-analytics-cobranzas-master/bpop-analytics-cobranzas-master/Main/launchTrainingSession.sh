#!/bin/bash


hora_local=$(TZ=America/Bogota date '+%Y%m%d_%H%M%S');
LOG_FILE="logs/""$hora_local""_log.txt"

echo "El log de la ejecucion se va a guardar en $LOG_FILE"

exec > >(cat >> ${LOG_FILE})
exec 2> >(tee -a ${LOG_FILE} >&2)

echo "Entrenando el modelo a las "$(date '+%H:%M:%S')

inicio_train=$1
fin_train=$2
inicio_test=$3
fin_test=$4

echo "Se ha escogido el periodo de train entre $inicio_train y $fin_train"
echo "Se ha escogido el periodo de test entre $inicio_test y $fin_test"
echo "Comenzando el script de entrenamiento del modelo"

Rscript mainTrainingSession.R $inicio_train $fin_train $inicio_test $fin_test

exit 0
