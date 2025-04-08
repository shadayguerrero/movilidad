#!/bin/bash

# === Rutas de entrada y salida ===
DATASET="/data/covid/aws/movement_parquet"
SCRIPTS="/data/shaday_data/scripts_modificados"
OUTPUT="/data/shaday_data/prueba_080425"
LOGFILE="$OUTPUT/run_bipartite_network_$(date +%Y%m%d_%H%M%S).log"

# === Crear directorios de salida si no existen ===
mkdir -p "$OUTPUT/homefiles"

# === Función para procesar un mes completo ===
process_month() {
  START_DATE=$1
  END_DATE=$2
  MONTH_ID=$3  # Formato yyyy-mm

  echo "========== Procesando $MONTH_ID =========="

  echo ">> [$(date)] Generando archivos diarios de home assignation para $MONTH_ID..."
  python "$SCRIPTS/Runner_HomeAssignStartAGEB.py" \
    --dataset "$DATASET" \
    --output "$OUTPUT/homefiles"

  echo ">> [$(date)] Uniendo homefiles diarios de $MONTH_ID en un único parquet..."
  python "$SCRIPTS/JoinHomeAssign_StartAGEB.py" \
    --start "$START_DATE" \
    --end "$END_DATE" \
    --dataset "$OUTPUT/homefiles" \
    --output "$OUTPUT/homefiles"

  HOMEFILE="$OUTPUT/homefiles/JoinHomeAssignation_StartAGEB_${START_DATE}_${END_DATE}.parquet"

  echo ">> [$(date)] Corriendo INER_BiNetwork.py para $MONTH_ID..."
  python "$SCRIPTS/INER_BiNetwork.py" \
    --date "$MONTH_ID" \
    --dataset "$DATASET" \
    --output "$OUTPUT" \
    --home "$HOMEFILE"

  echo "========== $MONTH_ID terminado =========="
  echo ""
}

# === Redirigir salida a log + consola ===
exec > >(tee -a "$LOGFILE") 2>&1

echo "===== Inicio de ejecución: $(date) ====="
echo "📂 Usando scripts modificados en: $SCRIPTS"
echo "📂 Guardando resultados en: $OUTPUT"
echo ""

# === Ejecutar para marzo 2020 ===
process_month "2020-03-01" "2020-03-31" "2020-03"

# === Ejecutar para abril 2020 ===
process_month "2020-04-01" "2020-04-30" "2020-04"

echo ""
echo "===== Fin de ejecución: $(date) ====="

