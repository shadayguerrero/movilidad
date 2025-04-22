#!/bin/bash

# === Rutas ===
FECHA="2020-03-01"
DATASET="/data/shaday_data/datos_prueba/dia01/parquet"
SCRIPTS="/data/shaday_data/GIT/movilidad/scripts_modificados"
OUTPUT="/data/shaday_data/prueba_080425/dia01"
LOGFILE="$OUTPUT/run_dia01_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "$OUTPUT"

# === Redirigir a log + consola ===
exec > >(tee -a "$LOGFILE") 2>&1

echo "===== Procesando día único: $FECHA ====="

# === Paso 1: Asignar AGEB de origen ("home") ===
echo ">> [$(date)] Asignando home..."
python "$SCRIPTS/HomeAssignation_StartAGEB.py" \
  --date "$FECHA" \
  --dataset "$DATASET" \
  --output "$OUTPUT"

# === Paso 2: Generar red de contacto del día ===
echo ">> [$(date)] Generando red de contacto..."
python "$SCRIPTS/INER_ContactNetwork.py" \
  --date "$FECHA" \
  --dataset "$DATASET" \
  --output "$OUTPUT"

# === Paso 3: (Opcional) Generar red bipartita diaria ===
echo ">> [$(date)] Generando red bipartita para el día..."
python "$SCRIPTS/INER_BiNetwork.py" \
  --date "2020-03" \
  --dataset "$DATASET" \
  --output "$OUTPUT" \
  --home "$OUTPUT/JoinHomeAssignation_StartAGEB_2020-03-01_2020-03-01.parquet"

echo "===== Finalizado: $(date) ====="

