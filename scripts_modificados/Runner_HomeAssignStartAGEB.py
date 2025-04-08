import argparse
import os
import subprocess

# Rango de fechas deseado (puedes ajustar si quieres limitarlo)
years = ["2020"]
months = [str(i).zfill(2) for i in range(1, 13)]
days = [str(i).zfill(2) for i in range(1, 32)]

# Argumentos esperados
parser = argparse.ArgumentParser()
parser.add_argument("-db", "--dataset", help="Directory where parquet dataset files are.")
parser.add_argument("-o", "--output", help="Directory for output files.")
args = parser.parse_args()

DATASET_MOVEMENT_PATH = args.dataset
OUTPUT_DIR = args.output

# Recorrer fechas y ejecutar HomeAssignation_StartAGEB.py para cada día válido
for y in years:
    for m in months:
        for d in days:
            data_path = f"{DATASET_MOVEMENT_PATH}/year={y}/month={m}/day={d}/"
            if os.path.exists(data_path):
                date = f"{y}-{m}-{d}"
                print(f"📆 Ejecutando para {date}...")
                subprocess.run([
                    "python",
                    "HomeAssignation_StartAGEB.py",
                    "-d", date,
                    "-db", DATASET_MOVEMENT_PATH,
                    "-o", OUTPUT_DIR
                ], check=True)

