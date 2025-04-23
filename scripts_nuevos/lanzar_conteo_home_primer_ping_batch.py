import os
import subprocess
import pandas as pd

# 👉 Define el rango de fechas
fechas = pd.date_range(start="2020-03-15", end="2020-03-22")

# Script principal
script = "conteo_home_primer_ping.py"

# Parámetros fijos
db_path = "/data/covid/aws/movement_parquet"
output_dir = "/data/shaday_data/GIT/movilidad/outputs/home_00"

# Crear el directorio de salida si no existe
os.makedirs(output_dir, exist_ok=True)

# Iterar sobre las fechas
for fecha in fechas:
    date_str = fecha.strftime("%Y-%m-%d")
    output_file = os.path.join(output_dir, f"conteo_home_primer_ping_{date_str.replace('-', '_')}.csv")

    if os.path.exists(output_file):
        print(f"✔️ Ya existe: {output_file}, saltando {date_str}")
        continue

    print(f"🚀 Procesando {date_str}...")
    comando = [
        "python", script,
        "-d", date_str,
        "-db", db_path,
        "-o", output_dir
    ]
    try:
        subprocess.run(comando, check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al procesar {date_str}: {e}")

