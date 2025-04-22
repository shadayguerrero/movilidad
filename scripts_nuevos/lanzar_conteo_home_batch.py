import os
import subprocess
import pandas as pd

# Rango de fechas
fechas = pd.date_range(start="2020-03-15", end="2020-03-22")

# Script principal
script = "conteo_home_por_tiempo_y_primero.py"

# Parámetros fijos
db_path = "/data/covid/aws/movement_parquet"
output_dir = "/data/shaday_data/GIT/movilidad/home_fusionado"

# Crear directorio si no existe
os.makedirs(output_dir, exist_ok=True)

# Iterar por fechas
for fecha in fechas:
    date_str = fecha.strftime("%Y-%m-%d")
    output_file = f"{output_dir}/conteo_home_{date_str.replace('-', '_')}.csv"

    if os.path.exists(output_file):
        print(f"✔️ Ya existe: {output_file}, saltando.")
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

