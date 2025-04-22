import os
import subprocess
import pandas as pd

# Rango de fechas a procesar
fechas = pd.date_range(start="2020-03-15", end="2020-03-22")

# Ruta al script principal
script = "dispositivos_por_municipio.py"

# Parámetros fijos
db_path = "/data/covid/aws/movement_parquet"
output_dir = "/data/shaday_data/GIT/movilidad/dispositivos_por_municipio"

# Crear el directorio de salida si no existe
os.makedirs(output_dir, exist_ok=True)

# Ejecutar por cada fecha
for fecha in fechas:
    date_str = fecha.strftime("%Y-%m-%d")
    output_file = f"{output_dir}/dispositivos_municipio_{date_str.replace('-', '_')}.csv"
    
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

