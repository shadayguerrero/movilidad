import os
import subprocess
import pandas as pd

# Rango de fechas
fechas = pd.date_range(start="2020-03-01", end="2022-03-31")

# Parámetros base
script = "/data/shaday_data/GIT/movilidad/scripts_nuevos/od_por_municipio.py"  # asegúrate de que esté en el mismo directorio o dale ruta completa
db_path = "/data/covid/aws/movement_parquet"
output_dir = "/data/shaday_data/GIT/movilidad/salidas_od"

# Crear carpeta de salida si no existe
os.makedirs(output_dir, exist_ok=True)

# Iterar por fechas
for fecha in fechas:
    date_str = fecha.strftime("%Y-%m-%d")
    output_file = f"{output_dir}/od_municipio_{date_str.replace('-', '_')}.csv"
    
    # Verifica si ya existe
    if os.path.exists(output_file):
        print(f"✔️ Ya existe: {output_file}, saltando.")
        continue

    # Ejecutar el script
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

