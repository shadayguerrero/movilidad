import duckdb
import glob
import os

# Ruta donde buscar archivos
BASE_PATH = "/data/covid/aws/movement_parquet"
OUTPUT_CSV = "/data/shaday_data/todos_agebs.csv"

# Subconjunto de días para evitar sobrecarga
example_days = [
    "year=2020/month=03/day=01",
    "year=2020/month=03/day=15",
    "year=2020/month=04/day=01",
    "year=2020/month=04/day=15"
]

# Construir lista de archivos
files = []
for day_path in example_days:
    full_path = os.path.join(BASE_PATH, day_path, "*")
    matches = glob.glob(full_path)
    files.extend(matches)

if not files:
    raise FileNotFoundError("❌ No se encontraron archivos en las rutas especificadas.")

print(f"📦 Archivos encontrados: {len(files)}")

# Conectar a DuckDB
con = duckdb.connect()

# Ejecutar consulta directamente usando la lista como arreglo en DuckDB
query = f"""
    SELECT DISTINCT CVEGEO
    FROM read_parquet({files})
    WHERE CVEGEO IS NOT NULL
"""

print(f"💾 Guardando AGEBs únicos en: {OUTPUT_CSV}")
con.execute(f"COPY ({query}) TO '{OUTPUT_CSV}' (HEADER, DELIMITER ',')")

con.close()
print("✅ Archivo generado exitosamente.")

