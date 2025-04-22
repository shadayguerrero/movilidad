import os
import duckdb
import pandas as pd
from datetime import datetime
from tqdm import tqdm

# CONFIGURACIÓN
input_path = "/data/covid/aws/movement_parquet"
output_csv = "/data/shaday_data/GIT/movilidad/primer_ping_08.csv"
fechas = pd.date_range(start="2020-03-15", end="2020-03-22")

# Resultados
registros = []

for fecha in tqdm(fechas, desc="Procesando fechas"):
    date_str = fecha.strftime("%Y-%m-%d")
    year = fecha.strftime("%Y")
    month = fecha.strftime("%m")
    day = fecha.strftime("%d")
    parquet_path = f"{input_path}/year={year}/month={month}/day={day}/*"

    try:
        con = duckdb.connect()
        con.execute(f"""
            CREATE TABLE primer_ping_full AS (
                SELECT 
                    caid,
                    to_timestamp(utc_timestamp) AS utc_time
                FROM read_parquet('{parquet_path}')
                WHERE cve_ent IS NOT NULL AND cve_mun IS NOT NULL
            );

            CREATE TABLE primer_ping AS (
                SELECT caid, MIN(utc_time) AS primer_ping
                FROM primer_ping_full
                GROUP BY caid
            );

            CREATE TABLE clasificados AS (
                SELECT *,
                    CASE 
                        WHEN primer_ping < TIMESTAMP '{date_str} 08:00:00' THEN 'antes_08'
                        ELSE 'despues_08'
                    END AS grupo
                FROM primer_ping
            );

            SELECT grupo, COUNT(*) AS num_dispositivos
            FROM clasificados
            GROUP BY grupo;
        """)
        df = con.df()
        df["fecha"] = date_str
        registros.append(df)
        con.close()
    except Exception as e:
        print(f"❌ Error al procesar {date_str}: {e}")

# Combinar resultados y exportar
if registros:
    df_total = pd.concat(registros)
    df_total = df_total.pivot(index="fecha", columns="grupo", values="num_dispositivos").fillna(0).astype(int)
    df_total.to_csv(output_csv)
    print(f"✅ Conteo exportado a: {output_csv}")
else:
    print("⚠️ No se procesaron datos.")

