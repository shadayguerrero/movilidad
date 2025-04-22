import os
import sys
import duckdb
import argparse

class Main:
    def __init__(self):
        #Opciones del usuario
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-d", "--date", help="Start date of 24 hour period of observational unit. Format: yyyy-mm-dd")
        parser.add_argument(
            "-db", "--dataset", help="Directory where parquet dataset files are")
        parser.add_argument(
            "-o", "--output", help="Directory for output CSVs.")
        self.args = parser.parse_args()

    def run(self):
        # Inicializando variables del usuario y paths
        date = self.args.date
        year, month, day = date.split("-")
        OUTPUT_DIR = self.args.output
        DATASET_MOVEMENT_PATH = self.args.dataset
        OUTPUT_NAME = f"DispositivosPorMunicipio_{date.replace('-', '_')}"
        OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_NAME + '.csv')
        DATASET_PATH = f"{DATASET_MOVEMENT_PATH}/year={year}/month={month}/day={day}/*"

        # Inicializando conexión con DuckDB
        cursor = duckdb.connect()

        # Leer parquet sin filtrar por ZMVM
        cursor.execute(f"""
        CREATE TABLE movement AS
        SELECT * FROM read_parquet('{DATASET_PATH}')
        """)

        # Obtener primer ping por caid
        cursor.execute("""
        CREATE TABLE first_ping AS (
            SELECT caid, utc_timestamp, cve_mun,
                   ROW_NUMBER() OVER (PARTITION BY caid ORDER BY utc_timestamp ASC) AS rn
            FROM movement
        )
        """)

        # Seleccionar solo los primeros pings
        cursor.execute("""
        CREATE TABLE first AS
        SELECT caid, cve_mun
        FROM first_ping
        WHERE rn = 1
        """)

        # Tabla de resumen: cuántos dispositivos por municipio
        cursor.execute("""
        CREATE TABLE summary AS
        SELECT cve_mun, COUNT(DISTINCT caid) AS num_dispositivos
        FROM first
        GROUP BY cve_mun
        ORDER BY num_dispositivos DESC
        """)

        # Exportar resultado
        cursor.execute(f"""
        COPY summary TO '{OUTPUT_PATH}' (FORMAT CSV, HEADER)
        """)

        print("✅ Exportación completa:", OUTPUT_PATH)

        # Limpieza
        cursor.execute("DROP TABLE movement")
        cursor.execute("DROP TABLE first_ping")
        cursor.execute("DROP TABLE first")
        cursor.execute("DROP TABLE summary")
        cursor.close()

if __name__ == '__main__':
    m = Main()
    m.run()

