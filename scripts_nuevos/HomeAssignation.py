import os
import sys
import duckdb
import pandas as pd
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
            "-o", "--output", help="Directory for output networks.")
        self.args = parser.parse_args()

    def run(self):
        # Inicializando variables del usuario y paths
        date = self.args.date
        year, month, day = date.split("-")
        OUTPUT_DIR = self.args.output
        DATASET_MOVEMENT_PATH = self.args.dataset
        OUTPUT_NAME = f"HomeAssign_Municipio_{date.replace('-', '_')}"
        OUTPUT_DIR = os.path.join(OUTPUT_DIR, OUTPUT_NAME + '.csv')
        DATASET_MOVEMENT_PATH = f"{DATASET_MOVEMENT_PATH}/year={year}/month={month}/day={day}/*"

        # Inicializando conexión con DuckDB
        cursor = duckdb.connect()

        # Crear tabla con todos los datos
        instruction = f"""
        CREATE TABLE movement AS
        SELECT * FROM read_parquet('{DATASET_MOVEMENT_PATH}')
        """
        cursor.execute(instruction)

        # Obtener el primer ping por dispositivo, incluyendo municipio
        instruction = """
        CREATE TABLE first AS (
            WITH records AS (
                SELECT caid, utc_timestamp, cve_mun,
                       ROW_NUMBER() OVER(PARTITION BY caid ORDER BY utc_timestamp ASC) AS rn
                FROM movement
            )
            SELECT * FROM records WHERE rn = 1
        )
        """
        cursor.execute(instruction)

        # Eliminar la tabla de movimiento para liberar memoria
        cursor.execute('DROP TABLE movement')

        # Crear tabla con los caids y el municipio de origen
        instruction = "CREATE TABLE home AS SELECT caid, cve_mun FROM first"
        cursor.execute(instruction)

        # Mostrar cuántos dispositivos únicos y municipios hay
        instruction = "SELECT COUNT(DISTINCT caid), COUNT(DISTINCT cve_mun) FROM first"
        cursor.execute(instruction)
        allcaid = cursor.fetchall()
        print("devices / municipios")
        print(allcaid)

        # Eliminar la tabla auxiliar
        cursor.execute('DROP TABLE first')

        # Exportar a CSV
        instruction = f"COPY home TO '{OUTPUT_DIR}' (FORMAT CSV, HEADER)"
        cursor.execute(instruction)

        # Limpiar
        cursor.execute('DROP TABLE home')
        cursor.close()

if __name__ == '__main__':
    m = Main()
    m.run()

