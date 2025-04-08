import argparse
import os
import duckdb

class Main:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-d", "--date", help="Start date of 24 hour period of observational unit. Format: yyyy-mm-dd")
        parser.add_argument(
            "-db", "--dataset", help="Directory where parquet dataset files are")
        parser.add_argument(
            "-o", "--output", help="Directory for output files.")
        self.args = parser.parse_args()

    def run(self):
        date = self.args.date
        year, month, day = date.split("-")
        OUTPUT_DIR = os.path.join(self.args.output, f"HomeAssign_StartAGEB_{date.replace('-', '_')}.csv")
        DATASET_MOVEMENT_PATH = f"{self.args.dataset}/year={year}/month={month}/day={day}/*"

        # Conectar DuckDB
        cursor = duckdb.connect()

        # Leer todos los datos del día, sin filtrar por AGEBs
        print(f"🔄 Procesando datos para {date}...")
        cursor.execute(f"CREATE TABLE zmvm AS SELECT * FROM read_parquet('{DATASET_MOVEMENT_PATH}')")

        # Seleccionar la primera lectura por caid
        cursor.execute("""
            CREATE TABLE first AS (
                WITH records AS (
                    SELECT caid, utc_timestamp, cvegeo,
                    ROW_NUMBER() OVER(PARTITION BY caid ORDER BY utc_timestamp ASC) rn
                    FROM zmvm
                )
                SELECT * FROM records WHERE rn = 1
            )
        """)
        cursor.execute("DROP TABLE zmvm")

        # Asignar 'home'
        cursor.execute("CREATE TABLE home AS SELECT caid, cvegeo FROM first")
        cursor.execute("DROP TABLE first")

        # Exportar
        cursor.execute(f"COPY home TO '{OUTPUT_DIR}' (FORMAT CSV, HEADER)")
        cursor.execute("DROP TABLE home")
        cursor.close()
        print(f"✅ Archivo guardado en: {OUTPUT_DIR}")

if __name__ == '__main__':
    m = Main()
    m.run()

