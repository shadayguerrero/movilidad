import argparse
import os
import duckdb
import pandas as pd
from datetime import datetime

class Main:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-d", "--date", help="Selected day for contact network. Format: yyyy-mm-dd")
        parser.add_argument(
            "-db", "--dataset", help="Directory where parquet dataset with daily movement files are")
        parser.add_argument(
            "-o", "--output", help="Directory for output network")
        self.args = parser.parse_args()

    def run(self):
        date = self.args.date
        year, month, day = date.split("-")
        OUTPUT_NAME = f"ContactNet_StartAGEB_{date.replace('-', '_')}.parquet"
        OUTPUT_PATH = os.path.join(self.args.output, OUTPUT_NAME)

        movement_path = f"{self.args.dataset}/year={year}/month={month}/day={day}/*"
        homefile = f"HomeAssign_StartAGEB_{date.replace('-', '_')}.csv"

        cursor = duckdb.connect()

        # Cargar home assignations
        cursor.execute(f"CREATE TABLE homeAs AS SELECT * FROM read_csv_auto('{homefile}', header=true)")

        # Filtrar a los AGEBs con al menos 50 dispositivos
        cursor.execute("""
            CREATE TABLE homeFiltered AS
            SELECT * FROM homeAs
            JOIN (
                SELECT cvegeo, COUNT(*) AS total
                FROM homeAs
                GROUP BY cvegeo
                HAVING COUNT(*) > 49
            ) AS counts ON homeAs.cvegeo = counts.cvegeo
        """)
        cursor.execute("DROP TABLE homeAs")

        # Cargar pings (sin filtro por cvegeo)
        cursor.execute(f"""
            CREATE TABLE pings AS
            SELECT parquet.caid, parquet.utc_timestamp, parquet.cvegeo, homeFiltered.cvegeo AS home
            FROM read_parquet('{movement_path}') AS parquet
            INNER JOIN homeFiltered ON parquet.caid = homeFiltered.caid
        """)
        cursor.execute("DROP TABLE homeFiltered")

        # Contactos por ventana de 5 minutos
        cursor.execute("SELECT MIN(utc_timestamp) FROM pings")
        min_time = cursor.fetchone()[0]
        fivemin = range(0, 86400, 300)

        cursor.execute("CREATE TABLE export (home1 VARCHAR, home2 VARCHAR, contact VARCHAR, count INTEGER)")

        for i in range(len(fivemin)-1):
            print(f"⏱️ Procesando ventana {i+1}/{len(fivemin)-1}")
            cursor.execute(f"""
                CREATE TABLE tcounts AS (
                    WITH records AS (
                        SELECT CVEGEO, home, COUNT(caid) AS numdev
                        FROM pings
                        WHERE utc_timestamp >= {min_time + fivemin[i]} AND utc_timestamp < {min_time + fivemin[i+1]}
                        GROUP BY CVEGEO, home
                    ),
                    valid AS (
                        SELECT *, COUNT(home) OVER (PARTITION BY CVEGEO) AS numhome
                        FROM records
                        WHERE NOT (numdev = 1 AND numhome = 1)
                    )
                    SELECT h1.home AS home1, h2.home AS home2, h1.CVEGEO AS contact, h1.numdev * h2.numdev AS count
                    FROM valid h1
                    INNER JOIN valid h2 ON h1.CVEGEO = h2.CVEGEO
                    WHERE h1.home >= h2.home
                )
            """)

            if i == 0:
                cursor.execute("INSERT INTO export SELECT * FROM tcounts")
            else:
                cursor.execute("""
                    INSERT INTO export(home1,home2,contact)
                    SELECT home1,home2,contact FROM tcounts
                    EXCEPT SELECT home1,home2,contact FROM export
                    EXCEPT SELECT home2,home1,contact FROM export
                """)
                cursor.execute("UPDATE export SET count = 0 WHERE count IS NULL")
                cursor.execute("""
                    UPDATE export AS ex
                    SET count = ex.count + p.count
                    FROM tcounts AS p
                    WHERE (ex.home1 = p.home1 AND ex.home2 = p.home2 AND ex.contact = p.contact)
                       OR (ex.home1 = p.home2 AND ex.home2 = p.home1 AND ex.contact = p.contact)
                """)
            cursor.execute("DROP TABLE tcounts")

        cursor.execute("DROP TABLE pings")
        cursor.execute(f"COPY export TO '{OUTPUT_PATH}' (FORMAT PARQUET)")
        cursor.execute("DROP TABLE export")
        cursor.close()

        print(f"✅ Red de contacto guardada en: {OUTPUT_PATH}")

if __name__ == '__main__':
    m = Main()
    m.run()

