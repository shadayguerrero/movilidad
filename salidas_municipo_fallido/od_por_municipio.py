import os
import sys
import duckdb
import argparse

class Main:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-d", "--date", help="Fecha: yyyy-mm-dd")
        parser.add_argument("-db", "--dataset", help="Directorio con los parquet")
        parser.add_argument("-o", "--output", help="Directorio para CSV de salida")
        self.args = parser.parse_args()

    def run(self):
        date = self.args.date
        year, month, day = date.split("-")
        parquet_path = f"{self.args.dataset}/year={year}/month={month}/day={day}/*"
        output_path = os.path.join(self.args.output, f"od_municipio_{date.replace('-', '_')}.csv")

        con = duckdb.connect()

        con.execute(f"""
            CREATE TABLE raw AS
            SELECT caid, utc_timestamp, cve_mun
            FROM read_parquet('{parquet_path}')
            WHERE cve_mun IS NOT NULL
        """)

        con.execute("""
            CREATE TABLE ordered AS (
                SELECT *, 
                       ROW_NUMBER() OVER (PARTITION BY caid ORDER BY utc_timestamp) as rn
                FROM raw
            )
        """)

        con.execute("""
            CREATE TABLE pares AS (
                SELECT 
                    a.caid,
                    a.cve_mun AS origen,
                    b.cve_mun AS destino
                FROM ordered a
                JOIN ordered b ON a.caid = b.caid AND a.rn + 1 = b.rn
                WHERE a.cve_mun != b.cve_mun
            )
        """)

        con.execute("""
            CREATE TABLE od_summary AS
            SELECT origen, destino, COUNT(*) AS peso
            FROM pares
            GROUP BY origen, destino
            ORDER BY peso DESC
        """)

        con.execute(f"COPY od_summary TO '{output_path}' (HEADER, FORMAT CSV)")

        print(f"✅ Red OD generada: {output_path}")
        con.close()

if __name__ == "__main__":
    m = Main()
    m.run()

