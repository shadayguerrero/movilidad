import os
import sys
import duckdb
import argparse

class Main:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-d", "--date", help="Fecha: yyyy-mm-dd")
        parser.add_argument("-db", "--dataset", help="Directorio parquet")
        parser.add_argument("-o", "--output", help="Directorio para CSV")
        self.args = parser.parse_args()

    def run(self):
        date = self.args.date
        year, month, day = date.split("-")
        parquet_path = f"{self.args.dataset}/year={year}/month={month}/day={day}/*"
        output_path = os.path.join(self.args.output, f"dispositivos_municipio_{date.replace('-', '_')}.csv")
        os.makedirs(self.args.output, exist_ok=True)

        con = duckdb.connect()

        # Leer datos, generar clave única de municipio
        con.execute(f"""
            CREATE TABLE pings AS
            SELECT 
                caid, 
                utc_timestamp, 
                CAST(cve_ent AS VARCHAR) || '_' || CAST(cve_mun AS VARCHAR) AS municipio
            FROM read_parquet('{parquet_path}')
            WHERE cve_mun IS NOT NULL AND cve_ent IS NOT NULL
        """)

        # Seleccionar primer ping por dispositivo
        con.execute("""
            CREATE TABLE first_ping AS (
                SELECT caid, municipio
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY caid ORDER BY utc_timestamp) AS rn
                    FROM pings
                )
                WHERE rn = 1
            )
        """)

        # Contar cuántos dispositivos por municipio
        con.execute("""
            CREATE TABLE resumen AS
            SELECT municipio, COUNT(DISTINCT caid) AS num_dispositivos
            FROM first_ping
            GROUP BY municipio
            ORDER BY num_dispositivos DESC
        """)

        # Exportar
        con.execute(f"COPY resumen TO '{output_path}' (FORMAT CSV, HEADER)")
        print(f"✅ Dispositivos por municipio exportados: {output_path}")

        con.close()

if __name__ == "__main__":
    m = Main()
    m.run()

