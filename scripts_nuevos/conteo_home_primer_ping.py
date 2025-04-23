import os
import sys
import duckdb
import argparse

class Main:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-d", "--date", help="Fecha: yyyy-mm-dd")
        parser.add_argument("-db", "--dataset", help="Ruta al dataset parquet")
        parser.add_argument("-o", "--output", help="Directorio de salida")
        self.args = parser.parse_args()

    def run(self):
        date = self.args.date
        year, month, day = date.split("-")
        parquet_path = f"{self.args.dataset}/year={year}/month={month}/day={day}/*"
        output_path = os.path.join(self.args.output, f"conteo_home_primer_ping_{date.replace('-', '_')}.csv")
        os.makedirs(self.args.output, exist_ok=True)

        con = duckdb.connect()

        # Leer todos los pings del día, convertir a timestamp, construir cve_mun_full
        con.execute(f"""
            CREATE TABLE dia AS
            SELECT 
                caid,
                to_timestamp(utc_timestamp) AS utc_time,
                CAST(cve_ent AS VARCHAR) || LPAD(CAST(cve_mun AS VARCHAR), 3, '0') AS cve_mun_full
            FROM read_parquet('{parquet_path}')
            WHERE cve_ent IS NOT NULL AND cve_mun IS NOT NULL
        """)

        # Seleccionar el primer ping por dispositivo
        con.execute("""
            CREATE TABLE primer_ping AS (
                SELECT caid, cve_mun_full
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY caid ORDER BY utc_time ASC) AS rn
                    FROM dia
                )
                WHERE rn = 1
            )
        """)

        # Contar número de dispositivos por municipio
        con.execute("""
            CREATE TABLE conteo AS (
                SELECT cve_mun_full, COUNT(*) AS num_dispositivos
                FROM primer_ping
                GROUP BY cve_mun_full
                ORDER BY num_dispositivos DESC
            )
        """)

        # Exportar resultados
        con.execute(f"""
            COPY conteo TO '{output_path}' (FORMAT CSV, HEADER)
        """)

        print(f"✅ Conteo de home por primer ping exportado: {output_path}")
        con.close()

if __name__ == "__main__":
    m = Main()
    m.run()

