import os
import sys
import duckdb
import argparse

class Main:
    def __init__(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("-d", "--date", help="Fecha: yyyy-mm-dd")
        parser.add_argument("-db", "--dataset", help="Ruta a los parquet")
        parser.add_argument("-o", "--output", help="Directorio de salida")
        self.args = parser.parse_args()

    def run(self):
        date = self.args.date
        year, month, day = date.split("-")
        parquet_path = f"{self.args.dataset}/year={year}/month={month}/day={day}/*"
        output_path = os.path.join(self.args.output, f"conteo_home_{date.replace('-', '_')}.csv")
        os.makedirs(self.args.output, exist_ok=True)

        con = duckdb.connect()

        # Parte 1: Pings entre 00:00 y 08:00
        con.execute(f"""
            CREATE TABLE noche AS
            SELECT 
                caid, 
                to_timestamp(utc_timestamp) AS utc_time,
                CAST(cve_ent AS VARCHAR) || LPAD(CAST(cve_mun AS VARCHAR), 3, '0') AS cve_mun_full
            FROM read_parquet('{parquet_path}')
            WHERE cve_ent IS NOT NULL AND cve_mun IS NOT NULL
              AND to_timestamp(utc_timestamp) >= TIMESTAMP '{date} 00:00:00'
              AND to_timestamp(utc_timestamp) < TIMESTAMP '{date} 05:00:00'
        """)

        con.execute("""
            CREATE TABLE noche_ordenada AS (
                SELECT *, 
                       LEAD(utc_time) OVER (PARTITION BY caid ORDER BY utc_time) AS siguiente,
                       LEAD(cve_mun_full) OVER (PARTITION BY caid ORDER BY utc_time) AS cve_sig
                FROM noche
            )
        """)

        con.execute("""
            CREATE TABLE noche_duracion AS (
                SELECT 
                    caid, 
                    cve_mun_full,
                    DATEDIFF('second', utc_time, siguiente) AS duracion
                FROM noche_ordenada
                WHERE siguiente IS NOT NULL AND cve_mun_full = cve_sig
            )
        """)

        con.execute("""
            CREATE TABLE noche_resumen AS (
                SELECT caid, cve_mun_full, SUM(duracion) AS tiempo_total
                FROM noche_duracion
                GROUP BY caid, cve_mun_full
            )
        """)

        con.execute("""
            CREATE TABLE noche_home AS (
                SELECT caid, cve_mun_full
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY caid ORDER BY tiempo_total DESC) AS rn
                    FROM noche_resumen
                )
                WHERE rn = 1
            )
        """)

        # Parte 2: Primer ping del día para los que no tienen datos en la noche
        con.execute(f"""
            CREATE TABLE dia AS
            SELECT 
                caid, 
                to_timestamp(utc_timestamp) AS utc_time,
                CAST(cve_ent AS VARCHAR) || LPAD(CAST(cve_mun AS VARCHAR), 3, '0') AS cve_mun_full
            FROM read_parquet('{parquet_path}')
            WHERE cve_ent IS NOT NULL AND cve_mun IS NOT NULL
        """)

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

        con.execute("""
            CREATE TABLE dia_home AS (
                SELECT p.*
                FROM primer_ping p
                LEFT JOIN noche_home n ON p.caid = n.caid
                WHERE n.caid IS NULL
            )
        """)

        # Unión de ambas
        con.execute("""
            CREATE TABLE home_final AS (
                SELECT * FROM noche_home
                UNION ALL
                SELECT * FROM dia_home
            )
        """)

        # Conteo por municipio
        con.execute("""
            CREATE TABLE conteo AS (
                SELECT cve_mun_full, COUNT(*) AS num_dispositivos
                FROM home_final
                GROUP BY cve_mun_full
                ORDER BY num_dispositivos DESC
            )
        """)

        # Exportar
        con.execute(f"""
            COPY conteo TO '{output_path}' (FORMAT CSV, HEADER)
        """)
        print(f"✅ Home diario exportado: {output_path}")
        con.close()

if __name__ == "__main__":
    m = Main()
    m.run()

