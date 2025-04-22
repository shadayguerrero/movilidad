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
        output_path = os.path.join(self.args.output, f"od_municipio_{date.replace('-', '_')}.csv")
        os.makedirs(self.args.output, exist_ok=True)

        con = duckdb.connect()

        # Leer datos con claves compuestas
        con.execute(f"""
            CREATE TABLE raw AS
            SELECT 
                caid, 
                utc_timestamp, 
                CAST(cve_ent AS VARCHAR) || '_' || CAST(cve_mun AS VARCHAR) AS municipio
            FROM read_parquet('{parquet_path}')
            WHERE cve_mun IS NOT NULL AND cve_ent IS NOT NULL
        """)

        # Ordenar pings por caid
        con.execute("""
            CREATE TABLE ordered AS (
                SELECT *, 
                       ROW_NUMBER() OVER (PARTITION BY caid ORDER BY utc_timestamp) AS rn
                FROM raw
            )
        """)

        # Extraer pares consecutivos origen → destino
        con.execute("""
            CREATE TABLE pares AS (
                SELECT 
                    a.caid,
                    a.municipio AS origen,
                    b.municipio AS destino
                FROM ordered a
                JOIN ordered b 
                  ON a.caid = b.caid 
                 AND a.rn + 1 = b.rn
                WHERE a.municipio != b.municipio
            )
        """)

        # Agregar y contar ocurrencias
        con.execute("""
            CREATE TABLE od_summary AS
            SELECT origen, destino, COUNT(*) AS peso
            FROM pares
            GROUP BY origen, destino
            ORDER BY peso DESC
        """)

        # Guardar salida
        con.execute(f"COPY od_summary TO '{output_path}' (FORMAT CSV, HEADER)")
        print(f"✅ Red OD exportada: {output_path}")

        con.close()

if __name__ == "__main__":
    m = Main()
    m.run()

