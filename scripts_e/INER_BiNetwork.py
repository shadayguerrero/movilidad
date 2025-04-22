import argparse
import logging
import os
import sys
import duckdb
from datetime import date, timedelta
import calendar
import pandas as pd

class Main:
    def __init__(self):
        #Opciones del usuario
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-d", "--date", help="Selected month for bipartite network. Format: yyyy-mm")
        parser.add_argument(
            "-db", "--dataset", help="Directory where parquet dataset with daily movement files are")
        parser.add_argument(
            "-o", "--output", help= "Directory for output networks.")
        parser.add_argument(
            "-ha","--home", help="Csv file with home designation for selected month.")
        self.args = parser.parse_args()


    def run(self):
        #Vaciando argumentos
        date = self.args.date
        year = date.split("-")[0]
        month = date.split("-")[1]
        OUTPUT_DIR = self.args.output
        DATASET_MOVEMENT_PATH = self.args.dataset
        homefile=self.args.home
        
        #Nombre del archivo de salida
        OUTPUT_NAME = "BiNetwoek_StartAGEB"
        OUTPUT_NAME= '_'.join([OUTPUT_NAME, date]).replace("-", "_")
        OUTPUT_DIR= os.path.join(OUTPUT_DIR, OUTPUT_NAME + '.parquet')
        
        #Inicializando query
        cursor = duckdb.connect()
        
        #Cargando tabla con los datos de los home asignados
        instruction='CREATE TABLE homeAs AS SELECT * FROM read_parquet(\''+homefile+'\')'
        cursor.execute(instruction)
        
        #Dias en el mes seleccionado
        ndays = calendar.monthrange(int(year), int(month))[1]
        ndays = [str(i).zfill(2) for i in range(1,ndays+1)]
        
        #Inicializando tabla de cuantas veces aparece un dispositivo en un ageb
        instruction='CREATE TABLE export (caid VARCHAR, day VARCHAR, home VARCHAR, location VARCHAR, numpings INTEGER)'
        cursor.execute(instruction)
        
        #Obteniendo datos por dia del mes seleccionado
        allfiles=[]
        for day in ndays:
            #Path a los parquets del dia
            file_path=DATASET_MOVEMENT_PATH+"/year="+year+"/month="+month+"/day="+day+"/*"
            print("Working on "+day)
            instruction='CREATE TABLE hdevices AS SELECT * FROM homeAs WHERE day=\''+day+'\'' 
            cursor.execute(instruction)
            
            #Cargando lecturas del dia, seleccionando solo aquellos dispositivos que aparecen dentro del home
            instruction='CREATE TABLE pings AS SELECT parquet.caid,parquet.utc_timestamp,parquet.CVEGEO FROM read_parquet(\''+file_path+'\') AS parquet INNER JOIN hdevices ON parquet.caid=hdevices.caid'
            cursor.execute(instruction)
            
            
            #Conteos de cuantas veces pasa un dispositivo en un ageb, agregando la columa de su ageb home
            instruction='CREATE TABLE temp AS SELECT counts.caid AS caid, hdevices.ageb AS home, counts.CVEGEO AS location,counts.num AS numpings FROM (SELECT caid, CVEGEO, COUNT(CVEGEO) AS num FROM pings GROUP BY caid, CVEGEO) AS counts JOIN hdevices ON counts.caid=hdevices.caid'
            cursor.execute(instruction)
            
            cursor.execute('DROP TABLE hdevices')
            
            cursor.execute('DROP TABLE pings')
            instruction='INSERT INTO export BY NAME(SELECT * FROM temp)'
            cursor.execute(instruction)
            cursor.execute('DROP TABLE temp')
            instruction='UPDATE export SET day=\''+day+'\' WHERE day IS NULL'
            cursor.execute(instruction)
        
        #Exportando la tabla de salida
        instruction='COPY export TO \''+OUTPUT_DIR+'\' (FORMAT PARQUET)'
        cursor.execute(instruction)
        cursor.execute('DROP TABLE export')
        cursor.close()
        

if __name__ == '__main__':

    m = Main()
    m.run()
