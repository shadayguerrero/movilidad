import argparse
import duckdb
import re
import os
from datetime import date, timedelta

class Main:
    def __init__(self):
        #Opciones del usuario
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-s", "--start", help="Start date for home assign table. Format: yyyy-mm-dd")
        parser.add_argument(
            "-e", "--end", help="End date for home assign table. Format: yyyy-mm-dd")
        parser.add_argument(
            "-db", "--dataset", help="Directory where home assignation csv files are (HomeAssign_StartAGEB_*)")
        parser.add_argument(
            "-o", "--output", help= "Directory for output files.")
        self.args = parser.parse_args()
        
    def run(self):
        #Parseando parametros de entrada
        print("Getting dates")
        sdate = self.args.start
        syear = int(sdate.split("-")[0])
        smonth = int(sdate.split("-")[1])
        sday = int(sdate.split("-")[2])
        
        edate = self.args.end
        eyear = int(edate.split("-")[0])
        emonth = int(edate.split("-")[1])
        eday = int(edate.split("-")[2])
        
        OUTPUT_DIR = self.args.output
        DATASET_MOVEMENT_PATH = self.args.dataset
        OUTPUT_NAME = "JoinHomeAssignation_StartAGEB_"
        OUTPUT_DIR= OUTPUT_DIR+"/"+OUTPUT_NAME+sdate+"_"+edate+'.parquet'
                
        #Dias de interes para la tabla
        sdate = date(syear,smonth,sday)
        edate = date(eyear,emonth,eday) 
        
        days=[sdate+timedelta(days=x) for x in range(((edate + timedelta(days=1))-sdate).days)]
        days=[x.strftime("%Y_%m_%d") for x in days]
        
        days=['HomeAssign_StartAGEB_'+d+'.csv' for d in days]
        
        print("Dates ready")
        
        #Obteniendo nombre de archivos con las tablas de home assignation
        allfiles=[]
                
        for file_path in os.listdir(DATASET_MOVEMENT_PATH):
            if os.path.isfile(os.path.join(DATASET_MOVEMENT_PATH, file_path)) and (file_path in days):
                allfiles.append(os.path.join(DATASET_MOVEMENT_PATH, file_path))

        print ("Files ready")
        
        #Inicializando query
        cursor = duckdb.connect()

        #Creando tabla con todos los dias seleccionados
        instruction='CREATE TABLE export (date VARCHAR, year VARCHAR, month VARCHAR, day VARCHAR, caid VARCHAR, ageb VARCHAR)'
        cursor.execute(instruction)
        
        #Leyendo y guardando las designaciones de home de los dias seleccionados
        for file in allfiles:
            day=re.split('_|\.',file)
            y=day[-4]
            m=day[-3]
            d=day[-2]
            day="-".join([y,m,d])
            instruction='CREATE TABLE day AS SELECT caid,cvegeo FROM read_csv_auto(\''+file+'\',delim = \',\', header = true)'
            cursor.execute(instruction)        
            instruction='INSERT INTO export (caid,ageb) SELECT caid,cvegeo FROM day'
            cursor.execute(instruction)
            cursor.execute('DROP TABLE day')
            instruction='UPDATE export SET date=\''+day+'\' WHERE date IS NULL'
            cursor.execute(instruction)
            instruction='UPDATE export SET year=\''+y+'\' WHERE year IS NULL'
            cursor.execute(instruction)
            instruction='UPDATE export SET month=\''+m+'\' WHERE month IS NULL'
            cursor.execute(instruction)
            instruction='UPDATE export SET day=\''+d+'\' WHERE day IS NULL'
            cursor.execute(instruction)
        #Exportando la tabla de salida
        instruction='COPY export TO \''+OUTPUT_DIR+'\' (FORMAT PARQUET)'
        cursor.execute(instruction)
        cursor.execute('DROP TABLE export')
        cursor.close()

if __name__ == '__main__':

    m = Main()
    m.run()