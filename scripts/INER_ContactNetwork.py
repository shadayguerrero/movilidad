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
            "-d", "--date", help="Selected day for contact network. Format: yyyy-mm-dd")
        parser.add_argument(
            "-db", "--dataset", help="Directory where parquet dataset with daily movement files are")
        parser.add_argument(
            "-a","--agebs", help="Csv file with path of agebs to be considered. Must have a column name CVEGEO")
        parser.add_argument(
            "-o", "--output", help= "Directory for output networks.")
        self.args = parser.parse_args()


    def run(self):
        #Vaciando argumentos
        date = self.args.date
        year = date.split("-")[0]
        month = date.split("-")[1]
        day = date.split("-")[2]
        OUTPUT_DIR = self.args.output
        agebs=self.args.agebs
        DATASET_MOVEMENT_PATH = self.args.dataset
        homefile='_'.join(['HomeAssign_StartAGEB',year,month,day+'.csv'])
        file_path=DATASET_MOVEMENT_PATH+"/year="+year+"/month="+month+"/day="+day+"/*"
        
        #Nombre del archivo de salida
        OUTPUT_NAME = "ContactNet_StartAGEB"
        OUTPUT_NAME= '_'.join([OUTPUT_NAME, date]).replace("-", "_")
        OUTPUT_DIR= os.path.join(OUTPUT_DIR, OUTPUT_NAME + '.parquet')
        
        #Inicializando query
        cursor = duckdb.connect()
        
        #Cargando tabla con los datos de los home asignados para el dia seleccionado
        instruction='CREATE TABLE temp AS SELECT * FROM read_csv_auto(\''+homefile+'\',header = true)'
        cursor.execute(instruction)
        
        #Haciendo la subseleccion de aquellos homes que tengan al menos 50 dispositivos asignados
        instruction='CREATE TABLE homeAs AS SELECT * FROM temp JOIN (SELECT cvegeo, total FROM (SELECT cvegeo,COUNT(caid) AS total FROM temp GROUP BY cvegeo) WHERE total > 49) AS counts ON temp.cvegeo=counts.cvegeo'
        cursor.execute(instruction)
        
        #Ekiminando tabla anterior
        cursor.execute('DROP TABLE temp')
        
        #Cargando los agebs que son parte de la zona metropolitana
        agebs=pd.read_csv(agebs)
        agebs=list(agebs.CVEGEO)
        
        #Preparando las condicionles de la columna de posicion agebs
        agebs='\',\''.join(agebs)
        agebs='\''+agebs+'\''
        
        #Cargando lecturas del dia, seleccionando solo aquellos dispositivos que aparecen dentro del home
        instruction='CREATE TABLE pings AS SELECT parquet.caid,parquet.utc_timestamp,parquet.CVEGEO, homeAs.cvegeo AS home FROM read_parquet(\''+file_path+'\') AS parquet INNER JOIN homeAs ON parquet.caid=homeAs.caid WHERE parquet.cvegeo IN ('+agebs+')'
        cursor.execute(instruction)
        
        cursor.execute('DROP TABLE homeAs')
        #Inicializando tabla de cuantos dispositivos se encuentran en un mismo tiempo en un ageb
        instruction='CREATE TABLE export (home1 VARCHAR, home2 VARCHAR, contact VARCHAR, count INTEGER)'
        cursor.execute(instruction)
        
        instruction='SELECT MIN(utc_timestamp) FROM pings'
        cursor.execute(instruction)
        time=cursor.fetchall()
        time=time[0][0]
        
        fivemin=range(0,86400,300)
        
        for i in range(len(fivemin)-1):
            instruction='CREATE TABLE tcounts AS (WITH records AS (SELECT * FROM (SELECT *, COUNT(home) OVER (PARTITION BY CVEGEO) AS numhome FROM (SELECT CVEGEO, home, COUNT(caid) AS numdev FROM pings WHERE (utc_timestamp >='+str(time+fivemin[i])+' AND utc_timestamp < '+str(time+fivemin[i+1])+') GROUP BY CVEGEO,home)) WHERE NOT (numhome=1 AND numdev=1)) SELECT h1.home AS home1, h2.home AS home2, h1.CVEGEO AS contact, h1.numdev*h2.numdev AS count FROM records AS h1 INNER JOIN records AS h2 ON h1.CVEGEO=h2.CVEGEO WHERE h1.home >= h2.home)'
            cursor.execute(instruction)            
            if i == 0:
                instruction='INSERT INTO export SELECT * FROM tcounts'
                cursor.execute(instruction)
            else:
                instruction='INSERT INTO export(home1,home2,contact) (SELECT home1,home2,contact FROM tcounts EXCEPT SELECT home1,home2,contact FROM export) EXCEPT SELECT home2,home1,contact FROM export'
                cursor.execute(instruction)
                instruction='UPDATE export SET count=0 WHERE count IS NULL'
                cursor.execute(instruction)
                instruction='UPDATE export AS ex SET count=ex.count+p.count FROM tcounts AS p WHERE (ex.home1=p.home1 AND ex.home2=p.home2 AND ex.contact=p.contact) OR (ex.home1=p.home2 AND ex.home2=p.home1 AND ex.contact=p.contact)' 
                cursor.execute(instruction)
            cursor.execute('DROP TABLE tcounts')
        
        cursor.execute('DROP TABLE pings')
        
        #Exportando la tabla de salida
        instruction='COPY export TO \''+OUTPUT_DIR+'\' (FORMAT PARQUET)'
        cursor.execute(instruction)
        cursor.execute('DROP TABLE export')        
        cursor.close()
        

if __name__ == '__main__':

    m = Main()
    m.run()
