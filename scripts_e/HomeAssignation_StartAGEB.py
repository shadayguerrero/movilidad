import argparse
import logging
import os
import sys
import duckdb
import pandas as pd

class Main:
    def __init__(self):
        #Opciones del usuario
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-d", "--date", help="Start date of 24 hour period of observational unit. Format: yyyy-mm-dd")
        parser.add_argument(
            "-db", "--dataset", help="Directory where parquet dataset files are")
        parser.add_argument(
            "-o", "--output", help= "Directory for output networks.")
        parser.add_argument(
            "-a","--agebs", help="Csv file with path of agebs to be considered. Must have a column name CVEGEO")
        self.args = parser.parse_args()


    def run(self):
    #Inicializando variables del usuario y paths
        date = self.args.date
        year = date.split("-")[0]
        month = date.split("-")[1]
        day = date.split("-")[2]
        OUTPUT_DIR = self.args.output
        DATASET_MOVEMENT_PATH = self.args.dataset
        agebs=self.args.agebs
        OUTPUT_NAME = "HomeAssign_StartAGEB"
        OUTPUT_NAME= '_'.join([OUTPUT_NAME, date]).replace("-", "_")
        OUTPUT_DIR= os.path.join(OUTPUT_DIR, OUTPUT_NAME + '.csv')
    
        DATASET_MOVEMENT_PATH=DATASET_MOVEMENT_PATH+"/year="+year+"/month="+month+"/day="+day+"/*"
    
    #Inicializando query
        cursor = duckdb.connect()

    #Cargando los agebs que son parte de la zona metropolitana
        agebs=pd.read_csv(agebs)
        agebs=list(agebs.CVEGEO)
    
    #Preparando las condicionles de la columna de posicion agebs
        agebs='\',\''.join(agebs)
        agebs='\''+agebs+'\''
    
    #Tabla de datos solo de la ZMVM
        instruction='CREATE TABLE zmvm AS SELECT * FROM read_parquet(\''+DATASET_MOVEMENT_PATH+'*\') WHERE cvegeo IN ('+agebs+')'
        cursor.execute(instruction)
    
    #Organizar los pings por caid y por hora, despues seleccionar la primera lectura de cada caid, hacer una tabla nueva con esta seleccion
        instruction='CREATE TABLE first AS (WITH records AS (SELECT caid,utc_timestamp,cvegeo,ROW_NUMBER() OVER(PARTITION BY caid ORDER BY utc_timestamp ASC) rn FROM zmvm) SELECT * FROM records WHERE RN=1)'
        cursor.execute(instruction)
    
    #Eliminar la primera tabla de todos los datos de zmvm
        cursor.execute('DROP TABLE zmvm')
    
   #Creando tabla solo con los caids y los home de esos ids
        instruction='CREATE TABLE home AS SELECT caid, cvegeo FROM first'
        cursor.execute(instruction)
        
        #Cuantos dispositivos hay en ese dia
        instruction='SELECT COUNT(DISTINCT caid),COUNT(DISTINCT cvegeo) FROM first'
        cursor.execute(instruction)
        allcaid=cursor.fetchall()
        print("devices/homes")
        print(allcaid)

    #Eliminar la tabla con todos los datos de las primeras lecturas del dia
        cursor.execute('DROP TABLE first')
        
    #Exportando la tabla de salida
        instruction='COPY home TO \''+OUTPUT_DIR+'\' (FORMAT CSV,HEADER)'
        cursor.execute(instruction)
        cursor.execute('DROP TABLE home')
    #Cerrando cursor
        cursor.close()

if __name__ == '__main__':

    m = Main()
    m.run()
