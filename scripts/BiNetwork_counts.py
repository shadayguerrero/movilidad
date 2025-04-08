import argparse
import logging
import os
import sys
import duckdb
import re

class Main:
    def __init__(self):
        #Opciones del usuario
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-c", "--counts", help="Parquet file with Bipartite network info for selected month. Ej: BiNetwork_StartAGEB_yyyy_mm.parquet ")
        parser.add_argument(
            "-cl", "--clusters", help="Csv file with clusters of interest for network.")
        parser.add_argument(
            "-o", "--output", help= "Directory for output file.")
        self.args = parser.parse_args()


    def run(self):
        #Vaciando argumentos
        BiNet = self.args.counts
        year=BiNet.split("_")[2]
        month=re.split("_|\.",BiNet)[3]
        clusterfile = self.args.clusters
        OUTPUT_DIR = self.args.output
        
        #Nombre del archivo de salida
        OUTPUT_NAME = "ConcurrenceClusterPlaces"
        OUTPUT_NAME= '_'.join([OUTPUT_NAME,year,month])
        OUTPUT_DIR= os.path.join(OUTPUT_DIR, OUTPUT_NAME)
        
        #Inicializando query
        cursor = duckdb.connect()
        
        #Cargando tabla con los datos de la red bipartita por dia
        instruction='CREATE TABLE counts AS SELECT * FROM read_parquet(\''+BiNet+'\')'
        cursor.execute(instruction)
        
        #Haciendo las cuentas totales de todo el mes para los pares home-AGEB
        instruction='CREATE TABLE BiNet AS SELECT home,location, SUM(numpings) AS numpings FROM counts GROUP BY home,location'
        cursor.execute(instruction)
        cursor.execute('DROP TABLE counts')
        
        #Cargando datos de los clusters de interes
        instruction='CREATE TABLE clusters AS SELECT * FROM read_csv(\''+clusterfile+'\',AUTO_DETECT=TRUE)'
        cursor.execute(instruction)
        
        #Combinando ambas tablas
        instruction='CREATE TABLE joinAGEB AS SELECT net.home AS home,net.location AS location ,net.numpings,cl.cluster_analisisMarina AS cluster FROM BiNet AS net LEFT JOIN (SELECT cluster_analisisMarina, CVEGEO FROM clusters GROUP BY cluster_analisisMarina,CVEGEO) AS cl ON net.home=cl.CVEGEO'
        cursor.execute(instruction)
        cursor.execute('DROP TABLE clusters')
        
        #normalizando conteos de visita por el numero total de visitas que recibe ese AGEB
        instruction='CREATE TABLE normC AS SELECT location,SUM(numpings) AS norm FROM BiNet GROUP BY location'
        cursor.execute(instruction)
        
        #Borrando la tabla de cuentas totales por dia por AGEB para liberar espacio
        cursor.execute('DROP TABLE BiNet')
        
        #normalizando conteos de visita por el numero total de visitas que recibe ese AGEB
        cursor.execute('ALTER TABLE joinAGEB ADD COLUMN norm_numpings DOUBLE')
        instruction='UPDATE joinAGEB SET norm_numpings=joinAGEB.numpings/normC.norm FROM normC WHERE joinAGEB.location=normC.location'
        cursor.execute(instruction)
        
        #Exportando la tabla de salida de numero de viajes por AGEB
        instruction='COPY joinAGEB TO \''+OUTPUT_DIR+'_byAGEB.parquet\' (FORMAT PARQUET)'
        cursor.execute(instruction)
        
        #Haciendo tabla de conteos por cluster
        instruction='CREATE TABLE joincluster AS SELECT cluster, location, SUM(numpings) AS numpings FROM joinAGEB GROUP BY cluster,location,numpings'
        cursor.execute(instruction)
        
        #Borrando la tabla de conteos por AGEB para liberar espacio
        cursor.execute('DROP TABLE joinAGEB')
        
        #normalizando conteos de visita de la tabla por clusters
        cursor.execute('ALTER TABLE joincluster ADD COLUMN norm_numpings DOUBLE')
        instruction='UPDATE joincluster SET norm_numpings=joincluster.numpings/normC.norm FROM normC WHERE joincluster.location=normC.location'
        cursor.execute(instruction)
        
        #Borrando la tabla de conteos por AGEB para liberar espacio
        cursor.execute('DROP TABLE normC')
                
        #Exportando la tabla de salida de numero de viajes por AGEB
        instruction='COPY joincluster TO \''+OUTPUT_DIR+'_byCluster.parquet\' (FORMAT PARQUET)'
        cursor.execute(instruction)
        cursor.execute('DROP TABLE joincluster')
        cursor.close()
        

if __name__ == '__main__':

    m = Main()
    m.run()
