import argparse
import os
import sys
import subprocess

year=["2020","2021"]
month=[str(i).zfill(2) for i in range(1,13)]
day=[str(i).zfill(2) for i in range(1,32)]

parser = argparse.ArgumentParser()
parser.add_argument("-db", "--dataset", help="Directory where parquet dataset files are.")
parser.add_argument("-a","--agebs", help="Csv file with path of agebs to be considered. Must have a column name CVEGEO")
parser.add_argument("-o", "--output", help= "Directory for output networks.")
args = parser.parse_args()

OUTPUT_DIR = args.output
DATASET_MOVEMENT_PATH = args.dataset
agebs=args.agebs

for y in year:
    for m in month:
        for d in day:
            DATASET_MOVEMENT_PATH_DATE=DATASET_MOVEMENT_PATH+"/year="+y+"/month="+m+"/day="+d+"/"
            if os.path.exists(DATASET_MOVEMENT_PATH_DATE):
                date=y+"-"+m+"-"+d
                subprocess.run(["python","HomeAssignation_StartAGEB.py","-d",date,"-db",DATASET_MOVEMENT_PATH,"-o",OUTPUT_DIR,"-a",agebs],check=True)
            
        
    

