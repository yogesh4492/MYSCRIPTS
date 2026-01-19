import csv
import os

def read_csv(file):
    if os.path.exists(file):
        with open(file,"r") as cr:
            return list(csv.DictReader(cr))
        

    
