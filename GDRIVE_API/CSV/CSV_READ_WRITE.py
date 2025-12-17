import csv

def read_csv(File):
    with open(File,"r") as read:
        return  list(csv.DictReader(read))
    
def write_csv(File,Fields,Data):
    with open(File,"w") as write:
        Write=csv.DictWriter(write,fieldnames=Fields)
        Write.writeheader()
        Write.writerows(Data)

    

