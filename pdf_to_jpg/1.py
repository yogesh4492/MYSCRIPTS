import typer
import csv
from pdf2image import convert_from_path
import glob
import os
app=typer.Typer()

@app.command()
def main(input_file:str=typer.Argument(...,help="input Pdf File PAth"),
         output_dir:str=typer.Option("JAn16","--output","-o",help="output dir to store jpg file")):
    images=convert_from_path(input_file,dpi=300)
    print(len(images))
    basename=os.path.basename(input_file.split(".")[0])
    print(basename)
    for i,j in enumerate(images,start=1):
        if i==1:
            j.save(f"{basename}.jpg","JPEG",quality=10)

if __name__=="__main__":
    app()