import subprocess 
import glob
import os
import typer 
from PIL import Image
from pillow_heif import register_heif_opener
from concurrent.futures import ThreadPoolExecutor,as_completed
from pdf2image import convert_from_path
from rich.progress import Progress,SpinnerColumn,BarColumn,TextColumn,TimeElapsedColumn,TimeRemainingColumn,MofNCompleteColumn
register_heif_opener()

app=typer.Typer()


@app.command("single_file")
def main(input_file:str=typer.Argument(...,help="Input_file name"),
         output_folder:str=typer.Argument(...,help="output folder name")):
     process_image_files(input_file,output_folder)
    
def detect_type(file_path):
    try:
        result=subprocess.run(
             ['exiftool',"-FILEtype","-s3",file_path],
             capture_output=True,
             text=True
        )
        return result.stdout.strip().lower()
    except Exception:
         return "Unknown"
        
def process_image_files(file,output_folder):
    result=detect_type(file)
    os.makedirs(output_folder,exist_ok=True)
    base_name=os.path.basename(file.split(".")[0])
    pdf_ext=['pdf']
    heic_ext=["heic","heif"]
    jpg_ext=["jpg",'jpeg']
    if result in heic_ext:
         img=Image.open(file).convert("RGB")
         output_name=base_name+".jpg"
         output_path=os.path.join(
              output_folder,
              output_name
         )
         img.save(output_path,"JPEG",quality=95)
         print(f"Converted {base_name}  --> {os.path.basename(output_path)}")  
    elif result in jpg_ext:
         img=Image.open(file).convert("RGB")
         output_name=base_name+".jpg"
         output_path=os.path.join(
              output_folder,
              output_name
         )
         img.save(output_path,"JPEG",quality=95)
         print(f"Converted {base_name}  --> {os.path.basename(output_path)}")
    elif result in pdf_ext:
         images=convert_from_path(file,dpi=300)
         basename=os.path.basename(file.split(".")[0])
         outname=os.path.join(output_folder,basename)
         for i,j in enumerate(images,start=1):
              if i==1:
               j.save(f"{outname}.jpg","JPEG",quality=95)
               print(f"Converted {basename}  --> {os.path.basename(outname)}.jpg")

@app.command("multiple_files")
def main(input_dir:str=typer.Argument(...,help='input Directory that Contain files'),
         output_dir:str=typer.Option("output","--output","-o",help="output Directory For Image files")):
        files=glob.glob(f"{input_dir}/**/*",recursive=True)
        print(files)
        process=Progress(
             SpinnerColumn(),
             TextColumn("{task.description}"),
             BarColumn(),
             MofNCompleteColumn(),
             TimeRemainingColumn(),
             TimeElapsedColumn()
        )

        with process:
             task=process.add_task("processing...",total=len(files))
             with ThreadPoolExecutor(max_workers=8) as tpe:
                  future={tpe.submit(process_image_files(f,output_dir)):f for f in files}
                  for i in as_completed(future):
                       process.update(task,advance=1)

                    
                  

if __name__=="__main__":
    app()