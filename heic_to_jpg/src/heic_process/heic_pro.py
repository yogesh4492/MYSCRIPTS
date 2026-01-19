from PIL import Image
from pillow_heif import register_heif_opener
from rich.progress import Progress
import subprocess
import glob
import os
import typer
from concurrent.futures import ThreadPoolExecutor,as_completed
from rich.progress import Progress,SpinnerColumn,BarColumn,TextColumn,TimeElapsedColumn,TimeRemainingColumn,MofNCompleteColumn

app=typer.Typer()


def process_images_files(file,output_folder):
    result=get_file_extension(file)
    os.makedirs(output_folder,exist_ok=True)
    base_name=os.path.basename(file.split(".")[0])
    heic_ext=["heic","heif"]
    jpg_ext=["jpg",'jpeg',"png"]
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
    else:
         print(f"{file}  files are not type of jpg and heic")

def get_file_extension(file):

    result=subprocess.run(
        ["exiftool","-FILEtype","-s3",file],
        capture_output=True,
        text=True
    )
    return result.stdout.strip().lower()

@app.command()
def main(input_file:str=typer.Argument(...,help="Input file")
         ,output_dir:str=typer.Option("JAN16","--output","-o",help="output directory")):
    process_images_files(input_file,output_dir)


@app.command("Multiple_file")
def main(input_dir:str=typer.Argument(...,help="Input files "),
         output_dir:str=typer.Option("JAN16")):
    files=glob.glob(f"{input_dir}/**/*",recursive=True)
    progress=Progress(
            SpinnerColumn(),
            TextColumn("{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn()
    )

    with progress:
        task=progress.add_task("processing....",total=len(files))

        with ThreadPoolExecutor(max_workers=8) as e:
            future={e.submit(process_images_files,f,output_dir):f for f in files}
            for i in as_completed(future):
                # print(i.result())
                progress.update(task,advance=1)


if __name__=="__main__":
    app()

