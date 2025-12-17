import json
import os
import typer
from concurrent.futures import ThreadPoolExecutor,as_completed
from rich.progress import Progress
app=typer.Typer()


def set_value(old,new):
    global OLD_VALUE,NEW_VALUE
    OLD_VALUE=old
    NEW_VALUE=new


def update_language(obj):
    if isinstance(obj,dict):
        new_dict={}
        for i,j in obj.items():
            new_key=i.replace(OLD_VALUE,NEW_VALUE) if isinstance(i,str) else i
            new_dict[new_key]=update_language(j)
        return  new_dict
    
    elif isinstance(obj,list):
        return [update_language(i) for i in  obj]
    elif isinstance(obj,str):
        return obj.replace(OLD_VALUE,NEW_VALUE)
    else:
        return obj

def dump_json(file,data):
    with open(file,"w",encoding="utf-8") as write:
        json.dump(data,write,indent=4,ensure_ascii=False) 

def read_json(file):
    with open(file,"r") as read:
        return json.load(read)
@app.command()
def main(
    input_dir:str=typer.Argument(...,help="Input Json Directory "),
    output_dir:str=typer.Argument(...,help="Output Json Directory For Updated json"),
    old_language:str=typer.Argument(...,help="Input Old Language "),
    new_language:str=typer.Argument(...,help="New language for replace with old")

):
    set_value(old_language,new_language)
    os.makedirs(output_dir,exist_ok=True)
    # files=list(input_dir.rglob("*.json"))

    files=os.listdir(input_dir)
    for i in files:
        data=read_json(os.path.join(input_dir,i))
        updated_data=update_language(data)
        dump_json(os.path.join(output_dir,i),updated_data)
    typer.echo("Json Files Updated Successfully.....")


if __name__=="__main__":
    app()