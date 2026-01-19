from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import csv
from PIL import Image
from pillow_heif import register_heif_opener
import typer
from rich.progress import Progress,SpinnerColumn,BarColumn,TextColumn
from concurrent.futures import ThreadPoolExecutor,as_completed


app=typer.Typer()

def auth():
    pass

def get_files():
    pass

def read_csv(file):
    pass

@app.command("Single_file")
def main(input_file:str=typer.Argument(...,help="Input file path/name"),
         output_file:str=typer.Argument(...,help="Output File name or also directory to store the converted file")):
    pass
    

@app.command("Multiple_file")
def main():
    pass

@app.command("Gdrive_files_csv")
def main():
    pass


if __name__=="__main__":
    app()