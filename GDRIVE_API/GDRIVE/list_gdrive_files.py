# from googleapiclient.discovery import build
# from google_auth_oauthlib.flow import InstalledAppFlow
# from google.auth.transport.requests import Request
# from googleapiclient.http import MediaFileUpload
# from googleapiclient.http import MediaIoBaseDownload
# from googleapiclient.http import MediaIoBaseUpload
# from GDRIVE.authentication import auth
# import csv
# import typer
# import os
# import pickle
# import json
# from rich.progress import Progress
# from concurrent.futures import ThreadPoolExecutor

# app=typer.Typer()
def get_folder_name(id,service):
    folder=service.files().get(
        fileId=id,
        fields="name"

    ).execute()
    return folder.get("name")

def list_files(folder_id,service):
    files=[]
    page_Token=None
    while True:
        resp=service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            spaces="drive",
            fields="nextPageToken,files(parents,id,name,mimeType,size,webViewLink)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=page_Token

        ).execute()
        for i in resp.get("files",[]):
            if i.get("mimeType")=="application/vnd.google-apps.folder":
                files.extend(list_files(i['id'],service))
            else:
                files.append(i)
        page_Token=resp.get("nextPageToken")
        if not page_Token:
            break
    return files

# def dump_csv(filename,data,fields):
#     with open(filename,"w") as w:
#         csw=csv.DictWriter(w,fieldnames=fields)
#         csw.writeheader()
#         csw.writerows(data)



# @app.command()
# def main(folder_id:str=typer.Argument(...,help="Folder Id In The "),output_csv:str=typer.Option("Files_Metadata.csv","--output",help="Output csv name")):
#     creds=auth()
#     service=build("drive","v3",credentials=creds)
#     files=list_files(folder_id,service)
#     print("Total Files : ",len(files))
#     fields=[]
#     for i in files[0].keys():
#         fields.append(i)
#     rows=[]
#     for i in files:
#         row={k:i[k] for k in i if k in fields}
#         row['size']=int(i.get('size'))/1024
#         rows.append(row)
#     dump_csv(output_csv,rows,fields)

    
    
    
    

# if __name__=="__main__":
#     app()
