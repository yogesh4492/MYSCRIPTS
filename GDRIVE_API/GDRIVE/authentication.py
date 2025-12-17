from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
# from google.auth.credentials import Credentials
import os
import pickle
scopes=['https://www.googleapis.com/auth/drive']

def auth():
    creds=[]
    if os.path.exists("token.pickle"):
        with open("token.pickle","rb") as r:
            creds=pickle.load(r)
    if not creds or not creds.valid:
        if creds and creds.refresh_token and creds.expired:
            creds.refresh(Request())
        else:
            flow=InstalledAppFlow.from_client_secrets_file("credentials.json",scopes)
            creds=flow.run_local_server(port=0)
            with open("token.pickle","wb") as w:
                pickle.dump(creds,w)
    service=build("drive","v3",credentials=creds)
    return service


        