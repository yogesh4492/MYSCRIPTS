# #!/usr/bin/env python3
# """
# Google Drive Bulk Downloader
# Downloads files from Google Drive with metadata export and folder structure preservation.
# """

# import csv
# import io
# import os
# import time
# import ssl
# from pathlib import Path
# from datetime import datetime
# from typing import Optional, List, Dict
# import concurrent.futures
# from threading import Lock

# import typer
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
# from google.auth.transport.requests import Request
# from googleapiclient.discovery import build
# from googleapiclient.http import MediaIoBaseDownload
# from googleapiclient.errors import HttpError
# from rich.console import Console
# from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn
# from rich.table import Table
# import pickle
# import httplib2

# # Scopes required for Google Drive API
# SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

# app = typer.Typer(help="Download files from Google Drive with metadata")
# console = Console()

# # Thread-safe counter for progress
# class DownloadStats:
#     def __init__(self):
#         self.lock = Lock()
#         self.files_downloaded = 0
#         self.files_failed = 0
#         self.total_size = 0
#         self.metadata = []
#         self.failed_files = []
    
#     def add_file(self, size: int, metadata: dict):
#         with self.lock:
#             self.files_downloaded += 1
#             self.total_size += size
#             self.metadata.append(metadata)
    
#     def add_failed(self, file_name: str, error: str):
#         with self.lock:
#             self.files_failed += 1
#             self.failed_files.append({'file_name': file_name, 'error': error})


# def authenticate(credentials_file: Path, token_file: Path):
#     """Authenticate with Google Drive API."""
#     creds = None
    
#     # Check if token file exists
#     if token_file.exists():
#         with open(token_file, 'rb') as token:
#             creds = pickle.load(token)
    
#     # If credentials are invalid or don't exist, authenticate
#     if not creds or not creds.valid:
#         if creds and creds.expired and creds.refresh_token:
#             creds.refresh(Request())
#         else:
#             if not credentials_file.exists():
#                 console.print(f"[red]Error: Credentials file not found at {credentials_file}[/red]")
#                 console.print("\n[yellow]To get credentials:[/yellow]")
#                 console.print("1. Go to https://console.cloud.google.com/")
#                 console.print("2. Create a new project or select existing one")
#                 console.print("3. Enable Google Drive API")
#                 console.print("4. Create OAuth 2.0 credentials (Desktop app)")
#                 console.print("5. Download the JSON file and save as 'credentials.json'")
#                 raise typer.Exit(1)
            
#             flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
#             creds = flow.run_local_server(port=0)
        
#         # Save credentials for future use
#         with open(token_file, 'wb') as token:
#             pickle.dump(creds, token)
    
#     # Build service with custom http client for better SSL handling
#     http = httplib2.Http(timeout=60)
#     http = creds.authorize(http)
    
#     return creds, http


# def get_file_metadata(service, file_id: str) -> dict:
#     """Get detailed metadata for a file."""
#     try:
#         file = service.files().get(
#             fileId=file_id,
#             fields='id, name, mimeType, size, createdTime, modifiedTime, owners, parents, webViewLink'
#         ).execute()
#         return file
#     except Exception as e:
#         console.print(f"[red]Error getting metadata for file {file_id}: {e}[/red]")
#         return None


# def build_folder_structure(service, folder_id: str, parent_path: Path = Path("")) -> Dict[str, Path]:
#     """Build a mapping of folder IDs to their paths."""
#     folder_map = {folder_id: parent_path}
    
#     try:
#         query = f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
#         results = service.files().list(
#             q=query,
#             fields='files(id, name)',
#             pageSize=1000
#         ).execute()
        
#         folders = results.get('files', [])
        
#         for folder in folders:
#             folder_path = parent_path / folder['name']
#             folder_map[folder['id']] = folder_path
#             # Recursively get subfolders
#             subfolder_map = build_folder_structure(service, folder['id'], folder_path)
#             folder_map.update(subfolder_map)
    
#     except Exception as e:
#         console.print(f"[yellow]Warning: Error building folder structure: {e}[/yellow]")
    
#     return folder_map


# def list_all_files(service, folder_id: str) -> List[dict]:
#     """List all files in a folder and its subfolders."""
#     all_files = []
#     page_token = None
    
#     while True:
#         try:
#             query = f"'{folder_id}' in parents and trashed = false"
#             results = service.files().list(
#                 q=query,
#                 fields='nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime, owners, parents, webViewLink)',
#                 pageSize=1000,
#                 pageToken=page_token
#             ).execute()
            
#             files = results.get('files', [])
#             all_files.extend(files)
            
#             page_token = results.get('nextPageToken')
#             if not page_token:
#                 break
        
#         except Exception as e:
#             console.print(f"[red]Error listing files: {e}[/red]")
#             break
    
#     # Recursively get files from subfolders
#     for file in all_files.copy():
#         if file['mimeType'] == 'application/vnd.google-apps.folder':
#             subfolder_files = list_all_files(service, file['id'])
#             all_files.extend(subfolder_files)
    
#     return all_files


# def download_file(service, file_id: str, output_path: Path, file_name: str, max_retries: int = 3) -> tuple:
#     """Download a single file from Google Drive with retry logic."""
    
#     for attempt in range(max_retries):
#         try:
#             # Create parent directories
#             output_path.parent.mkdir(parents=True, exist_ok=True)
            
#             request = service.files().get_media(fileId=file_id)
#             fh = io.BytesIO()
#             downloader = MediaIoBaseDownload(fh, request)
            
#             done = False
#             while not done:
#                 status, done = downloader.next_chunk()
            
#             # Write to file
#             with open(output_path, 'wb') as f:
#                 f.write(fh.getvalue())
            
#             return True, len(fh.getvalue()), None
        
#         except HttpError as e:
#             error_msg = f"HTTP Error {e.resp.status}: {e.error_details}"
#             if attempt < max_retries - 1:
#                 time.sleep(2 ** attempt)  # Exponential backoff
#                 continue
#             return False, 0, error_msg
        
#         except Exception as e:
#             error_msg = str(e)
#             if attempt < max_retries - 1:
#                 # Wait before retry with exponential backoff
#                 time.sleep(2 ** attempt)
#                 continue
#             return False, 0, error_msg
    
#     return False, 0, "Max retries exceeded"


# def download_file_wrapper(args):
#     """Wrapper for parallel download."""
#     creds, http, file_info, output_base, folder_map, stats = args
    
#     # Create a new service instance for this thread with the shared http client
#     service = build('drive', 'v3', credentials=creds, http=http, cache_discovery=False)
    
#     file_id = file_info['id']
#     file_name = file_info['name']
#     mime_type = file_info['mimeType']
    
#     # Skip Google Workspace files (Docs, Sheets, etc.)
#     if mime_type.startswith('application/vnd.google-apps'):
#         if mime_type != 'application/vnd.google-apps.folder':
#             console.print(f"[yellow]Skipping Google Workspace file: {file_name}[/yellow]")
#         return None
    
#     # Get parent folder path
#     parent_id = file_info.get('parents', [None])[0]
#     folder_path = folder_map.get(parent_id, Path(""))
    
#     # Create output path
#     output_path = output_base / folder_path / file_name
    
#     # Download file with retries
#     success, size, error = download_file(service, file_id, output_path, file_name)
    
#     if success:
#         # Prepare metadata
#         metadata = {
#             'file_id': file_id,
#             'file_name': file_name,
#             'mime_type': mime_type,
#             'size_bytes': file_info.get('size', size),
#             'created_time': file_info.get('createdTime', ''),
#             'modified_time': file_info.get('modifiedTime', ''),
#             'owner': file_info.get('owners', [{}])[0].get('emailAddress', 'Unknown'),
#             'web_link': file_info.get('webViewLink', ''),
#             'local_path': str(output_path.relative_to(output_base)),
#             'download_time': datetime.now().isoformat()
#         }
        
#         stats.add_file(size, metadata)
#         return ('success', file_name)
#     else:
#         stats.add_failed(file_name, error)
#         return ('failed', file_name, error)


# def save_metadata_csv(metadata: List[dict], output_file: Path):
#     """Save metadata to CSV file."""
#     if not metadata:
#         return
    
#     fieldnames = [
#         'file_id', 'file_name', 'mime_type', 'size_bytes', 
#         'created_time', 'modified_time', 'owner', 'web_link',
#         'local_path', 'download_time'
#     ]
    
#     with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
#         writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
#         writer.writeheader()
#         writer.writerows(metadata)


# @app.command()
# def download(
#     folder_id: str = typer.Argument(..., help="Google Drive folder ID to download"),
#     output_folder: Path = typer.Argument(..., help="Local output directory"),
#     credentials_file: Path = typer.Option(
#         "credentials.json",
#         "--credentials",
#         "-c",
#         help="Path to Google OAuth credentials JSON file"
#     ),
#     token_file: Path = typer.Option(
#         "token.pickle",
#         "--token",
#         "-t",
#         help="Path to save/load authentication token"
#     ),
#     metadata_file: Path = typer.Option(
#         "metadata.csv",
#         "--metadata",
#         "-m",
#         help="Output CSV file for metadata"
#     ),
#     workers: int = typer.Option(
#         4,
#         "--workers",
#         "-w",
#         help="Number of parallel download workers"
#     )
# ):
#     """
#     Download files from Google Drive folder with metadata export.
    
#     Example:
#         python gdrive_downloader.py 1abc123XYZ ./downloads
    
#     To get the folder ID from a Google Drive URL:
#         https://drive.google.com/drive/folders/FOLDER_ID
#     """
    
#     console.print("[bold green]Google Drive Bulk Downloader[/bold green]\n")
    
#     # Authenticate
#     console.print("[cyan]Authenticating with Google Drive...[/cyan]")
#     creds, http = authenticate(credentials_file, token_file)
#     service = build('drive', 'v3', credentials=creds, http=http, cache_discovery=False)
#     console.print("[green]✓ Authentication successful[/green]\n")
    
#     # Create output directory
#     output_folder.mkdir(parents=True, exist_ok=True)
    
#     # Build folder structure
#     console.print("[cyan]Building folder structure...[/cyan]")
#     folder_map = build_folder_structure(service, folder_id)
#     console.print(f"[green]✓ Found {len(folder_map)} folder(s)[/green]\n")
    
#     # List all files
#     console.print("[cyan]Scanning for files...[/cyan]")
#     all_files = list_all_files(service, folder_id)
    
#     # Filter out folders
#     downloadable_files = [f for f in all_files if f['mimeType'] != 'application/vnd.google-apps.folder']
#     console.print(f"[green]✓ Found {len(downloadable_files)} file(s) to download[/green]\n")
    
#     if not downloadable_files:
#         console.print("[yellow]No files to download![/yellow]")
#         return
    
#     # Initialize stats
#     stats = DownloadStats()
    
#     # Download files with progress bar
#     console.print("[bold]Starting downloads...[/bold]\n")
    
#     with Progress(
#         SpinnerColumn(),
#         TextColumn("[progress.description]{task.description}"),
#         BarColumn(),
#         TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
#         DownloadColumn(),
#         TransferSpeedColumn(),
#         console=console
#     ) as progress:
        
#         task = progress.add_task(
#             "[cyan]Downloading files...",
#             total=len(downloadable_files)
#         )
        
#         # Prepare arguments for parallel download
#         download_args = [
#             (creds, http, file_info, output_folder, folder_map, stats)
#             for file_info in downloadable_files
#         ]
        
#         # Use ThreadPoolExecutor for parallel downloads
#         with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
#             for result in executor.map(download_file_wrapper, download_args):
#                 if result:
#                     if result[0] == 'success':
#                         progress.console.print(f"  [green]✓ {result[1]}[/green]")
#                     elif result[0] == 'failed':
#                         progress.console.print(f"  [red]✗ {result[1]}: {result[2]}[/red]")
#                 progress.advance(task)
    
#     # Save metadata
#     console.print(f"\n[cyan]Saving metadata to {metadata_file}...[/cyan]")
#     save_metadata_csv(stats.metadata, output_folder / metadata_file)
#     console.print("[green]✓ Metadata saved[/green]\n")
    
#     # Save failed files log if any
#     if stats.failed_files:
#         failed_log = output_folder / "failed_downloads.csv"
#         with open(failed_log, 'w', newline='', encoding='utf-8') as csvfile:
#             writer = csv.DictWriter(csvfile, fieldnames=['file_name', 'error'])
#             writer.writeheader()
#             writer.writerows(stats.failed_files)
#         console.print(f"[yellow]Failed downloads logged to {failed_log}[/yellow]\n")
    
#     # Display summary
#     table = Table(title="Download Summary")
#     table.add_column("Metric", style="cyan")
#     table.add_column("Value", style="green")
    
#     table.add_row("Files Downloaded", str(stats.files_downloaded))
#     table.add_row("Files Failed", str(stats.files_failed))
#     table.add_row("Total Size", f"{stats.total_size / (1024*1024):.2f} MB")
#     table.add_row("Output Directory", str(output_folder))
#     table.add_row("Metadata File", str(output_folder / metadata_file))
    
#     console.print(table)
    
#     if stats.files_failed > 0:
#         console.print(f"\n[yellow]⚠ {stats.files_failed} file(s) failed to download. Check failed_downloads.csv for details.[/yellow]")
    
#     console.print("\n[bold green]✓ Download complete![/bold green]")


# if __name__ == "__main__":
#     app()

#!/usr/bin/env python3
"""
Google Drive Bulk Downloader
Downloads files from Google Drive with metadata export and folder structure preservation.
"""

import csv
import io
import os
import time
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict
import concurrent.futures
from threading import Lock

import typer
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, DownloadColumn, TransferSpeedColumn
from rich.table import Table
import pickle

# Scopes required for Google Drive API
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

app = typer.Typer(help="Download files from Google Drive with metadata")
console = Console()

# Thread-safe counter for progress
class DownloadStats:
    def __init__(self):
        self.lock = Lock()
        self.files_downloaded = 0
        self.files_failed = 0
        self.total_size = 0
        self.metadata = []
        self.failed_files = []
    
    def add_file(self, size: int, metadata: dict):
        with self.lock:
            self.files_downloaded += 1
            self.total_size += size
            self.metadata.append(metadata)
    
    def add_failed(self, file_name: str, error: str):
        with self.lock:
            self.files_failed += 1
            self.failed_files.append({'file_name': file_name, 'error': error})


def authenticate(credentials_file: Path, token_file: Path):
    """Authenticate with Google Drive API."""
    creds = None
    
    # Check if token file exists
    if token_file.exists():
        with open(token_file, 'rb') as token:
            creds = pickle.load(token)
    
    # If credentials are invalid or don't exist, authenticate
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not credentials_file.exists():
                console.print(f"[red]Error: Credentials file not found at {credentials_file}[/red]")
                console.print("\n[yellow]To get credentials:[/yellow]")
                console.print("1. Go to https://console.cloud.google.com/")
                console.print("2. Create a new project or select existing one")
                console.print("3. Enable Google Drive API")
                console.print("4. Create OAuth 2.0 credentials (Desktop app)")
                console.print("5. Download the JSON file and save as 'credentials.json'")
                raise typer.Exit(1)
            
            flow = InstalledAppFlow.from_client_secrets_file(
                str(credentials_file), SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save credentials for future use
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)
    
    return creds


def get_file_metadata(service, file_id: str) -> dict:
    """Get detailed metadata for a file."""
    try:
        file = service.files().get(
            fileId=file_id,
            fields='id, name, mimeType, size, createdTime, modifiedTime, owners, parents, webViewLink'
        ).execute()
        return file
    except Exception as e:
        console.print(f"[red]Error getting metadata for file {file_id}: {e}[/red]")
        return None


def build_folder_structure(service, folder_id: str, parent_path: Path = Path("")) -> Dict[str, Path]:
    """Build a mapping of folder IDs to their paths."""
    folder_map = {folder_id: parent_path}
    
    try:
        query = f"'{folder_id}' in parents and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        results = service.files().list(
            q=query,
            fields='files(id, name)',
            pageSize=1000
        ).execute()
        
        folders = results.get('files', [])
        
        for folder in folders:
            folder_path = parent_path / folder['name']
            folder_map[folder['id']] = folder_path
            # Recursively get subfolders
            subfolder_map = build_folder_structure(service, folder['id'], folder_path)
            folder_map.update(subfolder_map)
    
    except Exception as e:
        console.print(f"[yellow]Warning: Error building folder structure: {e}[/yellow]")
    
    return folder_map


def list_all_files(service, folder_id: str) -> List[dict]:
    """List all files in a folder and its subfolders."""
    all_files = []
    page_token = None
    
    while True:
        try:
            query = f"'{folder_id}' in parents and trashed = false"
            results = service.files().list(
                q=query,
                fields='nextPageToken, files(id, name, mimeType, size, createdTime, modifiedTime, owners, parents, webViewLink)',
                pageSize=1000,
                pageToken=page_token
            ).execute()
            
            files = results.get('files', [])
            all_files.extend(files)
            
            page_token = results.get('nextPageToken')
            if not page_token:
                break
        
        except Exception as e:
            console.print(f"[red]Error listing files: {e}[/red]")
            break
    
    # Recursively get files from subfolders
    for file in all_files.copy():
        if file['mimeType'] == 'application/vnd.google-apps.folder':
            subfolder_files = list_all_files(service, file['id'])
            all_files.extend(subfolder_files)
    
    return all_files


def download_file(service, file_id: str, output_path: Path, file_name: str, max_retries: int = 3) -> tuple:
    """Download a single file from Google Drive with retry logic."""
    
    for attempt in range(max_retries):
        try:
            # Create parent directories
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            request = service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            # Write to file
            with open(output_path, 'wb') as f:
                f.write(fh.getvalue())
            
            return True, len(fh.getvalue()), None
        
        except HttpError as e:
            error_msg = f"HTTP Error {e.resp.status}: {e.error_details}"
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            return False, 0, error_msg
        
        except Exception as e:
            error_msg = str(e)
            if attempt < max_retries - 1:
                # Wait before retry with exponential backoff
                time.sleep(2 ** attempt)
                continue
            return False, 0, error_msg
    
    return False, 0, "Max retries exceeded"


def download_file_wrapper(args):
    """Wrapper for parallel download."""
    creds, file_info, output_base, folder_map, stats = args
    
    # Create a new service instance for this thread
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    
    file_id = file_info['id']
    file_name = file_info['name']
    mime_type = file_info['mimeType']
    
    # Skip Google Workspace files (Docs, Sheets, etc.)
    if mime_type.startswith('application/vnd.google-apps'):
        if mime_type != 'application/vnd.google-apps.folder':
            console.print(f"[yellow]Skipping Google Workspace file: {file_name}[/yellow]")
        return None
    
    # Get parent folder path
    parent_id = file_info.get('parents', [None])[0]
    folder_path = folder_map.get(parent_id, Path(""))
    
    # Create output path
    output_path = output_base / folder_path / file_name
    
    # Download file with retries
    success, size, error = download_file(service, file_id, output_path, file_name)
    
    if success:
        # Prepare metadata
        metadata = {
            'file_id': file_id,
            'file_name': file_name,
            'mime_type': mime_type,
            'size_bytes': file_info.get('size', size),
            'created_time': file_info.get('createdTime', ''),
            'modified_time': file_info.get('modifiedTime', ''),
            'owner': file_info.get('owners', [{}])[0].get('emailAddress', 'Unknown'),
            'web_link': file_info.get('webViewLink', ''),
            'local_path': str(output_path.relative_to(output_base)),
            'download_time': datetime.now().isoformat()
        }
        
        stats.add_file(size, metadata)
        return ('success', file_name)
    else:
        stats.add_failed(file_name, error)
        return ('failed', file_name, error)


def save_metadata_csv(metadata: List[dict], output_file: Path):
    """Save metadata to CSV file."""
    if not metadata:
        return
    
    fieldnames = [
        'file_id', 'file_name', 'mime_type', 'size_bytes', 
        'created_time', 'modified_time', 'owner', 'web_link',
        'local_path', 'download_time'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(metadata)


@app.command()
def download(
    folder_id: str = typer.Argument(..., help="Google Drive folder ID to download"),
    output_folder: Path = typer.Argument(..., help="Local output directory"),
    credentials_file: Path = typer.Option(
        "credentials.json",
        "--credentials",
        "-c",
        help="Path to Google OAuth credentials JSON file"
    ),
    token_file: Path = typer.Option(
        "token.pickle",
        "--token",
        "-t",
        help="Path to save/load authentication token"
    ),
    metadata_file: Path = typer.Option(
        "metadata.csv",
        "--metadata",
        "-m",
        help="Output CSV file for metadata"
    ),
    workers: int = typer.Option(
        4,
        "--workers",
        "-w",
        help="Number of parallel download workers"
    )
):
    """
    Download files from Google Drive folder with metadata export.
    
    Example:
        python gdrive_downloader.py 1abc123XYZ ./downloads
    
    To get the folder ID from a Google Drive URL:
        https://drive.google.com/drive/folders/FOLDER_ID
    """
    
    console.print("[bold green]Google Drive Bulk Downloader[/bold green]\n")
    
    # Authenticate
    console.print("[cyan]Authenticating with Google Drive...[/cyan]")
    creds = authenticate(credentials_file, token_file)
    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    console.print("[green]✓ Authentication successful[/green]\n")
    
    # Create output directory
    output_folder.mkdir(parents=True, exist_ok=True)
    
    # Build folder structure
    console.print("[cyan]Building folder structure...[/cyan]")
    folder_map = build_folder_structure(service, folder_id)
    console.print(f"[green]✓ Found {len(folder_map)} folder(s)[/green]\n")
    
    # List all files
    console.print("[cyan]Scanning for files...[/cyan]")
    all_files = list_all_files(service, folder_id)
    
    # Filter out folders
    downloadable_files = [f for f in all_files if f['mimeType'] != 'application/vnd.google-apps.folder']
    console.print(f"[green]✓ Found {len(downloadable_files)} file(s) to download[/green]\n")
    
    if not downloadable_files:
        console.print("[yellow]No files to download![/yellow]")
        return
    
    # Initialize stats
    stats = DownloadStats()
    
    # Download files with progress bar
    console.print("[bold]Starting downloads...[/bold]\n")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        DownloadColumn(),
        TransferSpeedColumn(),
        console=console
    ) as progress:
        
        task = progress.add_task(
            "[cyan]Downloading files...",
            total=len(downloadable_files)
        )
        
        # Prepare arguments for parallel download
        download_args = [
            (creds, file_info, output_folder, folder_map, stats)
            for file_info in downloadable_files
        ]
        
        # Use ThreadPoolExecutor for parallel downloads
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            for result in executor.map(download_file_wrapper, download_args):
                if result:
                    if result[0] == 'success':
                        progress.console.print(f"  [green]✓ {result[1]}[/green]")
                    elif result[0] == 'failed':
                        progress.console.print(f"  [red]✗ {result[1]}: {result[2]}[/red]")
                progress.advance(task)
    
    # Save metadata
    console.print(f"\n[cyan]Saving metadata to {metadata_file}...[/cyan]")
    save_metadata_csv(stats.metadata, output_folder / metadata_file)
    console.print("[green]✓ Metadata saved[/green]\n")
    
    # Save failed files log if any
    if stats.failed_files:
        failed_log = output_folder / "failed_downloads.csv"
        with open(failed_log, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['file_name', 'error'])
            writer.writeheader()
            writer.writerows(stats.failed_files)
        console.print(f"[yellow]Failed downloads logged to {failed_log}[/yellow]\n")
    
    # Display summary
    table = Table(title="Download Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Files Downloaded", str(stats.files_downloaded))
    table.add_row("Files Failed", str(stats.files_failed))
    table.add_row("Total Size", f"{stats.total_size / (1024*1024):.2f} MB")
    table.add_row("Output Directory", str(output_folder))
    table.add_row("Metadata File", str(output_folder / metadata_file))
    
    console.print(table)
    
    if stats.files_failed > 0:
        console.print(f"\n[yellow]⚠ {stats.files_failed} file(s) failed to download. Check failed_downloads.csv for details.[/yellow]")
    
    console.print("\n[bold green]✓ Download complete![/bold green]")


if __name__ == "__main__":
    app()