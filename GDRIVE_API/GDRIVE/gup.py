#!/usr/bin/env python3
"""
Google Drive Bulk Uploader
Uploads files to Google Drive with metadata export and folder structure preservation.
"""

import csv
import mimetypes
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict
import concurrent.futures
from threading import Lock
from GDRIVE.authentication import *
import typer
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TransferSpeedColumn
from rich.table import Table
# import pickle

# Scopes required for Google Drive API
SCOPES = ['https://www.googleapis.com/auth/drive']

app = typer.Typer(help="Upload files to Google Drive with metadata")
console = Console()

# Thread-safe counter for progress
class UploadStats:
    def __init__(self):
        self.lock = Lock()
        self.files_uploaded = 0
        self.files_failed = 0
        self.total_size = 0
        self.metadata = []
        self.failed_files = []
        self.folder_cache = {}  # Cache folder IDs to avoid duplicate creation
    
    def add_file(self, size: int, metadata: dict):
        with self.lock:
            self.files_uploaded += 1
            self.total_size += size
            self.metadata.append(metadata)
    
    def add_failed(self, file_name: str, error: str):
        with self.lock:
            self.files_failed += 1
            self.failed_files.append({'file_name': file_name, 'error': error})
    
    def get_folder_id(self, key: str) -> Optional[str]:
        with self.lock:
            return self.folder_cache.get(key)
    
    def set_folder_id(self, key: str, folder_id: str):
        with self.lock:
            self.folder_cache[key] = folder_id


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
            
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save credentials for future use
        with open(token_file, 'wb') as token:
            pickle.dump(creds, token)
    
    return creds


def create_folder(service, folder_name: str, parent_id: Optional[str] = None) -> Optional[str]:
    """Create a folder in Google Drive."""
    try:
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        if parent_id:
            file_metadata['parents'] = [parent_id]
        
        folder = service.files().create(
            body=file_metadata,
            fields='id'
        ).execute()
        
        return folder.get('id')
    
    except Exception as e:
        console.print(f"[red]Error creating folder {folder_name}: {e}[/red]")
        return None


def get_or_create_folder(service, folder_path: Path, parent_id: Optional[str], stats: UploadStats) -> Optional[str]:
    """Get or create folder structure in Google Drive."""
    if not folder_path or folder_path == Path("."):
        return parent_id
    
    # Create cache key
    cache_key = f"{parent_id}:{folder_path}"
    
    # Check cache first
    cached_id = stats.get_folder_id(cache_key)
    if cached_id:
        return cached_id
    
    # Process parent folders first
    if folder_path.parent != Path("."):
        parent_id = get_or_create_folder(service, folder_path.parent, parent_id, stats)
    
    # Check if folder already exists
    try:
        query = f"name='{folder_path.name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        if parent_id:
            query += f" and '{parent_id}' in parents"
        
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)'
        ).execute()
        
        items = results.get('files', [])
        
        if items:
            folder_id = items[0]['id']
        else:
            # Create the folder
            folder_id = create_folder(service, folder_path.name, parent_id)
        
        # Cache the folder ID
        if folder_id:
            stats.set_folder_id(cache_key, folder_id)
        
        return folder_id
    
    except Exception as e:
        console.print(f"[red]Error getting/creating folder {folder_path}: {e}[/red]")
        return None


def upload_file(service, file_path: Path, parent_id: Optional[str], max_retries: int = 3) -> tuple:
    """Upload a single file to Google Drive with retry logic."""
    
    for attempt in range(max_retries):
        try:
            # Get file size
            file_size = file_path.stat().st_size
            
            # Determine MIME type
            mime_type, _ = mimetypes.guess_type(str(file_path))
            if not mime_type:
                mime_type = 'application/octet-stream'
            
            # Prepare file metadata
            file_metadata = {'name': file_path.name}
            if parent_id:
                file_metadata['parents'] = [parent_id]
            
            # Create media upload
            media = MediaFileUpload(
                str(file_path),
                mimetype=mime_type,
                resumable=True,
                chunksize=1024*1024  # 1MB chunks
            )
            
            # Upload file
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, name, mimeType, size, createdTime, webViewLink'
            ).execute()
            
            return True, file_size, file, None
        
        except HttpError as e:
            error_msg = f"HTTP Error {e.resp.status}: {str(e)}"
            if attempt < max_retries - 1:
                console.print(f"[yellow]Retry {attempt + 1}/{max_retries} for {file_path.name}[/yellow]")
                continue
            return False, 0, None, error_msg
        
        except Exception as e:
            error_msg = str(e)
            if attempt < max_retries - 1:
                console.print(f"[yellow]Retry {attempt + 1}/{max_retries} for {file_path.name}[/yellow]")
                continue
            return False, 0, None, error_msg
    
    return False, 0, None, "Max retries exceeded"


def upload_file_wrapper(args):
    """Wrapper for parallel upload."""
    creds, file_path, base_path, parent_folder_id, stats = args
    
    # Create a new service instance for this thread
    # service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    service=auth()
    
    # Calculate relative path
    relative_path = file_path.relative_to(base_path)
    folder_path = relative_path.parent
    
    # Get or create folder structure
    if folder_path != Path("."):
        target_folder_id = get_or_create_folder(service, folder_path, parent_folder_id, stats)
    else:
        target_folder_id = parent_folder_id
    
    if target_folder_id is None and folder_path != Path("."):
        error_msg = f"Failed to create folder structure for {relative_path}"
        stats.add_failed(str(relative_path), error_msg)
        return ('failed', str(relative_path), error_msg)
    
    # Upload file
    success, size, file_info, error = upload_file(service, file_path, target_folder_id)
    
    if success:
        # Prepare metadata
        metadata = {
            'file_id': file_info['id'],
            'file_name': file_info['name'],
            'mime_type': file_info['mimeType'],
            'size_bytes': file_info.get('size', size),
            'created_time': file_info.get('createdTime', ''),
            'web_link': file_info.get('webViewLink', ''),
            'local_path': str(relative_path),
            'upload_time': datetime.now().isoformat()
        }
        
        stats.add_file(size, metadata)
        return ('success', str(relative_path))
    else:
        stats.add_failed(str(relative_path), error)
        return ('failed', str(relative_path), error)


def find_all_files(root_dir: Path, exclude_patterns: List[str] = None) -> List[Path]:
    """Find all files in directory recursively."""
    if exclude_patterns is None:
        exclude_patterns = ['.git', '__pycache__', '.DS_Store', 'node_modules']
    
    all_files = []
    
    for item in root_dir.rglob('*'):
        if item.is_file():
            # Check if any exclude pattern matches
            if any(pattern in str(item) for pattern in exclude_patterns):
                continue
            all_files.append(item)
    
    return all_files


def save_metadata_csv(metadata: List[dict], output_file: Path):
    """Save metadata to CSV file."""
    if not metadata:
        return
    
    fieldnames = [
        'file_id', 'file_name', 'mime_type', 'size_bytes',
        'created_time', 'web_link', 'local_path', 'upload_time'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(metadata)


# @app.command()
# def upload(
#     folder_id: Optional[str] = typer.Argument(None, help="Google Drive parent folder ID (optional, uploads to root if not provided)"),
#     local_folder: Path = typer.Argument(..., help="Local folder to upload"),
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
#         "upload_metadata.csv",
#         "--metadata",
#         "-m",
#         help="Output CSV file for metadata"
#     ),
#     workers: int = typer.Option(
#         3,
#         "--workers",
#         "-w",
#         help="Number of parallel upload workers"
#     ),
#     exclude: Optional[List[str]] = typer.Option(
#         None,
#         "--exclude",
#         "-e",
#         help="Patterns to exclude (can be specified multiple times)"
#     )
# ):
#     """
#     Upload files to Google Drive with folder structure preservation.
    
#     Example:
#         # Upload to root
#         python gdrive_uploader.py ./my_folder
        
#         # Upload to specific folder
#         python gdrive_uploader.py ./my_folder 1abc123XYZ
        
#         # With exclusions
#         python gdrive_uploader.py ./my_folder -e .git -e node_modules
#     """
    
#     console.print("[bold green]Google Drive Bulk Uploader[/bold green]\n")
    
#     # Validate local folder
#     if not local_folder.exists():
#         console.print(f"[red]Error: Local folder '{local_folder}' does not exist![/red]")
#         raise typer.Exit(1)
    
#     if not local_folder.is_dir():
#         console.print(f"[red]Error: '{local_folder}' is not a directory![/red]")
#         raise typer.Exit(1)
    
#     # Authenticate
#     console.print("[cyan]Authenticating with Google Drive...[/cyan]")
#     creds = authenticate(credentials_file, token_file)
#     service = build('drive', 'v3', credentials=creds, cache_discovery=False)
#     console.print("[green]✓ Authentication successful[/green]\n")
    
#     # Verify parent folder if provided
#     if folder_id:
#         try:
#             service.files().get(fileId=folder_id, fields='id, name').execute()
#             console.print(f"[green]✓ Parent folder verified[/green]\n")
#         except Exception as e:
#             console.print(f"[red]Error: Cannot access folder ID {folder_id}: {e}[/red]")
#             raise typer.Exit(1)
#     else:
#         console.print("[yellow]No parent folder specified, uploading to Drive root[/yellow]\n")
    
#     # Find all files
#     console.print("[cyan]Scanning for files...[/cyan]")
#     exclude_patterns = list(exclude) if exclude else ['.git', '__pycache__', '.DS_Store', 'node_modules']
#     all_files = find_all_files(local_folder, exclude_patterns)
#     console.print(f"[green]✓ Found {len(all_files)} file(s) to upload[/green]\n")
    
#     if not all_files:
#         console.print("[yellow]No files to upload![/yellow]")
#         return
    
#     # Calculate total size
#     total_size = sum(f.stat().st_size for f in all_files)
#     console.print(f"[cyan]Total size: {total_size / (1024*1024):.2f} MB[/cyan]\n")
    
#     # Initialize stats
#     stats = UploadStats()
    
#     # Upload files with progress bar
#     console.print("[bold]Starting uploads...[/bold]\n")
    
#     with Progress(
#         SpinnerColumn(),
#         TextColumn("[progress.description]{task.description}"),
#         BarColumn(),
#         TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
#         # UploadColumn(),
#         TransferSpeedColumn(),
#         console=console
#     ) as progress:
        
#         task = progress.add_task(
#             "[cyan]Uploading files...",
#             total=len(all_files)
#         )
        
#         # Prepare arguments for parallel upload
#         upload_args = [
#             (creds, file_path, local_folder, folder_id, stats)
#             for file_path in all_files
#         ]
        
#         # Use ThreadPoolExecutor for parallel uploads
#         with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
#             for result in executor.map(upload_file_wrapper, upload_args):
#                 if result:
#                     if result[0] == 'success':
#                         progress.console.print(f"  [green]✓ {result[1]}[/green]")
#                     elif result[0] == 'failed':
#                         progress.console.print(f"  [red]✗ {result[1]}: {result[2]}[/red]")
#                 progress.advance(task)
    
#     # Save metadata
#     console.print(f"\n[cyan]Saving metadata to {metadata_file}...[/cyan]")
#     save_metadata_csv(stats.metadata, metadata_file)
#     console.print("[green]✓ Metadata saved[/green]\n")
    
#     # Save failed files log if any
#     if stats.failed_files:
#         failed_log = Path("failed_uploads.csv")
#         with open(failed_log, 'w', newline='', encoding='utf-8') as csvfile:
#             writer = csv.DictWriter(csvfile, fieldnames=['file_name', 'error'])
#             writer.writeheader()
#             writer.writerows(stats.failed_files)
#         console.print(f"[yellow]Failed uploads logged to {failed_log}[/yellow]\n")
    
#     # Display summary
#     table = Table(title="Upload Summary")
#     table.add_column("Metric", style="cyan")
#     table.add_column("Value", style="green")
    
#     table.add_row("Files Uploaded", str(stats.files_uploaded))
#     table.add_row("Files Failed", str(stats.files_failed))
#     table.add_row("Total Size", f"{stats.total_size / (1024*1024):.2f} MB")
#     table.add_row("Metadata File", str(metadata_file))
    
#     console.print(table)
    
#     if stats.files_failed > 0:
#         console.print(f"\n[yellow]⚠ {stats.files_failed} file(s) failed to upload. Check failed_uploads.csv for details.[/yellow]")
    
#     console.print("\n[bold green]✓ Upload complete![/bold green]")


# if __name__ == "__main__":
#     app()