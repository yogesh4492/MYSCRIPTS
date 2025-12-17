"""
S3 to Google Drive Transfer CLI
Recursively copies data from S3 to Google Drive maintaining folder structure
"""

import os
import csv
import tempfile
from datetime import datetime
from pathlib import Path
import typer
from CSV.CSV_READ_WRITE import *
from GDRIVE.authentication import *
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.table import Table
import boto3
from botocore.exceptions import ClientError
from googleapiclient.http import MediaFileUpload

# Initialize
app = typer.Typer(help="Transfer files from S3 to Google Drive")
console = Console()



class S3ToGDriveTransfer:
    def __init__(self, aws_profile=None):
        self.s3_client = None
        self.gdrive_service = auth()
        self.folder_cache = {}
        self.transferred_files = []
        self.aws_profile = aws_profile
        # self.gdrive_service = auth()
        
    def setup_s3(self):
        """Initialize S3 client"""
        try:
            if self.aws_profile:
                session = boto3.Session(profile_name=self.aws_profile)
                self.s3_client = session.client('s3')
            else:
                self.s3_client = boto3.client('s3')
            console.print("[green]✓[/green] S3 client initialized")
        except Exception as e:
            console.print(f"[red]✗[/red] Error initializing S3: {str(e)}")
            raise
    
    # def setup_gdrive(self, credentials_path='credentials.json'):
    #     """Initialize Google Drive client"""
    #     creds=[]
    #     if os.path.exists("token.pickle"):
    #         with open("token.pickle","rb") as r:
    #             creds=pickle.load(r)
    #     if not creds or not creds.valid:
    #         if creds and creds.refresh_token and creds.expired:
    #             creds.refresh(Request())
    #         else:
    #             flow=InstalledAppFlow.from_client_secrets_file(credentials_path,SCOPES)
    #             creds=flow.run_local_server(port=0)
    #             with open("token.pickle","wb") as w:
    #                 pickle.dump(creds,w)
    #             service=build("drive","v3",credentials=creds)
    #             return service

#         creds = None
        
#         if os.path.exists('token.pickle'):
#             with open('token.pickle', 'rb') as token:
#                 creds = pickle.load(token)
        
#         if not creds or not creds.valid:
#             if creds and creds.expired and creds.refresh_token:
#                 creds.refresh(Request())
#             else:
#                 if not os.path.exists(credentials_path):
#                     console.print(f"[red]✗[/red] credentials.json not found!")
#                     console.print("Please download from Google Cloud Console")
#                     raise FileNotFoundError("credentials.json not found")
                
#                 flow = InstalledAppFlow.from_client_secrets_file(
#                     credentials_path, SCOPES)
#                 creds = flow.run_local_server(port=0)
            
#             with open('token.pickle', 'wb') as token:
#                 pickle.dump(creds, token)
        
        # console.print("[green]✓[/green] Google Drive authenticated")
    
    def parse_s3_path(self, s3_path):
        """Parse S3 path into bucket and prefix"""
        s3_path = s3_path.replace('s3://', '')
        parts = s3_path.split('/', 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''
        return bucket, prefix
    
    def list_s3_objects(self, bucket, prefix):
        """List all objects in S3 bucket with given prefix"""
        objects = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Skip if it's a folder marker
                        if not obj['Key'].endswith('/'):
                            objects.append(obj)
        except ClientError as e:
            console.print(f"[red]✗[/red] Error listing S3 objects: {str(e)}")
            raise
        return objects
    
    def create_gdrive_folder(self, folder_name, parent_id=None):
        """Create folder in Google Drive"""
        cache_key = f"{folder_name}_{parent_id}"
        if cache_key in self.folder_cache:
            return self.folder_cache[cache_key]
        
        file_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        if parent_id:
            file_metadata['parents'] = [parent_id]
        
        folder = self.gdrive_service.files().create(
            body=file_metadata,
            fields='id, name, webViewLink'
        ).execute()
        
        folder_id = folder.get('id')
        self.folder_cache[cache_key] = folder_id
        return folder_id
    
    def ensure_folder_structure(self, path, root_folder_id):
        """Create folder structure in Google Drive"""
        if not path:
            return root_folder_id
        
        parts = path.split('/')
        current_parent = root_folder_id
        
        for part in parts:
            if part:
                current_parent = self.create_gdrive_folder(part, current_parent)
        
        return current_parent
    
    def download_from_s3(self, bucket, key, local_path):
        """Download file from S3 to local temp file"""
        try:
            self.s3_client.download_file(bucket, key, local_path)
            return True
        except ClientError as e:
            console.print(f"[red]✗[/red] Error downloading {key}: {str(e)}")
            return False
    
    def upload_to_gdrive(self, local_path, filename, parent_id):
        """Upload file to Google Drive"""
        file_metadata = {
            'name': filename,
            'parents': [parent_id]
        }
        
        media = MediaFileUpload(local_path, resumable=True)
        
        file = self.gdrive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, webViewLink, size, mimeType'
        ).execute()
        
        return file
    
    def transfer_file(self, bucket, s3_key, gdrive_parent_id, base_prefix):
        """Transfer single file from S3 to Google Drive"""
        # Get relative path
        relative_path = s3_key[len(base_prefix):].lstrip('/')
        path_parts = relative_path.split('/')
        filename = path_parts[-1]
        folder_path = '/'.join(path_parts[:-1]) if len(path_parts) > 1 else ''
        
        # Ensure folder structure exists
        target_folder_id = self.ensure_folder_structure(folder_path, gdrive_parent_id)
        
        # Download to temp file
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_path = tmp_file.name
        
        try:
            # Download from S3
            if not self.download_from_s3(bucket, s3_key, tmp_path):
                return None
            
            # Upload to Google Drive
            gdrive_file = self.upload_to_gdrive(tmp_path, filename, target_folder_id)
            
            # Store metadata
            metadata = {
                'filename': filename,
                's3_path': f"s3://{bucket}/{s3_key}",
                'folder_path': folder_path or 'Root',
                'gdrive_link': gdrive_file.get('webViewLink'),
                'gdrive_id': gdrive_file.get('id'),
                'file_size': gdrive_file.get('size', 'N/A'),
                'mime_type': gdrive_file.get('mimeType', 'N/A'),
                'transfer_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return metadata
            
        finally:
            # Clean up temp file
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
    
    def export_metadata_to_csv(self, output_file):
        """Export transfer metadata to CSV"""
        if not self.transferred_files:
            console.print("[yellow]No files to export[/yellow]")
            return
        
        fieldnames = [
            'filename',
            's3_path',
            'folder_path',
            'gdrive_link',
            'gdrive_id',
            'file_size',
            'mime_type',
            'transfer_time'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.transferred_files)
        
        console.print(f"\n[green]✓[/green] Metadata exported to: {output_file}")
    
    def transfer(self, s3_path, gdrive_folder_id, csv_output='transfer_metadata.csv'):
        """Main transfer method"""
        # Setup clients
        self.setup_s3()
        # self.setup_gdrive()
        
        # Parse S3 path
        bucket, prefix = self.parse_s3_path(s3_path)
        console.print(f"\n[cyan]S3 Bucket:[/cyan] {bucket}")
        console.print(f"[cyan]S3 Prefix:[/cyan] {prefix or '(root)'}")
        console.print(f"[cyan]GDrive Folder ID:[/cyan] {gdrive_folder_id}\n")
        
        # List all objects
        console.print("[yellow]Scanning S3 bucket...[/yellow]")
        objects = self.list_s3_objects(bucket, prefix)
        console.print(f"[green]Found {len(objects)} files to transfer[/green]\n")
        
        if not objects:
            console.print("[yellow]No files found to transfer[/yellow]")
            return
        
        # Transfer files with progress bar
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            console=console
        ) as progress:
            
            task = progress.add_task(
                "[cyan]Transferring files...", 
                total=len(objects)
            )
            
            for obj in objects:
                s3_key = obj['Key']
                filename = os.path.basename(s3_key)
                
                progress.update(task, description=f"[cyan]Transferring: {filename}")
                
                metadata = self.transfer_file(bucket, s3_key, gdrive_folder_id, prefix)
                
                if metadata:
                    self.transferred_files.append(metadata)
                    progress.update(task, advance=1)
                else:
                    progress.update(task, advance=1)
        
        # Export metadata
        self.export_metadata_to_csv(csv_output)
        
        # Summary
        self.print_summary()
    
    def print_summary(self):
        """Print transfer summary"""
        console.print("\n" + "="*60)
        console.print("[bold green]Transfer Complete![/bold green]")
        console.print("="*60)
        
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Total Files Transferred", str(len(self.transferred_files)))
        table.add_row("Total Folders Created", str(len(self.folder_cache)))
        
        console.print(table)


# @app.command()
# def transfer(
#     s3_path: str = typer.Argument(..., help="S3 path (e.g., s3://bucket-name/folder/)"),
#     gdrive_folder_id: str = typer.Argument(..., help="Google Drive folder ID"),
#     csv_output: str = typer.Option(
#         "transfer_metadata.csv",
#         "--csv",
#         "-c",
#         help="Output CSV file for metadata"
#     ),
#     aws_profile: str = typer.Option(
#         None,
#         "--profile",
#         "-p",
#         help="AWS profile name (optional)"
#     ),
#     credentials: str = typer.Option(
#         "credentials.json",
#         "--credentials",
#         help="Path to Google credentials.json"
#     )
# ):
#     """
#     Transfer files from S3 to Google Drive recursively
    
#     Example:
#         python script.py s3://my-bucket/my-folder/ 1a2b3c4d5e6f7g8h9i0j
#     """
#     console.print("[bold blue]S3 to Google Drive Transfer Tool[/bold blue]")
#     console.print("="*60 + "\n")
    
#     try:
#         transferer = S3ToGDriveTransfer(aws_profile=aws_profile)
#         transferer.transfer(s3_path, gdrive_folder_id, csv_output)
        
#     except Exception as e:
#         console.print(f"\n[bold red]Error:[/bold red] {str(e)}")
#         raise typer.Exit(code=1)


# @app.command()
# def list_s3(
#     s3_path: str = typer.Argument(..., help="S3 path to list"),
#     aws_profile: str = typer.Option(None, "--profile", "-p", help="AWS profile name")
# ):
    """
    # List files in S3 path (useful for preview before transfer)
    """
    

# if __name__ == "__main__":
#     app()