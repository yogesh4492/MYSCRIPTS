"""
Google Drive to S3 Transfer Tool with Metadata Logging

This script authenticates with Google Drive, recursively traverses a folder,
and uploads all files to S3 while maintaining the folder structure and logging
all transfer metadata to a CSV file.

Requirements:
    pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client boto3 typer

"""

import io
from typing import Optional
from datetime import datetime
import typer
from googleapiclient.http import MediaIoBaseDownload
from GDRIVE.authentication import auth
import boto3
from botocore.exceptions import ClientError
import csv

app = typer.Typer()

# Google Drive API scopes

class GDriveToS3Transfer:
    def __init__(self, s3_bucket: str, s3_prefix: str = ""):
        print("HEllo World")
        print("Hello World")
        self.drive_service = None
        self.s3_client = boto3.client('s3')
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.rstrip('/') + '/' if s3_prefix else ''
        self.transferred_count = 0
        self.failed_count = 0
        self.metadata_records = []
        # creds=auth()    
        # print(self.creds)
        self.drive_service = auth()
        typer.echo("✅ Successfully authenticated with Google Drive")
    
    def get_folder_name(self, folder_id: str) -> str:
        """Get folder name from ID"""
        try:
            folder = self.drive_service.files().get(
                fileId=folder_id,
                fields='name'
            ).execute()
            print(folder.get("name"))
            return folder.get('name', 'root')
        except Exception as e:
            typer.echo(f"⚠️  Warning: Could not get folder name: {e}")
            return 'root'
    
    def list_files_in_folder(self, folder_id: str):
        """List all files and folders in a Google Drive folder"""
        try:
            query = f"'{folder_id}' in parents and trashed=false"
            results = self.drive_service.files().list(
                q=query,
                fields='files(id, name, mimeType, size, createdTime, modifiedTime, owners)',
                pageSize=1000
            ).execute()
            return results.get('files', [])
        except Exception as e:
            typer.echo(f"❌ Error listing files in folder {folder_id}: {e}")
            return []
    
    def add_metadata_record(self, item: dict, full_path: str, s3_key: str, status: str, 
                           file_size_bytes: int = 0, exported: bool = False, error_msg: str = ""):
        """Add a record to the metadata list"""
        owners = item.get('owners', [])
        owner_email = owners[0].get('emailAddress', 'Unknown') if owners else 'Unknown'
        
        record = {
            'gdrive_file_id': item['id'],
            'gdrive_file_name': item['name'],
            'gdrive_path': full_path,
            'gdrive_mime_type': item['mimeType'],
            'gdrive_created_time': item.get('createdTime', ''),
            'gdrive_modified_time': item.get('modifiedTime', ''),
            'gdrive_owner': owner_email,
            'original_size_bytes': item.get('size', '0'),
            'transferred_size_bytes': file_size_bytes,
            'exported': 'Yes' if exported else 'No',
            's3_bucket': self.s3_bucket,
            's3_key': s3_key,
            's3_full_path': f"s3://{self.s3_bucket}/{s3_key}" if s3_key else '',
            'transfer_status': status,
            'transfer_timestamp': datetime.utcnow().isoformat(),
            'error_message': error_msg
        }
        self.metadata_records.append(record)
    
    def export_google_workspace_file(self, file_id: str, mime_type: str, file_name: str) -> Optional[tuple]:
        """Export Google Workspace files to appropriate format"""
        # Map Google Workspace MIME types to export formats
        export_formats = {
            'application/vnd.google-apps.document': ('application/vnd.openxmlformats-officedocument.wordprocessingml.document', '.docx'),
            'application/vnd.google-apps.spreadsheet': ('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', '.xlsx'),
            'application/vnd.google-apps.presentation': ('application/vnd.openxmlformats-officedocument.presentationml.presentation', '.pptx'),
            'application/vnd.google-apps.drawing': ('application/pdf', '.pdf'),
            'application/vnd.google-apps.script': ('application/vnd.google-apps.script+json', '.json'),
            'application/vnd.google-apps.form': ('application/zip', '.zip'),
        }
        
        if mime_type not in export_formats:
            typer.echo(f"  ⚠️  Unsupported Google Workspace type: {mime_type}")
            return None
        
        export_mime, extension = export_formats[mime_type]
        
        try:
            request = self.drive_service.files().export_media(
                fileId=file_id,
                mimeType=export_mime
            )
            file_buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    typer.echo(f"  Exporting: {progress}%", nl=False)
                    typer.echo("\r", nl=False)
            
            file_buffer.seek(0)
            # Add extension if not present
            if not file_name.endswith(extension):
                file_name = file_name + extension
            return file_buffer.read(), file_name
        except Exception as e:
            typer.echo(f"❌ Error exporting {file_name}: {e}")
            return None
    
    def download_file(self, file_id: str, file_name: str) -> Optional[bytes]:
        """Download file from Google Drive"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            file_buffer = io.BytesIO()
            downloader = MediaIoBaseDownload(file_buffer, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    progress = int(status.progress() * 100)
                    typer.echo(f"  Downloading: {progress}%", nl=False)
                    typer.echo("\r", nl=False)
            
            file_buffer.seek(0)
            return file_buffer.read()
        except Exception as e:
            typer.echo(f"❌ Error downloading {file_name}: {e}")
            return None
    
    def upload_to_s3(self, file_data: bytes, s3_key: str, file_name: str) -> bool:
        """Upload file to S3"""
        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=file_data
            )
            typer.echo(f"✅ Uploaded: {s3_key}")
            return True
        except ClientError as e:
            typer.echo(f"❌ Error uploading {file_name} to S3: {e}")
            return False
    
    def process_folder_recursively(self, folder_id: str, current_path: str = ""):
        """Recursively process all files and folders"""
        items = self.list_files_in_folder(folder_id)
        
        if not items:
            typer.echo(f"📁 Empty folder: {current_path or 'root'}")
            return
        
        for item in items:
            item_name = item['name']
            item_id = item['id']
            mime_type = item['mimeType']
            
            # Build the full path
            if current_path:
                full_path = f"{current_path}/{item_name}"
            else:
                full_path = item_name
            
            # Check if it's a folder
            if mime_type == 'application/vnd.google-apps.folder':
                typer.echo(f"\n📁 Entering folder: {full_path}")
                self.process_folder_recursively(item_id, full_path)
            else:
                # Handle Google Workspace files
                if mime_type.startswith('application/vnd.google-apps.'):
                    typer.echo(f"\n📝 Processing Google Workspace file: {full_path}")
                    
                    # Export the file
                    export_result = self.export_google_workspace_file(item_id, mime_type, item_name)
                    
                    if export_result:
                        file_data, exported_file_name = export_result
                        # Update the path with the exported filename
                        if current_path:
                            full_path_exported = f"{current_path}/{exported_file_name}"
                        else:
                            full_path_exported = exported_file_name
                        
                        # Upload to S3
                        s3_key = self.s3_prefix + full_path_exported
                        file_size = len(file_data)
                        
                        if self.upload_to_s3(file_data, s3_key, exported_file_name):
                            self.transferred_count += 1
                            self.add_metadata_record(item, full_path_exported, s3_key, 'SUCCESS', 
                                                    file_size, exported=True)
                        else:
                            self.failed_count += 1
                            self.add_metadata_record(item, full_path_exported, s3_key, 'FAILED', 
                                                    file_size, exported=True, error_msg='S3 upload failed')
                    else:
                        self.failed_count += 1
                        self.add_metadata_record(item, full_path, '', 'FAILED', 
                                               exported=True, error_msg='Export failed')
                    continue
                
                # Process regular file
                typer.echo(f"\n📄 Processing: {full_path}")
                file_size = item.get('size', 'Unknown')
                if file_size != 'Unknown':
                    size_mb = int(file_size) / (1024 * 1024)
                    typer.echo(f"  Size: {size_mb:.2f} MB")
                
                # Download from Google Drive
                file_data = self.download_file(item_id, item_name)
                
                if file_data:
                    # Upload to S3
                    s3_key = self.s3_prefix + full_path
                    transferred_size = len(file_data)
                    
                    if self.upload_to_s3(file_data, s3_key, item_name):
                        self.transferred_count += 1
                        self.add_metadata_record(item, full_path, s3_key, 'SUCCESS', transferred_size)
                    else:
                        self.failed_count += 1
                        self.add_metadata_record(item, full_path, s3_key, 'FAILED', 
                                               transferred_size, error_msg='S3 upload failed')
                else:
                    self.failed_count += 1
                    self.add_metadata_record(item, full_path, '', 'FAILED', error_msg='Download failed')
    
    def save_metadata_to_csv(self, output_file: str = None):
        """Save metadata records to CSV file"""
        if not self.metadata_records:
            typer.echo("⚠️  No metadata to save")
            return
        
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"gdrive_to_s3_metadata_{timestamp}.csv"
        
        try:
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'gdrive_file_id', 'gdrive_file_name', 'gdrive_path', 
                    'gdrive_mime_type', 'gdrive_created_time', 'gdrive_modified_time',
                    'gdrive_owner', 'original_size_bytes', 'transferred_size_bytes',
                    'exported', 's3_bucket', 's3_key', 's3_full_path',
                    'transfer_status', 'transfer_timestamp', 'error_message'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.metadata_records)
            
            typer.echo(f"\n✅ Metadata saved to: {output_file}")
        except Exception as e:
            typer.echo(f"❌ Error saving metadata to CSV: {e}")

def parse_s3_path(s3_path: str) -> tuple:
    """Parse S3 path to extract bucket and prefix"""
    # Remove s3:// prefix if present
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:]
    
    # Split bucket and prefix
    parts = s3_path.split('/', 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    
    return bucket, prefix

# @app.command()
# def transfer(
#     folder_id: str = typer.Argument(..., help="Google Drive folder ID"),
#     s3_path: str = typer.Argument(..., help="S3 path (e.g., 's3://bucket/prefix/' or 'bucket/prefix/')"),
#     credentials_file: str = typer.Option("credentials.json", help="Path to Google OAuth credentials JSON file"),
#     csv_output: str = typer.Option(None, help="Custom CSV output filename (default: auto-generated with timestamp)")
# ):
    """
    Transfer all files from a Google Drive folder to S3 recursively.
    
    Examples:
        python script.py 1a2b3c4d5e6f s3://my-bucket/gdrive-backup/
        python script.py 1a2b3c4d5e6f my-bucket/gdrive-backup/
        python script.py 1a2b3c4d5e6f my-bucket --csv-output=transfer_log.csv
    """
    typer.echo("=" * 60)
    typer.echo("🚀 Google Drive to S3 Transfer Tool")
    typer.echo("=" * 60)
    
    # Parse S3 path
    try:
        s3_bucket, s3_prefix = parse_s3_path(s3_path)
        typer.echo(f"\n🪣 S3 Bucket: {s3_bucket}")
        typer.echo(f"📂 S3 Prefix: {s3_prefix or '(root)'}")
    except Exception as e:
        typer.echo(f"❌ Error parsing S3 path: {e}")
        typer.echo("\nExpected format: 's3://bucket-name/prefix/' or 'bucket-name/prefix/'")
        raise typer.Exit(code=1)
    
    # Initialize transfer object
    transfer_obj = GDriveToS3Transfer(s3_bucket, s3_prefix)
    
    # Authenticate with Google Drive
    # try:
        # transfer_obj.authenticate_gdrive(credentials_file)
    # except Exception as e:
    #     typer.echo(f"❌ Authentication failed: {e}")
    #     raise typer.Exit(code=1)
    
    # Get folder name
    folder_name = transfer_obj.get_folder_name(folder_id)
    typer.echo(f"\n📂 Source folder: {folder_name}")
    typer.echo(f"☁️  Target S3: s3://{s3_bucket}/{s3_prefix}")
    typer.echo("\n" + "=" * 60)
    
    # Confirm before starting
    confirm = typer.confirm("\nDo you want to proceed with the transfer?")
    if not confirm:
        typer.echo("❌ Transfer cancelled")
        raise typer.Exit()
    
    # Start transfer
    start_time = datetime.now()
    try:
        transfer_obj.process_folder_recursively(folder_id)
    except KeyboardInterrupt:
        typer.echo("\n\n⚠️  Transfer interrupted by user")
    except Exception as e:
        typer.echo(f"\n❌ Error during transfer: {e}")
        raise typer.Exit(code=1)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Summary
    typer.echo("\n" + "=" * 60)
    typer.echo("📊 Transfer Summary")
    typer.echo("=" * 60)
    typer.echo(f"✅ Successfully transferred: {transfer_obj.transferred_count} files")
    typer.echo(f"❌ Failed: {transfer_obj.failed_count} files")
    typer.echo(f"⏱️  Total time: {duration}")
    typer.echo("=" * 60)
    
    # Save metadata to CSV
    transfer_obj.save_metadata_to_csv(csv_output)

# if __name__ == "__main__":
#     app()