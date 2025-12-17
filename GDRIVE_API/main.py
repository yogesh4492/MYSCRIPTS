from GDRIVE.authentication import *
from GDRIVE.list_gdrive_files import *
from JSON.JSON_HELPER import *
from CSV.CSV_READ_WRITE import *
from GDRIVE.gdrive_to_s3 import *
from GDRIVE.gup import *
from GDRIVE.gdown import *
from GDRIVE.s3_to_gdrive import *
from GDRIVE.gdrive_to_s3 import GDriveToS3Transfer
import typer
app=typer.Typer()

@app.command()
def s3_to_gdrive_transfer(
    s3_path: str = typer.Argument(..., help="S3 path (e.g., s3://bucket-name/folder/)"),
    gdrive_folder_id: str = typer.Argument(..., help="Google Drive folder ID"),
    csv_output: str = typer.Option(
        "transfer_metadata.csv",
        "--csv",
        "-c",
        help="Output CSV file for metadata"
    ),
    aws_profile: str = typer.Option(
        None,
        "--profile",
        "-p",
        help="AWS profile name (optional)"
    ),
    credentials: str = typer.Option(
        "credentials.json",
        "--credentials",
        help="Path to Google credentials.json"
    )
):
    """
    Transfer files from S3 to Google Drive recursively
    
    Example:
        python script.py s3://my-bucket/my-folder/ 1a2b3c4d5e6f7g8h9i0j
    """
    console.print("[bold blue]S3 to Google Drive Transfer Tool[/bold blue]")
    console.print("="*60 + "\n")
    
    try:
        transferer = S3ToGDriveTransfer(aws_profile=aws_profile)
        transferer.transfer(s3_path, gdrive_folder_id, csv_output)
        
    except Exception as e:
        console.print(f"\n[bold red]Error:[/bold red] {str(e)}")
        raise typer.Exit(code=1)

@app.command()
def list_s3_files(
    s3_path: str = typer.Argument(..., help="S3 path to list"),
    aws_profile: str = typer.Option(None, "--profile", "-p", help="AWS profile name")):
    try:
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
            s3_client = session.client('s3')
        else:
            s3_client = boto3.client('s3')
        
        s3_path = s3_path.replace('s3://', '')
        parts = s3_path.split('/', 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''
        
        console.print(f"\n[cyan]Listing:[/cyan] s3://{bucket}/{prefix}\n")
        
        paginator = s3_client.get_paginator('list_objects_v2')
        count = 0
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            rows=[]
            if 'Contents' in page:
                for obj in page['Contents']:
                    if not obj['Key'].endswith('/'):
                        row={}
                        row['S3PATH']=obj['Key']
                        console.print(f"  📄 {obj['Key']}")
                        count += 1
                        rows.append(row)
                        
        write_csv("S3_file_Metadata.csv",["S3PATH"],rows)
        console.print(f"\n[green]Total files: {count}[/green]")
        
    except Exception as e:
        console.print(f"[red]Error:[/red] {str(e)}")
        raise typer.Exit(code=1)



@app.command()
def gdrive_to_s3(
    folder_id: str = typer.Argument(..., help="Google Drive folder ID"),
    s3_path: str = typer.Argument(..., help="S3 path (e.g., 's3://bucket/prefix/' or 'bucket/prefix/')"),
    credentials_file: str = typer.Option("credentials.json", help="Path to Google OAuth credentials JSON file"),
    csv_output: str = typer.Option(None, help="Custom CSV output filename (default: auto-generated with timestamp)")):
    
        s3_bucket, s3_prefix = parse_s3_path(s3_path)
        print(s3_bucket,s3_prefix)
        transfer_obj = GDriveToS3Transfer(s3_bucket,s3_prefix)
        folder_name = transfer_obj.get_folder_name(folder_id)
        transfer_obj.process_folder_recursively(folder_id)
        transfer_obj.save_metadata_to_csv(csv_output)


@app.command()
def count_Gdrive_files(folder_id:str=typer.Argument(...,help="Input Gdrive folder Id")):
    service=auth()
    files=list_files(folder_id,service)
    print(f"Total Files: {len(files)}")

@app.command("list_gdrive_files_With_Metadata")
def main(folder_id:str=typer.Argument(...,help="Input Gdrive folder Id")
         ,output_csv:str=typer.Option("Metadata.csv","--output_csv",help="Output Csv File For MetaData")
         ,output_json:str=typer.Option("Metadata.json","--output_json",help="output Json File For MetaData")):
    service=auth()
    files=list_files(folder_id,service)
    print(f"Total Files: {len(files)}")
    fields=[i for i in files[0].keys()]
    fields.insert(1,"ParentFolder")
    fields.insert(2,"FULLPATH")
    rows=[]
    for i in files:
        row={k:i[k] for k in i if k in fields}
        row['parents']=i['parents'][0]
        row['ParentFolder']=get_folder_name(row['parents'],service)
        # if 
        row['FULLPATH']=os.path.join(row['ParentFolder'],row['name'])
        row['size']=int(row['size'])//1024
        if row['size']>100:
             row['size']=f"{row['size']/1024:.2f} MB"
        rows.append(row)
    write_csv(output_csv,fields,rows)
    typer.echo(f"Metadata Saved In Csv File : {output_csv}")
    Dump_json(output_json,rows)
    typer.echo(f"Metadata Saved In Jsom File : {output_json}")


@app.command()
def upload_To_GDrive(
    folder_id: Optional[str] = typer.Argument(None, help="Google Drive parent folder ID (optional, uploads to root if not provided)"),
    local_folder: Path = typer.Argument(..., help="Local folder to upload"),
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
        "upload_metadata.csv",
        "--metadata",
        "-m",
        help="Output CSV file for metadata"
    ),
    workers: int = typer.Option(
        3,
        "--workers",
        "-w",
        help="Number of parallel upload workers"
    )
):
    """
    Upload files to Google Drive with folder structure preservation.
    
    Example:
        # Upload to root
        python gdrive_uploader.py ./my_folder
        
        # Upload to specific folder
        python gdrive_uploader.py ./my_folder 1abc123XYZ
        
        # With exclusions
        python gdrive_uploader.py ./my_folder -e .git -e node_modules
    """
    
    console.print("[bold green]Google Drive Bulk Uploader[/bold green]\n")
    
    # Validate local folder
    if not local_folder.exists():
        console.print(f"[red]Error: Local folder '{local_folder}' does not exist![/red]")
        raise typer.Exit(1)
    
    if not local_folder.is_dir():
        console.print(f"[red]Error: '{local_folder}' is not a directory![/red]")
        raise typer.Exit(1)
    
    # Authenticate
    console.print("[cyan]Authenticating with Google Drive...[/cyan]")
    creds = authenticate(credentials_file, token_file)
    # service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    service=auth()
    console.print("[green]✓ Authentication successful[/green]\n")
    
    # Verify parent folder if provided
    if folder_id:
        try:
            service.files().get(fileId=folder_id, fields='id, name').execute()
            console.print(f"[green]✓ Parent folder verified[/green]\n")
        except Exception as e:
            console.print(f"[red]Error: Cannot access folder ID {folder_id}: {e}[/red]")
            raise typer.Exit(1)
    else:
        console.print("[yellow]No parent folder specified, uploading to Drive root[/yellow]\n")
    
    # Find all files
    console.print("[cyan]Scanning for files...[/cyan]")
    # exclude_patterns = list(exclude) if exclude else ['.git', '__pycache__', '.DS_Store', 'node_modules']
    all_files = find_all_files(local_folder)
    console.print(f"[green]✓ Found {len(all_files)} file(s) to upload[/green]\n")
    
    if not all_files:
        console.print("[yellow]No files to upload![/yellow]")
        return
    
    # Calculate total size
    total_size = sum(f.stat().st_size for f in all_files)
    console.print(f"[cyan]Total size: {total_size / (1024*1024):.2f} MB[/cyan]\n")
    
    # Initialize stats
    stats = UploadStats()
    
    # Upload files with progress bar
    console.print("[bold]Starting uploads...[/bold]\n")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        # UploadColumn(),
        TransferSpeedColumn(),
        console=console
    ) as progress:
        
        task = progress.add_task(
            "[cyan]Uploading files...",
            total=len(all_files)
        )
        
        # Prepare arguments for parallel upload
        upload_args = [
            (creds, file_path, local_folder, folder_id, stats)
            for file_path in all_files
        ]
        
        # Use ThreadPoolExecutor for parallel uploads
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            for result in executor.map(upload_file_wrapper, upload_args):
                if result:
                    if result[0] == 'success':
                        progress.console.print(f"  [green]✓ {result[1]}[/green]")
                    elif result[0] == 'failed':
                        progress.console.print(f"  [red]✗ {result[1]}: {result[2]}[/red]")
                progress.advance(task)
    
    # Save metadata
    console.print(f"\n[cyan]Saving metadata to {metadata_file}...[/cyan]")
    save_metadata_csv(stats.metadata, metadata_file)
    console.print("[green]✓ Metadata saved[/green]\n")
    
    # Save failed files log if any
    if stats.failed_files:
        failed_log = Path("failed_uploads.csv")
        with open(failed_log, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=['file_name', 'error'])
            writer.writeheader()
            writer.writerows(stats.failed_files)
        console.print(f"[yellow]Failed uploads logged to {failed_log}[/yellow]\n")
    
    # Display summary
    table = Table(title="Upload Summary")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("Files Uploaded", str(stats.files_uploaded))
    table.add_row("Files Failed", str(stats.files_failed))
    table.add_row("Total Size", f"{stats.total_size / (1024*1024):.2f} MB")
    table.add_row("Metadata File", str(metadata_file))
    
    console.print(table)
    
    if stats.files_failed > 0:
        console.print(f"\n[yellow]⚠ {stats.files_failed} file(s) failed to upload. Check failed_uploads.csv for details.[/yellow]")
    
    console.print("\n[bold green]✓ Upload complete![/bold green]")

@app.command()
def download_from_Gdrive(
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


if __name__=="__main__":
    app()