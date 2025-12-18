import tarfile
import shutil
from pathlib import Path
from typing import Optional
import typer
from typing_extensions import Annotated

app = typer.Typer()

def extract_tar_file(tar_path: Path, extract_to: Optional[Path] = None) -> bool:
    """
    Extract a tar file and return True if successful.
    
    Args:
        tar_path: Path to the tar file
        extract_to: Directory to extract to (defaults to tar file's directory)
    
    Returns:
        True if extraction was successful, False otherwise
    """
    if extract_to is None:
        extract_to = tar_path.parent
    
    try:
        with tarfile.open(tar_path, 'r:*') as tar:
            tar.extractall(path=extract_to)
        return True
    except Exception as e:
        typer.echo(f"✗ Error extracting {tar_path.name}: {str(e)}", err=True)
        return False

def find_and_extract_tar_files(
    root_dir: Path,
    remove_after: bool = True,
    dry_run: bool = False
) -> tuple[int, int]:
    """
    Recursively find and extract all tar files in directory tree.
    
    Args:
        root_dir: Root directory to search
        remove_after: Whether to remove tar files after extraction
        dry_run: If True, only show what would be done without doing it
    
    Returns:
        Tuple of (total_found, total_extracted)
    """
    # Find all tar files recursively
    tar_patterns = ['*.tar', '*.tar.gz', '*.tgz', '*.tar.bz2', '*.tar.xz']
    tar_files = []
    
    for pattern in tar_patterns:
        tar_files.extend(root_dir.rglob(pattern))
    
    # Remove duplicates and sort
    tar_files = sorted(set(tar_files))
    
    total_found = len(tar_files)
    total_extracted = 0
    
    if total_found == 0:
        typer.echo("No tar files found.")
        return 0, 0
    
    typer.echo(f"Found {total_found} tar file(s)\n")
    
    for tar_path in tar_files:
        relative_path = tar_path.relative_to(root_dir)
        
        if dry_run:
            typer.echo(f"[DRY RUN] Would extract: {relative_path}")
            if remove_after:
                typer.echo(f"[DRY RUN] Would remove: {relative_path}")
            total_extracted += 1
        else:
            typer.echo(f"Extracting: {relative_path}")
            
            # Extract the tar file
            if extract_tar_file(tar_path):
                total_extracted += 1
                typer.echo(f"✓ Extracted: {relative_path}")
                
                # Remove the tar file if requested
                if remove_after:
                    try:
                        tar_path.unlink()
                        typer.echo(f"✓ Removed: {relative_path}")
                    except Exception as e:
                        typer.echo(f"✗ Error removing {relative_path}: {str(e)}", err=True)
            else:
                typer.echo(f"✗ Failed to extract: {relative_path}")
        
        typer.echo()  # Empty line for readability
    
    return total_found, total_extracted

@app.command()
def extract(
    directory: Annotated[Path, typer.Argument(help="Root directory to search for tar files")],
    keep_tar: Annotated[bool, typer.Option("--keep", "-k", help="Keep tar files after extraction")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", "-d", help="Show what would be done without doing it")] = False,
    confirm: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt")] = False
):
    """
    Recursively extract all tar files from directory and subdirectories.
    By default, tar files are removed after extraction.
    """
    # Validate directory
    if not directory.exists():
        typer.echo(f"Error: Directory not found: {directory}", err=True)
        raise typer.Exit(1)
    
    if not directory.is_dir():
        typer.echo(f"Error: Not a directory: {directory}", err=True)
        raise typer.Exit(1)
    
    # Show what will happen
    action = "extract (keeping tar files)" if keep_tar else "extract and remove tar files"
    typer.echo(f"Will {action} in: {directory.absolute()}\n")
    
    # Confirmation prompt (unless --yes flag is used or it's a dry run)
    if not confirm and not dry_run and not keep_tar:
        response = typer.confirm("⚠️  Tar files will be DELETED after extraction. Continue?")
        if not response:
            typer.echo("Operation cancelled.")
            raise typer.Exit(0)
    
    # Process tar files
    total_found, total_extracted = find_and_extract_tar_files(
        directory,
        remove_after=not keep_tar,
        dry_run=dry_run
    )
    
    # Summary
    if dry_run:
        typer.echo(f"[DRY RUN] Summary:")
    else:
        typer.echo(f"Summary:")
    
    typer.echo(f"  Total tar files found: {total_found}")
    typer.echo(f"  Successfully extracted: {total_extracted}")
    
    if total_extracted < total_found:
        typer.echo(f"  Failed: {total_found - total_extracted}")

if __name__ == "__main__":
    app()