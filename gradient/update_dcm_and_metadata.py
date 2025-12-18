import pydicom
import csv
from pathlib import Path
from typing import Optional, List
import typer
from typing_extensions import Annotated
from glob import glob
from pydicom.multival import MultiValue
from pydicom.valuerep import PersonName
import re

app = typer.Typer(help="Replace text in CSV and DICOM files recursively")

# Text-based DICOM VRs
TEXT_VRS = {"LO", "PN", "SH", "LT", "ST", "UT", "CS", "AE", "UI", "AS", "DA", "TM", "DT"}

def enforce_vr_length(elem, value):
    """Enforce VR-specific length constraints."""
    if elem.VR == "SH":  # Short String: max 16 chars
        return value[:16]
    elif elem.VR == "LO":  # Long String: max 64 chars
        return value[:64]
    elif elem.VR == "CS":  # Code String: max 16 chars
        return value[:16]
    elif elem.VR == "AE":  # Application Entity: max 16 chars
        return value[:16]
    return value

def replace_text_in_dicom(ds, old_text, new_text, case_sensitive=False):
    """
    Replace text in DICOM dataset, handling all value types correctly.
    
    Returns:
        True if any modifications were made
    """
    modified = False
    
    for elem in ds.iterall():
        # Skip non-text VRs and pixel data
        if elem.VR not in TEXT_VRS or elem.tag == 0x7fe00010:
            continue
        
        val = elem.value
        
        # Handle string values
        if isinstance(val, str):
            if case_sensitive:
                if old_text in val:
                    elem.value = enforce_vr_length(elem, val.replace(old_text, new_text))
                    modified = True
            else:
                pattern = re.compile(re.escape(old_text), re.IGNORECASE)
                if pattern.search(val):
                    new_val = pattern.sub(new_text, val)
                    elem.value = enforce_vr_length(elem, new_val)
                    modified = True
        
        # Handle MultiValue (list of values)
        elif isinstance(val, MultiValue):
            new_vals = []
            changed = False
            for v in val:
                if isinstance(v, str):
                    if case_sensitive:
                        if old_text in v:
                            v = enforce_vr_length(elem, v.replace(old_text, new_text))
                            changed = True
                    else:
                        pattern = re.compile(re.escape(old_text), re.IGNORECASE)
                        if pattern.search(v):
                            v = enforce_vr_length(elem, pattern.sub(new_text, v))
                            changed = True
                new_vals.append(v)
            if changed:
                elem.value = new_vals
                modified = True
        
        # Handle PersonName
        elif isinstance(val, PersonName):
            pn = str(val)
            if case_sensitive:
                if old_text in pn:
                    elem.value = pn.replace(old_text, new_text)
                    modified = True
            else:
                pattern = re.compile(re.escape(old_text), re.IGNORECASE)
                if pattern.search(pn):
                    elem.value = pattern.sub(new_text, pn)
                    modified = True
    
    return modified

def process_csv_file(csv_path, output_path, old_text, new_text, case_sensitive=False):
    """Replace text in CSV file and save to output path."""
    replacements = 0
    
    try:
        # Read CSV content
        with open(csv_path, 'r', encoding='utf-8', newline='') as f:
            content = f.read()
        
        # Replace text
        if case_sensitive:
            new_content = content.replace(old_text, new_text)
            replacements = content.count(old_text)
        else:
            pattern = re.compile(re.escape(old_text), re.IGNORECASE)
            matches = pattern.findall(content)
            replacements = len(matches)
            new_content = pattern.sub(new_text, content)
        
        # Write to output
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            f.write(new_content)
        
        return replacements
    
    except Exception as e:
        typer.echo(f"❌ Error processing CSV {csv_path.name}: {str(e)}", err=True)
        return 0

def find_files_in_structure(root_dir):
    """Find CSV and DICOM files in the directory structure."""
    csv_files = []
    dicom_files = []
    
    # Find CSV files in 'csv' subfolder
    csv_dir = root_dir / 'csv'
    if csv_dir.exists():
        csv_files.extend(csv_dir.rglob('*.csv'))
    
    # Also check root directory for CSV files
    csv_files.extend(root_dir.glob('*.csv'))
    
    # Find DICOM files in 'dicomweb' subfolder (recursively)
    dicomweb_dir = root_dir / 'dicomweb'
    if dicomweb_dir.exists():
        dicom_pattern = str(dicomweb_dir / "**/*.dcm")
        dicom_files = [Path(f) for f in glob(dicom_pattern, recursive=True)]
    
    # Remove duplicates
    csv_files = list(set(csv_files))
    dicom_files = list(set(dicom_files))
    
    return csv_files, dicom_files

@app.command()
def replace(
    input_dir: Annotated[Path, typer.Argument(help="Root directory containing csv/ and dicomweb/ subfolders")],
    output_dir: Annotated[Path, typer.Argument(help="Output directory for modified files")],
    old_text: Annotated[str, typer.Option("--old", "-o", help="Text to replace")] = "GRDN",
    new_text: Annotated[str, typer.Option("--new", "-n", help="Replacement text")] = "SHAIP",
    case_sensitive: Annotated[bool, typer.Option("--case-sensitive", "-c", help="Case-sensitive replacement")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run", "-d", help="Show what would be done without doing it")] = False,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Show detailed output")] = False,
):
    """
    Replace text (GRDN -> SHAIP) in both CSV files and DICOM files.
    
    Expected directory structure:
      input_dir/
        ├── csv/
        │   └── *.csv (dicomweb metadata files)
        └── dicomweb/
            └── **/*.dcm (DICOM files in nested subfolders)
    """
    # Validate input
    if not input_dir.exists():
        typer.echo(f"❌ Error: Directory not found: {input_dir}", err=True)
        raise typer.Exit(1)
    
    if not input_dir.is_dir():
        typer.echo(f"❌ Error: Not a directory: {input_dir}", err=True)
        raise typer.Exit(1)
    
    typer.echo(f"🔍 Scanning directory: {input_dir.absolute()}\n")
    
    # Find files
    csv_files, dicom_files = find_files_in_structure(input_dir)
    
    typer.echo(f"📄 Found {len(csv_files)} CSV file(s)")
    typer.echo(f"🏥 Found {len(dicom_files)} DICOM file(s)\n")
    
    if not csv_files and not dicom_files:
        typer.echo("❌ No files found to process.", err=True)
        raise typer.Exit(1)
    
    typer.echo(f"🔄 Replacing '{old_text}' with '{new_text}'")
    mode = "case-sensitive" if case_sensitive else "case-insensitive"
    typer.echo(f"   Mode: {mode}\n")
    
    if dry_run:
        typer.echo("⚠️  [DRY RUN MODE - No files will be modified]\n")
    
    # Process CSV files
    csv_replacements = 0
    csv_updated = 0
    csv_skipped = 0
    
    if csv_files:
        typer.echo("="*60)
        typer.echo("📄 PROCESSING CSV FILES")
        typer.echo("="*60 + "\n")
        
        for csv_file in csv_files:
            relative_path = csv_file.relative_to(input_dir)
            output_path = output_dir / relative_path
            
            if verbose:
                typer.echo(f"Processing: {relative_path}")
            
            if dry_run:
                if verbose:
                    typer.echo(f"  [DRY RUN] Would save to: {output_path.relative_to(output_dir)}\n")
                csv_updated += 1
            else:
                replacements = process_csv_file(csv_file, output_path, old_text, new_text, case_sensitive)
                if replacements > 0:
                    if verbose:
                        typer.echo(f"  ✔ Made {replacements} replacement(s)\n")
                    csv_replacements += replacements
                    csv_updated += 1
                else:
                    csv_skipped += 1
                    if verbose:
                        typer.echo(f"  ➖ No replacements needed\n")
    
    # Process DICOM files
    dicom_updated = 0
    dicom_skipped = 0
    dicom_failed = 0
    
    if dicom_files:
        typer.echo("="*60)
        typer.echo("🏥 PROCESSING DICOM FILES")
        typer.echo("="*60 + "\n")
        
        for i, dicom_file in enumerate(dicom_files, 1):
            relative_path = dicom_file.relative_to(input_dir)
            output_path = output_dir / relative_path
            
            # Show progress every 50 files
            if i % 50 == 0:
                typer.echo(f"   Progress: {i}/{len(dicom_files)} files...")
            
            if verbose and i <= 10:
                typer.echo(f"Processing: {relative_path}")
            
            if dry_run:
                if verbose and i <= 10:
                    typer.echo(f"  [DRY RUN] Would save to: {output_path.relative_to(output_dir)}\n")
                dicom_updated += 1
            else:
                try:
                    ds = pydicom.dcmread(str(dicom_file))
                    
                    if replace_text_in_dicom(ds, old_text, new_text, case_sensitive):
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        ds.save_as(str(output_path))
                        dicom_updated += 1
                        if verbose and i <= 10:
                            typer.echo(f"  ✔ Updated\n")
                    else:
                        dicom_skipped += 1
                        if verbose and i <= 10:
                            typer.echo(f"  ➖ No changes needed\n")
                
                except Exception as e:
                    dicom_failed += 1
                    typer.echo(f"❌ Failed: {relative_path} → {e}")
        
        if not verbose and len(dicom_files) > 10:
            typer.echo(f"   (showing summary only, use -v for details)\n")
    
    # Summary
    typer.echo("\n" + "="*60)
    typer.echo("📊 SUMMARY")
    typer.echo("="*60)
    
    if csv_files:
        typer.echo(f"\n📄 CSV Files:")
        typer.echo(f"   Found: {len(csv_files)}")
        if not dry_run:
            typer.echo(f"   ✔ Updated: {csv_updated}")
            typer.echo(f"   ➖ Skipped: {csv_skipped}")
            typer.echo(f"   Total replacements: {csv_replacements}")
    
    if dicom_files:
        typer.echo(f"\n🏥 DICOM Files:")
        typer.echo(f"   Found: {len(dicom_files)}")
        if not dry_run:
            typer.echo(f"   ✔ Updated: {dicom_updated}")
            typer.echo(f"   ➖ Skipped: {dicom_skipped}")
            if dicom_failed > 0:
                typer.echo(f"   ❌ Failed: {dicom_failed}")
    
    if not dry_run:
        typer.echo(f"\n✅ All modified files saved to: {output_dir.absolute()}")
    else:
        typer.echo(f"\n⚠️  [DRY RUN] No files were modified")

if __name__ == "__main__":
    app()