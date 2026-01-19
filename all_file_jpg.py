import os
import csv
import io
import gc
import time
import shutil
import tempfile
from pathlib import Path
from typing import Tuple

import typer
from PIL import Image
from pillow_heif import register_heif_opener
from pdf2image import convert_from_path
from concurrent.futures import ThreadPoolExecutor,as_completed
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn, MofNCompleteColumn

import pickle


# ---------------- CONFIG ----------------
Image.MAX_IMAGE_PIXELS = None
register_heif_opener()

SCOPES = ["https://www.googleapis.com/auth/drive"]
app = typer.Typer(help="SAFE PDF/Image to JPG converter from Google Drive")

# ---------------- GOOGLE DRIVE AUTH ----------------
def get_gdrive_service():
    creds = None
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as f:
            creds = pickle.load(f)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open("token.pickle", "wb") as f:
            pickle.dump(creds, f)

    return build("drive", "v3", credentials=creds)


# ---------------- HELPERS ----------------
def extract_file_id(link: str) -> str:
    if "/d/" in link:
        return link.split("/d/")[1].split("/")[0]
    if "id=" in link:
        return link.split("id=")[1].split("&")[0]
    return link.strip()


def detect_file_type(path: Path) -> str:
    # Detect image
    try:
        with Image.open(path):
            return "image"
    except:
        pass

    # Detect PDF
    try:
        with open(path, "rb") as f:
            if f.read(4) == b"%PDF":
                return "pdf"
    except:
        pass

    return "unknown"


def download_drive_file(service, file_id: str, dest: str):
    meta = service.files().get(
        fileId=file_id,
        fields="mimeType,name"
    ).execute()

    if meta["mimeType"].startswith("application/vnd.google-apps"):
        raise ValueError("Google Docs/Sheets/Slides not supported")

    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    with open(dest, "wb") as f:
        f.write(fh.getvalue())


def upload_to_drive(service, file_path: str, folder_id: str, name: str) -> str:
    media = MediaFileUpload(file_path, mimetype="image/jpeg")
    file = service.files().create(
        body={"name": name, "parents": [folder_id]},
        media_body=media,
        fields="id,webViewLink",
    ).execute()

    service.permissions().create(
        fileId=file["id"],
        body={"type": "anyone", "role": "reader"}
    ).execute()

    return file["webViewLink"]


# ---------------- CORE CONVERTER ----------------
# def convert_to_jpg(input_path: Path, output_path: Path):
#     file_type = detect_file_type(input_path)

#     if file_type == "image":
#         img = Image.open(input_path)
#         if img.mode != "RGB":
#             img = img.convert("RGB")
#         img.save(output_path, "JPEG", quality=95, optimize=True)
#         img.close()

#     elif file_type == "pdf":
#         images = convert_from_path(
#             str(input_path),
#             dpi=300,
#             thread_count=1,  # MUST BE 1
#         )
#         img = images[0]
#         if img.mode != "RGB":
#             img = img.convert("RGB")
#         img.save(output_path, "JPEG", quality=95, optimize=True)
#         img.close()

#         for im in images:
#             im.close()
#         images.clear()

#     else:
#         raise ValueError("Unsupported file type")

#     gc.collect()
def convert_to_jpg(input_path: Path, output_dir: Path) -> Path:
    """
    Converts an image or PDF to JPG.
    Keeps original file name, adds .jpg extension.
    Returns path to the output JPG.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    file_stem = input_path.stem  # original name without extension
    output_path = output_dir / f"{file_stem}.jpg"

    file_type = detect_file_type(input_path)

    if file_type == "image":
        img = Image.open(input_path)
        if img.mode != "RGB":
            img = img.convert("RGB")
        img.save(output_path, "JPEG", quality=95, optimize=True)
        img.close()

    elif file_type == "pdf":
        images = convert_from_path(
            str(input_path),
            dpi=300,
            thread_count=1,  # must be 1 for stability
        )
        img = images[0]
        if img.mode != "RGB":
            img = img.convert("RGB")
        img.save(output_path, "JPEG", quality=95, optimize=True)
        img.close()

        for im in images:
            im.close()
        images.clear()

    else:
        raise ValueError("Unsupported file type")

    gc.collect()
    return output_path


# ---------------- CSV COMMAND ----------------
@app.command("csv-gdrive")
def csv_gdrive(
    csv_input: str,
    folder_id: str,
    output_csv: str = "output.csv",
    column: str = "drive_link",
):
    with open(csv_input, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    results = []

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        TimeElapsedColumn(),
    )

    with progress:
        task = progress.add_task("Processing files...", total=len(rows))
        with ThreadPoolExecutor(max_workers=8) as e:

            for idx, row in enumerate(rows):
                old_link = row.get(column, "").strip()

                try:
                    if not old_link:
                        raise ValueError("Empty link")

                    service = get_gdrive_service()
                    file_id = extract_file_id(old_link)

                    # 🔧 GET ORIGINAL FILENAME FROM DRIVE
                    meta = service.files().get(
                        fileId=file_id,
                        fields="name"
                    ).execute()
                    original_name = Path(meta["name"]).stem  # filename without extension

                    temp_dir = tempfile.mkdtemp()
                    inp = Path(temp_dir) / "input"

                    # 1️⃣ Download
                    download_drive_file(service, file_id, str(inp))

                    # 2️⃣ Convert
                    out = convert_to_jpg(inp, Path(temp_dir))

                    # 3️⃣ Upload with ORIGINAL name + .jpg
                    new_link = upload_to_drive(
                        service,
                        str(out),
                        folder_id,
                        f"{original_name}.jpg"  # 🔧 USE ORIGINAL NAME
                    )

                    status = "Success"

                except Exception as e:
                    new_link = ""
                    status = str(e)

                finally:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    gc.collect()
                    progress.advance(task)

                results.append({
                    **row,
                    "old_drive_link": old_link,
                    "new_drive_link": new_link,
                    "status": status
                })

                time.sleep(0.5)

        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)

        typer.echo(f"✅ Finished. Output saved to {output_csv}")
# def csv_gdrive(
#     csv_input: str,
#     folder_id: str,
#     output_csv: str = "output.csv",
#     column: str = "drive_link",
# ):
#     with open(csv_input, newline="", encoding="utf-8") as f:
#         rows = list(csv.DictReader(f))

#     results = []

#     progress = Progress(
#         SpinnerColumn(),
#         TextColumn("[bold blue]{task.description}"),
#         BarColumn(),
#         MofNCompleteColumn(),
#         TimeRemainingColumn(),
#         TimeElapsedColumn(),
#     )

#     with progress:
#         task = progress.add_task("Processing files...", total=len(rows))

#         for idx, row in enumerate(rows):
#             old_link = row.get(column, "").strip()

#             try:
#                 if not old_link:
#                     raise ValueError("Empty link")

#                 service = get_gdrive_service()
#                 file_id = extract_file_id(old_link)

#                 temp_dir = tempfile.mkdtemp()
#                 inp = Path(temp_dir) / "input"

#                 # 1️⃣ Download first
#                 download_drive_file(service, file_id, str(inp))

#                 # 2️⃣ Convert → keeps original name, adds .jpg
#                 out = convert_to_jpg(inp, Path(temp_dir))

#                 # 3️⃣ Upload using same base name + .jpg
#                 new_link = upload_to_drive(
#                     service,
#                     str(out),
#                     folder_id,
#                     out.name  # EXACT original filename + .jpg
#                     )

#                 status = "Success"

#             except Exception as e:
#                 new_link = ""
#                 status = str(e)

#             finally:
#                 shutil.rmtree(temp_dir, ignore_errors=True)
#                 gc.collect()
#                 progress.advance(task)

#             results.append({
#                 **row,
#                 "old_drive_link": old_link,
#                 "new_drive_link": new_link,
#                 "status": status
#             })

#             time.sleep(0.5)  # prevents Drive throttling

#     with open(output_csv, "w", newline="", encoding="utf-8") as f:
#         writer = csv.DictWriter(f, fieldnames=results[0].keys())
#         writer.writeheader()
#         writer.writerows(results)

    # typer.echo(f"✅ Finished. Output saved to {output_csv}")


# ---------------- ENTRY ----------------
if __name__ == "__main__":
    app()
