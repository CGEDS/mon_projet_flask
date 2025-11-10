import os
import io
import json
from pathlib import Path
from datetime import datetime, timezone
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
from supabase import create_client, Client
from dotenv import load_dotenv

# --- Chargement des variables d'environnement ---
load_dotenv()

# --- Config de base ---
BASE_DIR = Path(__file__).parent
CACHE_DIR = BASE_DIR / "pdf_cache"
CACHE_DIR.mkdir(exist_ok=True)

# --- Supabase ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Google Drive via Service Account JSON ---
SERVICE_ACCOUNT_INFO = json.loads(os.environ["GOOGLE_SERVICE_JSON"])

# ✅ Correction ici : interpréter les "\n" littéraux comme de vrais retours à la ligne
SERVICE_ACCOUNT_INFO["private_key"] = SERVICE_ACCOUNT_INFO["private_key"].replace("\\n", "\n")

SCOPES = ["https://www.googleapis.com/auth/drive"]
DRIVE_ROOT_ID = os.environ.get("DRIVE_ROOT_ID")

creds = service_account.Credentials.from_service_account_info(
    SERVICE_ACCOUNT_INFO, scopes=SCOPES
)
drive_service = build("drive", "v3", credentials=creds)

# --- Fonction pour synchroniser les dossiers Drive ---
def sync_folder(folder_id=DRIVE_ROOT_ID, parent_path=""):
    query = f"'{folder_id}' in parents and trashed=false"
    results = drive_service.files().list(
        q=query,
        fields="files(id,name,mimeType,modifiedTime,size)"
    ).execute()

    items = results.get("files", [])
    for f in items:
        relpath = f"{parent_path}/{f['name']}" if parent_path else f["name"]
        report_type = relpath.split("/")[0] if "/" in relpath else "AUTRE"

        if f["mimeType"] == "application/vnd.google-apps.folder":
            sync_folder(f["id"], relpath)
        else:
            size_bytes = int(f.get("size", 0))
            now = datetime.now(timezone.utc).isoformat()

            payload = {
                "relpath": relpath,
                "name": f["name"],
                "drive_id": f["id"],
                "views": 0,
                "downloads": 0,
                "status": "non_lu",
                "history": [],
                "last_modified": now,
                "size": size_bytes,
                "type": report_type
            }

            # Vérifie si le fichier existe déjà dans Supabase
            r = supabase.table("pdf_stats").select("*").eq("relpath", relpath).execute()
            if r.data:
                supabase.table("pdf_stats").update({
                    "drive_id": f["id"],
                    "last_modified": now,
                    "name": f["name"],
                    "size": size_bytes,
                    "type": report_type
                }).eq("relpath", relpath).execute()
            else:
                supabase.table("pdf_stats").insert(payload).execute()
