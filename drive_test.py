import os
from googleapiclient.discovery import build
from google.oauth2 import service_account

# --- CONFIG ---
SERVICE_ACCOUNT_FILE = "cgeds-477316-4793ae510ea4.json"
SCOPES = ["https://www.googleapis.com/auth/drive"]
DRIVE_ROOT_ID = "1bCOxBAPn_c11uOyE_CvZYxaV5wWGBBCa"  # même que ton app Flask

# --- Authentification ---
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

drive_service = build("drive", "v3", credentials=creds)

# --- Fonction pour lister les fichiers/dossiers d'un parent ---
def list_files(parent_id):
    results = drive_service.files().list(
        q=f"'{parent_id}' in parents and trashed=false",
        fields="files(id, name, mimeType)"
    ).execute()
    items = results.get("files", [])
    return items

# --- Lister tous les dossiers/fichiers du ROOT ---
items = list_files(DRIVE_ROOT_ID)
if not items:
    print("Aucun fichier ou dossier trouvé dans le Drive ROOT.")
else:
    print("Fichiers/Dossiers trouvés:")
    for i, f in enumerate(items, 1):
        print(f"{i}. {f['name']} ({f['mimeType']})")
