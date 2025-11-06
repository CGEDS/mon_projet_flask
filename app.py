import os, io, base64, threading, time, qrcode
from pathlib import Path
from datetime import datetime, timezone
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, send_file, abort, jsonify, flash
)
from googleapiclient.discovery import build
from google.oauth2 import service_account
import psycopg2
from zipfile import ZipFile, ZIP_DEFLATED

# --- CONFIG ---
BASE_DIR = Path(__file__).parent
SECRET_KEY = os.environ.get("FLASK_SECRET", "change_me_123")
USERS = {"CGEDS": "CGEDS2025"}

# --- PostgreSQL (Render) ---
DB_CONFIG = {
    "host": "dpg-d46j0j3e5dus73arhtdg-a.oregon-postgres.render.com",
    "database": "cgeds_db",
    "user": "cgeds_db_user",
    "password": "ThpgrpKHtheA9JwQyCYmqkbNq2vfhJEC",
    "port": 5432
}

def get_conn():
    return psycopg2.connect(**DB_CONFIG)

# --- Google Drive ---
SERVICE_ACCOUNT_FILE = BASE_DIR / "cgeds-477316-4793ae510ea4.json"
SCOPES = ["https://www.googleapis.com/auth/drive"]
DRIVE_ROOT_ID = "1bCOxBAPn_c11uOyE_CvZYxaV5wWGBBCa"

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
drive_service = build("drive", "v3", credentials=creds)

# --- PostgreSQL utils ---
def ensure_meta(relpath, name=None, content=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT relpath FROM pdf_stats WHERE relpath=%s", (relpath,))
    if cur.fetchone() is None:
        cur.execute("""
            INSERT INTO pdf_stats (relpath, name, views, downloads, status, history, content, last_modified)
            VALUES (%s, %s, 0, 0, 'non_lu', '[]'::jsonb, %s, %s)
        """, (relpath, name or relpath, content, datetime.now(timezone.utc)))
    else:
        cur.execute("""
            UPDATE pdf_stats SET content=%s, last_modified=%s WHERE relpath=%s
        """, (content, datetime.now(timezone.utc), relpath))
    conn.commit()
    cur.close()
    conn.close()

def record_action(relpath, user, action):
    ensure_meta(relpath)
    conn = get_conn()
    cur = conn.cursor()
    now = datetime.now(timezone.utc)
    cur.execute("""
        UPDATE pdf_stats
        SET history = history || jsonb_build_object('user', %s, 'action', %s, 'time', %s)
        WHERE relpath = %s
    """, (user, action, now.isoformat(), relpath))
    if action=="view":
        cur.execute("UPDATE pdf_stats SET views=views+1, status='lu', last_viewed=%s WHERE relpath=%s", (now, relpath))
    elif action=="download":
        cur.execute("UPDATE pdf_stats SET downloads=downloads+1 WHERE relpath=%s", (relpath,))
    elif action=="mark_lu":
        cur.execute("UPDATE pdf_stats SET status='lu' WHERE relpath=%s", (relpath,))
    elif action=="mark_non_lu":
        cur.execute("UPDATE pdf_stats SET status='non_lu' WHERE relpath=%s", (relpath,))
    conn.commit()
    cur.close()
    conn.close()

def get_meta(relpath):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT views, downloads, status, last_viewed FROM pdf_stats WHERE relpath=%s", (relpath,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return {"views": row[0], "downloads": row[1], "status": row[2], "last_viewed": row[3]}
    return {"views":0,"downloads":0,"status":"non_lu","last_viewed":None}

def list_reports_by_type(report_type):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT relpath, name FROM pdf_stats WHERE relpath LIKE %s", (f"{report_type}/%",))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{"relpath": r[0], "name": r[1]} for r in rows]

def get_content(relpath):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT content FROM pdf_stats WHERE relpath=%s", (relpath,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None

# --- Synchronisation Drive → PostgreSQL ---
def _sync_folder(folder_id, parent_path=""):
    query = f"'{folder_id}' in parents and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id,name,mimeType,modifiedTime)").execute()
    items = results.get("files", [])
    for f in items:
        relpath = f"{parent_path}/{f['name']}" if parent_path else f["name"]
        if f["mimeType"]=="application/vnd.google-apps.folder":
            _sync_folder(f["id"], relpath)
        else:
            data = drive_service.files().get_media(fileId=f["id"]).execute()
            ensure_meta(relpath, name=f["name"], content=data)

def sync_drive_to_postgres():
    while True:
        try:
            _sync_folder(DRIVE_ROOT_ID)
        except Exception as e:
            print("Erreur sync:", e)
        time.sleep(60)  # toutes les 60 sec

# --- Flask ---
app = Flask(__name__)
app.config["SECRET_KEY"] = SECRET_KEY
OFFICIAL_TYPES = ["ETAT_VISITE","RAPPORT_CL","RAPPORT_ETAT_LIEU","RAPPORT_ML","RECLAMATION","RECOMMANDATION"]
app.jinja_env.globals.update(
    list_reports_by_type=list_reports_by_type,
    get_meta=get_meta
)

# --- Routes (idem code précédent) ---
# ... (toutes tes routes restent identiques) ...

# --- Lancer la sync au démarrage ---
threading.Thread(target=sync_drive_to_postgres, daemon=True).start()

if __name__=="__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
