import os
import io
import threading
import time
import qrcode
from pathlib import Path
from datetime import datetime, timezone
from flask import (
    Flask, render_template, request, redirect, url_for, session,
    send_file, abort, jsonify, flash, Response
)
from googleapiclient.http import MediaIoBaseDownload
from zipfile import ZipFile, ZIP_DEFLATED
from services.drive import sync_folder, DRIVE_ROOT_ID, supabase, drive_service
import pikepdf
from urllib.parse import unquote
import re
from dotenv import load_dotenv

# --- CHARGEMENT DES VARIABLES D'ENV ---
load_dotenv()

# --- Répertoires et configuration ---
BASE_DIR = Path(__file__).parent
CACHE_DIR = BASE_DIR / "pdf_cache"
CACHE_DIR.mkdir(exist_ok=True)

# --- Variables Flask & Login ---
SECRET_KEY = os.getenv("FLASK_SECRET")
USER_CGEDS = os.getenv("FLASK_USER_CGEDS")
PASS_CGEDS = os.getenv("FLASK_PASS_CGEDS")

# --- Vérification ---
if not SECRET_KEY or not USER_CGEDS or not PASS_CGEDS:
    raise RuntimeError("❌ Variables FLASK_SECRET, FLASK_USER_CGEDS ou FLASK_PASS_CGEDS manquantes dans .env")

# --- Flask App ---
app = Flask(__name__)
app.secret_key = SECRET_KEY  # 🔐 clé secrète pour les sessions

# --- Dictionnaire des utilisateurs autorisés ---
USERS = {USER_CGEDS: PASS_CGEDS}

# --- Concurrence & cache ---
downloads_lock = threading.Lock()
downloads_in_progress = set()

# --- Flask App ---
app = Flask(__name__)
app.secret_key = SECRET_KEY  # 🔐 clé secrète pour les sessions

# --- Dictionnaire des utilisateurs autorisés ---
USERS = {USER_CGEDS: PASS_CGEDS}

# --- Concurrence & cache ---
downloads_lock = threading.Lock()
downloads_in_progress = set()


OFFICIAL_TYPES = [
    "ETAT_VISITE","RAPPORT_CL","RAPPORT_ETAT_LIEU",
    "RAPPORT_ML","RECLAMATION","RECOMMANDATION"
]
# --- Utils ---
def normalize_filename(filename):
    """Supprime un doublon '.pdf.pdf' à la fin du nom de fichier"""
    if filename.lower().endswith(".pdf.pdf"):
        filename = filename[:-4]
    return filename

def ensure_meta(relpath, name=None, drive_id=None):
    """Assure que l'entrée du PDF existe dans Supabase"""
    r = supabase.table("pdf_stats").select("*").eq("relpath", relpath).execute()
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "relpath": relpath,
        "name": name or relpath,
        "drive_id": drive_id,
        "views": 0,
        "downloads": 0,
        "status": "non_lu",
        "history": [],
        "last_modified": now
    }
    if r.data:
        update_data = {"name": payload["name"], "last_modified": now}
        if drive_id:
            update_data["drive_id"] = drive_id
        supabase.table("pdf_stats").update(update_data).eq("relpath", relpath).execute()
    else:
        supabase.table("pdf_stats").insert(payload).execute()

def record_action(relpath, user, action):
    """Enregistre une action sur le PDF (view, download, mark_lu/non_lu)"""
    ensure_meta(relpath)
    r = supabase.table("pdf_stats").select("*").eq("relpath", relpath).execute()
    if not r.data: return
    row = r.data[0]
    history = row.get("history") or []
    history.append({"user": user, "action": action, "time": datetime.now(timezone.utc).isoformat()})
    data = {"history": history}
    if action == "view":
        data.update({"views": row.get("views",0)+1, "status":"lu", "last_viewed": datetime.now(timezone.utc).isoformat()})
    elif action == "download":
        data.update({"downloads": row.get("downloads",0)+1})
    elif action == "mark_lu":
        data.update({"status":"lu"})
    elif action == "mark_non_lu":
        data.update({"status":"non_lu"})
    supabase.table("pdf_stats").update(data).eq("relpath", relpath).execute()

def get_meta(relpath):
    """Récupère les stats du PDF"""
    r = supabase.table("pdf_stats").select("*").eq("relpath", relpath).execute()
    if not r.data: return {"views":0,"downloads":0,"status":"non_lu","last_viewed":None}
    row = r.data[0]
    return {"views": row.get("views",0), "downloads": row.get("downloads",0), "status": row.get("status","non_lu"), "last_viewed": row.get("last_viewed")}

def list_reports_by_type(report_type, limit=50, offset=0):
    r = supabase.table("pdf_stats").select("*").like("relpath", f"{report_type}/%").range(offset, offset+limit-1).execute()
    return r.data or []

def compress_pdf(content: bytes) -> bytes:
    """Compresse un PDF via pikepdf"""
    try:
        pdf = pikepdf.open(io.BytesIO(content))
        io_bytes = io.BytesIO()
        pdf.save(io_bytes, optimize_streams=True, compression=pikepdf.CompressionLevel.default)
        pdf.close()
        io_bytes.seek(0)
        return io_bytes.read()
    except Exception as e:
        print(f"[ERREUR] Compression PDF: {e}")
        return content

def get_pdf(relpath, chunk_size=100*1024*1024):
    """Récupère le PDF depuis cache ou Drive avec gestion concurrence"""
    relpath = normalize_filename(relpath)
    cache_path = CACHE_DIR / relpath
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if cache_path.exists() and cache_path.stat().st_size > 0:
        return cache_path.read_bytes()

    with downloads_lock:
        if relpath in downloads_in_progress:
            while relpath in downloads_in_progress:
                time.sleep(0.5)
            if cache_path.exists() and cache_path.stat().st_size > 0:
                return cache_path.read_bytes()
            else:
                return None
        downloads_in_progress.add(relpath)

    try:
        r = supabase.table("pdf_stats").select("drive_id").eq("relpath", relpath).execute()
        if not r.data or not r.data[0].get("drive_id"): return None
        drive_id = r.data[0]["drive_id"]

        request_drive = drive_service.files().get_media(fileId=drive_id)
        with open(cache_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request_drive, chunksize=chunk_size)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        if cache_path.exists() and cache_path.stat().st_size > 0:
            return cache_path.read_bytes()
        return None
    finally:
        with downloads_lock:
            downloads_in_progress.discard(relpath)

# --- Flask Filters ---
def human_size(bytes, precision=2):
    if bytes is None: return "0 B"
    units = ["B","KB","MB","GB","TB"]
    size=float(bytes)
    for unit in units:
        if size < 1024: return f"{size:.{precision}f} {unit}"
        size/=1024
    return f"{size:.{precision}f} PB"
app.jinja_env.filters['human_size'] = human_size
app.jinja_env.globals.update(list_reports_by_type=list_reports_by_type, get_meta=get_meta)

# --- Routes ---
@app.route("/")
def home():
    if not session.get("user"): return redirect(url_for("login"))
    return render_template("dashboard.html", official=OFFICIAL_TYPES)

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method=="POST":
        u=request.form.get("username","").strip()
        p=request.form.get("password","").strip()
        if u in USERS and USERS[u]==p:
            session["user"]=u
            return redirect(url_for("home"))
        flash("Identifiants incorrects", "danger")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/type/<report_type>")
def report_type(report_type):
    if not session.get("user"): return redirect(url_for("login"))
    q = request.args.get("q","").strip().lower()
    status_filter = request.args.get("status","all")
    page = max(1,int(request.args.get("page",1)))
    per_page = max(5,int(request.args.get("per_page",20)))
    offset=(page-1)*per_page
    reports=list_reports_by_type(report_type,limit=per_page,offset=offset)
    if q:
        reports=[r for r in reports if q in r["name"].lower() or q in r["relpath"].lower()]
    if status_filter in ("lu","non_lu"):
        reports=[r for r in reports if r.get("status","non_lu")==status_filter]
    total=len(reports)
    title_map={
        "RAPPORT_CL":"CERTIFICAT DE LOCALISATION",
        "RAPPORT_ML":"MORCELLEMENT",
        "RECOMMANDATION":"RECOMMANDATION",
        "RECLAMATION":"RECLAMATION",
        "RAPPORT_ETAT_LIEU":"ETAT DE LIEU",
        "ETAT_VISITE":"ETAT DE VISITE"
    }
    page_title=title_map.get(report_type,report_type)
    return render_template("rapport_type.html",report_type=report_type,reports=reports,page=page,per_page=per_page,total=total,q=q,status_filter=status_filter,page_title=page_title)

@app.route("/report/<path:relpath>")
def report_page(relpath):
    if not session.get("user"):
        return redirect(url_for("login"))
    relpath = normalize_filename(relpath)
    record_action(relpath, session.get("user"), "view")
    # Redirection directe vers la lecture en streaming
    return redirect(url_for("stream_pdf", relpath=relpath))


@app.route("/view/<path:relpath>")
def view_report(relpath):
    """Redirige automatiquement vers la version streaming pour éviter les erreurs PDF lourds"""
    if not session.get("user"):
        return redirect(url_for("login"))
    # Redirection permanente vers la route de streaming
    return redirect(url_for("stream_pdf", relpath=relpath))

@app.route("/download/<path:relpath>")
def download_report(relpath):
    content = get_pdf(relpath)
    if not content: abort(404)
    record_action(relpath, session.get("user"), "download")
    return send_file(io.BytesIO(content), mimetype="application/pdf", download_name=os.path.basename(relpath), as_attachment=True)

@app.route("/download_all/<report_type>")
def download_all(report_type):
    reports = list_reports_by_type(report_type, limit=1000)
    buf = io.BytesIO()
    with ZipFile(buf, "w", compression=ZIP_DEFLATED) as z:
        for r in reports:
            content = get_pdf(r["relpath"])
            if content:
                z.writestr(r["relpath"], content)
                record_action(r["relpath"], session.get("user"), "download")
    buf.seek(0)
    return send_file(buf, mimetype="application/zip", download_name=f"{report_type}.zip", as_attachment=True)

@app.route("/generate_qr/<path:relpath>")
def generate_qr(relpath):
    if not session.get("user"): return redirect(url_for("login"))
    url=url_for("report_page",relpath=relpath,_external=True)
    img=qrcode.make(url)
    buf=io.BytesIO()
    img.save(buf,format="PNG")
    buf.seek(0)
    return send_file(buf,mimetype="image/png",download_name=f"{os.path.basename(relpath)}_qr.png")

@app.route("/search")
def search():
    if not session.get("user"): return redirect(url_for("login"))
    q=request.args.get("q","").strip().lower()
    hits=[]
    r=supabase.table("pdf_stats").select("*").ilike("name",f"%{q}%").execute()
    hits=r.data or []
    return render_template("search.html",query=q,hits=hits)

@app.route("/report-categories")
def report_categories():
    if not session.get("user"): return redirect(url_for("login"))
    return render_template("report_categories.html", page_title="Types de rapports", official_types=OFFICIAL_TYPES)

@app.route("/api/mark_status",methods=["POST"])
def api_mark_status():
    if not session.get("user"): return jsonify({"error":"non_auth"}),401
    data=request.json or {}
    rel=data.get("relpath")
    status=data.get("status")
    if not rel or status not in ("lu","non_lu"): return jsonify({"error":"bad_request"}),400
    record_action(rel,session.get("user"),"mark_lu" if status=="lu" else "mark_non_lu")
    return jsonify({"ok":True})

@app.route("/api/stats")
def api_stats():
    r = supabase.table("pdf_stats").select("type, status, views, downloads").execute()
    rows = r.data or []

    stats = {t: {"total": 0, "views": 0, "downloads": 0, "lus": 0, "non_lus": 0}
             for t in OFFICIAL_TYPES}

    for row in rows:
        t = row.get("type", "AUTRE")
        if t not in stats: continue
        stats[t]["total"] += 1
        stats[t]["views"] += row.get("views", 0)
        stats[t]["downloads"] += row.get("downloads", 0)
        if row.get("status") == "lu":
            stats[t]["lus"] += 1
        else:
            stats[t]["non_lus"] += 1

    labels = list(stats.keys())
    totals = [v["total"] for v in stats.values()]
    views = [v["views"] for v in stats.values()]
    downloads = [v["downloads"] for v in stats.values()]
    lus = [v["lus"] for v in stats.values()]
    non_lus = [v["non_lus"] for v in stats.values()]

    return jsonify({
        "labels": labels,
        "totals": totals,
        "views": views,
        "downloads": downloads,
        "lus": lus,
        "non_lus": non_lus
    })

@app.route("/qr/<path:relpath>")
def qr_page(relpath):
    if not session.get("user"): return redirect(url_for("login"))
    return render_template("qr_page.html",relpath=relpath)

@app.route("/pdf/<path:relpath>")
def serve_pdf(relpath):
    if not session.get("user"): return redirect(url_for("login"))
    content=get_pdf(relpath)
    if not content: abort(404)
    return send_file(io.BytesIO(content), mimetype="application/pdf", download_name=os.path.basename(relpath))

@app.route("/drive_webhook",methods=["POST"])
def drive_webhook():
    state = request.headers.get("X-Goog-Resource-State")
    if state in ("exists","updated"):
        threading.Thread(target=sync_folder, args=(DRIVE_ROOT_ID,), daemon=True).start()
    return "", 200

@app.route("/stream/<path:relpath>")
def stream_pdf(relpath):
    if not session.get("user"):
        return redirect(url_for("login"))

    relpath = normalize_filename(unquote(relpath))
    cache_path = CACHE_DIR / relpath
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    # Téléchargement si pas déjà en cache
    if not cache_path.exists() or cache_path.stat().st_size == 0:
        content = get_pdf(relpath)
        if not content:
            abort(404)
        cache_path.write_bytes(content)

    record_action(relpath, session.get("user"), "view")
    file_size = cache_path.stat().st_size

    range_header = request.headers.get('Range', None)
    if range_header:
        match = re.search(r'bytes=(\d+)-(\d*)', range_header)
        if match:
            start, end = match.groups()
            start = int(start)
            end = int(end) if end else file_size - 1
        else:
            start, end = 0, file_size - 1
        length = end - start + 1

        with open(cache_path, 'rb') as f:
            f.seek(start)
            data = f.read(length)

        rv = Response(data, 206, mimetype='application/pdf')
        rv.headers.add('Content-Range', f'bytes {start}-{end}/{file_size}')
        rv.headers.add('Accept-Ranges', 'bytes')
        rv.headers.add('Content-Length', str(length))
        return rv

    # --- Lecture en flux direct (sans tout lire en mémoire)
    def generate():
        with open(cache_path, 'rb') as f:
            while chunk := f.read(8192):
                yield chunk

    return Response(generate(), mimetype='application/pdf')

# --- Startup Threads ---
def preload_cache():
    print("[INFO] Préchargement du cache…")
    results = drive_service.files().list(q=f"'{DRIVE_ROOT_ID}' in parents and trashed=false",
                                         fields="files(id,name,mimeType)").execute()
    for f in results.get("files", []):
        if f["mimeType"] == "application/vnd.google-apps.folder":
            continue
        relpath = f["name"]
        ensure_meta(relpath, name=f["name"], drive_id=f["id"])
        cache_path = CACHE_DIR / relpath
        if not cache_path.exists() or cache_path.stat().st_size == 0:
            threading.Thread(target=get_pdf, args=(relpath,), daemon=True).start()

if __name__ == "__main__":
    threading.Thread(target=preload_cache, daemon=True).start()
    app.run(host="0.0.0.0", port=5000, debug=True)
