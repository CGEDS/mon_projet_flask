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

# --- PostgreSQL Render ---
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

    q = request.args.get("q", "").strip().lower()
    status_filter = request.args.get("status", "all")
    page = max(1, int(request.args.get("page", 1)))
    per_page = max(5, int(request.args.get("per_page", 20)))

    reports = list_reports_by_type(report_type)
    if q:
        reports = [r for r in reports if q in r["name"].lower() or q in r["relpath"].lower()]
    if status_filter in ("lu", "non_lu"):
        reports = [r for r in reports if get_meta(r["relpath"])["status"] == status_filter]

    total = len(reports)
    start = (page - 1) * per_page
    paged = reports[start:start + per_page]

    for r in paged:
        meta = get_meta(r["relpath"])
        r.update(meta)
        content = get_content(r["relpath"]) or b""
        r["size"] = len(content)

    title_map = {
        "RAPPORT_CL":"CERTIFICAT DE LOCALISATION",
        "RAPPORT_ML":"MORCELLEMENT",
        "RECOMMANDATION":"RECOMMANDATION",
        "RECLAMATION":"RECLAMATION",
        "RAPPORT_ETAT_LIEU":"ETAT DE LIEU",
        "ETAT_VISITE":"ETAT DE VISITE"
    }
    page_title = title_map.get(report_type, report_type)

    return render_template("rapport_type.html",
        report_type=report_type, reports=paged, page=page,
        per_page=per_page, total=total, q=q,
        status_filter=status_filter, page_title=page_title
    )

@app.route("/report/<path:relpath>")
def view_report(relpath):
    if not session.get("user"): return redirect(url_for("login"))
    content = get_content(relpath)
    if content is None: abort(404)
    record_action(relpath, session.get("user"), "view")
    return send_file(io.BytesIO(content), download_name=relpath.split("/")[-1])

@app.route("/report_page/<path:relpath>")
def report_page(relpath):
    if not session.get("user"):
        return redirect(url_for("login"))
    content = get_content(relpath)
    if content is None:
        abort(404)
    return render_template("report_page.html", relpath=relpath)

@app.route("/download/<path:relpath>")
def download_report(relpath):
    if not session.get("user"): return redirect(url_for("login"))
    content = get_content(relpath)
    if content is None: abort(404)
    record_action(relpath, session.get("user"), "download")
    return send_file(io.BytesIO(content), download_name=relpath.split("/")[-1], as_attachment=True)

@app.route("/download_all/<report_type>")
def download_all(report_type):
    if not session.get("user"): return redirect(url_for("login"))
    reports = list_reports_by_type(report_type)
    buf = io.BytesIO()
    with ZipFile(buf, "w", compression=ZIP_DEFLATED) as z:
        for r in reports:
            content = get_content(r["relpath"])
            if content: z.writestr(r["relpath"], content)
    buf.seek(0)
    return send_file(buf, mimetype="application/zip", download_name=f"{report_type}.zip", as_attachment=True)

@app.route("/qr_img/<path:relpath>")
def qr_img(relpath):
    if not session.get("user"): return redirect(url_for("login"))
    content = get_content(relpath)
    if content is None: abort(404)
    max_embed = 200*1024
    if len(content) <= max_embed:
        data_b64 = base64.b64encode(content).decode("utf-8")
        qr_content = f"data:application/pdf;base64,{data_b64}"
    else:
        qr_content = url_for("download_report", relpath=relpath, _external=True)
    img = qrcode.make(qr_content)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return send_file(buf, mimetype="image/png")

@app.route("/qr/<path:relpath>")
def qr_page(relpath):
    if not session.get("user"): return redirect(url_for("login"))
    return render_template("qr_page.html", relpath=relpath)

@app.route("/api/mark_status", methods=["POST"])
def api_mark_status():
    if not session.get("user"): return jsonify({"error":"non_auth"}),401
    data=request.json or {}
    rel = data.get("relpath")
    status = data.get("status")
    if not rel or status not in ("lu","non_lu"): return jsonify({"error":"bad_request"}),400
    record_action(rel, session.get("user"), "mark_lu" if status=="lu" else "mark_non_lu")
    return jsonify({"ok":True})

@app.route("/api/stats")
def api_stats():
    labels=[]; totals=[]; views=[]; downloads=[]; lus=[]; non_lus=[]
    for t in OFFICIAL_TYPES:
        reports=list_reports_by_type(t)
        labels.append(t)
        totals.append(len(reports))
        v=d=lu=nl=0
        for r in reports:
            m=get_meta(r["relpath"])
            v+=m.get("views",0)
            d+=m.get("downloads",0)
            if m.get("status","non_lu")=="lu": lu+=1
            else: nl+=1
        views.append(v); downloads.append(d); lus.append(lu); non_lus.append(nl)
    return jsonify({"labels":labels,"totals":totals,"views":views,"downloads":downloads,"lus":lus,"non_lus":non_lus})

@app.route("/report-categories")
def report_categories():
    if not session.get("user"):
        return redirect(url_for("login"))

    official_types = [
        "ETAT_VISITE",
        "RAPPORT_CL",
        "RAPPORT_ETAT_LIEU",
        "RAPPORT_ML",
        "RECLAMATION",
        "RECOMMANDATION"
    ]

    return render_template(
        "report_categories.html",
        page_title="Types de rapports",
        official_types=official_types
    )

@app.route("/search")
def search():
    if not session.get("user"): return redirect(url_for("login"))
    q = request.args.get("q","").strip().lower()
    hits=[]
    for t in OFFICIAL_TYPES:
        for r in list_reports_by_type(t):
            if q in r["name"].lower() or q in r["relpath"].lower():
                r.update(get_meta(r["relpath"]))
                hits.append(r)
    return render_template("search.html", query=q, hits=hits)

# --- Lancer la sync au démarrage ---
threading.Thread(target=sync_drive_to_postgres, daemon=True).start()

if __name__=="__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
