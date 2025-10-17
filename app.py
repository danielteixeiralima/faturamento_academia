# filename: app.py
# Aplicação Flask principal com .env.
import os
import shutil
import zipfile
import threading
from datetime import datetime
import time
from functools import wraps
import json
import uuid
import platform

from flask import (
    Flask,
    request,
    redirect,
    url_for,
    render_template,
    session,
    send_from_directory,
    flash,
    jsonify,
    Blueprint,
)
from werkzeug.security import check_password_hash
from dotenv import load_dotenv

from db import SessionLocal, init_db_and_seed_admin, get_paths
from models import User, UploadLog
from rpa import run_rpa_enter_google_folder, _ensure_local_zip_from_drive

# Carrega variáveis de ambiente do .env
load_dotenv()

# Define diretório de upload conforme SO
system_name = platform.system().lower()
if "win" in system_name:
    UPLOAD_DIR = os.getenv("CNAB_LOCAL_DIR_WINDOWS", r"C:\AUTOMACAO\conciliacao\arquivos")
else:
    UPLOAD_DIR = os.getenv("CNAB_LOCAL_DIR", "/home/felipe/Downloads/arquivos")

# Caminhos base (mantém compatibilidade)
BASE_DIR, UPLOAD_DIR_IGNORED, EXTRACT_DIR = get_paths()

# (Re)define diretório local automaticamente por SO (compatível com versões anteriores)
if platform.system() == "Windows":
    UPLOAD_DIR = os.getenv("CNAB_LOCAL_DIR_WINDOWS", r"C:\AUTOMACAO\conciliacao\arquivos")
else:
    UPLOAD_DIR = os.getenv("CNAB_LOCAL_DIR", "/home/felipe/Downloads/arquivos")

os.makedirs(UPLOAD_DIR, exist_ok=True)

# Inicializa app Flask
app = Flask(__name__, template_folder="templates", static_folder=None)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "chave_secreta_para_sessao")

# Inicializa DB e cria usuário admin caso não exista
init_db_and_seed_admin()

# ===== Jobs (Blueprint) =====
bp = Blueprint("jobs", __name__)
JOB_STATE = {"pending": False, "job_id": None, "created_at": None}


@bp.post("/api/iniciar-incorporadora")
def iniciar_incorporadora():
    JOB_STATE["pending"] = True
    JOB_STATE["job_id"] = str(uuid.uuid4())
    JOB_STATE["created_at"] = time.time()
    return jsonify({"ok": True, "job_id": JOB_STATE["job_id"]})


@bp.get("/api/pull-job")
def pull_job():
    if JOB_STATE["pending"]:
        return jsonify({"do": True, "job_id": JOB_STATE["job_id"]})
    return jsonify({"do": False})


@bp.post("/api/upload-zip")
def upload_zip():
    f = request.files.get("file")
    job_id = request.form.get("job_id") or "unknown"
    if not f:
        return jsonify({"ok": False, "err": "no file"}), 400
    save_as = os.path.join(UPLOAD_DIR, "arquivos.zip")
    f.save(save_as)
    JOB_STATE["pending"] = False
    return jsonify({"ok": True, "saved": save_as, "job_id": job_id})


# ===== Rotas auxiliares =====
@app.route("/upload_zip_automatico", methods=["POST"])
def upload_zip_automatico():
    """
    Garante que o arquivo final esteja em UPLOAD_DIR/arquivos.zip.
    Aceita retorno do stub como diretório ou arquivo. Procura candidatos em:
    - Caminho retornado por _ensure_local_zip_from_drive (arquivo .zip ou diretório)
    - UPLOAD_DIR
    - Caminho padrão do Windows C:\\AUTOMACAO\\conciliacao\\arquivos
    Seleciona o .zip mais recente e copia para UPLOAD_DIR/arquivos.zip.
    """
    log_dir = "/tmp"
    src = _ensure_local_zip_from_drive(log_dir)

    candidates = []

    def add_zip_candidates_from_dir(dpath: str) -> None:
        try:
            prefer = os.path.join(dpath, "arquivos.zip")
            if os.path.isfile(prefer):
                candidates.append(prefer)
            for name in os.listdir(dpath):
                full = os.path.join(dpath, name)
                if os.path.isfile(full) and name.lower().endswith(".zip"):
                    candidates.append(full)
        except Exception:
            pass

    if src:
        if os.path.isfile(src) and src.lower().endswith(".zip"):
            candidates.append(src)
        elif os.path.isdir(src):
            add_zip_candidates_from_dir(src)

    add_zip_candidates_from_dir(UPLOAD_DIR)

    if platform.system().lower().startswith("win"):
        add_zip_candidates_from_dir(r"C:\AUTOMACAO\conciliacao\arquivos")

    seen = set()
    unique_candidates = []
    for c in candidates:
        ap = os.path.abspath(c)
        if ap not in seen:
            seen.add(ap)
            unique_candidates.append(c)

    if not unique_candidates:
        return jsonify({"ok": False, "error": "Nenhum .zip encontrado nos diretórios verificados"}), 500

    try:
        src_zip = max(unique_candidates, key=os.path.getmtime)
    except Exception:
        src_zip = unique_candidates[0]

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    destino = os.path.join(UPLOAD_DIR, "arquivos.zip")
    try:
        if os.path.abspath(src_zip) != os.path.abspath(destino):
            shutil.copyfile(src_zip, destino)
        else:
            os.utime(destino, None)
    except Exception as e:
        return jsonify({"ok": False, "error": f"Falha ao salvar ZIP no destino: {e}"}), 500

    return jsonify({"ok": True, "path": destino})


@app.get("/api/arquivo-atual")
def arquivo_atual():
    destino = os.path.join(UPLOAD_DIR, "arquivos.zip")
    if os.path.isfile(destino):
        mtime = int(os.path.getmtime(destino))
        return jsonify({"ok": True, "path": destino, "mtime": mtime})
    else:
        return jsonify({"ok": False, "error": "Nenhum arquivo encontrado."})


@app.post("/api/upload-zip-manual")
def upload_zip_manual():
    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "Nenhum arquivo recebido."}), 400

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    save_as = os.path.join(UPLOAD_DIR, "arquivos.zip")

    name = (f.filename or "").lower()
    if not name.endswith(".zip"):
        return jsonify({"ok": False, "error": "Envie um .zip válido."}), 400

    try:
        f.save(save_as)
        return jsonify({"ok": True, "saved": save_as})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ===== Auth helpers =====
def is_logged_in():
    return session.get("user") is not None


def login_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not is_logged_in():
            return redirect(url_for("login"))
        return func(*args, **kwargs)
    return wrapper


# ===== Auth views =====
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        uname = request.form.get("username", "").strip()
        pwd = request.form.get("password", "")
        with SessionLocal() as db:
            user = db.query(User).filter_by(username=uname).first()
            if user and check_password_hash(user.password_hash, pwd):
                session["user"] = user.username
                return redirect(url_for("dashboard"))
        flash("Credenciais inválidas.")
        return redirect(url_for("login"))
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


def _last_upload_record(db, by_user):
    return (
        db.query(UploadLog)
        .filter_by(uploaded_by=by_user)
        .order_by(UploadLog.uploaded_at.desc())
        .first()
    )


# ===== Views =====
@app.route("/")
@login_required
def dashboard():
    with SessionLocal() as db:
        last_u = _last_upload_record(db, session["user"])
        last_time = last_u.uploaded_at.strftime("%d/%m/%Y %H:%M:%S") if last_u else ""
    return render_template("dashboard.html", last_upload=last_u, last_upload_time=last_time)


@app.route("/start", methods=["POST"])
@login_required
def start_rpa():
    extract_dir = session.get("last_extract_dir")
    if not extract_dir or not os.path.isdir(extract_dir):
        extract_dir = os.path.join(EXTRACT_DIR, "temporario")
        os.makedirs(extract_dir, exist_ok=True)
    target_folder = os.path.join(extract_dir, "google.com")
    os.makedirs(target_folder, exist_ok=True)
    t = threading.Thread(
        target=run_rpa_enter_google_folder,
        args=(extract_dir, target_folder, BASE_DIR),
        daemon=True,
    )
    t.start()
    return redirect(url_for("report"))


@app.route("/report")
@login_required
def report():
    return render_template("report.html")


@app.post("/start_async")
@login_required
def start_async():
    extract_dir = os.path.join(EXTRACT_DIR, "temporario")
    os.makedirs(extract_dir, exist_ok=True)

    target_folder = os.path.join(extract_dir, "google.com")
    os.makedirs(target_folder, exist_ok=True)

    t = threading.Thread(
        target=run_rpa_enter_google_folder,
        args=(extract_dir, target_folder, BASE_DIR),
        daemon=True,
    )
    t.start()

    return jsonify({"ok": True, "started_at": int(time.time())})


@app.get("/api/report")
@login_required
def api_report():
    report_path = os.path.join(BASE_DIR, "last_report.json")

    if not os.path.isfile(report_path):
        return jsonify(
            {"ready": False, "headers": [], "rows": [], "meta": {}, "updated_at": None, "mtime": 0}
        )

    try:
        with open(report_path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}

        mtime = int(os.path.getmtime(report_path))

        if isinstance(data, list):
            headers = list(data[0].keys()) if data else []
            data = {
                "ready": True,
                "updated_at": datetime.fromtimestamp(mtime).strftime("%d/%m/%Y %H:%M:%S"),
                "headers": headers,
                "rows": data,
                "meta": {},
            }

        data.setdefault("ready", True)
        data.setdefault("rows", [])
        data.setdefault("headers", (list(data["rows"][0].keys()) if data["rows"] else []))
        data.setdefault("meta", {})
        data.setdefault("updated_at", datetime.fromtimestamp(mtime).strftime("%d/%m/%Y %H:%M:%S"))
        data["mtime"] = mtime

        return jsonify(data)

    except Exception as e:
        return jsonify(
            {
                "ready": False,
                "headers": [],
                "rows": [],
                "meta": {},
                "updated_at": None,
                "mtime": 0,
                "error": str(e),
            }
        )


@app.route("/uploads/<path:filename>")
@login_required
def uploaded_file(filename):
    return send_from_directory(UPLOAD_DIR, filename, as_attachment=True)


# ===== Registra o Blueprint =====
app.register_blueprint(bp)

if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "5000"))
    app.run(host=host, port=port, debug=True, use_reloader=True)
