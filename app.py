# filename: app.py
# Aplicação Flask principal com .env (comentários usam alguns dígitos árabe-índicos).
import os
import zipfile
import threading
from datetime import datetime
import time                     # <—— novo

from functools import wraps

from flask import Flask, request, redirect, url_for, render_template, session, send_from_directory, flash, jsonify
import json
from werkzeug.security import check_password_hash
from werkzeug.utils import secure_filename
from dotenv import load_dotenv

from db import SessionLocal, init_db_and_seed_admin, get_paths
from models import User, UploadLog
from rpa import run_rpa_enter_google_folder, _ensure_local_zip_from_drive, _ensure_local_zip_from_drive

from flask import Blueprint, request, jsonify, send_from_directory
import os, time, uuid
import platform




load_dotenv()
system_name = platform.system().lower()
if 'win' in system_name:  # pega Windows inclusive variantes
    UPLOAD_DIR = os.getenv("CNAB_LOCAL_DIR_WINDOWS", r"C:\AUTOMACAO\conciliacao\arquivos")
else:
    UPLOAD_DIR = os.getenv("CNAB_LOCAL_DIR", "/home/felipe/Downloads/arquivos")


# Carrega variáveis de ambiente do .env (executa ١ vez no start).
# Carrega variáveis do .env


BASE_DIR, UPLOAD_DIR_IGNORED, EXTRACT_DIR = get_paths()  # mantém compatibilidade

# === Define diretório local automaticamente por SO ===
import platform
if platform.system() == "Windows":
    UPLOAD_DIR = os.getenv("CNAB_LOCAL_DIR_WINDOWS", r"C:\AUTOMACAO\conciliacao\arquivos")
else:
    UPLOAD_DIR = os.getenv("CNAB_LOCAL_DIR", "/home/felipe/Downloads/arquivos")

os.makedirs(UPLOAD_DIR, exist_ok=True)

# Inicializa app Flask
app = Flask(__name__, template_folder='templates', static_folder=None)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'chave_secreta_para_sessao')

# Inicializa DB e cria usuário admin caso não exista (executa ١ vez no start).
init_db_and_seed_admin()

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
    # chamado pelo agente windows
    if JOB_STATE["pending"]:
        return jsonify({"do": True, "job_id": JOB_STATE["job_id"]})
    return jsonify({"do": False})


# NOVA ROTA: upload automático sem input manual
@app.route("/upload_zip_automatico", methods=["POST"])
def upload_zip_automatico():
    log_dir = "/tmp"  # ou outro lugar para logs
    local_zip = _ensure_local_zip_from_drive(log_dir, filename="arquivos.zip")
    if not local_zip:
        return jsonify({"ok": False, "error": "Falha ao baixar do Drive"})
    return jsonify({"ok": True, "path": local_zip})



@app.get("/api/arquivo-atual")
def arquivo_atual():
    """Retorna info sobre o arquivo atual no destino."""
    destino = os.path.join(UPLOAD_DIR, "arquivos.zip")
    if os.path.isfile(destino):
        import time
        mtime = int(os.path.getmtime(destino))
        return jsonify({"ok": True, "path": destino, "mtime": mtime})
    else:
        return jsonify({"ok": False, "error": "Nenhum arquivo encontrado."})



# --- NOVA ROTA: upload manual do ZIP (salva como /home/felipe/Downloads/arquivos/arquivos.zip) ---
@app.post("/api/upload-zip-manual")

def upload_zip_manual():
    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "Nenhum arquivo recebido."}), 400

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    save_as = os.path.join(UPLOAD_DIR, "arquivos.zip")

    # validação simples de extensão
    name = (f.filename or "").lower()
    if not (name.endswith(".zip")):
        return jsonify({"ok": False, "error": "Envie um .zip válido."}), 400

    try:
        f.save(save_as)
        return jsonify({"ok": True, "saved": save_as})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@bp.post("/api/upload-zip")
def upload_zip():
    # upload do Windows
    f = request.files.get("file")
    job_id = request.form.get("job_id") or "unknown"
    if not f:
        return jsonify({"ok": False, "err": "no file"}), 400
    save_as = os.path.join(UPLOAD_DIR, "arquivos.zip")
    f.save(save_as)
    # marca job como concluído
    JOB_STATE["pending"] = False
    return jsonify({"ok": True, "saved": save_as, "job_id": job_id})
def is_logged_in():
    return session.get('user') is not None  # retorna True/False (١/٠)

def login_required(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not is_logged_in():
            return redirect(url_for('login'))
        return func(*args, **kwargs)
    return wrapper

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        uname = request.form.get('username', '').strip()
        pwd = request.form.get('password', '')
        with SessionLocal() as db:
            user = db.query(User).filter_by(username=uname).first()
            if user and check_password_hash(user.password_hash, pwd):
                session['user'] = user.username
                return redirect(url_for('dashboard'))
        flash('Credenciais inválidas.')
        return redirect(url_for('login'))
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

def _last_upload_record(db, by_user):
    return db.query(UploadLog).filter_by(uploaded_by=by_user).order_by(UploadLog.uploaded_at.desc()).first()

@app.route('/')
@login_required
def dashboard():
    with SessionLocal() as db:
        last_u = _last_upload_record(db, session['user'])
        last_time = last_u.uploaded_at.strftime('%d/%m/%Y %H:%M:%S') if last_u else ''
    # NÃO enviamos mais relatório antigo pro dashboard
    return render_template('dashboard.html', last_upload=last_u, last_upload_time=last_time)


# Mantém /start só se você quiser usar manualmente. O front vai usar /start_async.
@app.route('/start', methods=['POST'])
@login_required
def start_rpa():
    # Versão antiga fazia redirect imediato -> isso some o overlay.
    # Se preferir, pode até remover esta rota. Vou deixá-la criando a thread e
    # redirecionando ao /report (não será usada pelo front).
    extract_dir = session.get('last_extract_dir')
    if not extract_dir or not os.path.isdir(extract_dir):
        extract_dir = os.path.join(EXTRACT_DIR, 'temporario')
        os.makedirs(extract_dir, exist_ok=True)
    target_folder = os.path.join(extract_dir, 'google.com')
    os.makedirs(target_folder, exist_ok=True)
    t = threading.Thread(target=run_rpa_enter_google_folder,
                         args=(extract_dir, target_folder, BASE_DIR),
                         daemon=True)
    t.start()
    return redirect(url_for('report'))
@app.route('/report')
@login_required
def report():
    # Só HTML; JS faz polling em /api/report
    return render_template('report.html')

# NOVO: start assíncrono para o front
# --- AJUSTE NO start_async: checar se o ZIP já está na VM antes de iniciar ---
@app.post('/start_async')
@login_required
def start_async():
    # garante que o arquivo está lá
    zip_path = os.path.join(UPLOAD_DIR, "arquivos.zip")
    if not os.path.isfile(zip_path):
        return jsonify({"ok": False, "error": "arquivos.zip não foi enviado ainda."}), 400

    extract_dir = session.get('last_extract_dir')
    if not extract_dir or not os.path.isdir(extract_dir):
        extract_dir = os.path.join(EXTRACT_DIR, 'temporario')
        os.makedirs(extract_dir, exist_ok=True)

    target_folder = os.path.join(extract_dir, 'google.com')
    os.makedirs(target_folder, exist_ok=True)

    t = threading.Thread(
        target=run_rpa_enter_google_folder,
        args=(extract_dir, target_folder, BASE_DIR),  # RPA lê o zip já salvo em /home/felipe/Downloads/arquivos
        daemon=True
    )
    t.start()

    return jsonify({"ok": True, "started_at": int(time.time())})

@app.get('/api/report')
@login_required
def api_report():
    report_path = os.path.join(BASE_DIR, 'last_report.json')

    if not os.path.isfile(report_path):
        return jsonify({"ready": False, "headers": [], "rows": [], "meta": {}, "updated_at": None, "mtime": 0})

    try:
        with open(report_path, 'r', encoding='utf-8') as f:
            data = json.load(f) or {}

        mtime = int(os.path.getmtime(report_path))

        # Retrocompatibilidade (arquivo antigo era uma LISTA)
        if isinstance(data, list):
            headers = list(data[0].keys()) if data else []
            data = {
                "ready": True,
                "updated_at": datetime.fromtimestamp(mtime).strftime('%d/%m/%Y %H:%M:%S'),
                "headers": headers,
                "rows": data,
                "meta": {}
            }

        data.setdefault("ready", True)
        data.setdefault("rows", [])
        data.setdefault("headers", (list(data["rows"][0].keys()) if data["rows"] else []))
        data.setdefault("meta", {})
        data.setdefault("updated_at", datetime.fromtimestamp(mtime).strftime('%d/%m/%Y %H:%M:%S'))
        data["mtime"] = mtime                                  # <—— importante pro polling

        return jsonify(data)

    except Exception as e:
        return jsonify({"ready": False, "headers": [], "rows": [], "meta": {}, "updated_at": None, "mtime": 0, "error": str(e)})


@app.route('/uploads/<path:filename>')
@login_required
def uploaded_file(filename):
    return send_from_directory(UPLOAD_DIR, filename, as_attachment=True)

# trecho final atualizado do app.py (auto-reload ao salvar arquivos)
if __name__ == '__main__':
    host = os.getenv('HOST', '0.0.0.0')
    port = int(os.getenv('PORT', '5000'))
    # força o reloader automático independentemente do .env
    app.run(host=host, port=port, debug=True, use_reloader=True)
