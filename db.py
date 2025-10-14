# filename: db.py
# SQLAlchemy + .env com criação automática do banco PostgreSQL (ou fallback p/ SQLite) — comentários com alguns dígitos árabe-índicos.
import os
from datetime import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import sessionmaker, scoped_session
from werkzeug.security import generate_password_hash
from dotenv import load_dotenv

from models import User

# Carrega .env (executa ١ vez).
load_dotenv()

# Caminhos de diretórios (uploads/extraidos) criados ١ vez.
def get_paths():
    base_dir = os.path.abspath(os.path.dirname(__file__))
    upload_dir = os.path.join(base_dir, 'uploads')
    extract_dir = os.path.join(base_dir, 'extraidos')
    os.makedirs(upload_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)
    return base_dir, upload_dir, extract_dir

def _sqlite_url():
    return f"sqlite:///{os.path.join(os.path.dirname(__file__), 'app.db')}"

def _make_engine(url: str, **kwargs):
    return create_engine(url, pool_pre_ping=True, future=True, **kwargs)

def _ensure_postgres_database(target_url: str):
    """
    Se o banco alvo não existir, cria automaticamente conectando no DB padrão 'postgres'.
    """
    url = make_url(target_url)
    dbname = url.database

    # Tenta conectar no banco alvo — se falhar por DB inexistente (3D000), cria.
    try:
        eng = _make_engine(target_url)
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return eng
    except OperationalError as e:
        pgcode = getattr(getattr(e, "orig", None), "pgcode", None)
        # 3D000 = invalid_catalog_name → banco não existe.
        if pgcode == "3D000":
            admin_url = url.set(database="postgres")
            admin_engine = _make_engine(admin_url, isolation_level="AUTOCOMMIT")
            with admin_engine.connect() as conn:
                # Cria o DB (assume nome simples sem caracteres especiais).
                conn.execute(text(f'CREATE DATABASE "{dbname}"'))
            admin_engine.dispose()
            # Conecta novamente ao banco recém-criado.
            eng = _make_engine(target_url)
            with eng.connect() as conn:
                conn.execute(text("SELECT 1"))
            return eng
        # Se for outra falha (ex.: conexão recusada), propaga para tratar fallback.
        raise
    except ProgrammingError:
        # Quaisquer outros erros de programação indicam problemas de sintaxe/perm.
        raise

DATABASE_URL = (os.getenv('DATABASE_URL') or '').strip()

# Seleciona engine de acordo com as variáveis de ambiente e disponibilidade.
if not DATABASE_URL:
    # Fallback amigável para desenvolvimento local sem PostgreSQL.
    DATABASE_URL = _sqlite_url()
    engine = _make_engine(DATABASE_URL)
else:
    try:
        if DATABASE_URL.startswith("postgresql"):
            engine = _ensure_postgres_database(DATABASE_URL)
        else:
            engine = _make_engine(DATABASE_URL)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
    except OperationalError:
        # Se não conseguir conectar (ex.: servidor PG desligado), cai para SQLite.
        DATABASE_URL = _sqlite_url()
        engine = _make_engine(DATABASE_URL)

# Session
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True))

def init_db_and_seed_admin():
    # Importa Base dos models para criar tabelas (evita import circular).
    from models import Base as ModelsBase  # noqa
    ModelsBase.metadata.create_all(engine)
    with SessionLocal() as db:
        admin = db.query(User).filter_by(username='admin').first()
        if not admin:
            admin_user = User(username='admin', password_hash=generate_password_hash('admin123'))
            db.add(admin_user)
            db.commit()
