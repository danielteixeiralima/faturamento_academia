# filename: db.py
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.engine.url import make_url, URL
from sqlalchemy.orm import sessionmaker, scoped_session
from werkzeug.security import generate_password_hash
from dotenv import load_dotenv

from models import User

# .env sempre em UTF-8
load_dotenv(encoding="utf-8")

# Blindagem: evita arquivos/variáveis externas do libpq com encoding estranho
def _sanitize_pg_env():
    # Mantém apenas PGCLIENTENCODING; remove o resto (PGUSER, PGPASSWORD, PGSERVICE, etc.)
    for k in list(os.environ.keys()):
        if k.startswith("PG") and k not in {"PGCLIENTENCODING"}:
            del os.environ[k]
    os.environ["PGCLIENTENCODING"] = "utf8"
    # Desabilita pgpass e pg_service
    os.environ["PGPASSFILE"] = "NUL" if os.name == "nt" else "/dev/null"
    os.environ["PGSERVICEFILE"] = "NUL" if os.name == "nt" else "/dev/null"
    # Evita procurar pg_service.conf em diretórios do sistema
    os.environ["PGSYSCONFDIR"] = os.getcwd()

_sanitize_pg_env()

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

def _pg_connect_args_from_url(target_url: str):
    url = make_url(target_url)
    return {
        "user": url.username or "postgres",
        "password": url.password or "postgres",
        "host": url.host or "localhost",
        "port": int(url.port or 5432),
        "dbname": url.database or "postgres",
        "client_encoding": "utf8",
        "options": "-c client_encoding=UTF8",
    }

def _make_pg_engine_psycopg2(target_url: str):
    connect_args = _pg_connect_args_from_url(target_url)
    eng = create_engine(
        "postgresql+psycopg2://",
        connect_args=connect_args,
        pool_pre_ping=True,
        future=True,
    )
    return eng

def _make_pg_engine_pg8000(target_url: str, database_override: str | None = None):
    url = make_url(target_url)
    db_url = URL.create(
        "postgresql+pg8000",
        username=url.username or "postgres",
        password=url.password or "postgres",
        host=url.host or "localhost",
        port=int(url.port or 5432),
        database=(database_override or url.database or "postgres"),
    )
    eng = create_engine(db_url, pool_pre_ping=True, future=True)
    return eng

def _ensure_postgres_database(target_url: str):
    """
    Tenta conectar no banco alvo.
    Se não existir (3D000), conecta no DB 'postgres' e cria o alvo.
    Se houver erro de encoding com psycopg2, faz fallback para pg8000.
    """
    last_op_err = None
    had_unicode_err = False

    # 1) Tenta com psycopg2 (evitando DSN)
    try:
        eng = _make_pg_engine_psycopg2(target_url)
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return eng
    except UnicodeDecodeError:
        # Falha típica do libpq no Windows com arquivos/variáveis ANSI
        had_unicode_err = True
    except OperationalError as e:
        # Pode ser 3D000 (db inexistente) ou outro; tratamos abaixo
        last_op_err = e
    except ProgrammingError:
        # Repropaga erros de programação (ex.: sintaxe/perm)
        raise

    # 2) Se falhou por db inexistente com psycopg2, tenta criar usando pg8000
    if isinstance(last_op_err, OperationalError):
        pgcode = getattr(getattr(last_op_err, "orig", None), "pgcode", None)
        if pgcode == "3D000":
            url = make_url(target_url)
            dbname = url.database
            admin_engine = _make_pg_engine_pg8000(target_url, database_override="postgres")
            with admin_engine.connect() as conn:
                conn.execution_options(isolation_level="AUTOCOMMIT")
                conn.execute(text(f'CREATE DATABASE "{dbname}"'))
            admin_engine.dispose()
            last_op_err = None  # reset após criar o DB

    # 3) Tenta conectar ao banco alvo com pg8000 (contorna libpq/psycopg2)
    #    (também cobre o caso had_unicode_err=True)
    eng = _make_pg_engine_pg8000(target_url)
    with eng.connect() as conn:
        conn.execute(text("SELECT 1"))
    return eng


DATABASE_URL = (os.getenv('DATABASE_URL') or '').strip()

if not DATABASE_URL:
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
    except (OperationalError, UnicodeDecodeError):
        # Fallback para SQLite se Postgres indisponível ou ainda houver problema de encoding
        DATABASE_URL = _sqlite_url()
        engine = _make_engine(DATABASE_URL)

SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True))

def init_db_and_seed_admin():
    from models import Base as ModelsBase  # noqa
    ModelsBase.metadata.create_all(engine)
    with SessionLocal() as db:
        admin = db.query(User).filter_by(username='admin').first()
        if not admin:
            admin_user = User(username='admin', password_hash=generate_password_hash('admin123'))
            db.add(admin_user)
            db.commit()
