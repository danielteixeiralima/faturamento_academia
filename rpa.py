import os
import time
from datetime import datetime, date
import smtplib
from email.message import EmailMessage
import mimetypes

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
import platform
import subprocess  # ainda usamos para chamar smbclient quando necessário
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import base64
import re
from collections import defaultdict
from googleapiclient.http import MediaIoBaseDownload  # <-- novo
import io 


# =====================================================================
# CONFIGURAÇÕES GERAIS
# =====================================================================

SCOPES_DRIVE = ['https://www.googleapis.com/auth/drive.readonly']
creds_drive = Credentials.from_authorized_user_file('token.json', SCOPES_DRIVE)
drive_service = build('drive', 'v3', credentials=creds_drive)

# ID do arquivo no Drive (pode vir do .env para não ficar hardcoded)


# === Define diretório local automaticamente por SO ===
import platform, os
if platform.system() == "Windows":
    local_dir = os.getenv("CNAB_LOCAL_DIR_WINDOWS", r"C:\AUTOMACAO\conciliacao\arquivos")
else:
    local_dir = os.getenv("CNAB_LOCAL_DIR", "/home/felipe/Downloads/arquivos")

os.makedirs(local_dir, exist_ok=True)
local_zip = os.path.join(local_dir, "arquivos.zip")

AZ_URL = "https://srv1.aztronic.com.br/collect_sc/"

# Pasta onde o upload manual grava o zip (mantém configurável por env)
LINUX_DL = local_dir  # reaproveita a mesma variável

IS_WINDOWS = (platform.system() == "Windows")
USE_PYAUTO = False  # não usar mais PyAutoGUI em nenhum cenário

# Importa PyAutoGUI apenas no Windows; no Linux cria um placeholder
try:
    if USE_PYAUTO:
        import pyautogui
        pyautogui.FAILSAFE = False
    else:
        pyautogui = None
except Exception:
    pyautogui = None

# No Windows você quer tudo visível; no servidor você controla via env HEADLESS=0|1
HEADLESS = False if IS_WINDOWS else (os.getenv("HEADLESS", "0") == "1")

# =====================================================================
# CONFIGURAÇÃO DE EMAIL (LER DO .ENV)
# =====================================================================
EMAIL_USER = os.getenv("EMAIL_USER")  # ex: hub@inovailab.com
EMAIL_PASS = os.getenv("EMAIL_PASS")  # senha de app do Gmail
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")  # Gmail
SMTP_PORT = int(os.getenv("SMTP_PORT", "465"))  # 465 SSL ou 587 TLS

SCOPES = ['https://www.googleapis.com/auth/gmail.send']
# Carrega credenciais
creds = Credentials.from_authorized_user_file('token.json', SCOPES)

service = build('gmail', 'v1', credentials=creds)

def gerar_html_tabela(dados, meta):
    if not dados:
        return "<p>Nenhum dado encontrado</p>"
    
    # --- NOVO: remove a primeira linha se for Coluna_1, Coluna_2, etc ---
    if all(str(k).lower().startswith('coluna_') for k in dados[0].keys()):
        # significa que a primeira linha é apenas cabeçalho "coluna_X"
        dados = dados[1:]  # pula a primeira linha errada

    # headers reais
    headers = list(dados[0].keys())

    # estilo para o cabeçalho azul
    th_style = (
        "border:1px solid #ddd;"
        "padding:6px;"
        "background:#004080;"  # azul
        "color:white;"
        "font-weight:bold;"
        "font-size:12px;"
    )
    td_style = "border:1px solid #ddd;padding:6px;font-size:12px;"

    html = "<table style='border-collapse:collapse;width:100%;font-family:Arial,sans-serif;font-size:12px;'>"
    html += "<thead><tr>"
    for h in headers:
        html += f"<th style='{th_style}'>{h}</th>"
    html += "</tr></thead><tbody>"

    for r in dados:
        html += "<tr>"
        for h in headers:
            val = r.get(h, "")
            html += f"<td style='{td_style}'>{val}</td>"
        html += "</tr>"
    html += "</tbody></table>"

    if meta:
        html += f"<div style='margin-top:10px;font-weight:bold;'>TOTAL PAGO: {meta.get('total_pago','')}</div>"
        html += f"<div style='margin-top:2px;font-weight:bold;'>TOTAL BAIXADO: {meta.get('total_baixado','')}</div>"

    return html


# === DICIONÁRIO EXTRAÍDO DA PLANILHA ===
DADOS_INCORPORADORAS = {
    "CESMA": {
        "regional": "ES",
        "coligada": "32",
        "empreendimento": "SC2 SHOPPING MESTRE ALVARO LTDA.",
        "cnpj": "10.141.735/0001-99",
        "responsavel": "Igor Ribeiro Braga // Igor Leitão Cardeiro",
        "email": "igor.braga@sacavalcante.com.br; igor.cardeiro@sacavalcante.com.br"
    },
    "CESMO4": {
        "regional": "ES",
        "coligada": "30",
        "empreendimento": "SPE - CONSTRUTORA SA CAVALCANTE - ES XIII LTDA",
        "cnpj": "09.616.846/0001-58",
        "responsavel": "Igor Ribeiro Braga // Igor Leitão Cardeiro",
        "email": "igor.braga@sacavalcante.com.br; igor.cardeiro@sacavalcante.com.br"
    },
    "CEO": {
        "regional": "ES",
        "coligada": "23",
        "empreendimento": "SPE-CONSTRUTORA SA CAVALCANTE-ES XVI LTDA",
        "cnpj": "09.616.448/0001-09",
        "responsavel": "Igor Ribeiro Braga // Igor Leitão Cardeiro",
        "email": "igor.braga@sacavalcante.com.br; igor.cardeiro@sacavalcante.com.br"
    },
    "CESMO3": {
        "regional": "ES",
        "coligada": "66",
        "empreendimento": "SPE CONSTRUTORA SA CAVALCANTE LIII LTDA",
        "cnpj": "14.699.520/0001-49",
        "responsavel": "Igor Ribeiro Braga // Igor Leitão Cardeiro",
        "email": "igor.braga@sacavalcante.com.br; igor.cardeiro@sacavalcante.com.br"
    },
    "HILLSIDE": {
        "regional": "ES",
        "coligada": "58",
        "empreendimento": "PRAIA COMPRIDA INCORPORAÇÕES LTDA.",
        "cnpj": "13.181.315/0001-24",
        "responsavel": "Igor Ribeiro Braga // Igor Leitão Cardeiro",
        "email": "igor.braga@sacavalcante.com.br; igor.cardeiro@sacavalcante.com.br"
    },
    "RESERVA LAGOA RESIDENCIAL CLUBE": {
        "regional": "MA",
        "coligada": "34",
        "empreendimento": "SPE - SA CAVALCANTE INCORPORACOES IMOBILIARIAS MA X LTDA",
        "cnpj": "09.653.550/0001-84",
        "responsavel": "Vitória Castro // Neyrielle Coimbra",
        "email": "vitoria.castro@sacavalcante.com.br; ncoimbra@sacavalcante.com.br"
    },
    "ILHA PARQUE (FASE 1)": {
        "regional": "MA",
        "coligada": "36",
        "empreendimento": "SPE SA CAVALCANTE INCORPORACOES IMOBILIARIAS MA XII LTDA",
        "cnpj": "09.653.594/0001-04",
        "responsavel": "Vitória Castro // Neyrielle Coimbra",
        "email": "vitoria.castro@sacavalcante.com.br; ncoimbra@sacavalcante.com.br"
    },
    "LCO": {
        "regional": "MA",
        "coligada": "37",
        "empreendimento": "SPE SA CAVALCANTE INCORPORACOES IMOBILIARIAS MA XIII LTDA",
        "cnpj": "09.653.594/0001-04",
        "responsavel": "Vitória Castro // Neyrielle Coimbra",
        "email": "vitoria.castro@sacavalcante.com.br; ncoimbra@sacavalcante.com.br"
    },
    "AREINHA (SLO)": {
        "regional": "MA",
        "coligada": "48",
        "empreendimento": "SPE - AREINHA INCORPORAÇÕES IMOBILIÁRIAS LTDA",
        "cnpj": "14.877.206/0001-89",
        "responsavel": "Vitória Castro // Neyrielle Coimbra",
        "email": "vitoria.castro@sacavalcante.com.br; ncoimbra@sacavalcante.com.br"
    },
    "CENTRO EMPRESARIAL SHOPPING DA ILHA (CESDI)": {
        "regional": "MA",
        "coligada": "72",
        "empreendimento": "SPE - CONSTRUTORA SA CAVALCANTE LIV LTDA",
        "cnpj": "14.877.076/0001-04",
        "responsavel": "Vitória Castro // Neyrielle Coimbra",
        "email": "vitoria.castro@sacavalcante.com.br; ncoimbra@sacavalcante.com.br"
    },
    "LAGO A CORPORATE (MA)": {
        "regional": "MA",
        "coligada": "92",
        "empreendimento": "SPE CONSTR SA CAV LVL LTDA",
        "cnpj": "14.902.376/0001-04",
        "responsavel": "Vitória Castro // Neyrielle Coimbra",
        "email": "vitoria.castro@sacavalcante.com.br; ncoimbra@sacavalcante.com.br"
    },
    "RPE - RESERVA PENINSULA": {
        "regional": "MA",
        "coligada": "67",
        "empreendimento": "SPE Construtora Sá Cavalcante LVII LTDA",
        "cnpj": "",
        "responsavel": "Vitória Castro // Neyrielle Coimbra",
        "email": "vitoria.castro@sacavalcante.com.br; ncoimbra@sacavalcante.com.br"
    },
    "CONDCESRP1E2": {
        "regional": "PI",
        "coligada": "119",
        "empreendimento": "Condomínio Dos Grupamentos Empresariais A E B (Bl 06 E 07)",
        "cnpj": "34.957.181/0002-41",
        "responsavel": "Kesia Fernandes // Gelsa Bárbara",
        "email": "kesia.fernandes@sacavalcante.com.br; gbsantos@sacavalcante.com.br"
    },
    "CESRP": {
        "regional": "PI",
        "coligada": "64",
        "empreendimento": "SPE CONSTRUTORA SÁ CAVALCANTE L VIII",
        "cnpj": "14.652.779/0001-34",
        "responsavel": "Yohanna Cardoso // Gelsa Bárbara",
        "email": "yohana.cardoso@sacavalcante.com.br; gbsantos@sacavalcante.com.br"
    },
    "RISAS (PI)": {
        "regional": "PI",
        "coligada": "59",
        "empreendimento": "SPE - CONSTRUTORA SA CAVALCANTE LXII - PI LTDA",
        "cnpj": "14.092.770/0001-07",
        "responsavel": "Yohanna Cardoso // Gelsa Bárbara",
        "email": "yohana.cardoso@sacavalcante.com.br; gbsantos@sacavalcante.com.br"
    },
    "LA RESERVE": {
        "regional": "PI",
        "coligada": "128",
        "empreendimento": "SPE CONSTRUTORA SA CAVALCANTE 8X LTDA",
        "cnpj": "21.203.838/0001-65",
        "responsavel": "Yohanna Cardoso // Gelsa Bárbara",
        "email": "yohana.cardoso@sacavalcante.com.br; gbsantos@sacavalcante.com.br"
    },
    "RIO POTY OFFICES & RESIDENCES": {
        "regional": "PI",
        "coligada": "112",
        "empreendimento": "SPE CONSTRUTORA S A C LXII LTDA",
        "cnpj": "16.962.909/0001-60",
        "responsavel": "Yohanna Cardoso // Gelsa Bárbara",
        "email": "yohana.cardoso@sacavalcante.com.br; gbsantos@sacavalcante.com.br"
    }
}


def enviar_email_texto(destinatario: str, assunto: str, corpo_texto: str):
    msg = MIMEMultipart('alternative')
    msg['to'] = destinatario
    msg['subject'] = assunto
    msg.attach(MIMEText(corpo_texto, 'plain'))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId='me', body={'raw': raw}).execute()
    print(f"[diag] Email texto enviado para {destinatario}")

def enviar_email_gmailapi_com_imagem(destinatario, assunto, caminho_imagem):
    # Monta mensagem multipart com HTML e imagem inline
    msg = MIMEMultipart('related')
    msg['to'] = destinatario
    msg['subject'] = assunto

    # parte HTML com <img src="cid:print1">
    html_part = MIMEText(f"""
        <html>
          <body>
            <p>Segue abaixo o relatório da conciliação:</p>
            <img src="cid:print1">
          </body>
        </html>
    """, 'html')
    msg.attach(html_part)

    # anexa imagem
    with open(caminho_imagem, 'rb') as f:
        img_data = f.read()

    img = MIMEImage(img_data, name=os.path.basename(caminho_imagem))
    img.add_header('Content-ID', '<print1>')
    msg.attach(img)

    # codifica em base64 urlsafe
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    # envia pela Gmail API
    service.users().messages().send(userId='me', body={'raw': raw}).execute()
    print(f"[diag] E-mail enviado via Gmail API para {destinatario}")
def enviar_email_gmailapi(destinatarios, assunto, corpo_html):
    """
    Envia e-mail via Gmail API, permitindo múltiplos destinatários e cópia automática
    para gd_faturamento@sacavalcante.com.br.
    """
    if isinstance(destinatarios, str):
        destinatarios = [destinatarios]

    # sempre adiciona gd_faturamento como cópia
    cc_padrao = "gd_faturamento@sacavalcante.com.br"
    if cc_padrao not in destinatarios:
        cc = [cc_padrao]
    else:
        cc = []

    # cria mensagem
    message = MIMEMultipart('alternative')
    message['to'] = ", ".join(destinatarios)
    if cc:
        message['cc'] = ", ".join(cc)
    message['subject'] = assunto

    text_part = MIMEText("Seu cliente de email não suporta HTML.", 'plain')
    html_part = MIMEText(corpo_html, 'html')

    message.attach(text_part)
    message.attach(html_part)

    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    service.users().messages().send(userId='me', body={'raw': raw}).execute()
    print(f"[diag] Email enviado para {message['to']} (cc: {message.get('cc', '')}) via Gmail API")

# =====================================================================
# FUNÇÃO PARA ENVIAR EMAIL COM IMAGEM INLINE
# =====================================================================

def enviar_email_com_imagem(remetente, senha, destinatario, assunto, caminho_imagem):
    msg = EmailMessage()
    msg["From"] = remetente
    msg["To"] = destinatario
    msg["Subject"] = assunto

    # corpo em HTML com a imagem embutida
    msg.add_alternative(f"""
    <html>
      <body>
        <p>Segue abaixo o relatório da conciliação:</p>
        <img src="cid:img1">
      </body>
    </html>
    """, subtype='html')

    # anexa a imagem como inline
    with open(caminho_imagem, 'rb') as f:
        img_data = f.read()
    maintype, subtype = mimetypes.guess_type(caminho_imagem)[0].split('/')
    msg.get_payload()[0].add_related(img_data, maintype=maintype, subtype=subtype, cid="<img1>")

    # Enviar via SMTP_SSL (Gmail)
    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as smtp:
        smtp.login(remetente, senha)
        smtp.send_message(msg)

    print(f"[diag] E-mail enviado para {destinatario} com sucesso.")
def apply_device_metrics(context, page):
    try:
        if pyautogui:
            w, h = pyautogui.size()
        else:
            w, h = (1920, 1080)
    except Exception:
        w, h = (1920, 1080)
    session = context.new_cdp_session(page)
    session.send("Emulation.setDeviceMetricsOverride", {
        "width": int(w),
        "height": int(h),
        "deviceScaleFactor": 1,
        "mobile": False,
        "screenWidth": int(w),
        "screenHeight": int(h)
    })
    page.evaluate("() => window.dispatchEvent(new Event('resize'))")


def _dbg(log_dir, msg):
    from datetime import datetime
    ts = datetime.utcnow().isoformat()
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        os.makedirs(log_dir, exist_ok=True)
        with open(os.path.join(log_dir, "rpa_debug.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def _snap(page, log_dir, name):
    try:
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        os.makedirs(log_dir, exist_ok=True)
        png = os.path.join(log_dir, f"{ts}-{name}.png")
        html = os.path.join(log_dir, f"{ts}-{name}.html")
        page.screenshot(path=png, full_page=True)
        with open(html, "w", encoding="utf-8") as f:
            f.write(page.content())
        _dbg(log_dir, f"[snap] {png} ; [html] {html}")
    except Exception as e:
        _dbg(log_dir, f"[snap] falhou: {e!r}")

def _parse_target_date_from_env():
    """
    Lê CNAB_DATE do ambiente (YYYY-MM-DD ou DD/MM/YYYY). Se vazio/ruim, retorna None.
    """
    s = (os.getenv("CNAB_DATE") or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None

def _wait_file_stable(path, min_age_sec=2.0, timeout_sec=180):
    """
    Espera 'path' existir e ficar com tamanho estável por 'min_age_sec'.
    Timeout total em 'timeout_sec'. Retorna True/False.
    """
    start = time.time()
    last_size = -1
    last_change = time.time()
    while time.time() - start < timeout_sec:
        if os.path.isfile(path):
            try:
                sz = os.path.getsize(path)
            except Exception:
                sz = -1
            if sz != last_size:
                last_size = sz
                last_change = time.time()
            else:
                # tamanho igual desde a última checagem
                if time.time() - last_change >= min_age_sec and sz > 0:
                    return True
        time.sleep(0.5)
    return False



def upload_cnab_file(pg, filepath: str, log_dir: str, timeout_ms: int = 15000) -> bool:
    """
    Acha <input type=file name="File1"> em qualquer frame e faz set_input_files(filepath).
    """
    import time
    deadline = time.time() + (timeout_ms / 1000.0)
    content_fr = None
    while time.time() < deadline and not content_fr:
        for fr in pg.frames:
            try:
                if fr.locator('input[type="file"][name="File1"]').count() > 0:
                    content_fr = fr
                    break
            except Exception:
                pass
        time.sleep(0.2)

    if not content_fr:
        _dbg(log_dir, "input file name=File1 não encontrado.")
        return False

    try:
        content_fr.locator('input[type="file"][name="File1"]').set_input_files(filepath)
        _dbg(log_dir, f"arquivo enviado: {filepath}")
        return True
    except Exception as e:
        _dbg(log_dir, f"set_input_files falhou: {e!r}")
        return False

def _ensure_local_zip_from_drive(log_dir, filename="arquivos.zip"):
    """
    Busca o último arquivos.zip em TODO o Drive (não só raiz).
    """
    query = f"name = '{filename}' and trashed = false"

    resp = drive_service.files().list(
        q=query,
        spaces='drive',
        orderBy="modifiedTime desc",
        fields="files(id, name, modifiedTime)",
        pageSize=1
    ).execute()

    files = resp.get("files", [])
    if not files:
        _dbg(log_dir, f"Nenhum {filename} encontrado no Drive")
        return None

    file_id = files[0]["id"]
    local_zip = os.path.join(local_dir, filename)
    os.makedirs(local_dir, exist_ok=True)

    request = drive_service.files().get_media(fileId=file_id)
    with open(local_zip, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                _dbg(log_dir, f"Download {int(status.progress() * 100)}% concluído")

    _dbg(log_dir, f"Arquivo baixado do Drive para: {local_zip}")
    return local_zip


def normalizar_projeto(nome: str) -> str:
    """
    Normaliza o nome do projeto para bater com as chaves do dicionário DADOS_INCORPORADORAS.
    - Se for None, retorna string vazia.
    - Remove espaços extras.
    - Converte para maiúsculas.
    """
    if not nome:
        return ""
    nome = str(nome).strip()
    # se quiser, remove acentos também:
    import unicodedata
    nome = "".join(
        c for c in unicodedata.normalize("NFKD", nome)
        if not unicodedata.combining(c)
    )
    return nome.upper()


def ensure_zip_local_early(log_dir) -> str | None:
    """
    Sempre baixa o arquivos.zip mais recente do Drive (raiz).
    """
    global local_zip
    return _ensure_local_zip_from_drive(log_dir, filename="arquivos.zip")





def normalizar_msg(msg: str) -> str:
    """
    Converte msg para minúsculo, remove pontos e espaços extras.
    """
    if not msg:
        return ""
    # minúsculo
    msg = msg.lower()
    # remove pontos
    msg = msg.replace('.', ' ')
    # remove múltiplos espaços
    msg = re.sub(r'\s+', ' ', msg).strip()
    return msg
def run_rpa_enter_google_folder(base_dir: str, target_dir: str, log_dir: str):
    """
    Fluxo misto:
      - Lança o navegador via Playwright (headed)
      - Usa PyAutoGUI para: Ctrl+T, abrir URL, tabular, digitar usuário/senha e Enter
      - Quando carregar a tela pós-login, Playwright valida e clica no elemento id=4
    """
    zip_path_early = ensure_zip_local_early(log_dir)
    if not zip_path_early or not os.path.isfile(zip_path_early):
        _dbg(log_dir, "Não consegui obter o arquivos.zip (local/Drive). Abortando.")
        return
    def log_error(msg: str):
        try:
            os.makedirs(log_dir, exist_ok=True)
            with open(os.path.join(log_dir, 'rpa_erro.log'), 'a', encoding='utf-8') as logf:
                logf.write(f"[{datetime.utcnow().isoformat()}] {msg}\n")
        except Exception:
            pass

    # Helper para garantir zoom 100% com múltiplas estratégias e log
    def set_zoom_125(context, page, where=""):
        ok = False
        try:
            session = context.new_cdp_session(page)
            session.send("Emulation.setPageScaleFactor", {"pageScaleFactor": 1.00})
            print(f"[diag] ({where}) zoom 100% via CDP: OK")
            ok = True
        except Exception as e:
            print(f"[aviso] ({where}) CDP setPageScaleFactor falhou: {e!r}")
        try:
            page.evaluate("document.documentElement.style.zoom = '1.00'")
            for fr in page.frames:
                try:
                    fr.evaluate("document.documentElement.style.zoom = '1.00'")
                except Exception:
                    pass
            print(f"[diag] ({where}) CSS zoom=1.00 aplicado em page/frames (onde possível)")
        except Exception as e:
            print(f"[aviso] ({where}) CSS zoom falhou: {e!r}")
        try:
            page.bring_to_front()
            page.evaluate("document.body && (document.body.style.zoom='1.0')")
            print(f"[diag] ({where}) reafirmação de zoom 100% aplicada")
            ok = True
        except Exception as e:
            print(f"[aviso] ({where}) reafirmação de zoom falhou: {e!r}")
        return ok

    def _save_report_json(base_dir: str, *, headers, rows, meta, updated_at):
        """
        Salva um JSON em base_dir/last_report.json no formato que o /api/report lê.
        """
        import json, os
        payload = {
            "ready": True,
            "updated_at": updated_at,
            "headers": headers or [],
            "rows": rows or [],
            "meta": meta or {}
        }
        try:
            path = os.path.join(base_dir, 'last_report.json')
            os.makedirs(base_dir, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            print(f"[diag] relatório salvo em: {path}")
        except Exception as e:
            print(f"[aviso] falha ao salvar relatório: {e!r}")


    # Maximização “real” da janela e disparo de resize via CDP
    def hard_maximize_window_via_cdp(context, page, use_fullscreen=False):
        session = context.new_cdp_session(page)
        target = session.send("Browser.getWindowForTarget")
        window_id = target.get("windowId")
        session.send("Browser.setWindowBounds", {
            "windowId": window_id,
            "bounds": {"windowState": "fullscreen" if use_fullscreen else "maximized"}
        })
        page.evaluate("() => window.dispatchEvent(new Event('resize'))")


    try:
        print("a")
           
        # ===== PASSO 0: garantir arquivos.zip local =====
        zip_path_early = ensure_zip_local_early(log_dir)
        if not zip_path_early or not os.path.isfile(zip_path_early):
            _dbg(log_dir, "Não consegui obter o arquivos.zip (local/SMB). Abortando antes do navegador.")
            return

        with sync_playwright() as p:
            _dbg(log_dir, f"RPA iniciou. IS_WINDOWS={IS_WINDOWS} HEADLESS={HEADLESS} USE_PYAUTO={USE_PYAUTO}")


            # Diag: versões e plataforma
            try:
                print(f"[diag] platform=os.name={os.name}")
                print(f"[diag] playwright browsers installed: chromium")
            except Exception as e:
                print(f"[diag] erro ao obter diag: {e!r}")

            # Preparar argumentos comuns com DPI 1:1 e window-size dinâmico (sem fixar resolução)
            try:
                screen_w, screen_h = pyautogui.size()
            except Exception:
                screen_w, screen_h = (1920, 1080)  # fallback seguro se detecção falhar
            common_args = [
                "--force-device-scale-factor=1",
                "--high-dpi-support=1",
                f"--window-size={screen_w},{screen_h}",
                "--start-maximized",
                "--disable-gpu",
                "--disable-software-rasterizer",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--new-window",
                "--disable-features=CalculateNativeWinOcclusion",  # evita "occlusion" no Windows
            ]

            # 1) Tentar CONECTAR a um Chrome já aberto via CDP (sem baixar navegador)
            # === abrir navegador (condicional por SO) ===
            browser = None
            context = None
            page = None

            if IS_WINDOWS:
                # Perfil persistente no Windows para manter cookies/sessões
                user_data_dir = os.path.join(base_dir, "chrome_profile_rpa")
                os.makedirs(user_data_dir, exist_ok=True)
                context = p.chromium.launch_persistent_context(
                    user_data_dir,
                    headless=HEADLESS,   # no Win: False (UI visível)
                    viewport=None,
                    args=common_args,
                )
                page = context.pages[0] if context.pages else context.new_page()
                page.set_default_timeout(15000)
                _dbg(log_dir, "Chromium aberto.")
                _snap(page, log_dir, "01-pos-launch")


                browser = context.browser
            else:
                # Servidor Linux: simples e robusto
                browser = p.chromium.launch(headless=HEADLESS, args=common_args)
                context = browser.new_context(viewport=None)
                page = context.new_page()
                page.set_default_timeout(15000)

            # ajustes comuns pós-launch
            set_zoom_125(context, page, where="após launch")
            if not HEADLESS:
                try:
                    hard_maximize_window_via_cdp(context, page)
                except Exception as e:
                    print(f"[aviso] maximize ignorado (provável headless): {e!r}")
            apply_device_metrics(context, page)



            # Tentar trazer à frente, mas não travar o fluxo se falhar
            try:
                print("[diag] tentando page.bring_to_front() ...")
                page.bring_to_front()  # garante foco na janela antes do PyAutoGUI
                try:
                    page.evaluate("window.focus()")
                except Exception as e:
                    print(f"[aviso] window.focus() falhou: {e!r}")
                time.sleep(0.5)
                if USE_PYAUTO:
                    try:
                        pyautogui.hotkey("win", "up")
                    except Exception:
                        pass
                # força maximizar a janela ativa
                print("[diag] bring_to_front + maximize OK.")
            except Exception as e:
                print(f"[aviso] bring_to_front falhou e será ignorado: {e!r}")

            # 2) ====== SEÇÃO PyAutoGUI (mantida) ======
            time.sleep(3)  # passo 1
            page.goto(AZ_URL, wait_until="domcontentloaded") # passo 2
            _dbg(log_dir, f"goto OK: {AZ_URL}")
            _snap(page, log_dir, "02-pos-goto")

            # Aplicar zoom novamente após a navegação
            set_zoom_125(context, page, where="após goto()")
            if not HEADLESS:
                try:
                    hard_maximize_window_via_cdp(context, page)
                except Exception as e:
                    print(f"[aviso] maximize ignorado (provável headless): {e!r}")
            apply_device_metrics(context, page)

            time.sleep(2)  # passo 3

            page.bring_to_front()
            # passo 4
            time.sleep(3)  # passo 5
            page.evaluate("window.focus()")    # passo 6
            try:
                if USE_PYAUTO:
                    try:
                        pyautogui.hotkey("win", "up")
                    except Exception:
                        pass
                # reforça maximizado/foco após navegar
            except Exception as e:
                print(f"[aviso] hotkey maximize falhou: {e!r}")
            time.sleep(6)  # passo 7

            # ====== LOGIN (via Playwright por IDs; sem PyAutoGUI) ======
            def login_via_ids(pg, username: str, password: str, timeout_ms: int = 20000) -> bool:
                import time, re
                deadline = time.time() + (timeout_ms / 1000.0)

                def try_in_context(ctx):
                    try:
                        # campos com id e fallback por name
                        u = ctx.locator('#user, input[name="user"]').first
                        p = ctx.locator('#pwd, input[name="pwd"]').first

                        u.wait_for(state='visible', timeout=2500)
                        p.wait_for(state='visible', timeout=2500)

                        u.scroll_into_view_if_needed(timeout=1000)
                        u.click()
                        u.fill(username)

                        p.scroll_into_view_if_needed(timeout=1000)
                        p.click()
                        p.fill(password)

                        # botão "Entrar"
                        btn = ctx.locator('#btn-sign').first
                        if btn.count() > 0:
                            btn.scroll_into_view_if_needed(timeout=1000)
                            btn.click(timeout=3000, force=True)
                        else:
                            # fallback por texto do botão
                            alt = ctx.get_by_role("button", name=re.compile(r"^\s*entrar\s*$", re.I)).first
                            if alt.count() > 0:
                                alt.scroll_into_view_if_needed(timeout=1000)
                                alt.click(timeout=3000, force=True)
                            else:
                                # último recurso: Enter no campo senha
                                p.press("Enter")
                        return True
                    except Exception:
                        return False

                # tenta no topo e em todos os frames até o timeout
                while time.time() < deadline:
                    if try_in_context(pg):
                        return True
                    for fr in pg.frames:
                        if try_in_context(fr):
                            return True
                    time.sleep(0.2)
                return False

            ok_login = login_via_ids(page, "auto.ia", "S@cavalcant3", timeout_ms=20000)
            _dbg(log_dir, f"login via IDs: {'OK' if ok_login else 'FALHOU'}")
            _snap(page, log_dir, "03-pos-login")

            if not ok_login:
                print("[aviso] login via IDs falhou — verifique se #user/#pwd/#btn-sign existem.")
            else:
                print("[diag] credenciais preenchidas e envio do login realizado.")
            time.sleep(2.5)  # respiro adicional após enviar o login


            # 3) ====== A partir daqui, só Playwright ======

            # helper: achar a aba que abriu o domínio (PyAutoGUI usou Ctrl+T)
            def wait_for_az_page_all(br, timeout_ms=20000):
                print("[diag] aguardando aba do aztronic (todos os contexts)...")
                end = time.time() + (timeout_ms / 1000.0)
                while time.time() < end:
                    try:
                        for ctx in br.contexts:
                            for pg in ctx.pages:
                                try:
                                    url = (pg.url or "").lower()
                                    title = ""
                                    try:
                                        title = (pg.title() or "").lower()
                                    except Exception:
                                        pass
                                except Exception as e:
                                    print(f"[aviso] erro ao obter url/title: {e!r}")
                                    url, title = "", ""

                                if (
                                    "aztronic.com.br" in url
                                    or "collect_sc_homolog" in url
                                    or "default_tes.asp" in url
                                    or "collect aztronic" in title
                                ):
                                    print(f"[diag] aba encontrada: url={url!r} title={title!r}")
                                    try:
                                        pg.wait_for_load_state("domcontentloaded", timeout=3000)
                                    except Exception as e:
                                        print(f"[aviso] domcontentloaded timeout/erro: {e!r}")
                                    return pg
                    except Exception as e:
                        print(f"[aviso] erro ao inspecionar contexts/pages: {e!r}")
                    time.sleep(0.5)
                raise PWTimeout("Não encontrei a aba do AZtronic aberta via PyAutoGUI (em nenhum context).")

            page = wait_for_az_page_all(context.browser)

            
            # Espera a “tela inicial” pós-login.
            # Usamos sinais visuais simples que aparecem na sua captura:
            # - texto 'AZtronic Collect System' OU item 'CADASTROS' no menu lateral.
            def wait_post_login(pg, timeout_ms=20000):
                import re, time
                print("[diag] aguardando tela pós-login (url/frames)...")

                deadline = time.time() + (timeout_ms / 1000.0)
                checkpoints = [
                    'text=CADASTROS',
                    'text=ATENDIMENTO',
                    'text=COBRANÇA',
                    'text=RELATÓRIOS',
                    'text=CONTÁBIL/FISCAL',
                    'text=ADMINISTRAÇÃO',
                ]

                # 1) espere a navegação estabilizar
                try:
                    pg.wait_for_load_state("domcontentloaded", timeout=8000)
                except Exception:
                    pass

                last_err = None
                while time.time() < deadline:
                    try:
                        # 2) URL de lobby
                        try:
                            if re.search(r"default_tes\.asp", (pg.url or ""), re.I):
                                print("[diag] URL de lobby confirmada.")
                                return
                        except Exception:
                            pass

                        # 3) procura no top-level
                        for sel in checkpoints:
                            try:
                                if pg.locator(sel).first.is_visible():
                                    print(f"[diag] pós-login detectado (top-level) via {sel}")
                                    return
                            except Exception:
                                pass

                        # 4) procura em todos os frames
                        for fr in pg.frames:
                            for sel in checkpoints:
                                try:
                                    loc = fr.locator(sel).first
                                    if loc.count() > 0 and loc.is_visible():
                                        print(f"[diag] pós-login detectado (frame) via {sel}")
                                        return
                                except Exception:
                                    continue

                        time.sleep(0.4)
                    except Exception as e:
                        last_err = e
                        time.sleep(0.4)

                raise PWTimeout("Tela pós-login não confirmada: nem URL default_tes.asp nem itens do menu visíveis (tente ajustar os textos/frames).")

            wait_post_login(page)

            # ===== DEBUG: dump de textos logo após confirmar a tela pós-login =====
            def _safe_inner_text(loc):
                try:
                    return (loc.inner_text(timeout=800) or "").strip()
                except Exception:
                    return ""

            def debug_dump_text(pg, max_chars=8000, sample_per_selector=500):
                print("\n[debug] ===== DUMP DE TEXTOS (TOP-LEVEL) =====")
                try:
                    all_text = pg.evaluate("() => document.body ? document.body.innerText : ''") or ""
                    print(f"[debug] URL: {pg.url}")
                    print(f"[debug] Tamanho texto top-level: {len(all_text)} chars")
                    print(all_text[:max_chars])
                except Exception as e:
                    print(f"[debug] Falha ao ler innerText top-level: {e!r}")

                selectors = [
                    "a", "button", "[role=button]", "li", "div", "span", "td", "th",
                    "nav *", "aside *", "header *", "footer *"
                ]
                print("\n[debug] ===== AMOSTRAS TOP-LEVEL POR SELETOR =====")
                for sel in selectors:
                    try:
                        loc = pg.locator(sel)
                        cnt = loc.count()
                        if cnt == 0:
                            continue
                        print(f"[debug] {sel} -> {cnt} elementos (amostrando até {sample_per_selector})")
                        limit = min(cnt, sample_per_selector)
                        for i in range(limit):
                            t = _safe_inner_text(loc.nth(i))
                            if t:
                                print(f"  [{i:02d}] {t[:200]}")
                    except Exception as e:
                        print(f"[debug]   (erro ao amostrar {sel}: {e!r})")

                print("\n[debug] ===== FRAMES =====")
                try:
                    frames = pg.frames
                except Exception as e:
                    frames = []
                    print(f"[debug] Falha ao obter frames: {e!r}")

                for fr in frames:
                    try:
                        print(f"\n[debug] --- FRAME name={getattr(fr, 'name', None)!r} url={fr.url!r} ---")
                        try:
                            ftxt = fr.evaluate("() => document.body ? document.body.innerText : ''") or ""
                            print(f"[debug] Tamanho texto frame: {len(ftxt)} chars")
                            print(ftxt[:max_chars])
                        except Exception as e:
                            print(f"[debug] Falha ao ler innerText do frame: {e!r}")

                        print("[debug] Amostras no frame:")
                        for sel in selectors:
                            try:
                                loc = fr.locator(sel)
                                cnt = loc.count()
                                if cnt == 0:
                                    continue
                                print(f"  {sel} -> {cnt} elementos (amostrando até {sample_per_selector})")
                                limit = min(cnt, sample_per_selector)
                                for i in range(limit):
                                    t = _safe_inner_text(loc.nth(i))
                                    if t:
                                        print(f"    [{i:02d}] {t[:200]}")
                            except Exception as e:
                                print(f"    (erro ao amostrar {sel}: {e!r})")
                    except Exception as e:
                        print(f"[debug] Falha ao inspecionar frame: {e!r}")

            debug_dump_text(page, sample_per_selector=500)
            # ===== FIM DEBUG =====

            def extract_report_table(pg, timeout_ms=12000):
                """
                Encontra o frame que contém o relatório e extrai:
                - rows: lista de dicts (linhas da tabela)
                - meta: {'total_pago': '...', 'total_baixado': '...'}
                """
                import time, re

                deadline = time.time() + (timeout_ms / 1000.0)

                # (1) localizar frame do relatório
                target_fr = None
                while time.time() < deadline and not target_fr:
                    for fr in pg.frames:
                        try:
                            if fr.locator('text=/RELATÓRIO\\s+IMPORTAÇÃO\\s+RETORNO\\s+BANCÁRIO/i').count() > 0:
                                target_fr = fr
                                break
                        except Exception:
                            pass
                    if not target_fr:
                        # fallback: frame com maior número de TRs
                        best_fr, best_score = None, -1
                        for fr in pg.frames:
                            try:
                                tr_count = fr.locator("tr").count()
                                if tr_count > best_score:
                                    best_fr, best_score = fr, tr_count
                            except Exception:
                                continue
                        target_fr = best_fr if best_score > 0 else None
                    if not target_fr:
                        time.sleep(0.2)

                if not target_fr:
                    print("[aviso] não foi possível localizar o frame com a tabela do relatório.")
                    return [], {}

                # (2) escolher a maior <table>
                try:
                    tables = target_fr.locator("table")
                    tcount = tables.count()
                except Exception:
                    tcount = 0
                if tcount == 0:
                    print("[aviso] nenhum elemento <table> encontrado no frame alvo.")
                    return [], {}

                best_i, best_rows = 0, -1
                for i in range(tcount):
                    try:
                        r = tables.nth(i).locator("tr").count()
                        if r > best_rows:
                            best_rows, best_i = r, i
                    except Exception:
                        continue

                table = tables.nth(best_i)

                # (3) extrair linhas – primeiro com locator.evaluate, depois fallback com frame.evaluate
                rows = []
                try:
                    rows = table.evaluate(
                        """(tbl) => {
                            const getText = (n) => (n && n.innerText !== undefined) ? n.innerText.trim() : "";
                            const headers = [];
                            const result = [];

                            const thead = tbl.querySelector("thead");
                            let headerCells = [];
                            if (thead && thead.querySelectorAll("th").length) {
                                headerCells = Array.from(thead.querySelectorAll("th"));
                            } else {
                                const firstRow = tbl.querySelector("tr");
                                if (firstRow && firstRow.querySelectorAll("th").length) {
                                    headerCells = Array.from(firstRow.querySelectorAll("th"));
                                }
                            }
                            if (headerCells.length) {
                                headerCells.forEach((th,i)=>headers.push(getText(th) || `Coluna_${i+1}`));
                            }

                            const trs = Array.from(tbl.querySelectorAll("tr"));
                            for (let r = 0; r < trs.length; r++) {
                                const tr = trs[r];
                                const ths = Array.from(tr.querySelectorAll("th"));
                                const tds = Array.from(tr.querySelectorAll("td"));

                                if (r === 0 && (ths.length && !headers.length)) {
                                    ths.forEach((th,i)=>headers.push(getText(th) || `Coluna_${i+1}`));
                                    continue;
                                }
                                if (!tds.length) continue;

                                if (!headers.length) {
                                    for (let i=0;i<tds.length;i++) headers[i] = `Coluna_${i+1}`;
                                }

                                const rowObj = {};
                                for (let i=0;i<headers.length;i++){
                                    const cell = tds[i];
                                    rowObj[headers[i]] = getText(cell || null);
                                }
                                result.push(rowObj);
                            }
                            return result;
                        }"""
                    )
                except Exception as e:
                    print(f"[aviso] table.evaluate falhou: {e!r}; tentando fallback em frame.evaluate")
                    try:
                        rows = target_fr.evaluate(
                            """() => {
                                const getText = (n) => (n && n.innerText !== undefined) ? n.innerText.trim() : "";
                                const tbls = Array.from(document.querySelectorAll("table"));
                                tbls.sort((a,b)=> b.querySelectorAll("tr").length - a.querySelectorAll("tr").length);
                                const tbl = tbls[0];
                                if (!tbl) return [];
                                const headers = [];
                                const result = [];
                                const thead = tbl.querySelector("thead");
                                let headerCells = [];
                                if (thead && thead.querySelectorAll("th").length) {
                                    headerCells = Array.from(thead.querySelectorAll("th"));
                                } else {
                                    const firstRow = tbl.querySelector("tr");
                                    if (firstRow && firstRow.querySelectorAll("th").length) {
                                        headerCells = Array.from(firstRow.querySelectorAll("th"));
                                    }
                                }
                                if (headerCells.length) {
                                    headerCells.forEach((th,i)=>headers.push(getText(th) || `Coluna_${i+1}`));
                                }
                                const trs = Array.from(tbl.querySelectorAll("tr"));
                                for (let r = 0; r < trs.length; r++) {
                                    const tr = trs[r];
                                    const ths = Array.from(tr.querySelectorAll("th"));
                                    const tds = Array.from(tr.querySelectorAll("td"));
                                    if (r === 0 && (ths.length && !headers.length)) {
                                        ths.forEach((th,i)=>headers.push(getText(th) || `Coluna_${i+1}`));
                                        continue;
                                    }
                                    if (!tds.length) continue;
                                    if (!headers.length) {
                                        for (let i=0;i<tds.length;i++) headers[i] = `Coluna_${i+1}`;
                                    }
                                    const rowObj = {};
                                    for (let i=0;i<headers.length;i++){
                                        const cell = tds[i];
                                        rowObj[headers[i]] = getText(cell || null);
                                    }
                                    result.push(rowObj);
                                }
                                return result;
                            }"""
                        )
                    except Exception as e2:
                        print(f"[aviso] fallback frame.evaluate também falhou: {e2!r}")
                        rows = []

                # (4) limpeza leve de ruído
                if rows:
                    keys = list(rows[0].keys())
                    cleaned = []
                    for r in rows:
                        try:
                            s = "".join((r.get(k, "") for k in keys)).strip()
                        except Exception:
                            s = ""
                        if s != "T":
                            cleaned.append(r)
                    rows = cleaned

                # (5) Captura TOTAL PAGO / TOTAL BAIXADO varrendo o texto do frame
                    # (5) Captura TOTAL PAGO / TOTAL BAIXADO varrendo o texto do frame
                meta = {}
                try:
                    page_text = target_fr.evaluate("() => document.body ? document.body.innerText : ''") or ""
                    # normaliza NBSP ( ) para espaço comum e comprime espaços
                    page_text = page_text.replace('\xa0', ' ')
                    # regex correto (um \s só) + tolerante a espaços
                    m1 = re.search(r"TOTAL\s*PAGO:\s*([0-9\.,]+)", page_text, re.I)
                    m2 = re.search(r"TOTAL\s*BAIXADO:\s*([0-9\.,]+)", page_text, re.I)
                    if m1:
                        meta["total_pago"] = m1.group(1)
                    if m2:
                        meta["total_baixado"] = m2.group(1)

                    # pequeno fallback: tenta encontrar pelos elementos, caso o texto venha diferente
                    if "total_pago" not in meta or "total_baixado" not in meta:
                        try:
                            vals = target_fr.evaluate("""
                                () => {
                                    const txt = (n) => (n && n.innerText !== undefined) ? n.innerText.replace(/\u00A0/g,' ').trim() : "";
                                    const nodes = Array.from(document.querySelectorAll('td,div,span'));
                                    const out = {};
                                    for (const el of nodes) {
                                        const t = txt(el).toUpperCase();
                                        if (!out.tp && /TOTAL\\s*PAGO\\s*:/.test(t)) out.tp = t.replace(/^.*TOTAL\\s*PAGO\\s*:\\s*/,'');
                                        if (!out.tb && /TOTAL\\s*BAIXADO\\s*:/.test(t)) out.tb = t.replace(/^.*TOTAL\\s*BAIXADO\\s*:\\s*/,'');
                                        if (out.tp && out.tb) break;
                                    }
                                    return out;
                                }
                            """) or {}
                            if vals.get("tp") and "total_pago" not in meta:
                                meta["total_pago"] = vals["tp"]
                            if vals.get("tb") and "total_baixado" not in meta:
                                meta["total_baixado"] = vals["tb"]
                        except Exception:
                            pass
                except Exception as e:
                    print(f"[aviso] falha ao extrair meta: {e!r}")


                return rows, meta




            def print_table_or_dict(data):
                """
                Imprime no terminal como lista de dicts (JSON-like) e,
                se couber, também uma versão em colunas alinhadas.
                """
                from pprint import pprint
                if not data:
                    print("[diag] Nenhum dado de tabela para exibir.")
                    return

                # JSON-like
                print("\n[saida] ===== DADOS (lista de dicionários) =====")
                pprint(data, width=160)

                # Tabela simples alinhada (limita largura)
                keys = list(data[0].keys())
                # calcula largura por coluna (cap em 40)
                widths = {k: min(max(len(k), *(len((row.get(k) or "")) for row in data)), 40) for k in keys}

                def trunc(s, w):
                    s = (s or "")
                    return s if len(s) <= w else s[: w-1] + "…"

                print("\n[saida] ===== DADOS (tabela) =====")
                # header
                line = " | ".join(trunc(k, widths[k]).ljust(widths[k]) for k in keys)
                print(line)
                print("-" * len(line))
                # rows
                for row in data:
                    print(" | ".join(trunc(row.get(k, ""), widths[k]).ljust(widths[k]) for k in keys))

            # (novo)
            def drag_select_from_header_and_copy_table(pg, header_text="Ocorrência", timeout_ms=15000):
                """
                1) Localiza o frame do relatório.
                2) Encontra a célula de cabeçalho pelo texto (ex.: 'Ocorrência').
                3) Move o mouse até a célula, pressiona e segura (left down).
                4) Faz scroll sucessivo até o final da página, mantendo pressionado (seleção).
                5) Solta o mouse e lê o texto selecionado (window.getSelection()).
                6) Converte o texto selecionado em linhas/colunas (tabela) e retorna lista de dicts.
                Se não conseguir selecionar, retorna [] para permitir fallback.
                """
                import re, time

                # 1) localizar frame alvo (mesmo critério do extrator de relatório)
                deadline = time.time() + (timeout_ms / 1000.0)
                target_fr = None
                while time.time() < deadline and not target_fr:
                    for fr in pg.frames:
                        try:
                            if fr.locator('text=/RELATÓRIO\\s+IMPORTAÇÃO\\s+RETORNO\\s+BANCÁRIO/i').count() > 0:
                                target_fr = fr
                                break
                        except Exception:
                            pass
                    if not target_fr:
                        # fallback: maior tabela
                        best_fr, best_score = None, -1
                        for fr in pg.frames:
                            try:
                                tr_count = fr.locator("tr").count()
                                if tr_count > best_score:
                                    best_fr, best_score = fr, tr_count
                            except Exception:
                                continue
                        target_fr = best_fr if best_score > 0 else None
                    if not target_fr:
                        time.sleep(0.2)

                if not target_fr:
                    print("[aviso] frame do relatório não encontrado para seleção por arraste.")
                    return []

                # 2) localizar a célula com o texto do cabeçalho (Ocorrência)
                header_rx = re.compile(rf"\b{re.escape(header_text)}\b", re.I)
                header = None
                try:
                    header = target_fr.get_by_text(header_rx, exact=False).first
                    header.wait_for(state="visible", timeout=timeout_ms)
                    header.scroll_into_view_if_needed(timeout=2000)
                except Exception as e:
                    print(f"[aviso] cabeçalho '{header_text}' não encontrado: {e!r}")
                    return []

                # 3) iniciar a seleção com o mouse (down no início)
                try:
                    bb = header.bounding_box()
                    if not bb:
                        print("[aviso] bounding_box não disponível para o cabeçalho.")
                        return []
                    start_x = bb["x"] + min(bb["width"] * 0.2, 10)
                    start_y = bb["y"] + bb["height"] / 2
                    pg.mouse.move(start_x, start_y)
                    pg.mouse.down(button="left")
                except Exception as e:
                    print(f"[aviso] falha ao iniciar arraste: {e!r}")
                    return []

                # 4) scroll até o final mantendo o botão pressionado
                try:
                    # alguns passos longos de scroll; ajustável conforme a página
                    for _ in range(40):
                        pg.mouse.wheel(0, 800)
                        time.sleep(0.08)
                    # move o mouse para perto do rodapé para garantir a extensão total da seleção
                    # usa a posição máxima visível do viewport
                    try:
                        vp = pg.viewport_size
                        end_x = start_x
                        end_y = (vp["height"] - 10) if vp else (start_y + 1000)
                        pg.mouse.move(end_x, end_y)
                    except Exception:
                        pass
                finally:
                    # 5) soltar o mouse
                    try:
                        pg.mouse.up(button="left")
                    except Exception:
                        pass

                # 6) capturar o texto selecionado dentro do frame
                try:
                    selected_text = target_fr.evaluate("""() => {
                        const sel = window.getSelection && window.getSelection();
                        return sel ? String(sel) : "";
                    }""") or ""
                except Exception as e:
                    print(f"[aviso] não foi possível obter seleção: {e!r}")
                    selected_text = ""

                if not selected_text.strip():
                    print("[diag] seleção vazia após arraste; retornando [].")
                    return []

                # 7) converter o texto em tabela (heurística robusta)
                lines = [ln.strip() for ln in selected_text.splitlines() if ln.strip()]
                if not lines:
                    return []

                # tenta detectar cabeçalhos na primeira linha (separadores múltiplos espaços ou tab)
                sep = None
                if "\t" in lines[0]:
                    sep = "\t"
                elif "  " in lines[0]:
                    sep = re.compile(r"\s{2,}")  # 2+ espaços
                else:
                    # fallback: separa por um ou mais espaços
                    sep = re.compile(r"\s{1,}")

                headers = [h.strip() for h in (re.split(sep, lines[0]) if isinstance(sep, re.Pattern) else lines[0].split(sep))]
                rows_data = []
                for ln in lines[1:]:
                    cols = [c.strip() for c in (re.split(sep, ln) if isinstance(sep, re.Pattern) else ln.split(sep))]
                    # ajusta largura: se faltar colunas, completa; se sobrar, junta as últimas
                    if len(cols) < len(headers):
                        cols = cols + [""] * (len(headers) - len(cols))
                    elif len(cols) > len(headers):
                        # cola excesso na última coluna
                        extra = " ".join(cols[len(headers)-1:])
                        cols = cols[:len(headers)-1] + [extra]
                    rows_data.append(dict(zip(headers, cols)))

                return rows_data

            # Abrir "COBRANÇA" -> "Aut. Bancária-Cnab" e clicar "Baixar Arquivo"
            def open_menu_path_and_click(pg, path_labels, timeout_ms=15000):
                import re, time
                print(f"[diag] abrir caminho no menu: {' > '.join(path_labels)}")

                # localizar frame do menu
                menu_fr = None
                for fr in pg.frames:
                    u = (fr.url or "").lower()
                    n = (getattr(fr, "name", "") or "").lower()
                    if "comum/menu.asp" in u or n == "menu":
                        menu_fr = fr
                        break
                if not menu_fr:
                    print("[aviso] frame do menu não encontrado.")
                    return False

                def rx(s):
                    s = s.replace("\xa0", " ").strip()
                    s = re.sub(r"\s+", r"\\s*", re.escape(s))
                    return re.compile(s, re.I)

                def ensure_expanded(label_pat):
                    # tenta clicar no próprio rótulo; se não expandir, tenta clicar um pouco à esquerda (ícone +)
                    try:
                        el = menu_fr.get_by_text(label_pat, exact=False).first
                        el.scroll_into_view_if_needed(timeout=2000)
                        # Logar coordenadas se possível
                        try:
                            bb = el.bounding_box()
                            if bb:
                                x = bb["x"] + 6
                                y = bb["y"] + bb["height"]/2
                                print(f"[log] expandir '{label_pat.pattern}' -> coords=({x:.1f}, {y:.1f}) bbox={bb}")
                        except Exception:
                            pass
                        el.click(timeout=2500, force=True)
                        time.sleep(0.3)
                        return True
                    except Exception:
                        try:
                            el = menu_fr.locator(f"text=/{label_pat.pattern}/i").first
                            el.scroll_into_view_if_needed(timeout=2000)
                            el.click(timeout=2500, force=True)
                            time.sleep(0.3)
                            return True
                        except Exception:
                            return False


                # expande todos os níveis exceto o último (alvo final)
                for label in path_labels[:-1]:
                    if not ensure_expanded(rx(label)):
                        print(f"[aviso] não consegui expandir '{label}'")
                # clica alvo final
                target_pat = rx(path_labels[-1])
                end = time.time() + (timeout_ms / 1000.0)
                while time.time() < end:
                    try:
                        loc = menu_fr.get_by_text(target_pat, exact=False)
                        if loc.count() > 0:
                            it = loc.first
                            it.scroll_into_view_if_needed(timeout=2000)
                            it.click(timeout=3000, force=True)
                            print(f"[log] clique item final '{path_labels[-1]}' via locator.click()")
                            print("[diag] clique no item final realizado")
                            return True

                    except Exception:
                        pass
                    time.sleep(0.3)
                return False
            
            def click_menu_plus_by_subitem_id(pg, subitem_id: str, timeout_ms=8000):
                """
                No frame do menu (comum/menu.asp), encontra <div class="subItem" id=<id>>
                e clica no ícone '+' (<img class="expandir" | class*="expan" | src*="maiscruz">).
                Nunca usa frame.mouse; clica sempre via locator.click().
                """
                import time

                # 1) achar o frame do menu
                menu_fr = None
                for fr in pg.frames:
                    u = (fr.url or "").lower()
                    n = (getattr(fr, "name", "") or "").lower()
                    if "comum/menu.asp" in u or n == "menu":
                        menu_fr = fr
                        break
                if not menu_fr:
                    print("[aviso] frame do menu não encontrado.")
                    return False

                # 2) localizar a linha do item (classe 'subItem' – atenção ao "I" maiúsculo)
                deadline = time.time() + (timeout_ms/1000.0)
                row = None
                while time.time() < deadline:
                    try:
                        cand = menu_fr.locator(f'div.subItem[id="{subitem_id}"]')
                        if cand.count() > 0:
                            row = cand.first
                            break
                    except Exception:
                        pass
                    time.sleep(0.2)

                if not row:
                    print(f"[aviso] não achei div.subItem id='{subitem_id}'.")
                    return False

                row.scroll_into_view_if_needed(timeout=2000)

                # 3) clicar explicitamente no <img> do '+'
                try:
                    # cobre class="expandir" (seu DOM) e variantes, além do src do ícone
                    img = row.locator('img.expandir, img[class*="expan"], img[src*="maiscruz"]').first
                    if img.count() == 0:
                        img = row.locator("img").first  # último recurso

                    # clique direto no IMG (independente de frame)
                    img.scroll_into_view_if_needed(timeout=1500)
                    # (opcional) log para confirmar que é o ícone certo
                    # print("[debug] img:", img.evaluate("el => el.outerHTML").strip()[:200])

                    img.click(timeout=3000, force=True)

                    # confirmação leve: algum filho com parent=<id> costuma ficar visível
                    try:
                        menu_fr.locator(f'div.subItem[parent="{subitem_id}"]').first.wait_for(
                            state="visible", timeout=4000
                        )
                    except Exception:
                        pass

                    print("[diag] clique no '+' realizado")
                    return True

                except Exception as e:
                    # último recurso: clicar um pouco no início da linha (offset relativo ao próprio elemento)
                    try:
                        bb = row.bounding_box()
                        if bb:
                            y_rel = int(bb["height"] / 2) if bb["height"] else 10
                            row.click(position={"x": 10, "y": y_rel}, timeout=2000, force=True)
                            print("[diag] clique por offset na linha (início) realizado")
                            return True
                    except Exception as e2:
                        print(f"[aviso] falhou clicar no '+': {e!r} / fallback: {e2!r}")
                    return False







            def click_by_text_anywhere(pg, target_text: str, timeout_ms=10000):
                import re, time
                print(f"[diag] procurando e clicando por texto: {target_text!r}")
                pattern = re.compile(re.escape(target_text), re.I)

                deadline = time.time() + (timeout_ms / 1000.0)

                def try_click_in_context(ctx):
                    try:
                        loc = ctx.get_by_role("link", name=pattern)
                        if loc.count() > 0:
                            el = loc.first
                            el.scroll_into_view_if_needed(timeout=2000)
                            el.click(timeout=3000, force=True)
                            print("[diag] clique via role=link/name regex")
                            return True

                    except Exception:
                        pass
                    try:
                        loc = ctx.get_by_role("button", name=pattern)
                        if loc.count() > 0:
                            el = loc.first
                            el.scroll_into_view_if_needed(timeout=2000)
                            el.click(timeout=3000, force=True)
                            print("[diag] clique via role=button/name regex")
                            return True

                    except Exception:
                        pass
                    for sel in [
                        f'text=/{re.escape(target_text)}/i',
                        f'xpath=//*[normalize-space(text())="{target_text}"]',
                        f'xpath=//*[contains(normalize-space(.), "{target_text}")]',
                    ]:
                        try:
                            loc = ctx.locator(sel)
                            if loc.count() > 0:
                                el = loc.first
                                el.scroll_into_view_if_needed(timeout=2000)
                                el.click(timeout=3000, force=True)
                                print(f"[diag] clique via seletor {sel}")
                                return True

                        except Exception:
                            continue
                    return False

                while time.time() < deadline:
                    if try_click_in_context(pg):
                        return True
                    for fr in pg.frames:
                        if try_click_in_context(fr):
                            return True
                    time.sleep(0.3)
                return False

            # Fallback textual em qualquer frame (caso o menu ainda esteja recolhido)
            def open_file_dialog_and_type_folder(pg, folder_path: str, timeout_ms=15000):
                """
                Encontra o input <input type="file" name="File1"> em qualquer frame,
                clica nele para abrir o diálogo nativo e digita a pasta desejada.
                """
                import time

                # 1) achar o frame que contém o input name=File1
                content_fr = None
                deadline = time.time() + (timeout_ms / 1000.0)
                while time.time() < deadline and not content_fr:
                    for fr in pg.frames:
                        try:
                            if fr.locator('input[type="file"][name="File1"]').count() > 0:
                                content_fr = fr
                                break
                        except Exception:
                            continue
                    time.sleep(0.2)

                if not content_fr:
                    print("[aviso] input file name=File1 não encontrado em nenhum frame.")
                    return False

                file_input = content_fr.locator('input[type="file"][name="File1"]').first
                file_input.scroll_into_view_if_needed(timeout=2000)

                # 2) traz a janela à frente e clica no input para abrir o diálogo
                # 2) traz a janela à frente e clica no input para abrir o diálogo
                try:
                    pg.bring_to_front()
                except Exception:
                    pass
                time.sleep(0.2)
                file_input.click(timeout=3000, force=True)

                # 3) usar PyAutoGUI no diálogo nativo (Windows): Alt+D -> digita pasta -> Enter
                time.sleep(0.8)  # dá tempo do diálogo aparecer
                try:
                    # Vai para a barra de endereço e navega até a pasta
                    pyautogui.hotkey("alt", "d")          # foco na barra de endereço
                    time.sleep(0.2)
                    pyautogui.typewrite(folder_path, interval=0.01)
                    time.sleep(0.2)
                    pyautogui.press("enter")               # entra na pasta

                    # ==== SEQUÊNCIA SOLICITADA ====
                    time.sleep(0.6)                        # pequeno respiro após navegar
                    # Garante foco no campo "Nome do arquivo" (mais robusto)
                    try:
                        pyautogui.hotkey("alt", "n")       # foca a caixa "Nome" (opcional, ajuda no Windows)
                        time.sleep(0.2)
                    except Exception:
                        pass

                    pyautogui.typewrite("arquivos", interval=0.01)  # digita "arquivos"
                    time.sleep(0.2)
                    pyautogui.press("tab")                 # tab
                    time.sleep(0.2)
                    pyautogui.press("enter")      
                    time.sleep(3)         # enter (confirmar)

                    print(f"[diag] caminho digitado no diálogo: {folder_path} -> 'arquivos' + TAB + ENTER enviados")
                except Exception as e:
                    print(f"[aviso] falha ao interagir com o diálogo nativo: {e!r}")
                    return False

                return True
            
            def wait_and_close_alert(pg, timeout_ms=6000):
                """
                Fecha um alerta/modal nativo se aparecer após o 'Processar'.
                1) Tenta capturar 'dialog' (alert/confirm/prompt) do browser e aceitar.
                2) Se não houver evento de dialog, tenta localizar um botão 'OK' visível
                (inclusive dentro de frames) e clicar nele.
                Retorna True se algo foi fechado; False se nada apareceu.
                """
                import re, time
                deadline = time.time() + (timeout_ms / 1000.0)

                # 1) dialog nativo (window.alert/confirm/prompt)
                try:
                    dialog = pg.wait_for_event("dialog", timeout=timeout_ms)
                    try:
                        txt = dialog.message
                    except Exception:
                        txt = ""
                    dialog.accept()
                    print(f"[diag] alerta/confirm aceito. msg={txt!r}")
                    return True
                except Exception:
                    pass

                # 2) botão 'OK' renderizado no DOM (inclusive em iframes)
                pattern = re.compile(r"^\s*ok\s*$", re.I)

                def try_click_ok(ctx):
                    try:
                        loc = ctx.get_by_role("button", name=pattern)
                        if loc.count() > 0:
                            el = loc.first
                            el.scroll_into_view_if_needed(timeout=1500)
                            el.click(timeout=2500, force=True)
                            print("[diag] botão 'OK' clicado (role=button).")
                            return True
                    except Exception:
                        pass
                    for sel in ['text=/^\\s*OK\\s*$/i', 'xpath=//*[normalize-space(text())="OK"]']:
                        try:
                            loc = ctx.locator(sel)
                            if loc.count() > 0:
                                el = loc.first
                                el.scroll_into_view_if_needed(timeout=1500)
                                el.click(timeout=2500, force=True)
                                print(f"[diag] botão 'OK' clicado ({sel}).")
                                return True
                        except Exception:
                            continue
                    return False

                while time.time() < deadline:
                    if try_click_ok(pg):
                        return True
                    for fr in pg.frames:
                        if try_click_ok(fr):
                            return True
                    time.sleep(0.2)

                print("[diag] nenhum alerta/modal detectado.")
                return False

            def click_processar_button(pg, timeout_ms=10000):
                """
                Encontra e clica no botão 'Processar' (input[type=submit]) no frame de conteúdo.
                Procura em todos os frames por segurança.
                """
                import re, time
                deadline = time.time() + (timeout_ms / 1000.0)

                while time.time() < deadline:
                    for fr in pg.frames:
                        try:
                            # 1) CSS direto no input submit com 'Processar' no value
                            btn = fr.locator('input[type="submit"][value*="Processar"]').first
                            _dbg(log_dir, "botao Processar: clique OK")
                            _snap(page, log_dir, "04-pos-processar")

                            if btn.count() > 0:
                                btn.scroll_into_view_if_needed(timeout=1500)
                                btn.click(timeout=3000, force=True)
                                print("[diag] botão 'Processar' clicado (via CSS value*=Processar).")
                                return True
                        except Exception:
                            pass
                        try:
                            # 2) Papel de botão pelo nome (caso mapeie o role)
                            btn = fr.get_by_role("button", name=re.compile(r"processar", re.I)).first
                            if btn.count() > 0:
                                btn.scroll_into_view_if_needed(timeout=1500)
                                btn.click(timeout=3000, force=True)
                                print("[diag] botão 'Processar' clicado (via role=button).")
                                return True
                        except Exception:
                            pass
                    time.sleep(0.2)

                print("[aviso] botão 'Processar' não encontrado em nenhum frame.")
                return False


            # 1) abre COBRANÇA (seção)
            open_menu_path_and_click(page, ["COBRANÇA"], timeout_ms=12000)

            # 2) clica no '+' do Aut. Bancária-Cnab (id=33)
            if not click_menu_plus_by_subitem_id(page, "33", timeout_ms=8000):
                print("[aviso] não consegui clicar no + de 'Aut. Bancária-Cnab' (id=33).")

            # 3) agora que expandiu, clique no item desejado do drop-down
            click_by_text_anywhere(page, "Baixar Arquivo", timeout_ms=8000)

            # 4) após carregar a tela de 'Arquivo de Baixa CNAB', abrir o diálogo e digitar a pasta
                        # 4) após carregar a tela de 'Arquivo de Baixa CNAB', abrir o diálogo e digitar a pasta
                        # === UPLOAD CNAB: Windows usa diálogo; Linux faz upload direto no <input> ===
                        # 4) gerar o ZIP com os .RET e fazer upload direto do arquivo
            # === UPLOAD CNAB ===
            # Windows: gera ZIP via aplicativo do cliente
            # Linux: continua montando ZIP a partir de CNAB_SRC_LINUX
            # === UPLOAD CNAB ===
            # já garantimos no PASSO 0; mas revalida:
            # === UPLOAD CNAB ===
            # Reutiliza o caminho obtido no PASSO 0 (zip_path_early)
            zip_path = zip_path_early
            if not (zip_path and os.path.isfile(zip_path)):
                _dbg(log_dir, "Não há ZIP para enviar — abortando.")
                return

            ok_up = upload_cnab_file(page, zip_path, log_dir, timeout_ms=20000)

            if not ok_up:
                _dbg(log_dir, "Upload do ZIP falhou.")
                return


            # 5) clicar no botão 'Processar' e seguir
            if click_processar_button(page, timeout_ms=15000):
                # fecha alerta/modal, se existir
                wait_and_close_alert(page, timeout_ms=6000)

                # dá tempo do servidor gerar/atualizar o relatório
                time.sleep(2)

                # 1ª tentativa: seleção por arraste a partir do cabeçalho "Ocorrência"
                # 1ª tentativa: seleção por arraste a partir do cabeçalho "Ocorrência"
                dados, meta = extract_report_table(page, timeout_ms=20000)
                if not dados:
                    dados = drag_select_from_header_and_copy_table(page, header_text="Ocorrência", timeout_ms=20000)


                meta = {}
                if not dados:
                    dados, meta = extract_report_table(page, timeout_ms=20000)
                else:
                    _rows_ignore, meta = extract_report_table(page, timeout_ms=12000)
                if dados and all(str(k).lower().startswith('coluna_') for k in dados[0].keys()):
                    # Pega a primeira linha dos dados para virar o header
                    new_headers = list(dados[0].values())  # assume que a primeira linha tem os títulos
                    # Pega as linhas restantes
                    new_rows = dados[1:]

                    # Reconstrói as linhas usando os novos headers
                    new_data = []
                    for r in new_rows:
                        # pega os valores na mesma ordem
                        vals = list(r.values())
                        # completa se faltar ou corta se sobrar
                        if len(vals) < len(new_headers):
                            vals += [""] * (len(new_headers)-len(vals))
                        elif len(vals) > len(new_headers):
                            vals = vals[:len(new_headers)]
                        new_data.append(dict(zip(new_headers, vals)))
                    
                    dados = new_data
                if dados:
                    dados = [r for r in dados if r.get('Msg') and r.get('Msg').strip()]

                # --- NOVO FILTRO: elimina linhas fantasmas / incompletas ---
                if dados:
                    # exemplo: exige pelo menos 3 campos preenchidos na linha
                    dados_filtrados = []
                    for r in dados:
                        filled = sum(1 for v in r.values() if v and v.strip())
                        # if filled < 3:
                        #     # ignora linha quase vazia ou apenas mensagem
                        #     continue
                        dados_filtrados.append(r)
                    dados = dados_filtrados
                whitelist = tuple(map(normalizar_msg, [
                    "Título já se encontra baixado",
                    "Entrada confirmada pelo banco",
                    "OK",
                ]))
                linhas_problematicas = []
                # --- NOVO: separar linhas problemáticas pela Msg ---
                # for r in dados:
                #     ocorrencia = (r.get("Ocorrência") or r.get("Ocorrencia") or "").strip().lower()
                #     projeto = normalizar_projeto(r.get("Projeto"))
                #     dt_credito = r.get("DT. Crédito/Entrada") or r.get("DT. Credito/Entrada") or ""
                #     msg_norm = normalizar_msg(r.get('Msg', ''))

                #     if ocorrencia == "pagamento" and not any(msg_norm.startswith(w) for w in whitelist):
                #         print(f"[debug] Projeto original='{r.get('Projeto')}', normalizado='{projeto}'")

                #         if projeto == "":  # explicitamente vazio
                #             destino = "ratinho2345@gmail.com"
                #             assunto = "Pendência conciliação [SEM PROJETO]"
                #             corpo = f"Segue pendência de conciliação [SEM PROJETO] {dt_credito}."
                #         else:
                #             destino = "daniel@inovailab.com"
                #             assunto = f"Pendência conciliação {projeto}"
                #             corpo = f"Segue pendência de conciliação {projeto} {dt_credito}."

                #         enviar_email_texto(destino, assunto, corpo)
                #         _dbg(log_dir, f"Pendência enviada para {destino}: {projeto or '[SEM PROJETO]'} {dt_credito}")

                
                if dados:
                    for r in dados:
                        msg_norm = normalizar_msg(r.get('Msg', ''))
                        # se não bate com nenhum da whitelist, é problemático
                        if not any(msg_norm.startswith(w) for w in whitelist):
                            linhas_problematicas.append(r)

                # Se tiver problemáticas, envia email com tabela delas
                # if linhas_problematicas:
                #     for lp in linhas_problematicas:
                #         # Normaliza projeto e transforma em upper case para bater com as chaves do dicionário
                #         projeto = normalizar_projeto(lp.get("Projeto")).upper()

                #         if not projeto:
                #             # Sem projeto → Ratinho
                #             # destino = "ratinho2345@gmail.com"
                #             destino = "gd_faturamento@sacavalcante.com.br"
                #         else:
                #             # Com projeto → tenta encontrar no dicionário
                #             info = DADOS_INCORPORADORAS.get(projeto)
                #             if info and info.get("email"):
                #                 # Limpa a lista de e-mails (aceita ; ou , e remove espaços)
                #                 destinatarios = [
                #                     e.strip() for e in info["email"].replace(";", ",").split(",") if e.strip()
                #                 ]
                #                 destino = ", ".join(destinatarios)
                #             else:
                #                 # Fallback se não encontrou no dicionário
                #                 destino = "daniel@inovailab.com"

                #         # Monta HTML só para essa linha
                #         html_linha = gerar_html_tabela([lp], meta=None)
                #         assunto_pb = f"[ALERTA] Linha problemática ({projeto or '[SEM PROJETO]'}) {datetime.now().strftime('%d/%m/%Y')}"

                #         try:
                #             enviar_email_gmailapi(destino, assunto_pb, html_linha)
                #             _dbg(log_dir, f"Enviado e-mail linha problemática para {destino}: {projeto or '[SEM PROJETO]'}")
                #         except Exception as e:
                #             _dbg(log_dir, f"Falha ao enviar e-mail linha problemática ({destino}): {e}")





                # === NOVO: PENDÊNCIAS (Pagamento com erro) ===
                # Regras:
                # - erro = Msg não pertence à whitelist
                # - Ocorrência == "Pagamento"
                # - Com Projeto -> daniel@inovailab.com
                # - Sem Projeto -> ratinho2345@gmail.com

                def _get(r, keys):
                    for k in keys:
                        if k in r and r[k] is not None:
                            return str(r[k]).strip()
                    return ""

                

                # filtra só os problemáticos do tipo Pagamento
                grupos_por_projeto = defaultdict(list)

                for r in dados:
                    msg_norm = normalizar_msg(r.get('Msg', ''))
                    ocorr = (r.get("Ocorrência") or r.get("Ocorrencia") or "").strip().lower()
                    projeto = normalizar_projeto(r.get("Projeto"))

                    if not projeto:
                        # <<< NOVO >>> sempre manda para [SEM PROJETO], sem olhar ocorrencia
                        grupos_por_projeto[projeto].append(r)
                    else:
                        # mantém regra antiga para quem tem projeto
                        if ocorr == "pagamento" and not any(msg_norm.startswith(w) for w in whitelist):
                            grupos_por_projeto[projeto].append(r)


                # --- ENVIA UM E-MAIL POR PROJETO ---
                for projeto, linhas in grupos_por_projeto.items():
                    if not projeto:
                        projeto_ok = "[SEM PROJETO]"
                    else:
                        projeto_ok = projeto

                    html_linhas = gerar_html_tabela(linhas, meta=None)
                    assunto = f"Pendência de conciliação {projeto_ok} {datetime.now().strftime('%d/%m/%Y')}"

                    if not projeto:
                        corpo_html = (
                            f"<p>Verificamos que a pendência está relacionada à ausência de projeto vinculado.</p>"
                            f"<p>Gentileza confirmar e informar o projeto correto para que seja possível concluir a conciliação.</p>"
                            f"{html_linhas}"
                        )
                    else:
                        datas_credito = []
                        for l in linhas:
                            dt_cred = l.get("DT. Crédito/Entrada") or l.get("DT. Credito/Entrada") or ""
                            if dt_cred and dt_cred not in datas_credito:
                                datas_credito.append(dt_cred)
                        datas_txt = ", ".join(datas_credito) if datas_credito else datetime.now().strftime("%d/%m/%Y")

                        corpo_html = (
                            f"<p>📌 <b>Pendência de Conciliação Identificada</b></p>"
                            f"<p><b>Referência:</b> {projeto_ok} – {datas_txt}</p>"
                            f"<p>Bom dia.</p>"
                            f"<p>Favor verificar a ocorrência e informar a parcela correta para baixa, "
                            f"de modo a possibilitar a finalização da conciliação.</p>"
                            f"<p>Aguardamos sua devolutiva.</p>"
                            f"<p>Obrigada.</p>"
                            f"{html_linhas}"
                        )

                    # 🔽 SUBSTITUI AQUI
                    info = DADOS_INCORPORADORAS.get(projeto_ok.upper())
                    if info and info.get("email"):
                        destinatarios = [
                            e.strip() for e in info["email"].replace(";", ",").split(",") if e.strip()
                        ]
                    else:
                        destinatarios = ["gd_faturamento@sacavalcante.com.br"]

                    enviar_email_gmailapi(destinatarios, assunto, corpo_html)
                    _dbg(log_dir, f"E-mail enviado para {', '.join(destinatarios)} (com cópia) — {len(linhas)} linhas do projeto {projeto_ok}")






                # === NOVO BLOCO: enviar e-mails de pendência individual ===
                # Normaliza apelidos válidos a partir do seu dicionário
                apelidos_validos = {a.lower(): a for a in DADOS_INCORPORADORAS.keys()}

                K_OCOR = ["Ocorrência", "Ocorrencia"]
                K_PROJ = ["Projeto"]
                K_DTCRED = ["DT. Credito/Entrada", "DT. Crédito/Entrada", "DT. Credito/Entrada"]

                # for r in linhas_problematicas:
                #     ocorr = (r.get("Ocorrência") or r.get("Ocorrencia") or "").strip().lower()
                #     if ocorr != "pagamento":
                #         continue

                #     projeto = normalizar_projeto(r.get("Projeto"))
                #     dt_credito = (
                #         r.get("DT. Crédito/Entrada")
                #         or r.get("DT. Credito/Entrada")
                #         or ""
                #     ).strip()

                #     if projeto:  # sempre Daniel se houver projeto
                #         corpo = f"Segue pendência de conciliação {projeto} {dt_credito}."
                #         assunto = f"Pendência de conciliação - {projeto}"
                #         enviar_email_texto("daniel@inovailab.com", assunto, corpo)
                #         _dbg(log_dir, f"Pendência enviada para daniel@inovailab.com: {projeto} {dt_credito}")
                #     else:  # sem projeto vai para Ratinho
                #         corpo = f"Segue pendência de conciliação [SEM PROJETO] {dt_credito}."
                #         assunto = "Pendência de conciliação - [SEM PROJETO]"
                #         enviar_email_texto("ratinho2345@gmail.com", assunto, corpo)
                #         _dbg(log_dir, f"Pendência enviada para ratinho2345@gmail.com (sem projeto): {dt_credito}")



                
                if dados and all(str(v).strip().lower().startswith('coluna_') for v in dados[0].values()):
                    dados = dados[1:]
                print_table_or_dict(dados)

                headers = list(dados[0].keys()) if dados else []
                _save_report_json(
                    log_dir,
                    headers=headers,
                    rows=dados,
                    meta=meta,
                    updated_at=datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                )
                print("[diag] HEADERS DETECTADOS:", list(dados[0].keys()))

                # === NOVO: Gera HTML da Tabela ===
                html_corpo = gerar_html_tabela(dados, meta)
                print(f"[diag] dados extraídos: {len(dados)} linhas")
                print(f"[diag] html_corpo: {html_corpo[:500]}")
                # === ENVIAR EMAIL COM TABELA HTML ===
                assunto = f"Conciliação Incorporadora {datetime.now().strftime('%d/%m/%Y')}"
                # destinatario = "danieltl@poli.ufrj.br"
                destinatario = "gd_faturamento@sacavalcante.com.br"

                try:
                    enviar_email_gmailapi(destinatario, assunto, html_corpo)
                    _dbg(log_dir, "E-mail enviado com sucesso via Gmail API com tabela HTML.")
                except Exception as e:
                    _dbg(log_dir, f"Falha ao enviar e-mail via Gmail API: {e}")



            else:
                print("[aviso] não foi possível clicar no botão 'Processar'.")




            # (opcional) encerrar o navegador conforme solicitado
            try:
                browser.close()
            except Exception:
                pass
            return


            # (Opcional) valide algo após o clique:
            # page.wait_for_selector('text="...resultado do clique..."', timeout=10000)

            # Deixe o browser aberto durante desenvolvimento; feche em produção:
            # browser.close()

    except Exception as e:
        print(f"[erro] exceção capturada no fluxo principal: {e!r}")
        log_error(f"Erro no fluxo RPA (PyAutoGUI + Playwright): {e}")
