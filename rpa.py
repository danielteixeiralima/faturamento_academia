# filename: rpa.py
# RPA para o portal W12 (Bodytech) — fluxo: login, seleção de unidade e navegação inicial.
# Compatível com app.py:
#   from rpa import run_rpa_enter_google_folder, _ensure_local_zip_from_drive, _ensure_local_zip_from_drive

import os
import re
import time
from datetime import datetime
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout, Error as PWError

AZ_URL = "https://evo5.w12app.com.br/#/acesso/bodytech/autenticacao"

LOGIN_EMAIL = os.getenv("W12_EMAIL", "inova.ia@sacavalcante.com.br")
LOGIN_PASS  = os.getenv("W12_PASS",  "omega536")

HEADLESS = (os.getenv("HEADLESS", "0") == "1")
KEEP_OPEN = (os.getenv("KEEP_BROWSER_OPEN", "0") == "1")

def _get_upload_dir() -> str:
    if os.name == "nt":
        return os.getenv("CNAB_LOCAL_DIR_WINDOWS", r"C:\AUTOMACAO\conciliacao\arquivos")
    return os.getenv("CNAB_LOCAL_DIR", "/home/felipe/Downloads/arquivos")

def _dbg(log_dir: str, msg: str) -> None:
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    line = f"[{ts}] [rpa] {msg}"
    print(line, flush=True)
    try:
        os.makedirs(log_dir, exist_ok=True)
        with open(os.path.join(log_dir, "rpa_debug.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def _save_report_json(base_dir: str, payload: dict) -> None:
    try:
        os.makedirs(base_dir, exist_ok=True)
        path = os.path.join(base_dir, "last_report.json")
        payload = dict(payload or {})
        payload.setdefault("ready", True)
        payload.setdefault("updated_at", datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
        payload.setdefault("headers", list(payload.get("rows", [{}])[0].keys()) if payload.get("rows") else [])
        payload.setdefault("meta", {})
        import json
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        _dbg(base_dir, f"Relatório salvo em: {path}")
    except Exception as e:
        _dbg(base_dir, f"Falha ao salvar last_report.json: {e!r}")

def _screenshot(page, log_dir: str, prefix: str = "screenshot_erro") -> str:
    try:
        ts = int(time.time())
        dest = os.path.join(log_dir, f"{prefix}_{ts}.png")
        if page:
            page.screenshot(path=dest, full_page=True)
            _dbg(log_dir, f"Screenshot salvo em: {dest}")
        return dest
    except Exception as e:
        _dbg(log_dir, f"Falha ao salvar screenshot: {e!r}")
        return ""

# STUB para compat com app.py (não baixa nada do Drive).
def _ensure_local_zip_from_drive(log_dir: str, filename: str = "arquivos.zip") -> str | None:
    upload_dir = _get_upload_dir()
    os.makedirs(upload_dir, exist_ok=True)
    candidate = os.path.join(upload_dir, filename)
    if os.path.isfile(candidate):
        _dbg(log_dir, f"[stub] Usando ZIP local existente: {candidate}")
        return candidate
    _dbg(log_dir, f"[stub] {filename} não encontrado em {upload_dir}")
    return None

# ---------- helpers ----------
def _safe_click(page, locator, log_dir: str, what: str = "", attempts: int = 4, timeout: int = 6000) -> None:
    """
    Evita 'subtree intercepts pointer events':
      1) click normal
      2) ESC para fechar overlays + novo click
      3) click(force=True)
      4) JS el.click()
    """
    last_err = None
    for i in range(attempts):
        try:
            locator.wait_for(state="visible", timeout=timeout)
        except Exception as e:
            last_err = e
            _dbg(log_dir, f"[safe_click] {what} não visível (tentativa {i+1}/{attempts}): {e!r}")
            page.wait_for_timeout(200)
            continue

        try:
            locator.scroll_into_view_if_needed(timeout=1500)
        except Exception:
            pass

        try:
            locator.click(timeout=timeout)
            return
        except Exception as e1:
            last_err = e1
            _dbg(log_dir, f"[safe_click] Click normal falhou em '{what}': {e1!r}")

        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(120)
        except Exception:
            pass

        try:
            locator.click(timeout=timeout)
            return
        except Exception as e2:
            last_err = e2
            _dbg(log_dir, f"[safe_click] Click após ESC falhou '{what}': {e2!r}")

        try:
            locator.click(timeout=timeout, force=True)
            return
        except Exception as e3:
            last_err = e3
            _dbg(log_dir, f"[safe_click] Click force=True falhou '{what}': {e3!r}")

        try:
            handle = locator.element_handle(timeout=1200)
            if handle:
                page.evaluate("(el) => el.click()", handle)
                return
        except Exception as e4:
            last_err = e4
            _dbg(log_dir, f"[safe_click] JS click falhou '{what}': {e4!r}")

        page.wait_for_timeout(150)

    raise PWError(f"Não foi possível clicar em '{what}': {last_err!r}")

# ---------- login ----------
def _wait_and_fill_login(page, log_dir: str) -> None:
    # Campo de e-mail — forçamos o #usuario com placeholder 'E-mail'
    email_input = None
    try:
        loc = page.locator("input#usuario[placeholder='E-mail']")
        loc.wait_for(state="visible", timeout=7000)
        email_input = loc.first
    except Exception:
        # fallback tolerante
        for css in ("input#usuario[autocomplete='username']", "input#usuario", "input[placeholder='Usuário / E-mail']"):
            loc = page.locator(css)
            try:
                loc.wait_for(state="visible", timeout=3000)
                email_input = loc.first
                break
            except PWTimeout:
                continue
        if email_input is None:
            email_input = page.get_by_role("textbox", name=re.compile(r"E-mail|Usuário\s*/\s*E-mail", re.I))

    email_input.click()
    email_input.fill(LOGIN_EMAIL)

    # Campo de senha
    pwd_input = None
    for css in ("input#senha[type='password']", "input#senha", "input[autocomplete='current-password']", "input[placeholder='Senha']"):
        loc = page.locator(css)
        try:
            loc.wait_for(state="visible", timeout=4000)
            pwd_input = loc.first
            break
        except PWTimeout:
            continue
    if pwd_input is None:
        pwd_input = page.get_by_role("textbox", name=re.compile(r"Senha", re.I))

    pwd_input.click()
    pwd_input.fill(LOGIN_PASS)

    # Entrar
    try:
        btn = page.get_by_role("button", name=re.compile(r"^\s*entrar\s*$", re.I))
        _safe_click(page, btn, log_dir, what="Botão Entrar", attempts=3)
    except Exception:
        fallback = page.locator("button:has(span.mat-button-wrapper:has-text('Entrar'))").first
        _safe_click(page, fallback, log_dir, what="Botão Entrar (fallback)", attempts=3)

# ---------- selecionar unidade ----------
def _selecionar_unidade(page, log_dir: str, unidade_texto: str) -> None:
    # abre o menu do usuário (seta para baixo)
    try:
        user_dd = page.locator("i.material-icons.icone-seta-novo-user-data.no-margin-left", has_text="arrow_drop_down").first
        _safe_click(page, user_dd, log_dir, what="Dropdown do usuário", attempts=3)
    except Exception:
        alt_user = page.locator("i.material-icons", has_text="arrow_drop_down").first
        _safe_click(page, alt_user, log_dir, what="Dropdown do usuário (alt)", attempts=3)

    # abre o select de unidade
    try:
        seta = page.locator("div.mat-select-arrow-wrapper").first
        _safe_click(page, seta, log_dir, what="Abrir seletor de unidade", attempts=3)
    except Exception:
        seta2 = page.locator(".mat-select-arrow").first
        _safe_click(page, seta2, log_dir, what="Abrir seletor de unidade (alt)", attempts=3)

    # pesquisa
    try:
        search_input = page.locator("input.pesquisar-dropdrown[placeholder='Pesquisar']").first
        search_input.wait_for(state="visible", timeout=6000)
        search_input.click()
        search_input.fill(unidade_texto)
    except Exception:
        si = page.locator("input[placeholder='Pesquisar']").first
        si.wait_for(state="visible", timeout=6000)
        si.click()
        si.fill(unidade_texto)

    # seleciona a unidade
    opc = page.get_by_text(unidade_texto, exact=True)
    _safe_click(page, opc, log_dir, what=f"Selecionar unidade '{unidade_texto}'", attempts=3)

    # dá um tempinho para a sidebar atualizar
    page.wait_for_timeout(500)

# ---------- expandir 'Financeiro' e abrir NFS ----------
def _abrir_financeiro_e_nfs(page, log_dir: str) -> None:
    # Aguarda o item 'Financeiro' aparecer na sidebar
    try:
        finance_text = page.locator("span.nav-text", has_text=re.compile(r"^\s*Financeiro\s*$", re.I)).first
        finance_text.wait_for(state="visible", timeout=10000)
    except Exception as e:
        _dbg(log_dir, f"'Financeiro' não visível ainda: {e!r}")
        page.wait_for_timeout(600)

    # Clicar no ícone AO LADO de 'Financeiro' (preferimos o ícone do mesmo container)
    # XPath: acha o span 'Financeiro' e procura um <i.material-icons> com arrow_down/right no mesmo item
    icon_xpath = (
        "xpath=//span[contains(@class,'nav-text') and normalize-space()='Financeiro']"
        "/ancestor::*[self::li or self::div][1]"
        "//i[contains(@class,'material-icons') and (normalize-space()='keyboard_arrow_down' or normalize-space()='keyboard_arrow_right')]"
    )

    icon = page.locator(icon_xpath).first
    try:
        _safe_click(page, icon, log_dir, what="Abrir dropdown de 'Financeiro'", attempts=3)
    except Exception:
        # fallback: clicar no próprio texto 'Financeiro' pode alternar o dropdown
        fin = page.locator("span.nav-text", has_text=re.compile(r"^\s*Financeiro\s*$", re.I)).first
        _safe_click(page, fin, log_dir, what="Financeiro (fallback)", attempts=3)

    # Espera o subitem "Notas Fiscais de Serviço" ficar acessível
    nfs = page.locator("span.nav-text[data-cy='Notas Fiscais de Serviço']").first
    try:
        nfs.wait_for(state="visible", timeout=8000)
    except Exception:
        # fallback por texto
        nfs = page.get_by_text("Notas Fiscais de Serviço", exact=True)

    _safe_click(page, nfs, log_dir, what="Notas Fiscais de Serviço", attempts=4)

# ---------- abrir DatePicker (Hoje) ----------
def _abrir_datepicker(page, log_dir: str) -> None:
    try:
        btn_data = page.locator("button[data-cy='EFD-DatePickerBTN']").first
        _safe_click(page, btn_data, log_dir, what="Abrir DatePicker", attempts=3)
    except Exception:
        btn_today = page.locator("button:has(i.material-icons:has-text('today'))").first
        _safe_click(page, btn_today, log_dir, what="Abrir DatePicker (fallback)", attempts=3)

# ---------- função principal ----------
def run_rpa_enter_google_folder(base_dir: str, target_dir: str, log_dir: str) -> None:
    _dbg(log_dir, "Iniciando (abrindo navegador automaticamente).")
    _ensure_local_zip_from_drive(log_dir, filename="arquivos.zip")  # compat com app.py

    page = None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=HEADLESS,
                args=[
                    "--disable-gpu",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--start-maximized",
                    "--force-device-scale-factor=1",
                    "--high-dpi-support=1",
                    "--window-size=1920,1080",
                ],
            )
            context = browser.new_context(viewport=None)
            page = context.new_page()
            page.set_default_timeout(15000)

            page.goto(AZ_URL, wait_until="domcontentloaded", timeout=30000)

            _wait_and_fill_login(page, log_dir)

            # aguarda SPA estabilizar minimamente
            page.wait_for_load_state("domcontentloaded", timeout=20000)
            page.wait_for_timeout(600)

            # Seleciona a unidade
            _selecionar_unidade(page, log_dir, "BT TIJUC - Shopping Tijuca - 11")

            # Agora SIM: expande Financeiro e clica em Notas Fiscais de Serviço
            _abrir_financeiro_e_nfs(page, log_dir)

            # Abre o DatePicker
            _abrir_datepicker(page, log_dir)

            _save_report_json(
                log_dir,
                {
                    "ready": True,
                    "headers": ["etapa", "status", "detalhe"],
                    "rows": [
                        {"etapa": "login", "status": "ok", "detalhe": LOGIN_EMAIL},
                        {"etapa": "unidade", "status": "ok", "detalhe": "BT TIJUC - Shopping Tijuca - 11"},
                        {"etapa": "menu", "status": "ok", "detalhe": "Financeiro > Notas Fiscais de Serviço"},
                        {"etapa": "datepicker", "status": "ok", "detalhe": "Aberto"},
                    ],
                    "meta": {},
                },
            )

            if KEEP_OPEN:
                _dbg(log_dir, "KEEP_BROWSER_OPEN=1 — mantendo navegador aberto por 60s para inspeção.")
                page.wait_for_timeout(60000)

            context.close()
            browser.close()

    except Exception as e:
        _screenshot(page, log_dir, "screenshot_erro")
        _dbg(log_dir, f"Exceção no fluxo: {e!r}")
        _save_report_json(
            log_dir,
            {
                "ready": True,
                "headers": ["erro", "timestamp"],
                "rows": [{"erro": str(e), "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S")}],
                "meta": {},
            },
        )
