# filename: rpa.py
import os
import re
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# -----------------------------------------------------------
# .env
# -----------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

W12_URL = os.getenv("W12_URL", "https://evo5.w12app.com.br/#/acesso/bodytech/autenticacao")
W12_USER = os.getenv("W12_USER")
W12_PASS = os.getenv("W12_PASS")
HEADLESS = os.getenv("HEADLESS", "0").strip() in ("1", "true", "True")
WINDOWS = (os.name == "nt")

# -----------------------------------------------------------
# Compat: stub para o app usar a “verificação de zip local”
# -----------------------------------------------------------
def _ensure_local_zip_from_drive(log_dir: str, filename: str = "arquivos.zip"):
    if WINDOWS:
        root_dir = os.getenv("CNAB_LOCAL_DIR_WINDOWS", r"C:\AUTOMACAO\conciliacao\arquivos")
    else:
        root_dir = os.getenv("CNAB_LOCAL_DIR", "/home/felipe/Downloads/arquivos")
    path = os.path.join(root_dir, filename)
    if os.path.isfile(path):
        _log(f"[stub] Usando ZIP local existente: {path}")
        return path
    _log(f"[stub] NÃO ENCONTREI {path}")
    return None


# -----------------------------------------------------------
# Utilidades
# -----------------------------------------------------------
def _log(msg: str):
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    print(f"[{ts}] [rpa] {msg}", flush=True)


def _save_report_json(base_dir: str, payload: dict):
    try:
        path = os.path.join(base_dir, "last_report.json")
        payload = payload or {}
        payload.setdefault("ready", True)
        payload.setdefault("headers", [])
        payload.setdefault("rows", [])
        payload.setdefault("meta", {})
        payload.setdefault("updated_at", datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
        import json
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception as e:
        _log(f"Falha ao salvar last_report.json: {e!r}")


def _screenshot(page, prefix="screenshot_erro"):
    try:
        fname = f"{prefix}_{int(time.time())}.png"
        path = os.path.join(BASE_DIR, fname)
        page.screenshot(path=path, full_page=True)
        _log(f"Screenshot de erro salvo em: {path}")
    except Exception as e:
        _log(f"Falha ao salvar screenshot: {e!r}")


def _click_resiliente(locator, desc="elemento", timeout_normal=3000):
    """
    1) click normal
    2) se interceptado: click(force=True)
    3) fallback: DOM click via evaluate
    """
    try:
        locator.scroll_into_view_if_needed(timeout=timeout_normal)
    except Exception:
        pass

    try:
        locator.wait_for(state="visible", timeout=timeout_normal)
    except Exception:
        try:
            locator.wait_for(state="attached", timeout=timeout_normal)
        except Exception as e:
            _log(f"{desc}: não ficou visível/anexado a tempo: {e!r}")
            raise

    try:
        locator.click(timeout=timeout_normal)
        return
    except Exception as e1:
        msg = str(e1)
        _log(f"Falha ao clicar {desc} (normal): {e1!r}")

        if "intercepts pointer events" in msg or "not receivable at point" in msg or "element receives pointer-events" in msg:
            try:
                locator.click(force=True, timeout=timeout_normal)
                _log(f"{desc}: clique com force=True OK.")
                return
            except Exception as e2:
                _log(f"{desc}: force=True também falhou: {e2!r}")

        try:
            handle = locator.element_handle(timeout=1000)
            if handle:
                handle.scroll_into_view_if_needed(timeout=1000)
                locator.page.evaluate("(el) => el.click()", handle)
                _log(f"{desc}: clique via evaluate(el.click()) OK.")
                return
        except Exception as e3:
            _log(f"{desc}: fallback evaluate falhou: {e3!r}")

        raise e1


def _click_with_retry(page, locator, attempts=3, desc="elemento"):
    last = None
    for i in range(1, attempts + 1):
        try:
            if i > 1:
                page.keyboard.press("Escape")
                time.sleep(0.12)
            _click_resiliente(locator, desc=desc)
            return True
        except Exception as e:
            last = e
            _log(f"Falha ao clicar {desc} (tentativa {i}/{attempts}): {e!r}")
            time.sleep(0.2)
    if last:
        raise last
    return False


def _wait_label_data(page, expected_text: str, timeout_ms: int = 6000):
    try:
        page.wait_for_function(
            """(expected) => {
                const el = document.querySelector('span[data-cy="EFD-dataItem"]');
                if (!el) return false;
                return (el.textContent || '').trim().toLowerCase() === expected.toLowerCase();
            }""",
            arg=expected_text,
            timeout=timeout_ms
        )
        return True
    except Exception:
        return False


def _wait_post_login_ready(page, timeout_ms=15000):
    deadline = time.time() + timeout_ms / 1000.0
    while time.time() < deadline:
        try:
            url = page.url or ""
            if "#/acesso" not in url:
                return True
            if page.locator('i.material-icons.icone-seta-novo-user-data').first.count() > 0:
                return True
            if page.locator('span.nav-text', has_text="Financeiro").first.count() > 0:
                return True
            if page.locator('span.nav-text', has_text="Gerencial").first.count() > 0:
                return True
        except Exception:
            pass
        time.sleep(0.25)
    return False


def _abrir_financeiro_e_item(page, item_texto: str):
    """
    Expande especificamente o 'Financeiro' (pelo caret/anchor DENTRO do próprio item Financeiro)
    e clica no submenu 'item_texto' dentro desse mesmo <li>.
    """
    li_fin = page.locator('li:has(> a:has(span.nav-text:has-text("Financeiro")))').first
    li_fin.wait_for(state="visible", timeout=8000)

    caret = li_fin.locator(
        'i.material-icons',
        has_text=re.compile(r"(keyboard_arrow_(down|right)|expand_more|chevron_(right|down))", re.I)
    ).first

    anchor = li_fin.locator('> a:has(span.nav-text)').first
    submenu_item = li_fin.locator('li a:has(span.nav-text:has-text("' + item_texto + '"))').first

    for _ in range(5):
        if submenu_item.count() > 0 and submenu_item.is_visible():
            break
        try:
            li_fin.scroll_into_view_if_needed(timeout=1000)
        except Exception:
            pass

        if caret.count() > 0 and caret.is_visible():
            _click_resiliente(caret, desc="caret 'Financeiro'")
        else:
            _click_resiliente(anchor, desc="anchor 'Financeiro'")
        time.sleep(0.25)

    if submenu_item.count() == 0:
        submenu_item = page.locator('li a:has(span.nav-text:has-text("' + item_texto + '"))').first

    _click_with_retry(page, submenu_item, attempts=5, desc=f"menu '{item_texto}' (dentro de Financeiro)")
    time.sleep(0.25)
    return True


def _last_overlay(page):
    """Retorna o último overlay visível do Angular Material."""
    overlay = page.locator("div.cdk-overlay-pane").filter(has_not=page.locator(".cdk-overlay-pane[aria-hidden='true']")).last
    return overlay


def _open_overlay_and_get(page, opener_locator, desc="overlay", tries=3):
    """
    Clica no botão que abre o overlay e retorna o overlay visível.
    Reabre se não aparecer.
    """
    last_exc = None
    for i in range(1, tries + 1):
        try:
            _click_with_retry(page, opener_locator, desc=f"{desc} (abrir)")
            ov = _last_overlay(page)
            ov.wait_for(state="visible", timeout=2000)
            time.sleep(0.12)  # animação leve
            return ov
        except Exception as e:
            last_exc = e
            _log(f"Falha ao abrir {desc} (tentativa {i}/{tries}): {e!r}")
            time.sleep(0.2)
    if last_exc:
        raise last_exc
    raise PWTimeout(f"Não foi possível abrir {desc}")


# -----------------------------------------------------------
# Entrypoint chamado pelo app (/start_async)
# -----------------------------------------------------------
def run_rpa_enter_google_folder(base_dir: str, target_dir: str, log_dir: str):
    if not (W12_USER and W12_PASS):
        _log("Credenciais não configuradas no .env (W12_USER / W12_PASS). Abortando.")
        _save_report_json(base_dir, {"rows": [], "meta": {"status": "no-credentials"}})
        return

    _save_report_json(base_dir, {"rows": [], "meta": {"status": "starting"}})
    _log("Iniciando (abrindo navegador automaticamente).")

    page = None
    try:
        with sync_playwright() as p:
            args = [
                "--start-maximized",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ]
            browser = p.chromium.launch(headless=HEADLESS, args=args)
            context = browser.new_context(viewport=None)
            page = context.new_page()
            page.set_default_timeout(8000)

            # -------------------------
            # Login
            # -------------------------
            _log("Abrindo página de login...")
            page.goto(W12_URL, wait_until="domcontentloaded")

            _log("Preenchendo usuário...")
            user_input = page.locator('input#usuario')
            user_input.wait_for(state="visible")
            user_input.fill(W12_USER)

            _log("Preenchendo senha...")
            pass_input = page.locator('input#senha')
            pass_input.wait_for(state="visible")
            pass_input.fill(W12_PASS)

            _log("Clicando em Entrar...")
            btn_entrar = page.get_by_role("button", name=re.compile(r"^\s*Entrar\s*$", re.I)).first
            if btn_entrar.count() == 0:
                btn_entrar = page.locator('span.mat-button-wrapper', has_text=re.compile(r"^\s*Entrar\s*$", re.I)).first
            _click_with_retry(page, btn_entrar, desc="Entrar")

            if not _wait_post_login_ready(page, timeout_ms=15000):
                _log("Pós-login demorou; aguardando mais 5s…")
                if not _wait_post_login_ready(page, timeout_ms=5000):
                    raise PWTimeout("Falha ao detectar área autenticada após o login.")
            _log(f"Pós-login OK. URL atual: {page.url}")

            # -------------------------
            # Seleciona unidade "BT TIJUC - Shopping Tijuca - 11"
            # -------------------------
            _log("Abrindo seletor de unidade...")
            seta_usuario = page.locator('i.material-icons.icone-seta-novo-user-data').first
            if seta_usuario.count():
                _click_with_retry(page, seta_usuario, desc="seta do usuário (unidade)")

            _log("Abrindo lista de unidades (mat-select)...")
            mat_arrow = page.locator("div.mat-select-arrow-wrapper").first
            _click_with_retry(page, mat_arrow, desc="seta do mat-select da unidade")

            _log("Pesquisando 'shopping tijuca'...")
            search_un = page.locator('input.pesquisar-dropdrown').first
            search_un.wait_for(state="visible")
            search_un.fill("shopping tijuca")
            time.sleep(0.3)

            _log("Selecionando 'BT TIJUC - Shopping Tijuca - 11'...")
            alvo = page.get_by_text(re.compile(r"BT\s+TIJUC\s+-\s+Shopping\s+Tijuca\s+-\s+11", re.I)).first
            _click_with_retry(page, alvo, desc="unidade BT TIJUC - Shopping Tijuca - 11")

            # -------------------------
            # Menu: Financeiro -> Notas Fiscais de Serviço
            # -------------------------
            _log("Abrindo menu 'Financeiro' e clicando em 'Notas Fiscais de Serviço'…")
            _abrir_financeiro_e_item(page, "Notas Fiscais de Serviço")

            # Aguarda a tela carregar o botão de Data
            page.locator('button[data-cy="EFD-DatePickerBTN"]').first.wait_for(state="visible", timeout=15000)

            # -------------------------
            # Data: Ontem + Aplicar
            # -------------------------
            _log("Abrindo seletor de data…")
            btn_data = page.locator('button[data-cy="EFD-DatePickerBTN"]').first
            _click_with_retry(page, btn_data, desc="botão Data:")

            _log("Selecionando 'Ontem'…")
            ontem_item = page.get_by_text(re.compile(r"^\s*Ontem\s*$", re.I)).first
            _click_with_retry(page, ontem_item, desc="item Ontem")

            _log("Aplicando filtro de data…")
            aplicar_data = page.locator('button[data-cy="EFD-ApplyButton"]').first
            _click_with_retry(page, aplicar_data, desc="APLICAR (data)")

            if _wait_label_data(page, "Ontem", timeout_ms=5000):
                _log("Rótulo de Data confirmou 'Ontem'.")
            else:
                _log("Aviso: rótulo 'Data:' não confirmou 'Ontem' no tempo esperado.")

            # -------------------------
            # Exibir por + APLICAR
            # -------------------------
            _log("Abrindo 'Exibir por:'…")
            page.keyboard.press("Escape")
            btn_exibir = page.locator('button[data-cy="abrirFiltro"]').first

            overlay = _open_overlay_and_get(page, btn_exibir, desc="Exibir por overlay")

            data_item = overlay.get_by_text(re.compile(r"^\s*Data lançamento\s*$", re.I)).first
            picked_icon = overlay.locator(
                'div.mat-list-item-content:has-text("Data lançamento") mat-icon.picked, '
                'div.mat-list-item-content:has-text("Data lançamento") .mat-icon.picked'
            )
            ja_marcado = picked_icon.count() > 0
            if ja_marcado:
                _log("‘Data lançamento’ já está marcado — indo direto no APLICAR.")
            else:
                _log("Selecionando 'Data lançamento'…")
                _click_with_retry(page, data_item, desc="Data lançamento")
                try:
                    overlay.wait_for(state="visible", timeout=1200)
                except Exception:
                    _log("Overlay fechou após selecionar — reabrindo para clicar APLICAR…")
                    overlay = _open_overlay_and_get(page, btn_exibir, desc="Exibir por overlay (reabrir)")

            _log("Aplicando 'Exibir por:'…")
            aplicar_exibir = overlay.locator('button[data-cy="AplicarFiltro"]').first
            if not aplicar_exibir.count():
                aplicar_exibir = page.get_by_role("button", name=re.compile(r"^\s*APLICAR\s*$", re.I)).first
            _click_with_retry(page, aplicar_exibir, desc="APLICAR (Exibir por)")

            try:
                overlay.wait_for(state="hidden", timeout=1500)
            except Exception:
                pass
            time.sleep(0.6)

            # -------------------------
            # + FILTROS -> Tributação -> Todos + “Não usar - 12.34.56” -> Aplicar
            # -------------------------
            _log("Abrindo '+ FILTROS'…")
            btn_mais_filtros = page.get_by_role("button", name=re.compile(r"^\s*\+\s*FILTROS\s*$", re.I)).first
            _click_with_retry(page, btn_mais_filtros, desc="+ FILTROS")

            _log("Abrindo 'Tributação'…")
            btn_tribut = page.locator('button.simula-mat-menu', has_text=re.compile(r"^\s*Tributação\s*$", re.I)).first
            if not btn_tribut.count():
                btn_tribut = page.get_by_text(re.compile(r"^\s*Tributação\s*$", re.I)).first
            _click_with_retry(page, btn_tribut, desc="Tributação")

            _log("Marcando 'Todos' …")
            todos_item = page.get_by_text(re.compile(r"^\s*Todos\s*$", re.I)).first
            _click_with_retry(page, todos_item, desc="Todos")

            _log("Selecionando 'Não usar - 12.34.56' …")
            nao_usar_item = page.get_by_text(re.compile(r"^\s*Não usar\s*-\s*12\.34\.56\s*$", re.I)).first
            _click_with_retry(page, nao_usar_item, desc="Não usar - 12.34.56")

            _log("Aplicando filtro 'Tributação'…")
            overlay2 = _last_overlay(page)
            aplicar_filtro = overlay2.locator('button[data-cy="AplicarFiltro"]').first if overlay2.count() else page.locator('button[data-cy="AplicarFiltro"]').first
            if not aplicar_filtro.count():
                aplicar_filtro = page.get_by_role("button", name=re.compile(r"^\s*APLICAR\s*$", re.I)).first
            _click_with_retry(page, aplicar_filtro, desc="APLICAR (Tributação)")

            # Refresh curto e clicar SelecionarTodos
            _log("Aguardando refresh após APLICAR (Tributação) e exibindo checkbox 'SelecionarTodos'…")
            try:
                page.wait_for_function(
                    """() => !!document.querySelector('[data-cy="SelecionarTodosCheck"]')""",
                    timeout=10000
                )
            except Exception:
                _log("Aviso: checkbox 'SelecionarTodosCheck' demorou. Tentando localizar direto…")

            time.sleep(0.4)
            selecionar_todos = page.locator('[data-cy="SelecionarTodosCheck"]').first
            if selecionar_todos.count():
                _click_with_retry(page, selecionar_todos, desc="SelecionarTodos (checkbox)")
            else:
                _log("Checkbox 'SelecionarTodosCheck' não encontrado; fallback: marcar primeira linha.")
                primeiro_check = page.locator('[data-cy="SelecionarUmCheck"]').first
                _click_with_retry(page, primeiro_check, desc="checkbox primeira linha (fallback)")

            # -------------------------
            # ENVIAR
            # -------------------------
            _log("Clicando 'ENVIAR'…")
            btn_enviar = page.get_by_role("button", name=re.compile(r"^\s*ENVIAR\s*$", re.I)).first
            btn_enviar.wait_for(state="visible", timeout=8000)
            _click_with_retry(page, btn_enviar, desc="ENVIAR")

            # -------------------------
            # Calendário: selecionar ontem
            # -------------------------
            _log("Abrindo calendário (ícone ao lado do campo)…")
            cal_icon = page.get_by_role("button", name=re.compile(r"Open calendar", re.I)).first
            _click_with_retry(page, cal_icon, desc="ícone calendário")

            ontem = datetime.now() - timedelta(days=1)
            dia = str(ontem.day)

            _log(f"Selecionando dia '{dia}' no calendário…")
            dia_cell = page.locator(f'//td[normalize-space()="{dia}"]').first
            if not dia_cell.count():
                dia_cell = page.get_by_role("gridcell", name=re.compile(rf"^\s*{re.escape(dia)}\s*$")).first
            _click_with_retry(page, dia_cell, desc=f"dia {dia}")

            # >>> Pausa final para inspeção antes de fechar o navegador <<<
            _log("Pausando 5s para conferência visual antes de fechar o navegador…")
            time.sleep(5)

            _log("Fluxo finalizado até a seleção da data de ontem no calendário.")
            _save_report_json(base_dir, {
                "rows": [],
                "meta": {"status": "ok", "finished_at": datetime.now().isoformat()}
            })

    except PWTimeout as te:
        _log(f"Timeout em alguma etapa: {te}")
        try:
            if page:
                _screenshot(page)
        except Exception:
            pass
        _save_report_json(base_dir, {"rows": [], "meta": {"status": "timeout", "error": str(te)}})

    except Exception as e:
        _log(f"Exceção no fluxo: {e!r}")
        try:
            if page:
                _screenshot(page)
        except Exception:
            pass
        _save_report_json(base_dir, {"rows": [], "meta": {"status": "error", "error": str(e)}})
