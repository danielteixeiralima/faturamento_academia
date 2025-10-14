# filename: rpa.py
# RPA para o portal W12 (Bodytech)
# Fluxo: login → escolher unidade → Financeiro > Notas Fiscais de Serviço
#        → Data: Ontem → APLICAR → (aguarda refresh curto)
#        → Exibir por: Data lançamento → APLICAR → (aguarda refresh curto)
#        → + FILTROS > Tributação > (Todos + "Não usar - 12.34.56") → APLICAR → (aguarda refresh curto)
#        → selecionar 1 linha → ENVIAR → abrir calendário (selecionar ontem).
#
# Compatível com app.py:
#   from rpa import run_rpa_enter_google_folder, _ensure_local_zip_from_drive, _ensure_local_zip_from_drive

import os
import re
import time
from datetime import datetime, timedelta

from playwright.sync_api import (
    sync_playwright,
    TimeoutError as PWTimeout,
    Error as PWError,
)

AZ_URL = "https://evo5.w12app.com.br/#/acesso/bodytech/autenticacao"

# Credenciais (pode sobrescrever via .env)
LOGIN_EMAIL = os.getenv("W12_EMAIL", "inova.ia@sacavalcante.com.br")
LOGIN_PASS = os.getenv("W12_PASS", "omega536")

# Execução
HEADLESS = (os.getenv("HEADLESS", "0") == "1")
KEEP_OPEN = (os.getenv("KEEP_BROWSER_OPEN", "0") == "1")


# =====================================================================
# Utilitários básicos
# =====================================================================
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


# =====================================================================
# Helpers Playwright
# =====================================================================
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
            page.wait_for_timeout(150)
            continue

        try:
            locator.scroll_into_view_if_needed(timeout=1200)
        except Exception:
            pass

        try:
            locator.click(timeout=timeout)
            return
        except Exception as e1:
            last_err = e1
            _dbg(log_dir, f"[safe_click] Click normal falhou '{what}': {e1!r}")

        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(100)
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
            handle = locator.element_handle(timeout=800)
            if handle:
                page.evaluate("(el) => el.click()", handle)
                return
        except Exception as e4:
            last_err = e4
            _dbg(log_dir, f"[safe_click] JS click falhou '{what}': {e4!r}")

        page.wait_for_timeout(120)

    raise PWError(f"Não foi possível clicar em '{what}': {last_err!r}")


def _esperar_pagina_atualizar(page, log_dir: str, timeout_ms: int = 6500) -> None:
    """
    Aguarda a página/SPA estabilizar após uma ação que recarrega dados, mas sem travar muito tempo:
      - tenta networkidle curto
      - espera sumir overlays/spinners
      - garante que o botão 'Exibir por:' volte a ficar visível
    """
    t0 = time.time()

    # 1) networkidle curto
    try:
        page.wait_for_load_state("networkidle", timeout=min(4000, timeout_ms))
    except Exception:
        pass

    # 2) overlays/spinners sumirem (cada um com tolerância curta)
    for sel in [
        ".cdk-overlay-backdrop.cdk-overlay-backdrop-showing",
        ".mat-progress-spinner",
        ".mat-progress-bar",
        "mat-progress-bar",
    ]:
        left = max(500, int(timeout_ms - (time.time() - t0) * 1000))
        if left <= 0:
            break
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                loc.first.wait_for(state="hidden", timeout=min(2500, left))
        except Exception:
            page.wait_for_timeout(100)

    # 3) confirma um elemento da tela voltou
    left = max(400, int(timeout_ms - (time.time() - t0) * 1000))
    if left > 0:
        try:
            page.locator("button[data-cy='abrirFiltro']").first.wait_for(state="visible", timeout=left)
        except Exception:
            pass

    page.wait_for_timeout(300)  # folga bem curta


# =====================================================================
# Passos do fluxo
# =====================================================================
def _wait_and_fill_login(page, log_dir: str) -> None:
    # Campo de e-mail — focar no #usuario com placeholder 'E-mail'
    email_input = None
    try:
        loc = page.locator("input#usuario[placeholder='E-mail']")
        loc.wait_for(state="visible", timeout=7000)
        email_input = loc.first
    except Exception:
        # fallbacks tolerantes
        for css in ("input#usuario[autocomplete='username']", "input#usuario", "input[placeholder='Usuário / E-mail']"):
            loc = page.locator(css)
            try:
                loc.wait_for(state="visible", timeout=2500)
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
            loc.wait_for(state="visible", timeout=3000)
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
        search_input.wait_for(state="visible", timeout=5000)
        search_input.click()
        search_input.fill(unidade_texto)
    except Exception:
        si = page.locator("input[placeholder='Pesquisar']").first
        si.wait_for(state="visible", timeout=5000)
        si.click()
        si.fill(unidade_texto)

    # seleciona a unidade
    opc = page.get_by_text(unidade_texto, exact=True)
    _safe_click(page, opc, log_dir, what=f"Selecionar unidade '{unidade_texto}'", attempts=3)

    page.wait_for_timeout(400)  # aguarda sidebar atualizar


def _abrir_financeiro_e_nfs(page, log_dir: str) -> None:
    # Localiza 'Financeiro'
    try:
        page.locator("span.nav-text", has_text=re.compile(r"^\s*Financeiro\s*$", re.I)).first.wait_for(
            state="visible", timeout=9000
        )
    except Exception as e:
        _dbg(log_dir, f"'Financeiro' não visível ainda: {e!r}")
        page.wait_for_timeout(400)

    # Clica no ícone expandir do mesmo container do 'Financeiro'
    icon_xpath = (
        "xpath=//span[contains(@class,'nav-text') and normalize-space()='Financeiro']"
        "/ancestor::*[self::li or self::div][1]"
        "//i[contains(@class,'material-icons') and (normalize-space()='keyboard_arrow_down' or normalize-space()='keyboard_arrow_right')]"
    )
    icon = page.locator(icon_xpath).first
    try:
        _safe_click(page, icon, log_dir, what="Abrir dropdown de 'Financeiro'", attempts=3)
    except Exception:
        fin = page.locator("span.nav-text", has_text=re.compile(r"^\s*Financeiro\s*$", re.I)).first
        _safe_click(page, fin, log_dir, what="Financeiro (fallback)", attempts=3)

    # Clica no subitem NFS
    nfs = page.locator("span.nav-text[data-cy='Notas Fiscais de Serviço']").first
    try:
        nfs.wait_for(state="visible", timeout=7000)
    except Exception:
        nfs = page.get_by_text("Notas Fiscais de Serviço", exact=True)

    _safe_click(page, nfs, log_dir, what="Notas Fiscais de Serviço", attempts=4)


def _abrir_datepicker(page, log_dir: str) -> None:
    try:
        btn_data = page.locator("button[data-cy='EFD-DatePickerBTN']").first
        _safe_click(page, btn_data, log_dir, what="Abrir DatePicker", attempts=3)
    except Exception:
        btn_today = page.locator("button:has(i.material-icons:has-text('today'))").first
        _safe_click(page, btn_today, log_dir, what="Abrir DatePicker (fallback)", attempts=3)


def _selecionar_ontem_na_lista(page, log_dir: str) -> None:
    """
    Dentro do menu de datas (aquele que mostra Hoje / Ontem / etc), clicar em 'Ontem'.
    """
    ontem = page.get_by_text(re.compile(r"^\s*Ontem\s*$", re.I))
    _safe_click(page, ontem, log_dir, what="Data: Ontem (lista)", attempts=4)


def _aplicar_data_range(page, log_dir: str) -> None:
    """
    Clica no botão 'APLICAR' do seletor de datas principal (data-cy='EFD-ApplyButton').
    """
    aplicar = page.locator("button[data-cy='EFD-ApplyButton']").first
    if aplicar.count() == 0:
        aplicar = page.locator("button:has-text('APLICAR')").first
    _safe_click(page, aplicar, log_dir, what="Aplicar data (EFD-ApplyButton)", attempts=3)


def _set_exibir_por_data_lancamento_and_apply(page, log_dir: str) -> None:
    """
    Clicar 'Exibir por:' → clicar no item Data lançamento (o mesmo bloco que exibe o 'done')
    → clicar APLICAR (data-cy='AplicarFiltro').
    """
    # Abrir "Exibir por:"
    btn = page.locator("button[data-cy='abrirFiltro']").first
    _safe_click(page, btn, log_dir, what="Abrir 'Exibir por:'", attempts=3)

    # Item "Data lançamento" exatamente no bloco da lista
    item_container = page.locator(
        "div.mat-list-item-content",
    ).filter(has=page.locator("div.mat-list-text", has_text=re.compile(r"^\s*Data lançamento\s*$", re.I))).first
    _safe_click(page, item_container, log_dir, what="Exibir por: Data lançamento (item da lista)", attempts=3)

    # APLICAR
    aplicar = page.locator("button[data-cy='AplicarFiltro']").first
    if aplicar.count() == 0:
        aplicar = page.locator("button:has-text('APLICAR')").first
    _safe_click(page, aplicar, log_dir, what="Aplicar 'Exibir por:'", attempts=3)


def _abrir_filtros_e_aplicar_tributacao(page, log_dir: str) -> None:
    """
    + FILTROS → Tributação → marcar 'Todos' e 'Não usar - 12.34.56' → APLICAR
    """
    # + FILTROS
    plus = page.locator("button.mat-button:has-text('+ FILTROS'), button:has-text('+ FILTROS')").first
    _safe_click(page, plus, log_dir, what="+ FILTROS", attempts=3)

    # Tributação
    trib = page.locator("button.simula-mat-menu", has_text=re.compile(r"^\s*Tributação\s*$", re.I)).first
    _safe_click(page, trib, log_dir, what="Filtro: Tributação", attempts=3)

    # Itens dentro do multiselect: 'Todos' e 'Não usar - 12.34.56'
    todos = page.get_by_text(re.compile(r"^\s*Todos\s*$", re.I))
    _safe_click(page, todos, log_dir, what="Tributação: Todos", attempts=3)

    nao_usar = page.get_by_text(re.compile(r"^\s*Não usar\s*-\s*12\.34\.56\s*$", re.I))
    _safe_click(page, nao_usar, log_dir, what="Tributação: Não usar - 12.34.56", attempts=3)

    # APLICAR do multiselect de Tributação
    aplicar = page.locator("button[data-cy='AplicarFiltro']").first
    if aplicar.count() == 0:
        aplicar = page.locator("button#btn:has-text('APLICAR'), button:has-text('APLICAR')").first
    _safe_click(page, aplicar, log_dir, what="Aplicar filtro Tributação", attempts=3)


def _selecionar_primeira_linha(page, log_dir: str) -> None:
    """
    Seleciona um registro: checkbox com data-cy='SelecionarUmCheck'
    """
    chk_label = page.locator("mat-checkbox[data-cy='SelecionarUmCheck'] label").first
    if chk_label.count() == 0:
        chk_label = page.locator("mat-checkbox label.mat-checkbox-layout").first
    _safe_click(page, chk_label, log_dir, what="Selecionar 1 linha", attempts=3)


def _clicar_enviar(page, log_dir: str) -> None:
    enviar = page.locator("button[type='submit']", has_text=re.compile(r"^\s*ENVIAR\s*$", re.I)).first
    if enviar.count() == 0:
        enviar = page.locator("button:has-text('ENVIAR')").first
    _safe_click(page, enviar, log_dir, what="ENVIAR", attempts=3)


def _abrir_icon_calendar(page, log_dir: str) -> None:
    """
    Abre o datepicker (ícone do calendário) depois do ENVIAR.
    """
    cal = page.locator("button.mat-icon-button[aria-label='Open calendar']").first
    if cal.count() == 0:
        cal = page.locator("button.mat-icon-button", has=page.locator("svg.mat-datepicker-toggle-default-icon")).first
    _safe_click(page, cal, log_dir, what="Abrir ícone do calendário", attempts=3)


def _selecionar_ontem_no_datepicker(page, log_dir: str) -> None:
    """
    No calendário do Angular Material aberto, selecionar 'ontem'.
    Estratégia principal:
      - Encontrar a célula 'Hoje' (classe .mat-calendar-body-today) e clicar a célula anterior.
    Fallback:
      - Calcular dia de ontem e clicar o número do dia (evitando células 'disabled').
    """
    # aguarda calendário
    matcal = page.locator(".mat-calendar, mat-calendar")
    matcal.wait_for(state="visible", timeout=7000)

    # 1) Tenta partir do "Hoje"
    try:
        handle = page.locator(".mat-calendar-body-today").first.element_handle(timeout=1800)
        if handle:
            page.evaluate(
                """
                (todayContent) => {
                  const td = todayContent.closest('td.mat-calendar-body-cell');
                  if (!td) return false;
                  let target = td.previousElementSibling;
                  if (!target) {
                    const tr = td.parentElement;
                    if (!tr) return false;
                    const prevTr = tr.previousElementSibling;
                    if (!prevTr) return false;
                    const tds = Array.from(prevTr.querySelectorAll('td.mat-calendar-body-cell'));
                    if (!tds.length) return false;
                    target = tds[tds.length - 1];
                  }
                  if (target.classList.contains('mat-calendar-body-disabled')) {
                    return false;
                  }
                  const btn = target.querySelector('.mat-calendar-body-cell-content');
                  if (btn) { btn.click(); return true; }
                  return false;
                }
                """,
                handle,
            )
            page.wait_for_timeout(250)
            return
    except Exception:
        pass

    # 2) Fallback: clicar pelo número do dia (não 'disabled')
    ontem = datetime.now() - timedelta(days=1)
    day = str(ontem.day)

    candidates = page.locator(
        f"td.mat-calendar-body-cell:not(.mat-calendar-body-disabled) "
        f".mat-calendar-body-cell-content:text-is('{day}')"
    )
    if candidates.count() > 0:
        _safe_click(page, candidates.nth(0), log_dir, what=f"Selecionar dia {day} (ontem)", attempts=3)
        page.wait_for_timeout(250)
        return

    # 3) Último recurso: fecha com ESC (nada selecionado)
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass


# =====================================================================
# Função principal chamada pelo app.py
# =====================================================================
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
            page.set_default_timeout(12000)

            page.goto(AZ_URL, wait_until="domcontentloaded", timeout=30000)

            # Login
            _wait_and_fill_login(page, log_dir)
            page.wait_for_load_state("domcontentloaded")
            page.wait_for_timeout(400)

            # Unidade
            _selecionar_unidade(page, log_dir, "BT TIJUC - Shopping Tijuca - 11")

            # Menu
            _abrir_financeiro_e_nfs(page, log_dir)

            # DatePicker > Ontem > APLICAR → AGUARDAR REFRESH CURTO
            _abrir_datepicker(page, log_dir)
            _selecionar_ontem_na_lista(page, log_dir)
            _aplicar_data_range(page, log_dir)
            _esperar_pagina_atualizar(page, log_dir, timeout_ms=6500)

            # Exibir por: Data lançamento > APLICAR → AGUARDAR REFRESH CURTO
            _set_exibir_por_data_lancamento_and_apply(page, log_dir)
            _esperar_pagina_atualizar(page, log_dir, timeout_ms=6500)

            # + FILTROS > Tributação > Todos + Não usar - 12.34.56 > APLICAR → AGUARDAR REFRESH CURTO
            _abrir_filtros_e_aplicar_tributacao(page, log_dir)
            _esperar_pagina_atualizar(page, log_dir, timeout_ms=6500)

            # Seleciona 1 linha
            _selecionar_primeira_linha(page, log_dir)

            # ENVIAR
            _clicar_enviar(page, log_dir)

            # Abrir calendário e selecionar ontem (segunda etapa)
            _abrir_icon_calendar(page, log_dir)
            _selecionar_ontem_no_datepicker(page, log_dir)

            # Report
            _save_report_json(
                log_dir,
                {
                    "ready": True,
                    "headers": ["etapa", "status", "detalhe"],
                    "rows": [
                        {"etapa": "login", "status": "ok", "detalhe": LOGIN_EMAIL},
                        {"etapa": "unidade", "status": "ok", "detalhe": "BT TIJUC - Shopping Tijuca - 11"},
                        {"etapa": "menu", "status": "ok", "detalhe": "Financeiro > NFS"},
                        {"etapa": "data", "status": "ok", "detalhe": "Ontem + Aplicar (espera curta)"},
                        {"etapa": "exibir por", "status": "ok", "detalhe": "Data lançamento + Aplicar (espera curta)"},
                        {"etapa": "filtro", "status": "ok", "detalhe": "Tributação: Todos + Não usar - 12.34.56 + Aplicar (espera curta)"},
                        {"etapa": "seleção", "status": "ok", "detalhe": "1 linha marcada"},
                        {"etapa": "enviar", "status": "ok", "detalhe": "Clique realizado"},
                        {"etapa": "datepicker envio", "status": "ok", "detalhe": "Ontem selecionado"},
                    ],
                    "meta": {},
                },
            )

            if KEEP_OPEN:
                _dbg(log_dir, "KEEP_BROWSER_OPEN=1 — mantendo navegador aberto por 45s para inspeção.")
                page.wait_for_timeout(45000)

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
