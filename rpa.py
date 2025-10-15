# rpa.py
import os
import re
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# =========================
# Configurações
# =========================
TENANT = "bodytech"
BASE_URL_LOGIN = f"https://evo5.w12app.com.br/#/acesso/{TENANT}/login"
APP_HOME_URL = "https://evo5.w12app.com.br/#/app/bodytech/-2/inicio/geral"

SCREENSHOT_DIR = Path.home() / "Downloads" / "faturamento_academia"
SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_TIMEOUT = 6000
SHORT_TIMEOUT = 3000
VERY_SHORT_TIMEOUT = 1500
FAST_TIMEOUT = 1200

UNIDADE_ALVO_REGEX = re.compile(r"^\s*BT TIJUC\s*-\s*Shopping Tijuca\s*-\s*11\s*$", re.IGNORECASE)

# =========================
# Utilidades
# =========================
def log(msg: str) -> None:
    print(f"[rpa] {msg}", flush=True)

def fmt_date_br(d: datetime) -> str:
    return d.strftime("%d/%m/%Y")

def previous_business_day(ref: datetime | None = None) -> datetime:
    if ref is None:
        ref = datetime.now()
    d = ref.date() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return datetime(d.year, d.month, d.day)

async def wait_loading_quiet(page, fast: bool = False) -> None:
    try:
        await page.wait_for_load_state("networkidle", timeout=(1500 if fast else DEFAULT_TIMEOUT))
    except PlaywrightTimeout:
        pass
    for sel in [
        "evo-loading",
        ".mat-progress-bar",
        ".cdk-overlay-backdrop",
        ".cdk-global-overlay-wrapper .mat-progress-spinner",
    ]:
        try:
            await page.wait_for_selector(sel, state="detached", timeout=(FAST_TIMEOUT if fast else SHORT_TIMEOUT))
        except PlaywrightTimeout:
            try:
                await page.wait_for_selector(sel, state="hidden", timeout=VERY_SHORT_TIMEOUT)
            except PlaywrightTimeout:
                pass

async def safe_click(loc, desc: str, force: bool = False, timeout: int = SHORT_TIMEOUT) -> bool:
    try:
        await loc.wait_for(state="visible", timeout=timeout)
        await loc.click(force=force, timeout=timeout)
        log(f"{desc}: clique executado")
        return True
    except PlaywrightTimeout:
        log(f"{desc}: timeout ao clicar")
        return False
    except Exception as e:
        log(f"{desc}: erro ao clicar: {e}")
        return False

async def click_with_retries(loc, desc: str, attempts: int = 3, force_last: bool = True, timeout: int = SHORT_TIMEOUT) -> bool:
    for i in range(1, attempts + 1):
        ok = await safe_click(loc, f"{desc} (tentativa {i}/{attempts})", force=False, timeout=timeout)
        if ok:
            return True
    if force_last:
        try:
            await loc.wait_for(state="visible", timeout=timeout)
            await loc.click(force=True, timeout=timeout)
            log(f"{desc}: clique com force=True executado")
            return True
        except Exception as e:
            log(f"{desc}: clique com force=True falhou: {e}")
    return False

def ensure_env() -> tuple[str, str]:
    load_dotenv(override=True)
    user = os.getenv("W12_USER", "").strip().lower()
    pwd = os.getenv("W12_PASS", "").strip().lower()
    if not user or not pwd:
        raise RuntimeError("Credenciais não configuradas no .env (W12_USER e W12_PASS).")
    return user, pwd

def _corrigir_url_tenant(url: str) -> str:
    if "/acesso//" in url:
        return url.replace("/acesso//", f"/acesso/{TENANT}/")
    return re.sub(r"/acesso/[^/]+/", f"/acesso/{TENANT}/", url)

async def garantir_tenant_bodytech(page, max_correcoes: int = 5) -> None:
    for i in range(max_correcoes):
        url = page.url
        if f"/acesso/{TENANT}/" in url:
            return
        corr = _corrigir_url_tenant(url)
        if corr != url:
            log(f"Ajustando tenant na URL (tentativa {i+1}/{max_correcoes}): {url} -> {corr}")
            await page.goto(corr, wait_until="domcontentloaded")
            await asyncio.sleep(0.2)
        else:
            return

async def _forcar_url_via_barra(page, url: str) -> None:
    try:
        await page.keyboard.press("Control+L")
        await asyncio.sleep(0.05)
        await page.keyboard.type(url, delay=4)
        await page.keyboard.press("Enter")
        log("URL ajustada via barra do navegador")
    except Exception as e:
        log(f"Fallback da barra de URL falhou: {e}")

async def tenant_watchdog(page, stop_event: asyncio.Event) -> None:
    try:
        while not stop_event.is_set():
            url = page.url
            if "/app/bodytech/" in url:
                stop_event.set()
                break
            if "/acesso/evo5/" in url or "/acesso//" in url:
                corr = _corrigir_url_tenant(url)
                log(f"Watchdog corrigindo URL: {url} -> {corr}")
                try:
                    await page.goto(corr, wait_until="domcontentloaded")
                except Exception:
                    pass
            await asyncio.sleep(0.15)
    except Exception:
        pass

async def find_first_visible(page, selectors: list[str], timeout_each: int = 3000):
    for css in selectors:
        loc = page.locator(css).first
        try:
            await loc.wait_for(state="visible", timeout=timeout_each)
            return loc
        except Exception:
            continue
    return None

async def wait_for_login_fields(page, max_wait_ms: int = 12000):
    email_selectors = [
        "input#usuario",
        "input[name='usuario']",
        "input[name='email']",
        "input[formcontrolname='usuario']",
        "input[formcontrolname='email']",
        "input[type='email']",
        "input[placeholder*='E-mail' i]",
        "input[placeholder*='Email' i]",
    ]
    pass_selectors = [
        "input#senha",
        "input[name='senha']",
        "input[formcontrolname='senha']",
        "input[type='password']",
        "input[placeholder*='Senha' i]",
    ]

    end_time = datetime.now().timestamp() + (max_wait_ms / 1000.0)
    email_loc = None
    pass_loc = None
    while datetime.now().timestamp() < end_time:
        await garantir_tenant_bodytech(page, max_correcoes=1)

        if "/acesso/bodytech/" not in page.url:
            await _forcar_url_via_barra(page, BASE_URL_LOGIN)

        if email_loc is None:
            email_loc = await find_first_visible(page, email_selectors, timeout_each=800)
        if pass_loc is None:
            pass_loc = await find_first_visible(page, pass_selectors, timeout_each=800)

        if email_loc and pass_loc:
            return email_loc, pass_loc

        await asyncio.sleep(0.2)

    raise PlaywrightTimeout("Campos de login não ficaram visíveis a tempo.")

# =========================
# Etapas do fluxo
# =========================
async def do_login(page, user: str, pwd: str) -> None:
    log("Abrindo página de login")
    await page.goto(BASE_URL_LOGIN, wait_until="domcontentloaded", timeout=20000)

    stop_wd = asyncio.Event()
    wd_task = asyncio.create_task(tenant_watchdog(page, stop_wd))

    try:
        email_input, pass_input = await wait_for_login_fields(page, max_wait_ms=15000)
        log("Página de login/autenticação detectada — campos visíveis")

        entrar_btn = page.get_by_role("button", name=re.compile(r"^\s*Entrar\s*$", re.IGNORECASE)).first
        try:
            await entrar_btn.wait_for(state="visible", timeout=3000)
        except PlaywrightTimeout:
            entrar_btn = page.locator("button", has_text=re.compile(r"^\s*Entrar\s*$", re.IGNORECASE)).first

        await email_input.fill("")
        await email_input.fill(user)
        await pass_input.fill("")
        await pass_input.fill(pwd)

        log(f"[DEBUG] Usuário no campo: '{await email_input.input_value()}'")
        log(f"[DEBUG] Senha no campo: '{await pass_input.input_value()}'")

        if not await click_with_retries(entrar_btn, "Entrar", attempts=2, timeout=DEFAULT_TIMEOUT):
            raise RuntimeError("Falha ao clicar em Entrar")

        await asyncio.sleep(0.4)

        try:
            if "/autenticacao" in page.url:
                prosseguir_btn = page.get_by_role("button", name=re.compile(r"^\s*Prosseguir\s*$", re.IGNORECASE)).first
                await safe_click(prosseguir_btn, "Prosseguir", force=False, timeout=FAST_TIMEOUT)
        except Exception:
            pass

        await page.goto(APP_HOME_URL, wait_until="domcontentloaded")
        await wait_loading_quiet(page, fast=True)
        log(f"Pós-login. URL atual: {page.url}")
    finally:
        stop_wd.set()
        try:
            await wd_task
        except Exception:
            pass

async def selecionar_unidade_obrigatorio(page) -> None:
    log("Abrindo seletor de unidade")
    await asyncio.sleep(0.6)

    opened = False
    mat_arrow = page.locator("div.mat-select-arrow-wrapper").first
    try:
        await mat_arrow.wait_for(state="visible", timeout=2500)
        await mat_arrow.click()
        opened = True
        log("mat-select de unidade aberto (tentativa 1)")
    except Exception:
        opened = False

    if not opened:
        user_drop = page.locator("i.material-icons.icone-seta-novo-user-data").filter(has_text="arrow_drop_down").first
        try:
            await user_drop.wait_for(state="visible", timeout=2500)
            await user_drop.click()
            await asyncio.sleep(0.3)
            await mat_arrow.wait_for(state="visible", timeout=2500)
            await mat_arrow.click()
            opened = True
            log("mat-select de unidade aberto via dropdown do usuário (tentativa 2)")
        except Exception:
            opened = False

    if not opened:
        combobox = page.get_by_role("combobox").nth(0)
        try:
            await combobox.wait_for(state="visible", timeout=2500)
            await combobox.click(force=True)
            opened = True
            log("mat-select de unidade aberto forçando combobox (tentativa 3)")
        except Exception:
            opened = False

    if not opened:
        raise RuntimeError("Não foi possível abrir o seletor de unidade")

    overlay = page.locator("div.cdk-overlay-pane").filter(
        has_not=page.locator(".cdk-overlay-pane[aria-hidden='true']")
    ).last

    search_input = overlay.locator("input.pesquisar-dropdrown[placeholder='Pesquisar']").first
    if not await search_input.is_visible():
        search_input = overlay.locator("input[placeholder='Pesquisar']").first

    await search_input.wait_for(state="visible", timeout=4000)
    await search_input.fill("shopping tijuca")

    item = overlay.get_by_text(UNIDADE_ALVO_REGEX).first
    if not await click_with_retries(item, "Unidade BT TIJUC - Shopping Tijuca - 11", attempts=3, timeout=5000):
        await search_input.fill("")
        await search_input.fill("tijuca")
        item = overlay.get_by_text(UNIDADE_ALVO_REGEX).first
        if not await click_with_retries(item, "Unidade BT TIJUC - Shopping Tijuca - 11", attempts=3, timeout=5000):
            raise RuntimeError("Não foi possível selecionar a unidade")

    await wait_loading_quiet(page, fast=True)
    log("Unidade selecionada com sucesso")

async def abrir_menu_financeiro_e_ir_para_nfs(page) -> None:
    log("Abrindo menu Financeiro e acessando Notas Fiscais de Serviço")

    financeiro_span = page.locator("span.nav-text", has_text=re.compile(r"^\s*Financeiro\s*$", re.IGNORECASE)).first
    await financeiro_span.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)

    li_fin = financeiro_span.locator("xpath=ancestor::li[1]")
    chevron = li_fin.locator("i.material-icons").filter(has_text=re.compile(r"keyboard_arrow_(down|right)")).first

    try:
        await chevron.wait_for(state="visible", timeout=FAST_TIMEOUT)
        await chevron.click()
    except Exception:
        await financeiro_span.click()

    await asyncio.sleep(0.25)

    nfs = page.get_by_text(re.compile(r"^\s*Notas Fiscais de Serviço\s*$", re.IGNORECASE)).first
    try:
        await nfs.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    except PlaywrightTimeout:
        await chevron.click(force=True)
        await nfs.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)

    if not await click_with_retries(nfs, "Notas Fiscais de Serviço", attempts=2, timeout=DEFAULT_TIMEOUT):
        await nfs.click(force=True, timeout=DEFAULT_TIMEOUT)
        log("Notas Fiscais de Serviço: clique forçado executado")

    await wait_loading_quiet(page, fast=True)

async def aplicar_data_ontem(page) -> None:
    log("Aplicando filtro de data: Ontem")
    btn_data = page.locator("button[data-cy='EFD-DatePickerBTN']").first
    await btn_data.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await btn_data.click()

    ontem = page.get_by_text(re.compile(r"^\s*Ontem\s*$", re.IGNORECASE)).first
    await ontem.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await ontem.click()

    aplicar = page.locator("button[data-cy='EFD-ApplyButton']").first
    await aplicar.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await aplicar.click()

    await wait_loading_quiet(page, fast=True)

async def exibir_por_data_lancamento(page) -> None:
    log("Configurando 'Exibir por' → 'Data de Lançamento'")

    abrir = page.locator("button[data-cy='abrirFiltro']").first
    await abrir.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)

    patt = re.compile(r"^\s*Data\s*(de\s*)?lan[çc]amento\s*$", re.IGNORECASE)

    async def open_overlay_or_retry() -> object:
        for _ in range(2):
            try:
                await abrir.click()
            except Exception:
                await abrir.click(force=True)
            overlay = page.locator("div.cdk-overlay-pane").filter(
                has_not=page.locator(".cdk-overlay-pane[aria-hidden='true']")
            ).last
            try:
                await overlay.wait_for(state="visible", timeout=1500)
                return overlay
            except PlaywrightTimeout:
                await asyncio.sleep(0.2)
                continue
        return None

    overlay = await open_overlay_or_retry()
    if overlay is None:
        # força sequência: ESC + reabre
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.2)
        overlay = await open_overlay_or_retry()
        if overlay is None:
            # último recurso: tenta clicar aplicar global (se já estiver correto)
            try:
                aplicar_global = page.locator("button[data-cy='AplicarFiltro']").first
                await aplicar_global.click(timeout=FAST_TIMEOUT)
                await wait_loading_quiet(page, fast=True)
                log("AplicarFiltro clicado sem overlay (fallback)")
                return
            except Exception:
                raise RuntimeError("Não foi possível abrir o overlay de 'Exibir por'.")

    # tenta achar como radio pelo role
    try:
        radio = overlay.get_by_role("radio", name=patt).first
        if await radio.count() > 0:
            try:
                await radio.click(timeout=FAST_TIMEOUT)
            except Exception:
                await radio.click(force=True, timeout=FAST_TIMEOUT)
        else:
            raise PlaywrightTimeout("Radio não encontrado")
    except Exception:
        # fallback por texto
        try:
            opt = overlay.get_by_text(patt).first
            await opt.wait_for(state="visible", timeout=FAST_TIMEOUT)
            try:
                await opt.click(timeout=FAST_TIMEOUT)
            except Exception:
                await opt.click(force=True, timeout=FAST_TIMEOUT)
        except Exception:
            # pode já estar selecionado; seguimos para Aplicar
            pass

    # clicar Aplicar (prioriza dentro do overlay; se não, global)
    try:
        aplicar = overlay.locator("button[data-cy='AplicarFiltro']").first
        await aplicar.wait_for(state="visible", timeout=FAST_TIMEOUT)
        await aplicar.click(timeout=FAST_TIMEOUT)
    except Exception:
        aplicar2 = page.locator("button[data-cy='AplicarFiltro']").first
        await aplicar2.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        await aplicar2.click(timeout=FAST_TIMEOUT)

    await asyncio.sleep(0.3)
    await wait_loading_quiet(page, fast=True)
    log("'Exibir por' aplicado com Data de Lançamento")

async def aplicar_filtro_tributacao(page) -> None:
    log("Abrindo + FILTROS")
    btn_mais_filtros = page.get_by_role("button", name=re.compile(r"\+\s*FILTROS", re.IGNORECASE)).first
    await btn_mais_filtros.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    try:
        await btn_mais_filtros.click()
    except Exception:
        await btn_mais_filtros.click(force=True)

    log("Abrindo Tributação")
    btn_tributacao = page.locator("button.simula-mat-menu", has_text=re.compile(r"^\s*Tributação\s*$", re.IGNORECASE)).first
    await btn_tributacao.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await btn_tributacao.click()

    pane = page.locator("div.cdk-overlay-pane").filter(
        has_not=page.locator(".cdk-overlay-pane[aria-hidden='true']")
    ).last

    todos = pane.get_by_text(re.compile(r"^\s*Todos\s*$", re.IGNORECASE)).first
    await todos.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await todos.click()

    nao_usar = pane.get_by_text(re.compile(r"^\s*Não usar\s*-\s*12\.34\.56\s*$", re.IGNORECASE)).first
    await nao_usar.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await nao_usar.click()

    aplicar = page.locator("button[data-cy='AplicarFiltro'], button#btn").first
    await aplicar.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await aplicar.click()

    await asyncio.sleep(0.4)
    await wait_loading_quiet(page, fast=True)

async def selecionar_todos_e_enviar(page) -> None:
    log("Selecionando todos os registros")
    sel_todos = page.locator("mat-checkbox[data-cy='SelecionarTodosCheck']").first
    await sel_todos.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await sel_todos.click()

    log("Clicando ENVIAR")
    enviar = page.get_by_role("button", name=re.compile(r"^\s*ENVIAR\s*$", re.IGNORECASE)).first
    await enviar.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await enviar.click()

    await asyncio.sleep(0.4)
    await wait_loading_quiet(page, fast=True)

async def digitar_data_util_anterior_no_input(page) -> None:
    alvo = previous_business_day()
    data_txt = fmt_date_br(alvo)
    log(f"Preenchendo campo de data com dia útil anterior: {data_txt}")

    campo = page.locator("input#evoDatepicker[placeholder='Selecione a data']").first
    await campo.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await campo.click()
    await page.keyboard.press("Control+A")
    await page.keyboard.press("Backspace")
    await campo.type(data_txt, delay=24)
    await asyncio.sleep(5)

# =========================
# Runner chamado pelo app.py
# =========================
async def _run() -> None:
    user, pwd = ensure_env()
    log("Iniciando fluxo com navegador visível")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, args=["--start-maximized"])
        context = await browser.new_context(viewport={"width": 1366, "height": 768})

        # Init-script: fixa tenant/bodytech e corrige hash/rotas no client
        await context.add_init_script("""
(() => {
  try {
    localStorage.setItem('tenant', 'bodytech');
    localStorage.setItem('dominio', 'bodytech');
    sessionStorage.setItem('tenant', 'bodytech');
    sessionStorage.setItem('dominio', 'bodytech');
    const forceTenant = () => {
      try {
        const h = location.hash || '';
        if (h.includes('/acesso/evo5/')) {
          location.hash = h.replace('/acesso/evo5/', '/acesso/bodytech/');
        } else if (h.includes('/acesso//')) {
          location.hash = h.replace('/acesso//', '/acesso/bodytech/');
        }
      } catch (_e) {}
    };
    forceTenant();
    const _ps = history.pushState;
    const _rs = history.replaceState;
    history.pushState = function() { const r = _ps.apply(this, arguments); setTimeout(forceTenant, 0); return r; };
    history.replaceState = function() { const r = _rs.apply(this, arguments); setTimeout(forceTenant, 0); return r; };
    window.addEventListener('hashchange', forceTenant, true);
  } catch (_err) {}
})();
        """)

        page = await context.new_page()

        try:
            await do_login(page, user, pwd)
            await selecionar_unidade_obrigatorio(page)
            await abrir_menu_financeiro_e_ir_para_nfs(page)
            await aplicar_data_ontem(page)
            await exibir_por_data_lancamento(page)
            await aplicar_filtro_tributacao(page)
            await selecionar_todos_e_enviar(page)
            await digitar_data_util_anterior_no_input(page)

            log("Pausa final de 5 segundos para inspeção")
            await asyncio.sleep(5)

        except Exception as e:
            ts = int(datetime.now().timestamp())
            img = SCREENSHOT_DIR / f"screenshot_erro_{ts}.png"
            try:
                await page.screenshot(path=str(img), full_page=True)
                log(f"Erro no fluxo. Screenshot salvo em: {img}")
            except Exception as se:
                log(f"Erro no fluxo e falha ao salvar screenshot: {se}")
            raise
        finally:
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass

def run_rpa_enter_google_folder(extract_dir: str, target_folder: str, base_dir: str) -> None:
    asyncio.run(_run())

def _ensure_local_zip_from_drive(dest_dir: str) -> str:
    system_tmp = Path(dest_dir) if dest_dir else Path("/tmp")
    system_tmp.mkdir(parents=True, exist_ok=True)
    win_default = Path(os.getenv("CNAB_LOCAL_DIR_WINDOWS", r"C:\AUTOMACAO\conciliacao\arquivos")) / "arquivos.zip"
    lin_default = Path(os.getenv("CNAB_LOCAL_DIR", "/home/felipe/Downloads/arquivos")) / "arquivos.zip"
    candidate = win_default if win_default.exists() else lin_default
    log(f"[stub] Usando ZIP local existente: {candidate if candidate.exists() else system_tmp}")
    return str(candidate if candidate.exists() else system_tmp / "arquivos.zip")
