# rpa.py
import os
import re
import json
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Pattern, List, Tuple, Optional
import unicodedata

from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# =========================
# Carrega .env e parâmetros
# =========================
load_dotenv(override=True)

HEADLESS = os.getenv("HEADLESS", "1").strip() != "0"
DEBUG_LOGIN = os.getenv("W12_DEBUG_LOGIN", "0").strip() == "1"

def ensure_env() -> tuple[str, str]:
    user = os.getenv("W12_USER", "").strip()
    pwd  = os.getenv("W12_PASS", "").strip()
    if not user or not pwd:
        raise RuntimeError("Credenciais não configuradas no .env (W12_USER e W12_PASS).")
    return user, pwd

# ====== URLs (ordem: bodytech → formula) ======
def _env_urls_in_order() -> List[str]:
    """
    Prioriza EVO_URL_FIRST / EVO_URL_SECOND.
    Se ausentes, tenta EVO_URL_BT / EVO_URL_FORMULA.
    Se mesmo assim não houver, varre variáveis EVO_URL*,
    detecta tenants e ordena bodytech → formula.
    Se só houver 1 URL, usa só ela.
    """
    # 1) pares explícitos
    u1 = os.getenv("EVO_URL_FIRST", "").strip()
    u2 = os.getenv("EVO_URL_SECOND", "").strip()
    if u1 and u2:
        return [u1, u2]

    # 2) nomes alternativos
    ubt = os.getenv("EVO_URL_BT", "").strip()
    ufo = os.getenv("EVO_URL_FORMULA", "").strip()
    if ubt and ufo:
        return [ubt, ufo]

    # 3) coletar todas EVO_URL* do ambiente
    cand: List[str] = []
    for k, v in os.environ.items():
        if k.startswith("EVO_URL"):
            vv = v.strip()
            if vv and vv not in cand:
                cand.append(vv)

    if len(cand) == 1:
        return cand

    def _tenant(url: str) -> Optional[str]:
        m = re.search(r"/#/acesso/([^/]+)/", url)
        return m.group(1) if m else None

    bt  = [u for u in cand if _tenant(u) == "bodytech"]
    frm = [u for u in cand if _tenant(u) == "formula"]

    ordered: List[str] = []
    ordered.extend(bt[:1])
    ordered.extend(frm[:1])

    if ordered:
        return ordered

    # 4) fallback: EVO_URL genérica
    u = os.getenv("EVO_URL", "").strip()
    return [u] if u else []

def _extract_tenant_from_url(url: str) -> str:
    m = re.search(r"/#/acesso/([^/]+)/", url)
    return (m.group(1) if m else "formula").strip()

# =========================
# Constantes e diretórios
# =========================
SCREENSHOT_DIR = Path.home() / "Downloads" / "faturamento_academia"
SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_TIMEOUT = 6000
SHORT_TIMEOUT   = 3000
VERY_SHORT_TIMEOUT = 1500
FAST_TIMEOUT    = 1200

# =========================
# Unidades (regex)
# =========================
# Sequência "clássica" (tenant bodytech)
UNIDADE_ALVO_REGEX = re.compile(r"^\s*BT TIJUC\s*-\s*Shopping Tijuca\s*-\s*11\s*$", re.IGNORECASE)
PRAIA_DA_COSTA_REGEX = re.compile(r"^\s*BT\s*VELHA\s*-\s*Shop\.\s*Praia da Costa\s*-\s*27\s*$", re.IGNORECASE)
SHOPPING_DA_ILHA_REGEX = re.compile(r"^\s*BT\s*SLUIS\s*-\s*Shopping da Ilha\s*-\s*80\s*$", re.IGNORECASE)
SHOPPING_VITORIA_REGEX = re.compile(r"^\s*BT\s*VITOR\s*-\s*Shopping Vit[oó]ria\s*-\s*89\s*$", re.IGNORECASE)
SHOPPING_RIO_POTY_REGEX = re.compile(r"^\s*BT\s*TERES\s*-\s*Shop(?:ping)?\.?\s*Rio\s*Poty\s*-\s*102\s*$", re.IGNORECASE)

# Pós-Rio Poty (tenant formula)
SHOPPING_MESTRE_ALVARO_EXATO = re.compile(
    r"^\s*FR\s*MALVA\s*-\s*Shopping\s*Mestre\s*[ÁA]lvaro\s*-\s*71\s*$",
    re.IGNORECASE
)

# Moxuara — mais tolerante (pega "moxuara" / "moxuará" / "moxuaro" por segurança)
SHOPPING_MOXUARA_REGEX = re.compile(r"\bmoxuar[aoá]\b", re.IGNORECASE)

# Padrão genérico para “Não usar - {código}”
NAO_USAR_ANY = re.compile(r"^\s*Não\s*usar\s*(?:-\s*\d+(?:\.\d+)*)?\s*$", re.IGNORECASE)

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

def _strip_accents_lower(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c)).lower()

def _matches_any(term: str, needles: List[str]) -> bool:
    t = _strip_accents_lower(term)
    return any(n in t for n in needles)

async def wait_loading_quiet(page, fast: bool = False) -> None:
    try:
        await page.wait_for_load_state("networkidle", timeout=(1500 if fast else DEFAULT_TIMEOUT))
    except PlaywrightTimeout:
        pass
    for sel in [
        "evo-loading", ".mat-progress-bar", ".cdk-overlay-backdrop",
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

def _corrigir_url_tenant(url: str, tenant: str) -> str:
    if "/acesso//" in url:
        return url.replace("/acesso//", f"/acesso/{tenant}/")
    return re.sub(r"/acesso/[^/]+/", f"/acesso/{tenant}/", url)

async def garantir_tenant(page, tenant: str) -> None:
    corr = _corrigir_url_tenant(page.url, tenant)
    if corr != page.url:
        await page.goto(corr, wait_until="domcontentloaded")
        await asyncio.sleep(0.1)

async def _forcar_url_via_barra(page, url: str) -> None:
    try:
        await page.keyboard.press("Control+L")
        await asyncio.sleep(0.05)
        await page.keyboard.type(url, delay=4)
        await page.keyboard.press("Enter")
        log("URL ajustada via barra do navegador")
    except Exception as e:
        log(f"Fallback da barra de URL falhou: {e}")

async def tenant_watchdog(page, stop_event: asyncio.Event, tenant: str) -> None:
    try:
        last_seen = ""
        corrections = 0
        while not stop_event.is_set():
            url = page.url
            if url == last_seen:
                await asyncio.sleep(0.15)
                continue
            last_seen = url

            if f"/app/{tenant}/" in url:
                stop_event.set()
                break

            if "/acesso/" in url and f"/acesso/{tenant}/" not in url:
                corr = _corrigir_url_tenant(url, tenant)
                if corr != url:
                    log(f"Watchdog corrigindo URL: {url} -> {corr}")
                    corrections += 1
                    try:
                        await page.goto(corr, wait_until="domcontentloaded")
                    except Exception:
                        pass
                    if corrections >= 6:
                        await asyncio.sleep(1.0)
                        corrections = 0
                else:
                    await asyncio.sleep(0.15)
            else:
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

async def wait_for_login_fields(page, tenant: str, base_login_url: str, max_wait_ms: int = 12000):
    email_selectors = [
        "input#usuario","input[name='usuario']","input[name='email']",
        "input[formcontrolname='usuario']","input[formcontrolname='email']",
        "input[type='email']","input[placeholder*='E-mail' i]","input[placeholder*='Email' i]",
    ]
    pass_selectors = [
        "input#senha","input[name='senha']","input[formcontrolname='senha']",
        "input[type='password']","input[placeholder*='Senha' i]",
    ]

    end_time = datetime.now().timestamp() + (max_wait_ms / 1000.0)
    email_loc = None
    pass_loc = None
    while datetime.now().timestamp() < end_time:
        await garantir_tenant(page, tenant)
        if f"/acesso/{tenant}/" not in page.url:
            await _forcar_url_via_barra(page, base_login_url)
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
async def do_login(page, tenant: str, base_login_url: str, user: str, pwd: str) -> None:
    log(f"Abrindo página de login (tenant={tenant})")
    await page.goto(base_login_url, wait_until="domcontentloaded", timeout=20000)

    stop_wd = asyncio.Event()
    wd_task = asyncio.create_task(tenant_watchdog(page, stop_wd, tenant))

    try:
        email_input, pass_input = await wait_for_login_fields(page, tenant, base_login_url, max_wait_ms=15000)
        log("Página de login/autenticação detectada — campos visíveis")

        entrar_btn = page.get_by_role("button", name=re.compile(r"^\s*Entrar\s*$", re.IGNORECASE)).first
        try:
            await entrar_btn.wait_for(state="visible", timeout=3000)
        except PlaywrightTimeout:
            entrar_btn = page.locator("button", has_text=re.compile(r"^\s*Entrar\s*$", re.IGNORECASE)).first

        if DEBUG_LOGIN:
            log(f"Preenchendo usuário: {user}")
        await email_input.fill("")
        await email_input.fill(user)
        await pass_input.fill("")
        await pass_input.fill(pwd if not os.getenv("W12_LOG_PASSWORD_PLAINTEXT") else os.getenv("W12_PASS",""))

        if not await click_with_retries(entrar_btn, "Entrar", attempts=2, timeout=DEFAULT_TIMEOUT):
            raise RuntimeError("Falha ao clicar em Entrar")

        await asyncio.sleep(0.4)

        # /autenticacao → Prosseguir (se aparecer)
        try:
            if "/autenticacao" in page.url:
                prosseguir_btn = page.get_by_role("button", name=re.compile(r"^\s*Prosseguir\s*$", re.IGNORECASE)).first
                await safe_click(prosseguir_btn, "Prosseguir", force=False, timeout=FAST_TIMEOUT)
        except Exception:
            pass

        app_home_url = f"https://evo5.w12app.com.br/#/app/{tenant}/-2/inicio/geral"
        await page.goto(app_home_url, wait_until="domcontentloaded")
        await wait_loading_quiet(page, fast=True)
        log(f"Pós-login. URL atual: {page.url}")
    finally:
        stop_wd.set()
        try:
            await wd_task
        except Exception:
            pass

# --- menu do usuário (canto superior direito) ---
async def abrir_menu_usuario(page):
    log("Abrindo menu do usuário (canto superior direito)")
    trigger = page.locator("i.material-icons.icone-seta-novo-user-data.no-margin-left").first
    if not await trigger.is_visible():
        trigger = page.locator("i.material-icons.icone-seta-novo-user-data").first
        if not await trigger.is_visible():
            trigger = page.locator("div.novo-user-data").first

    await trigger.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await trigger.click()

    pane = page.locator("div.cdk-overlay-pane .mat-menu-panel, div.cdk-overlay-pane").last
    await pane.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    return pane

# === Seleção de unidade (robusta; inclui varredura com scroll) ===
async def selecionar_unidade_por_nome(page, search_terms: List[str], target_regex: Pattern) -> None:
    pane = await abrir_menu_usuario(page)
    log("Localizando seletor 'Selecionar unidade' dentro do menu do usuário")

    # Abrir o mat-select pelo arrow wrapper (preferencial)
    select_trigger = pane.locator(".mat-select-arrow-wrapper").first
    if not await select_trigger.is_visible():
        # fallbacks
        select_trigger = pane.locator("mat-select, .mat-select-trigger, div.mat-select-arrow-wrapper").first
        if not await select_trigger.is_visible():
            select_trigger = pane.get_by_role("combobox").first

    await select_trigger.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await select_trigger.click()

    overlay = page.locator("div.cdk-overlay-pane").filter(
        has_not=page.locator(".cdk-overlay-pane[aria-hidden='true']")
    ).last
    await overlay.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)

    # Normaliza "agulhas" (termos) para comparação sem acento
    needles = [_strip_accents_lower(t) for t in (search_terms or [])]

    # 1) Tentar com campo de busca (se existir)
    search_input = overlay.locator("input.pesquisar-dropdrown[placeholder='Pesquisar'], input[placeholder='Pesquisar']").first
    if await search_input.count():
        for term in search_terms:
            await search_input.fill("")
            await search_input.type(term, delay=8)

            # tentar por texto exato/regex
            try:
                item = overlay.get_by_text(target_regex).first
                await item.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
                if await click_with_retries(item, f"Unidade alvo ({term})", attempts=3, timeout=DEFAULT_TIMEOUT):
                    await wait_loading_quiet(page, fast=True)
                    log("Unidade selecionada com sucesso (via busca)")
                    return
            except Exception:
                pass

            # fallback de teclado
            try:
                await search_input.press("ArrowDown")
                await asyncio.sleep(0.1)
                await search_input.press("Enter")
                await wait_loading_quiet(page, fast=True)
                log("Unidade selecionada (via setas/Enter)")
                return
            except Exception:
                pass

    # 2) Tentar clicar direto no bloco <div> com texto — primeiro via regex
    try:
        item_bloco = overlay.locator("div.p-x-xs.p-y-sm", has_text=target_regex).first
        await item_bloco.wait_for(state="visible", timeout=FAST_TIMEOUT)
        if await click_with_retries(item_bloco, "Unidade alvo (div bloco - regex)", attempts=3, timeout=DEFAULT_TIMEOUT):
            await wait_loading_quiet(page, fast=True)
            log("Unidade selecionada (div bloco - regex)")
            return
    except Exception:
        pass

    # 3) Varredura com SCROLL dentro do overlay procurando por termos normalizados
    try:
        options = overlay.locator("div.p-x-xs.p-y-sm")
        seen_texts = set()
        for _ in range(14):  # varre ~14 páginas com PageDown
            count = await options.count()
            for i in range(count):
                opt = options.nth(i)
                try:
                    txt = (await opt.inner_text()).strip()
                except Exception:
                    continue
                if txt in seen_texts:
                    continue
                seen_texts.add(txt)

                if target_regex.search(txt) or _matches_any(txt, needles):
                    try:
                        await opt.scroll_into_view_if_needed(timeout=SHORT_TIMEOUT)
                    except Exception:
                        pass
                    if await click_with_retries(opt, f"Unidade alvo (scan: '{txt}')", attempts=3, timeout=DEFAULT_TIMEOUT):
                        await wait_loading_quiet(page, fast=True)
                        log(f"Unidade selecionada (scan): {txt}")
                        return
            # rolar mais um "pedaço" da lista
            try:
                await overlay.hover()
                await page.keyboard.press("PageDown")
                await asyncio.sleep(0.25)
            except Exception:
                break
    except Exception:
        pass

    # 4) Último fallback: texto cru
    item = overlay.get_by_text(target_regex).first
    if await click_with_retries(item, "Unidade alvo (fallback final)", attempts=3, timeout=DEFAULT_TIMEOUT):
        await wait_loading_quiet(page, fast=True)
        return

    raise RuntimeError("Não foi possível selecionar a unidade alvo dentro do menu do usuário")

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

    # Preferir data-cy quando disponível
    nfs = page.locator('span.nav-text[data-cy="Notas Fiscais de Serviço"]').first
    try:
        await nfs.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    except PlaywrightTimeout:
        nfs = page.get_by_text(re.compile(r"^\s*Notas Fiscais de Serviço\s*$", re.IGNORECASE)).first
        await nfs.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)

    if not await click_with_retries(nfs, "Notas Fiscais de Serviço", attempts=2, timeout=DEFAULT_TIMEOUT):
        await nfs.click(force=True, timeout=DEFAULT_TIMEOUT)

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
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.2)
        overlay = await open_overlay_or_retry()
        if overlay is None:
            try:
                aplicar_global = page.locator("button[data-cy='AplicarFiltro']").first
                await aplicar_global.click(timeout=FAST_TIMEOUT)
                await wait_loading_quiet(page, fast=True)
                return
            except Exception:
                raise RuntimeError("Não foi possível abrir o overlay de 'Exibir por'.")

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
        try:
            opt = overlay.get_by_text(patt).first
            await opt.wait_for(state="visible", timeout=FAST_TIMEOUT)
            try:
                await opt.click(timeout=FAST_TIMEOUT)
            except Exception:
                await opt.click(force=True, timeout=FAST_TIMEOUT)
        except Exception:
            pass

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

# === Tributação — marcar TODOS e DESMARCAR QUALQUER “Não usar - …” ===
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
    await pane.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)

    # 1) 'Todos'
    try:
        todos = pane.get_by_text(re.compile(r"^\s*Todos\s*$", re.IGNORECASE)).first
        await todos.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        await todos.click()
    except Exception:
        pass

    # 2) desmarcar TODOS os "Não usar - ..."
    vistos = set()
    for _ in range(5):
        matches = pane.get_by_text(NAO_USAR_ANY)
        count = await matches.count()
        for i in range(count):
            handle = matches.nth(i)
            try:
                text = (await handle.inner_text()).strip()
            except Exception:
                text = f"Não usar (idx {i})"
            if text in vistos:
                continue
            vistos.add(text)
            try:
                await handle.scroll_into_view_if_needed(timeout=DEFAULT_TIMEOUT)
            except Exception:
                pass
            try:
                await handle.click()
                log(f"Tributação: desmarcado '{text}'")
            except Exception:
                try:
                    await handle.click(force=True)
                    log(f"Tributação: desmarcado '{text}' (force)")
                except Exception:
                    log(f"Tributação: falha ao desmarcar '{text}'")
        try:
            await pane.hover()
            await page.keyboard.press("PageDown")
            await asyncio.sleep(0.2)
        except Exception:
            break

    # 3) aplicar
    aplicar = page.locator("button[data-cy='AplicarFiltro'], button#btn", has_text=re.compile(r"Aplicar", re.IGNORECASE)).first
    await aplicar.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await aplicar.click()

    await asyncio.sleep(0.4)
    await wait_loading_quiet(page, fast=True)

# === Validação universal: existe "Selecionar todos"? ===
async def has_select_all_checkbox(page) -> bool:
    await asyncio.sleep(2.0)  # janela para a tabela renderizar
    sel = page.locator(
        "mat-checkbox[data-cy='SelecionarTodosCheck'], "
        "mat-header-row mat-checkbox, "
        "mat-table mat-header-row mat-checkbox, "
        "mat-checkbox .mat-checkbox-inner-container"
    ).first
    try:
        await sel.wait_for(state="visible", timeout=1500)
        return True
    except PlaywrightTimeout:
        return False

async def selecionar_todos_e_enviar(page) -> None:
    log("Selecionando todos os registros")
    sel_todos = page.locator("mat-checkbox[data-cy='SelecionarTodosCheck']").first
    await sel_todos.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await sel_todos.click()

    log("Clicando ENVIAR (abre modal)")
    enviar = page.get_by_role("button", name=re.compile(r"^\s*ENVIAR\s*$", re.IGNORECASE)).first
    await enviar.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await enviar.click()

    await asyncio.sleep(0.4)
    await wait_loading_quiet(page, fast=True)

async def digitar_data_util_anterior_no_input(page) -> None:
    alvo = previous_business_day()
    data_txt = fmt_date_br(alvo)
    log(f"Preenchendo campo de data com dia útil anterior no modal: {data_txt}")

    campo = page.locator("mat-dialog-container input#evoDatepicker[placeholder='Selecione a data']").first
    if not await campo.is_visible():
        campo = page.locator("input#evoDatepicker[placeholder='Selecione a data']").first

    await campo.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await campo.click()
    await page.keyboard.press("Control+A")
    await page.keyboard.press("Backspace")
    await campo.type(data_txt, delay=24)
    await asyncio.sleep(0.2)

async def cancelar_modal_enviar_nf(page) -> None:
    log("Cancelando modal 'Enviar NF'")
    dialog = page.get_by_role("dialog", name=re.compile(r"^\s*Enviar\s*NF\s*$", re.IGNORECASE)).first
    if not await dialog.count():
        dialog = page.locator("mat-dialog-container").last
    cancelar = dialog.get_by_role("button", name=re.compile(r"^\s*Cancelar\s*$", re.IGNORECASE)).first
    if not await cancelar.count():
        cancelar = dialog.locator("button", has_text=re.compile(r"^\s*Cancelar\s*$", re.IGNORECASE)).first
    await cancelar.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await cancelar.click()
    try:
        await dialog.wait_for(state="detached", timeout=DEFAULT_TIMEOUT)
    except Exception:
        await page.keyboard.press("Escape")
        await asyncio.sleep(0.2)
    await wait_loading_quiet(page, fast=True)
    log("Modal 'Enviar NF' cancelado com sucesso")

# =========================================================
# ======== Validação “sem paginação / via scroll” =========
# =========================================================
def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _is_valido(status_txt: str) -> bool:
    # "Válido" (case-insensitive; normalizando acentos)
    return _norm(unicodedata.normalize("NFKD", status_txt)).lower() == "valido"

async def _require_count_gt0(locator, err_msg: str):
    if not await locator.count():
        raise RuntimeError(err_msg)

async def _scroll_table_step(page) -> None:
    """Rola um passo para baixo para forçar render de novas linhas."""
    await page.keyboard.press("PageDown")
    await asyncio.sleep(0.25)

async def _coletar_invalidos_novos(page, vistos: set) -> tuple[list[dict], int]:
    """
    Lê a grade ancorando em [data-cy='cliente'] (pivô da linha).
    Para cada 'cliente', sobe ao ancestral que contém também:
      - .label.very-tiny                 (status)
      - span[data-cy='informacoes'].full (motivo)
    Retorna (lista_de_invalidos, qtd_clientes_novos).
    """
    # espera até existir pelo menos 1 célula de cliente (2.5s)
    try:
        await page.wait_for_selector("[data-cy='cliente']", state="attached", timeout=2500)
    except Exception:
        # não há linhas visíveis
        log("Validação: não há [data-cy='cliente'] visível (tabela vazia?).")
        return [], 0

    clientes = page.locator("[data-cy='cliente']")
    total = await clientes.count()
    if total == 0:
        log("Validação: nenhum [data-cy='cliente'] encontrado.")
        return [], 0

    invalidos: list[dict] = []
    novos = 0

    for i in range(total):
        cel_cliente = clientes.nth(i)
        cliente_txt = _norm(await cel_cliente.inner_text())
        if not cliente_txt or cliente_txt in vistos:
            continue

        # Ancestor que contenha também status e motivo
        # (XPath único e determinístico, sem "fallback" leniente)
        linha = cel_cliente.locator(
            "xpath=ancestor::*[.//*[contains(@class,'label') and contains(@class,'very-tiny')]"
            " and .//span[@data-cy='informacoes' and contains(@class,'full')]][1]"
        )

        if not await linha.count():
            raise RuntimeError(
                f"Não achei ancestral da linha para o cliente '{cliente_txt}' "
                f"que contenha status (.label.very-tiny) e motivo (span[data-cy='informacoes'].full)."
            )

        cel_status = linha.locator(".label.very-tiny").first
        if not await cel_status.count():
            raise RuntimeError(f"Status ausente na linha do cliente '{cliente_txt}' (.label.very-tiny).")
        status_txt = _norm(await cel_status.inner_text())

        cel_motivo = linha.locator("span[data-cy='informacoes'].full").first
        if not await cel_motivo.count():
            raise RuntimeError(
                f"Motivo ausente na linha do cliente '{cliente_txt}' (span[data-cy='informacoes'].full)."
            )
        motivo_txt = _norm(await cel_motivo.inner_text())

        vistos.add(cliente_txt)
        novos += 1

        if not _is_valido(status_txt):
            invalidos.append({
                "cliente": cliente_txt,
                "status": status_txt or "(sem status)",
                "motivo": motivo_txt or "(sem detalhes)"
            })

    return invalidos, novos

async def validar_antes_de_enviar(page) -> Optional[List[dict]]:
    """
    Varre a grade SEM paginação:
    - Rola com PageDown até não surgirem clientes novos por 3 passos seguidos.
    - Exige na mesma linha: [data-cy='cliente'], .label.very-tiny, span[data-cy='informacoes'].full
    - Se houver inválidos, mostra alert e retorna a lista.
    """
    log("Validação (sem paginação): iniciando…")

    vistos: set[str] = set()
    invalidos_total: list[dict] = []
    estagnado = 0
    passos = 0
    MAX_PASSOS = 400  # trava de segurança

    while True:
        passos += 1
        if passos > MAX_PASSOS:
            log(f"Validação: limite de passos atingido ({MAX_PASSOS}). Encerrando varredura.")
            break

        invalidos, novos = await _coletar_invalidos_novos(page, vistos)
        invalidos_total.extend(invalidos)

        if novos == 0:
            estagnado += 1
        else:
            estagnado = 0

        if estagnado >= 3:
            break

        await _scroll_table_step(page)

    log(f"Validação: {len(vistos)} clientes varridos; {len(invalidos_total)} inválidos.")
    if invalidos_total:
        linhas = [f"- {i['cliente']} | status: {i['status']} | motivo: {i['motivo']}" for i in invalidos_total[:20]]
        extra = "" if len(invalidos_total) <= 20 else f"\n(+ {len(invalidos_total)-20} outros)"
        msg = "Foram encontrados cadastros NÃO válidos:\n\n" + "\n".join(linhas) + extra
        await page.evaluate("m=>alert(m)", msg)

    return invalidos_total

# === Pipeline por unidade
async def processar_unidade(page, nome_log: str, search_terms: List[str], regex: Pattern) -> None:
    log(f"---- Iniciando unidade: {nome_log} ----")
    await selecionar_unidade_por_nome(page, search_terms, regex)
    await abrir_menu_financeiro_e_ir_para_nfs(page)
    await aplicar_data_ontem(page)
    await exibir_por_data_lancamento(page)
    await aplicar_filtro_tributacao(page)

    # >>> Validação estrita (sem paginação). Aborta se houver inválidos.
    invalidos = await validar_antes_de_enviar(page)
    if invalidos and len(invalidos) > 0:
        log(f"Unidade {nome_log}: inválidos detectados. Abortando seleção/envio.")
        return

    # Fluxo normal, somente se todos válidos
    if await has_select_all_checkbox(page):
        log("Checkbox 'Selecionar todos' presente — seguindo fluxo normal")
        await selecionar_todos_e_enviar(page)
        await digitar_data_util_anterior_no_input(page)
        await cancelar_modal_enviar_nf(page)
        log(f"Unidade {nome_log}: fluxo concluído")
    else:
        log(f"Unidade {nome_log}: sem checkbox 'Selecionar todos' (sem registros). Pulando para a próxima.")

# =========================
# Execução por tenant
# =========================
async def run_for_tenant(page, tenant: str, base_login_url: str, user: str, pwd: str) -> None:
    await do_login(page, tenant, base_login_url, user, pwd)

    if tenant == "bodytech":
        unidades_bt: List[Tuple[str, List[str], Pattern]] = [
            ("BT TIJUC - Shopping Tijuca - 11",
             ["shopping tijuca", "tijuca", "BT TIJUC"],
             UNIDADE_ALVO_REGEX),
            ("BT VELHA - Shop. Praia da Costa - 27",
             ["Shop. Praia da Costa", "praia da costa", "BT VELHA"],
             PRAIA_DA_COSTA_REGEX),
            ("BT SLUIS - Shopping da Ilha - 80",
             ["Shopping da Ilha", "da ilha", "BT SLUIS"],
             SHOPPING_DA_ILHA_REGEX),
            ("BT VITOR - Shopping Vitória - 89",
             ["Shopping Vitória", "vitoria", "Vitória", "BT VITOR"],
             SHOPPING_VITORIA_REGEX),
            ("BT TERES - Shopping Rio Poty - 102",
             ["Shopping Rio Poty", "Shop. Rio Poty", "rio poty", "BT TERES"],
             SHOPPING_RIO_POTY_REGEX),
        ]
        for nome, termos, rx in unidades_bt:
            try:
                await processar_unidade(page, nome, termos, rx)
            except Exception as e:
                ts = int(datetime.now().timestamp())
                img = SCREENSHOT_DIR / f"screenshot_erro_{re.sub(r'\\W+', '_', nome)}_{ts}.png"
                try:
                    await page.screenshot(path=str(img), full_page=True)
                    log(f"Erro no fluxo ({nome}). Screenshot: {img}")
                except Exception as se:
                    log(f"Falha ao salvar screenshot ({nome}): {se}")
                continue
        return

    elif tenant == "formula":
        unidades_formula: List[Tuple[str, List[str], Pattern]] = [
            ("FR MALVA - Shopping Mestre Álvaro - 71",
             ["Mestre Álvaro", "MALVA", "Álvaro", "Mestre"],
             SHOPPING_MESTRE_ALVARO_EXATO),
            ("Shopping Moxuara",
             ["moxuara", "shopping moxuara", "moxuará"],
             SHOPPING_MOXUARA_REGEX),
        ]
        for nome, termos, rx in unidades_formula:
            try:
                await processar_unidade(page, nome, termos, rx)
            except Exception as e:
                ts = int(datetime.now().timestamp())
                tag = re.sub(r'\\W+', '_', nome)
                img = SCREENSHOT_DIR / f"screenshot_erro_{tag}_{ts}.png"
                try:
                    await page.screenshot(path=str(img), full_page=True)
                    log(f"Erro no fluxo ({nome}). Screenshot: {img}")
                except Exception as se:
                    log(f"Falha ao salvar screenshot ({nome}): {se}")
                continue
        return

    else:
        log(f"Tenant '{tenant}' sem sequência definida. Nada a executar.")
        return

# =========================
# Runner principal (contexto novo por tenant + pausa/fechar após bodytech)
# =========================
async def _run() -> None:
    user, pwd = ensure_env()
    urls = _env_urls_in_order()
    if not urls:
        raise RuntimeError("Nenhuma EVO_URL encontrada no ambiente.")

    log(f"HEADLESS={'1' if HEADLESS else '0'} | DEBUG_LOGIN={'1' if DEBUG_LOGIN else '0'}")
    log("Ordem de execução:")
    for i, u in enumerate(urls, 1):
        log(f"  {i}. {u}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, args=["--start-maximized"])

        try:
            for idx, url in enumerate(urls, 1):
                tenant = _extract_tenant_from_url(url)
                log(f"=== ({idx}/{len(urls)}) Tenant '{tenant}' ===")

                # Contexto novo por tenant
                context = await browser.new_context(viewport={"width": 1366, "height": 768})

                # ---- add_init_script sem f-string / chaves escapadas ----
                tenant_js = tenant
                await context.add_init_script(
                    """
((tenant) => {
  try {
    localStorage.setItem('tenant', tenant);
    localStorage.setItem('dominio', tenant);
    sessionStorage.setItem('tenant', tenant);
    sessionStorage.setItem('dominio', tenant);

    const forceTenant = () => {
      try {
        const h = location.hash || '';
        if (h.includes('/acesso//')) {
          location.hash = h.replace('/acesso//', '/acesso/' + tenant + '/');
        } else {
          const rx = /\\/acesso\\/[^/]+\\//;
          if (rx.test(h)) {
            location.hash = h.replace(rx, '/acesso/' + tenant + '/');
          }
        }
      } catch (_e) {}
    };

    forceTenant();
    const _ps = history.pushState;
    const _rs = history.replaceState;
    history.pushState = function() {
      const r = _ps.apply(this, arguments);
      setTimeout(forceTenant, 0);
      return r;
    };
    history.replaceState = function() {
      const r = _rs.apply(this, arguments);
      setTimeout(forceTenant, 0);
      return r;
    };
    window.addEventListener('hashchange', forceTenant, true);
  } catch (_err) {}
})(__TENANT__);
""".replace("__TENANT__", json.dumps(tenant_js))
                )

                page = await context.new_page()

                try:
                    await run_for_tenant(page, tenant, url, user, pwd)

                    # Se acabamos de rodar bodytech (último é Rio Poty), esperar 5s e fechar a página
                    if tenant == "bodytech":
                        log("Finalizado fluxo do tenant 'bodytech'. Aguardando 5s antes de abrir a próxima URL…")
                        await asyncio.sleep(5)
                        try:
                            await page.close()
                        except Exception:
                            pass

                except Exception:
                    ts = int(datetime.now().timestamp())
                    img = SCREENSHOT_DIR / f"screenshot_erro_tenant_{tenant}_{ts}.png"
                    try:
                        await page.screenshot(path=str(img), full_page=True)
                        log(f"Erro no fluxo (tenant={tenant}). Screenshot: {img}")
                    except Exception as se:
                        log(f"Falha ao salvar screenshot (tenant={tenant}): {se}")
                    raise
                finally:
                    try:
                        await context.close()
                    except Exception:
                        pass

            log("Pausa final de 5 segundos para inspeção")
            await asyncio.sleep(5)

        finally:
            try:
                await browser.close()
            except Exception:
                pass

# Mantém a assinatura esperada pelo seu app.py
def run_rpa_enter_google_folder(extract_dir: str, target_folder: str, base_dir: str) -> None:
    asyncio.run(_run())

# Stub antigo (mantido se for referenciado por app.py)
def _ensure_local_zip_from_drive(dest_dir: str) -> str:
    system_tmp = Path(dest_dir) if dest_dir else Path("/tmp")
    system_tmp.mkdir(parents=True, exist_ok=True)
    win_default = Path(os.getenv("CNAB_LOCAL_DIR_WINDOWS", r"C:\AUTOMACAO\conciliacao\arquivos")) / "arquivos.zip"
    lin_default = Path(os.getenv("CNAB_LOCAL_DIR", "/home/felipe/Downloads/arquivos")) / "arquivos.zip"
    candidate = win_default if win_default.exists() else lin_default
    log(f"[stub] Usando ZIP local existente: {candidate if candidate.exists() else system_tmp}")
    return str(candidate if candidate.exists() else system_tmp / "arquivos.zip")
