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

def _normalize_str(s: str) -> str:
    """Remove acentos e deixa minúsculo."""
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c)).lower().strip()

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

# async def aplicar_data_ontem(page) -> None:
#     log("Aplicando filtro de data (modo de teste manual corrigido)")

#     # 1️⃣ Abre o seletor de data
#     btn_data = page.locator("button[data-cy='EFD-DatePickerBTN']").first
#     await btn_data.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
#     await btn_data.click()
#     await asyncio.sleep(1)

#     # 2️⃣ Clica no campo de input principal (id=mat-input-1)
#     campo_data = page.locator("input#mat-input-1")
#     await campo_data.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
#     await campo_data.click(force=True)
#     log("Campo de data clicado (mat-input-1)")

#     # 3️⃣ Clica no botão de mês anterior
#     btn_prev_mes = page.locator("button.mat-calendar-previous-button").first
#     await btn_prev_mes.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
#     await btn_prev_mes.click()
#     log("Botão 'Previous month' clicado")

#     # 4️⃣ Clica no dia 29 duas vezes
#     dia_29 = page.get_by_role("gridcell", name="29").first
#     await dia_29.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
#     await dia_29.click()
#     await asyncio.sleep(0.3)
#     await dia_29.click()
#     log("Dia 29 selecionado duas vezes")

#     # 5️⃣ Clica no botão “Aplicar”
#     aplicar = page.locator("button[data-cy='EFD-ApplyButton']").first
#     await aplicar.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
#     await aplicar.click()
#     log("Botão 'Aplicar' clicado")

#     # Espera a página atualizar
#     await wait_loading_quiet(page, fast=True)


async def aplicar_data_ontem(page) -> None:
    log("Aplicando filtro de data personalizada (voltar até janeiro e selecionar dia 7)")

    # 1️⃣ Abre o seletor de data
    btn_data = page.locator("button[data-cy='EFD-DatePickerBTN']").first
    await btn_data.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await btn_data.click()
    await asyncio.sleep(0.8)

    # 2️⃣ Clica em “Período personalizado”
    periodo_personalizado = page.get_by_text(re.compile(r"^\s*Período personalizado\s*$", re.IGNORECASE)).first
    await periodo_personalizado.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await periodo_personalizado.click()
    log("Selecionado: Período personalizado")

    # 3️⃣ Clica no campo “Selecionar data” — seletor dinâmico (id pode variar)
    campo_data = page.locator("input[placeholder='Selecionar data'], input[matinput][placeholder*='Selecionar']")
    await campo_data.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await campo_data.click(force=True)
    log("Campo 'Selecionar data' clicado com sucesso")

    # 4️⃣ Clica 10 vezes na seta de mês anterior (Previous month) para chegar até janeiro
    btn_prev_mes = page.locator("button.mat-calendar-previous-button[aria-label*='Previous month']").first
    await btn_prev_mes.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    for i in range(10):
        await btn_prev_mes.click()
        log(f"Seta de mês anterior clicada ({i+1}/10)")
        await asyncio.sleep(0.3)

    # 5️⃣ Clica duas vezes no dia 7
    dia_7 = page.locator("div.mat-calendar-body-cell-content", has_text="7").first
    await dia_7.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await dia_7.click()
    await asyncio.sleep(0.3)
    await dia_7.click()
    log("Dia 7 clicado duas vezes")

    # 6️⃣ Clica no botão “Aplicar”
    aplicar = page.locator(
        "button[data-cy='EFD-ApplyButton'], button",
        has_text=re.compile(r"Aplicar", re.IGNORECASE)
    ).first
    await aplicar.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await aplicar.click()
    log("Botão 'Aplicar' clicado")

    # 7️⃣ Aguarda atualização
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

    # 2) desmarcar todos os "Não usar - ..." visíveis (sem scroll)
    matches = pane.get_by_text(NAO_USAR_ANY)
    count = await matches.count()
    for i in range(count):
        handle = matches.nth(i)
        try:
            text = (await handle.inner_text()).strip()
        except Exception:
            text = f"Não usar (idx {i})"
        try:
            await handle.click()
            log(f"Tributação: desmarcado '{text}'")
        except Exception:
            try:
                await handle.click(force=True)
                log(f"Tributação: desmarcado '{text}' (force)")
            except Exception:
                log(f"Tributação: falha ao desmarcar '{text}'")

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

async def selecionar_data_ontem_modal(page) -> None:
    """
    Dentro do modal de envio:
    - Abre o calendário
    - Seleciona o dia anterior ao atual
    - Se hoje for dia 1, volta um mês e seleciona o último dia
    """
    hoje = datetime.now()
    ontem = hoje - timedelta(days=1)
    log(f"Abrindo calendário e selecionando a data de ontem: {ontem.strftime('%d/%m/%Y')}")

    # Abre o calendário (ícone do datepicker)
    btn_calendar = page.locator("svg.mat-datepicker-toggle-default-icon").first
    await btn_calendar.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await btn_calendar.click()
    await asyncio.sleep(0.4)

    # Caso o dia atual seja 1 → voltar um mês
    if hoje.day == 1:
        log("Hoje é dia 1 — voltando um mês e selecionando o último dia do mês anterior")
        btn_prev = page.locator("button.mat-calendar-previous-button").first
        await btn_prev.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        await btn_prev.click()
        await asyncio.sleep(0.4)

        # Seleciona o último dia do mês (31, 30, 29 ou 28)
        for dia in ["31", "30", "29", "28"]:
            dia_loc = page.locator("div.mat-calendar-body-cell-content", has_text=dia).first
            if await dia_loc.count():
                await dia_loc.click()
                log(f"Selecionado último dia do mês anterior: {dia}")
                break
    else:
        # Seleciona o dia de ontem
        dia_ontem = str(ontem.day)
        dia_loc = page.locator("div.mat-calendar-body-cell-content", has_text=dia_ontem).first
        await dia_loc.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        await dia_loc.click()
        log(f"Dia {dia_ontem} selecionado com sucesso")

    await asyncio.sleep(0.5)

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
    """Detecta se o status é 'Válido' (ignora acentos, case e espaços extras)."""
    s = ''.join(c for c in unicodedata.normalize("NFKD", status_txt) if not unicodedata.combining(c))
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s == "valido"

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

        cel_status = linha.locator(
            "span.label.very-tiny.vermelho, span.label.very-tiny:has-text('Inválido'), span.label.very-tiny"
        ).first

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

# === Abrir perfil do cliente inválido ===
# === Abrir perfil do cliente inválido e verificar país ===
async def abrir_perfil_cliente_invalido(page, cliente_id: str) -> None:
    """
    Duplica a aba atual, abre o perfil do cliente inválido e:
      - Se País != Brasil: considera estrangeiro, fecha a aba e retorna.
      - Se País == Brasil e CPF vazio: considera criança, ajusta Responsáveis e salva, fecha a aba e retorna.
      - Se País == Brasil e CPF preenchido: fecha a aba e segue normalmente.
    """
    log(f"Abrindo perfil do cliente inválido: {cliente_id}")

    # 1️⃣ Nova aba preservando filtros/URL da lista
    new_page = await page.context.new_page()
    await new_page.goto(page.url, wait_until="domcontentloaded")
    await wait_loading_quiet(new_page, fast=True)

    # 2️⃣ Buscar cliente pelo código no campo global
    campo_busca = new_page.locator(
        "input#evoAutocomplete[placeholder*='Pesquise por nome'], input.pesquisar-dropdown"
    )
    await campo_busca.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await campo_busca.fill("")
    await campo_busca.type(str(cliente_id), delay=40)

    resultado = new_page.locator("div.buscas").first
    await resultado.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await resultado.click()
    await wait_loading_quiet(new_page, fast=False)

    # 3️⃣ Ir para "Cadastro"
    aba_cadastro = new_page.locator("a[aria-label='Cadastro'], a[ui-sref*='dadosPessoais']").first
    await aba_cadastro.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await aba_cadastro.click()
    await wait_loading_quiet(new_page, fast=True)
    await asyncio.sleep(0.5)

    # 4️⃣ Ler valor do País — agora buscando especificamente um <span> com texto de país
    try:
        # Busca todos os <span> dentro de mat-select-value-text
        spans_pais = new_page.locator("span.mat-select-value-text span")
        qtd_spans = await spans_pais.count()
        valor_pais = ""

        for i in range(qtd_spans):
            txt = (await spans_pais.nth(i).inner_text()).strip()
            if re.search(r"brasil", _normalize_str(txt)):
                valor_pais = txt
                break
        if not valor_pais and qtd_spans > 0:
            # fallback: pega o último valor encontrado (geralmente País vem depois do DDI)
            valor_pais = (await spans_pais.nth(qtd_spans - 1).inner_text()).strip()

        if not valor_pais:
            raise RuntimeError("Campo 'País' não encontrado entre spans.")

    except Exception as e:
        log(f"Falha ao localizar campo País: {e}")
        valor_pais = ""

    eh_brasil = "brasil" in _normalize_str(valor_pais)
    log(f"Valor do campo País detectado: '{valor_pais}' → eh_brasil={eh_brasil}")

    # 5️⃣ Ler valor do CPF
    try:
        campo_cpf = new_page.locator("input#cpf").first
        valor_cpf = ""
        if await campo_cpf.count():
            await campo_cpf.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
            valor_cpf = (await campo_cpf.input_value()).strip()
    except Exception as e:
        log(f"Falha ao ler CPF: {e}")
        valor_cpf = ""

    log(f"Valor do CPF detectado: '{valor_cpf or '(vazio)'}'")

    # 6️⃣ Decisões de validação
    if not eh_brasil:
        print("\n⚠️ Usuário estrangeiro detectado\n")
        log("Usuário estrangeiro — fechando a aba e retornando ao fluxo.")
        await new_page.close()
        return

    if eh_brasil and not valor_cpf:
        print("\n👶 Usuário identificado como CRIANÇA (Brasil, sem CPF)\n")
        log("Usuário brasileiro sem CPF — tratando como criança.")
        await tratar_crianca_responsavel(new_page)
        await new_page.close()
        log("Aba do cliente (criança) fechada. Retomando o processo na aba principal.")
        return

    # Se chegou aqui: é brasileiro e tem CPF → seguir normalmente
    log("Usuário brasileiro com CPF — nenhum tratamento adicional necessário.")
    await new_page.close()
    return



async def _is_checked(checkbox) -> bool:
    """
    Retorna True se o mat-checkbox estiver marcado.
    Checa por [class*='mat-checkbox-checked'] ou aria-checked='true'.
    """
    try:
        root = checkbox.locator("xpath=ancestor::mat-checkbox[1]")
        if await root.get_attribute("class") and "mat-checkbox-checked" in (await root.get_attribute("class")):
            return True
        aria = await root.get_attribute("aria-checked")
        return (aria == "true")
    except Exception:
        return False

async def _check_if_needed(checkbox, desc: str = "checkbox") -> None:
    """
    Marca o checkbox apenas se ainda estiver desmarcado.
    Recebe o locator do ".mat-checkbox-inner-container".
    """
    if not await _is_checked(checkbox):
        try:
            await checkbox.click()
            log(f"{desc}: marcado como True")
        except Exception:
            await checkbox.click(force=True)
            log(f"{desc}: marcado como True (force)")
    else:
        log(f"{desc}: já estava True (mantido)")

async def tratar_crianca_responsavel(page) -> None:
    """
    Fluxo para criança:
      - Aba 'Responsáveis'
      - Editar o 1º registro (ícone 'edit')
      - Marcar 2 checkboxes como True
      - Salvar
    """
    # 1) Ir para a aba Responsáveis
    # (funciona tanto em AngularJS md-tabs quanto em Angular Material)
    aba_resp = page.get_by_role("tab", name=re.compile(r"^\s*Respons[aá]veis\s*$", re.IGNORECASE)).first
    if not await aba_resp.count():
        aba_resp = page.locator("md-tab-item, .md-tab, [role='tab']").filter(
            has_text=re.compile(r"Respons[aá]veis", re.IGNORECASE)
        ).first
    await aba_resp.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    await aba_resp.click()
    await wait_loading_quiet(page, fast=True)
    await asyncio.sleep(0.4)

    # 2) Editar o primeiro registro (ícone 'edit')
    botao_editar = page.locator("mat-icon", has_text=re.compile(r"^\s*edit\s*$", re.IGNORECASE)).first
    # Caso o mat-icon esteja dentro de um botão:
    if await botao_editar.count() == 0:
        botao_editar = page.locator("button mat-icon", has_text=re.compile(r"^\s*edit\s*$", re.IGNORECASE)).first
    await botao_editar.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    # clicar no container do botão se necessário
    try:
        await botao_editar.click()
    except Exception:
        await botao_editar.locator("xpath=ancestor::button[1]").click()
    await wait_loading_quiet(page, fast=True)
    await asyncio.sleep(0.3)

    # 3) Marcar as duas checkboxes (as das imagens 3 e 4)
    # Usamos os dois primeiros ".mat-checkbox-inner-container" do formulário de edição.
    form_edit = page.locator("form, mat-dialog-container, .mat-dialog-content").first
    cbs = form_edit.locator(".mat-checkbox-inner-container")
    count = await cbs.count()
    if count < 2:
        # fallback para procurar globalmente no editor visível
        cbs = page.locator(".mat-checkbox-inner-container")
        count = await cbs.count()

    if count == 0:
        raise RuntimeError("Não encontrei checkboxes na edição do responsável.")

    # Marca a 1ª e a 2ª checkbox como True (apenas se estiverem false)
    await _check_if_needed(cbs.nth(0), "Checkbox #1 (responsável)")
    if count > 1:
        await _check_if_needed(cbs.nth(1), "Checkbox #2 (responsável)")

    # 4) Salvar (botão da imagem 5)
    # Preferimos por texto. Se não houver, clicamos no 'evo-button primary/success'.
    salvar = page.get_by_role("button", name=re.compile(r"^\s*Salvar\s*$", re.IGNORECASE)).first
    if not await salvar.count():
        salvar = page.locator("button.evo-button.primary, button.evo-button.success, button.mat-button").filter(
            has_text=re.compile(r"^\s*Salvar\s*$", re.IGNORECASE)
        ).first
    await salvar.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
    try:
        await salvar.click()
    except Exception:
        await salvar.click(force=True)
    await wait_loading_quiet(page, fast=True)
    await asyncio.sleep(0.3)

    log("Edição do responsável salva com sucesso (criança tratada).")



async def coletar_registros_tabela(page, limite_por_pagina: int = 100):
    """
    Coleta todos os registros de todas as páginas da tabela.
    Se encontrar cadastros inválidos (ex: CPF Inválido),
    abre automaticamente o perfil de cada cliente inválido em sequência.
    Após corrigir todos, atualiza a aba principal, refaz filtros e envia.
    """
    try:
        todos_registros = []
        pagina = 1

        while True:
            log(f"📄 Coletando página {pagina}…")
            await page.evaluate("window.scrollTo(0, 0)")
            await asyncio.sleep(1)

            linhas = page.locator("mat-table mat-row, table tbody tr")
            total = await linhas.count()
            log(f"Total de linhas detectadas nesta página: {total}")

            registros = []
            for i in range(total):
                linha = linhas.nth(i)
                celulas = linha.locator("mat-cell, td")
                qtd_celulas = await celulas.count()
                if qtd_celulas == 0:
                    continue

                textos = []
                for j in range(qtd_celulas):
                    try:
                        raw = (await celulas.nth(j).inner_text()).strip()
                        clean = ' '.join(raw.split())
                        textos.append(clean)
                    except Exception:
                        textos.append("")

                registro = {
                    "cliente": textos[1] if len(textos) > 1 else "",
                    "cpf": textos[2] if len(textos) > 2 else "",
                    "descricao": textos[3] if len(textos) > 3 else "",
                    "recebimento": textos[4] if len(textos) > 4 else "",
                    "lancamento": textos[5] if len(textos) > 5 else "",
                    "vencimento": textos[6] if len(textos) > 6 else "",
                    "valor": textos[7] if len(textos) > 7 else "",
                    "valor_emissao": textos[8] if len(textos) > 8 else "",
                    "cadastro": textos[9] if len(textos) > 9 else "",
                    "detalhes": textos[10] if len(textos) > 10 else "",
                }
                registros.append(registro)

            todos_registros.extend(registros)
            log(f"✅ Página {pagina}: {len(registros)} registros coletados (total: {len(todos_registros)})")

            # 🔎 Verifica todos os inválidos da página atual
            invalidos = [
                r for r in registros
                if "invalido" in _normalize_str(r.get("cadastro", "")) or
                   "invalido" in _normalize_str(r.get("detalhes", ""))
            ]

            if invalidos:
                print("\n🚨 === CADASTROS INVÁLIDOS DETECTADOS === 🚨\n")
                print(json.dumps(invalidos, ensure_ascii=False, indent=2))
                print(f"\nTotal de inválidos nesta página: {len(invalidos)}\n")

                # 👉 Processa todos os inválidos sequencialmente
                for idx, cliente in enumerate(invalidos, 1):
                    match = re.search(r"\b(\d{4,})\b", cliente.get("cliente", ""))
                    if not match:
                        log(f"⚠️ ({idx}/{len(invalidos)}) Não foi possível extrair ID de cliente: {cliente.get('cliente')}")
                        continue

                    cliente_id = match.group(1)
                    log(f"[{idx}/{len(invalidos)}] Abrindo perfil do cliente inválido: {cliente_id}")
                    await abrir_perfil_cliente_invalido(page, cliente_id)
                    await asyncio.sleep(0.8)

                log(f"✅ Todos os {len(invalidos)} clientes inválidos foram tratados. Recarregando a tela e aplicando filtros novamente…")

                # 🔁 Atualiza e refaz os filtros
                await page.reload(wait_until="domcontentloaded")
                await wait_loading_quiet(page, fast=True)
                await aplicar_data_ontem(page)
                await exibir_por_data_lancamento(page)
                await aplicar_filtro_tributacao(page)

                # ✅ Todos válidos agora → enviar diretamente
                if await has_select_all_checkbox(page):
                    log("Todos os cadastros agora estão válidos — enviando notas fiscais.")
                    await selecionar_todos_e_enviar(page)
                    await selecionar_data_ontem_modal(page)
                    await cancelar_modal_enviar_nf(page)
                    log("Envio finalizado após correção dos cadastros inválidos.")
                else:
                    log("Nenhum registro encontrado após atualização.")

                return todos_registros

            # Continua paginação se houver mais páginas
            btn_proximo = page.locator("button.mat-paginator-navigation-next:not([disabled])").first
            if not await btn_proximo.count():
                log("🚫 Botão 'Próximo' desabilitado — última página alcançada.")
                break

            await btn_proximo.click()
            await wait_loading_quiet(page, fast=True)
            pagina += 1
            await asyncio.sleep(1.2)

        print("\n✅ Nenhum cadastro inválido encontrado!\n")
        return todos_registros

    except Exception as e:
        log(f"Erro ao coletar registros da tabela: {e}")
        return []






# === Pipeline por unidade
async def processar_unidade(page, nome_log: str, search_terms: List[str], regex: Pattern) -> None:
    log(f"---- Iniciando unidade: {nome_log} ----")
    await selecionar_unidade_por_nome(page, search_terms, regex)
    await abrir_menu_financeiro_e_ir_para_nfs(page)
    await aplicar_data_ontem(page)
    await exibir_por_data_lancamento(page)
    await aplicar_filtro_tributacao(page)
    await definir_itens_por_pagina(page, 100)
    await coletar_registros_tabela(page)


    # >>> Validação estrita (sem paginação). Aborta se houver inválidos.
    # >>> Validação estrita (sem paginação). Se houver inválidos, abre o primeiro.
    invalidos = await validar_antes_de_enviar(page)
    tem_invalidos = invalidos and len(invalidos) > 0

    if tem_invalidos:
        log(f"Unidade {nome_log}: inválidos detectados. Abrindo primeiro cliente inválido para análise...")
        primeiro = invalidos[0]
        match = re.search(r"\b(\d{4,})\b", primeiro["cliente"])
        if match:
            cliente_id = match.group(1)
            await abrir_perfil_cliente_invalido(page, cliente_id)
        else:
            log("⚠️ Não foi possível extrair o número do cliente inválido.")
    else:
        log(f"Unidade {nome_log}: nenhum inválido detectado (todos válidos).")


      

    # Fluxo normal, somente se todos válidos
    # Envia cadastros válidos (sempre roda — com ou sem inválidos)
    if await has_select_all_checkbox(page):
        log("Checkbox 'Selecionar todos' presente — iniciando envio de notas fiscais")

        await selecionar_todos_e_enviar(page)
        await selecionar_data_ontem_modal(page)
        await cancelar_modal_enviar_nf(page)

        log(f"Unidade {nome_log}: processo de envio finalizado com sucesso.")
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
        # for nome, termos, rx in unidades_bt:
        #     try:
        #         await processar_unidade(page, nome, termos, rx)
        #     except Exception as e:
        #         ts = int(datetime.now().timestamp())
        #         nome_sanitizado = re.sub(r'\W+', '_', nome)
        #         img = SCREENSHOT_DIR / f"screenshot_erro_{nome_sanitizado}_{ts}.png"
        #         try:
        #             await page.screenshot(path=str(img), full_page=True)
        #             log(f"Erro no fluxo ({nome}). Screenshot: {img}")
        #         except Exception as se:
        #             log(f"Falha ao salvar screenshot ({nome}): {se}")
        #         continue

        ## ORDEM DOS SHOPPINGS

        for nome, termos, rx in unidades_bt[2:]:
            try:
                await processar_unidade(page, nome, termos, rx)
            except Exception as e:
                ts = int(datetime.now().timestamp())
                nome_sanitizado = re.sub(r'\W+', '_', nome)
                img = SCREENSHOT_DIR / f"screenshot_erro_{nome_sanitizado}_{ts}.png"
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

async def definir_itens_por_pagina(page, qtd: int = 100) -> None:
    try:
        log(f"Ajustando 'Itens por página' para {qtd}")
        paginator = page.locator("mat-paginator").first
        await paginator.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)

        seletor = paginator.locator("mat-select").first
        await seletor.click()

        opcao = page.get_by_role("option", name=re.compile(fr"^\s*{qtd}\s*$")).first
        await opcao.wait_for(state="visible", timeout=DEFAULT_TIMEOUT)
        await opcao.click()

        await wait_loading_quiet(page, fast=True)
        log(f"Itens por página ajustado para {qtd}")
    except Exception as e:
        log(f"Falha ao ajustar itens por página: {e}")

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
                context = await browser.new_context(no_viewport=True)
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
                await page.set_viewport_size({"width": 1920, "height": 1080})
                try:
                    await run_for_tenant(page, tenant, url, user, pwd)
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
