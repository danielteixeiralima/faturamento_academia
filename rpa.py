# filename: rpa.py
import os
import re
import time
from datetime import datetime
from typing import Optional

# =========================
# Configuração principal
# =========================

TARGET_URL = "https://evo5.w12app.com.br/#/acesso/bodytech/autenticacao"

# Credenciais (pode sobrescrever por .env)
W12_EMAIL = os.getenv("W12_EMAIL", "inova.ia@sacavalcante.com.br")
W12_PASSWORD = os.getenv("W12_PASSWORD", "omega536")

# Diretório onde o app espera encontrar o ZIP (apenas para manter compatibilidade de logs)
if os.name == "nt":
    DEFAULT_UPLOAD_DIR = os.getenv("CNAB_LOCAL_DIR_WINDOWS", r"C:\AUTOMACAO\conciliacao\arquivos")
else:
    DEFAULT_UPLOAD_DIR = os.getenv("CNAB_LOCAL_DIR", "/home/felipe/Downloads/arquivos")


# =========================
# Utilidades de Log/Status
# =========================

def _dbg(log_dir: str, msg: str) -> None:
    """Apende logs em rpa_debug.log e printa no console."""
    try:
        os.makedirs(log_dir, exist_ok=True)
    except Exception:
        pass
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(os.path.join(log_dir, "rpa_debug.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _save_report_json(base_dir: str, *, meta: dict) -> None:
    """
    Escreve progresso/erro em last_report.json para o dashboard do app.
    base_dir: use o terceiro argumento recebido de run_rpa_enter_google_folder (no seu app, é o BASE_DIR).
    """
    payload = {
        "ready": True,
        "updated_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "headers": [],
        "rows": [],
        "meta": meta or {},
    }
    try:
        os.makedirs(base_dir, exist_ok=True)
        path = os.path.join(base_dir, "last_report.json")
        import json
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
    except Exception as e:
        _dbg(base_dir, f"[report] falha ao salvar last_report.json: {e!r}")


# ======================================
# Compat: obter ZIP local (sem Drive)
# ======================================

def _ensure_local_zip_from_drive(log_dir: str, filename: str = "arquivos.zip") -> Optional[str]:
    """
    Mantém compatibilidade com o app.py. Aqui **não** baixamos do Drive.
    Apenas checamos se {DEFAULT_UPLOAD_DIR}/arquivos.zip existe e retornamos.
    Caso não exista, retornamos None.
    """
    try:
        os.makedirs(DEFAULT_UPLOAD_DIR, exist_ok=True)
    except Exception:
        pass

    local_zip = os.path.join(DEFAULT_UPLOAD_DIR, filename)
    if os.path.isfile(local_zip):
        _dbg(log_dir, f"[stub] Usando ZIP local existente: {local_zip}")
        return local_zip

    _dbg(log_dir, f"[stub] ZIP não encontrado: {local_zip}")
    return None


# ======================================
# Auxiliares de interação (Playwright)
# ======================================

def _wait_visible(page, selector: str, timeout_ms: int = 20000):
    """Aguarda um seletor ficar visível e retorna o Locator (ou lança)."""
    loc = page.locator(selector)
    loc.wait_for(state="visible", timeout=timeout_ms)
    return loc


def _click_if_present(page, selector: str, timeout_ms: int = 10000) -> bool:
    """Tenta clicar em um seletor se houver; retorna True/False."""
    try:
        loc = page.locator(selector)
        if loc.count() > 0:
            loc.first.scroll_into_view_if_needed(timeout=2000)
            loc.first.click(timeout=timeout_ms, force=True)
            return True
        return False
    except Exception:
        return False


def _type_safely(page, selector: str, text: str, timeout_ms: int = 20000) -> None:
    """Clica no campo, limpa (se possível) e preenche."""
    loc = _wait_visible(page, selector, timeout_ms)
    try:
        loc.click(timeout=3000, force=True)
    except Exception:
        pass
    try:
        loc.fill("")  # limpa, se suportado
    except Exception:
        pass
    loc.type(text, delay=20)


def _click_by_text_candidates(page, texts, timeout_ms=15000) -> bool:
    """
    Clica por texto, tentando várias abordagens. Retorna True se algum clique ocorrer.
    """
    end = time.time() + (timeout_ms / 1000.0)
    pats = []
    for t in texts:
        try:
            pats.append(re.compile(t, re.I))
        except re.error:
            pats.append(re.compile(re.escape(t), re.I))

    while time.time() < end:
        # role=button
        for pat in pats:
            try:
                btn = page.get_by_role("button", name=pat)
                if btn.count() > 0:
                    btn.first.scroll_into_view_if_needed(timeout=2000)
                    btn.first.click(timeout=3000, force=True)
                    return True
            except Exception:
                pass

        # 'text=' / locator
        for t in texts:
            try:
                loc = page.locator(f'text=/{re.escape(t)}/i')
                if loc.count() > 0:
                    loc.first.scroll_into_view_if_needed(timeout=2000)
                    loc.first.click(timeout=3000, force=True)
                    return True
            except Exception:
                pass

        time.sleep(0.2)
    return False


# ======================================
# Fluxo do W12 (Bodytech)
# ======================================

def _do_w12_flow(page, log_dir: str) -> None:
    """
    Executa o fluxo descrito:
      - Preenche e-mail (#usuario)
      - Preenche senha (#senha)
      - Clica Entrar
      - Clica arrow_drop_down
      - Abre seletor (mat-select arrow)
      - Pesquisa "shopping tijuca" e seleciona opção
      - Expande com keyboard_arrow_down
      - Clica "Notas Fiscais de Serviço"
      - Clica o botão de data [data-cy="EFD-DatePickerBTN"]
    """
    # 1) Login
    _save_report_json(log_dir, meta={"stage": "login", "detail": "Preenchendo e-mail..."})
    _type_safely(page, 'input#usuario, input[autocomplete="username"], input[placeholder="E-mail"]', W12_EMAIL)

    _save_report_json(log_dir, meta={"stage": "login", "detail": "Preenchendo senha..."})
    _type_safely(page, 'input#senha, input[autocomplete="current-password"], input[type="password"]', W12_PASSWORD)

    _save_report_json(log_dir, meta={"stage": "login", "detail": "Clicando em Entrar..."})
    ok = _click_by_text_candidates(page, ["^\\s*Entrar\\s*$", "Entrar"])
    if not ok:
        # fallback: localizar por estrutura do Angular Material
        if not _click_if_present(page, 'button.mat-button-base:has-text("Entrar")', timeout_ms=8000):
            raise RuntimeError("Botão 'Entrar' não foi encontrado.")

    # 2) Pós login: seta dropdown (arrow_drop_down)
    _save_report_json(log_dir, meta={"stage": "pos-login", "detail": "Aguardando menu (arrow_drop_down)..."})
    page.locator('i.material-icons:has-text("arrow_drop_down")').first.wait_for(state="visible", timeout=30000)
    page.locator('i.material-icons:has-text("arrow_drop_down")').first.click(timeout=5000)

    # 3) Abre o mat-select (seta do select)
    _save_report_json(log_dir, meta={"stage": "pos-login", "detail": "Abrindo seletor de unidade..."})
    if not _click_if_present(page, "div.mat-select-arrow-wrapper", timeout_ms=8000):
        # alguns temas usam o gatilho no container do mat-select
        if not _click_if_present(page, "mat-select .mat-select-trigger", timeout_ms=8000):
            raise RuntimeError("Não foi possível abrir o seletor (mat-select).")

    # 4) Campo de pesquisa "Pesquisar" e digitar "shopping tijuca"
    _save_report_json(log_dir, meta={"stage": "pos-login", "detail": "Pesquisando 'shopping tijuca'..."})
    _type_safely(page, 'input.pesquisar-dropdrown[placeholder="Pesquisar"], input[placeholder="Pesquisar"]', "shopping tijuca")

    # 5) Selecionar opção "BT TIJUC - Shopping Tijuca - 11" (match por 'Shopping Tijuca' para ser robusto)
    _save_report_json(log_dir, meta={"stage": "pos-login", "detail": "Selecionando unidade 'Shopping Tijuca'..."})
    opt = page.locator('div.p-x-xs.p-y-sm:has-text("Shopping Tijuca")')
    opt.first.wait_for(state="visible", timeout=15000)
    opt.first.click(timeout=5000, force=True)

    # 6) Clique no ícone keyboard_arrow_down (expansão)
    _save_report_json(log_dir, meta={"stage": "pos-login", "detail": "Expandindo menu (keyboard_arrow_down)..."})
    page.locator('i.material-icons:has-text("keyboard_arrow_down")').first.wait_for(state="visible", timeout=20000)
    page.locator('i.material-icons:has-text("keyboard_arrow_down")').first.click(timeout=5000)

    # 7) Menu "Notas Fiscais de Serviço"
    _save_report_json(log_dir, meta={"stage": "pos-login", "detail": "Abrindo 'Notas Fiscais de Serviço'..."})
    if not _click_by_text_candidates(page, ["Notas Fiscais de Serviço"]):
        # alguns temas usam span.nav-text
        if not _click_if_present(page, 'span.nav-text:has-text("Notas Fiscais de Serviço")', timeout_ms=8000):
            raise RuntimeError("Não foi possível abrir 'Notas Fiscais de Serviço'.")

    # 8) Botão de Data (data-cy="EFD-DatePickerBTN")
    _save_report_json(log_dir, meta={"stage": "pos-login", "detail": "Abrindo seletor de data..."})
    btn = page.locator('[data-cy="EFD-DatePickerBTN"]')
    btn.first.wait_for(state="visible", timeout=20000)
    btn.first.click(timeout=5000)

    _save_report_json(log_dir, meta={"stage": "done", "detail": "Fluxo executado com sucesso até o seletor de data."})


# ======================================
# Função principal (chamada pelo app.py)
# ======================================

def run_rpa_enter_google_folder(base_dir: str, target_dir: str, log_dir: str) -> None:
    """
    Mantém assinatura esperada pelo app.py:
        run_rpa_enter_google_folder(extract_dir, target_folder, BASE_DIR)

    Comportamento:
      - Abre o Chromium via Playwright (headed).
      - Navega para o TARGET_URL.
      - Executa o fluxo do W12 descrito acima.
      - Atualiza 'last_report.json' no diretório 'log_dir' (no seu app é o BASE_DIR).
    """
    _dbg(log_dir, "[rpa] Iniciando (abrindo navegador automaticamente).")
    _save_report_json(log_dir, meta={"stage": "start", "detail": "Inicializando RPA e abrindo navegador..."})

    # Compat: checagem do ZIP local (não obrigatório para este fluxo)
    try:
        local_zip = os.path.join(DEFAULT_UPLOAD_DIR, "arquivos.zip")
        if os.path.isfile(local_zip):
            _dbg(log_dir, f"[stub] Usando ZIP local existente: {local_zip}")
        else:
            _dbg(log_dir, f"[stub] ZIP não encontrado em {local_zip} (ok para este fluxo).")
    except Exception as e:
        _dbg(log_dir, f"[stub] Falha ao verificar ZIP local: {e!r}")

    # Abre Playwright e navega
    browser = None
    p = None
    try:
        from playwright.sync_api import sync_playwright
        p = sync_playwright().start()

        # Args úteis: viewport None (usa janela “nativa”), disable-gpu em alguns ambientes Windows ajuda
        browser = p.chromium.launch(
            headless=False,
            args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--start-maximized",
            ],
        )
        context = browser.new_context(viewport=None)
        page = context.new_page()

        _save_report_json(log_dir, meta={"stage": "nav", "detail": f"Abrindo {TARGET_URL}..."})
        page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=60000)

        # Aguarda um sinal do formulário de login
        _wait_visible(page, 'input#usuario, input[autocomplete="username"], input[placeholder="E-mail"]', timeout_ms=30000)

        # Executa o fluxo
        _do_w12_flow(page, log_dir)

        # (Opcional) manter aberto para inspeção — aqui vamos apenas deixar aberto.
        _dbg(log_dir, "[rpa] Fluxo concluído. Janela permanecerá aberta para inspeção.")

    except Exception as e:
        _save_report_json(log_dir, meta={"stage": "error", "detail": f"Exceção no fluxo: {e}"})
        _dbg(log_dir, f"[rpa] Exceção no fluxo: {e!r}")

        # Tenta tirar um screenshot de erro
        try:
            if 'page' in locals():
                path = os.path.join(log_dir, f"screenshot_erro_{int(time.time())}.png")
                page.screenshot(path=path, full_page=True)
                _dbg(log_dir, f"[rpa] Screenshot de erro salvo em: {path}")
        except Exception as e2:
            _dbg(log_dir, f"[rpa] Falha ao salvar screenshot de erro: {e2!r}")

    finally:
        # Não fechamos automaticamente o browser para facilitar debug manual.
        # Se quiser fechar ao final, descomente as linhas abaixo:
        # try:
        #     browser.close()
        # except Exception:
        #     pass
        try:
            if p:
                p.stop()
        except Exception:
            pass
