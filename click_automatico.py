from playwright.sync_api import sync_playwright
import time

URL = "https://fluxword.com"
USER = "admin"
PASS = "admin123"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()

    print("[1] Acessando página inicial...")
    page.goto(URL, wait_until="domcontentloaded")

    if page.locator("#username").count() > 0:
        print("[2] Fazendo login...")
        page.fill("#username", USER)
        page.fill("#password", PASS)
        page.click("button[type=submit]")
        page.wait_for_load_state("networkidle")

    print("[3] Esperando botão aparecer...")
    page.wait_for_selector("#btn-start", timeout=30000)

    print("[4] Clicando no botão 'Iniciar Processo'...")
    page.click("#btn-start")

    print("[5] Aguardando overlay aparecer...")
    page.wait_for_selector("#overlay", state="visible", timeout=15000)

    print("[6] Aguardando overlay sumir (processo terminar)...")
    for i in range(600):  # até 10 minutos
        try:
            el = page.query_selector("#overlay")
            if not el:
                print(f"[7] Overlay não encontrado após {i} segundos → processo concluído.")
                break
            classes = el.get_attribute("class") or ""
            if "hidden" in classes:
                print(f"[7] Overlay sumiu após {i} segundos → processo concluído.")
                break
        except Exception:
            # Pode ocorrer se o elemento for removido entre o get e a leitura
            print(f"[7] Overlay desapareceu (tratado) → processo concluído.")
            break
        time.sleep(1)
    else:
        print("[!] Tempo limite atingido, overlay não sumiu.")


    # Diagnóstico final
    msg = page.inner_text("#upload-msg")
    print("[diag] Mensagem final:", msg)

    browser.close()

print("✅ Script completo finalizado com sucesso.")
