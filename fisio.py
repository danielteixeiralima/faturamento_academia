# -*- coding: utf-8 -*-
import asyncio
from playwright.async_api import async_playwright

EMAIL = "paulocalargef@gmail.com"
SENHA = "@12345"

async def _run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        await page.goto("https://app.zenfisio.com/")
        await asyncio.sleep(1)

        for _ in range(2):
            await page.keyboard.press("Tab")

        await page.keyboard.type(EMAIL)
        await asyncio.sleep(1)

        await page.keyboard.press("Tab")
        await asyncio.sleep(1)

        await page.keyboard.type(SENHA)
        await asyncio.sleep(1)

        for _ in range(5):
            await page.keyboard.press("Tab")

        await page.keyboard.press("Enter")
        await asyncio.sleep(3)

        await browser.close()

def run_rpa_enter_google_folder(extract_dir: str, target_folder: str, base_dir: str) -> None:
    asyncio.run(_run())

if __name__ == "__main__":
    asyncio.run(_run())
