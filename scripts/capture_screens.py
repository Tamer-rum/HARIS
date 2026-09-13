"""Capture console screenshots for the README, the deck and the video storyboard.

Needs a running console and Playwright driving the locally installed Chrome
(no browser download):

    pip install playwright
    START-HARIS.bat                                   # or: streamlit run app.py
    python scripts/capture_screens.py                 # -> docs/screenshots/

Each capture runs in a fresh browser session and presses the same buttons a
judge would: the autonomous storm cycle, then the field-intervention demo.
"""
from __future__ import annotations

import argparse
from pathlib import Path

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]


def open_tab(page, name: str) -> None:
    # The console sections are a horizontal st.radio; the first match is its label.
    page.get_by_text(name, exact=True).first.click()
    page.wait_for_timeout(2_000)


def press(page, label: str) -> None:
    page.get_by_text(label, exact=True).first.click()


def main() -> int:
    ap = argparse.ArgumentParser(description="Capture HARIS console screenshots.")
    ap.add_argument("--out", default=str(ROOT / "docs" / "screenshots"))
    ap.add_argument("--url", default="http://localhost:8501")
    ap.add_argument("--height", type=int, default=1850, help="viewport height in CSS pixels")
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chrome", headless=True,
            args=["--use-gl=swiftshader", "--enable-webgl", "--ignore-gpu-blocklist"],
        )
        # Streamlit scrolls inside its own container, so the viewport height
        # decides how much of the page a screenshot can show.
        ctx = browser.new_context(viewport={"width": 1600, "height": args.height}, device_scale_factor=1.25)
        page = ctx.new_page()
        page.goto(args.url, wait_until="load", timeout=120_000)
        page.get_by_text("OVERVIEW", exact=True).first.wait_for(timeout=120_000)
        page.wait_for_timeout(5_000)
        page.screenshot(path=str(out / "console_overview.png"))
        print("captured console_overview.png")

        open_tab(page, "AUTONOMOUS OPERATIONS")
        press(page, "RUN AUTONOMOUS HARIS")
        page.wait_for_selector(".trace", timeout=180_000)
        page.wait_for_timeout(4_000)
        page.screenshot(path=str(out / "console_storm.png"), full_page=True)
        trace = page.query_selector(".trace")
        if trace:
            trace.screenshot(path=str(out / "trace_storm.png"))
        print("captured console_storm.png, trace_storm.png")

        deck = page.locator('[data-testid="stDeckGlJsonChart"]').first
        deck.wait_for(timeout=60_000)
        deck.evaluate("e => e.scrollIntoView({block: 'center'})")
        page.wait_for_timeout(8_000)  # basemap tiles and deck.gl layers
        top = page.get_by_text("STORM MAP", exact=False).first.bounding_box()
        decks = page.locator('[data-testid="stDeckGlJsonChart"]')
        bottom = decks.nth(decks.count() - 1).bounding_box()
        page.screenshot(path=str(out / "storm_map.png"), clip={
            "x": 0, "y": top["y"] - 16, "width": page.viewport_size["width"],
            "height": bottom["y"] + bottom["height"] - top["y"] + 32,
        })
        print("captured storm_map.png")
        open_tab(page, "AUTONOMOUS OPERATIONS")

        press(page, "RUN FIELD INTERVENTION DEMO")
        page.wait_for_timeout(8_000)
        open_tab(page, "TRUSTED DISPATCH")
        page.wait_for_timeout(3_000)
        page.screenshot(path=str(out / "trusted_dispatch.png"), full_page=True)
        print("captured trusted_dispatch.png")

        open_tab(page, "HISTORY & AUDIT")
        page.wait_for_timeout(3_000)
        page.screenshot(path=str(out / "history_audit.png"), full_page=True)
        print("captured history_audit.png")
        ctx.close()
        browser.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
