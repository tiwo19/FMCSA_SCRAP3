

# --- RESTORED PREVIOUS WORKING VERSION: MULTI-WORKER ENRICHMENT ---
import json
try:
    import nest_asyncio
    nest_asyncio.apply()
except ImportError:
    print("[ENRICH][WARN] nest_asyncio not installed, may fail in nested event loop environments.")

import time
import random
from pathlib import Path
from scrape_fmcsa_playwright import extract_active_insurance_details, solve_recaptcha_2captcha
import asyncio
from playwright.async_api import async_playwright


# --- Production config ---
TWO_CAPTCHA_API_KEY = "2f361c440d14c4c56ae93cb13ccc38d3"
INPUT_JSON = "fmcsa_register_enriched.json"
OUTPUT_JSON = "fmcsa_register_enriched.json"
BATCH_SIZE = 100
BATCH_SIZE = 10
MAX_WORKERS = 3


async def enrich_insurance_for_mc_async(mc_entry):
    insurance_url = mc_entry.get("insurance_link")
    mc_number = mc_entry.get("mc_number")
    print(f"[ENRICH][DEBUG] MC: {mc_number}, insurance_url: {insurance_url}")
    if not insurance_url or not mc_number:
        print(f"[ENRICH][DEBUG] Skipping MC: {mc_number} due to missing insurance_url or mc_number")
        return mc_entry

    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-gpu',
                    '--disable-software-rasterizer',
                    '--disable-extensions',
                    '--disable-background-networking',
                    '--disable-background-timer-throttling',
                    '--disable-breakpad',
                    '--disable-client-side-phishing-detection',
                    '--disable-default-apps',
                    '--disable-hang-monitor',
                    '--disable-popup-blocking',
                    '--disable-prompt-on-repost',
                    '--disable-sync',
                    '--metrics-recording-only',
                    '--no-first-run',
                    '--safebrowsing-disable-auto-update',
                    '--enable-automation',
                    '--password-store=basic',
                    '--use-mock-keychain',
                ]
            )
        except Exception as e:
            print(f"[ENRICH][ERROR] Failed to launch browser for {mc_number}: {e}")
            return mc_entry
        page = await browser.new_page()
        try:
            print(f"[ENRICH][DEBUG] Browser launched for {mc_number}, navigating to {insurance_url}")
            await page.goto(insurance_url, timeout=120000)
            await page.wait_for_load_state('networkidle', timeout=120000)
            digits = ''.join(filter(str.isdigit, mc_number))
            input_found = False
            try:
                await page.wait_for_selector('input[name="n_docketno"]', timeout=5000)
                await page.fill('input[name="n_docketno"]', digits)
                try:
                    await page.select_option('select[name="s_prefix"]', 'MC')
                except Exception:
                    pass
                input_found = True
            except Exception:
                pass
            if not input_found:
                try:
                    await page.wait_for_selector('input[name="n_dotno"]', timeout=5000)
                    await page.fill('input[name="n_dotno"]', digits)
                    input_found = True
                except Exception:
                    pass
            if input_found:
                try:
                    frame = await page.query_selector('iframe[src*="recaptcha"]')
                    if frame:
                        print(f"[ENRICH] CAPTCHA detected for {mc_number}, solving...")
                        token = await solve_recaptcha_2captcha(page, TWO_CAPTCHA_API_KEY)
                        if token:
                            print(f"[ENRICH] CAPTCHA solved for {mc_number}, token: {token}")
                            # Optionally inject token if needed (site may require this)
                            # await page.evaluate(f'document.getElementById("g-recaptcha-response").innerHTML = "{token}";')
                        else:
                            print(f"[ENRICH] CAPTCHA failed for {mc_number}")
                    else:
                        print(f"[ENRICH] No CAPTCHA detected for {mc_number}")
                except Exception as e:
                    print(f"[ENRICH] CAPTCHA solve error for {mc_number}: {e}")
                try:
                    submit_btn = await page.query_selector('input[type="submit"]')
                    if submit_btn:
                        await submit_btn.click()
                        await page.wait_for_load_state('networkidle', timeout=60000)
                        await asyncio.sleep(2)
                    else:
                        print(f"[ENRICH] Submit button not found for {mc_number}")
                except Exception as e:
                    print(f"[ENRICH] Submit failed for {mc_number}: {e}")
            else:
                print(f"[ENRICH] No MC or USDOT input found for {mc_number}, assuming already on results page.")
            await asyncio.sleep(2)
            try:
                html_btn_form = await page.query_selector('form[action*="pkg_carrquery.prc_getdetail"]')
                if html_btn_form:
                    print(f"[ENRICH] Found HTML button for {mc_number}, clicking...")
                    await html_btn_form.evaluate('form => form.submit()')
                    await page.wait_for_load_state('networkidle', timeout=30000)
                    # Add extra delay to allow details page to fully load
                    await asyncio.sleep(6)
                else:
                    print(f"[ENRICH] No HTML button found for {mc_number}, may already be on details page.")
            except Exception as e:
                print(f"[ENRICH] Error clicking HTML button for {mc_number}: {e}")
            # Add delay before searching for insurance forms
            await asyncio.sleep(4)
            try:
                # DEBUG: List all form actions on the page
                forms = await page.query_selector_all('form')
                print(f"[ENRICH][DEBUG] Found {len(forms)} forms on page for {mc_number}")
                for idx, form in enumerate(forms):
                    action = await form.get_attribute('action')
                    print(f"[ENRICH][DEBUG] Form {idx} action: {action}")
                # Wait a bit more if no forms found (page may still be loading)
                if len(forms) == 0:
                    print(f"[ENRICH][DEBUG] No forms found, waiting extra 3 seconds for {mc_number}")
                    await asyncio.sleep(3)
                    forms = await page.query_selector_all('form')
                    print(f"[ENRICH][DEBUG] After wait, found {len(forms)} forms on page for {mc_number}")
                    for idx, form in enumerate(forms):
                        action = await form.get_attribute('action')
                        print(f"[ENRICH][DEBUG] Form {idx} action: {action}")
                active_pending_form = await page.query_selector('form[action*="prc_activeinsurance"]')
                if active_pending_form:
                    print(f"[ENRICH] Found Active/Pending Insurance button for {mc_number}, clicking...")
                    await active_pending_form.evaluate('form => form.submit()')
                    await page.wait_for_load_state('networkidle', timeout=30000)
                    # Add extra delay to allow insurance page to fully load
                    await asyncio.sleep(6)
                else:
                    print(f"[ENRICH] No Active/Pending Insurance button for {mc_number}, scraping current page.")
            except Exception as e:
                print(f"[ENRICH] Error clicking Active/Pending Insurance for {mc_number}: {e}")
            insurance_html = await page.content()
            print(f"[ENRICH][DEBUG] HTML content length for {mc_number}: {len(insurance_html)}")
            debug_path = f"insurance_debug_{mc_number.replace('/', '_').replace(' ', '_')}.html"
            with open(debug_path, "w", encoding="utf-8") as f:
                f.write(insurance_html)
            print(f"[ENRICH][DEBUG] Saved HTML for {mc_number} to {debug_path}")
            insurance_data = extract_active_insurance_details(insurance_html)
            print(f"[ENRICH][DEBUG] Extracted insurance data for {mc_number}: {insurance_data}")
            # Flatten insurance_data directly into mc_entry with correct naming
            mc_entry["insurance_form"] = insurance_data.get("Form", "")
            mc_entry["insurance_type"] = insurance_data.get("Type", "")
            mc_entry["insurance_insurance_carrier"] = insurance_data.get("Insurance Carrier", "")
            mc_entry["insurance_policy_surety"] = insurance_data.get("Policy/Surety", "")
            mc_entry["insurance_posted_date"] = insurance_data.get("Posted Date", "")
            mc_entry["insurance_effective_date"] = insurance_data.get("Effective Date", "")
            mc_entry["insurance_cancellation_date"] = insurance_data.get("Cancellation Date", "")
            mc_entry["insurance_insurance_status"] = insurance_data.get("insurance_status", "")
            coverage = insurance_data.get("Coverage", {})
            if isinstance(coverage, dict):
                mc_entry["insurance_coverage_from"] = coverage.get("From", "")
                mc_entry["insurance_coverage_to"] = coverage.get("To", "")
            else:
                mc_entry["insurance_coverage_from"] = ""
                mc_entry["insurance_coverage_to"] = ""
            # Remove all flat insurance_* fields except insurance_link (to avoid stale data)
            for k in list(mc_entry.keys()):
                if k.startswith("insurance_") and k != "insurance_link" and k not in [
                    "insurance_form", "insurance_type", "insurance_insurance_carrier", "insurance_policy_surety",
                    "insurance_posted_date", "insurance_effective_date", "insurance_cancellation_date",
                    "insurance_insurance_status", "insurance_coverage_from", "insurance_coverage_to"
                ]:
                    del mc_entry[k]
            print(f"[ENRICH] Insurance data updated for {mc_number}")
        except Exception as e:
            print(f"[ENRICH] Failed for {mc_number}: {e}")
        # No sleep in production
        await browser.close()
    return mc_entry

def load_entries():
    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        return json.load(f)

def save_entries(entries):
    import tempfile, os
    with tempfile.NamedTemporaryFile('w', delete=False, dir='.', encoding='utf-8') as tf:
        json.dump(entries, tf, indent=2)
        temp_json = tf.name
    os.replace(temp_json, OUTPUT_JSON)

def needs_enrichment(entry):
    insurance = entry.get('insurance', {})
    if not insurance or not isinstance(insurance, dict):
        return True
    if not insurance.get('insurance_status') or insurance.get('insurance_status') in ['N/A', '', None]:
        return True
    return False


async def main_async():

    entries = load_entries()
    updated = False
    # Find all MCs needing enrichment
    to_enrich = [(i, entry) for i, entry in enumerate(entries) if needs_enrichment(entry)]
    print(f"[ENRICH] Total MCs needing enrichment: {len(to_enrich)}")
    # Batch in groups of BATCH_SIZE
    import math
    batches = [to_enrich[i:i+BATCH_SIZE] for i in range(0, len(to_enrich), BATCH_SIZE)]
    for batch_num, batch in enumerate(batches, 1):
        print(f"[ENRICH] Processing batch {batch_num}/{len(batches)} with {len(batch)} MCs...")
        # Run up to MAX_WORKERS concurrently
        sem = asyncio.Semaphore(MAX_WORKERS)
        async def enrich_one(idx, entry):
            async with sem:
                mc_number = entry.get('mc_number')
                max_attempts = 3
                for attempt in range(1, max_attempts + 1):
                    print(f"[ENRICH][DEBUG] Enriching MC: {mc_number} (Attempt {attempt}/{max_attempts})")
                    enriched = await enrich_insurance_for_mc_async(entry)
                    if 'insurance' in enriched and (not enriched['insurance'] or not isinstance(enriched['insurance'], dict)):
                        del enriched['insurance']
                    key_fields = [
                        'insurance_form', 'insurance_type', 'insurance_insurance_carrier',
                        'insurance_policy_surety', 'insurance_posted_date', 'insurance_effective_date',
                        'insurance_cancellation_date', 'insurance_insurance_status',
                        'insurance_coverage_from', 'insurance_coverage_to'
                    ]
                    if any(enriched.get(k) for k in key_fields):
                        entries[idx] = enriched
                        print(f"[ENRICH][DEBUG] Finished MC: {mc_number} (Success)")
                        return True
                    else:
                        print(f"[ENRICH][DEBUG] MC: {mc_number} enrichment attempt {attempt} failed, retrying...")
                        await asyncio.sleep(3)
                entries[idx] = enriched
                print(f"[ENRICH][ERROR] MC: {mc_number} enrichment failed after {max_attempts} attempts.")
                return False
        # Run all in batch concurrently
        results = await asyncio.gather(*(enrich_one(idx, entry) for idx, entry in batch))
        if any(results):
            updated = True
            save_entries(entries)
            print(f"[ENRICH] Batch {batch_num} saved atomically.")
    if updated:
        print("[ENRICH] Insurance enrichment complete.")
    else:
        print("[ENRICH] No entries needed enrichment.")

if __name__ == "__main__":
    asyncio.run(main_async())
