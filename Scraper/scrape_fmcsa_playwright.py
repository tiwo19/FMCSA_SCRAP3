async def fetch_sms_registration_details(page, usdot):
    """
    Navigates to the SMS Overview page for the given USDOT, clicks Carrier Registration Details,
    and extracts all registration fields (including email) from the popup/modal.
    Returns a dict of extracted fields.
    """
    import re
    from bs4 import BeautifulSoup
    # --- SMS URL ---
    sms_url = f"https://ai.fmcsa.dot.gov/SMS/Carrier/{usdot}/Overview.aspx?FirstView=True"
    # --- STEALTH & USER-AGENT ---
    try:
        from playwright_stealth import stealth_async
        await stealth_async(page)
    except Exception:
        print("[SMS][WARN] playwright_stealth not installed, continuing without stealth.")
    await page.set_extra_http_headers({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    })
    await page.goto(sms_url, timeout=120000)
    await page.wait_for_load_state('networkidle', timeout=120000)
    html = None
    max_retries = 3
    modal_selector = 'article#regInfo, .smsModal, .modal-content, aside#CarrierRegistration, #CarrierRegistrationModal'
    for attempt in range(1, max_retries + 1):
        try:
            # Remove overlays before click
            await page.evaluate('''for (const sel of ['.ajaxLoadingPnl', '.modal-backdrop', '#simplemodal-overlay']) { document.querySelectorAll(sel).forEach(e => e.remove()); }''')
            reg_link = await page.query_selector('a:has-text("Carrier Registration Details")')
            if not reg_link:
                reg_link = await page.query_selector('button:has-text("Carrier Registration Details")')
            if not reg_link:
                reg_link = await page.query_selector('a:has-text("Registration")')
            if reg_link:
                await reg_link.scroll_into_view_if_needed()
                await page.evaluate('''for (const sel of ['.ajaxLoadingPnl', '.modal-backdrop', '#simplemodal-overlay']) { document.querySelectorAll(sel).forEach(e => e.remove()); }''')
                try:
                    await page.wait_for_selector('.ajaxLoadingPnl', state='hidden', timeout=15000)
                except Exception:
                    print(f"[SMS][RETRY] ajaxLoadingPnl overlay did not disappear before click (attempt {attempt})")
                try:
                    await reg_link.click(timeout=30000)
                except Exception as e:
                    print(f"[SMS][RETRY] Normal click failed, trying force click (attempt {attempt}): {e}")
                    try:
                        await reg_link.click(timeout=30000, force=True)
                    except Exception as e2:
                        print(f"[SMS][RETRY] Force click also failed, trying JS click (attempt {attempt}): {e2}")
                        try:
                            await page.evaluate('(el) => el.click()', reg_link)
                        except Exception as e3:
                            print(f"[SMS][RETRY] JS click also failed (attempt {attempt}): {e3}")
                print(f"[SMS][RETRY] Clicked Carrier Registration Details (attempt {attempt})")
                await page.evaluate('''for (const sel of ['.ajaxLoadingPnl', '.modal-backdrop', '#simplemodal-overlay']) { document.querySelectorAll(sel).forEach(e => e.remove()); }''')
                # Wait for modal to appear after click
                try:
                    await page.wait_for_selector('article#regInfo, .smsModal, .modal-content, aside#CarrierRegistration, #CarrierRegistrationModal', timeout=10000)
                except Exception:
                    print(f"[SMS][RETRY] Modal did not appear after click (attempt {attempt})")
                await page.wait_for_timeout(2000)
                reginfo_content = None
                try:
                    reginfo_content = await page.query_selector('article#regInfo')
                except Exception:
                    reginfo_content = None
                modal_content = None
                if not reginfo_content:
                    try:
                        modal_content = await page.query_selector(modal_selector)
                    except Exception:
                        modal_content = None
                html_ok = False
                if reginfo_content:
                    try:
                        reginfo_html = await reginfo_content.inner_html()
                    except Exception:
                        reginfo_html = ''
                    if reginfo_html and len(reginfo_html) > 50:
                        html = await page.content()
                        html_ok = True
                    else:
                        print(f"[SMS][RETRY] article#regInfo found but empty or too short (attempt {attempt})")
                        print(f"[SMS][DEBUG] regInfo HTML (truncated): \n{reginfo_html[:500]}")
                elif modal_content:
                    try:
                        modal_html = await modal_content.inner_html()
                    except Exception:
                        modal_html = ''
                    if modal_html and len(modal_html) > 50:
                        html = await page.content()
                        html_ok = True
                    else:
                        print(f"[SMS][RETRY] Modal content blank, too short, or irrelevant (attempt {attempt})")
                        print(f"[SMS][DEBUG] Modal HTML (truncated): \n{modal_html[:500]}")
                if html_ok:
                    break  # Success
                else:
                    print(f"[SMS][RETRY] Modal not loaded, refreshing page and retrying (attempt {attempt})")
                    await page.goto(sms_url, timeout=120000)
                    try:
                        await page.wait_for_load_state('networkidle', timeout=120000)
                    except Exception:
                        print(f"[SMS][RETRY] Page did not reach networkidle after refresh (attempt {attempt})")
                    await page.wait_for_timeout(2000)
            else:
                print(f"[SMS][ERROR] Could not find Carrier Registration Details link/button for USDOT {usdot} (attempt {attempt})")
                await page.goto(sms_url, timeout=120000)
                await page.wait_for_load_state('networkidle', timeout=120000)
        except Exception as e:
            print(f"[SMS][ERROR] Could not open Carrier Registration Details for USDOT {usdot} (attempt {attempt}): {e}")
            await page.goto(sms_url, timeout=120000)
            try:
                await page.wait_for_load_state('networkidle', timeout=120000)
            except Exception:
                print(f"[SMS][RETRY] Page did not reach networkidle after refresh (error branch, attempt {attempt})")
            await page.wait_for_timeout(2000)
    # Fallback: open CarrierRegistration.aspx in a new tab/context if all retries fail
    if not html:
        # Fallback: open CarrierRegistration.aspx in a new tab/context
        fallback_url = f"https://ai.fmcsa.dot.gov/SMS/Carrier/{usdot}/CarrierRegistration.aspx"
        print(f"[SMS][FALLBACK] Trying direct CarrierRegistration.aspx for USDOT {usdot}: {fallback_url}")
        for fallback_attempt in range(1, 4):
            try:
                context = page.context
                new_page = await context.new_page()
                try:
                    from playwright_stealth import stealth_async as stealth_async_fallback
                    await stealth_async_fallback(new_page)
                except Exception:
                    pass
                await new_page.set_extra_http_headers({
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                })
                await new_page.goto(fallback_url, timeout=60000)
                await new_page.wait_for_load_state('networkidle', timeout=60000)
                await new_page.evaluate('''for (const sel of ['.ajaxLoadingPnl', '.modal-backdrop', '#simplemodal-overlay']) { document.querySelectorAll(sel).forEach(e => e.remove()); }''')
                reg_html = await new_page.content()
                if "Legal Name:" in reg_html or "MCS-150 Date:" in reg_html:
                    html = reg_html
                    await new_page.close()
                    print(f"[SMS][FALLBACK] Successfully loaded CarrierRegistration.aspx for USDOT {usdot} (attempt {fallback_attempt})")
                    break
                else:
                    print(f"[SMS][FALLBACK] Fallback page content blank or irrelevant (attempt {fallback_attempt})")
                    await new_page.close()
            except Exception as e:
                print(f"[SMS][FALLBACK][ERROR] Could not load fallback CarrierRegistration.aspx for USDOT {usdot} (attempt {fallback_attempt}): {e}")
            await page.wait_for_timeout(3000 * fallback_attempt)
        if not html:
            html = await page.content()

    # Parse the HTML for registration fields (prefer table/DOM extraction)
    soup = BeautifulSoup(html, 'html.parser')
    reg_info = {k: '' for k in [
        'mcs_150_date', 'legal_name', 'dba_name', 'usdot', 'address', 'telephone', 'fax', 'email',
        'vehicle_miles_traveled', 'vmt_year', 'power_units', 'drivers', 'carrier_operation']}

    # Try to find registration info in <ul class="col1"> and <ul class="col2"> lists
    def normalize_label(label):
        return label.strip().replace(':', '').replace('\xa0', ' ').lower()

    label_map = {
        'mcs-150 date': 'mcs_150_date',
        'legal name': 'legal_name',
        'dba name': 'dba_name',
        'u.s. dot#': 'usdot',
        'address': 'address',
        'telephone': 'telephone',
        'fax': 'fax',
        'email': 'email',
        'vehicle miles traveled': 'vehicle_miles_traveled',
        'vmt year': 'vmt_year',
        'power units': 'power_units',
        'drivers': 'drivers',
        'carrier operation': 'carrier_operation',
    }
    found_labels = set()
    found_any = False
    for col_class in ['col1', 'col2']:
        for ul in soup.find_all('ul', class_=col_class):
            for li in ul.find_all('li'):
                label_tag = li.find('label')
                value_tag = li.find('span', class_='dat')
                if label_tag and value_tag:
                    label = normalize_label(label_tag.get_text())
                    value = value_tag.get_text(strip=True)
                    # Only assign if value is not empty, label is recognized, and value is not a label itself
                    if label in label_map and value and not value.lower().endswith('dot#:') and value != label:
                        reg_info[label_map[label]] = value
                        found_labels.add(label)
                        found_any = True
                    elif label in label_map and not value:
                        # Don't assign blank values, leave as default
                        continue
                    else:
                        print(f"[SMS][WARN] Unrecognized label in SMS modal: '{label}' (value: '{value}')")
    # Special: MCS-150 Date is in the <h3> header as 'Carrier Registration Information (MCS-150 Date: <span>02/05/2025)</span>'
    regbox = soup.find(id='regBox')
    if regbox:
        h3 = regbox.find('h3')
        if h3 and 'MCS-150 Date:' in h3.get_text():
            import re
            m = re.search(r'MCS-150 Date:\s*([\d/]+)', h3.get_text())
            if m:
                reg_info['mcs_150_date'] = m.group(1)
                found_any = True
    # Fallback: if usdot is still missing, use MC digits
    if not reg_info['usdot']:
        digits = ''.join(filter(str.isdigit, usdot))
        reg_info['usdot'] = digits
    # Debug: if not all fields found, print all found labels and values
    missing = [k for k, v in reg_info.items() if not v]
    if missing:
        print(f"[SMS][DEBUG] Missing SMS fields after <ul> extraction: {missing}")
        print("[SMS][DEBUG] All found labels:")
        for col_class in ['col1', 'col2']:
            for ul in soup.find_all('ul', class_=col_class):
                for li in ul.find_all('li'):
                    label_tag = li.find('label')
                    value_tag = li.find('span', class_='dat')
                    if label_tag and value_tag:
                        print(f"{label_tag.get_text(strip=True)}: {value_tag.get_text(strip=True)}")
    # If nothing found, fallback to table logic (legacy)
    if not found_any:
        table = None
        for t in soup.find_all('table'):
            ths = [th.get_text(strip=True) for th in t.find_all('th')]
            if any('Legal Name' in th for th in ths) and any('MCS-150 Date' in th for th in ths):
                table = t
                break
        if table:
            for row in table.find_all('tr'):
                th = row.find('th')
                td = row.find('td')
                if th and td:
                    label = normalize_label(th.get_text())
                    value = td.get_text(strip=True)
                    if label in label_map and value and not value.endswith(":"):
                        reg_info[label_map[label]] = value
                        found_labels.add(label)
                    else:
                        print(f"[SMS][WARN] Unrecognized label in SMS modal (table fallback): '{label}' (value: '{value}')")
            # MCS-150 Date in table header?
            ths = table.find_all('th')
            for th in ths:
                if 'MCS-150 Date:' in th.get_text():
                    import re
                    m = re.search(r'MCS-150 Date:\s*([\d/]+)', th.get_text())
                    if m:
                        reg_info['mcs_150_date'] = m.group(1)

    # Fallback: try regex on text if table extraction failed for any field, but only assign if value is not just a label
    text = soup.get_text("\n", strip=True)
    patterns = {
        'mcs_150_date': r'MCS-150 Date:\s*([\d/]+)',
        'legal_name': r'Legal Name:\s*([^\n]*)',
        'dba_name': r'DBA Name:\s*([^\n]*)',
        'usdot': r'U\.S\. DOT#: \s*(\d+)',
        'address': r'Address:\s*([^\n]*)',
        'telephone': r'Telephone:\s*([\(\)\d\- ]+)',
        'fax': r'Fax:\s*([^\n]*)',
        'email': r'Email:\s*([\w\.-]+@[\w\.-]+)',
        'vehicle_miles_traveled': r'Vehicle Miles Traveled:\s*([\d,]+)',
        'vmt_year': r'VMT Year:\s*(\d+)',
        'power_units': r'Power Units:\s*(\d+)',
        'drivers': r'Drivers:\s*(\d+)',
        'carrier_operation': r'Carrier Operation:\s*([^\n]*)',
    }
    for key, pat in patterns.items():
        if not reg_info[key]:
            m = re.search(pat, text)
            if m:
                val = m.group(1).strip()
                # Only assign if value is not just a label (e.g., not 'Email:')
                if val and not val.endswith(":"):
                    reg_info[key] = val
                    print(f"[SMS][DEBUG] Filled '{key}' using regex fallback: {reg_info[key]}")

    # Final check: if email is still missing, log a warning for manual review
    if not reg_info['email']:
        print(f"[SMS][WARN] Email field missing after all extraction attempts for USDOT {usdot}")

    print(f"[SMS][DEBUG] Extracted registration info for USDOT {usdot}: {reg_info}")
    if not reg_info['legal_name'] or not reg_info['usdot']:
        print(f"[SMS][DEBUG] Modal HTML for USDOT {usdot} (truncated):\n{html[:1000]}")
    return reg_info

import asyncio
import json
import pandas as pd
from playwright.async_api import async_playwright
from datetime import datetime, timedelta
from twocaptcha import TwoCaptcha
import os
import concurrent.futures
import math



async def solve_recaptcha_2captcha(page, api_key):
    # Detect sitekey
    frame = await page.query_selector('iframe[src*="recaptcha"]')
    if not frame:
        print("[2Captcha] No reCAPTCHA iframe found.")
        return None
    # Get sitekey from the iframe src
    src = await frame.get_attribute('src')
    import re
    m = re.search(r'[?&]k=([\w-]+)', src)
    if not m:
        print("[2Captcha] No sitekey found in iframe src.")
        return None
    sitekey = m.group(1)
    url = page.url
    print(f"[2Captcha] Solving reCAPTCHA for sitekey: {sitekey} on {url}")
    solver = TwoCaptcha(api_key)
    import concurrent.futures
    async def captchaSolver(sitekey, url):
        loop = asyncio.get_running_loop()
        with concurrent.futures.ThreadPoolExecutor() as pool:
            result = await loop.run_in_executor(pool, lambda: solver.recaptcha(sitekey=sitekey, url=url))
            return result
    try:
        result = await captchaSolver(sitekey, url)
        token = result['code']
        print("[2Captcha] Got token from 2Captcha.")
        # Inject token into the page
        await page.evaluate('''(token) => {
            document.getElementById('g-recaptcha-response').style.display = '';
            document.getElementById('g-recaptcha-response').value = token;
            let el = document.getElementsByName('g_recaptcha_response');
            if (el && el.length > 0) el[0].value = token;
        }''', token)
        await page.wait_for_timeout(2000)
        return token
    except Exception as e:
        print(f"[2Captcha] Error solving reCAPTCHA: {e}")
        return None

REGISTER_URL = "https://li-public.fmcsa.dot.gov/LIVIEW/pkg_REGISTER.prc_reg_list"
SAFER_URL_TEMPLATE = "https://safer.fmcsa.dot.gov/CompanySnapshot.aspx?mc_num={}"

OUTPUT_CSV = "fmcsa_register_enriched.csv"
OUTPUT_JSON = "fmcsa_register_enriched.json"

# --- Utility Functions ---
#Converts any date string to YYYY-MM-DD format for consistency.
def normalize_date(date_str):
    try:
        return pd.to_datetime(date_str).strftime("%Y-%m-%d")
    except Exception:
        return date_str

#'''Checks if the carrier’s decision date is within the last 30 days.
#Returns True if “new”, else False.
#Already handled in your pipeline by checking the decision date from the Register.
#Used to flag new MC numbers for your dashboard/report.'''
def is_new_mc(decision_date, days=30):
    try:
        date = pd.to_datetime(decision_date)
        return (datetime.now() - date).days <= days
    except Exception:
        return False

# --- Main Scraper Logic ---
# Fetch register dates from the FMCSA page
#Uses Playwright to open the FMCSA Register page.
#Finds all date rows in the table.
#Extracts the display date and the hidden pd_date value for each row.
#Returns a list of date objects: [{display: ..., pd_date: ...}, ...].
async def fetch_register_dates(page):
    print(f"[DEBUG] Navigating to {REGISTER_URL} (timeout=120s)...")
    try:
        await page.goto(REGISTER_URL, timeout=120000)
    except Exception as e:
        print(f"[ERROR] Timeout or error navigating to {REGISTER_URL}: {e}")
        # Save screenshot and HTML for debugging
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        screenshot_path = f"register_debug_{ts}_goto.png"
        html_path = f"register_debug_{ts}_goto.html"
        try:
            await page.screenshot(path=screenshot_path)
            html = await page.content()
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"[DEBUG] Saved screenshot to {screenshot_path} and HTML to {html_path}")
        except Exception as ee:
            print(f"[DEBUG] Could not save screenshot or HTML: {ee}")
        return []
    # Set a realistic user-agent
    await page.set_extra_http_headers({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    })
    try:
        await page.wait_for_selector('table', timeout=60000)
    except Exception as e:
        print(f"[DEBUG] Table not found after 60s: {e}")
        # Save screenshot and HTML for debugging
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        screenshot_path = f"register_debug_{ts}.png"
        html_path = f"register_debug_{ts}.html"
        await page.screenshot(path=screenshot_path)
        html = await page.content()
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"[DEBUG] Saved screenshot to {screenshot_path} and HTML to {html_path}")
        # Check for CAPTCHA or interstitial
        if 'captcha' in html.lower():
            print("[DEBUG] CAPTCHA detected on register page. Manual intervention or 2Captcha needed.")
        else:
            print("[DEBUG] No table and no obvious CAPTCHA. Check HTML dump.")
        return []
    rows = await page.query_selector_all('table tr')
    dates = []
    for row in rows:
        th = await row.query_selector('th')
        if th:
            date_text = (await th.inner_text()).strip()
            pd_date_input = await row.query_selector('input[name="pd_date"]')
            if pd_date_input:
                pd_date = (await pd_date_input.get_attribute('value')).strip()
                dates.append({'display': date_text, 'pd_date': pd_date})
    return dates
#----Navigates to the register page and submits the HTML Detail form for a specific date.----
#Waits for the detail page to load.
#Extracts the HTML content of the detail page.
#Parses the HTML content into a pandas DataFrame.
#Returns a list of DataFrames, one for each carrier.
#Extracts carrier number, title, and decision date from each row.
#Filters tables to only those with at least 3 columns and MC/FF numbers.
async def fetch_register_details(page, pd_date):
    # Submit the form for the given date
    await page.goto(REGISTER_URL, timeout=60000)
    try:
        await page.wait_for_selector('form[action*="prc_reg_detail"]', timeout=60000)
    except Exception as e:
        print(f"[REGISTER] Timeout or error waiting for register detail form: {e}")
        return []
    forms = await page.query_selector_all('form[action*="prc_reg_detail"]')
    found = False
    for form in forms:
        input_val = await form.query_selector('input[name="pd_date"]')
        if input_val and (await input_val.get_attribute('value')).strip() == pd_date:
            await form.evaluate('form => form.submit()')
            try:
                await page.wait_for_load_state('networkidle', timeout=60000)
            except Exception as e:
                print(f"[REGISTER] Timeout or error waiting for register detail page: {e}")
                return []
            found = True
            break
    if not found:
        print(f"[REGISTER] No matching form found for pd_date {pd_date}")
        return []
    # Parse sections and entries
    from io import StringIO
    html = await page.content()
    df_list = pd.read_html(StringIO(html))
    # Only keep tables with at least 3 columns and MC/FF numbers
    carrier_tables = [df for df in df_list if df.shape[1] >= 3 and df.iloc[:,0].astype(str).str.contains(r'MC-|FF-', regex=True).any()]
    entries = []
    for table in carrier_tables:
        for _, row in table.iterrows():
            number = str(row.iloc[0]).strip()
            # Normalize MC number: remove trailing -C, -R, -A, etc. and extra spaces
            import re
            number = re.sub(r"\s*-[A-Z]$", "", number)
            number = number.replace("  ", " ").strip()
            title = str(row.iloc[1]).strip()
            date = str(row.iloc[2]).strip()
            # Extract company name and state from title (format: 'COMPANY NAME - CITY, STATE')
            if '-' in title and ',' in title:
                name_part, city_state = title.rsplit('-', 1)
                company_name = name_part.strip()
                city_state = city_state.strip()
                if ',' in city_state:
                    city, state = city_state.rsplit(',', 1)
                    state = state.strip()
                else:
                    state = ''
            else:
                company_name = title.strip()
                state = ''
            # Filter for WA/OR only
            if state in ['WA', 'OR']:
                entries.append({
                    'mc_number': number,
                    'company_name': company_name,
                    'state': state,
                    'decision_date': date
                })
    return entries

async def fetch_safer_snapshot(page, mc_number):
    # Always reload the SAFER query page before each search
    import random
    # Add random delay to avoid rate-limiting
    await asyncio.sleep(random.uniform(2, 5))
    await page.goto("https://safer.fmcsa.dot.gov/CompanySnapshot.aspx", timeout=120000)
    await page.wait_for_selector('input[name="query_param"][value="MC_MX"]', timeout=120000)
    await page.check('input[name="query_param"][value="MC_MX"]')
    digits = ''.join(filter(str.isdigit, mc_number))
    await page.fill('input[name="query_string"]', digits)
    await page.click('input[type="SUBMIT"]')
    await page.wait_for_load_state('networkidle', timeout=120000)
    print(f"[SAFER] Queried MC/MX Number: {digits}")
    html = await page.content()
    print(f"[SAFER] First 500 chars of result HTML: {html[:500]}")

    # Use BeautifulSoup for robust parsing
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, 'html.parser')
    safer_data = {}

    # --- USDOT Status (Operating Status) ---
    usdot_status = ''
    usdot_status_label = soup.find('th', string=lambda s: s and 'USDOT Status' in s)
    if usdot_status_label:
        td = usdot_status_label.find_next('td')
        if td:
            usdot_status = td.get_text(strip=True)
    safer_data['usdot_status'] = usdot_status

    # --- Extract USDOT number for SMS lookup ---
    usdot = ''
    usdot_label = soup.find('th', string=lambda s: s and 'USDOT Number' in s)
    if usdot_label:
        td = usdot_label.find_next('td')
        if td:
            usdot = td.get_text(strip=True)
    # Fallback: try to extract from MC number if not found
    if not usdot:
        digits = ''.join(filter(str.isdigit, mc_number))
        usdot = digits

    # --- SMS Registration Details (Email, etc) ---
    try:
        sms_details = await fetch_sms_registration_details(page, usdot)
        # Always set all SMS fields, use SMS value if present (even if blank)
        sms_fields = [
            'mcs_150_date', 'legal_name', 'dba_name', 'usdot', 'address',
            'telephone', 'fax', 'email', 'vehicle_miles_traveled', 'vmt_year',
            'power_units', 'drivers', 'carrier_operation'
        ]
        for k in sms_fields:
            safer_data[k] = sms_details.get(k, '')
        print(f"[DEBUG][SAFER] After SMS extraction for MC {mc_number}, safer_data keys: {list(safer_data.keys())}")
        print(f"[DEBUG][SAFER] SMS details for MC {mc_number}: {sms_details}")
    except Exception as e:
        print(f"[SMS][ERROR] Could not fetch SMS registration details for MC {mc_number}: {e}")
    # --- Insurance Link (reference only, no extraction) ---
    insurance_link = ''
    for a in soup.find_all('a', href=True):
        href = a['href']
        if 'pkg_carrquery.prc_carrlist' in href and 'n_dotno=' in href:
            insurance_link = href
            if not insurance_link.startswith('http'):
                insurance_link = 'https://li-public.fmcsa.dot.gov' + insurance_link if insurance_link.startswith('/') else 'https://li-public.fmcsa.dot.gov/LIVIEW/' + insurance_link
            break
    if not insurance_link:
        for a in soup.find_all('a', href=True):
            parent = a.find_parent()
            if parent and 'For Licensing and Insurance details' in parent.get_text():
                insurance_link = a['href']
                if not insurance_link.startswith('http'):
                    insurance_link = 'https://li-public.fmcsa.dot.gov' + insurance_link if insurance_link.startswith('/') else 'https://li-public.fmcsa.dot.gov/LIVIEW/' + insurance_link
                break
    if not insurance_link:
        insurance_a = soup.find('a', string=lambda s: s and 'For Licensing and Insurance details' in s)
        if insurance_a:
            insurance_link = insurance_a.get('href', '')
            if not insurance_link.startswith('http'):
                insurance_link = 'https://li-public.fmcsa.dot.gov' + insurance_link if insurance_link.startswith('/') else 'https://li-public.fmcsa.dot.gov/LIVIEW/' + insurance_link
    safer_data['insurance_link'] = insurance_link if insurance_link else ''
    print(f"[SAFER][DEBUG] MC: {mc_number}, insurance_link: {insurance_link}")
    if not insurance_link:
        print(f"[SAFER][DEBUG] MC: {mc_number} - insurance link NOT FOUND. Dumping HTML snippet:")
        print(html[:1000])
    # Do NOT extract or follow insurance details. Only keep the link for reference.
    return safer_data

    # --- Safety Rating ---
    safety_rating = ''
    rating_table = None
    for table in soup.find_all('table'):
        if table.find('th', string=lambda s: s and 'Review Information' in s):
            rating_table = table
            break
    if rating_table:
        for tr in rating_table.find_all('tr'):
            ths = tr.find_all('th')
            tds = tr.find_all('td')
            if ths and tds:
                if 'Rating:' in ths[0].get_text():
                    safety_rating = tds[0].get_text(strip=True)
    safer_data['safety_rating'] = safety_rating

    # --- Contact Info ---
    def get_field(label):
        th = soup.find('th', string=lambda s: s and label in s)
        if th:
            td = th.find_next('td')
            if td:
                return td.get_text(separator=' ', strip=True)
        return ''
    safer_data['physical_address'] = get_field('Physical Address')
    safer_data['mailing_address'] = get_field('Mailing Address')
    safer_data['phone'] = get_field('Phone')

    # --- Fleet Size ---
    safer_data['power_units'] = get_field('Power Units')
    safer_data['drivers'] = get_field('Drivers')

    # --- Out-of-Service Percentages ---
    # Find the Inspections table (look for header row with 'Inspection Type')
    oos_percentages = {'vehicle': '', 'driver': '', 'hazmat': ''}
    inspections_table = None
    for table in soup.find_all('table'):
        header = table.find('th', string=lambda s: s and 'Inspection Type' in s)
        if header:
            inspections_table = table
            break
    if inspections_table:
        rows = inspections_table.find_all('tr')
        for row in rows:
            th = row.find('th')
            if th and 'Out of Service %' in th.get_text():
                tds = row.find_all('td')
                if len(tds) >= 3:
                    oos_percentages['vehicle'] = tds[0].get_text(strip=True)
                    oos_percentages['driver'] = tds[1].get_text(strip=True)
                    oos_percentages['hazmat'] = tds[2].get_text(strip=True)
    safer_data['oos_percent_vehicle'] = oos_percentages['vehicle']
    safer_data['oos_percent_driver'] = oos_percentages['driver']
    safer_data['oos_percent_hazmat'] = oos_percentages['hazmat']

    # --- Crash Data ---
    # Find the Crashes table (look for header row with 'Type', 'Fatal', 'Injury', 'Tow', 'Total')
    crash_data = {'fatal': '', 'injury': '', 'tow': '', 'total': ''}
    crash_table = None
    for table in soup.find_all('table'):
        header = table.find('th', string=lambda s: s and 'Type' in s)
        if header and table.find('th', string=lambda s: s and 'Fatal' in s):
            crash_table = table
            break
    if crash_table:
        for row in crash_table.find_all('tr'):
            th = row.find('th')
            if th and 'Crashes' in th.get_text():
                tds = row.find_all('td')
                if len(tds) >= 4:
                    crash_data['fatal'] = tds[0].get_text(strip=True)
                    crash_data['injury'] = tds[1].get_text(strip=True)
                    crash_data['tow'] = tds[2].get_text(strip=True)
                    crash_data['total'] = tds[3].get_text(strip=True)
    safer_data['crash_fatal'] = crash_data['fatal']
    safer_data['crash_injury'] = crash_data['injury']
    safer_data['crash_tow'] = crash_data['tow']
    safer_data['crash_total'] = crash_data['total']

    # --- Insurance Details Extraction (use robust extraction) ---
    # If insurance_link is present, follow and extract insurance info using extract_active_insurance_details
    insurance_data = {}
    if insurance_link:
        if insurance_link.startswith("/"):
            insurance_url = f"https://li-public.fmcsa.dot.gov{insurance_link}"
        elif insurance_link.startswith("http"):
            insurance_url = insurance_link
        else:
            insurance_url = f"https://li-public.fmcsa.dot.gov/LIVIEW/{insurance_link}"
        try:
            await page.goto(insurance_url, timeout=60000)
            await page.wait_for_load_state('networkidle', timeout=60000)
        except Exception as e:
            print(f"[INSURANCE] Error navigating to insurance_url: {e}")
            return safer_data
        # If a search form is present, fill and submit it
        try:
            usdot_input = await page.query_selector('input[name="pv_usdot"]')
            if usdot_input:
                await usdot_input.fill(digits)
                submit_btn = await page.query_selector('input[type="submit"]')
                if submit_btn:
                    await submit_btn.click()
                    await page.wait_for_load_state('networkidle', timeout=60000)
        except Exception as e:
            print(f"[INSURANCE] Form fill/submit failed: {e}")
        # Now on the details page, extract insurance info using extract_active_insurance_details
        insurance_html = await page.content()
        insurance_data = extract_active_insurance_details(insurance_html)
        # Try to follow and extract from Active/Pending Insurance first
        try:
            active_pending_btn = await page.query_selector('form[action*="prc_activeinsurance"]')
            if active_pending_btn:
                await active_pending_btn.evaluate('form => form.submit()')
                await page.wait_for_load_state('networkidle', timeout=60000)
                active_html = await page.content()
                insurance_data = extract_active_insurance_details(active_html)
        except Exception as e:
            print(f"[INSURANCE] Could not extract from Active/Pending Insurance: {e}")
        # If not found, try Insurance History
        if not insurance_data:
            try:
                history_btn = await page.query_selector('form[action*="prc_insurancehistory"]')
                if history_btn:
                    await history_btn.evaluate('form => form.submit()')
                    await page.wait_for_load_state('networkidle', timeout=60000)
                    history_html = await page.content()
                    insurance_data = extract_active_insurance_details(history_html)
            except Exception as e:
                print(f"[INSURANCE] Could not extract from Insurance History: {e}")
    safer_data.update(insurance_data)
    # Place insurance data under 'insurance' key in output
    safer_data['insurance'] = insurance_data
    # Remove top-level insurance_status and insurance_expiration if present (now only in insurance object)
    if 'insurance_status' in safer_data:
        del safer_data['insurance_status']
    if 'insurance_expiration' in safer_data:
        del safer_data['insurance_expiration']
    # Always include insurance_status and insurance_expiration fields, default to 'N/A' if not found
    if 'insurance_status' not in safer_data:
        safer_data['insurance_status'] = 'N/A'
    if 'insurance_expiration' not in safer_data:
        safer_data['insurance_expiration'] = 'N/A'
    # Log if insurance info is missing
    if not insurance_link:
        print(f"[SAFER] No insurance link found for MC: {mc_number}")
    if safer_data['insurance_status'] == 'N/A' or safer_data['insurance_expiration'] == 'N/A':
        print(f"[SAFER] Insurance info missing for MC: {mc_number}")
    return safer_data

# --- Insurance Extraction Helper ---
from bs4 import BeautifulSoup
def extract_insurance_info(html):
    soup = BeautifulSoup(html, "html.parser")
    result = {}
    # US DOT, Docket, Legal Name
    details_table = soup.find("table", summary=lambda s: s and "formating purposes only" in s)
    if details_table:
        rows = details_table.find_all("tr")
        for row in rows:
            ths = row.find_all("th")
            tds = row.find_all("td")
            if ths and tds:
                if "US DOT" in ths[0].text:
                    result["usdot"] = tds[0].text.strip()
                if "Docket Number" in ths[-1].text:
                    result["docket_number"] = tds[-1].text.strip()
            if ths and "Legal Name" in ths[0].text:
                result["legal_name"] = row.find("td").text.strip()
    # Remove legacy authority, insurance, and BOC-3/Blanket Company extraction (handled in new logic)
    pass
    return result


# --- Active Insurance Extraction Helper ---
def extract_active_insurance_details(html):
    soup = BeautifulSoup(html, "html.parser")
    result = {}
    # --- Insurance Type Table ---
    insurance_types = []
    for table in soup.find_all('table'):
        headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
        if 'insurance type' in headers and 'insurance required' in headers and 'insurance on file' in headers:
            for row in table.find_all('tr')[1:]:
                cells = row.find_all(['th', 'td'])
                if len(cells) == 3:
                    typ = cells[0].get_text(strip=True)
                    required = cells[1].get_text(strip=True)
                    on_file = cells[2].get_text(strip=True)
                    insurance_types.append({
                        'type': typ,
                        'required': required,
                        'on_file': on_file
                    })
            break
    if insurance_types:
        result['insurance_types'] = insurance_types

    # --- Authority Type Table ---
    authority_types = []
    for table in soup.find_all('table'):
        headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
        if 'authority type' in headers and 'authority status' in headers and 'application pending' in headers:
            for row in table.find_all('tr')[1:]:
                cells = row.find_all(['th', 'td'])
                if len(cells) == 3:
                    authority_types.append({
                        'authority_type': cells[0].get_text(strip=True),
                        'authority_status': cells[1].get_text(strip=True),
                        'application_pending': cells[2].get_text(strip=True)
                    })
            break
    if authority_types:
        result['authority_types'] = authority_types

    # --- Property/Passenger/Household Goods/Private/Enterprise Table ---
    property_types = []
    for table in soup.find_all('table'):
        headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
        if 'property' in headers and 'passenger' in headers and 'household goods' in headers and 'private' in headers and 'enterprise' in headers:
            for row in table.find_all('tr')[1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) == 5:
                    property_types.append({
                        'property': cells[0].get_text(strip=True),
                        'passenger': cells[1].get_text(strip=True),
                        'household_goods': cells[2].get_text(strip=True),
                        'private': cells[3].get_text(strip=True),
                        'enterprise': cells[4].get_text(strip=True)
                    })
            break
    if property_types:
        result['property_types'] = property_types
    # Extract Form, Type, Carrier, Policy, Posted Date, Coverage, Effective Date, Cancellation Date
    # Look for a table with these headers
    for table in soup.find_all('table'):
        headers = [th.get_text(strip=True).lower() for th in table.find_all('th')]
        print(f"[EXTRACT DEBUG] Table headers: {headers}")
        if 'form' in headers and 'type' in headers and 'insurance carrier' in headers:
            for row in table.find_all('tr')[1:]:
                cells = row.find_all(['td', 'th'])
                print(f"[EXTRACT DEBUG] Row cell count: {len(cells)} | Values: {[c.get_text(strip=True) for c in cells]}")
                # Accept rows with at least 7 cells (sometimes trailing columns are missing)
                if len(cells) >= 7:
                    result['Form'] = cells[0].get_text(strip=True)
                    result['Type'] = cells[1].get_text(strip=True)
                    result['Insurance Carrier'] = cells[2].get_text(strip=True)
                    result['Policy/Surety'] = cells[3].get_text(strip=True)
                    result['Posted Date'] = cells[4].get_text(strip=True)
                    result['Coverage'] = {'From': cells[5].get_text(strip=True), 'To': cells[6].get_text(strip=True)}
                    # Defensive: Only set Effective/Cancellation if present
                    result['Effective Date'] = cells[7].get_text(strip=True) if len(cells) > 7 else ''
                    result['Cancellation Date'] = cells[8].get_text(strip=True) if len(cells) > 8 and cells[8].get_text(strip=True) else None
                    # Insurance status logic
                    if result.get('Cancellation Date'):
                        result['insurance_status'] = 'Lapsed'
                    else:
                        result['insurance_status'] = 'Active'
                    # Flag for renewal if Effective Date > 1 year ago
                    from datetime import datetime
                    try:
                        eff_date = pd.to_datetime(result.get('Effective Date', ''), errors='coerce')
                        if pd.notnull(eff_date) and (datetime.now() - eff_date).days > 365:
                            result['flag_renewal'] = True
                    except Exception:
                        pass
    return result


# --- Parallel Batch Processing ---
def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def flatten(entry):
    # Start with all top-level fields
    flat = {k: str(v) if v is not None else '' for k, v in entry.items()}

    # Ensure all SMS registration fields are included
    sms_fields = [
        'mcs_150_date', 'legal_name', 'dba_name', 'usdot', 'address',
        'telephone', 'fax', 'email', 'vehicle_miles_traveled', 'vmt_year',
        'power_units', 'drivers', 'carrier_operation'
    ]
    for field in sms_fields:
        sms_val = entry.get(field, '')
        # Clean up address newlines and whitespace for address fields
        if field == 'address' and sms_val:
            sms_val = str(sms_val).replace('\n', ' ').replace('\r', ' ').replace('  ', ' ').strip()
        # Only overwrite if sms_val is not empty
        if sms_val not in [None, '']:
            flat[field] = str(sms_val)
        # else: keep whatever is already in flat[field] (from top-level or previous update)

    # Debug: print SMS fields before export for validation
    print("[DEBUG][FLATTEN] Final SMS fields for MC {}:".format(flat.get('mc_number', '')))
    for field in sms_fields:
        print(f"  {field}: {flat.get(field, '')}")

    # Handle insurance data (keep your existing insurance logic)
    ins = entry.get('insurance', {})
    insurance_fields = {
        'insurance_form': 'Form',
        'insurance_type': 'Type',
        'insurance_insurance_carrier': 'Insurance Carrier',
        'insurance_policy_surety': 'Policy/Surety',
        'insurance_posted_date': 'Posted Date',
        'insurance_effective_date': 'Effective Date',
        'insurance_cancellation_date': 'Cancellation Date',
        'insurance_insurance_status': 'insurance_status',
        'insurance_coverage_from': ('Coverage', 'From'),
        'insurance_coverage_to': ('Coverage', 'To')
    }
    if isinstance(ins, dict) and ins:
        for flat_key, ins_key in insurance_fields.items():
            if isinstance(ins_key, tuple):
                # Handle nested fields like Coverage
                nested = ins.get(ins_key[0], {})
                flat[flat_key] = str(nested.get(ins_key[1], ''))
            else:
                flat[flat_key] = str(ins.get(ins_key, ''))
    else:
        # Initialize empty insurance fields
        for field in insurance_fields:
            flat[field] = ''
    return flat

async def process_mc_batch(mc_batch, base_entry_map):
    async with async_playwright() as p:
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
        async def process_one(mc_number):
            import re
            norm_mc = re.sub(r"\s*-[A-Z]$", "", mc_number)
            norm_mc = norm_mc.replace("  ", " ").strip()
            entry = base_entry_map.get(norm_mc, base_entry_map.get(mc_number, {})).copy()
            page = await browser.new_page()
            max_retries = 3
            for attempt in range(1, max_retries + 1):
                try:
                    print(f"[PARALLEL SAFER] Fetching for MC: {mc_number} (Attempt {attempt})")
                    safer_info = await fetch_safer_snapshot(page, mc_number)
                    entry.update(safer_info)
                    break
                except Exception as e:
                    print(f"Error fetching SAFER for {mc_number} (Attempt {attempt}): {e}")
                    if attempt < max_retries:
                        await asyncio.sleep(3 * attempt)
                    else:
                        print(f"[ERROR] Giving up on MC: {mc_number} after {max_retries} attempts.")
            await page.close()
            return entry
        tasks = [process_one(mc_number) for mc_number in mc_batch]
        results = await asyncio.gather(*tasks)
        await browser.close()
    return results

def process_mc_batch_sync(mc_batch, base_entry_map):
    # Wrapper for ProcessPoolExecutor (runs in a separate process)
    import asyncio
    return asyncio.run(process_mc_batch(mc_batch, base_entry_map))

def main_parallel():
    import asyncio
    import os
    import json
    from collections import defaultdict
    # Production batch size and workers for EC2 c6i.8xlarge
    batch_size = 100
    max_workers = 15
    progress_file = 'fmcsa_progress.jsonl'

    # Step 1: Scrape register dates and details (single process)
    async def get_all_entries():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()
            print("Fetching register dates...")
            dates = await fetch_register_dates(page)
            print(f"Found {len(dates)} dates.")

            all_entries = []
            # --- TEST: Only process the first date for speed ---
            if dates:
                date = dates[0]
                print(f"[TEST MODE] Only scraping details for {date['display']}...")
                try:
                    entries = await fetch_register_details(page, date['pd_date'])
                    print(f"[REGISTER] Sample entry for {date['display']}: {entries[0] if entries else 'No entries'}")
                    for entry in entries:
                        entry['register_date'] = date['display']
                        entry['is_new_mc'] = is_new_mc(entry['decision_date'])
                        all_entries.append(entry)
                except Exception as e:
                    print(f"Error scraping {date['display']}: {e}")
            await browser.close()
            return all_entries
    all_entries = asyncio.run(get_all_entries())

    # --- PRODUCTION: No test limits ---
    # Process all entries

    # Only keep ACTIVE MCs for enrichment
    # --- Normalize MC numbers for base_entry_map to prevent duplication ---
    def normalize_mc(mc):
        import re
        if not mc:
            return mc
        return re.sub(r"\s*-[A-Z]$", "", mc).replace("  ", " ").strip()

    base_entry_map = {normalize_mc(e['mc_number']): e for e in all_entries}
    mc_list = list(base_entry_map.keys())
    print(f"Total MCs to enrich: {len(mc_list)}")

    # --- Progress/Resume logic with re-enrichment for incomplete data ---
    def needs_reenrichment(entry):
        # Returns True if insurance or enrichment is missing/incomplete
        # Customize this logic as needed for your data completeness criteria
        insurance = entry.get('insurance', {})
        # Check for missing or empty insurance_status or insurance_expiration
        if not insurance or not isinstance(insurance, dict):
            return True
        if not insurance.get('insurance_status') or insurance.get('insurance_status') in ['N/A', '', None]:
            return True
        # You can add more checks for other required fields here
        return False

    processed_mcs = set()
    batch_results = []
    incomplete_mcs = set()
    progress_entries = {}
    if os.path.exists(progress_file):
        print(f"Resuming from progress file: {progress_file}")
        with open(progress_file, 'r', encoding='utf-8') as pf:
            for line in pf:
                try:
                    entry = json.loads(line)
                    mc = entry['mc_number']
                    progress_entries[mc] = entry
                    if needs_reenrichment(entry):
                        incomplete_mcs.add(mc)
                    else:
                        processed_mcs.add(mc)
                    batch_results.append(entry)
                except Exception:
                    continue
    # MCs to process: not processed, or incomplete in progress file
    mc_list_to_process = [mc for mc in mc_list if (mc not in processed_mcs) or (mc in incomplete_mcs)]
    print(f"MCs left to process (including incomplete): {len(mc_list_to_process)}")
    batches = list(chunked(mc_list_to_process, batch_size))
    print(f"Processing {len(batches)} batches with {max_workers} workers...")

    # Step 2: Parallel enrichment with incremental output
    import threading
    progress_lock = threading.Lock()
    def write_progress(entries):
        with progress_lock:
            with open(progress_file, 'a', encoding='utf-8') as pf:
                for entry in entries:
                    pf.write(json.dumps(entry) + '\n')

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_mc_batch_sync, batch, base_entry_map): idx for idx, batch in enumerate(batches)}
        for future in concurrent.futures.as_completed(futures):
            batch_result = future.result()
            print(f"Batch {futures[future]+1}/{len(batches)} done, {len(batch_result)} entries.")
            write_progress(batch_result)
            batch_results.extend(batch_result)

    # Only keep ACTIVE carriers
    filtered_entries = [entry for entry in batch_results if entry.get('usdot_status', '').upper() == 'ACTIVE']

    # Step 3: Normalize, deduplicate, and export
    flat_entries = [flatten(e) for e in filtered_entries]
    # Print MC and insurance_link before export for debug
    for entry in flat_entries:
        print(f"[EXPORT DEBUG] MC: {entry.get('mc_number')} insurance_link: {entry.get('insurance_link')}")
    # Ensure all insurance fields are present as columns
    insurance_fields = [
        'insurance_form', 'insurance_type', 'insurance_insurance_carrier', 'insurance_policy_surety',
        'insurance_posted_date', 'insurance_effective_date', 'insurance_cancellation_date', 'insurance_insurance_status',
        'insurance_coverage_from', 'insurance_coverage_to'
    ]
    # Add missing insurance columns as empty if not present
    for entry in flat_entries:
        for field in insurance_fields:
            if field not in entry:
                entry[field] = ''
        # Remove the raw 'insurance' dict from each entry if present
        if 'insurance' in entry:
            del entry['insurance']
    # Ensure all columns are strings for CSV export
    for entry in flat_entries:
        for k, v in entry.items():
            if v is None:
                entry[k] = ''
            elif not isinstance(v, str):
                entry[k] = str(v)
    import csv
    import tempfile
    df = pd.DataFrame(flat_entries)
    if not df.empty:
        # Deduplicate by normalized mc_number, keeping the record with a non-empty insurance_link if available
        def normalize_mc(mc):
            import re
            if not mc:
                return mc
            return re.sub(r"\s*-[A-Z]$", "", mc).replace("  ", " ").strip()

        deduped = {}
        for _, row in df.iterrows():
            mc = row.get('mc_number', '')
            norm_mc = normalize_mc(mc)
            if not norm_mc:
                continue
            def has_sms_fields(r):
                return bool(r.get('email')) or bool(r.get('legal_name')) or bool(r.get('mcs_150_date'))
            if norm_mc not in deduped:
                deduped[norm_mc] = row
                print(f"[DEDUP] Adding new MC {norm_mc}: email={row.get('email')}, legal_name={row.get('legal_name')}")
            else:
                current = deduped[norm_mc]
                if has_sms_fields(row) and not has_sms_fields(current):
                    deduped[norm_mc] = row
                    print(f"[DEDUP] Replacing MC {norm_mc} with enriched SMS: email={row.get('email')}, legal_name={row.get('legal_name')}")
                elif has_sms_fields(row) and has_sms_fields(current):
                    if not current.get('insurance_link') and row.get('insurance_link'):
                        deduped[norm_mc] = row
                        print(f"[DEDUP] Replacing MC {norm_mc} with better insurance_link: {row.get('insurance_link')}")
                elif not has_sms_fields(row) and has_sms_fields(current):
                    pass
                else:
                    if not current.get('insurance_link') and row.get('insurance_link'):
                        deduped[norm_mc] = row
                        print(f"[DEDUP] Replacing MC {norm_mc} with insurance_link: {row.get('insurance_link')}")
        df = pd.DataFrame(list(deduped.values()))
        if 'register_date' in df.columns:
            df['register_date'] = df['register_date'].apply(normalize_date)
        if 'decision_date' in df.columns:
            df['decision_date'] = df['decision_date'].apply(normalize_date)
        desired_order = [
            'mc_number', 'company_name', 'state', 'decision_date', 'register_date', 'is_new_mc', 'usdot_status',
            'safety_rating', 'physical_address', 'mailing_address', 'phone', 'power_units', 'drivers',
            'oos_percent_vehicle', 'oos_percent_driver', 'oos_percent_hazmat', 'crash_fatal', 'crash_injury',
            'crash_tow', 'crash_total',
            'insurance_link',
            'mcs_150_date', 'legal_name', 'dba_name', 'usdot', 'address', 'telephone', 'fax', 'email',
            'vehicle_miles_traveled', 'vmt_year', 'carrier_operation',
            'insurance_form', 'insurance_type', 'insurance_insurance_carrier', 'insurance_policy_surety',
            'insurance_posted_date', 'insurance_effective_date', 'insurance_cancellation_date',
            'insurance_insurance_status', 'insurance_coverage_from', 'insurance_coverage_to'
        ]
        for col in desired_order:
            if col not in df.columns:
                df[col] = ''
        df = df[desired_order]
        df = df.fillna('')
        for col in df.columns:
            df[col] = df[col].astype(str)
        print("[EXPORT][DEBUG] DataFrame head:")
        print(df.head())
        if len(df) > 4:
            print("[EXPORT][DEBUG] Sample row:")
            print(df.iloc[4].to_dict())
        else:
            print(f"[EXPORT][DEBUG] Not enough rows to print row 4. Row count: {len(df)}")
        records = df.to_dict(orient="records")
        # Atomic write for CSV
        import tempfile
        with tempfile.NamedTemporaryFile('w', delete=False, dir='.', encoding='utf-8', newline='') as tf:
            writer = csv.DictWriter(tf, fieldnames=desired_order, quoting=csv.QUOTE_ALL)
            writer.writeheader()
            for row in records:
                writer.writerow(row)
            temp_csv = tf.name
        import os
        os.replace(temp_csv, OUTPUT_CSV)
        # Atomic write for JSON
        with tempfile.NamedTemporaryFile('w', delete=False, dir='.', encoding='utf-8') as tf:
            import json
            json.dump(records, tf, indent=2)
            temp_json = tf.name
        os.replace(temp_json, OUTPUT_JSON)
        print(f"[EXPORT] Exported {len(df)} records to {OUTPUT_CSV} and {OUTPUT_JSON} (atomic write)")
    else:
        print("No records to export. DataFrame is empty.")


if __name__ == "__main__":
    main_parallel()
