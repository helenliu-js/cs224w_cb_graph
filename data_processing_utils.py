from dotenv import load_dotenv
import io
import pdfplumber
from dateutil import parser

import json
import os
from tqdm import tqdm
import requests
from bs4 import BeautifulSoup
import re

load_dotenv()
fed_prints_api_key = os.getenv("FED_PRINTS_KEY")

def query_fed_prints_by_author(author_name):


    url = f"https://fedinprint.org/api/author/{author_name}/items"
    params = {
        "limit": 10000
    }
    headers = {
        "x-api-key": fed_prints_api_key
    }
    response = requests.get(url, params=params, headers=headers)
    data = response.json()
    print("Total Records", len(data["records"]))
    return data

def get_author_short_name(author_name):
    return author_name.split(":")[-1].split("-")[0]


def get_saved_ids(author_name):
    author_short_name = get_author_short_name(author_name)
    if not os.path.exists(f"text_data/{author_short_name}.json"):
        return []
    with open(f"text_data/{author_short_name}.json", "r", encoding="utf-8") as f:
        text_data = json.load(f)
        return list(map(lambda x: x["id"], text_data))

def extract_chicagofed_html(url):

    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    iso_date = ""
    date_div = soup.select_one("div.cfedDetail__lastUpdated")

    if date_div:
        txt = date_div.get_text(" ", strip=True)
        m = re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", txt)
        if m:
            raw_date = m.group(0)
            try:
                iso_date = parser.parse(raw_date).date().isoformat()
            except:
                iso_date = ""

    paragraphs = []

    for body in soup.select("div.cfedContent__body"):
        h = body.find("h3")
        if h:
            paragraphs.append(h.get_text(" ", strip=True))

        for txt_div in body.select("div.cfedContent__text"):
            for p in txt_div.find_all("p"):
                t = p.get_text(" ", strip=True)
                if t:
                    paragraphs.append(t)

    full_text = "\n\n".join(paragraphs)

    return {
        "date": iso_date,
        "text": full_text,
        "length": len(full_text)
    }


def extract_stlouisfed_html(url):

    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # ---------- DATE ----------
    iso_date = ""

    # Typically the first <p> after title block is the date
    date_tag = soup.select_one("div.component.content p")
    if date_tag:
        raw_date = date_tag.get_text(" ", strip=True)
        try:
            iso_date = parser.parse(raw_date).date().isoformat()
        except:
            iso_date = ""

    paragraphs = []
    body = soup.select_one("div.field-content div.wrapper")
    if body:
        for tag in body.find_all(["p", "h2", "h3"]):
            t = tag.get_text(" ", strip=True)
            if t:
                paragraphs.append(t)

    full_text = "\n\n".join(paragraphs)

    return {
        "date": iso_date,
        "text": full_text,
        "length": len(full_text),
    }


def _regional_banks():
    return ["www.newyorkfed.org", "www.federalreserve.gov", "www.dallasfed.org", "www.chicagofed.org",
            "www.clevelandfed.org", "www.stlouisfed.org", "www.bostonfed.org"]

def retrieve_remaining_ids(author_name, data):

    ids     = get_saved_ids(author_name)
    records = data["records"]

    idx = 0
    url_links = {}

    for record in records:

        if record["id"] in ids:
            continue

        if "file" not in record:
            continue

        excluded_patterns = [
            "fedlwp", "fedhwp", "fedcwp", "fedlrv", "fedgfn", "fedgfe",
            "fedlre", "fedlar", "fedcer", "fedles", "fedlps", "fedcwq",
            "fedfci", "fedkcc", "fedfmo", "fedmsr", "fedmwp", "fedmem",
            "fednls", "fedlcb", "fedgsq", "fedgrb", "fedbcp", "fedlpr",
            "fedfar", "fedaer", "fedgpr", "fedfel"]

        # Skip if any pattern matches
        if any(pattern in record["id"] for pattern in excluded_patterns):
            continue

        for file in record["file"]:

            file_function = file.get("filefunction", "")

            if "Video" in file_function:
                continue
            if "Figures" in file_function:
                continue
            if "Summary" in file_function:
                continue

            url = file["fileurl"]
            print(record["id"], file)

            if url.endswith(".pdf"):
                if "pdf" not in url_links:
                    url_links["pdf"] = {}
                url_links['pdf'][record["id"]] = url
            else:
                for regional_bank in _regional_banks():
                    if regional_bank in url:
                        bank_key = regional_bank.split(".")[1]
                        if bank_key not in url_links:
                            url_links[bank_key] = {}
                        url_links[bank_key][record["id"]] = url

            idx += 1

    print("Total Remaining Files", idx)
    return url_links, idx


DATE_PATTERNS = [
    r"[A-Z][a-z]+ \d{1,2}, \d{4}",     # October 17, 2019
    r"[A-Z][a-z]+ \d{1,2} \d{4}",      # October 17 2019
    r"\d{1,2} [A-Z][a-z]+ \d{4}",      # 17 October 2019 (ECB-style)
    r"\d{4}-\d{2}-\d{2}",              # 2019-10-17
]

def extract_date(text):
    first_500 = text[:500]  # restrict to beginning (where date always appears)

    for pattern in DATE_PATTERNS:
        match = re.search(pattern, first_500)
        if match:
            try:
                dt = parser.parse(match.group())
                return dt.strftime("%Y-%m-%d")
            except:
                pass

    return None  # fallback if no date found

def download_pdf(url):
    """Download PDF and return bytes (None if fail)."""
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        return io.BytesIO(r.content)
    except Exception as e:
        print(f"[ERROR] Failed downloading {url}: {e}")
        return None


def extract_pdf_text(pdf_bytes):
    """Extract text from PDF bytes using pdfplumber."""
    try:
        all_pages = []
        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                all_pages.append(text)
        return "\n\n".join(all_pages)
    except Exception as e:
        print("[ERROR] Failed parsing PDF:", e)
        return ""

def pdfs_to_json(url_list, output_json="speeches.json"):

    if os.path.exists(output_json):
        with open(output_json, "r", encoding="utf-8") as f:
            try:
                existing_data = json.load(f)
                if not isinstance(existing_data, list):
                    existing_data = []
            except json.JSONDecodeError:
                existing_data = []
    else:
        existing_data = []

    existing_ids = {entry["id"] for entry in existing_data}

    new_entries = []

    for url_id, url in tqdm(url_list.items(), desc="Processing PDFs"):
        if url_id in existing_ids:
            print(f"Skipping {url_id}: already exists in JSON.")
            continue

        pdf_bytes = download_pdf(url)
        if pdf_bytes is None:
            continue

        text = extract_pdf_text(pdf_bytes)

        new_entries.append({
            "id": url_id,
            "url": url,
            "text": text,
            "length": len(text),
            "date": extract_date(text),
            "parsing_from": "pdf"
        })

    combined = existing_data + new_entries

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, ensure_ascii=False)

    print(f"\nAppended {len(new_entries)} new entries → {output_json}")
    print(f"Total entries now: {len(combined)}")


def extract_board_html(url):

    r = requests.get(url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    date_tag = soup.find("p", class_="date")
    if date_tag:
        date = date_tag.get_text(" ", strip=True)
    else:
        p_tags = soup.find_all("p")
        date = p_tags[0].get_text(strip=True) if p_tags else ""

    article_div = soup.find("div", id="article")
    paragraphs = []

    if article_div:
        for p in article_div.find_all("p"):
            txt = p.get_text(" ", strip=True)
            if txt:
                paragraphs.append(txt)

    full_text = "\n\n".join(paragraphs)
    full_text = re.sub(r"\n{2,}", "\n\n", full_text).strip()

    return {
        "date": date,
        "text": full_text,
        "length": len(full_text)
    }


def extract_nyfed_html(url):

    r = requests.get(url)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    date_div = soup.find("div", class_="ts-contact-info")
    date_text = date_div.get_text(" ", strip=True) if date_div else ""

    article_div = soup.find("div", class_="ts-article-text")

    paragraphs = []
    if article_div:
        for p in article_div.find_all("p"):
            text = p.get_text(" ", strip=True)
            if text:
                paragraphs.append(text)

    full_text = "\n\n".join(paragraphs)
    full_text = re.sub(r"\n{2,}", "\n\n", full_text).strip()

    return {
        "date": date_text,
        "text": full_text,
        "length": len(full_text)
    }


def extract_bostonfed_html(url):

    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    iso_date = ""

    d = soup.select_one("div.date-container")
    if d:
        raw_date = d.get_text(" ", strip=True)
        try:
            iso_date = parser.parse(raw_date).date().isoformat()
        except:
            iso_date = ""

    # ---------- SPEECH TEXT ----------
    paragraphs = []

    for p in soup.select("div.bodytextlist p"):
        txt = p.get_text(" ", strip=True)
        if txt:
            paragraphs.append(txt)

    full_text = "\n\n".join(paragraphs)

    return {
        "date": iso_date,
        "text": full_text,
        "length": len(full_text)
    }

def html_speeches_to_json(url_dict, url_type, output_json="speeches.json"):

    if os.path.exists(output_json):
        with open(output_json, "r", encoding="utf-8") as f:
            try:
                existing_data = json.load(f)
                if not isinstance(existing_data, list):
                    existing_data = []
            except json.JSONDecodeError:
                existing_data = []
    else:
        existing_data = []

    existing_ids = {entry["id"] for entry in existing_data}

    new_entries = []

    for url_id, url in tqdm(url_dict.items(), desc="Processing HTML Speeches"):

        if url_id in existing_ids:
            print(f"Skipping {url_id}: already in JSON.")
            continue

        if url_type == "newyorkfed":
            parsed = extract_nyfed_html(url)
        elif url_type == "federalreserve":
            parsed = extract_board_html(url)
        elif url_type == "dallasfed":
            parsed = extract_dallasfed_html(url)
        elif url_type == "chicagofed":
            parsed = extract_chicagofed_html(url)
        elif url_type == "clevelandfed":
            parsed = extract_clevelandfed_html(url)
        elif url_type == "philadelphiafed":
            parsed = extract_philadelphiafed_html(url)
        elif url_type == "stlouisfed":
            parsed = extract_stlouisfed_html(url)
        elif url_type == "bostonfed":
            parsed = extract_bostonfed_html(url)
        else:
            raise ValueError("Unknown url type")

        new_entries.append({
            "id": url_id,
            "url": url,
            "text": parsed.get("text"),
            "length": parsed.get("length"),
            "date": parsed.get("date"),
            "parsing_from": "html"
        })

    # --- Save ---
    combined = existing_data + new_entries

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, ensure_ascii=False)

    print(f"\nAppended {len(new_entries)} new HTML entries → {output_json}")
    print(f"Total entries now: {len(combined)}")


def extract_dallasfed_html(url):
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    date_box = soup.select_one("div.dal-inline-list")
    iso_date = ""

    if date_box:

        text = date_box.get_text(" ", strip=True)
        m = re.search(r"[A-Za-z]+\s+\d{1,2},\s+\d{4}", text)
        if m:
            raw_date = m.group(0)
            try:
                iso_date = parser.parse(raw_date).date().isoformat()
            except:
                iso_date = ""

    main = soup.select_one("div.dal-main-content")
    parts = []

    if main:
        for tag in main.find_all(["h1", "h2", "h3", "p"]):
            t = tag.get_text(" ", strip=True)
            if t:
                parts.append(t)

    full_text = "\n".join(parts)

    return {
        "date": iso_date,
        "text": full_text,
        "length": len(full_text),
    }


def extract_clevelandfed_html(url):
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")


    iso_date = ""
    date_tag = soup.select_one("span.field-release-date")
    if date_tag:
        raw_date = date_tag.get_text(" ", strip=True)
        raw_date = raw_date.strip()
        try:
            iso_date = parser.parse(raw_date.replace(".", "/")).date().isoformat()
        except:
            iso_date = ""

    # ---------- MAIN TEXT ----------
    paragraphs = []

    # This is the real speech container
    rich_text_blocks = soup.select("div.component.rich-text div.component-content")

    for block in rich_text_blocks:
        for tag in block.find_all(["p", "h2", "h3"]):
            text = tag.get_text(" ", strip=True)
            if text:
                paragraphs.append(text)

    full_text = "\n\n".join(paragraphs)

    return {
        "date": iso_date,
        "text": full_text,
        "length": len(full_text)
    }

def extract_philadelphiafed_html(url):
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # ---------- TITLE ----------
    iso_date = ""

    date_tag = soup.select_one(".article__meta-date")
    if date_tag:

        raw_date = date_tag.get_text(" ", strip=True).strip()
        raw_date_clean = raw_date.replace("’", "'")
        try:
            iso_date = parser.parse(raw_date_clean).date().isoformat()
        except:
            iso_date = ""

    # ---------- SPEECH TEXT ----------
    paragraphs = []

    body = soup.select_one("div.article-body")
    if body:
        for tag in body.find_all(["p", "h2", "h3"]):
            t = tag.get_text(" ", strip=True)
            if t:
                paragraphs.append(t)

    full_text = "\n\n".join(paragraphs)

    return {
        "date": iso_date,
        "text": full_text,
        "length": len(full_text),
    }
