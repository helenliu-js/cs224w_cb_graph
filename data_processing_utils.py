from dotenv import load_dotenv
import os
import io
import json
import requests
import pdfplumber
from tqdm import tqdm
import re

from dateutil import parser

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
    print(len(data["records"]))
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
        if "fedlwp" in record["id"]:
            continue
        if "fedhwp" in record["id"]:
            continue
        if "fedcwp" in record["id"]:
            continue
        if "fedlrv" in record["id"]:
            continue
        if "fedgfn" in record["id"]:
            continue
        if "fedgfe" in record["id"]:
            continue
        if "fedlre" in record["id"]:
            continue
        if "fedlar" in record["id"]:
            continue
        if "fedcer" in record["id"]:
            continue
        if "fedles" in record["id"]:
            continue
        if "fedlps" in record["id"]:
            continue
        if "fedcwq" in record["id"]:
            continue
        if "fedfci" in record["id"]:
            continue
        if "fedkcc" in record["id"]:
            continue
        if "fedfmo" in record["id"]:
            continue

        for file in record["file"]:
            print("file", file)

            url = file["fileurl"]
            if url.endswith(".pdf"):
                url_links[record["id"]] = url

        idx += 1

    print(idx)
    print(len(url_links))
    return url_links


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
    results = []

    for url_id, url in tqdm(url_list.items(), desc="Processing PDFs"):
        pdf_bytes = download_pdf(url)
        if pdf_bytes is None:
            continue

        text = extract_pdf_text(pdf_bytes)
        results.append({
            "id": url_id,
            "url": url,
            "text": text,
            "length": len(text),
            "date": extract_date(text)
        })

    # save as JSON
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(results)} entries → {output_json}")
