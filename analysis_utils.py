import json
import glob
from pathlib import Path
from datetime import datetime, date
import functools
import pandas as pd
from collections import defaultdict

DATA_DIR = Path("data")
SPEECH_FOLDER = DATA_DIR / "text_data/"
TOPIC_SCORE_FOLDER = DATA_DIR / "topic_scores/"
RATES_FILE = DATA_DIR / "price_data/2025-10-26 Fed Funds 12M 6M Historical Swap Rates.xlsx"
EMBEDDING_FILE = DATA_DIR / "speeches_with_embeddings.json"

START_DATE = datetime(2018, 6, 1)

def load_topic_scores_by_sid(path=TOPIC_SCORE_FOLDER):

    json_files = glob.glob(str(path) + "/*.json")
    scores = {}
    speeches = load_speeches()

    for json_file in json_files:
        with open(json_file, "r", encoding="utf-8") as f:
            raw = json.load(f)

        for row in raw:
            sid = row["id"]
            if sid not in speeches:
                continue
            scores[sid] = row["gpt-5"]

    return scores

def load_topic_scores_by_date(path=TOPIC_SCORE_FOLDER, apply_average=True):

    json_files = glob.glob(str(path) + "/*.json")
    scores = {}
    speeches = load_speeches()

    for json_file in json_files:
        with open(json_file, "r", encoding="utf-8") as f:
            raw = json.load(f)

        for row in raw:
            sid = row["id"]
            if sid not in speeches:
                continue
            date = speeches[sid]["date"]
            if date not in scores:
                scores[date] = []

            scores[date] += [row["gpt-5"]]

    if apply_average:
        final_scores = {}
        for date, values in scores.items():
            score_dict = {}
            for value in values:
                for topic in value:
                    if topic not in score_dict:
                        score_dict[topic] = 0
                    score_dict[topic] += value[topic]/len(values)
            final_scores[date] = score_dict
    else:
        final_scores = {}
        for date in scores:
            final_scores[date] = scores[date][-1]

    return final_scores

def parse_date(dstr: str) -> date:
    """
    Try several common date formats and return a datetime.date.
    Adjust/add formats if your data differs.
    """
    dstr = dstr.strip()
    formats = [
        "%Y-%m-%d",       # 2023-08-25
        "%Y/%m/%d",       # 2023/08/25
        "%Y-%m-%dT%H:%M:%S",  # 2023-08-25T00:00:00
        "%B %d, %Y",      # August 25, 2023
        "%b %d, %Y",      # Aug 25, 2023
    ]
    for fmt in formats:
        try:
            return datetime.strptime(dstr, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized date format: {dstr}")


@functools.lru_cache(maxsize=None)
def load_speeches(path=SPEECH_FOLDER):
    json_files = glob.glob(str(path) + "/*.json")

    speeches = {}

    for json_file in json_files:
        with open(json_file, "r", encoding="utf-8") as f:
            raw = json.load(f)

        for row in raw:

            sid = row["id"]
            date = parse_date(row["date"])
            if date < START_DATE:
                continue

            speeches[sid] = {
                "author": json_file.split("/")[-1].split(".")[0],
                "text": row["text"],
                "date": parse_date(row["date"]),
            }
    return speeches


@functools.lru_cache(maxsize=None)
def load_speeches_with_embeddings(path=EMBEDDING_FILE):

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    return raw

def load_rates(path=RATES_FILE):

    df = pd.read_excel(path)
    df["Date"] = df["Date"].apply(lambda x: str(x).split(" ")[0])
    df["Date"] = df["Date"].apply(parse_date)
    df = df.set_index("Date").sort_index()
    df = df[["Rate"]]
    df.index = pd.to_datetime(df.index)
    df["Rate_Change"] = df["Rate"].diff()

    speech_by_dates = group_speeches_by_date(load_speeches())
    dates = pd.to_datetime(list(speech_by_dates.keys()))

    df = df.reindex(df.index.union(dates))
    df = df.ffill()
    df = df.loc[dates]
    return df

def group_speeches_by_date(speeches):
    speeches_by_date = defaultdict(list)
    for sid, info in speeches.items():
        d = info["date"]
        speeches_by_date[d].append(sid)
    speeches_by_date = dict(sorted(speeches_by_date.items(), key=lambda x: x[0]))
    return speeches_by_date

# Build global_idx the same way you did before building graphs
def build_global_indices(speeches, topic_scores, rates_df):
    # 1) Authors
    author_names = sorted({v["author"] for v in speeches.values()})
    author2idx = {name: i for i, name in enumerate(author_names)}

    # 2) Topics
    topic_names = set()
    for sid, topics in topic_scores.items():
        for tname in topics.keys():
            topic_names.add(tname)
    topic_names = sorted(topic_names)
    topic2idx = {name: i for i, name in enumerate(topic_names)}

    # 3) Speech ids
    speech_ids = sorted(speeches.keys())
    speech2idx = {sid: i for i, sid in enumerate(speech_ids)}

    # 4) Dates
    all_dates = sorted(set(rates_df.index))  # dates where we have rates
    date2idx = {d: i for i, d in enumerate(all_dates)}

    return {
        "author2idx": author2idx,
        "topic2idx": topic2idx,
        "speech2idx": speech2idx,
        "date2idx": date2idx,
        "dates": all_dates,
    }