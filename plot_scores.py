from datetime import datetime
import json

def parse_date(s: str):
    formats = [
        "%Y-%m-%d",      # 2020-10-13
        "%B %d, %Y",     # November 13, 2009
    ]

    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    print(s)
    raise ValueError(f"Unknown date format: {s}")


def find_date_by_id(author_key, select_id):
    with open(f"data/text_data/{author_key}.json", "r", encoding="utf-8") as f:
        text_data = json.load(f)
        selected_text = list(filter(lambda x: x["id"]==select_id, text_data))[0]
        date = selected_text["date"]
        return parse_date(date)


import json
author_key = "waller"
topic = "Fed Funds Rate"

import plotly.graph_objects as go

score_curve = {}
with open(f"info_folder/score_{author_key}.json", "r", encoding="utf-8") as f:
    score_data = json.load(f)
    for row in score_data:
        date = find_date_by_id(author_key, row["id"])
        topic_score = row["gpt-5"][topic]
        score_curve[date] = topic_score

score_curve = dict(sorted(score_curve.items(), key=lambda x: x[0]))
fig = go.Figure()

fig.add_trace(go.Scatter(
    x=list(score_curve.keys()),
    y=list(score_curve.values()),
    mode='lines+markers',
    name=f"{author_key} – {topic}",
    line=dict(width=2)
))

fig.update_layout(
    title=f"{author_key.capitalize()} – {topic} Score Over Time",
    xaxis_title="Date",
    yaxis_title="Score",
    template="plotly_white"
)

fig.show()