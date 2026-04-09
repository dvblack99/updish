#!/usr/bin/env python3
"""
app.py — UpDish Chat API
Flask app that provides a /chat endpoint for the AI chatbot.
Runs in Docker on port 5055.
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import urllib.request
import urllib.error
import json
import os
import re

app = Flask(__name__)
CORS(app, origins=["https://davidjamesblack.com"])

DB_PATH = os.environ.get("DB_PATH", "/data/updish.db")
PROXY_URL = os.environ.get("PROXY_URL", "http://host.docker.internal:8181/v1/chat/completions")
MODEL = "grok-3-fast-beta"
TOKEN = os.environ.get("UPDISH_TOKEN", "updish")

SYSTEM_PROMPT = """You are a helpful assistant for UpDish, a Vancouver restaurant and café discovery tool.

You have access to a database of Vancouver food businesses including their details and website content.

When answering questions:
- Be specific and helpful — name actual businesses with addresses
- If you know a business has relevant menu items or features from their website, mention them
- Include website links when available using markdown: [Business Name](url)
- Keep answers concise and practical
- If asked about prices, hours, or menus, only state what you actually found in the data
- If you don't have enough data to answer confidently, say so

The user has filtered the map to a specific set of businesses. Only answer about those businesses unless the user explicitly asks about others."""


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def search_businesses(businesses, question):
    """
    Given a list of business dicts and a question,
    return the most relevant ones using keyword matching on
    structured_data and name.
    """
    q = question.lower()
    # Extract keywords (skip common words)
    stopwords = {"who", "what", "where", "when", "which", "how", "the", "a", "an",
                 "is", "are", "has", "have", "do", "does", "can", "best", "good",
                 "any", "for", "in", "on", "at", "to", "and", "or", "with"}
    keywords = [w for w in re.findall(r'\w+', q) if w not in stopwords and len(w) > 2]

    scored = []
    for b in businesses:
        score = 0
        search_text = (
            (b.get("name") or "") + " " +
            (b.get("structured_data_text") or "") + " " +
            (b.get("neighbourhood") or "")
        ).lower()

        for kw in keywords:
            if kw in search_text:
                score += 1

        if score > 0:
            scored.append((score, b))

    scored.sort(key=lambda x: -x[0])
    # Return top 20 most relevant
    return [b for _, b in scored[:20]]


def fetch_businesses_with_data(ids):
    """Fetch full business data including structured_data for specific IDs."""
    if not ids:
        return []
    conn = get_db()
    placeholders = ",".join("?" * len(ids))
    rows = conn.execute(f"""
        SELECT name, address, neighbourhood, type, website,
               structured_data, html_content
        FROM businesses
        WHERE id IN ({placeholders})
        AND licence_status = 'active'
    """, ids).fetchall()
    conn.close()

    result = []
    for r in rows:
        b = dict(r)
        if b.get("structured_data"):
            try:
                b["structured"] = json.loads(b["structured_data"])
            except json.JSONDecodeError:
                b["structured"] = None
        del b["structured_data"]
        del b["html_content"]
        result.append(b)
    return result


def build_context(businesses_full):
    """Build a concise context string for the AI from business data."""
    lines = []
    for b in businesses_full:
        s = b.get("structured") or {}
        line = f"**{b['name']}**"
        if b.get("address"):
            line += f" — {b['address']}"
        if b.get("neighbourhood"):
            line += f", {b['neighbourhood']}"
        if b.get("website"):
            line += f" | Website: {b['website']}"
        if s.get("price_range"):
            line += f" | Price: {s['price_range']}"
        if s.get("cuisines"):
            line += f" | Cuisine: {', '.join(s['cuisines'][:3])}"
        if s.get("description"):
            line += f"\n  {s['description']}"
        if s.get("highlights"):
            line += f"\n  Features: {', '.join(s['highlights'][:5])}"
        if s.get("menu_items"):
            items = s["menu_items"][:8]
            item_strs = [f"{i['name']} ({i.get('price','?')})" for i in items if i.get("name")]
            if item_strs:
                line += f"\n  Menu highlights: {', '.join(item_strs)}"
        if s.get("hours"):
            hours_str = ", ".join([f"{k}: {v}" for k, v in list(s["hours"].items())[:3]])
            line += f"\n  Hours: {hours_str}"
        lines.append(line)

    return "\n\n".join(lines)


def call_xai(messages):
    payload = json.dumps({
        "model": MODEL,
        "max_tokens": 800,
        "temperature": 0.3,
        "messages": messages
    }).encode()

    req = urllib.request.Request(
        PROXY_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-Token": TOKEN,
        }
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
        return data["choices"][0]["message"]["content"]


@app.route("/updish/api/chat", methods=["POST"])
def chat():
    try:
        body = request.get_json()
        question = (body.get("question") or "").strip()
        business_ids = body.get("business_ids") or []  # IDs of currently filtered businesses
        history = body.get("history") or []  # previous messages [{role, content}]

        if not question:
            return jsonify({"error": "No question provided"}), 400

        if not business_ids:
            return jsonify({"error": "No businesses in current filter"}), 400

        # Cap at 500 businesses
        business_ids = business_ids[:500]

        # Get lightweight summary of all filtered businesses for initial relevance scoring
        conn = get_db()
        placeholders = ",".join("?" * len(business_ids))
        rows = conn.execute(f"""
            SELECT id, name, address, neighbourhood, type, website,
                   structured_data
            FROM businesses
            WHERE id IN ({placeholders})
            AND licence_status = 'active'
        """, business_ids).fetchall()
        conn.close()

        # Build searchable summaries
        businesses_summary = []
        for r in rows:
            b = dict(r)
            sd = ""
            if b.get("structured_data"):
                try:
                    s = json.loads(b["structured_data"])
                    parts = []
                    if s.get("cuisines"): parts.extend(s["cuisines"])
                    if s.get("highlights"): parts.extend(s["highlights"])
                    if s.get("description"): parts.append(s["description"])
                    if s.get("menu_items"):
                        parts.extend([i.get("name","") for i in s["menu_items"][:10]])
                    sd = " ".join(parts)
                except Exception:
                    pass
            b["structured_data_text"] = sd
            businesses_summary.append(b)

        # Find most relevant businesses
        relevant = search_businesses(businesses_summary, question)

        # If no keyword matches, just take first 20
        if not relevant:
            relevant = businesses_summary[:20]

        # Fetch full data for relevant businesses
        relevant_ids = [b["id"] for b in relevant]
        businesses_full = fetch_businesses_with_data(relevant_ids)

        # Build context
        context = build_context(businesses_full)
        total_filtered = len(business_ids)
        shown = len(businesses_full)

        context_header = (
            f"The user has filtered to {total_filtered} businesses. "
            f"I've identified {shown} most relevant to their question.\n\n"
            f"RELEVANT BUSINESSES:\n\n{context}"
        )

        # Build messages
        messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        messages.append({"role": "system", "content": context_header})
        for h in history[-6:]:  # last 6 exchanges for context
            messages.append(h)
        messages.append({"role": "user", "content": question})

        answer = call_xai(messages)
        return jsonify({"answer": answer, "businesses_checked": shown})

    except urllib.error.URLError as e:
        return jsonify({"error": f"Proxy error: {str(e)}"}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/updish/api/status", methods=["GET"])
def status():
    """Returns pipeline status for the map dashboard."""
    try:
        conn = get_db()
        total = conn.execute("SELECT COUNT(*) FROM businesses WHERE licence_status='active'").fetchone()[0]
        with_web = conn.execute("SELECT COUNT(*) FROM businesses WHERE website IS NOT NULL AND website != '' AND licence_status='active'").fetchone()[0]
        with_html = conn.execute("SELECT COUNT(*) FROM businesses WHERE html_content IS NOT NULL AND licence_status='active'").fetchone()[0]
        with_structured = conn.execute("SELECT COUNT(*) FROM businesses WHERE structured_data IS NOT NULL AND licence_status='active'").fetchone()[0]

        last_sync = conn.execute("""
            SELECT finished_at FROM pipeline_log
            WHERE stage='sync_licences' AND status='ok'
            ORDER BY finished_at DESC LIMIT 1
        """).fetchone()

        conn.close()
        return jsonify({
            "total": total,
            "with_websites": with_web,
            "with_html": with_html,
            "with_structured": with_structured,
            "last_sync": last_sync[0] if last_sync else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5055, debug=False)
