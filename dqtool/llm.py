# dqtool/llm.py
"""
LLM integration: uses OpenAI if OPENAI_API_KEY is set.
Function: suggest_rules(profile_df) -> list of rule dicts
Each rule dict: {"column": str, "rule": str, "confidence": float}
"""
import os
import json
from typing import List

import pandas as pd

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # or gpt-4o, gpt-3.5, etc.


def _build_prompt(profile_df_json: str) -> str:
    return (
        "You are a data quality assistant. "
        "Given column metadata in JSON format, suggest actionable data quality rules.\n\n"
        "Return ONLY a JSON array of rules with fields: column, rule, confidence (0.0–1.0).\n\n"
        "Column metadata:\n"
        f"{profile_df_json}\n"
    )


def _call_openai(prompt: str) -> str:
    """
    Separate function to call OpenAI (so it's easy to mock/replace).
    """
    try:
        import openai  # make sure `openai` is installed
    except ImportError:
        return ""

    if not OPENAI_API_KEY:
        return ""

    openai.api_key = OPENAI_API_KEY

    try:
        resp = openai.ChatCompletion.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,
            temperature=0.0,
        )
        return resp["choices"][0]["message"]["content"]
    except Exception:
        return ""


def suggest_rules(profile_df: pd.DataFrame) -> List[dict]:
    """
    profile_df: pandas DataFrame (columns: column, dtype, null_pct, distinct_pct, ...)
    returns: list of rules, each: {"column": ..., "rule": ..., "confidence": ...}
    """
    profile_json = profile_df.to_json(orient="records")
    prompt = _build_prompt(profile_json)

    text = _call_openai(prompt)

    if text:
        # Try to parse JSON from the LLM response
        try:
            rules = json.loads(text)
            if isinstance(rules, list):
                return rules
        except Exception:
            # Try to extract JSON array manually
            import re

            m = re.search(r"\[.*\]", text, re.S)
            if m:
                try:
                    return json.loads(m.group(0))
                except Exception:
                    pass

    # ---------- Fallback (no OpenAI or parse error) ----------
    rules: List[dict] = []
    for r in profile_df.to_dict(orient="records"):
        col = r.get("column", "")
        dtype = str(r.get("dtype", ""))
        null_pct = float(r.get("null_pct", 0.0))
        distinct_pct = float(r.get("distinct_pct", 0.0))

        col_lower = col.lower()

        # Example heuristic rules
        if "email" in col_lower:
            rules.append(
                {
                    "column": col,
                    "rule": "value must match regex ^[^@]+@[^@]+\\.[^@]+$",
                    "confidence": 0.9,
                }
            )
        if "phone" in col_lower or "mobile" in col_lower:
            rules.append(
                {
                    "column": col,
                    "rule": "value should be digits only and length between 8 and 15",
                    "confidence": 0.8,
                }
            )
        if null_pct > 50:
            rules.append(
                {
                    "column": col,
                    "rule": "null percentage should be < 50%",
                    "confidence": 0.6,
                }
            )
        if distinct_pct < 1:
            rules.append(
                {
                    "column": col,
                    "rule": "column should probably be unique (or check for too many duplicates)",
                    "confidence": 0.7,
                }
            )

    return rules
