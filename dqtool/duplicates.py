# dqtool/duplicates.py

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict

import numpy as np
import pandas as pd
from rapidfuzz import fuzz


@dataclass
class DuplicatePair:
    """Represents a potential duplicate match between two rows."""
    left_index: int
    right_index: int
    score: float
    group_id: int


def _normalize_value(v: object) -> str:
    """Lowercase, strip, collapse spaces, convert NaN/None to empty string."""
    if pd.isna(v):
        return ""
    s = str(v).strip().lower()
    # collapse multiple spaces
    while "  " in s:
        s = s.replace("  ", " ")
    return s


def _build_key(row: pd.Series, subset_cols: List[str]) -> str:
    """Build a single concatenated key from selected columns."""
    parts = [_normalize_value(row[c]) for c in subset_cols]
    # filter out empty parts to avoid ' ||  || ' keys
    parts = [p for p in parts if p]
    return " | ".join(parts)


def _block_key(key: str, block_size: int = 2) -> str:
    """
    Simple blocking key:
    - Take first `block_size` characters of the key.
    - If key is short, return whole key.
    """
    if not key:
        return ""
    return key[:block_size]


def find_duplicates_advanced(
    df: pd.DataFrame,
    subset_cols: Optional[List[str]] = None,
    threshold: int = 90,
    block_size: int = 2,
    min_non_null: int = 1,
    max_pairs_per_block: int = 50_000,
) -> Tuple[pd.Series, pd.DataFrame]:
    """
    Enterprise-style duplicate matching with:
      - normalization
      - blocking
      - fuzzy similarity scoring
      - group_ids for clusters

    Returns:
      mask: pd.Series[bool]      -> True for rows that participate in at least one duplicate pair
      pairs_df: pd.DataFrame     -> columns: group_id, left_index, right_index,
                                    score, left_preview, right_preview

    Parameters:
      df              : input DataFrame
      subset_cols     : columns to use for matching (default: all object/string cols)
      threshold       : similarity threshold (0–100). Higher = stricter
      block_size      : how many chars from normalized key to use for blocking
      min_non_null    : minimum non-null values across subset_cols to consider a row
      max_pairs_per_block: safety limit per block to avoid O(n^2) explosion
    """
    if subset_cols is None or len(subset_cols) == 0:
        subset_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
        if not subset_cols:
            # fallback: use all columns as strings
            subset_cols = df.columns.tolist()

    # Filter out rows with too many nulls to avoid matching rows with all None/empty
    non_null_counts = df[subset_cols].notna().sum(axis=1)
    candidate_idx = df.index[non_null_counts >= min_non_null]

    if len(candidate_idx) == 0:
        # nothing to compare
        return pd.Series([False] * len(df), index=df.index), pd.DataFrame()

    # Build normalized keys
    keys_series = df.loc[candidate_idx, subset_cols].apply(
        lambda row: _build_key(row, subset_cols), axis=1
    )

    # Drop rows with completely empty keys (all fields blank / null)
    valid_mask = keys_series.str.len() > 0
    keys_series = keys_series[valid_mask]
    candidate_idx = candidate_idx[valid_mask]

    if len(candidate_idx) == 0:
        return pd.Series([False] * len(df), index=df.index), pd.DataFrame()

    # Blocking: group rows by a short prefix of the key
    blocks: Dict[str, List[int]] = {}
    for idx, key in zip(candidate_idx, keys_series):
        b = _block_key(key, block_size=block_size)
        if not b:
            continue
        blocks.setdefault(b, []).append(idx)

    pairs: List[DuplicatePair] = []
    parent: Dict[int, int] = {}  # union-find parent map

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x: int, y: int):
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[ry] = rx

    # Init union-find parents
    for idx in candidate_idx:
        parent[idx] = idx

    # Compare within each block using fuzzy matching
    for b_key, idx_list in blocks.items():
        if len(idx_list) < 2:
            continue

        # Safety: limit number of comparisons per block
        # n comparisons ≈ n*(n-1)/2
        if len(idx_list) * (len(idx_list) - 1) // 2 > max_pairs_per_block:
            # skip overly large block to avoid explosion
            continue

        # pre-fetch keys
        block_keys = {idx: keys_series.loc[idx] for idx in idx_list}

        for i in range(len(idx_list)):
            for j in range(i + 1, len(idx_list)):
                left_idx = idx_list[i]
                right_idx = idx_list[j]
                k1 = block_keys[left_idx]
                k2 = block_keys[right_idx]

                score = fuzz.token_set_ratio(k1, k2)
                if score >= threshold:
                    # union in cluster
                    union(left_idx, right_idx)
                    pairs.append(DuplicatePair(
                        left_index=left_idx,
                        right_index=right_idx,
                        score=float(score),
                        group_id=-1,  # temporary, will be assigned later
                    ))

    if not pairs:
        # no duplicates found
        return pd.Series([False] * len(df), index=df.index), pd.DataFrame()

    # Assign group_ids based on union-find roots
    root_to_group: Dict[int, int] = {}
    next_group_id = 1
    for idx in candidate_idx:
        root = find(idx)
        if root not in root_to_group:
            root_to_group[root] = next_group_id
            next_group_id += 1

    for p in pairs:
        root = find(p.left_index)
        p.group_id = root_to_group[root]

    # Build mask of rows participating in at least one duplicate group
    dup_indices = set()
    for p in pairs:
        dup_indices.add(p.left_index)
        dup_indices.add(p.right_index)

    mask = df.index.to_series().isin(dup_indices)

    # Build human-readable pairs table
    rows = []
    preview_cols = subset_cols[:3]  # up to 3 columns for preview
    for p in pairs:
        left_row = df.loc[p.left_index, preview_cols]
        right_row = df.loc[p.right_index, preview_cols]

        left_preview = " | ".join(str(left_row.get(c, "")) for c in preview_cols)
        right_preview = " | ".join(str(right_row.get(c, "")) for c in preview_cols)

        rows.append({
            "group_id": p.group_id,
            "left_index": p.left_index,
            "right_index": p.right_index,
            "score": p.score,
            "left_preview": left_preview,
            "right_preview": right_preview,
        })

    pairs_df = pd.DataFrame(rows).sort_values(["group_id", "score"], ascending=[True, False])

    return mask, pairs_df


def detect_duplicates_simple(
    df: pd.DataFrame,
    subset_cols: Optional[List[str]] = None,
    threshold: int = 90,
) -> pd.Series:
    """
    Backwards-compatible simple wrapper around find_duplicates_advanced
    that returns only a boolean mask (for your existing Streamlit code).
    """
    mask, _ = find_duplicates_advanced(
        df,
        subset_cols=subset_cols,
        threshold=threshold,
        block_size=2,
        min_non_null=1,
    )
    return mask
