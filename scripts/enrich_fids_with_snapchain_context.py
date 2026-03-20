#!/usr/bin/env python3
"""Build deterministic per-FID Snapchain role tags for Overlap-DB.

This script uses the local Clawberto-Farcaster-Context skill repo to talk
directly to the Snapchain hub and emits two repo-friendly artifacts:

1. `snapchain-active-context.*.json`
   Active authors seen in the rolling window, tagged from their recent casts
   and enriched with hub profile metadata.
2. `neynar-score-gte-0.99.users.with-context-tags.*.json`
   Existing high-score users merged with direct Snapchain role tags from the
   same rolling window.

No Neynar API calls are used here.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROLE_PATTERNS = {
    "builder": (
        r"\b(ship|shipping|shipped|deploy|deployed|launch|launched|code|coding|developer|dev|"
        r"engineer|engineering|product|protocol|sdk|api|repo|opensource|open source|"
        r"mini app|miniapp|app studio|agent|agents|tooling|infra)\b",
    ),
    "trader": (
        r"\b(trade|trader|trading|pnl|entry|exit|position|positions|"
        r"chart|charts|leverage|scalp|swing|breakout|liquidation|perp|perps|orderflow|"
        r"bid|ask|spot|futures|volatility)\b",
    ),
    "creator": (
        r"\b(create|creator|content|video|videos|clip|clips|stream|streaming|podcast|"
        r"newsletter|write|writing|thread|threads|audience|distribution|editing|edit|"
        r"recording|recorded|publish|published|posting)\b",
    ),
    "artist": (
        r"\b(art|artist|artwork|design|designer|visual|illustration|illustrator|draw|drawing|"
        r"sketch|paint|painting|photo|photography|music|album|cover art|creative coding)\b",
    ),
}

ROLE_QUALIFIER_PATTERNS = {
    "builder": re.compile(
        r"\b(code|coding|developer|dev|engineer|engineering|protocol|sdk|api|repo|"
        r"opensource|open source|mini app|miniapp|app studio|agent|agents|tooling|infra|"
        r"deploy|deployed)\b",
        re.IGNORECASE,
    ),
    "trader": re.compile(
        r"\b(trade|trader|trading|pnl|leverage|chart|charts|liquidation|perp|perps|"
        r"orderflow|futures|spot|bid|ask|scalp|swing|breakout)\b",
        re.IGNORECASE,
    ),
}

KEYWORD_PATTERN = re.compile(r"[a-z][a-z0-9']{2,}")
STOPWORDS = {
    "about", "after", "all", "also", "and", "any", "are", "because", "been", "before",
    "but", "can", "could", "day", "days", "for", "from", "get", "has", "have", "here",
    "into", "its", "just", "more", "new", "not", "now", "one", "our", "out", "really",
    "some", "that", "the", "their", "them", "there", "they", "this", "today", "too",
    "was", "what", "when", "will", "with", "you", "your",
}

HUB_PROFILE_FIELD_MAP = {
    "USER_DATA_TYPE_DISPLAY": "displayName",
    "USER_DATA_TYPE_BIO": "bio",
    "USER_DATA_TYPE_PFP": "pfpUrl",
    "USER_DATA_TYPE_BANNER": "bannerUrl",
    "USER_DATA_TYPE_URL": "url",
    "USER_DATA_PRIMARY_ADDRESS_ETHEREUM": "primaryAddressEthereum",
    "USER_DATA_PRIMARY_ADDRESS_SOLANA": "primaryAddressSolana",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enrich Overlap-DB with deterministic role tags from direct Snapchain context.",
    )
    parser.add_argument(
        "--input-users",
        default="data/neynar-score-gte-0.99.users.json",
        help="Existing users file to merge with Snapchain role tags.",
    )
    parser.add_argument(
        "--output-users",
        default="",
        help="Merged output file for the high-score cohort. Defaults to a window-labeled path.",
    )
    parser.add_argument(
        "--output-summary",
        default="",
        help="Summary file for the high-score cohort. Defaults to a window-labeled path.",
    )
    parser.add_argument(
        "--output-active-users",
        default="",
        help="Output file for active Snapchain authors. Defaults to a window-labeled path.",
    )
    parser.add_argument(
        "--output-active-summary",
        default="",
        help="Summary file for active Snapchain authors. Defaults to a window-labeled path.",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=24.0,
        help="How many trailing hours of Snapchain context to scan.",
    )
    parser.add_argument(
        "--hub-url",
        default="http://54.157.62.17:3381",
        help="Snapchain hub URL.",
    )
    parser.add_argument(
        "--shards",
        default="1,2",
        help="Comma-separated shard indices to scan.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="Hub event page size.",
    )
    parser.add_argument(
        "--event-id-span",
        type=int,
        default=3_000_000_000,
        help="Base event-id span before rolling-window scaling.",
    )
    parser.add_argument(
        "--sample-snippets",
        type=int,
        default=3,
        help="Max evidence snippets per assigned role.",
    )
    parser.add_argument(
        "--skip-active-export",
        action="store_true",
        help="Skip writing the broader active-context author dataset.",
    )
    parser.add_argument(
        "--skip-cohort-merge",
        action="store_true",
        help="Skip merging the existing high-score cohort with Snapchain tags.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose logging from the imported skill module.",
    )
    return parser.parse_args()


def format_window_label(hours: float) -> str:
    if float(hours).is_integer():
        hours_int = int(hours)
        if hours_int % 24 == 0:
            days = hours_int // 24
            return f"{days}d" if days != 1 else "24h"
        return f"{hours_int}h"
    compact = str(hours).replace(".", "_")
    return f"{compact}h"


def derive_default_paths(hours: float) -> dict[str, Path]:
    label = format_window_label(hours)
    return {
        "cohort_users": Path(f"data/neynar-score-gte-0.99.users.with-context-tags.last-{label}.json"),
        "cohort_summary": Path(f"data/neynar-score-gte-0.99.context-tags.last-{label}.summary.json"),
        "active_users": Path(f"data/snapchain-active-context.users.last-{label}.json"),
        "active_summary": Path(f"data/snapchain-active-context.last-{label}.summary.json"),
    }


def load_skill_module() -> Any:
    skill_path = Path("/Users/marko/Clawberto-Farcaster-Context/scripts/farcaster_daily_scraper.py")
    if not skill_path.exists():
        raise FileNotFoundError(f"Skill script not found: {skill_path}")

    spec = importlib.util.spec_from_file_location("farcaster_daily_scraper", skill_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module spec from {skill_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_users(path: Path) -> list[dict[str, Any]]:
    users = json.loads(path.read_text("utf8"))
    if not isinstance(users, list):
        raise ValueError("Input users file must contain a JSON array")
    normalized = []
    for user in users:
        if not isinstance(user, dict):
            continue
        fid = user.get("fid")
        if isinstance(fid, int) and fid > 0:
            normalized.append(user)
    return normalized


def compile_patterns(mapping: dict[str, tuple[str, ...]]) -> dict[str, tuple[re.Pattern[str], ...]]:
    return {
        key: tuple(re.compile(pattern, re.IGNORECASE) for pattern in patterns)
        for key, patterns in mapping.items()
    }


def cast_weight(cast: dict[str, Any]) -> float:
    cast_type = cast.get("type")
    engagement = cast.get("engagement") or {}
    likes = int(engagement.get("likes_count") or 0)
    recasts = int(engagement.get("recasts_count") or 0)
    replies = int(engagement.get("replies_count") or 0)
    base = 1.0 if cast_type == "post" else 0.75
    engagement_boost = min(2.0, math.log1p(likes + recasts + replies + 1))
    return round(base + (0.35 * engagement_boost), 4)


def match_scores(
    text: str,
    compiled: dict[str, tuple[re.Pattern[str], ...]],
    weight: float,
) -> dict[str, tuple[float, int]]:
    scores: dict[str, tuple[float, int]] = {}
    for label, patterns in compiled.items():
        hits = 0
        for pattern in patterns:
            hits += len(pattern.findall(text))
        if hits > 0:
            scores[label] = (round(weight * hits, 4), hits)
    return scores


def top_keywords(texts: list[str], limit: int = 12) -> list[str]:
    counter: Counter[str] = Counter()
    for text in texts:
        for token in KEYWORD_PATTERN.findall(text.lower()):
            if token in STOPWORDS:
                continue
            counter[token] += 1
    return [token for token, _ in counter.most_common(limit)]


def assign_labels(
    score_map: dict[str, float],
    hit_map: dict[str, int],
    cast_match_map: dict[str, int],
    *,
    min_score: float,
    min_hits: int,
) -> list[str]:
    if not score_map:
        return []

    ranked = sorted(score_map.items(), key=lambda item: (-item[1], item[0]))
    top_score = ranked[0][1]
    assigned = []
    for label, score in ranked:
        if score < min_score:
            continue
        if hit_map.get(label, 0) < min_hits:
            continue
        if cast_match_map.get(label, 0) <= 0:
            continue
        if score >= max(min_score + 1.0, top_score * 0.6):
            assigned.append(label)
            continue
        if label == ranked[0][0] and hit_map.get(label, 0) >= min_hits + 1:
            assigned.append(label)
    return assigned


def summarize_text(text: str, limit: int) -> str:
    compact = " ".join(str(text or "").split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 3] + "..."


def extract_evidence(
    cast_records: list[dict[str, Any]],
    label_patterns: tuple[re.Pattern[str], ...],
    limit: int,
) -> list[dict[str, Any]]:
    evidence = []
    for cast in cast_records:
        text = str(cast.get("text") or "")
        if not any(pattern.search(text) for pattern in label_patterns):
            continue
        evidence.append(
            {
                "hash": cast.get("hash"),
                "timestamp": cast.get("timestamp"),
                "type": cast.get("type"),
                "weight": cast.get("_weight"),
                "text": summarize_text(text, 220),
            }
        )
        if len(evidence) >= limit:
            break
    return evidence


def latest_user_data_values(payload: dict[str, Any]) -> dict[str, str]:
    latest_by_type: dict[str, tuple[int, str]] = {}
    messages = payload.get("messages") or []
    for message in messages:
        if not isinstance(message, dict):
            continue
        data = message.get("data") or {}
        if not isinstance(data, dict):
            continue
        body = data.get("userDataBody") or {}
        if not isinstance(body, dict):
            continue
        data_type = str(body.get("type") or "")
        value = str(body.get("value") or "")
        timestamp = int(data.get("timestamp") or -1)
        if not data_type:
            continue
        current = latest_by_type.get(data_type)
        if current is None or timestamp >= current[0]:
            latest_by_type[data_type] = (timestamp, value)
    return {data_type: value for data_type, (_, value) in latest_by_type.items()}


def fetch_hub_profile_by_fid(
    module: Any,
    session: Any,
    hub_url: str,
    fid: int,
) -> dict[str, Any]:
    profile = {
        "username": None,
        "displayName": None,
        "bio": None,
        "pfpUrl": None,
        "bannerUrl": None,
        "url": None,
        "primaryAddressEthereum": None,
        "primaryAddressSolana": None,
    }

    try:
        user_data_payload = module.request_json(
            session=session,
            url=f"{hub_url.rstrip('/')}{module.HUB_USER_DATA_BY_FID_URL_PATH}",
            params={"fid": int(fid), "pageSize": 100},
            timeout=20,
        )
        latest_values = latest_user_data_values(user_data_payload)
        for data_type, field_name in HUB_PROFILE_FIELD_MAP.items():
            value = latest_values.get(data_type)
            if value:
                profile[field_name] = value
        username = module.latest_username_from_user_data_payload(user_data_payload)
        if username:
            profile["username"] = username
    except RuntimeError:
        pass

    try:
        proof_payload = module.request_json(
            session=session,
            url=f"{hub_url.rstrip('/')}{module.HUB_USERNAME_PROOFS_BY_FID_URL_PATH}",
            params={"fid": int(fid)},
            timeout=20,
        )
        proof_username = module.latest_username_from_name_proofs_payload(proof_payload)
        if proof_username:
            profile["username"] = proof_username
    except RuntimeError:
        pass

    return profile


def fetch_hub_profiles(
    module: Any,
    hub_url: str,
    fids: list[int],
) -> dict[int, dict[str, Any]]:
    if not fids:
        return {}

    session = module.build_plain_session()
    profiles: dict[int, dict[str, Any]] = {}
    for index, fid in enumerate(fids, start=1):
        profiles[fid] = fetch_hub_profile_by_fid(module=module, session=session, hub_url=hub_url, fid=fid)
        if index % 250 == 0:
            print(json.dumps({"event": "profile_enrichment_progress", "done": index, "total": len(fids)}), flush=True)
    return profiles


def enrich_rows_with_context(
    rows: list[dict[str, Any]],
    casts: list[dict[str, Any]],
    window_hours: float,
    sample_snippets: int,
    metadata_by_fid: dict[int, dict[str, Any]],
    *,
    merged: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    role_patterns = compile_patterns(ROLE_PATTERNS)

    by_fid: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for cast in casts:
        fid = cast.get("author_fid")
        if isinstance(fid, int) and fid > 0:
            by_fid[fid].append(cast)

    role_counts: Counter[str] = Counter()
    active_users = 0
    tagged_users = 0
    profiles_resolved = 0
    enriched_rows: list[dict[str, Any]] = []

    for row in rows:
        fid = int(row["fid"])
        user_casts = sorted(
            by_fid.get(fid, []),
            key=lambda cast: str(cast.get("timestamp") or ""),
            reverse=True,
        )
        texts: list[str] = []
        role_scores: Counter[str] = Counter()
        role_hits: Counter[str] = Counter()
        role_cast_matches: Counter[str] = Counter()
        role_qualifier_hits: Counter[str] = Counter()
        activity_days: set[str] = set()

        for cast in user_casts:
            text = str(cast.get("text") or "").strip()
            if not text:
                continue
            weight = cast_weight(cast)
            cast["_weight"] = weight
            texts.append(text)
            timestamp = str(cast.get("timestamp") or "")
            if timestamp:
                activity_days.add(timestamp[:10])

            for label, (score, hits) in match_scores(text, role_patterns, weight).items():
                role_scores[label] += score
                role_hits[label] += hits
                role_cast_matches[label] += 1
                qualifier_pattern = ROLE_QUALIFIER_PATTERNS.get(label)
                if qualifier_pattern and qualifier_pattern.search(text):
                    role_qualifier_hits[label] += 1

        profile = metadata_by_fid.get(fid) or {}
        profile_texts = []
        if profile.get("displayName"):
            profile_texts.append(str(profile["displayName"]))
        if profile.get("bio"):
            profile_texts.append(str(profile["bio"]))
        profile_blob = " ".join(part.strip() for part in profile_texts if str(part).strip())
        if profile_blob:
            texts.append(profile_blob)
            for label, (score, hits) in match_scores(profile_blob, role_patterns, 0.9).items():
                role_scores[label] += score
                role_hits[label] += hits
                qualifier_pattern = ROLE_QUALIFIER_PATTERNS.get(label)
                if qualifier_pattern and qualifier_pattern.search(profile_blob):
                    role_qualifier_hits[label] += 1

        assigned_roles = assign_labels(
            dict(role_scores),
            dict(role_hits),
            dict(role_cast_matches),
            min_score=2.5,
            min_hits=2,
        )
        assigned_roles = [
            role
            for role in assigned_roles
            if ROLE_QUALIFIER_PATTERNS.get(role) is None or role_qualifier_hits.get(role, 0) > 0
        ]

        if user_casts:
            active_users += 1
        if assigned_roles:
            tagged_users += 1
        if any(profile.values()):
            profiles_resolved += 1

        for role in assigned_roles:
            role_counts[role] += 1

        role_evidence = {
            role: extract_evidence(user_casts, role_patterns[role], sample_snippets)
            for role in assigned_roles
        }

        base_record = dict(row) if merged else {"fid": fid}
        base_record["snapchainProfile"] = profile
        base_record["roleTags"] = assigned_roles
        base_record["roleTagMeta"] = {
            "windowHours": window_hours,
            "tagScores": {key: round(value, 4) for key, value in sorted(role_scores.items())},
            "tagHits": {key: int(value) for key, value in sorted(role_hits.items())},
            "activity": {
                "casts": len(user_casts),
                "posts": sum(1 for cast in user_casts if cast.get("type") == "post"),
                "comments": sum(1 for cast in user_casts if cast.get("type") == "comment"),
                "likesReceived": sum(int((cast.get("engagement") or {}).get("likes_count") or 0) for cast in user_casts),
                "recastsReceived": sum(int((cast.get("engagement") or {}).get("recasts_count") or 0) for cast in user_casts),
                "repliesReceived": sum(int((cast.get("engagement") or {}).get("replies_count") or 0) for cast in user_casts),
                "activeDays": len(activity_days),
                "lastCastAt": user_casts[0].get("timestamp") if user_casts else None,
            },
            "keywords": top_keywords(texts),
            "tagEvidence": role_evidence,
        }
        enriched_rows.append(base_record)

    summary = {
        "activeUsers": active_users,
        "profilesResolved": profiles_resolved,
        "roleCounts": dict(sorted(role_counts.items())),
        "taggedUsers": tagged_users,
        "totalUsers": len(rows),
    }
    return enriched_rows, summary


def fetch_context_casts(
    module: Any,
    target_fids: set[int] | None,
    hours: float,
    hub_url: str,
    shards: tuple[int, ...],
    page_size: int,
    event_id_span: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    session = module.build_plain_session()
    now_utc = datetime.now(timezone.utc)
    target_fc_start = module.utc_to_farcaster_time(now_utc - timedelta(hours=float(hours)))

    cast_records_by_hash: dict[str, dict[str, Any]] = {}
    likes_by_hash: Counter[str] = Counter()
    recasts_by_hash: Counter[str] = Counter()
    replies_by_hash: Counter[str] = Counter()
    removed_cast_hashes: set[str] = set()

    raw_events_processed = 0
    merge_events_processed = 0
    tracked_cast_messages = 0
    reaction_messages = 0
    shard_max_heights = module.get_hub_shard_max_heights(session=session, hub_url=hub_url)

    for shard_index in shards:
        tip_event_id = module.find_hub_tip_event_id(
            session=session,
            hub_url=hub_url,
            shard_index=shard_index,
            shard_max_height=shard_max_heights.get(shard_index),
        )
        if tip_event_id is None:
            continue

        span = max(50_000_000, int(round(float(event_id_span) * float(hours) / 24.0)))
        shard_start_id = max(0, tip_event_id - span)

        while True:
            first_batch = module.get_hub_events_page(
                session=session,
                hub_url=hub_url,
                shard_index=shard_index,
                from_event_id=shard_start_id,
                page_size=1,
            )
            if not first_batch:
                break
            first_ts = module.event_timestamp_farcaster_seconds(first_batch[0])
            if first_ts is None or first_ts <= target_fc_start:
                break
            next_start_id = max(0, tip_event_id - (span * 2))
            if next_start_id == shard_start_id:
                break
            span *= 2
            shard_start_id = next_start_id
            if shard_start_id == 0:
                break

        refined_start_id = module.find_hub_start_event_id_for_target_time(
            session=session,
            hub_url=hub_url,
            shard_index=shard_index,
            lower_event_id=shard_start_id,
            upper_event_id=tip_event_id,
            target_fc_timestamp=target_fc_start,
        )
        if refined_start_id > shard_start_id:
            shard_start_id = refined_start_id

        cursor = shard_start_id
        while True:
            batch = module.get_hub_events_page(
                session=session,
                hub_url=hub_url,
                shard_index=shard_index,
                from_event_id=cursor,
                page_size=page_size,
            )
            if not batch:
                break

            raw_events_processed += len(batch)
            for event in batch:
                event_id = module.get_int(event.get("id"), default=0)
                event_type = str(event.get("type") or "")
                if event_type != "HUB_EVENT_TYPE_MERGE_MESSAGE":
                    continue
                merge_events_processed += 1

                merge_body = event.get("mergeMessageBody") or {}
                message = merge_body.get("message")
                if not isinstance(message, dict):
                    continue
                data = message.get("data")
                if not isinstance(data, dict):
                    continue

                message_ts = module.get_int(data.get("timestamp"), default=-1)
                if message_ts < 0:
                    continue
                cast_dt_utc = module.farcaster_time_to_utc(message_ts)
                if cast_dt_utc < now_utc - timedelta(hours=float(hours)) or cast_dt_utc > now_utc:
                    continue

                message_type = data.get("type")
                if module.is_cast_add_message_type(message_type):
                    cast_add_body = data.get("castAddBody") or {}
                    if not isinstance(cast_add_body, dict):
                        cast_add_body = {}

                    cast_hash = module.normalize_hash(message.get("hash"))
                    if cast_hash is None:
                        continue

                    fid = module.get_int(data.get("fid"), default=-1)
                    parent_hash = module.parse_parent_cast_hash(cast_add_body)
                    parent_url_value = cast_add_body.get("parentUrl")
                    parent_url = (
                        str(parent_url_value)
                        if isinstance(parent_url_value, str) and parent_url_value
                        else None
                    )

                    if parent_hash and parent_hash in cast_records_by_hash:
                        replies_by_hash[parent_hash] += 1

                    if target_fids is not None and fid not in target_fids:
                        continue

                    tracked_cast_messages += 1
                    text = str(cast_add_body.get("text") or "")
                    cast_records_by_hash[cast_hash] = {
                        "hash": cast_hash,
                        "timestamp": cast_dt_utc.isoformat(),
                        "type": "comment" if (parent_hash or parent_url) else "post",
                        "author_fid": fid,
                        "text": text,
                        "parent_hash": parent_hash,
                        "parent_url": parent_url,
                        "engagement": {
                            "likes_count": 0,
                            "recasts_count": 0,
                            "replies_count": 0,
                        },
                        "_event_id": event_id,
                    }

                elif module.is_reaction_message_type(message_type):
                    reaction_messages += 1
                    reaction_body = data.get("reactionBody") or {}
                    if not isinstance(reaction_body, dict):
                        continue
                    target_hash = module.target_hash_from_reaction_body(reaction_body)
                    if target_hash is None or target_hash not in cast_records_by_hash:
                        continue
                    delta = 1 if module.is_reaction_add_message_type(message_type) else -1
                    kind = module.reaction_kind(reaction_body.get("type"))
                    if kind == "like":
                        likes_by_hash[target_hash] += delta
                    elif kind == "recast":
                        recasts_by_hash[target_hash] += delta

                elif module.is_cast_remove_message_type(message_type):
                    cast_remove_body = data.get("castRemoveBody") or {}
                    if not isinstance(cast_remove_body, dict):
                        continue
                    target_hash = module.normalize_hash(cast_remove_body.get("targetHash"))
                    if target_hash and target_hash in cast_records_by_hash:
                        removed_cast_hashes.add(target_hash)

            next_cursor = module.get_int(batch[-1].get("id"), default=cursor) + 1
            if next_cursor <= cursor:
                break
            cursor = next_cursor

    for removed_hash in removed_cast_hashes:
        cast_records_by_hash.pop(removed_hash, None)

    results: list[dict[str, Any]] = []
    tracked_fids: set[int] = set()
    for cast_hash, cast in cast_records_by_hash.items():
        cast["engagement"]["likes_count"] = max(0, int(likes_by_hash.get(cast_hash, 0)))
        cast["engagement"]["recasts_count"] = max(0, int(recasts_by_hash.get(cast_hash, 0)))
        cast["engagement"]["replies_count"] = max(0, int(replies_by_hash.get(cast_hash, 0)))
        cast.pop("_event_id", None)
        fid = int(cast.get("author_fid") or 0)
        if fid > 0:
            tracked_fids.add(fid)
        results.append(cast)

    results.sort(key=lambda cast: (str(cast.get("timestamp") or ""), str(cast.get("hash") or "")))
    stats = {
        "hours": hours,
        "mergeEventsProcessed": merge_events_processed,
        "rawEventsProcessed": raw_events_processed,
        "reactionMessagesProcessed": reaction_messages,
        "targetCastMessages": tracked_cast_messages,
        "targetFids": len(target_fids) if target_fids is not None else None,
        "trackedAuthors": len(tracked_fids),
        "targetCastsKept": len(results),
    }
    return results, stats


def sort_active_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
        role_tag_meta = row.get("roleTagMeta") or {}
        activity = role_tag_meta.get("activity") or {}
        tagged = bool(row.get("roleTags"))
        last_cast_at = activity.get("lastCastAt") or ""
        return (
            0 if tagged else 1,
            -int(activity.get("casts") or 0),
            -int(activity.get("likesReceived") or 0),
            str(last_cast_at),
            int(row.get("fid") or 0),
        )

    return sorted(rows, key=sort_key)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{json.dumps(payload, indent=2)}\n", encoding="utf8")


def main() -> None:
    args = parse_args()
    module = load_skill_module()
    module.setup_logging(bool(args.verbose))

    defaults = derive_default_paths(args.hours)
    cohort_users_path = Path(args.output_users) if args.output_users else defaults["cohort_users"]
    cohort_summary_path = Path(args.output_summary) if args.output_summary else defaults["cohort_summary"]
    active_users_path = Path(args.output_active_users) if args.output_active_users else defaults["active_users"]
    active_summary_path = Path(args.output_active_summary) if args.output_active_summary else defaults["active_summary"]

    users: list[dict[str, Any]] = []
    if not args.skip_cohort_merge:
        input_users_path = Path(args.input_users)
        users = load_users(input_users_path)
    else:
        input_users_path = Path(args.input_users)

    cohort_fids = {int(user["fid"]) for user in users}
    if args.skip_active_export and args.skip_cohort_merge:
        raise ValueError("Nothing to do: both active export and cohort merge are disabled")
    if args.skip_active_export and not cohort_fids:
        raise ValueError("Cohort merge requested but no valid input users were loaded")

    shards = tuple(int(part.strip()) for part in args.shards.split(",") if part.strip())
    fetch_target_fids = None if not args.skip_active_export else cohort_fids

    casts, fetch_stats = fetch_context_casts(
        module=module,
        target_fids=fetch_target_fids,
        hours=args.hours,
        hub_url=args.hub_url,
        shards=shards,
        page_size=args.page_size,
        event_id_span=args.event_id_span,
    )

    metadata_by_fid: dict[int, dict[str, Any]] = {}
    active_rows: list[dict[str, Any]] = []
    active_summary: dict[str, Any] | None = None

    if not args.skip_active_export:
        active_fids = sorted({int(cast["author_fid"]) for cast in casts if int(cast.get("author_fid") or 0) > 0})
        active_seed_rows = [{"fid": fid} for fid in active_fids]
        active_rows, initial_active_enrichment = enrich_rows_with_context(
            rows=active_seed_rows,
            casts=casts,
            window_hours=args.hours,
            sample_snippets=args.sample_snippets,
            metadata_by_fid={},
            merged=False,
        )
        tagged_active_fids = sorted(
            int(row["fid"])
            for row in active_rows
            if row.get("roleTags")
        )
        if tagged_active_fids:
            metadata_by_fid = fetch_hub_profiles(module=module, hub_url=args.hub_url, fids=tagged_active_fids)
            active_rows, active_enrichment = enrich_rows_with_context(
                rows=active_seed_rows,
                casts=casts,
                window_hours=args.hours,
                sample_snippets=args.sample_snippets,
                metadata_by_fid=metadata_by_fid,
                merged=False,
            )
        else:
            active_enrichment = initial_active_enrichment
        active_rows = sort_active_rows(active_rows)
        active_summary = {
            "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "hubUrl": args.hub_url,
            "outputUsers": str(active_users_path),
            "outputSummary": str(active_summary_path),
            "scanWindowHours": args.hours,
            "shards": list(shards),
            "source": "snapchain_direct_via_farcaster_context_skill",
            "fetch": fetch_stats,
            "profilesRequested": len(metadata_by_fid),
            "enrichment": active_enrichment,
        }
        write_json(active_users_path, active_rows)
        write_json(active_summary_path, active_summary)

    cohort_summary: dict[str, Any] | None = None
    cohort_rows_path: Path | None = None
    if not args.skip_cohort_merge and users:
        cohort_rows, cohort_enrichment = enrich_rows_with_context(
            rows=users,
            casts=casts,
            window_hours=args.hours,
            sample_snippets=args.sample_snippets,
            metadata_by_fid={fid: metadata_by_fid.get(fid, {}) for fid in cohort_fids},
            merged=True,
        )
        cohort_summary = {
            "generatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "hubUrl": args.hub_url,
            "inputUsers": str(input_users_path),
            "outputUsers": str(cohort_users_path),
            "outputSummary": str(cohort_summary_path),
            "scanWindowHours": args.hours,
            "shards": list(shards),
            "source": "snapchain_direct_via_farcaster_context_skill",
            "fetch": fetch_stats,
            "enrichment": cohort_enrichment,
        }
        cohort_rows_path = cohort_users_path
        write_json(cohort_rows_path, cohort_rows)
        write_json(cohort_summary_path, cohort_summary)

    print(
        json.dumps(
            {
                "event": "context_tagged",
                "scanWindowHours": args.hours,
                "castsKept": fetch_stats["targetCastsKept"],
                "trackedAuthors": fetch_stats["trackedAuthors"],
                "activeUsersOutput": str(active_users_path) if active_summary is not None else None,
                "cohortUsersOutput": str(cohort_rows_path) if cohort_summary is not None else None,
            }
        )
    )


if __name__ == "__main__":
    main()
