from __future__ import annotations

import os
import json
import base64
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import yaml
from fastapi import FastAPI, HTTPException, Request
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
SA_JSON_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "").strip()
SA_JSON_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "").strip()
MAPPING_PATH = os.getenv("MAPPING_PATH", "mapping.yaml").strip()

AUTO_ADD_COLUMNS = os.getenv("AUTO_ADD_COLUMNS", "false").lower() == "true"

app = FastAPI(title="QMS Publish Service")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_mapping() -> dict:
    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_sa_info() -> dict:
    if SA_JSON_PATH:
        with open(SA_JSON_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    if SA_JSON_B64:
        raw = base64.b64decode(SA_JSON_B64.encode("utf-8")).decode("utf-8")
        return json.loads(raw)
    raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON_PATH or GOOGLE_SERVICE_ACCOUNT_JSON_B64")


def sheets_service():
    if not GOOGLE_SHEET_ID:
        raise RuntimeError("Missing GOOGLE_SHEET_ID")
    creds = Credentials.from_service_account_info(load_sa_info(), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)


def read_range(svc, rng: str) -> List[List[Any]]:
    res = svc.spreadsheets().values().get(spreadsheetId=GOOGLE_SHEET_ID, range=rng).execute()
    return res.get("values", [])


def write_range(svc, rng: str, values: List[List[Any]]):
    svc.spreadsheets().values().update(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=rng,
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


def append_row(svc, tab: str, values: List[Any]):
    svc.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{tab}!A:A",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [values]},
    ).execute()


def batch_update(svc, updates: List[Tuple[str, List[List[Any]]]]):
    data = [{"range": rng, "values": vals} for rng, vals in updates]
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=GOOGLE_SHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()


def a1_col(n: int) -> str:
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def get_headers(svc, tab: str) -> List[str]:
    rows = read_range(svc, f"{tab}!1:1")
    return rows[0] if rows else []


def ensure_columns(svc, tab: str, required: List[str]) -> List[str]:
    headers = get_headers(svc, tab)
    if not headers:
        write_range(svc, f"{tab}!1:1", [required])
        return required

    missing = [c for c in required if c not in headers]
    if missing:
        if not AUTO_ADD_COLUMNS:
            raise HTTPException(
                status_code=400,
                detail=f"Missing columns in '{tab}': {missing} (AUTO_ADD_COLUMNS=false)",
            )
        write_range(svc, f"{tab}!1:1", [headers + missing])
        return headers + missing

    return headers


def find_row_num_by_key(svc, tab: str, headers: List[str], key_col: str, key_value: str) -> Optional[int]:
    if key_col not in headers:
        raise HTTPException(status_code=400, detail=f"Key column '{key_col}' not found in tab '{tab}'")

    key_idx = headers.index(key_col)
    values = read_range(svc, f"{tab}!A:ZZ")  # includes header

    for row_num, row in enumerate(values[1:], start=2):
        cell = row[key_idx] if key_idx < len(row) else ""
        if str(cell).strip() == str(key_value).strip():
            return row_num
    return None


def get_row_values(svc, tab: str, row_num: int, width: int) -> List[Any]:
    rng = f"{tab}!A{row_num}:{a1_col(width)}{row_num}"
    rows = read_range(svc, rng)
    row = rows[0] if rows else []
    return (row + [""] * width)[:width]


def patch_row(headers: List[str], current: List[Any], payload: Dict[str, Any], mapping: Dict[str, str]) -> List[Any]:
    """
    Patch semantics:
    - update only fields present in payload AND value is not None
    - missing fields do not overwrite sheet
    """
    out = list(current)
    for payload_key, sheet_col in mapping.items():
        if sheet_col not in headers:
            continue
        if payload_key not in payload:
            continue
        val = payload.get(payload_key)
        if val is None:
            continue
        idx = headers.index(sheet_col)
        out[idx] = val
    return out


def set_cell(headers: List[str], row: List[Any], col: str, value: Any) -> List[Any]:
    if col not in headers:
        return row
    idx = headers.index(col)
    row[idx] = value
    return row


def get_flag(obj: Dict[str, Any], keys: List[str]) -> bool:
    for k in keys:
        if k in obj:
            v = obj.get(k)
            if isinstance(v, bool):
                return v
            return str(v).strip().lower() in ("1", "true", "yes", "y")
    return False


def get_processes(body: Dict[str, Any], keys: List[str]) -> List[Dict[str, Any]]:
    for k in keys:
        if k in body:
            raw = body[k]
            if raw is None:
                return []
            if not isinstance(raw, list):
                raise HTTPException(status_code=400, detail=f"'{k}' must be a list")
            return [p for p in raw if isinstance(p, dict)]
    return []


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/publish")
async def publish(req: Request):
    body = await req.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="JSON body must be an object")

    cfg = load_mapping()
    tabs = cfg["tabs"]
    keys = cfg["keys"]
    soft = cfg.get("soft_delete", {})
    upd = cfg.get("updated_at", {})
    payload_cfg = cfg.get("payload", {})

    main_tab = tabs["main"]
    proc_tab = tabs["processes"]

    main_pk_col = keys["main_pk_col"]
    proc_fk_col = keys["processes_fk_col"]
    proc_pk_col = keys["processes_pk_col"]

    delete_flag_keys = payload_cfg.get("delete_flag_keys", ["is_deleted"])
    processes_keys = payload_cfg.get("processes_keys", ["processes"])

    main_mapping: Dict[str, str] = cfg.get("main_mapping", {})
    proc_mapping: Dict[str, str] = cfg.get("process_mapping", {})

    soft_col = soft.get("col", "is_deleted")
    soft_at_col = soft.get("at_col", "deleted_at")
    updated_at_col = upd.get("col", "updated_at")

    # RowID in payload (must exist)
    row_id = body.get("RowID") or body.get("row_id")
    if not row_id:
        raise HTTPException(status_code=400, detail="Missing RowID in payload")
    row_id = str(row_id).strip()

    svc = sheets_service()

    # Ensure headers exist (we won't auto-add by default)
    main_headers = ensure_columns(
        svc,
        main_tab,
        required=[main_pk_col] + list(set(main_mapping.values())),
    )
    proc_headers = ensure_columns(
        svc,
        proc_tab,
        required=[proc_fk_col, proc_pk_col] + list(set(proc_mapping.values())),
    )

    updates: List[Tuple[str, List[List[Any]]]] = []

    # ----------------------------
    # MAIN: add/update/delete
    # ----------------------------
    main_row_num = find_row_num_by_key(svc, main_tab, main_headers, main_pk_col, row_id)

    main_is_deleted = get_flag(body, delete_flag_keys)

    if main_is_deleted:
        # Soft delete main row if exists
        if main_row_num is not None:
            # ensure soft cols exist if needed
            main_headers = ensure_columns(svc, main_tab, required=[soft_col, soft_at_col, updated_at_col] + main_headers)
            width = len(main_headers)
            current = get_row_values(svc, main_tab, main_row_num, width)
            current = set_cell(main_headers, current, soft_col, True)
            current = set_cell(main_headers, current, soft_at_col, now_iso())
            current = set_cell(main_headers, current, updated_at_col, now_iso())
            rng = f"{main_tab}!A{main_row_num}:{a1_col(width)}{main_row_num}"
            updates.append((rng, [current]))
        main_action = "soft_delete"
    else:
        # Upsert main with PATCH semantics
        width = len(main_headers)
        if main_row_num is None:
            # Create new row aligned to headers
            new_row = [""] * width
            # ensure RowID written
            if main_pk_col in main_headers:
                new_row[main_headers.index(main_pk_col)] = row_id
            # patch known fields
            new_row = patch_row(main_headers, new_row, body, main_mapping)
            # timestamps
            main_headers = ensure_columns(svc, main_tab, required=[updated_at_col] + main_headers) if updated_at_col else main_headers
            if updated_at_col in main_headers:
                # extend row if headers changed
                width = len(main_headers)
                new_row = (new_row + [""] * width)[:width]
                new_row[main_headers.index(updated_at_col)] = now_iso()
            append_row(svc, main_tab, new_row)
            main_action = "add"
        else:
            current = get_row_values(svc, main_tab, main_row_num, width)
            patched = patch_row(main_headers, current, body, main_mapping)
            # updated_at
            if updated_at_col:
                main_headers = ensure_columns(svc, main_tab, required=[updated_at_col] + main_headers)
                width = len(main_headers)
                patched = (patched + [""] * width)[:width]
                patched = set_cell(main_headers, patched, updated_at_col, now_iso())
            rng = f"{main_tab}!A{main_row_num}:{a1_col(width)}{main_row_num}"
            updates.append((rng, [patched]))
            main_action = "update"

    # ----------------------------
    # PROCESSES: upsert + inferred deletes
    # ----------------------------
    processes = get_processes(body, processes_keys)

    # Build existing map for this RowID: proc_uid -> row_num
    proc_fk_idx = proc_headers.index(proc_fk_col)
    proc_pk_idx = proc_headers.index(proc_pk_col)
    proc_values = read_range(svc, f"{proc_tab}!A:ZZ")

    existing_for_row: Dict[str, int] = {}
    for row_num, row in enumerate(proc_values[1:], start=2):
        fk = row[proc_fk_idx] if proc_fk_idx < len(row) else ""
        if str(fk).strip() != row_id:
            continue
        puid = row[proc_pk_idx] if proc_pk_idx < len(row) else ""
        puid = str(puid).strip()
        if puid:
            existing_for_row[puid] = row_num

    incoming_uids: List[str] = []
    proc_actions = {"added": 0, "updated": 0, "deleted": 0}

    # ensure soft cols exist (only if AUTO_ADD_COLUMNS=true OR already present)
    # We will attempt to patch if present; if missing and AUTO_ADD_COLUMNS=false, it won't block.
    if updated_at_col:
        proc_headers = ensure_columns(svc, proc_tab, required=[updated_at_col] + proc_headers)
    proc_headers = ensure_columns(svc, proc_tab, required=[proc_fk_col, proc_pk_col] + proc_headers)

    for p in processes:
        # We accept Unique Process ID in payload as either "Unique Process ID" or "process_uid"
        puid = p.get("Unique Process ID") or p.get("unique_process_id") or p.get("process_uid")
        if not puid:
            continue
        puid = str(puid).strip()
        incoming_uids.append(puid)

        # allow process-level is_deleted
        p_is_deleted = get_flag(p, delete_flag_keys)

        # ensure FK present
        p = dict(p)
        p["RowID"] = row_id
        p["Unique Process ID"] = puid

        if puid in existing_for_row:
            rnum = existing_for_row[puid]
            width = len(proc_headers)
            current = get_row_values(svc, proc_tab, rnum, width)
            patched = patch_row(proc_headers, current, p, proc_mapping)
            if p_is_deleted and soft_col in proc_headers:
                patched = set_cell(proc_headers, patched, soft_col, True)
            if p_is_deleted and soft_at_col in proc_headers:
                patched = set_cell(proc_headers, patched, soft_at_col, now_iso())
            if updated_at_col and updated_at_col in proc_headers:
                patched = set_cell(proc_headers, patched, updated_at_col, now_iso())
            rng = f"{proc_tab}!A{rnum}:{a1_col(width)}{rnum}"
            updates.append((rng, [patched]))
            proc_actions["updated"] += 1
        else:
            # append new row
            width = len(proc_headers)
            new_row = [""] * width
            # set FK + PK
            new_row[proc_headers.index(proc_fk_col)] = row_id
            new_row[proc_headers.index(proc_pk_col)] = puid
            new_row = patch_row(proc_headers, new_row, p, proc_mapping)
            if p_is_deleted and soft_col in proc_headers:
                new_row = set_cell(proc_headers, new_row, soft_col, True)
            if p_is_deleted and soft_at_col in proc_headers:
                new_row = set_cell(proc_headers, new_row, soft_at_col, now_iso())
            if updated_at_col and updated_at_col in proc_headers:
                new_row = set_cell(proc_headers, new_row, updated_at_col, now_iso())
            append_row(svc, proc_tab, new_row)
            proc_actions["added"] += 1

    # inferred deletes: existing - incoming
    removed = set(existing_for_row.keys()) - set(incoming_uids)
    if removed:
        # only soft delete if those columns exist (or AUTO_ADD_COLUMNS=true)
        proc_headers = ensure_columns(svc, proc_tab, required=[soft_col, soft_at_col] + proc_headers) if AUTO_ADD_COLUMNS else proc_headers
        for puid in removed:
            rnum = existing_for_row[puid]
            width = len(proc_headers)
            current = get_row_values(svc, proc_tab, rnum, width)
            if soft_col in proc_headers:
                current = set_cell(proc_headers, current, soft_col, True)
            if soft_at_col in proc_headers:
                current = set_cell(proc_headers, current, soft_at_col, now_iso())
            if updated_at_col and updated_at_col in proc_headers:
                current = set_cell(proc_headers, current, updated_at_col, now_iso())
            rng = f"{proc_tab}!A{rnum}:{a1_col(width)}{rnum}"
            updates.append((rng, [current]))
            proc_actions["deleted"] += 1

    # Apply all updates at once
    if updates:
        batch_update(svc, updates)

    return {
        "ok": True,
        "row_id": row_id,
        "main_action": main_action,
        "process_actions": proc_actions,
        "ts": now_iso(),
    }
