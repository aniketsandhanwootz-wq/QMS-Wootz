from __future__ import annotations

import base64
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import yaml
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Loads .env automatically for local dev (Render env vars still work)
load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
SA_JSON_PATH = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_PATH", "").strip()
SA_JSON_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "").strip()
MAPPING_PATH = os.getenv("MAPPING_PATH", "mapping.yaml").strip()

AUTO_ADD_COLUMNS = os.getenv("AUTO_ADD_COLUMNS", "false").lower() == "true"

app = FastAPI(title="QMS Publish Service")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def gen_alnum_id(length: int = 7) -> str:
    # Uppercase alphanumeric, collision-resistant enough for Sheets use
    import secrets
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    return "".join(secrets.choice(alphabet) for _ in range(length))
    
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


def uniq(seq: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for x in seq:
        if x and x not in seen:
            out.append(x)
            seen.add(x)
    return out


def get_headers(svc, tab: str) -> List[str]:
    rows = read_range(svc, f"{tab}!1:1")
    return rows[0] if rows else []


def ensure_columns(svc, tab: str, required: List[str]) -> List[str]:
    headers = get_headers(svc, tab)
    required = uniq(required)

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
    out = list(current)
    for payload_key, sheet_col in mapping.items():
        if sheet_col not in headers:
            continue
        if payload_key not in payload:
            continue
        val = payload.get(payload_key)
        if val is None:
            continue
        out[headers.index(sheet_col)] = val
    return out


def set_cell(headers: List[str], row: List[Any], col: str, value: Any) -> List[Any]:
    if col not in headers:
        return row
    row[headers.index(col)] = value
    return row


def get_flag(obj: Dict[str, Any], keys: List[str]) -> bool:
    for k in keys:
        if k in obj:
            v = obj.get(k)
            if isinstance(v, bool):
                return v
            return str(v).strip().lower() in ("1", "true", "yes", "y")
    return False


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
    processes_key = payload_cfg.get("processes_key", "processes")

    main_mapping: Dict[str, str] = cfg.get("main_mapping", {})
    proc_mapping: Dict[str, str] = cfg.get("process_mapping", {})

    soft_col = soft.get("col", "is_deleted")
    soft_at_col = soft.get("at_col", "deleted_at")
    updated_at_col = upd.get("col", "updated_at")

    # Glide-only: row_id is required
    if "row_id" not in body or not str(body.get("row_id", "")).strip():
        raise HTTPException(status_code=400, detail="Missing 'row_id' in payload")
    row_id = str(body["row_id"]).strip()

    svc = sheets_service()

    # Ensure headers exist
    main_required = [main_pk_col] + list(main_mapping.values())
    main_headers = ensure_columns(svc, main_tab, required=main_required)

    proc_required = [proc_fk_col, proc_pk_col] + list(proc_mapping.values())
    proc_headers = ensure_columns(svc, proc_tab, required=proc_required)

    updates: List[Tuple[str, List[List[Any]]]] = []

    # ----------------------------
    # MAIN: add/update/soft-delete
    # ----------------------------
    main_row_num = find_row_num_by_key(svc, main_tab, main_headers, main_pk_col, row_id)
    main_is_deleted = get_flag(body, delete_flag_keys)

    if main_is_deleted:
        if main_row_num is not None:
            main_headers = ensure_columns(svc, main_tab, required=[soft_col, soft_at_col, updated_at_col] + main_headers)
            width = len(main_headers)
            current = get_row_values(svc, main_tab, main_row_num, width)
            current = set_cell(main_headers, current, soft_col, True)
            current = set_cell(main_headers, current, soft_at_col, now_iso())
            if updated_at_col:
                current = set_cell(main_headers, current, updated_at_col, now_iso())
            rng = f"{main_tab}!A{main_row_num}:{a1_col(width)}{main_row_num}"
            updates.append((rng, [current]))
        main_action = "soft_delete"
    else:
        if main_row_num is None:
            width = len(main_headers)
            new_row = [""] * width
            new_row[main_headers.index(main_pk_col)] = row_id
            new_row = patch_row(main_headers, new_row, body, main_mapping)

            if updated_at_col:
                main_headers = ensure_columns(svc, main_tab, required=[updated_at_col] + main_headers)
                width = len(main_headers)
                new_row = (new_row + [""] * width)[:width]
                new_row[main_headers.index(updated_at_col)] = now_iso()

            append_row(svc, main_tab, new_row)
            main_action = "add"
        else:
            width = len(main_headers)
            current = get_row_values(svc, main_tab, main_row_num, width)
            patched = patch_row(main_headers, current, body, main_mapping)

            if updated_at_col:
                main_headers = ensure_columns(svc, main_tab, required=[updated_at_col] + main_headers)
                width = len(main_headers)
                patched = (patched + [""] * width)[:width]
                patched = set_cell(main_headers, patched, updated_at_col, now_iso())

            rng = f"{main_tab}!A{main_row_num}:{a1_col(width)}{main_row_num}"
            updates.append((rng, [patched]))
            main_action = "update"

    # ----------------------------
    # PROCESSES: only if provided
    # - UID is mandatory per process
    # - if processes provided, we do inferred deletes (sync behavior)
    # ----------------------------
    proc_actions = {"added": 0, "updated": 0, "deleted": 0}

    processes_present = processes_key in body
    processes_raw = body.get(processes_key, None)
    
    # Glide sends processes as a JSON-encoded string like "[{\"Process\":...}]"
    if processes_present and isinstance(processes_raw, str):
        s = processes_raw.strip()
        if not s:
            processes_raw = []
        else:
            try:
                processes_raw = json.loads(s, strict=False) # convert string -> list[dict]
            except json.JSONDecodeError as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid JSON in '{processes_key}' string: {e}",
                )
    
    if processes_present:
        if processes_raw is None:
            processes: List[Dict[str, Any]] = []
        elif not isinstance(processes_raw, list):
            raise HTTPException(status_code=400, detail=f"'{processes_key}' must be a list (or JSON string)")
        else:
            processes = [p for p in processes_raw if isinstance(p, dict)]

        # Existing map for this row: UID -> row_num
        proc_fk_idx = proc_headers.index(proc_fk_col)
        if "UID" not in proc_headers:
            raise HTTPException(status_code=400, detail="Processes tab missing required column 'UID'")
        proc_uid_idx = proc_headers.index("UID")
        
        proc_values = read_range(svc, f"{proc_tab}!A:ZZ")
        
        existing_for_row: Dict[str, int] = {}
        for row_num, row in enumerate(proc_values[1:], start=2):
            fk = row[proc_fk_idx] if proc_fk_idx < len(row) else ""
            if str(fk).strip() != row_id:
                continue
            uid_cell = row[proc_uid_idx] if proc_uid_idx < len(row) else ""
            uid_cell = str(uid_cell).strip()
            if uid_cell:
                existing_for_row[uid_cell] = row_num

        incoming_uids: List[str] = []

        # Make sure timestamps columns exist if you want them
        if updated_at_col:
            proc_headers = ensure_columns(svc, proc_tab, required=[updated_at_col] + proc_headers)

        for p in processes:
            if "UID" not in p or not str(p.get("UID", "")).strip():
                raise HTTPException(status_code=400, detail="Each process must include non-empty 'UID'")

            uid = str(p["UID"]).strip()
            incoming_uids.append(uid)
            p_is_deleted = get_flag(p, delete_flag_keys)
            
            # PK for Processes (7-digit) -> goes into proc_pk_col (now 🔒 Row ID)
            proc_row_pk = str(p.get("_proc_row_id", "")).strip()
            if not proc_row_pk:
                proc_row_pk = gen_alnum_id(7)
            
            payload_p = dict(p)
            payload_p["row_id"] = row_id          # mapped to ID column (FK)
            payload_p["UID"] = uid                # mapped to UID column (optional data)
            payload_p["_proc_row_id"] = proc_row_pk  # internal key (will be written via proc_pk_col)

            if uid in existing_for_row:
                rnum = existing_for_row[uid]
                width = len(proc_headers)
                current = get_row_values(svc, proc_tab, rnum, width)
                patched = patch_row(proc_headers, current, payload_p, proc_mapping)

                # soft delete if requested
                if p_is_deleted:
                    proc_headers = ensure_columns(svc, proc_tab, required=[soft_col, soft_at_col] + proc_headers)
                    width = len(proc_headers)
                    patched = (patched + [""] * width)[:width]
                    if soft_col in proc_headers:
                        patched = set_cell(proc_headers, patched, soft_col, True)
                    if soft_at_col in proc_headers:
                        patched = set_cell(proc_headers, patched, soft_at_col, now_iso())

                if updated_at_col and updated_at_col in proc_headers:
                    patched = set_cell(proc_headers, patched, updated_at_col, now_iso())

                rng = f"{proc_tab}!A{rnum}:{a1_col(len(proc_headers))}{rnum}"
                updates.append((rng, [patched]))
                proc_actions["updated"] += 1
            else:
                width = len(proc_headers)
                new_row = [""] * width
                new_row[proc_headers.index(proc_fk_col)] = row_id        # goes to "ID"
                new_row[proc_headers.index(proc_pk_col)] = proc_row_pk   # goes to "🔒 Row ID" (7-digit)
                new_row = patch_row(proc_headers, new_row, payload_p, proc_mapping)

                if p_is_deleted:
                    proc_headers = ensure_columns(svc, proc_tab, required=[soft_col, soft_at_col] + proc_headers)
                    width = len(proc_headers)
                    new_row = (new_row + [""] * width)[:width]
                    if soft_col in proc_headers:
                        new_row = set_cell(proc_headers, new_row, soft_col, True)
                    if soft_at_col in proc_headers:
                        new_row = set_cell(proc_headers, new_row, soft_at_col, now_iso())

                if updated_at_col and updated_at_col in proc_headers:
                    new_row = set_cell(proc_headers, new_row, updated_at_col, now_iso())

                append_row(svc, proc_tab, new_row)
                proc_actions["added"] += 1

        # inferred deletes (sync): existing - incoming
        removed = set(existing_for_row.keys()) - set(incoming_uids)
        if removed:
            proc_headers = ensure_columns(svc, proc_tab, required=[soft_col, soft_at_col] + proc_headers) if AUTO_ADD_COLUMNS else proc_headers
            for uid in removed:
                rnum = existing_for_row[uid]
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
