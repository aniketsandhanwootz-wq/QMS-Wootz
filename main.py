from __future__ import annotations

import os
import json
import base64
import yaml
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build


# =========================
# Config & Constants
# =========================

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "").strip()
MAIN_TAB = os.getenv("MAIN_TAB", "Main").strip()
SUB_TAB = os.getenv("SUB_TAB", "Processes").strip()

# service account JSON is stored in env as base64 for Render
SA_JSON_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "").strip()

CONFIG_PATH = os.getenv("COLUMN_CONFIG_PATH", "columns.yaml")

AUTO_ADD_COLUMNS = os.getenv("AUTO_ADD_COLUMNS", "true").lower() == "true"


# =========================
# Helpers: Google Sheets API
# =========================

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _load_service_account_info() -> dict:
    if not SA_JSON_B64:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON_B64")
    raw = base64.b64decode(SA_JSON_B64.encode("utf-8")).decode("utf-8")
    return json.loads(raw)

def _sheets_service():
    if not SHEET_ID:
        raise RuntimeError("Missing GOOGLE_SHEET_ID")
    info = _load_service_account_info()
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds)

def _read_range(svc, rng: str) -> List[List[Any]]:
    res = svc.spreadsheets().values().get(spreadsheetId=SHEET_ID, range=rng).execute()
    return res.get("values", [])

def _write_range(svc, rng: str, values: List[List[Any]]):
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=rng,
        valueInputOption="RAW",
        body={"values": values},
    ).execute()

def _append_row(svc, tab: str, values: List[Any]):
    svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"{tab}!A:A",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [values]},
    ).execute()

def _batch_update(svc, updates: List[Tuple[str, List[List[Any]]]]):
    # updates: [(range, [[...]]), ...]
    data = [{"range": rng, "values": vals} for rng, vals in updates]
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"valueInputOption": "RAW", "data": data},
    ).execute()


# =========================
# Column mapping / schema
# =========================

def _load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _get_headers(svc, tab: str) -> List[str]:
    rows = _read_range(svc, f"{tab}!1:1")
    if not rows:
        return []
    return rows[0]

def _ensure_headers(svc, tab: str, required: List[str]) -> List[str]:
    """
    Ensures required columns exist in header row.
    If AUTO_ADD_COLUMNS true, missing columns are appended to header.
    """
    headers = _get_headers(svc, tab)
    if not headers:
        # create header row from required
        _write_range(svc, f"{tab}!1:1", [required])
        return required

    missing = [c for c in required if c not in headers]
    if missing and not AUTO_ADD_COLUMNS:
        raise HTTPException(
            status_code=400,
            detail=f"Missing columns in '{tab}': {missing} (AUTO_ADD_COLUMNS=false)",
        )

    if missing:
        new_headers = headers + missing
        _write_range(svc, f"{tab}!1:1", [new_headers])
        return new_headers

    return headers

def _col_index_map(headers: List[str]) -> Dict[str, int]:
    return {h: i for i, h in enumerate(headers)}

def _find_row_by_key(
    svc,
    tab: str,
    headers: List[str],
    key_col: str,
    key_value: str,
) -> Optional[int]:
    """
    Returns 1-based row index in sheet (including header row) where key matches.
    Data starts at row 2.
    """
    if key_col not in headers:
        return None
    key_idx = headers.index(key_col)

    # Read a reasonable range; for bigger sheets you can switch to search API/indexing.
    values = _read_range(svc, f"{tab}!A2:Z")
    for i, row in enumerate(values, start=2):  # sheet row number
        cell = row[key_idx] if key_idx < len(row) else ""
        if str(cell).strip() == str(key_value).strip():
            return i
    return None

def _row_to_values(headers: List[str], payload: Dict[str, Any], mapping: Dict[str, str]) -> List[Any]:
    """
    Convert payload into a row aligned to headers using mapping:
      mapping: {"RowID": "RowID", "Qty": "Qty", "Part Number": "Part Number", ...}
      meaning payload_key -> sheet_column
    """
    values = [""] * len(headers)

    for payload_key, sheet_col in mapping.items():
        if sheet_col not in headers:
            continue
        idx = headers.index(sheet_col)
        v = payload.get(payload_key, "")
        if v is None:
            v = ""
        values[idx] = v

    return values

def _is_delete_snapshot(main_obj: Dict[str, Any], processes: Optional[List[Dict[str, Any]]]) -> bool:
    """
    Delete inference rule:
      if all main fields besides RowID are empty AND processes list empty => delete
    """
    # keep RowID aside
    empties = []
    for k, v in main_obj.items():
        if k == "RowID":
            continue
        empties.append(v is None or str(v).strip() == "")
    main_empty = all(empties) if empties else True

    proc_empty = (not processes) or (len(processes) == 0)
    return main_empty and proc_empty


# =========================
# FastAPI Models
# =========================

class PublishPayload(BaseModel):
    # "RowID, Qty, Part Number, Vendor, Drawing URL, ... + processes"
    # We accept arbitrary keys, so we store as dict via Request.json()
    pass

app = FastAPI(title="Publish Webhook Service")


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/publish")
async def publish(req: Request):
    """
    Accepts JSON snapshot. Infers add/update/delete for MAIN and SUB tabs.
    """
    body = await req.json()
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="JSON body must be an object")

    cfg = _load_config()
    main_cfg = cfg.get("main", {})
    sub_cfg = cfg.get("sub", {})

    row_key_payload = main_cfg.get("row_id_payload_key", "RowID")
    main_key_col = main_cfg.get("row_id_sheet_col", "RowID")
    sub_key_col = sub_cfg.get("process_id_sheet_col", "process_uid")
    sub_fk_col = sub_cfg.get("row_id_sheet_col", "RowID")

    processes_key = cfg.get("processes_payload_key", "processes")

    row_id = str(body.get(row_key_payload, "")).strip()
    if not row_id:
        raise HTTPException(status_code=400, detail=f"Missing '{row_key_payload}'")

    processes = body.get(processes_key)
    if processes is not None and not isinstance(processes, list):
        raise HTTPException(status_code=400, detail=f"'{processes_key}' must be a list if present")

    # Google Sheets service
    svc = _sheets_service()

    # ---- MAIN TAB: Upsert/Delete ----
    main_mapping: Dict[str, str] = main_cfg.get("mapping", {})
    # Always include UpdatedAt column if configured
    main_required = list(set(main_mapping.values()))
    if main_cfg.get("updated_at_col"):
        main_required.append(main_cfg["updated_at_col"])

    main_headers = _ensure_headers(svc, MAIN_TAB, required=main_required)
    main_row_num = _find_row_by_key(svc, MAIN_TAB, main_headers, main_key_col, row_id)

    # build main object snapshot for delete detection (only mapped keys)
    main_snapshot = {k: body.get(k) for k in main_mapping.keys()}
    # ensure row id key exists
    main_snapshot[row_key_payload] = row_id

    delete_main = _is_delete_snapshot(main_snapshot, processes)

    updates: List[Tuple[str, List[List[Any]]]] = []

    if delete_main:
        # Soft-delete preferred: mark a column rather than physically removing row
        del_col = main_cfg.get("deleted_col", "is_deleted")
        del_at_col = main_cfg.get("deleted_at_col", "deleted_at")
        # ensure columns exist
        main_headers = _ensure_headers(svc, MAIN_TAB, required=[del_col, del_at_col] + main_headers)

        if main_row_num is None:
            # nothing to delete; treat as no-op
            main_action = "noop_delete"
        else:
            col_map = _col_index_map(main_headers)
            row_values = _read_range(svc, f"{MAIN_TAB}!A{main_row_num}:ZZ{main_row_num}")
            current = row_values[0] if row_values else []
            # normalize row to header length
            normalized = (current + [""] * len(main_headers))[:len(main_headers)]
            normalized[col_map[del_col]] = True
            normalized[col_map[del_at_col]] = _now_iso()

            updates.append((f"{MAIN_TAB}!A{main_row_num}:"
                            f"{_a1_col(len(main_headers))}{main_row_num}", [normalized]))
            main_action = "soft_delete"
    else:
        # Upsert main
        if main_cfg.get("updated_at_col"):
            body["_updated_at"] = _now_iso()
            # allow mapping key for updated_at
            # (we support mapping updated_at via special key)
        row_values = _row_to_values(main_headers, body, main_mapping)

        # set updated_at if configured and mapping doesn't cover it
        updated_col = main_cfg.get("updated_at_col")
        if updated_col and updated_col in main_headers:
            idx = main_headers.index(updated_col)
            row_values[idx] = _now_iso()

        if main_row_num is None:
            _append_row(svc, MAIN_TAB, row_values)
            main_action = "add"
        else:
            rng = f"{MAIN_TAB}!A{main_row_num}:{_a1_col(len(main_headers))}{main_row_num}"
            updates.append((rng, [row_values]))
            main_action = "update"

    # ---- SUB TAB: Upsert processes + delete removed ones ----
    sub_mapping: Dict[str, str] = sub_cfg.get("mapping", {})
    sub_required = list(set(sub_mapping.values()))
    # Always require FK + process UID columns
    sub_required += [sub_fk_col, sub_key_col]
    if sub_cfg.get("updated_at_col"):
        sub_required.append(sub_cfg["updated_at_col"])

    sub_headers = _ensure_headers(svc, SUB_TAB, required=sub_required)

    # Read existing process rows for this RowID to detect deletes
    # We'll scan A2:ZZ and filter by RowID (OK for moderate sizes).
    existing_rows = _read_range(svc, f"{SUB_TAB}!A2:ZZ")
    fk_idx = sub_headers.index(sub_fk_col) if sub_fk_col in sub_headers else None
    pid_idx = sub_headers.index(sub_key_col) if sub_key_col in sub_headers else None

    existing_for_row: Dict[str, int] = {}  # process_uid -> sheet row number
    if fk_idx is not None and pid_idx is not None:
        for i, r in enumerate(existing_rows, start=2):
            fk_val = r[fk_idx] if fk_idx < len(r) else ""
            if str(fk_val).strip() != row_id:
                continue
            pid_val = r[pid_idx] if pid_idx < len(r) else ""
            pid_val = str(pid_val).strip()
            if pid_val:
                existing_for_row[pid_val] = i

    incoming_process_ids: List[str] = []
    sub_actions = {"added": 0, "updated": 0, "deleted": 0}

    if processes:
        for proc in processes:
            if not isinstance(proc, dict):
                continue

            proc_id_payload_key = sub_cfg.get("process_id_payload_key", "process_uid")
            proc_uid = str(proc.get(proc_id_payload_key, "")).strip()
            if not proc_uid:
                continue
            incoming_process_ids.append(proc_uid)

            # Merge FK into proc object so mapping can write it
            proc_with_fk = dict(proc)
            proc_with_fk[sub_fk_col] = row_id  # allow direct mapping
            # If reports are nested, flatten here if needed (optional)
            proc_row_vals = _row_to_values(sub_headers, proc_with_fk, sub_mapping)

            # updated_at
            if sub_cfg.get("updated_at_col") and sub_cfg["updated_at_col"] in sub_headers:
                idx = sub_headers.index(sub_cfg["updated_at_col"])
                proc_row_vals[idx] = _now_iso()

            if proc_uid in existing_for_row:
                rnum = existing_for_row[proc_uid]
                rng = f"{SUB_TAB}!A{rnum}:{_a1_col(len(sub_headers))}{rnum}"
                updates.append((rng, [proc_row_vals]))
                sub_actions["updated"] += 1
            else:
                _append_row(svc, SUB_TAB, proc_row_vals)
                sub_actions["added"] += 1

    # Delete processes removed (present in sheet, absent in incoming)
    removed = set(existing_for_row.keys()) - set(incoming_process_ids)
    if removed:
        # soft-delete for processes
        del_col = sub_cfg.get("deleted_col", "is_deleted")
        del_at_col = sub_cfg.get("deleted_at_col", "deleted_at")
        sub_headers = _ensure_headers(svc, SUB_TAB, required=[del_col, del_at_col] + sub_headers)
        col_map = _col_index_map(sub_headers)

        for proc_uid in removed:
            rnum = existing_for_row[proc_uid]
            row_values = _read_range(svc, f"{SUB_TAB}!A{rnum}:ZZ{rnum}")
            current = row_values[0] if row_values else []
            normalized = (current + [""] * len(sub_headers))[:len(sub_headers)]
            normalized[col_map[del_col]] = True
            normalized[col_map[del_at_col]] = _now_iso()

            rng = f"{SUB_TAB}!A{rnum}:{_a1_col(len(sub_headers))}{rnum}"
            updates.append((rng, [normalized]))
            sub_actions["deleted"] += 1

    # Apply batched updates
    if updates:
        _batch_update(svc, updates)

    return {
        "ok": True,
        "row_id": row_id,
        "main_action": main_action,
        "sub_actions": sub_actions,
        "timestamp": _now_iso(),
    }


# =========================
# A1 helper for range columns
# =========================
def _a1_col(n: int) -> str:
    """1 -> A, 26 -> Z, 27 -> AA"""
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s
