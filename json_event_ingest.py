"""Utility to ingest SECS-like JSON payloads into a SQLite table.

This script parses one or more JSON payloads that match the sample structure
from the prompt and inserts flattened rows into a SQLite database table. Each
row corresponds to a combination of report (RPTID) and product entry, and any
missing fields are stored as NULL.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

Row = Tuple[Optional[int], Optional[int], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]


def iter_json_values(text: str) -> Iterator[Any]:
    """Yield JSON values from a string that may contain multiple payloads.

    This uses ``json.JSONDecoder.raw_decode`` to iteratively decode any valid
    JSON values separated by whitespace. It allows users to concatenate payloads
    without wrapping them in an outer list.
    """

    decoder = json.JSONDecoder()
    idx = 0
    length = len(text)
    while idx < length:
        while idx < length and text[idx].isspace():
            idx += 1
        if idx >= length:
            break
        obj, next_idx = decoder.raw_decode(text, idx)
        yield obj
        idx = next_idx


def normalize_payload(payload: Any) -> List[Dict[str, Any]]:
    """Ensure the payload is a list of event dictionaries."""
    if isinstance(payload, str):
        payload = json.loads(payload)
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        return payload
    raise ValueError(f"Unsupported payload type: {type(payload)}")


def extract_rows(event: Dict[str, Any]) -> List[Row]:
    """Flatten a single event dictionary into database rows."""
    rows: List[Row] = []
    stream = event.get("Stream")
    function = event.get("Function")
    ceid = event.get("CEID")
    data = event.get("DATA", {}) or {}

    for report in data.get("RPTID_Set", []) or []:
        rptid = report.get("RPTID")
        eqp = report.get("EQP_Control_State_Set") or {}
        eqpid = eqp.get("EQPID")

        product_list = report.get("Product_Object_List")
        if product_list:
            for product in product_list:
                rows.append(
                    (
                        stream,
                        function,
                        ceid,
                        rptid,
                        eqpid,
                        product.get("LOTID") if product else None,
                        product.get("CARRIERID") if product else None,
                        product.get("JIGID") if product else None,
                        product.get("MATID") if product else None,
                        product.get("MATERIALID") if product else None,
                    )
                )
        else:
            rows.append((stream, function, ceid, rptid, eqpid, None, None, None, None, None))
    return rows


def ensure_table(connection: sqlite3.Connection, table: str) -> None:
    """Create the target table if it does not exist."""
    connection.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            STREAM INTEGER,
            FUNTIONC INTEGER,
            CEID TEXT,
            RPTID TEXT,
            EQPID TEXT,
            LOTID TEXT,
            CARIERID TEXT,
            JIGID TEXT,
            MATID TEXT,
            MATERIALID TEXT
        )
        """
    )


def insert_rows(connection: sqlite3.Connection, table: str, rows: Iterable[Row]) -> None:
    """Insert the provided rows into the target table."""
    connection.executemany(
        f"""
        INSERT INTO {table} (STREAM, FUNTIONC, CEID, RPTID, EQPID, LOTID, CARIERID, JIGID, MATID, MATERIALID)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        list(rows),
    )


def load_payloads(path: Path) -> List[Any]:
    """Read and parse all JSON payloads from the given file."""
    text = path.read_text(encoding="utf-8")
    return list(iter_json_values(text))


def ingest(database: Path, table: str, payloads: List[Any]) -> int:
    """Ingest all payloads into the SQLite database.

    Returns the number of rows inserted.
    """
    rows: List[Row] = []
    for payload in payloads:
        for event in normalize_payload(payload):
            rows.extend(extract_rows(event))

    with sqlite3.connect(database) as conn:
        ensure_table(conn, table)
        insert_rows(conn, table, rows)
        conn.commit()
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Insert SECS-like JSON payloads into a SQLite table.")
    parser.add_argument("input", type=Path, help="Path to a file containing one or more JSON payloads.")
    parser.add_argument("--db", default="events.db", type=Path, help="Path to the SQLite database file (default: events.db).")
    parser.add_argument("--table", default="events", help="Target table name (default: events).")
    args = parser.parse_args()

    payloads = load_payloads(args.input)
    inserted = ingest(args.db, args.table, payloads)
    print(f"Inserted {inserted} row(s) into '{args.table}' in {args.db}.")


if __name__ == "__main__":
    main()
