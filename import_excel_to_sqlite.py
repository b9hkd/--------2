#!/usr/bin/env python3
"""
Simple importer: Excel (.xlsx) -> SQLite.

Usage:
  python import_excel_to_sqlite.py Заявки_CRM_2025_солнечные_панели.xlsx [--db crm_orders.sqlite]

The script creates the SQLite file if it does not exist and upserts rows
based on the `id` column, so repeated imports are safe.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Iterable, List

import pandas as pd


# Mapping from Excel column names to SQLite column names
COLUMN_MAP = {
    "ID": "id",
    "Дата заказа": "order_date",
    "Клиент": "client",
    "Тип клиента": "customer_type",
    "Город": "city",
    "Состав заказа": "order_items",
    "Тип заказа": "order_type",
    "Менеджер": "manager",
    "Источник заявки": "lead_source",
    "Сумма заказа": "order_amount",
}

REQUIRED_COLUMNS = set(COLUMN_MAP.keys())


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            order_date TEXT NOT NULL,
            client TEXT NOT NULL,
            customer_type TEXT NOT NULL,
            city TEXT NOT NULL,
            order_items TEXT NOT NULL,
            order_type TEXT NOT NULL,
            manager TEXT NOT NULL,
            lead_source TEXT NOT NULL,
            order_amount INTEGER NOT NULL
        );
        """
    )


def normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns in Excel: {sorted(missing)}")

    # Keep only known columns, rename to snake_case for SQLite
    df = df[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP).copy()

    # Normalize types
    df["id"] = pd.to_numeric(df["id"], errors="raise").astype(int)
    df["order_amount"] = pd.to_numeric(df["order_amount"], errors="raise").astype(int)
    df["order_date"] = pd.to_datetime(df["order_date"], errors="raise").dt.date.astype(str)

    # Strip whitespace from text fields
    text_cols = [c for c in df.columns if c not in {"id", "order_amount", "order_date"}]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()

    return df


def upsert_rows(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    rows = df.to_dict("records")
    conn.executemany(
        """
        INSERT INTO orders (
            id, order_date, client, customer_type, city,
            order_items, order_type, manager, lead_source, order_amount
        ) VALUES (
            :id, :order_date, :client, :customer_type, :city,
            :order_items, :order_type, :manager, :lead_source, :order_amount
        )
        ON CONFLICT(id) DO UPDATE SET
            order_date = excluded.order_date,
            client = excluded.client,
            customer_type = excluded.customer_type,
            city = excluded.city,
            order_items = excluded.order_items,
            order_type = excluded.order_type,
            manager = excluded.manager,
            lead_source = excluded.lead_source,
            order_amount = excluded.order_amount;
        """,
        rows,
    )
    conn.commit()
    return len(rows)


def read_excel(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, engine="openpyxl")
    return normalize_frame(df)


def collect_paths(inputs: Iterable[str]) -> List[Path]:
    paths: List[Path] = []
    for item in inputs:
        p = Path(item)
        if p.is_dir():
            paths.extend(sorted(p.glob("*.xlsx")))
        else:
            paths.append(p)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Import Excel orders into SQLite")
    parser.add_argument("excel", nargs="+", help="Path(s) to .xlsx file(s) or folder with them")
    parser.add_argument("--db", default="crm_orders.sqlite", help="SQLite file path (will be created)")
    args = parser.parse_args()

    excel_paths = collect_paths(args.excel)
    if not excel_paths:
        raise SystemExit("No Excel files found")

    conn = sqlite3.connect(args.db)
    ensure_schema(conn)

    total = 0
    for path in excel_paths:
        df = read_excel(path)
        count = upsert_rows(conn, df)
        total += count
        print(f"Imported {count} rows from {path}")

    print(f"Done. Total rows processed: {total}. Database: {args.db}")


if __name__ == "__main__":
    main()
