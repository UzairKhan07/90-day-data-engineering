"""
Database helpers for the E-Commerce Data Platform.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_connection_params() -> dict:
    host = os.getenv("POSTGRES_HOST")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    dbname = os.getenv("POSTGRES_DB")
    port = os.getenv("POSTGRES_PORT", "5432")

    missing = [k for k, v in {
        "POSTGRES_HOST": host,
        "POSTGRES_USER": user,
        "POSTGRES_PASSWORD": password,
        "POSTGRES_DB": dbname,
    }.items() if not v]

    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    return {
        "host": host,
        "port": int(port),
        "user": user,
        "password": password,
        "dbname": dbname,
    }
        

@contextmanager
def get_connection() -> Generator:
    """Context manager that yields a psycopg2 connection."""
    conn = psycopg2.connect(**get_connection_params())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()