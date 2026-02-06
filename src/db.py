# src/db.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    schema: str = "public"


def load_db_config(path: Path) -> DBConfig:
    cfg = yaml.safe_load(path.read_text())
    p = cfg["postgres"]
    return DBConfig(
        host=p["host"],
        port=int(p["port"]),
        dbname=p["dbname"],
        user=p["user"],
        password=p["password"],
        schema=p.get("schema", "public"),
    )


def make_engine(db: DBConfig) -> Engine:
    url = f"postgresql+psycopg2://{db.user}:{db.password}@{db.host}:{db.port}/{db.dbname}"
    # future=True in SA 2.0 style; pool_pre_ping helps local dev
    return create_engine(url, future=True, pool_pre_ping=True)
