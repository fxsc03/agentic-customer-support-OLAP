"""OLAP 工具集 — 面向只读分析查询的 SQL 执行工具。"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_engine: Optional[Engine] = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        url = os.environ.get("DATABASE_URL")
        if not url:
            raise RuntimeError("DATABASE_URL environment variable not set")
        _engine = create_engine(url, pool_size=8, pool_pre_ping=True)
    return _engine


# ------------------------------------------------------------------
# Tool implementations
# ------------------------------------------------------------------

def tpch_sql_query(sql: str, params: Optional[dict] = None, **kwargs) -> str:
    """Execute a read-only SQL query and return results as JSON.

    Only SELECT statements are allowed.
    """
    stripped = sql.strip().rstrip(";").strip()
    first_word = stripped.split()[0].upper() if stripped else ""
    if first_word not in ("SELECT", "WITH", "EXPLAIN", "SHOW", "DESCRIBE", "DESC"):
        return json.dumps({"error": f"Only SELECT queries allowed, got: {first_word}"})

    engine = get_engine()
    t0 = time.perf_counter()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            rows = [dict(row._mapping) for row in result]
        elapsed_ms = round((time.perf_counter() - t0) * 1000, 1)
        return json.dumps({
            "rows": rows[:200],
            "row_count": len(rows),
            "truncated": len(rows) > 200,
            "query_time_ms": elapsed_ms,
        }, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


def tpch_get_schema(table_name: str = "", **kwargs) -> str:
    """Get DDL / column info for one or all TPC-H tables."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            if table_name:
                rows = conn.execute(
                    text("SHOW COLUMNS FROM " + table_name)
                )
                cols = [dict(r._mapping) for r in rows]
                return json.dumps({"table": table_name, "columns": cols}, default=str)
            else:
                rows = conn.execute(text("SHOW TABLES"))
                tables = [list(r._mapping.values())[0] for r in rows]
                return json.dumps({"tables": tables})
    except Exception as e:
        return json.dumps({"error": str(e)})


def tpch_explain_query(sql: str, **kwargs) -> str:
    """Return EXPLAIN output for a query (helps understand execution plan)."""
    engine = get_engine()
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"EXPLAIN {sql}"))
            plan = [dict(r._mapping) for r in rows]
        return json.dumps(plan, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


def tpch_get_table_stats(**kwargs) -> str:
    """Return row counts for all TPC-H tables."""
    engine = get_engine()
    tpch_tables = [
        "region", "nation", "supplier", "customer",
        "part", "partsupp", "orders", "lineitem",
    ]
    stats = {}
    try:
        with engine.connect() as conn:
            for t in tpch_tables:
                try:
                    cnt = conn.execute(text(f"SELECT COUNT(*) AS c FROM {t}")).scalar()
                    stats[t] = cnt
                except Exception:
                    stats[t] = "table not found"
        return json.dumps(stats)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ------------------------------------------------------------------
# Tool registry (name -> implementation)
# ------------------------------------------------------------------

TOOL_IMPL = {
    "tpch_sql_query": tpch_sql_query,
    "tpch_get_schema": tpch_get_schema,
    "tpch_explain_query": tpch_explain_query,
    "tpch_get_table_stats": tpch_get_table_stats,
}


# ------------------------------------------------------------------
# OpenAI tool specs (for LLM function calling)
# ------------------------------------------------------------------

TOOL_SPECS = [
    {
        "type": "function",
        "function": {
            "name": "tpch_sql_query",
            "description": (
                "Execute a read-only SQL SELECT query against the TPC-H database. "
                "Returns up to 200 rows as JSON with query timing. "
                "Use :param_name placeholders for parameters."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL SELECT query to execute",
                    },
                    "params": {
                        "type": "object",
                        "description": "Named bind parameters (optional)",
                    },
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tpch_get_schema",
            "description": (
                "Get column definitions for a specific TPC-H table, "
                "or list all available tables if table_name is empty."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Table name (e.g. 'lineitem'). Empty string to list all tables.",
                    },
                },
                "required": ["table_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tpch_explain_query",
            "description": "Return the EXPLAIN execution plan for a SQL query.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL query to explain",
                    },
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "tpch_get_table_stats",
            "description": "Return row counts for all TPC-H tables (region, nation, supplier, customer, part, partsupp, orders, lineitem).",
            "parameters": {
                "type": "object",
                "properties": {},
            },
        },
    },
]
