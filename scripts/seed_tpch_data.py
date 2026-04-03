"""
TPC-H 数据灌入脚本 — 纯 SQLAlchemy，生成小规模 TPC-H 数据到 SeekDB。

用法:
    cd agentic-customer-support-OLAP
    DATABASE_URL='mysql+pymysql://root%40sys:123@127.0.0.1:2881/test2' python scripts/seed_tpch_data.py
    DATABASE_URL='...' python scripts/seed_tpch_data.py --scale 0.01   # SF=0.01 极小
    DATABASE_URL='...' python scripts/seed_tpch_data.py --scale 0.1    # SF=0.1 中等
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import date, timedelta

from sqlalchemy import create_engine, text

BATCH_SIZE = 500

REGIONS = [
    (0, "AFRICA"), (1, "AMERICA"), (2, "ASIA"), (3, "EUROPE"), (4, "MIDDLE EAST"),
]

NATIONS = [
    (0, "ALGERIA", 0), (1, "ARGENTINA", 1), (2, "BRAZIL", 1), (3, "CANADA", 1),
    (4, "EGYPT", 4), (5, "ETHIOPIA", 0), (6, "FRANCE", 3), (7, "GERMANY", 3),
    (8, "INDIA", 2), (9, "INDONESIA", 2), (10, "IRAN", 4), (11, "IRAQ", 4),
    (12, "JAPAN", 2), (13, "JORDAN", 4), (14, "KENYA", 0), (15, "MOROCCO", 0),
    (16, "MOZAMBIQUE", 0), (17, "PERU", 1), (18, "CHINA", 2), (19, "ROMANIA", 3),
    (20, "SAUDI ARABIA", 4), (21, "VIETNAM", 2), (22, "RUSSIA", 3),
    (23, "UNITED KINGDOM", 3), (24, "UNITED STATES", 1),
]

SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"]
PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
SHIP_MODES = ["MAIL", "SHIP", "AIR", "TRUCK", "REG AIR", "RAIL", "FOB"]
SHIP_INSTRUCTS = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
STATUSES = ["F", "O"]
BRANDS = [f"Brand#{i}{j}" for i in range(1, 6) for j in range(1, 6)]
TYPES = [
    f"{m} {t}"
    for m in ["STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"]
    for t in ["ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"]
]
CONTAINERS = [
    f"{s} {c}"
    for s in ["SM", "MED", "LG", "WRAP", "JUMBO"]
    for c in ["CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"]
]
MFGRS = [f"Manufacturer#{i}" for i in range(1, 6)]

START_DATE = date(1992, 1, 1)
END_DATE = date(1998, 12, 31)
DATE_RANGE = (END_DATE - START_DATE).days


def _rand_date(rng: random.Random) -> date:
    return START_DATE + timedelta(days=rng.randint(0, DATE_RANGE))


def _batch_insert(conn, sql: str, rows: list[dict], label: str):
    total = len(rows)
    for start in range(0, total, BATCH_SIZE):
        chunk = rows[start : start + BATCH_SIZE]
        conn.execute(text(sql), chunk)
        done = min(start + BATCH_SIZE, total)
        print(f"  {label}: {done}/{total}", end="\r")
    print(f"  {label}: {total}/{total} done")


def gen_suppliers(rng: random.Random, n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "s_suppkey": i,
            "s_name": f"Supplier#{i:09d}",
            "s_address": f"{rng.randint(1,999)} Supplier Rd",
            "s_nationkey": rng.randint(0, 24),
            "s_phone": f"{rng.randint(10,34)}-{rng.randint(100,999)}-{rng.randint(100,999)}-{rng.randint(1000,9999)}",
            "s_acctbal": str(round(rng.uniform(-999.99, 9999.99), 2)),
            "s_comment": "",
        })
    return rows


def gen_customers(rng: random.Random, n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "c_custkey": i,
            "c_name": f"Customer#{i:09d}",
            "c_address": f"{rng.randint(1,999)} Customer Ave",
            "c_nationkey": rng.randint(0, 24),
            "c_phone": f"{rng.randint(10,34)}-{rng.randint(100,999)}-{rng.randint(100,999)}-{rng.randint(1000,9999)}",
            "c_acctbal": str(round(rng.uniform(-999.99, 9999.99), 2)),
            "c_mktsegment": rng.choice(SEGMENTS),
            "c_comment": "",
        })
    return rows


def gen_parts(rng: random.Random, n: int) -> list[dict]:
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "p_partkey": i,
            "p_name": f"Part-{i}-{rng.choice(['red','green','blue','yellow','white'])}",
            "p_mfgr": rng.choice(MFGRS),
            "p_brand": rng.choice(BRANDS),
            "p_type": rng.choice(TYPES),
            "p_size": rng.randint(1, 50),
            "p_container": rng.choice(CONTAINERS),
            "p_retailprice": str(round(rng.uniform(1.0, 2000.0), 2)),
            "p_comment": "",
        })
    return rows


def gen_partsupp(rng: random.Random, n_parts: int, n_suppliers: int) -> list[dict]:
    rows = []
    for pk in range(1, n_parts + 1):
        supps = rng.sample(range(1, n_suppliers + 1), min(4, n_suppliers))
        for sk in supps:
            rows.append({
                "ps_partkey": pk,
                "ps_suppkey": sk,
                "ps_availqty": rng.randint(1, 9999),
                "ps_supplycost": str(round(rng.uniform(1.0, 1000.0), 2)),
                "ps_comment": "",
            })
    return rows


def gen_orders_and_lineitems(
    rng: random.Random,
    n_orders: int,
    n_customers: int,
    partsupp_pairs: list[tuple[int, int]],
) -> tuple[list[dict], list[dict]]:
    orders = []
    lineitems = []
    for oid in range(1, n_orders + 1):
        odate = _rand_date(rng)
        n_lines = rng.randint(1, 7)
        orders.append({
            "o_orderkey": oid,
            "o_custkey": rng.randint(1, n_customers),
            "o_orderstatus": rng.choice(STATUSES),
            "o_totalprice": "0.00",
            "o_orderdate": odate.isoformat(),
            "o_orderpriority": rng.choice(PRIORITIES),
            "o_clerk": f"Clerk#{rng.randint(1, 1000):09d}",
            "o_shippriority": 0,
            "o_comment": "",
        })
        total = 0.0
        for ln in range(1, n_lines + 1):
            ps = rng.choice(partsupp_pairs)
            qty = rng.randint(1, 50)
            price = round(rng.uniform(1.0, 2000.0), 2)
            disc = round(rng.uniform(0, 0.10), 2)
            tax = round(rng.uniform(0, 0.08), 2)
            ext = round(qty * price, 2)
            total += ext * (1 - disc)
            ship_d = odate + timedelta(days=rng.randint(1, 121))
            commit_d = odate + timedelta(days=rng.randint(30, 90))
            receipt_d = ship_d + timedelta(days=rng.randint(1, 30))
            rf = "R" if rng.random() < 0.25 else ("A" if rng.random() < 0.5 else "N")
            ls = "F" if ship_d <= date(1995, 6, 17) else "O"
            lineitems.append({
                "l_orderkey": oid,
                "l_partkey": ps[0],
                "l_suppkey": ps[1],
                "l_linenumber": ln,
                "l_quantity": str(qty),
                "l_extendedprice": str(ext),
                "l_discount": str(disc),
                "l_tax": str(tax),
                "l_returnflag": rf,
                "l_linestatus": ls,
                "l_shipdate": ship_d.isoformat(),
                "l_commitdate": commit_d.isoformat(),
                "l_receiptdate": receipt_d.isoformat(),
                "l_shipinstruct": rng.choice(SHIP_INSTRUCTS),
                "l_shipmode": rng.choice(SHIP_MODES),
                "l_comment": "",
            })
        orders[-1]["o_totalprice"] = str(round(total, 2))
    return orders, lineitems


# SQL templates
SQL_REGION = "INSERT INTO region (r_regionkey, r_name, r_comment) VALUES (:rk, :rn, '')"
SQL_NATION = "INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) VALUES (:nk, :nn, :nr, '')"
SQL_SUPPLIER = (
    "INSERT INTO supplier (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment) "
    "VALUES (:s_suppkey, :s_name, :s_address, :s_nationkey, :s_phone, :s_acctbal, :s_comment)"
)
SQL_CUSTOMER = (
    "INSERT INTO customer (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment) "
    "VALUES (:c_custkey, :c_name, :c_address, :c_nationkey, :c_phone, :c_acctbal, :c_mktsegment, :c_comment)"
)
SQL_PART = (
    "INSERT INTO part (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment) "
    "VALUES (:p_partkey, :p_name, :p_mfgr, :p_brand, :p_type, :p_size, :p_container, :p_retailprice, :p_comment)"
)
SQL_PARTSUPP = (
    "INSERT INTO partsupp (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment) "
    "VALUES (:ps_partkey, :ps_suppkey, :ps_availqty, :ps_supplycost, :ps_comment)"
)
SQL_ORDER = (
    "INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment) "
    "VALUES (:o_orderkey, :o_custkey, :o_orderstatus, :o_totalprice, :o_orderdate, :o_orderpriority, :o_clerk, :o_shippriority, :o_comment)"
)
SQL_LINEITEM = (
    "INSERT INTO lineitem (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, "
    "l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment) "
    "VALUES (:l_orderkey, :l_partkey, :l_suppkey, :l_linenumber, :l_quantity, :l_extendedprice, "
    ":l_discount, :l_tax, :l_returnflag, :l_linestatus, :l_shipdate, :l_commitdate, :l_receiptdate, :l_shipinstruct, :l_shipmode, :l_comment)"
)

TRUNCATE_ORDER = [
    "lineitem", "orders", "partsupp", "stock",
    "customer", "supplier", "part", "nation", "region",
]


def main():
    parser = argparse.ArgumentParser(description="Seed TPC-H data")
    parser.add_argument("--scale", type=float, default=0.01,
                        help="Scale factor: 0.01=~1500 orders, 0.1=~15000 orders, 1.0=~150000 orders")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    sf = args.scale
    n_suppliers = max(10, int(10_000 * sf))
    n_customers = max(15, int(15_000 * sf))
    n_parts = max(20, int(20_000 * sf))
    n_orders = max(100, int(150_000 * sf))

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set")
        sys.exit(1)

    engine = create_engine(db_url)
    rng = random.Random(args.seed)

    print(f"=== TPC-H Data Seed (SF={sf}) ===")
    print(f"  Suppliers:  {n_suppliers}")
    print(f"  Customers:  {n_customers}")
    print(f"  Parts:      {n_parts}")
    print(f"  Orders:     {n_orders}")
    print()

    t0 = time.time()

    with engine.connect() as conn:
        conn.execute(text("SET FOREIGN_KEY_CHECKS=0"))
        for tbl in TRUNCATE_ORDER:
            try:
                conn.execute(text(f"TRUNCATE TABLE {tbl}"))
                print(f"  TRUNCATE {tbl}")
            except Exception:
                pass
        conn.execute(text("SET FOREIGN_KEY_CHECKS=1"))
        conn.commit()

    print("\nInserting data ...")

    with engine.connect() as conn:
        for rk, rn in REGIONS:
            conn.execute(text(SQL_REGION), {"rk": rk, "rn": rn})
        conn.commit()
        print("  region: 5/5 done")

    with engine.connect() as conn:
        for nk, nn, nr in NATIONS:
            conn.execute(text(SQL_NATION), {"nk": nk, "nn": nn, "nr": nr})
        conn.commit()
        print("  nation: 25/25 done")

    supp_rows = gen_suppliers(rng, n_suppliers)
    with engine.connect() as conn:
        _batch_insert(conn, SQL_SUPPLIER, supp_rows, "supplier")
        conn.commit()

    cust_rows = gen_customers(rng, n_customers)
    with engine.connect() as conn:
        _batch_insert(conn, SQL_CUSTOMER, cust_rows, "customer")
        conn.commit()

    part_rows = gen_parts(rng, n_parts)
    with engine.connect() as conn:
        _batch_insert(conn, SQL_PART, part_rows, "part")
        conn.commit()

    ps_rows = gen_partsupp(rng, n_parts, n_suppliers)
    with engine.connect() as conn:
        _batch_insert(conn, SQL_PARTSUPP, ps_rows, "partsupp")
        conn.commit()

    ps_pairs = [(r["ps_partkey"], r["ps_suppkey"]) for r in ps_rows]
    order_rows, li_rows = gen_orders_and_lineitems(rng, n_orders, n_customers, ps_pairs)

    with engine.connect() as conn:
        _batch_insert(conn, SQL_ORDER, order_rows, "orders")
        conn.commit()

    with engine.connect() as conn:
        _batch_insert(conn, SQL_LINEITEM, li_rows, "lineitem")
        conn.commit()

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")

    print("\n=== Verification ===")
    with engine.connect() as conn:
        for tbl in ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]:
            try:
                cnt = conn.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
                print(f"  {tbl:12s}: {cnt}")
            except Exception:
                print(f"  {tbl:12s}: (not found)")


if __name__ == "__main__":
    main()
