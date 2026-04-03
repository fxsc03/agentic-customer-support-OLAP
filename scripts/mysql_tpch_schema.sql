-- TPC-H schema for MySQL / SeekDB
-- 8 tables: region, nation, supplier, customer, part, partsupp, orders, lineitem
--
-- Usage:
--   mysql -h127.0.0.1 --port=2881 -u'root@sys' --password=123 test2 < scripts/mysql_tpch_schema.sql

CREATE TABLE IF NOT EXISTS region (
  r_regionkey INT PRIMARY KEY,
  r_name      VARCHAR(25) NOT NULL,
  r_comment   VARCHAR(152)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS nation (
  n_nationkey INT PRIMARY KEY,
  n_name      VARCHAR(25) NOT NULL,
  n_regionkey INT NOT NULL,
  n_comment   VARCHAR(152),
  CONSTRAINT fk_nation_region FOREIGN KEY (n_regionkey) REFERENCES region (r_regionkey)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS supplier (
  s_suppkey   INT PRIMARY KEY,
  s_name      VARCHAR(25) NOT NULL,
  s_address   VARCHAR(40),
  s_nationkey INT NOT NULL,
  s_phone     VARCHAR(15),
  s_acctbal   DECIMAL(15,2),
  s_comment   VARCHAR(101),
  CONSTRAINT fk_supplier_nation FOREIGN KEY (s_nationkey) REFERENCES nation (n_nationkey)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS customer (
  c_custkey    INT PRIMARY KEY,
  c_name       VARCHAR(25) NOT NULL,
  c_address    VARCHAR(40),
  c_nationkey  INT NOT NULL,
  c_phone      VARCHAR(15),
  c_acctbal    DECIMAL(15,2),
  c_mktsegment VARCHAR(10),
  c_comment    VARCHAR(117),
  CONSTRAINT fk_customer_nation FOREIGN KEY (c_nationkey) REFERENCES nation (n_nationkey)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS part (
  p_partkey    INT PRIMARY KEY,
  p_name       VARCHAR(55) NOT NULL,
  p_mfgr       VARCHAR(25),
  p_brand      VARCHAR(10),
  p_type       VARCHAR(25),
  p_size       INT,
  p_container  VARCHAR(10),
  p_retailprice DECIMAL(15,2),
  p_comment    VARCHAR(23)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS partsupp (
  ps_partkey   INT NOT NULL,
  ps_suppkey   INT NOT NULL,
  ps_availqty  INT,
  ps_supplycost DECIMAL(15,2),
  ps_comment   VARCHAR(199),
  PRIMARY KEY (ps_partkey, ps_suppkey),
  CONSTRAINT fk_ps_part FOREIGN KEY (ps_partkey) REFERENCES part (p_partkey),
  CONSTRAINT fk_ps_supplier FOREIGN KEY (ps_suppkey) REFERENCES supplier (s_suppkey)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS orders (
  o_orderkey     INT PRIMARY KEY,
  o_custkey      INT NOT NULL,
  o_orderstatus  CHAR(1),
  o_totalprice   DECIMAL(15,2),
  o_orderdate    DATE,
  o_orderpriority VARCHAR(15),
  o_clerk        VARCHAR(15),
  o_shippriority INT,
  o_comment      VARCHAR(79),
  CONSTRAINT fk_orders_customer FOREIGN KEY (o_custkey) REFERENCES customer (c_custkey)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS lineitem (
  l_orderkey     INT NOT NULL,
  l_partkey      INT NOT NULL,
  l_suppkey      INT NOT NULL,
  l_linenumber   INT NOT NULL,
  l_quantity     DECIMAL(15,2),
  l_extendedprice DECIMAL(15,2),
  l_discount     DECIMAL(15,2),
  l_tax          DECIMAL(15,2),
  l_returnflag   CHAR(1),
  l_linestatus   CHAR(1),
  l_shipdate     DATE,
  l_commitdate   DATE,
  l_receiptdate  DATE,
  l_shipinstruct VARCHAR(25),
  l_shipmode     VARCHAR(10),
  l_comment      VARCHAR(44),
  PRIMARY KEY (l_orderkey, l_linenumber),
  CONSTRAINT fk_li_order FOREIGN KEY (l_orderkey) REFERENCES orders (o_orderkey),
  CONSTRAINT fk_li_partsupp FOREIGN KEY (l_partkey, l_suppkey) REFERENCES partsupp (ps_partkey, ps_suppkey)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
