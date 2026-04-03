# agentic-customer-support-OLAP

基于 LLM 多 Agent 架构的 TPC-H 分析型数据库 (OLAP) benchmark 系统。用户用自然语言提出分析问题，系统自动路由到领域子 Agent，由 LLM 生成 SQL 并执行，返回结构化的分析结果。

## 架构概览

```
用户自然语言问题
       │
       ▼
┌──────────────────────┐
│ TpchSupervisorAgent  │  ← 第一层：LLM 意图分类（无工具）
│  (supervisor_tpch)   │
└──────┬───────────────┘
       │ 路由到 4 个领域之一
       ▼
┌──────────────────────────────────────────────────┐
│  PricingRevenueAgent   │  ShippingLogisticsAgent │
│  (定价/营收/折扣分析)    │  (物流/配送/订单优先级)   │
├────────────────────────┼─────────────────────────┤
│  CustomerMarketAgent   │  SupplierPartAgent      │
│  (客户/市场/订单行为)    │  (供应商/零件/供应链)     │
└──────────┬─────────────┴──────────┬──────────────┘
           │  第二层：ReAct 循环     │
           │  LLM 生成 SQL → 执行   │
           ▼                       ▼
    ┌─────────────────────────────────┐
    │         工具集 (只读)            │
    │  tpch_sql_query    执行 SELECT  │
    │  tpch_get_schema   查看表结构    │
    │  tpch_explain_query 执行计划    │
    │  tpch_get_table_stats 行数统计  │
    └──────────────┬──────────────────┘
                   ▼
           ┌──────────────┐
           │   SeekDB     │
           │  (TPC-H 库)  │
           └──────────────┘
```

### 两层 Agent 设计

| 层级 | 组件 | 作用 | LLM 调用 |
|------|------|------|---------|
| 第一层 | TpchSupervisorAgent | 将自然语言问题分类到 4 个分析领域 | 1 次（仅返回领域名） |
| 第二层 | 领域子 Agent | ReAct 循环：理解问题 → 查 schema → 写 SQL → 执行 → 解读结果 | 2~5 次 |

### 与 OLTP (TPC-C) 架构的对比

| 维度 | OLTP (agentic-customer-support) | OLAP (本项目) |
|------|--------------------------------|--------------|
| 子 Agent 划分 | 按事务类型（NewOrder, Payment, Delivery...） | 按分析领域（定价、物流、客户、供应链） |
| 工具设计 | 每个操作一个预定义工具（如 `tpcc_update_stock`） | 通用 Text-to-SQL 工具（LLM 直接生成复杂 SQL） |
| SQL 来源 | 工具内部硬编码 SQL 模板 | LLM 根据 schema 和问题动态生成 |
| 数据操作 | 读写混合（INSERT / UPDATE / SELECT） | 纯只读（仅 SELECT） |
| 查询复杂度 | 单表或简单 JOIN | 多表 JOIN + GROUP BY + 子查询 + 聚合函数 |
| 参数注入 | 框架自动注入 w_id/d_id/c_id | 无需注入，LLM 自行推断参数 |
| 依赖 | mlflow + databricks SDK + openai | 仅 openai + sqlalchemy（轻量自包含） |

## 数据库 Schema

标准 TPC-H 8 张表：

```
region ← nation ← supplier ← partsupp → part
                ↑                ↑
                └── customer ← orders ← lineitem
```

| 表 | 说明 | 主键 |
|----|------|------|
| region | 5 个地区 | r_regionkey |
| nation | 25 个国家 | n_nationkey |
| supplier | 供应商 | s_suppkey |
| customer | 客户 | c_custkey |
| part | 零件 | p_partkey |
| partsupp | 零件-供应商关系 | (ps_partkey, ps_suppkey) |
| orders | 订单 | o_orderkey |
| lineitem | 订单行明细 | (l_orderkey, l_linenumber) |

## 4 个领域子 Agent 与 TPC-H 22 条查询的映射

### PricingRevenueAgent (定价/营收)

覆盖 TPC-H 查询：Q1, Q6, Q14, Q17, Q19

- Q1: 按退货标记和行状态汇总定价（`SUM`, `AVG`, `COUNT`）
- Q6: 特定折扣和数量范围的潜在营收提升
- Q14: 促销商品营收占比
- Q17: 小批量订单的平均营收
- Q19: 特定品牌/容器/数量组合的折后营收

### ShippingLogisticsAgent (物流/配送)

覆盖 TPC-H 查询：Q3, Q5, Q7, Q8, Q10, Q12, Q21

- Q3: 未发货订单的营收排名（按配送优先级）
- Q5: 区域内本地供应商营收
- Q7/Q8: 国家间贸易额 / 区域市场份额
- Q10: 退货金额最高的客户
- Q12: 不同运输方式的订单优先级分布
- Q21: 供应商交付延迟分析

### CustomerMarketAgent (客户/市场)

覆盖 TPC-H 查询：Q4, Q13, Q18, Q22

- Q4: 有延迟交付的订单数量分布
- Q13: 客户按订单数量的分布（含零单客户）
- Q18: 大客户（高消费总额）排名
- Q22: 潜在客户机会（有余额无订单）

### SupplierPartAgent (供应商/零件)

覆盖 TPC-H 查询：Q2, Q9, Q11, Q15, Q16, Q20

- Q2: 特定零件类型的最低成本供应商
- Q9: 各国各年的产品利润
- Q11: 库存价值排名前列的重要零件
- Q15: 总营收最高的供应商
- Q16: 零件-供应商多样性分析
- Q20: 供应量超阈值的潜在促销零件

## 工具集

所有子 Agent 共享同一套只读工具：

| 工具 | 功能 | 典型用途 |
|------|------|---------|
| `tpch_sql_query` | 执行 SELECT 查询，返回 JSON 结果（最多 200 行），附带查询耗时 | 核心分析工具 |
| `tpch_get_schema` | 查看单表列定义或列出所有表 | LLM 在写 SQL 前确认列名 |
| `tpch_explain_query` | 返回 EXPLAIN 执行计划 | 诊断慢查询 |
| `tpch_get_table_stats` | 返回所有 8 张表的行数 | 了解数据规模 |

安全约束：`tpch_sql_query` 仅允许 `SELECT` / `WITH` / `EXPLAIN` / `SHOW` / `DESCRIBE` 开头的语句，拒绝任何写操作。

## ReAct 循环流程

每个子 Agent 内部执行 ReAct (Reasoning + Acting) 循环（最多 15 轮）：

```
                    ┌─────────────┐
                    │  用户问题    │
                    └──────┬──────┘
                           ▼
              ┌────────────────────────┐
              │ system prompt + 问题   │
              │ 发送给 LLM             │
              └────────────┬───────────┘
                           ▼
                  ┌─────────────────┐
           ┌──── │  LLM 返回什么？   │ ────┐
           │     └─────────────────┘     │
     tool_calls                      纯文本回复
           │                             │
           ▼                             ▼
  ┌─────────────────┐           ┌──────────────┐
  │ 执行工具         │           │ 结束循环      │
  │ (SQL查询/查schema)│           │ 返回最终回复  │
  └────────┬────────┘           └──────────────┘
           │
           ▼
  ┌─────────────────┐
  │ 工具结果追加到   │
  │ 消息历史         │──────→ 回到 LLM
  └─────────────────┘
```

## 目录结构

```
agentic-customer-support-OLAP/
├── pyproject.toml                        # 项目依赖
├── README.md                             # 本文档
│
├── configs/agents/                       # Agent YAML 配置
│   ├── supervisor_tpch.yaml              #   路由 Agent (意图分类 prompt)
│   ├── pricing_revenue.yaml              #   定价/营收 Agent (含 TPC-H schema)
│   ├── shipping_logistics.yaml           #   物流/配送 Agent
│   ├── customer_market.yaml              #   客户/市场 Agent
│   └── supplier_part.yaml                #   供应商/零件 Agent
│
├── olap_agent/                           # 核心代码
│   ├── __init__.py
│   ├── tools.py                          #   工具实现 + OpenAI tool spec
│   ├── base_agent.py                     #   ReAct 循环引擎
│   └── supervisor.py                     #   Supervisor + 4 个领域子 Agent
│
├── scripts/                              # 数据库脚本
│   ├── mysql_tpch_schema.sql             #   TPC-H 建表 DDL (MySQL/SeekDB)
│   └── seed_tpch_data.py                 #   数据灌入 (可配 scale factor)
│
└── tests/                                # 测试 & Benchmark
    ├── test_query.py                     #   单查询端到端测试
    └── tpch_benchmark.py                 #   多线程 benchmark 驱动
```

## 快速开始

### 前置条件

- Python >= 3.10
- SeekDB / MySQL 实例运行中
- OpenAI 兼容的 LLM API（如硅基流动 SiliconFlow）

### 1. 安装依赖

```bash
cd agentic-customer-support-OLAP
pip install openai sqlalchemy pymysql pyyaml backoff cryptography "httpx[socks]"
```

### 2. 配置环境变量

```bash
# 数据库连接（使用独立的 tpch 库，避免与 TPC-C 表冲突）
export DATABASE_URL='mysql+pymysql://root%40sys:123@127.0.0.1:2881/tpch'

# LLM API（以硅基流动为例）
export OPENAI_API_KEY='sk-mzfkbvddcaszmmodnkrpcnwywpoanjhpyuxpttcejcifmwhu'
export OPENAI_BASE_URL='https://api.siliconflow.cn/v1'
export OPENAI_MODEL='Pro/deepseek-ai/DeepSeek-V3'
```

fish shell 用户：

```fish
set -Ux DATABASE_URL 'mysql+pymysql://root%40sys:123@127.0.0.1:2881/tpch'
set -Ux OPENAI_API_KEY 'sk-mzfkbvddcaszmmodnkrpcnwywpoanjhpyuxpttcejcifmwhu'
set -Ux OPENAI_BASE_URL 'https://api.siliconflow.cn/v1'
set -Ux OPENAI_MODEL 'Pro/deepseek-ai/DeepSeek-V3'
```

### 3. 建表

```bash
mysql -h127.0.0.1 --port=2881 -u'root@sys' --password=123 -e "CREATE DATABASE IF NOT EXISTS tpch;"
mysql -h127.0.0.1 --port=2881 -u'root@sys' --password=123 tpch < scripts/mysql_tpch_schema.sql
```

### 4. 灌入数据

```bash
# 极小规模 (SF=0.01, ~1500 订单, ~6000 行明细)
python scripts/seed_tpch_data.py --scale 0.01

# 中等规模 (SF=0.1, ~15000 订单)
python scripts/seed_tpch_data.py --scale 0.1
```

### 5. 单查询测试

```bash
python tests/test_query.py "各区域的总营收是多少"
python tests/test_query.py "哪个供应商的供货成本最低"
python tests/test_query.py "BUILDING市场的客户订单总金额排名前10"
```

### 6. 运行 Benchmark

```bash
# 2 线程，120 秒
python tests/tpch_benchmark.py --workers 2 --duration 120

# 保存结果到 JSON
python tests/tpch_benchmark.py --workers 4 --duration 300 --output results.json
```

## Benchmark 输出示例

```
==============================================================================
  OLAP AGENT TPC-H BENCHMARK RESULTS
==============================================================================
  Workers:            2
  Elapsed:            120.0s
  Total queries:      12
  Success:            11
  Fail:               1
  Success rate:       91.7%
  Routing accuracy:   83.3%
  Avg latency:        18.5s
  P50 latency:        15.2s
  P95 latency:        35.1s
  Throughput:         5.5 queries/min

  Domain                  Cnt   OK Fail   Rate     Avg     P50     P95   LLM Tools
  ----------------------------------------------------------------------------
  pricing_revenue           4    4    0 100.0%  14.233  13.100  16.500   2.5   1.8
  shipping_logistics        3    3    0 100.0%  22.100  20.300  25.000   3.0   2.3
  customer_market           3    2    1  66.7%  18.500  18.500  18.500   2.0   1.5
  supplier_part             2    2    0 100.0%  20.800  20.800  20.800   2.5   2.0
==============================================================================
```

### 关键指标说明

| 指标 | 含义 |
|------|------|
| Throughput (queries/min) | 每分钟完成的分析查询数 |
| Routing accuracy | Supervisor 路由到正确领域的比例 |
| LLM rounds | 每条查询平均调用 LLM 的次数 |
| Tool calls | 每条查询平均调用工具的次数 |
| P50 / P95 latency | 端到端延迟的中位数和 95 分位 |

## 扩展指南

### 添加新的分析领域

1. 在 `configs/agents/` 下创建新的 YAML 配置文件（定义 system_prompt 和 tools）
2. 在 `olap_agent/supervisor.py` 中添加新的子 Agent 类和 `DOMAIN_AGENT_MAP` 映射
3. 在 `supervisor_tpch.yaml` 的 system prompt 中添加新领域的描述和关键词

### 添加新的工具

1. 在 `olap_agent/tools.py` 中实现工具函数
2. 添加对应的 OpenAI tool spec 到 `TOOL_SPECS`
3. 在 `TOOL_IMPL` 字典中注册
4. 在需要使用该工具的 Agent YAML 配置中的 `tools` 列表里添加工具名

### 更换 LLM

修改环境变量即可，支持所有 OpenAI 兼容的 API：

```bash
# 本地 Ollama
export OPENAI_BASE_URL='http://127.0.0.1:11434/v1'
export OPENAI_MODEL='qwen2:latest'
export OPENAI_API_KEY='ollama'

# DeepSeek 官方
export OPENAI_BASE_URL='https://api.deepseek.com/v1'
export OPENAI_MODEL='deepseek-chat'
```
