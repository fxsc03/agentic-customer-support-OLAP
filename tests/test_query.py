"""
单查询测试入口 — 验证完整的 Supervisor → SubAgent → SQL → DB 流程。
支持 verbose 模式，逐轮打印 LLM 输出和工具调用详情。

用法:
    cd agentic-customer-support-OLAP
    python tests/test_query.py
    python tests/test_query.py "各区域的总营收是多少"
"""
from __future__ import annotations

import json
import os
import sys
import textwrap

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from olap_agent.supervisor import TpchSupervisorAgent


EXAMPLE_QUERIES = [
    "统计每种退货标记(l_returnflag)和行状态(l_linestatus)的总数量、总金额、平均折扣",
    "找出1995年到1996年间，BUILDING市场的客户未完成订单的总营收前10",
    "各区域的供应商在1994年的总营收排名",
    "哪些客户的订单总金额超过平均值的3倍",
    "查看每种运输方式(l_shipmode)的平均配送延迟天数",
]


def _indent(text: str, prefix: str = "    ") -> str:
    return textwrap.indent(text, prefix)


def verbose_event_handler(event_type: str, data) -> None:
    """Print each ReAct step as it happens."""
    if event_type == "llm_start":
        print(f"\n{'─'*60}")
        print(f"  LLM Round {data}")
        print(f"{'─'*60}")

    elif event_type == "llm_output":
        content = data.get("content", "")
        tool_calls = data.get("tool_calls")

        if content:
            print(f"\n  [LLM 思考/回复]")
            print(_indent(content))

        if tool_calls:
            print(f"\n  [LLM 请求调用 {len(tool_calls)} 个工具]")
            for i, tc in enumerate(tool_calls, 1):
                fn = tc["function"]
                try:
                    args_pretty = json.dumps(
                        json.loads(fn["arguments"]), indent=2, ensure_ascii=False,
                    )
                except (json.JSONDecodeError, TypeError):
                    args_pretty = fn["arguments"]
                print(f"    #{i} {fn['name']}")
                print(_indent(args_pretty, "      "))

    elif event_type == "tool_start":
        pass

    elif event_type == "tool_result":
        name = data.name
        elapsed = data.elapsed_ms
        result_str = data.result
        try:
            result_obj = json.loads(result_str)
            result_preview = json.dumps(result_obj, indent=2, ensure_ascii=False)
        except (json.JSONDecodeError, TypeError):
            result_preview = result_str

        MAX_PREVIEW = 1500
        if len(result_preview) > MAX_PREVIEW:
            result_preview = result_preview[:MAX_PREVIEW] + f"\n    ... (truncated, total {len(result_str)} chars)"

        print(f"\n  [工具返回] {name}  ({elapsed}ms)")
        print(_indent(result_preview, "    "))


def main():
    query = sys.argv[1] if len(sys.argv) > 1 else EXAMPLE_QUERIES[0]

    print(f"\nQuery: {query}")
    print("=" * 60)

    supervisor = TpchSupervisorAgent()
    result = supervisor.run(query, on_event=verbose_event_handler)

    print(f"\n\n{'='*60}")
    print(f"Summary")
    print(f"{'='*60}")
    print(f"  Routing:     {result.domain} ({result.routing_elapsed_s}s)")
    print(f"  LLM rounds:  {result.agent_result.llm_rounds}")
    print(f"  Tool calls:  {len(result.agent_result.tool_calls)}")
    for tc in result.agent_result.tool_calls:
        sql_preview = ""
        if tc.name == "tpch_sql_query":
            sql_preview = tc.arguments.get("sql", "")[:80]
        print(f"    - {tc.name} ({tc.elapsed_ms}ms) {sql_preview}")
    print(f"  Total time:  {result.agent_result.total_elapsed_s}s")
    print(f"\nFinal Answer:")
    print(_indent(result.agent_result.text))


if __name__ == "__main__":
    main()
