"""
TPC-H Agent Benchmark — 多线程跑 TPC-H 分析查询，统计延迟和吞吐。

用法:
    cd agentic-customer-support-OLAP
    python tests/tpch_benchmark.py --workers 2 --duration 120
    python tests/tpch_benchmark.py --workers 1 --duration 60 --output results.json
"""
from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import sys
import threading
import time
from dataclasses import dataclass, field

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from olap_agent.supervisor import TpchSupervisorAgent

# ------------------------------------------------------------------
# TPC-H 查询模板池（按领域分类，覆盖全部 22 个 TPC-H 查询的业务语义）
# ------------------------------------------------------------------

QUERY_POOL: dict[str, list[str]] = {
    "pricing_revenue": [
        "统计每种退货标记和行状态的总数量、总金额和平均折扣（Q1风格）",
        "1994年折扣在0.05到0.07之间、数量小于24的商品的潜在营收提升（Q6风格）",
        "1995年通过促销产生的营收占总营收的百分比（Q14风格）",
        "品牌Brand#23、MED BOX容器中，数量低于该品牌平均数量0.2倍的订单行营收（Q17风格）",
        "Brand#12到Brand#23的特定容器和数量范围内的折后营收（Q19风格）",
    ],
    "shipping_logistics": [
        "1995年3月15日前，BUILDING市场未发货订单的营收TOP10，按金额降序（Q3风格）",
        "1994年ASIA区域各国的本地供应商营收（Q5风格）",
        "1995-1996年FRANCE和GERMANY之间的双向贸易额（Q7风格）",
        "AMERICA区域各国在所有订单中的营收占比，按年份统计（Q8风格）",
        "退货金额最高的前20个客户（Q10风格）",
        "MAIL和SHIP两种运输方式中，高优先级和低优先级订单的数量分布（Q12风格）",
        "哪些供应商的订单从未按时交付且涉及SAUDI ARABIA（Q21风格）",
    ],
    "customer_market": [
        "1993年7月到10月之间有问题订单（延迟交付）的数量分布（Q4风格）",
        "客户按订单数量的分布（含0单客户），类似于Q13",
        "总消费金额前100的大客户（Q18风格）",
        "电话号码以13或31开头、有正余额但无订单的客户数量和总余额（Q22风格）",
    ],
    "supplier_part": [
        "EUROPE区域内，size=15且type含BRASS的零件的最低供应成本供应商（Q2风格）",
        "各国各年的产品利润（营收减去供应成本），按年份和国家统计（Q9风格）",
        "GERMANY供应商中，库存总价值排名前1%的零件（Q11风格）",
        "GERMANY供应商中总营收最高的那个供应商（Q15风格）",
        "Brand#45以外、非MEDIUM POLISHED类型、特定尺寸的零件有多少个不同供应商（Q16风格）",
        "CANADA供应商中1994年供应数量超过阈值的零件（Q20风格）",
    ],
}

DOMAIN_WEIGHTS = {
    "pricing_revenue": 0.30,
    "shipping_logistics": 0.30,
    "customer_market": 0.20,
    "supplier_part": 0.20,
}

DOMAINS = list(DOMAIN_WEIGHTS.keys())
WEIGHTS = [DOMAIN_WEIGHTS[d] for d in DOMAINS]


# ------------------------------------------------------------------
# Metrics
# ------------------------------------------------------------------

@dataclass
class TxRecord:
    domain: str
    routed_to: str
    latency: float
    success: bool
    llm_rounds: int = 0
    tool_calls: int = 0
    error: str = ""


@dataclass
class MetricsCollector:
    records: list[TxRecord] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, rec: TxRecord):
        with self._lock:
            self.records.append(rec)

    def summary(self) -> dict:
        recs = self.records
        if not recs:
            return {"error": "no queries recorded"}

        by_domain: dict[str, list[TxRecord]] = {}
        for r in recs:
            by_domain.setdefault(r.routed_to, []).append(r)

        total = len(recs)
        successes = sum(1 for r in recs if r.success)
        all_lats = sorted(r.latency for r in recs if r.success)

        per_domain = {}
        for dom, drecs in sorted(by_domain.items()):
            lats = sorted(r.latency for r in drecs if r.success)
            ok = sum(1 for r in drecs if r.success)
            entry: dict = {
                "count": len(drecs),
                "success": ok,
                "fail": len(drecs) - ok,
                "success_rate": round(ok / len(drecs) * 100, 1) if drecs else 0,
                "avg_llm_rounds": round(statistics.mean(r.llm_rounds for r in drecs), 1),
                "avg_tool_calls": round(statistics.mean(r.tool_calls for r in drecs), 1),
            }
            if lats:
                entry.update({
                    "avg_s": round(statistics.mean(lats), 3),
                    "p50_s": round(_pct(lats, 50), 3),
                    "p95_s": round(_pct(lats, 95), 3),
                    "p99_s": round(_pct(lats, 99), 3),
                })
            per_domain[dom] = entry

        result: dict = {
            "total_queries": total,
            "total_success": successes,
            "total_fail": total - successes,
            "overall_success_rate": round(successes / total * 100, 1) if total else 0,
            "per_domain": per_domain,
        }
        if all_lats:
            result["overall_avg_latency_s"] = round(statistics.mean(all_lats), 3)
            result["overall_p50_latency_s"] = round(_pct(all_lats, 50), 3)
            result["overall_p95_latency_s"] = round(_pct(all_lats, 95), 3)
        routing_accuracy = sum(
            1 for r in recs if r.domain == r.routed_to
        ) / total if total else 0
        result["routing_accuracy"] = round(routing_accuracy * 100, 1)
        return result


def _pct(sorted_data: list[float], p: float) -> float:
    if not sorted_data:
        return 0.0
    k = (len(sorted_data) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(sorted_data) - 1)
    return sorted_data[f] + (k - f) * (sorted_data[c] - sorted_data[f])


# ------------------------------------------------------------------
# Worker
# ------------------------------------------------------------------

def worker(
    wid: int,
    supervisor: TpchSupervisorAgent,
    metrics: MetricsCollector,
    stop: threading.Event,
    warmup_until: float,
):
    rng = random.Random(42 + wid)
    while not stop.is_set():
        domain = rng.choices(DOMAINS, weights=WEIGHTS, k=1)[0]
        query = rng.choice(QUERY_POOL[domain])
        is_warmup = time.monotonic() < warmup_until

        t0 = time.perf_counter()
        success = False
        error_msg = ""
        routed_to = ""
        llm_rounds = 0
        tool_calls = 0
        try:
            result = supervisor.run(query)
            success = True
            routed_to = result.domain
            llm_rounds = result.agent_result.llm_rounds
            tool_calls = len(result.agent_result.tool_calls)
        except Exception as e:
            error_msg = str(e)[:120]
            routed_to = domain
        latency = time.perf_counter() - t0

        if not is_warmup:
            metrics.add(TxRecord(
                domain=domain,
                routed_to=routed_to,
                latency=latency,
                success=success,
                llm_rounds=llm_rounds,
                tool_calls=tool_calls,
                error=error_msg,
            ))

        status = "OK" if success else f"FAIL({error_msg[:50]})"
        phase = "[warmup]" if is_warmup else ""
        print(f"  W{wid} {phase} {routed_to:<20s} {latency:6.2f}s LLM={llm_rounds} Tools={tool_calls} {status}")


# ------------------------------------------------------------------
# Report
# ------------------------------------------------------------------

def print_report(summary: dict, elapsed: float, n_workers: int):
    print("\n" + "=" * 78)
    print("  OLAP AGENT TPC-H BENCHMARK RESULTS")
    print("=" * 78)
    print(f"  Workers:            {n_workers}")
    print(f"  Elapsed:            {elapsed:.1f}s")
    print(f"  Total queries:      {summary['total_queries']}")
    print(f"  Success:            {summary['total_success']}")
    print(f"  Fail:               {summary['total_fail']}")
    print(f"  Success rate:       {summary['overall_success_rate']}%")
    print(f"  Routing accuracy:   {summary.get('routing_accuracy', 'n/a')}%")
    if "overall_avg_latency_s" in summary:
        print(f"  Avg latency:        {summary['overall_avg_latency_s']}s")
        print(f"  P50 latency:        {summary['overall_p50_latency_s']}s")
        print(f"  P95 latency:        {summary['overall_p95_latency_s']}s")
    qpm = summary["total_success"] / (elapsed / 60) if elapsed > 0 else 0
    print(f"  Throughput:         {qpm:.1f} queries/min")
    print()
    print(f"  {'Domain':<22s} {'Cnt':>4s} {'OK':>4s} {'Fail':>4s} {'Rate':>6s} "
          f"{'Avg':>7s} {'P50':>7s} {'P95':>7s} {'LLM':>5s} {'Tools':>5s}")
    print("  " + "-" * 76)
    for dom, d in summary.get("per_domain", {}).items():
        avg = f"{d.get('avg_s', '-'):>7}" if "avg_s" in d else f"{'n/a':>7}"
        p50 = f"{d.get('p50_s', '-'):>7}" if "p50_s" in d else f"{'n/a':>7}"
        p95 = f"{d.get('p95_s', '-'):>7}" if "p95_s" in d else f"{'n/a':>7}"
        print(f"  {dom:<22s} {d['count']:>4d} {d['success']:>4d} {d['fail']:>4d} "
              f"{d['success_rate']:>5.1f}% {avg} {p50} {p95} "
              f"{d['avg_llm_rounds']:>5.1f} {d['avg_tool_calls']:>5.1f}")
    print("=" * 78)


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="OLAP Agent TPC-H Benchmark")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--duration", type=int, default=120)
    parser.add_argument("--warmup", type=int, default=10)
    parser.add_argument("--output", type=str, default="")
    args = parser.parse_args()

    print("=== OLAP Agent TPC-H Benchmark ===")
    print(f"  Workers:  {args.workers}")
    print(f"  Duration: {args.duration}s (warmup {args.warmup}s)")
    print()

    supervisor = TpchSupervisorAgent()
    metrics = MetricsCollector()
    stop = threading.Event()
    warmup_until = time.monotonic() + args.warmup

    threads = []
    bench_start = time.time()
    for i in range(args.workers):
        t = threading.Thread(target=worker, args=(i, supervisor, metrics, stop, warmup_until), daemon=True)
        t.start()
        threads.append(t)

    try:
        time.sleep(args.warmup + args.duration)
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        stop.set()

    for t in threads:
        t.join(timeout=120)

    elapsed = time.time() - bench_start - args.warmup
    elapsed = max(elapsed, 0.1)

    summary = metrics.summary()
    if "error" in summary:
        print(f"\n  No queries completed. Check LLM connectivity.")
    else:
        summary["elapsed_s"] = round(elapsed, 1)
        summary["workers"] = args.workers
        qpm = summary["total_success"] / (elapsed / 60) if elapsed > 0 else 0
        summary["throughput_queries_per_min"] = round(qpm, 1)
        print_report(summary, elapsed, args.workers)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        print(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
