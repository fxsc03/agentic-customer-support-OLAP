"""TPC-H Supervisor Agent — 路由分析问题到领域子 Agent。

架构:
  TpchSupervisorAgent (意图分类，无工具)
  ├── PricingRevenueAgent     → 定价/营收/折扣分析
  ├── ShippingLogisticsAgent  → 物流/配送/订单优先级
  ├── CustomerMarketAgent     → 客户/市场/订单行为
  └── SupplierPartAgent       → 供应商/零件/供应链
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from olap_agent.base_agent import AgentResult, BaseAgent


# ------------------------------------------------------------------
# Domain sub-agents (thin wrappers — config in YAML)
# ------------------------------------------------------------------

class PricingRevenueAgent(BaseAgent):
    def __init__(self, **kw):
        super().__init__(agent_type="pricing_revenue", **kw)


class ShippingLogisticsAgent(BaseAgent):
    def __init__(self, **kw):
        super().__init__(agent_type="shipping_logistics", **kw)


class CustomerMarketAgent(BaseAgent):
    def __init__(self, **kw):
        super().__init__(agent_type="customer_market", **kw)


class SupplierPartAgent(BaseAgent):
    def __init__(self, **kw):
        super().__init__(agent_type="supplier_part", **kw)


DOMAIN_AGENT_MAP = {
    "pricing_revenue": PricingRevenueAgent,
    "shipping_logistics": ShippingLogisticsAgent,
    "customer_market": CustomerMarketAgent,
    "supplier_part": SupplierPartAgent,
}

VALID_DOMAINS = list(DOMAIN_AGENT_MAP.keys())


# ------------------------------------------------------------------
# Supervisor result
# ------------------------------------------------------------------

@dataclass
class SupervisorResult:
    domain: str
    agent_result: AgentResult
    routing_elapsed_s: float = 0.0


# ------------------------------------------------------------------
# Supervisor
# ------------------------------------------------------------------

class TpchSupervisorAgent:
    """Routes analytical questions to the appropriate domain agent."""

    def __init__(self, config_dir: Optional[str] = None):
        self._router = BaseAgent(
            agent_type="supervisor_tpch",
            config_dir=config_dir,
            tool_specs=[],
        )
        self._sub_agents: dict[str, BaseAgent] = {}
        self._config_dir = config_dir

    def _get_sub_agent(self, domain: str) -> BaseAgent:
        if domain not in self._sub_agents:
            cls = DOMAIN_AGENT_MAP.get(domain)
            if cls is None:
                raise ValueError(f"Unknown domain: {domain}. Valid: {VALID_DOMAINS}")
            kw = {}
            if self._config_dir:
                kw["config_dir"] = self._config_dir
            self._sub_agents[domain] = cls(**kw)
        return self._sub_agents[domain]

    def route(self, query: str) -> str:
        """Classify query into a domain using the router LLM."""
        result = self._router.run(query, max_iter=1)
        raw = result.text.strip().lower().replace("-", "_").replace(" ", "_")
        for d in VALID_DOMAINS:
            if d in raw:
                return d
        return "pricing_revenue"

    def run(
        self,
        query: str,
        on_event: Optional[Callable[[str, Any], None]] = None,
    ) -> SupervisorResult:
        """Route the query and execute via the appropriate sub-agent."""
        t0 = time.perf_counter()
        domain = self.route(query)
        routing_elapsed = time.perf_counter() - t0

        sub_agent = self._get_sub_agent(domain)
        agent_result = sub_agent.run(query, on_event=on_event)

        return SupervisorResult(
            domain=domain,
            agent_result=agent_result,
            routing_elapsed_s=round(routing_elapsed, 3),
        )
