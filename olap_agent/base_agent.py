"""Base Agent — 自包含的 ReAct 循环引擎，不依赖 mlflow / databricks。

核心流程:
  1. 用 system prompt + 用户消息调用 LLM
  2. 若 LLM 返回 tool_calls → 执行工具 → 结果追加到消息 → 继续循环
  3. 若 LLM 返回文本 → 结束循环，返回最终回复
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional

import backoff
import yaml
from openai import OpenAI

from olap_agent.tools import TOOL_IMPL, TOOL_SPECS


# ------------------------------------------------------------------
# Config loading
# ------------------------------------------------------------------

@dataclass
class AgentConfig:
    name: str
    description: str
    system_prompt: str
    llm_endpoint: str = ""
    temperature: float = 0.01
    max_tokens: int = 4096
    tool_names: list[str] = field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: str | Path) -> AgentConfig:
        with open(path) as f:
            d = yaml.safe_load(f)
        llm = d.get("llm", {})
        params = llm.get("params", {})
        return cls(
            name=d["name"],
            description=d.get("description", ""),
            system_prompt=d.get("system_prompt", ""),
            llm_endpoint=llm.get("endpoint", ""),
            temperature=params.get("temperature", 0.01),
            max_tokens=params.get("max_tokens", 4096),
            tool_names=d.get("tools", []),
        )


def _find_config_dir() -> Path:
    here = Path(__file__).resolve().parent
    candidates = [
        here.parent / "configs" / "agents",
        here / "configs" / "agents",
        Path.cwd() / "configs" / "agents",
    ]
    for c in candidates:
        if c.is_dir():
            return c
    raise FileNotFoundError(f"Cannot find configs/agents/ directory, tried: {candidates}")


# ------------------------------------------------------------------
# Base Agent
# ------------------------------------------------------------------

@dataclass
class ToolCallRecord:
    name: str
    arguments: dict
    result: str
    elapsed_ms: float


@dataclass
class AgentResult:
    text: str
    tool_calls: list[ToolCallRecord] = field(default_factory=list)
    llm_rounds: int = 0
    total_elapsed_s: float = 0.0


class BaseAgent:
    """LLM + Tools ReAct agent."""

    def __init__(
        self,
        agent_type: str,
        config_dir: Optional[str | Path] = None,
        tool_specs: Optional[list[dict]] = None,
    ):
        cfg_dir = Path(config_dir) if config_dir else _find_config_dir()
        cfg_path = cfg_dir / f"{agent_type}.yaml"
        if not cfg_path.exists():
            raise FileNotFoundError(f"Agent config not found: {cfg_path}")
        self.config = AgentConfig.from_yaml(cfg_path)
        self.agent_type = agent_type

        model = os.environ.get("OPENAI_MODEL") or self.config.llm_endpoint
        self.model = model

        self.client = OpenAI(
            api_key=os.environ.get("OPENAI_API_KEY", "dummy"),
            base_url=os.environ.get("OPENAI_BASE_URL"),
        )

        if tool_specs is not None:
            self.tool_specs = tool_specs
        elif self.config.tool_names:
            self.tool_specs = [
                s for s in TOOL_SPECS
                if s["function"]["name"] in self.config.tool_names
            ]
        else:
            self.tool_specs = []

    @backoff.on_exception(
        backoff.expo, Exception, max_tries=6, max_time=120,
        giveup=lambda e: "401" in str(e),
    )
    def _call_llm(self, messages: list[dict]) -> dict:
        params: dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
        }
        if self.tool_specs:
            params["tools"] = self.tool_specs
        resp = self.client.chat.completions.create(**params)
        msg = resp.choices[0].message
        out: dict[str, Any] = {
            "role": "assistant",
            "content": msg.content or "",
        }
        if msg.tool_calls:
            out["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in msg.tool_calls
            ]
        return out

    def _execute_tool(self, name: str, args: dict) -> str:
        fn = TOOL_IMPL.get(name)
        if fn is None:
            return json.dumps({"error": f"Unknown tool: {name}"})
        try:
            return fn(**args)
        except Exception as e:
            return json.dumps({"error": str(e)})

    def run(
        self,
        user_message: str,
        max_iter: int = 15,
        on_event: Optional[Callable[[str, Any], None]] = None,
    ) -> AgentResult:
        """Run the ReAct loop and return the final result.

        Args:
            on_event: Optional callback ``(event_type, data)`` fired on each
                      significant step.  Event types:
                      - "llm_start"   (round_number)
                      - "llm_output"  (llm_message_dict)
                      - "tool_start"  ({"name": ..., "arguments": ...})
                      - "tool_result" (ToolCallRecord)
                      - "done"        (AgentResult)
        """
        _emit = on_event or (lambda *_: None)

        t0 = time.perf_counter()
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": self.config.system_prompt},
            {"role": "user", "content": user_message},
        ]
        tool_records: list[ToolCallRecord] = []
        llm_rounds = 0

        for _ in range(max_iter):
            last = messages[-1]

            if tcs := last.get("tool_calls"):
                for tc in tcs:
                    fn = tc["function"]
                    args = json.loads(fn["arguments"])
                    _emit("tool_start", {"name": fn["name"], "arguments": args})
                    t1 = time.perf_counter()
                    result_str = self._execute_tool(fn["name"], args)
                    rec = ToolCallRecord(
                        name=fn["name"],
                        arguments=args,
                        result=result_str,
                        elapsed_ms=round((time.perf_counter() - t1) * 1000, 1),
                    )
                    tool_records.append(rec)
                    _emit("tool_result", rec)
                    messages.append({
                        "role": "tool",
                        "content": result_str,
                        "tool_call_id": tc["id"],
                    })

            elif last["role"] == "assistant" and "tool_calls" not in last:
                break
            else:
                llm_rounds += 1
                _emit("llm_start", llm_rounds)
                llm_out = self._call_llm(messages)
                _emit("llm_output", llm_out)
                messages.append(llm_out)
                if "tool_calls" not in llm_out and llm_out.get("content"):
                    break

        final_text = ""
        for m in reversed(messages):
            if m["role"] == "assistant" and m.get("content"):
                final_text = m["content"]
                break

        return AgentResult(
            text=final_text,
            tool_calls=tool_records,
            llm_rounds=llm_rounds,
            total_elapsed_s=round(time.perf_counter() - t0, 3),
        )
