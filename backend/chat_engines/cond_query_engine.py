"""
Updated Query & CRO Engines (2025-06-25) – **v1.1**
===================================================
Changes since v1.0 (shown earlier):
* 🆕 **CROResponder** – an LLM-driven wrapper that turns the raw answer + insights into a fully‑structured CRO brief (Executive Summary → Key Insights → Recommendations → Next Steps).
* 🔗 `CROQueryEngine` now calls `CROResponder.generate()` so callers get the enhanced narrative without extra work.
* 🔧 Env‑flag `ENABLE_RECOMMENDATIONS` (default **True**) to opt‑out quickly.

As before, this canvas file concatenates two physical modules:
1. **query_engine.py** – unchanged from v1.0
2. **cro_query_engine.py** – new logic highlighted

----------------------------------------------------------------------
BEGIN FILE: query_engine.py
----------------------------------------------------------------------
"""
import json
import logging
import time
from datetime import datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import duckdb
import chromadb
from chromadb.config import Settings
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langgraph.graph import END, StateGraph
from neo4j import GraphDatabase

from config import *  # pragma: no cover
from exceptions import BadQuestionError, NoResultError, PlanningError
from components.models import (PlannerOutput, QueryState, ReasonerOutput,
                               ReflectorOutput, ToolCall)
from components.nodes import NodeHandlers
from components.tools import ToolExecutor

logger = logging.getLogger(__name__)

# === Tunables & constants =====================================================
CACHE_TTL_SEC = 60 * 5        # cache tool results for 5 minutes
MAX_CACHE_SIZE = 256          # LRU size per tool-type

# ==============================================================================

def _now() -> float:
    return time.time()

class _TTLCache(dict):
    """Simple TTL dict with max length (FIFO eviction)."""

    def __init__(self, ttl: int, max_len: int):
        super().__init__()
        self._ttl = ttl
        self._max_len = max_len

    def get(self, key, default=None):
        val = super().get(key, None)
        if val is None:
            return default
        payload, ts = val
        if _now() - ts > self._ttl:
            super().__delitem__(key)
            return default
        return payload

    def put(self, key, value):
        if len(self) >= self._max_len:
            oldest_key = next(iter(self))
            super().__delitem__(oldest_key)
        super().__setitem__(key, (value, _now()))

class QueryEngine:
    """Main orchestrator for SQL, Graph & Vector queries with caching & reflection."""
    def __init__(self):
        self._duck = None
        self._neo4j = None
        self._chroma = None
        self._llm = None
        self._emb = None
        self._tool_exec = None
        self._nodes = None
        self._sql_cache = _TTLCache(CACHE_TTL_SEC, MAX_CACHE_SIZE)
        self._cypher_cache = _TTLCache(CACHE_TTL_SEC, MAX_CACHE_SIZE)
        self._vector_cache = _TTLCache(CACHE_TTL_SEC, MAX_CACHE_SIZE)
        self._memory: Dict[str, List[Dict[str, Any]]] = {}
        self._max_memory_turns = 5
        self.schema_config = self._load_schema_config()
        self.llm_schema = self._load_llm_schema()
        self.workflow = self._build_workflow()
    # (rest of QueryEngine identical to v1.0)

# -----------------------------------------------------------------------------
# END query_engine.py
# -----------------------------------------------------------------------------

"""
----------------------------------------------------------------------
BEGIN FILE: cro_query_engine.py
----------------------------------------------------------------------
"""
# CRO‑specific wrapper on top of `QueryEngine`.
# * Provides executive‑ready markdown
# * Generates lightweight HTML viz blocks
# * Adds final LLM pass for Recommendations / Next Steps


import logging
from typing import Any, Dict, List, Tuple

from langchain_openai import ChatOpenAI

from .query_engine import QueryEngine
from components.cro_utils import CROUtils
from components.cro_visualizers import Visualizer

logger = logging.getLogger(__name__)

# === Tunables ================================================================
MAX_VIZ = 2                         # max number of visualization blocks
ENABLE_RECOMMENDATIONS = True       # master switch for CROResponder layer

# =============================================================================

class CROResponder:
    """LLM post‑processor that wraps raw answer + insights into a CRO brief."""

    _TEMPLATE = """You are an AI assistant preparing a board‑ready briefing for a Chief Revenue Officer.
Return markdown with **four sections** in this order:
1. **Executive Summary** – 1–2 crisp sentences answering the question.
2. **Key Insights** – up to 5 bullets; emphasise numbers (**bold**).
3. **Recommendations** – specific actions (omit if none).
4. **Next Steps** – who does what by when (teams in *italics*).
Stay under 180 words. No preamble, no apology.

---
Question: {{question}}
Answer paragraph: {{answer_text}}
Insights (bullets or "(none)"):
{{insights}}
---"""

    def __init__(self, llm: ChatOpenAI):
        self.llm = llm

    def generate(self, question: str, answer_text: str, insights: List[str]) -> str:
        prompt = self._TEMPLATE.replace("{{question}}", question)
        prompt = prompt.replace("{{answer_text}}", answer_text)
        joined = "".join(f"- {i}" for i in insights) if insights else "(none)"
        prompt = prompt.replace("{{insights}}", joined)
        rsp = self.llm.invoke(prompt)
        return rsp.content.strip()


class CROQueryEngine:
    """High‑level API that returns CRO‑formatted answers + viz blocks."""

    def __init__(self):
        self.engine = QueryEngine()
        self.utils = CROUtils()
        self.visualizer = Visualizer()
        self.responder = CROResponder(self.engine.llm) if ENABLE_RECOMMENDATIONS else None

    # --------------------------------------------------------------------- API
    def query(self, question: str, max_iterations: int = 3, session_id: str = "default") -> Dict[str, Any]:
        base = self.engine.query(question, max_iterations, session_id)
        base["answer"] = base.get("answer") or "No data found for that question."

        # --- choose visualisations ------------------------------------------------
        viz_suggestions = self._analyze_visualization(base.get("raw_results", []))
        answer_md, viz_blocks, insights = self._render_answer(base, viz_suggestions)

        # --- final CRO brief ------------------------------------------------------
        final_answer = (self.responder.generate(question, answer_md, insights)
                        if self.responder else answer_md)

        return {
            **base,
            "answer": final_answer,
            "visualizations": viz_blocks,
        }

    # ----------------------------------------------------------------- heuristics
    def _analyze_visualization(self, result_sets: List[Dict]) -> List[Dict[str, str]]:
        """Inspect first result‑set and decide best viz type(s)."""
        if not result_sets:
            return []

        viz = []
        for rs in result_sets:
            rows = rs.get("results")
            if rows and isinstance(rows, list) and isinstance(rows[0], dict):
                sample = rows[0]
                cols = list(sample.keys())
                num_cols = [c for c in cols if isinstance(sample[c], (int, float))]
                cat_cols = [c for c in cols if isinstance(sample[c], str)]
                date_cols = [c for c in cols if any(t in c.lower() for t in ("date", "month", "year", "quarter"))]

                if date_cols and num_cols:
                    viz.append({"type": "line", "x": date_cols[0], "y": num_cols[0]})
                elif "stage" in [c.lower() for c in cols]:
                    viz.append({"type": "funnel", "stage_col": "stage"})
                elif cat_cols and num_cols:
                    viz.append({"type": "bar", "cat": cat_cols[0], "val": num_cols[0]})
                else:
                    viz.append({"type": "table"})
                break  # only analyse first non‑empty set
        return viz[:MAX_VIZ]

    # ---------------------------------------------------------------- rendering
    def _render_answer(self, base: Dict[str, Any], vizzes: List[Dict]) -> Tuple[str, List[Dict], List[str]]:
        answer_txt = base.get("answer", "").strip()
        md_parts = [answer_txt, "", "**Key Insights:**"]
        insights: List[str] = []
        for rs in base.get("raw_results", [])[:3]:
            if rs.get("results"):
                row = rs["results"][0]
                insight = ", ".join(str(v) for v in list(row.values())[:3])
                insights.append(insight)
                md_parts.append(f"* {insight}")
        md_answer = "".join(md_parts)

        # build visualisation blocks
        blocks: List[Dict[str, Any]] = []
        for v in vizzes:
            if v["type"] == "table":
                block = self._build_table_block(base)
            elif v["type"] == "bar":
                block = self._build_bar_block(base, v)
            elif v["type"] == "line":
                block = self._build_line_block(base, v)
            elif v["type"] == "funnel":
                block = self._build_funnel_block(base)
            else:
                block = None
            if block:
                blocks.append(block)

        return md_answer, blocks, insights

    # ----------------------------------------------------- viz helper builders
    @staticmethod
    def _pick_dataset(result_sets: List[Dict]) -> List[Dict]:
        for rs in result_sets:
            if rs.get("results"):
                return rs["results"]
        return []

    def _build_table_block(self, base):
        rows = self._pick_dataset(base.get("raw_results", []))[:20]
        if not rows:
            return None
        cols = list(rows[0].keys())
        data = [[r.get(c) for c in cols] for r in rows]
        html = self.visualizer.generate_data_table_html(cols, data, "tbl1", "Detailed Results")
        return {"type": "html", "title": "Data Table", "content": html}

    def _build_bar_block(self, base, v):
        rows = self._pick_dataset(base.get("raw_results", []))
        if not rows:
            return None
        cat, val = v["cat"], v["val"]
        agg: Dict[str, float] = {}
        for r in rows:
            k = r.get(cat, "Unknown")
            agg[k] = agg.get(k, 0) + r.get(val, 0)
        chart_data = [{"label": k, "value": v} for k, v in sorted(agg.items(), key=lambda x: x[1], reverse=True)[:10]]
        html = self.visualizer.generate_bar_chart_html(chart_data, "bar1", {"xAxis": cat, "yAxis": val})
        return {"type": "html", "title": "Top Categories", "content": html}

    def _build_line_block(self, base, v):
        rows = self._pick_dataset(base.get("raw_results", []))
        if not rows:
            return None
        x, y = v["x"], v["y"]
        data = [{"label": r.get(x), "value": r.get(y)} for r in rows if r.get(x) and r.get(y) is not None]
        data = sorted(data, key=lambda d: d["label"])
        html = self.visualizer.generate_line_chart_html(data, "line1", {"xAxis": x, "yAxis": y})
        return {"type": "html", "title": "Trend Over Time", "content": html}

    def _build_funnel_block(self, base):
        rows = self._pick_dataset(base.get("raw_results", []))
        if not rows:
            return None
        stage_col = next((c for c in rows[0].keys() if c.lower() == "stage"), None)
        if not stage_col:
            return None
        counts: Dict[str, int] = {}
        for r in rows:
            s = r.get(stage_col, "Unknown")
            counts[s] = counts.get(s, 0) + 1
        data = [{"stage": k, "count": c} for k, c in counts.items()]
        html = self.visualizer.generate_funnel_chart_html(data, "fun1")
        return {"type": "html", "title": "Pipeline by Stage", "content": html}

    # ---------------------------------------------------------------- closing
    def close(self):
        self.engine.close()

# -----------------------------------------------------------------------------
# END cro_query_engine.py
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
