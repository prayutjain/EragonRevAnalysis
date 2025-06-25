# CRO Analysis

## 1. Purpose & Scope

This brief focuses on the **backend intelligence**—from raw CSVs to an answer that sounds board‑room ready.  We purposely hide implementation minutiae so a technical stakeholder can appreciate *why* the design matters, not just *how* it is coded.

## 2. Data Ingestion & Context Construction

The engine starts with an **auto‑discovery pipeline**  that loads any unfamiliar dataset and prepares three complimentary stores:

| Store         | What It Holds                                                                                                                                 | Why It Helps CRO Queries                                                                                                                                |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **DuckDB**    | Clean tabular copies of each CSV                                                                                                              | Fast OLAP aggregations; keeps token count low by letting SQL do the heavy lifting instead of the LLM                                                    |
| **Neo4j**     | A knowledge graph built from detected keys *plus* 50+ business‑specific edges (stage progression, deal similarity, stakeholder overlap, etc.) | Gives the LLM **spatial awareness**—it can jump between related nodes instead of scanning every row, shrinking prompt size and surfacing richer context |
| **Chroma DB** | Sentence‑level embeddings of each row (plus metadata)                                                                                         | Semantic fallback when column names are fuzzy; vectors act as a “catch‑all” without bloating graph size                                                 |

\### Why this tri‑store mix matters

* **Token reduction:** Instead of pasting raw rows into the prompt, we feed the LLM *pointers*—table/column names, relationship paths, and a handful of exemplar values—keeping context windows lean.
* **Contextual depth:** Graph edges encode intent (e.g., `(:Opportunity)-[:PROGRESSED_TO]->(:Opportunity)`), letting the engine reason about sequences, clusters, and peer groups without extra natural‑language explanation.
* **Data‑agnostic:** The discovery step cleans column names, infers keys, and exports a simplified JSON schema (`llm_schema.json`).  Swap in a new CSV tomorrow and the engine self‑re‑maps—no manual schema prompt edits.

> **Net effect:** Better answers, faster, with fewer OpenAI tokens—while remaining future‑proof when the sales‑ops team ships a new dataset.

\## 3. Query Engine Architecture & Flow
\### 3.1 High‑Level Diagram

```
User Question  ─▶  Planner  ─▶  Retriever  ─▶  Reasoner  ─▶  Reflector*  ─▶  CRO Formatter
                       │            │            │            │
                       ▼            ▼            ▼            ▼
                  SQL Tool     Graph Tool   Vector Tool   Forecast Tool*
                       │            │            │            │
                       └──── Tool Executor & Cache ──────────┘
```

*Planner chooses the lightest tool first (SQL) and escalates only if needed, keeping latency and token usage down.*

\### 3.2 Happy‑Path Walkthrough

1. **Input:** *“What are the top 5 accounts by total opportunity value?”*
2. **Planner** reads the JSON schema → selects an aggregation SQL query.
3. **Retriever** runs DuckDB; only the result rows (≈ 100 bytes) hit the LLM.
4. **Reasoner** drafts an answer, citing `accounts_by_value.sql`.
5. **Reflector** sees high confidence → ends loop.
6. **Formatter** rewrites for executives and attaches an optional bar chart suggestion.

> **Swap‑ability:** Because reasoning is decoupled from storage, DuckDB can be replaced by Snowflake, or Chroma by Pinecone, with *zero* prompt changes.
