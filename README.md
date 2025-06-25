# CRO Revenue Intelligence – README

Welcome to the **Enterprise CRO Analytics System**, an AI‑powered platform that lets Chief Revenue Officers ask plain‑English questions and receive data‑driven answers, complete with visualisations and citations.

---

## ✨ Key Features

* **Natural‑language Q\&A** over revenue data
* **Tri‑store data model** (DuckDB + Neo4j + Chroma) for speed, relationships, and semantic fallback
* **Auto‑discovery ingestion**—just drop your CSVs, restart, and query
* **Streaming API** for near‑instant feedback in the UI
* **Executive formatting** with optional charts & confidence indicators

---

## 🚀 Quick Start (Local Dev)

```bash
# 1. Clone
$ git clone https://github.com/your‑org/cro‑analytics.git && cd cro‑analytics

# 2. Back‑end
$ python -m venv .venv && source .venv/bin/activate
$ pip install -r requirements.txt
$ cp .env.example .env            # add your OpenAI key etc.
$ python src/api_server.py        # default port 8083

# 3. Front‑end (optional)
$ cd frontend
$ npm install && npm run dev      # http://localhost:3000
```

\### Prerequisites

| Tool                  | Version               |
| --------------------- | --------------------- |
| Python                | ≥ 3.12                |
| Node / npm            | ≥ 20                  |
| DuckDB                | bundled as Python lib |
| Neo4j Desktop or Aura | 5.x                   |

> **Docker‑first?** A `docker-compose.yml` spins up API, Neo4j, and Chroma with one command: `docker compose up`.

---

## 🗂️ Project Structure

```
.
├─ src/
│   ├─ ingestion/
│   │   ├─ data_ingestion.py
│   │   └─ schema_discovery.py
│   ├─ engines/
│   │   ├─ query_engine.py      # core planner/retriever/reasoner
│   │   └─ cro_query_engine.py  # executive formatter & viz helper
│   ├─ api_server.py            # FastAPI gateway
│   └─ components/              # visualisers, pydantic models, utils
├─ data/                        # drop CSVs here
├─ frontend/                    # React/Next.js UI
└─ docs/
    └─ query_engine.md          # in‑depth engine architecture
```

---

## ⚙️ Configuration

| Variable                                    | Purpose                |
| ------------------------------------------- | ---------------------- |
| `OPENAI_API_KEY`                            | LLM access             |
| `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` | Graph store            |
| `CHROMA_PATH`                               | Vector DB path         |
| `DATA_PATH`                                 | Folder containing CSVs |

All variables live in `.env` and are loaded by `python‑dotenv`.

---

## 🧪 Testing

```bash
$ pytest tests/    # unit tests for ingestion & query planner
$ python tests/test_queries.py smoke   # end‑to‑end Q&A suite
```

---

## 🛠️ Extending

* **New data source?** Drop the file in `data/` and restart; the ingestion script rebuilds DuckDB tables, graph nodes, and embeddings.
* **Swap storage** layers (e.g., Snowflake for DuckDB) by editing `config.yaml`—the LLM prompt remains unchanged.
* **Custom tools** can be added to `ToolExecutor`; register in the Planner node to enable auto‑selection.

