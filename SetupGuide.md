# Setup Guide

This guide walks you through getting the **Enterprise CRO Analytics System** running on your laptop, a Docker host, or the cloud. It covers both **backend** services and the **React/Next.js** frontend.

---

## 1 Prerequisites

| Tool                    | Version | Notes                                 |
| ----------------------- | ------- | ------------------------------------- |
| Python                  | ≥ 3.12  | Use `pyenv` or system package manager |
| Node.js + npm           | ≥ 20    | Needed only for the frontend          |
| Docker & Docker Compose | Latest  | Optional but recommended              |
| Neo4j                   | 5.x     | Desktop, Aura, or Docker image        |
| Git                     | any     | Clone the repo                        |

> **Tip (quickest path)** – If you have Docker, skip to § 4 (Docker Compose). It spins up everything—including Neo4j and Chroma—in one command.

---

## 2 Local Development (no Docker)

\### 2.1 Clone & Create Virtual Env

```bash
$ git clone https://github.com/your-org/cro-analytics.git && cd cro-analytics
$ python -m venv .venv && source .venv/bin/activate
```

\### 2.2 Install Python Deps

```bash
(.venv) $ pip install -r requirements.txt
```

\### 2.3 Set Environment Variables

```bash
$ cp .env.example .env            # then edit with your keys
```

| Key                             | Description                              |
| ------------------------------- | ---------------------------------------- |
| `OPENAI_API_KEY`                | ChatCompletion access                    |
| `NEO4J_URI`                     | e.g. `bolt://localhost:7687`             |
| `NEO4J_USER` / `NEO4J_PASSWORD` | Neo4j creds                              |
| `CHROMA_PATH`                   | Folder where the vector DB will live     |
| `DATA_PATH`                     | Path to your raw CSVs (default `./data`) |

\### 2.4 Start Databases

```bash
# Neo4j Desktop UI OR
$ neo4j start   # if installed via package

# Chroma runs in‑process; no daemon needed
```

\### 2.5 Ingest Data
The first boot will auto‑run ingestion. To trigger manually:

```bash
(.venv) $ python src/ingestion/data_ingestion.py --fresh
```

You should now have DuckDB tables, a populated Neo4j graph, and Chroma embeddings.

\### 2.6 Run the API

```bash
(.venv) $ python src/api_server.py    # default http://localhost:8083
```

`/docs` (FastAPI Swagger) should list endpoints like `/qa` and `/qa/stream`.

\### 2.7 Start the Frontend

```bash
$ cd frontend
$ npm install            # first time only
$ cp .env.local.example .env.local   # set NEXT_PUBLIC_API_URL=http://localhost:8083
$ npm run dev            # http://localhost:3000
```

Ask the chatbot: *“What are the top 5 accounts by total opportunity value?”*

---

## 3 Unit & Integration Tests

```bash
(.venv) $ pytest tests/            # ingestion + engine units
(.venv) $ python tests/test_queries.py smoke   # end‑to‑end Q&A
```

---

## 4 Docker Compose (One‑liner)

```bash
$ docker compose up --build
```

*Services spun up:* `api` (FastAPI), `neo4j`, `chroma`, and `frontend` (Next.js). Visit `http://localhost:8083/docs` and `http://localhost:3000`.

\### 4.1 Customising

* Override variables in `.env.docker`
* Mount a host `./data` folder via volume to ingest new CSVs automatically

---

## 5 Production Deployment Sketch

| Layer    | Option                    | Notes                                         |
| -------- | ------------------------- | --------------------------------------------- |
| API      | AWS ECS / GKE             | Container image `cro-api:<tag>`               |
| Graph    | Neo4j Aura                | Scales to billions of nodes                   |
| Vectors  | Chroma Server or Pinecone | Pinecone drop‑in via config.yaml              |
| Frontend | Vercel / CloudFront       | Build output `npm run build && npm run start` |
| CI/CD    | GitHub Actions            | Lint → Tests → Build → Push → Deploy          |

*Encryption, secret management, and autoscaling policies should be set per your org’s standards.*

---

## 6 Updating Data

1. Drop new CSV(s) into `data/`.
2. Restart the API or run `python src/ingestion/data_ingestion.py --fresh`.
3. The auto‑discovery logic rebuilds DuckDB tables, re‑syncs Neo4j, and re‑embeds Chroma.

No schema hand‑holding required.

---

## 7 Troubleshooting

| Symptom                       | Likely Cause                | Fix                                |
| ----------------------------- | --------------------------- | ---------------------------------- |
| `bolt://… connection refused` | Neo4j not running           | `neo4j start` or check Docker logs |
| `OpenAIAuthenticationError`   | Wrong API key               | Re‑export `OPENAI_API_KEY`         |
| Queries return **empty**      | Ingestion skipped a file    | Check `DATA_PATH`, rerun ingestion |
| Frontend CORS errors          | API not on `localhost:8083` | Update `NEXT_PUBLIC_API_URL`       |

---

## 8 Next Steps

* Configure SSL & SSO for enterprise roll‑out.
* Swap DuckDB for Snowflake if datasets exceed local disk.
* Enable GPU embedding generation via `--device cuda` flag in `data_ingestion.py`.

Happy querying! 🎉
