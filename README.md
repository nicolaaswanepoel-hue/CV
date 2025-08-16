# Nico’s Weather Lab — Forecast Accuracy

A small data pipeline + interactive dashboard that tracks how weather forecasts compare to reality.  
Built for fun, learning, and to give reviewers something to say “oooh shiny” about.

---

## 🚀 Live Dashboard

👉 **[Open Dashboard](https://nicolaaswanepoel-hue.github.io/CV/)**

---

## 🔗 Data Flow

```mermaid
graph LR
  API((Weather API)) --> D(Docker)
  D --> A(Airflow)
  A --> P[(Postgres)]
  P --> G[CSV Export]
  G --> W[GitHub Pages Dashboard]
If Mermaid doesn’t render:

Weather API → Docker → Airflow → Postgres → CSV Export → GitHub Pages

📂 Repo Structure
graphql

CV/
├─ airflow/                 # Airflow project root
│  └─ dags/                 # DAGs (ingest, compute, export)
├─ docker/                  # Docker Compose and images
│  ├─ compose.core.yml
│  └─ images/
├─ docs/                    # GitHub Pages site (served from /docs)
│  ├─ index.html            # Mini dashboard (Chart.js + PapaParse)
│  └─ data/
│     └─ metrics_latest.csv # Latest exported metrics (daily)
├─ sql/                     # (Optional) Useful queries/snippets
└─ README.md

⚙️ How It Works
Collect — Airflow DAGs fetch weather forecasts and observations into Postgres.

Compute — For each day & forecast horizon, compute:

MAE: Average Miss (typical error)
RMSE: Big Miss Score (penalizes large errors)
Bias: Forecast Lean (over/under prediction)
Export — Write a CSV to docs/data/metrics_latest.csv.
Visualize — Static dashboard (/docs/index.html) reads the CSV and renders interactive charts (Chart.js).

Zero backend. Zero hosting cost. Always a shareable link.

🛠️ Stack
Docker for local infra
Airflow for orchestration
Postgres for storage
Python / Pandas / Meteostat for data work
Chart.js + PapaParse for the dashboard
GitHub Pages for hosting

🧪 Local Dev (Quick Start)

# 1) Bring up core stack
docker compose -f docker/compose.core.yml up -d

# 2) Access Airflow UI
# http://localhost:8080  (default: admin / admin)

# 3) After DAGs run, latest metrics CSV appears at:
# docs/data/metrics_latest.csv

# 4) Commit CSV so Pages can serve it
git add docs/data/metrics_latest.csv
git commit -m "Update metrics CSV"
git push

📝 Notes
Negative horizons in early runs may indicate demo/backfill mode while data accumulates.

The dashboard auto cache-busts the CSV with a ?v= query to avoid stale data.

Future ideas:
Multiple cities
Auto git push from Airflow with a token
Superset/Metabase for power users