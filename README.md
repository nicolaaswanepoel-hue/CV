# 🌦️ Nico’s Weather Lab

**Forecast Accuracy Dashboard** — A small data pipeline + interactive dashboard that tracks how weather forecasts compare against reality.  
Built for fun, learning, and to give HR (and my living room) something to say *“oooh shiny”* about.

---

## 🚀 Live Dashboard

👉 [View the dashboard here](https://nicolaaswanepoel-hue.github.io/CV/)  

---

## 🔗 Data Flow

```mermaid
graph LR
  API((Weather API)) --> D(Docker)
  D --> A(Airflow)
  A --> P[(Postgres DB)]
  P --> G[GitHub CSV Export]
  G --> W[Weather Dashboard 🌐]

CV/
├── dags/                 # Airflow DAGs (compute + export jobs)
├── docker/               # Docker Compose setup
├── docs/                 # GitHub Pages site
│   ├── index.html        # Dashboard
│   └── data/metrics_latest.csv  # Latest exported metrics
└── README.md             # This file

⚙️ How It Works

Data Collection:
Forecast + actual weather data are pulled via API and stored in Postgres.

Compute Metrics:
Airflow DAG computes:

Average Miss (MAE) → typical error

Big Miss Score (RMSE) → punishes big mistakes

Forecast Lean (Bias) → systematic over/under prediction

Export:
Latest metrics written to docs/data/metrics_latest.csv.

Dashboard:
Static site (GitHub Pages) reads the CSV, renders interactive charts + KPIs with Chart.js.
No backend required — it’s all static & self-updating.

🛠️ Stack

Airflow (data pipelines)

Postgres (storage)

Docker (local infra)

GitHub Actions + Pages (deployment)

Chart.js + PapaParse + Mermaid (frontend visualization)

Built with ☕, 🐧, and curiosity.