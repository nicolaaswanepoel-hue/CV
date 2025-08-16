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
