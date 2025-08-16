# Nico’s Weather Lab — Forecast Accuracy

A small data pipeline + interactive dashboard that tracks how weather forecasts compare to reality.  
Built for a skillset showcase for my resume.

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
