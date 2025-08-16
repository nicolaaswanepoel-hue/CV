<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Nico’s Weather Lab — Forecast Accuracy</title>
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
  <style>
    :root { --bg:#0c1224; --ink:#e9edff; --muted:#9aa7ff; --card:#121a39; --border:#263169; --accent:#42a5ff; }
    *{box-sizing:border-box} html,body{margin:0;font:16px/1.5 ui-sans-serif,system-ui,Segoe UI,Roboto,Ubuntu;background:var(--bg);color:var(--ink)}
    .wrap{max-width:1100px;margin:42px auto;padding:0 16px}
    header{text-align:center;margin-bottom:30px}
    header h1{font-size:34px;margin:0;background:linear-gradient(90deg,#7a88ff,#42a5ff);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
    header p{color:var(--muted);font-size:18px;margin:8px 0 0}
    .card{background:linear-gradient(180deg,#111836,#0b1230);border:1px solid var(--border);border-radius:18px;padding:20px;box-shadow:0 10px 30px rgba(0,0,0,.35);margin-bottom:28px}
    .controls{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px}
    select,button{background:#0e1532;color:var(--ink);border:1px solid var(--border);border-radius:12px;padding:9px 11px;transition:all .2s}
    select:hover,button:hover{border-color:var(--accent);color:var(--accent);cursor:pointer}
    .kpis{display:flex;gap:10px;flex-wrap:wrap;margin:8px 0 12px}
    .kpi{flex:1;min-width:150px;background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px;transition:transform .2s}
    .kpi:hover{transform:translateY(-4px)}
    .kpi .label{color:#8fa0ff;font-size:12px;text-transform:uppercase;letter-spacing:.4px}
    .kpi .value{font-size:22px;margin-top:6px}
    canvas{width:100%!important;height:420px!important}
    footer{color:var(--muted);margin-top:10px;font-size:12px;text-align:center}
    .diagram{background:#0b1230;border:1px solid var(--border);border-radius:14px;padding:20px;margin-top:24px}
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <h1>🌦️ Nico’s Weather Lab</h1>
      <p>Tracking how forecasts measure up — made interactive & friendly for everyone</p>
    </header>

    <div class="card">
      <div class="controls">
        <label>Variable:
          <select id="varSel">
            <option value="temperature_2m">Temperature (°C)</option>
            <option value="precipitation">Precipitation (mm)</option>
            <option value="wind_speed_10m">Wind speed (m/s)</option>
          </select>
        </label>
        <button id="toggleMetric">Metric: Average Miss (MAE)</button>
      </div>

      <div class="kpis">
        <div class="kpi"><div class="label">Average Miss (MAE)</div><div class="value" id="kpi-mae">—</div></div>
        <div class="kpi"><div class="label">Big Miss Score (RMSE)</div><div class="value" id="kpi-rmse">—</div></div>
        <div class="kpi"><div class="label">Forecast Lean (Bias)</div><div class="value" id="kpi-bias">—</div></div>
      </div>

      <canvas id="chart"></canvas>
      <footer id="foot">Loading…</footer>
    </div>

    <div class="diagram card">
      <h2 style="margin-top:0">🔗 Data Flow</h2>
      <div class="mermaid">
        graph LR
          API((Weather API)) --> D(Docker)
          D --> A(Airflow)
          A --> P[(Postgres DB)]
          P --> G[GitHub CSV Export]
          G --> W[Weather Dashboard 🌐]
      </div>
    </div>
  </div>

  <script>
    mermaid.initialize({ startOnLoad: true, theme: "dark" });

    const csvUrl = "./data/metrics_latest.csv?v=" + Date.now(); // cache-bust
    let rows = [];
    let metricMode = "mae"; // mae -> rmse -> bias

    const numFmt = (v, d=1) => (v==null || Number.isNaN(+v)) ? "—" : (+v).toFixed(d);

    function summarize(varName){
      const s = rows.filter(r=>r.var===varName);
      const avg = k => s.length ? s.reduce((a,x)=>a + (+x[k]),0)/s.length : null;
      return { mae: avg("mae"), rmse: avg("rmse"), bias: avg("bias") };
    }

    function rebuildChart(varName){
      const data = rows.filter(r=>r.var===varName).sort((a,b)=>(+a.horizon_hours)-(+b.horizon_hours));
      const labels = data.map(r=>r.horizon_hours);
      const series = data.map(r=>+r[metricMode]);
      if (window.ct) window.ct.destroy();
      const ctx = document.getElementById("chart");
      window.ct = new Chart(ctx, {
        type:"line",
        data:{ labels, datasets:[{ label: metricMode==="mae"?"Average Miss (MAE)":metricMode==="rmse"?"Big Miss Score (RMSE)":"Forecast Lean (Bias)", data: series, borderColor:"#42a5ff", backgroundColor:"rgba(66,165,255,0.2)", borderWidth:2, tension:.25, pointRadius:3 }]},
        options:{ responsive:true, plugins:{ legend:{display:true}, tooltip:{callbacks:{label:(c)=>`${c.dataset.label}: ${numFmt(c.parsed.y)}`}} },
                  scales:{ x:{title:{display:true,text:"Hours Ahead (h)"}}, y:{title:{display:true,text:"Error (units vary)"}} } }
      });
    }

    function updateKpis(varName){
      const s = summarize(varName);
      document.getElementById("kpi-mae").textContent  = numFmt(s.mae);
      document.getElementById("kpi-rmse").textContent = numFmt(s.rmse);
      document.getElementById("kpi-bias").textContent = numFmt(s.bias);
    }

    function setMeta(){
      if (!rows.length) return;
      const day = rows[0].day, city = rows[0].city;
      document.getElementById("meta")?.textContent = ` — ${city}, latest day: ${day}`;
      document.getElementById("foot").textContent = `Data: metrics_latest.csv · Negative horizons indicate demo/backfill mode`;
    }

    function initUI(){
      const varSel = document.getElementById("varSel");
      varSel.addEventListener("change", ()=>{ rebuildChart(varSel.value); updateKpis(varSel.value); });
      document.getElementById("toggleMetric").addEventListener("click",(e)=>{
        metricMode = metricMode==="mae" ? "rmse" : metricMode==="rmse" ? "bias" : "mae";
        e.target.textContent = "Metric: " + (metricMode==="mae"?"Average Miss (MAE)":metricMode==="rmse"?"Big Miss Score (RMSE)":"Forecast Lean (Bias)");
        rebuildChart(varSel.value);
      });
    }

    Papa.parse(csvUrl, {
      download:true, header:true, dynamicTyping:true,
      complete: (res)=>{ rows = res.data.filter(r=>r.day); setMeta(); initUI(); const v=document.getElementById("varSel").value; rebuildChart(v); updateKpis(v); },
      error: (err)=>{ document.getElementById("foot").textContent = "Failed to load CSV: " + err; }
    });
  </script>
</body>
</html>
