# Flowstate

Global capital flow intelligence engine.

Flowstate runs a daily macro state vector across 31 variables 
spanning energy markets, inflation, monetary capacity, credit 
stress, and sovereign debt flows. It produces a single daily 
verdict — Stable, Stressed, Critical, or Cascade — updated 
four times per day.

## Free Public API

No signup. No API key. Just call it.

    GET https://api.flowstate.io/v1/current
    GET https://api.flowstate.io/v1/history?days=30
    GET https://api.flowstate.io/v1/state

Full documentation at https://flowstate.io/api

## Current Reading

Updated automatically four times daily.
See flowstate.io for the live state vector.

## Data

All scan data publicly archived in /data/flow_data.csv
Updated with every engine run. Free to use.

## Framework

Flowstate measures five structural variables —
Flow, Capacity, Pressure, Drift, and Trust —
derived from cross-domain dynamical systems research
validated across astrophysics, financial markets,
and seismic dynamics.

## License

Data: CC0 — free to use for any purpose.
Code: MIT License.

## Status

Engine running since 2026-03-22
```

Commit directly to main.

---

## 2. Add to .gitignore

Click .gitignore, pencil edit, scroll to the very bottom, add these lines:
```
# Flowstate — never commit these
.env
config.py
secrets.py
twelvedata_cache.json
*.db
*.sqlite
*.sqlite3
engine.log
*.log
flow_data_latest.json
```

Commit directly to main.

---

## 3. Create the folder structure

In GitHub, create these folders by adding placeholder files. Click Add file → Create new file. Type the path including folder name:
```
data/.gitkeep
.github/workflows/.gitkeep
