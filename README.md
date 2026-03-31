# Capacity — Structural Health Monitor

Real-time structural capacity assessment of the financial system.

Based on Flow Theory by J.H.F. Festirstein.

## What it measures

Not where financial conditions are — but whether the system is gaining or losing the ability to absorb shocks.

## Engine

`capacity_engine.py` runs 4x daily via GitHub Actions, pulling data from FRED and market feeds.

## Site

Static site served via Netlify. Updates automatically when the engine commits new data.
