```
USER INPUT: "Company name or ticker"
        │
        ▼
┌───────────────────────────────────┐
│  1   SCRAPE                       │  data_engine.py
│      Yahoo → EDGAR → Macrotrends  │
│      → Companies House            │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  1.5  DATA QA                     │  data_qa.py
│       Cross-source reconciliation │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  2   STANDARDISE               ⛔ │  standardiser.py
│      Raw fields → canonical names │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  2.5  CANONICAL FUNDAMENTALS      │  canonical_fundamentals.py
│       Derive EBIT / FCF / EBITDA  │
│       from primitives (3 methods) │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  2.6  HIGH-RISK FIELDS            │  high_risk_fields.py
│       GP · Net Debt · Shares      │
│       Tax Rate · NWC · EV multiples│
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  3   VALIDATE                  ⛔ │  validator.py
│      19 check categories          │
│      Quality score 0–100          │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  4   NORMALISE                 ⛔ │  normaliser.py
│      CAGRs · margins · ratios     │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  5   CLASSIFY                  ⛔ │  classifier.py
│      asset_light · industrial     │
│      consumer · resources · etc.  │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  5.5  ANOMALY DETECTOR            │  anomaly_detector.py
│       70+ rules                   │
│       ACCT SIGN SCAL HIST PATT DCF│
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  5.6  FMP CROSS-CHECK             │  fmp.py
│       (only if API key set)       │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  6   ASSUME                    ⛔ │  assumption_engine.py
│      WACC · tax rate · terminal   │
│      growth · forecast horizon    │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  7   FORECAST                  ⛔ │  forecaster.py
│      Revenue · margins · capex    │
│      → projected FCF per year     │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  8   VALUE                     ⛔ │  valuation_engine.py
│      Discount FCF → EV → equity   │
│      Bear │ Base │ Bull scenarios  │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  9   COHERENCE                    │  coherence_engine.py
│      Reality-check the valuation  │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  9.5  REMEDIATE                   │  remediation_engine.py
│       Auto-correct flagged issues │
└───────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────┐
│  10  EXPLAIN                      │  explainer.py
│      Confidence · narrative       │
│      Review agenda                │
└───────────────────────────────────┘
        │
        ├────────────────────────────────────────────┐
        │                                            │
        ▼                                            ▼
┌─────────────────────────┐            ┌─────────────────────────┐
│  10.5  COMPARABLES      │            │  OUTPUT                  │
│  peer_selector.py       │            │                          │
│  comparables.py         │            │  excel_model.py → .xlsx  │
│                         │            │  app.py → Streamlit UI   │
│  5 peers · multiples    │            │                          │
│  → implied VPS range    │            └─────────────────────────┘
└─────────────────────────┘


  ⛔ = pipeline halts if this stage fails
```
