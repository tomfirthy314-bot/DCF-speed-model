#!/bin/bash
# DCF Tool — launch the Streamlit front end
cd "$(dirname "$0")"
echo "Starting DCF Valuation Tool..."
streamlit run app.py --server.headless false --browser.gatherUsageStats false
