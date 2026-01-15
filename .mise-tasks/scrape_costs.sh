#!/usr/bin/env bash
#MISE description="Summarize scrape costs per company for the last hour (prod)"
uv run agent_scripts/monitoring/summarize_scrape_costs.py --env prod --lookback-minutes 60
