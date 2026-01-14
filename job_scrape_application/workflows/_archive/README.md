# Archived Temporal Workflows

This directory contains Temporal workflow code that was previously used but has been replaced by DBOS workflows.

## Why Archived?

The application migrated from Temporal to DBOS for workflow orchestration. DBOS provides:
- Simpler deployment (no separate Temporal server needed)
- SQLite-based queue persistence
- Easier local development and testing

## Archived Files

| Original Name | Archived Name | Description |
|--------------|---------------|-------------|
| `worker.py` | `temporal_worker.py` | Temporal worker process that executed activities |
| `scrape_workflow.py` | `temporal_scrape_workflow.py` | Main scraping workflow for Temporal |
| `webhook_workflow.py` | `temporal_webhook_workflow.py` | Webhook ingestion workflow for Firecrawl callbacks |
| `create_schedule.py` | `temporal_create_schedule.py` | Temporal schedule creation utilities |
| `trigger_schedule.py` | `temporal_trigger_schedule.py` | Manual schedule trigger utilities |
| `greenhouse_workflow.py` | `temporal_greenhouse_workflow.py` | Greenhouse-specific scraping workflow |
| `heuristic_workflow.py` | `temporal_heuristic_workflow.py` | Heuristic processing workflow |

## Current Implementation

Active workflow code is now in:
- `job_scrape_application/dbos_runtime/` - DBOS workflow runner and queue management
- `job_scrape_application/workflows/activities/` - Activity functions (shared between old Temporal and new DBOS)

## Restoration

If you need to restore Temporal functionality:
1. Move files back to parent directory (remove `temporal_` prefix)
2. Update imports in tests
3. Install and configure Temporal server
4. Update `job_scrape_application/workflows/__init__.py` to export workflow classes

## Archive Date

Archived: January 2026
