# Archived Legacy Components

These files were archived during the DBOS migration (Phase 6 cleanup).

## Archived Files

| File | Description |
|------|-------------|
| `temporal_health_check.py` | Temporal server health check utilities |
| `temporal_real_server_check.py` | Real Temporal server connection checks |
| `test_workflow.py` | Test workflow for Temporal |
| `monitor.py` | Temporal workflow monitoring utilities |

## Why Archived

These components were designed for the Temporal workflow system which has been replaced by DBOS.
They are preserved here for reference but are no longer used in the active codebase.

## Restoring

If you need to restore any of these files for reference:
```bash
mv _archive/filename.py ./
```
