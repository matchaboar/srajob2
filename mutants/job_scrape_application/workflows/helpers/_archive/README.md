# Archived Workflow Helpers

These helper files were archived during the DBOS migration (Phase 6 cleanup).

## Archived Files

| File | Description |
|------|-------------|
| `workflow_debug.py` | Temporal workflow checkpoint utilities |
| `workflow_logging.py` | Temporal workflow-aware logging utilities |

## Why Archived

These helpers were designed specifically for Temporal workflow contexts.
They use `temporalio.workflow` module APIs that are not available in DBOS.
The DBOS runtime uses standard Python logging instead.

## Restoring

If you need to restore any of these files:
```bash
mv _archive/filename.py ./
```
