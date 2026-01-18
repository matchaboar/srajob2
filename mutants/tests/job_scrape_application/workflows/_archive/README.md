# Archived Temporal Workflow Tests

This directory contains tests for Temporal workflows that are no longer active.

## Archived Files

| File | Description |
|------|-------------|
| `test_spidercloud_workflow.py` | Tests for SpiderCloud scraping via Temporal |
| `test_process_webhook_listing_dedup.py` | Tests for webhook listing deduplication |
| `test_process_webhook_duplicates.py` | Tests for webhook duplicate handling |
| `test_heuristic_workflow_module.py` | Tests for heuristic processing workflow |

## Why Archived?

These tests use `temporalio.testing.WorkflowEnvironment` and test Temporal-specific workflow orchestration. Since the application now uses DBOS for workflow orchestration, these tests are no longer relevant.

## Current Tests

Active workflow tests are in the parent directory:
- `test_job_detail_extraction_e2e.py` - End-to-end extraction tests using DBOS
- `test_debug_fixtures.py` - Debug fixture testing
- `test_dependency_container.py` - Core dependency injection tests

## Archive Date

Archived: January 2026
