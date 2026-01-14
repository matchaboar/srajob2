# Archived Temporal Mock Tests

This directory contains tests that used Temporal workflow testing infrastructure.

## Archived Files

| File | Description |
|------|-------------|
| `test_firecrawl_mock_temporal.py` | Tests for Firecrawl mocking in Temporal workflows |
| `test_firecrawl_webhook_http.py` | Tests for Firecrawl webhook HTTP handling |

## Why Archived?

These tests use Temporal's `WorkflowEnvironment` and `Worker` classes to test webhook workflows. Since the application now uses DBOS for workflow orchestration, these tests are no longer relevant.

## Archive Date

Archived: January 2026
