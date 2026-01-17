"""Pytest fixtures for DBOS workflow tests.

Fixtures are defined in the parent conftest.py and inherited automatically.
This module exists to allow workflow-specific fixtures to be added here.
"""

from __future__ import annotations

# Fixtures are inherited from parent conftest.py:
# - reset_dbos: Initializes DBOS with SQLite for testing
# - workflow_test: Provides WorkflowTest instance with DBOS initialized
