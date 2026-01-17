# DBOS Workflows Plan

## Goal
Create proper DBOS workflows for the job scraping system, replacing the current imperative runner loops with declarative workflow-based orchestration using fine-grained steps for maximum retry granularity.

## Architecture Changes

### Current Architecture
- **runner.py**: Three async loops (schedule, listing queue, detail queue) that call activities directly
- **Steps**: `enqueue_listing_sites`, `load_schedule_interval_minutes`, `filter_new_job_urls`, etc.
- **Activities**: Large monolithic functions (`process_spidercloud_listing_batch` ~900 lines, `process_spidercloud_job_batch` ~500 lines)
- **Queue**: SQLite-based queue with listing and detail URLs

### Target Architecture
- **Workflows**: `@DBOS.workflow()` decorated async functions orchestrate steps
- **Fine-grained steps**: Each non-deterministic operation is a separate `@DBOS.step()` (10+ per workflow)
- **DBOS scheduled workflow**: Built-in scheduling instead of external loop
- **Result pattern**: Steps return `Success[T] | Failure` for non-retryable error handling

---

## Workflows

### 1. `scrape_listing_batch` Workflow
**File**: `job_scrape_application/workflows/workflow/scrape_listing_batch.py`

**Steps (12 total)**:
```
1.  parse_listing_batch_input       [deterministic - in workflow]
2.  record_scrape_url_attempts      [Convex mutation - step]
3.  scrape_listing_urls             [SpiderCloud HTTP - step]
4.  extract_job_urls_from_scrape    [deterministic - in workflow]
5.  validate_and_filter_job_urls    [deterministic - in workflow]
6.  resolve_pagination_limit        [Convex query - step]
7.  fetch_seen_urls_for_site        [Convex query - step]
8.  filter_new_job_urls             [Convex query - step] (existing)
9.  enqueue_detail_urls             [SQLite insert - step]
10. complete_listing_urls           [SQLite update - step]
11. emit_listing_telemetry          [PostHog HTTP - step]
```

**Result type**: `ListingScrapeResult(queued: int, completed: int, source_url: str)`

### 2. `scrape_job_detail_batch` Workflow
**File**: `job_scrape_application/workflows/workflow/scrape_job_detail_batch.py`

**Steps (10 total)**:
```
1.  parse_detail_batch_input        [deterministic - in workflow]
2.  record_scrape_url_attempts      [Convex mutation - step]
3.  filter_new_job_urls             [Convex query - step] (existing)
4.  complete_existing_urls          [SQLite update - step]
5.  scrape_job_details              [SpiderCloud HTTP - step]
6.  normalize_job_fields            [deterministic - in workflow]
7.  store_job_scrape                [Convex mutation - step] (per URL)
8.  complete_detail_url             [SQLite update - step] (per URL)
9.  handle_404_urls                 [SQLite update - step]
10. emit_detail_telemetry           [PostHog HTTP - step]
```

**Result type**: `DetailScrapeResult(stored: int, invalid: int, failed: int, source_url: str)`

### 3. `enqueue_scheduled_listings` Workflow (DBOS Scheduled)
**File**: `job_scrape_application/workflows/workflow/enqueue_scheduled_listings.py`

**Steps (8 total)**:
```
1.  load_schedule_interval_minutes  [Convex query - step] (existing)
2.  check_detail_queue_pending      [SQLite query - step]
3.  fetch_enabled_sites             [Convex query - step]
4.  generate_pagination_urls        [deterministic - in workflow]
5.  apply_pagination_limits         [deterministic - in workflow]
6.  enqueue_listing_urls            [SQLite insert - step]
7.  record_workflow_run             [SQLite insert - step]
8.  emit_schedule_telemetry         [PostHog HTTP - step]
```

**Result type**: `ScheduleResult(queued: int, sites_processed: int)`

**Scheduling**: Use `@DBOS.scheduled(cron="*/15 * * * *")` or interval-based scheduling

---

## New Steps to Create

### SpiderCloud Steps (`workflows/activities/step/`)
| Step | File | Purpose |
|------|------|---------|
| `scrape_listing_urls` | `scrape_listing_urls.py` | Call SpiderCloud API for listing pages |
| `scrape_job_details` | `scrape_job_details.py` | Call SpiderCloud API for job detail pages |

### Convex Steps (`workflows/activities/step/`)
| Step | File | Purpose |
|------|------|---------|
| `resolve_pagination_limit` | `resolve_pagination_limit.py` | Query site pagination config |
| `fetch_seen_urls_for_site` | `fetch_seen_urls_for_site.py` | Get already-seen URLs |
| `store_job_scrape` | `store_job_scrape.py` | Insert job to Convex (per URL) |
| `fetch_enabled_sites` | `fetch_enabled_sites.py` | List sites for scheduled scraping |

### SQLite Queue Steps (`dbos_runtime/step/`)
| Step | File | Purpose |
|------|------|---------|
| `check_detail_queue_pending` | `check_detail_queue_pending.py` | Check if detail queue has work |
| `enqueue_scrape_urls` | `enqueue_scrape_urls.py` | Insert URLs to queue |
| `complete_scrape_urls` | `complete_scrape_urls.py` | Mark URLs completed/failed |
| `record_workflow_run` | `record_workflow_run.py` | Record workflow execution metrics |

### Telemetry Steps (`workflows/activities/step/`)
| Step | File | Purpose |
|------|------|---------|
| `emit_scrape_telemetry` | `emit_scrape_telemetry.py` | Send events to PostHog |

---

## Result Pattern

All workflows return `Result[T]` type (from `workflows/result.py`):

```python
@dataclass
class Success(Generic[T]):
    value: T

@dataclass
class Failure:
    error_type: str
    message: str

Result = Success[T] | Failure
```

Steps that encounter non-retryable errors (validation failures, 404s, schema errors) return `Failure` instead of throwing. The workflow handles these gracefully.

---

## Runner Changes

Replace `runner.py` imperative loops with workflow invocations:

```python
# Before: Direct activity calls in loops
await handler({...})

# After: Workflow invocations
result = await scrape_listing_batch(batch)
match result:
    case Success(value=data):
        logger.info("Completed: %s", data)
    case Failure(error_type=et, message=msg):
        logger.error("[%s] %s", et, msg)
```

The main function will:
1. Initialize DBOS
2. Start the scheduled workflow for listing enqueue
3. Start queue consumer loops that invoke workflows (or use DBOS queues if available)

---

## Files to Create

```
job_scrape_application/workflows/workflow/
├── __init__.py
├── scrape_listing_batch.py
├── scrape_job_detail_batch.py
└── enqueue_scheduled_listings.py

job_scrape_application/workflows/activities/step/
├── scrape_listing_urls.py
├── scrape_job_details.py
├── resolve_pagination_limit.py
├── fetch_seen_urls_for_site.py
├── store_job_scrape.py
├── fetch_enabled_sites.py
└── emit_scrape_telemetry.py

job_scrape_application/dbos_runtime/step/
├── check_detail_queue_pending.py
├── enqueue_scrape_urls.py
├── complete_scrape_urls.py
└── record_workflow_run.py
```

## Files to Modify

- `job_scrape_application/dbos_runtime/runner.py` - Replace loops with workflow invocations
- `job_scrape_application/workflows/activities/step/__init__.py` - Export new steps
- `job_scrape_application/dbos_runtime/step/__init__.py` - Export new steps

---

## Implementation Order

1. **Phase 1**: Create `WorkflowTest` test utility module
2. **Phase 2**: Create step files (extract from activities)
3. **Phase 3**: Create workflow files (orchestrate steps)
4. **Phase 4**: Write tests for each workflow using `WorkflowTest`
5. **Phase 5**: Update runner.py to use workflows
6. **Phase 6**: Add DBOS scheduled workflow
7. **Phase 7**: Remove old activity code (gradual deprecation)

---

## Testing Infrastructure: `WorkflowTest` Module

**File**: `job_scrape_application/workflows/workflow/test_utils.py`

A reusable test harness that makes workflow tests simple and declarative. Builds on existing `WorkflowTestHelper` pattern but tailored for DBOS workflows.

### Design Goals
1. **Minimal boilerplate** - import workflow, configure fixtures, run test
2. **Sensible defaults** - auto-mock all steps with reasonable behavior
3. **Selective overrides** - override specific step mocks when testing edge cases
4. **Captured data** - easily inspect what steps were called with what args

### API Design

```python
from job_scrape_application.workflows.workflow.test_utils import WorkflowTest

@pytest.fixture
def workflow_test(tmp_path, monkeypatch):
    """Fixture that provides pre-configured WorkflowTest instance."""
    return WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)

async def test_listing_workflow_extracts_job_urls(workflow_test):
    # 1. Configure fixtures
    workflow_test.with_spidercloud_response(
        url="https://boards.greenhouse.io/company/jobs",
        response={"jobs": [{"url": "https://company.com/job/123"}]}
    )

    # 2. Import and run workflow
    from job_scrape_application.workflows.workflow import scrape_listing_batch

    result = await workflow_test.run(
        scrape_listing_batch,
        batch={"urls": [{"url": "https://boards.greenhouse.io/company/jobs"}]}
    )

    # 3. Assert on result
    assert isinstance(result, Success)
    assert result.value.queued == 1

    # 4. Inspect captured step calls
    assert len(workflow_test.step_calls["enqueue_detail_urls"]) == 1
    assert workflow_test.step_calls["enqueue_detail_urls"][0]["urls"] == ["https://company.com/job/123"]
```

### Key Features

#### Auto-mocking of all steps
```python
# Default behavior for each step type:
# - Convex queries: return empty list/dict
# - Convex mutations: return None, capture payload
# - SpiderCloud: match fixtures by URL
# - SQLite queue ops: capture calls, return success
# - Telemetry: no-op
```

#### Selective step overrides
```python
# Override a specific step to test error handling
workflow_test.mock_step(
    "filter_new_job_urls",
    side_effect=Exception("Convex timeout")
)

# Or return custom data
workflow_test.mock_step(
    "fetch_enabled_sites",
    return_value=[{"url": "https://example.com", "_id": "site1"}]
)
```

#### Step call inspection
```python
# Check what was called
assert workflow_test.step_calls["store_job_scrape"][0]["title"] == "Software Engineer"

# Check call count
assert workflow_test.call_count("complete_scrape_urls") == 2
```

#### Result pattern support
```python
# Test failure handling
workflow_test.mock_step(
    "scrape_job_details",
    return_value=Failure(error_type="rate_limited", message="Too many requests")
)

result = await workflow_test.run(scrape_job_detail_batch, batch={...})
assert isinstance(result, Failure)
assert result.error_type == "rate_limited"
```

### Implementation

```python
class WorkflowTest:
    """Test harness for DBOS workflows."""

    def __init__(self, tmp_path: Path, monkeypatch):
        self.tmp_path = tmp_path
        self.monkeypatch = monkeypatch
        self.step_calls: dict[str, list[dict]] = defaultdict(list)
        self._fixtures: dict[str, Any] = {}
        self._step_overrides: dict[str, Any] = {}

    def with_spidercloud_response(self, url: str, response: dict) -> "WorkflowTest":
        """Add a SpiderCloud fixture."""
        self._fixtures[url] = response
        return self

    def mock_step(self, step_name: str, return_value=None, side_effect=None) -> "WorkflowTest":
        """Override a step's behavior."""
        self._step_overrides[step_name] = {"return": return_value, "side_effect": side_effect}
        return self

    async def run(self, workflow_fn, **kwargs) -> Result:
        """Run a workflow with all mocks applied."""
        self._apply_mocks()
        return await workflow_fn(**kwargs)

    def _apply_mocks(self):
        """Apply all configured mocks."""
        # Set up DBOS environment
        self.monkeypatch.setenv("DBOS_SQLITE_PATH", str(self.tmp_path / "dbos.sqlite"))

        # Auto-mock all step modules
        for step_module in KNOWN_STEP_MODULES:
            self._mock_step_module(step_module)

    def call_count(self, step_name: str) -> int:
        """Get number of times a step was called."""
        return len(self.step_calls[step_name])
```

### Files to Create

```
job_scrape_application/workflows/workflow/
└── test_utils.py           # WorkflowTest class

tests/job_scrape_application/workflows/workflow/
├── conftest.py             # workflow_test fixture
├── test_scrape_listing_batch.py
├── test_scrape_job_detail_batch.py
└── test_enqueue_scheduled_listings.py
```

### Example Test File

```python
# tests/job_scrape_application/workflows/workflow/test_scrape_listing_batch.py

import pytest
from job_scrape_application.workflows.result import Success, Failure

@pytest.fixture
def workflow_test(tmp_path, monkeypatch):
    from job_scrape_application.workflows.workflow.test_utils import WorkflowTest
    return WorkflowTest(tmp_path=tmp_path, monkeypatch=monkeypatch)

class TestScrapeListingBatch:
    async def test_extracts_job_urls_from_greenhouse(self, workflow_test):
        workflow_test.with_spidercloud_response(
            url="https://boards.greenhouse.io/company",
            response={"jobs": [
                {"url": "https://boards.greenhouse.io/company/jobs/123"},
                {"url": "https://boards.greenhouse.io/company/jobs/456"},
            ]}
        )

        from job_scrape_application.workflows.workflow import scrape_listing_batch
        result = await workflow_test.run(
            scrape_listing_batch,
            batch={"urls": [{"url": "https://boards.greenhouse.io/company", "sourceUrl": "https://boards.greenhouse.io/company"}]}
        )

        assert isinstance(result, Success)
        assert result.value.queued == 2

    async def test_handles_empty_listing_page(self, workflow_test):
        workflow_test.with_spidercloud_response(
            url="https://boards.greenhouse.io/empty",
            response={"jobs": []}
        )

        from job_scrape_application.workflows.workflow import scrape_listing_batch
        result = await workflow_test.run(
            scrape_listing_batch,
            batch={"urls": [{"url": "https://boards.greenhouse.io/empty"}]}
        )

        assert isinstance(result, Success)
        assert result.value.queued == 0

    async def test_filters_existing_jobs(self, workflow_test):
        workflow_test.with_spidercloud_response(
            url="https://boards.greenhouse.io/company",
            response={"jobs": [{"url": "https://company.com/job/123"}]}
        )
        # All URLs already exist
        workflow_test.mock_step("filter_new_job_urls", return_value=[])

        from job_scrape_application.workflows.workflow import scrape_listing_batch
        result = await workflow_test.run(
            scrape_listing_batch,
            batch={"urls": [{"url": "https://boards.greenhouse.io/company"}]}
        )

        assert isinstance(result, Success)
        assert result.value.queued == 0
```

---

## Verification

1. `uvx ruff check .` - Lint
2. `uv run scripts/lint_dbos_step.py` - Verify @DBOS.step on Convex/HTTP calls
3. `uv run pytest tests/job_scrape_application/ -v` - Run existing tests
4. `uv run pytest tests/job_scrape_application/workflows/workflow/ -v` - Run new workflow tests
5. Manual: Start runner and verify listing→detail flow works end-to-end
