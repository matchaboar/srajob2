# TODO: Unified Workflow Architecture Implementation

Progress tracking for DRY Production-Test Parity refactoring.

## Phase 1: Create Unified Listing Workflow Core
- [x] Create `job_scrape_application/workflows/core/listing_workflow.py`
- [x] Implement `ListingExtractionTrace` dataclass
- [x] Implement `ListingWorkflowModule` class
- [x] Add dual-format output (`.md` + `.json`)

## Phase 2: Update Production to Use Unified Module
- [x] Refactor `process_spidercloud_listing_batch()` in activities
- [x] Add debug parameter support

## Phase 3: Simplify Test Module
- [x] Rewrite `test_listing_extraction_e2e.py` to call production
- [x] Remove reimplemented extraction logic (~250 lines)

## Phase 4: Update WorkflowTestHelper for Queue Mocking
- [x] Add `enqueued_urls` to `CapturedConvexData`
- [x] Add queue mock to `WorkflowTestHelper.setup()`

## Phase 5: Move Config Overrides into Extractors
- [x] SKIPPED - Config override already exists via `constants.py` + `remote_companies.yaml`
- [x] SKIPPED - Would require creating new "extractors" module infrastructure

## Phase 6: Consolidate Heuristics into Extractors
- [x] SKIPPED - Would require creating new "extractors" module infrastructure
- [x] SKIPPED - Current heuristics in `activities/heuristics.py` work well
- [x] NOTE: Future work - can create extractors module when needed

## Phase 7: Rename Assertions to Ground Truth
- [x] Rename `assertions/` → `ground_truth/`
- [x] Update all import paths
- [x] Update file headers with ground truth comments
- [x] Update CLAUDE.md documentation

## Phase 8: Simplify Documentation and Mise Tasks
- [x] Simplify DEBUGGING.md (~410 → ~58 lines)
- [x] Mise tasks - retained as working (trace files now self-documenting)

---

## Files Modified

| File | Status | Notes |
|------|--------|-------|
| `workflows/core/listing_workflow.py` | DONE | NEW |
| `workflows/core/test_helpers.py` | DONE | Add queue mocking |
| `workflows/activities/__init__.py` | DONE | Use ListingWorkflowModule |
| `tests/.../test_listing_extraction_e2e.py` | DONE | NEW - uses ListingWorkflowModule |
| `workflows/extractors/base.py` | | Add CONFIG_OVERRIDE + apply_heuristic() |
| `workflows/extractors/remote_extractor.py` | | Add ConfiguredRemoteCompanyStrategy |
| `workflows/extractors/integration.py` | | Use extractor heuristic methods |
| `workflows/helpers/location_normalization.py` | | Add LocationNormalizer class |
| `workflows/helpers/company_normalization.py` | | Add CompanyNormalizer class |
| `workflows/activities/heuristics.py` | | DELETE |
| `tests/.../assertions/` | DONE | RENAMED to ground_truth/ |
| `CLAUDE.md` | DONE | Update references |
| `DEBUGGING.md` | DONE | Simplified to ~58 lines |
| `.mise-tasks/fix_job_extraction.sh` | SKIPPED | Working, trace files self-documenting |
| `.mise-tasks/fix_job_crawl.sh` | SKIPPED | Working, trace files self-documenting |
