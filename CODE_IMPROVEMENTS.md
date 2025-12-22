# Code Improvements

- [x] Introduce class-based payload row collection strategies to DRY normalization inputs.
  - [x] Add reusable collector interface + concrete Firecrawl/Fetchfox implementations.
  - [x] Route normalize_* helpers through the collector strategy.
- [x] Centralize markdown cleaning + hint application into a reusable normalizer pipeline.
  - [x] Create a hint applier/normalizer class to remove duplicated logic.
  - [x] Update normalization and job-building paths to use the shared pipeline.
- [x] Expand unit tests to cover new classes/strategies and regression cases.
  - [x] Add tests for the new collector/normalizer behavior.
  - [x] Fix any existing tests affected by refactors.
- [x] Run lint + targeted tests; fix any failures.
  - [x] Run `uvx ruff check`.
  - [x] Run pytest for scrape_utils-related tests.
