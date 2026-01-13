# Automated Debug Workflow for Job Extraction Issues

This document describes the automated workflow for debugging production job extraction issues using the `mise run fix_job_extraction` command.

## Overview

The workflow automates the tedious parts of debugging job extraction issues while leaving the critical thinking to Claude Code.

**What's Automated:**
- ✅ Fetching job data from Convex prod
- ✅ Downloading SpiderCloud fixture
- ✅ Creating directory structure
- ✅ Generating placeholder assertion file
- ✅ Launching Claude Code with context

**What Claude Does:**
- 📝 Fill in assertion values (location, level, description requirements)
- 🔍 Run debug test to identify issues
- 🔧 Fix the site handler
- ✅ Verify the fix works

## Usage

```bash
# Run with job share URL
mise run fix_job_extraction https://srajob.netlify.app/job/k57abc123xyz

# Or with Convex job ID directly
mise run fix_job_extraction k57abc123xyz
```

## What Happens Automatically

### 1. Job Data Fetch
```bash
# Fetches from Convex prod
npx convex run --prod jobs:getJobById '{"id":"k57abc123xyz"}'
```

Extracts and displays:
- Title, Company, Location
- Remote status, Level
- Description length
- Original job URL

### 2. Site Name Detection
Automatically extracts site name from URL:
- `explore.jobs.netflix.net` → `netflix`
- `company.greenhouse.io` → `company`
- `company.ashbyhq.com` → `company`
- `careers.company.com` → `company`

Creates identifier: `{site}_{job_id}`

### 3. Fixture Generation
```bash
PYTHONPATH=. uv run python agent_scripts/dump_spidercloud_response.py \
  "{JOB_URL}" \
  --out tests/job_scrape_application/workflows/fixtures/debug/{identifier}_detail.json \
  --use-handler-config
```

Saves SpiderCloud response to debug folder.

### 4. Assertion File Creation
Creates placeholder at:
```
tests/job_scrape_application/workflows/assertions/debug/{identifier}.yml
```

With template:
```yaml
site_id: {site}
detail_url: {job_url}
expected:
  # TODO: Fill in values by examining fixture
  title: "{current_title}"
  company: "{current_company}"
  location_contains: "FILL_THIS"  # <-- Claude fills this
  is_remote: {current_remote}
  level: mid  # <-- Claude adjusts this
  description_min_words: 300  # <-- Claude adjusts this
  description_not_contains: '{"'  # <-- Claude adds more patterns
  cost_milli_cents_min: 1
  posted_at_not_null: true
```

### 5. Claude Code Launch
Opens Claude Code with structured prompt containing:
- Job details and current extraction
- File paths (fixture, assertions)
- Step-by-step instructions
- Example commands to run
- Reference to debugging docs

## Claude's Workflow

When Claude Code opens, it should:

### 1. Examine the Fixture
```bash
# View fixture structure
python -c "
import json
with open('{fixture_path}', 'r') as f:
    data = json.load(f)
    item = data[0][0]
    md = item['content']['commonmark']
    lines = md.split('\n')
    for i, line in enumerate(lines[:30]):
        print(f'{i:3d}: {line[:100]}')
"
```

### 2. Fill in Assertions
Edit the assertion file with correct values:
- Extract actual location from job posting
- Determine correct level based on title/description
- Set minimum word count (usually 300+)
- Add patterns that should NOT appear (JSON blocks, metadata)

### 3. Run Debug Test
```bash
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py::test_debug_job_extraction[{identifier}] -v
```

### 4. Analyze Extraction Output
```bash
cat ./site-detail-e2e-examples/{site}_extraction.json
```

Look for issues:
- Description word count too low?
- JSON blocks or metadata in description?
- Wrong location parsing?
- Missing or incorrect fields?

### 5. Fix the Handler
Edit `job_scrape_application/workflows/site_handlers/{site}.py`

Common fixes:
- Implement `normalize_markdown()` to clean description
- Add `extract_location_hint()` for location parsing
- Update regex patterns

### 6. Verify Fix
```bash
# Re-run debug test
uv run pytest tests/job_scrape_application/workflows/test_debug_fixtures.py -v

# Ensure main test still passes
uv run pytest "tests/job_scrape_application/workflows/test_job_detail_extraction_e2e.py::test_job_detail_extraction_accuracy[{site}]" -v
```

## Benefits

### Time Savings
- **Before**: 10-15 minutes of manual setup
- **After**: 30 seconds automated + Claude work

### Reduced Errors
- Consistent naming conventions
- No typos in file paths
- Correct fixture format

### Better Context
Claude Code receives:
- Full job data
- Pre-generated files
- Clear instructions
- Example commands

### Preserved Examples
Debug fixtures remain for:
- Future reference
- Regression testing
- Documentation

## File Structure

After running the command:

```
tests/job_scrape_application/workflows/
├── fixtures/
│   └── debug/
│       ├── README.md
│       └── {site}_{job_id}_detail.json  ← Fixture
└── assertions/
    └── debug/
        └── {site}_{job_id}.yml          ← Assertions (Claude fills TODOs)

./site-detail-e2e-examples/
└── {site}_extraction.json               ← Test output
```

## Example: Netflix Fix

### Command
```bash
mise run fix_job_extraction https://srajob.netlify.app/job/k1719yxs9nmtvbsye23v9jt0ys7z5trb
```

### Automated Output
```
✓ Fixture saved to: tests/.../fixtures/debug/netflix_k1719yxs9nmtvbsye23v9jt0ys7z5trb_detail.json
✓ Placeholder assertion created: tests/.../assertions/debug/netflix_k1719yxs9nmtvbsye23v9jt0ys7z5trb.yml
```

### Claude's Work
1. Examined fixture, found JSON blocks in markdown
2. Filled in assertions with correct location, level
3. Ran test → Failed with 122 words (expected 300+)
4. Implemented `normalize_markdown()` in Netflix handler
5. Re-ran test → Passed with 1010 words
6. Verified main test still passes

### Result
- Before: 122 words (JSON blocks)
- After: 1010 words (clean content)
- Time: ~10 minutes total

## Troubleshooting

### Fixture Fetch Fails
If SpiderCloud fetch fails:
- Check job URL is valid
- Job may have been removed
- Try fetching manually

### Site Name Detection Wrong
Edit the assertion file's `site_id` field manually.

### Claude Code Doesn't Launch
Run the prompt manually:
```bash
claude "$(cat /tmp/fix_job_prompt.txt)"
```

## Future Improvements

Potential enhancements:
- Auto-detect common issues (JSON blocks, missing fields)
- Suggest handler fixes based on patterns
- Auto-fill more assertion fields
- Integration with CI/CD for regression testing

## See Also

- `tests/job_scrape_application/workflows/fixtures/debug/README.md` - Full debug workflow docs
- `CLAUDE.md` - Main development guide
- `DEBUGGING.md` - Advanced debugging techniques
