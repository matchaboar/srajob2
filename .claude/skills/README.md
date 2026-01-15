# Claude Skills for Job Scraper

This directory contains Claude Code skills that provide automated workflows for common job scraper development tasks.

## Available Skills

### 1. fix-job-extraction
Debug and fix job detail extraction issues for production jobs.

**Usage:** `/fix-job-extraction <job-url-or-id>`

**What it does:**
- Fetches job from Convex prod
- Downloads SpiderCloud fixture
- Creates assertion file with expected values
- Provides debugging commands

**Example:**
```bash
/fix-job-extraction https://srajob.netlify.app/job/k57abc123xyz
```

---

### 2. fix-job-crawl
Debug and fix job listing page URL extraction issues.

**Usage:** `/fix-job-crawl <company-name-or-url>`

**What it does:**
- Fetches listing page from SpiderCloud
- Extracts URLs using production handler
- Creates assertion file with extracted URLs
- Provides validation commands

**Example:**
```bash
/fix-job-crawl airbnb
/fix-job-crawl https://api.greenhouse.io/v1/boards/airbnb/jobs
```

---

### 3. add-site
Add a new company career site to the scraper.

**Usage:** `/add-site <job-detail-url>`

**What it does:**
- Identifies company, handler, and platform
- Generates test fixtures
- Creates site schedule YAML entry
- Guides through setup process

**Example:**
```bash
/add-site https://careers.newcompany.com/jobs/12345
```

---

### 4. refresh-fixtures
Bulk regenerate test fixtures from site schedule.

**Usage:** `/refresh-fixtures [--only sites...] [--limit N]`

**What it does:**
- Loads sites from schedule
- Generates listing + detail fixtures for each
- Creates timestamped files
- Provides validation commands

**Example:**
```bash
/refresh-fixtures --schedule-env prod
/refresh-fixtures --schedule-env prod --only airbnb purestorage --limit 5
```

---

## How Skills Work

Skills are markdown files with:
1. **Frontmatter** - Defines skill name and description
2. **Documentation** - Explains what the skill does
3. **Examples** - Shows usage patterns
4. **Implementation** - Documents the underlying script invocation

When you invoke a skill (e.g., `/fix-job-extraction <args>`), Claude Code:
1. Reads the skill file to understand what to do
2. Executes the referenced Python script with appropriate arguments
3. Parses the JSON output from the script
4. Presents results and next steps to you

## Script Output Format

All core scripts support `--output-format json` to enable skill integration:

```json
{
  "fixture_path": "tests/.../fixture.json",
  "assertion_path": "tests/.../assertion.yml",
  "identifier": "test_identifier",
  "company": "Company Name",
  "handler": "greenhouse",
  ...
}
```

Skills parse this JSON to extract relevant information and provide context-aware guidance.

## Benefits of Skills

### vs Manual Workflow
- **Faster**: Single command instead of multiple steps
- **Consistent**: Always follows best practices
- **Context-aware**: Provides relevant commands and paths
- **Error-resistant**: Validates inputs and provides helpful messages

### vs Mise Tasks
- **Interactive**: Claude can interpret results and provide guidance
- **Flexible**: Can adapt workflow based on outputs
- **Educational**: Explains what's happening at each step
- **Integrated**: Works seamlessly with Claude Code's conversational interface

## Adding New Skills

To add a new skill:

1. Create `skill-name.md` in this directory
2. Add frontmatter with name and description
3. Document usage, implementation, and examples
4. Ensure the underlying script supports `--output-format json`
5. Add to `.claude/settings.local.json` permissions:
   ```json
   {
     "permissions": {
       "allow": [
         "Skill(skill-name)"
       ]
     }
   }
   ```

## Underlying Scripts

Skills invoke scripts in `agent_scripts/core/`:
- `generate_debug_fixture.py` - fix-job-extraction
- `generate_debug_listing_fixture.py` - fix-job-crawl
- `generate_new_site_fixture.py` - add-site
- `refresh_all_site_fixtures.py` - refresh-fixtures

See `agent_scripts/README.md` for detailed script documentation.
