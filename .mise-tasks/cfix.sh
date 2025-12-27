#!/usr/bin/env bash
#MISE description="Fix things with codex"
uv run pytest
if [ $? -ne 0 ]; then
  echo "Error: pytest failed or encountered an error, running codex" >&2
  codex exec \
  --model gpt-5.2-codex \
  --sandbox danger-full-access \
  --config model_reasoning_effort="high" \
  --config model_verbosity="medium" \
  "`uv run pytest` fix bug"
else
  echo "Success: All tests passed"
fi

uvx ruff check **.py
if [ $? -ne 0 ]; then
  echo "Error: ruff failed or encountered an error, running codex" >&2
  codex exec \
  --model gpt-5.2-codex \
  --sandbox danger-full-access \
  --config model_reasoning_effort="high" \
  --config model_verbosity="medium" \
  "`uvx ruff check **.py` fix linting issues"
else
  echo "Success: All tests passed"
fi

pnpm --dir job_board_application exec tsc -p . -noEmit --pretty false
if [ $? -ne 0 ]; then
  echo "Error: TypeScript type check failed or encountered an error, running codex" >&2
  codex exec \
  --model gpt-5.2-codex \
  --sandbox danger-full-access \
  --config model_reasoning_effort="high" \
  --config model_verbosity="medium" \
  "`pnpm --dir job_board_application exec tsc -p . -noEmit --pretty false` fix type errors"
else
  echo "Success: All type checks passed"
fi
