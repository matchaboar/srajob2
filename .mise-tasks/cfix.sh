#!/usr/bin/env bash
#MISE description="Fix things with codex"
run_check() {
  local label="$1"
  local fix_hint="$2"
  local display_cmd="$3"
  shift 3

  echo "----- $label: [${display_cmd}] -----"
  local output
  output=$("$@" 2>&1)
  local status=$?

  if [ $status -ne 0 ]; then
    echo "Error: $label failed or encountered an error, running codex" >&2
    codex exec \
      --model gpt-5.2-codex \
      --sandbox danger-full-access \
      --config model_reasoning_effort="high" \
      --config model_verbosity="medium" \
      "$output $fix_hint"
  else
    echo "Success: $label passed"
  fi
}

run_check "pytest" "fix bug" "[uv run pytest]" uv run pytest
run_check "ruff" "fix linting issues" "[uvx ruff check **.py]" uvx ruff check **.py
run_check "TypeScript type check" "fix type errors" "[pnpm --dir job_board_application exec tsc -p . -noEmit --pretty false]" pnpm --dir job_board_application exec tsc -p . -noEmit --pretty false
run_check "CONVEX DB TypeScript type check" "fix type errors" "[pnpm --dir job_board_application exec tsc -p convex -noEmit --pretty false]" pnpm --dir job_board_application exec tsc -p convex -noEmit --pretty false
