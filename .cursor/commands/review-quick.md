# Quick Review — Fast Pre-Push Check

Perform a fast self-review of the current branch. Report in French.
This is the lightweight version — no tests, no deep analysis.

## Steps

1. Run `just review-quick` in the terminal and capture the output.

2. Run `git diff master...HEAD --stat` and `git diff master...HEAD --shortstat` to get the diff summary.

3. Scan the diff for obvious problems:
   - Secrets (passwords, API keys, tokens)
   - `print()` statements in non-test files
   - `SELECT *` in SQL files
   - `TODO` / `FIXME` without issue reference
   - `.env` file in the diff (should be `.env.example` only)

4. Check that all commits follow conventional format: `type(scope): description`

5. Output a concise report in French:
   - Branch name and commit count
   - Lines changed (with warning if > 500 or > 1500)
   - Issues found (if any)
   - Quick verdict: OK / NEEDS ATTENTION
