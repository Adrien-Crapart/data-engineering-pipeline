Perform a fast self-review of the current branch. This is the lightweight version — no tests, no deep analysis.

## Steps

1. Run `just review-quick` and capture the output.

2. Run `git diff master...HEAD --stat` and `git diff master...HEAD --shortstat` to get the diff summary.

3. Scan the diff for obvious problems:
   - Secrets (passwords, API keys, tokens)
   - `print()` statements in non-test files
   - `SELECT *` in SQL files
   - `TODO`/`FIXME` without issue reference
   - `.env` file in the diff (should be `.env.example` only)
   - `:latest` Docker tags

4. Check that all commits follow conventional format: `type(scope): description`

## Output

A concise report with:
- Branch name and commit count
- Lines changed (warn if > 500, error if > 1500)
- Issues found (if any)
- Quick verdict: **OK** / **NEEDS ATTENTION**
