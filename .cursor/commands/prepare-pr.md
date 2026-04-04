# Prepare PR — Generate Pull Request Description

Analyze the current branch and generate a complete PR description following the team standards.
Output everything in English (code/PR conventions), but explain in French.

## Steps

1. Determine the base branch (default: `master`). Run:
   - `git branch --show-current` to get the current branch name
   - `git log master..HEAD --oneline` to list all commits
   - `git diff master...HEAD --stat` to see changed files
   - `git diff master...HEAD --shortstat` to get the line count

2. Analyze the commits and changed files to understand:
   - What functional unit this PR represents
   - Which pipeline layers are affected (Bronze/Silver/Gold)
   - Which DAGs are modified
   - Which dbt models are added/changed
   - Whether Docker/infra files are touched

3. Generate the PR description using the template from `.cursor/rules/pull-request-standards.mdc`:

```markdown
## Summary
[1-3 sentences: what this PR does and why]

## Pipeline Impact
- Layers affected: [staging / core / mart / orchestration]
- New/modified models: [list of dbt models]
- DAG changes: [yes/no — describe if yes]

## Data Flow
[ASCII diagram or description of the data path affected]

## Review Strategy
Suggested review order (follow commit layering):
1. Start with: [first layer to review]
2. Then: [second layer]
3. Finally: [last layer]

## Testing
- [ ] dbt build passes
- [ ] Quality checks pass (GX + Soda)
- [ ] DAG parses without error
- [ ] Docker images build if infra changed
- [ ] Unit tests pass (`just test`)

## Breaking Changes
[None / describe impact and migration steps]
```

4. Also suggest:
   - Whether the PR should be split (if > 1500 lines changed)
   - The ideal merge strategy (merge commit vs squash)
   - Any reviewers who should be tagged based on file ownership

5. Output the generated PR description as a ready-to-paste Markdown block, preceded by an analysis summary in French.
