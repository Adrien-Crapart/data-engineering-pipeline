Analyze the current branch and generate a complete pull request description following project standards.

## Steps

1. Determine base branch (default: `master`). Run:
   - `git branch --show-current`
   - `git log master..HEAD --oneline`
   - `git diff master...HEAD --stat`
   - `git diff master...HEAD --shortstat`

2. Analyze commits and changed files to understand:
   - What functional unit this PR represents
   - Which pipeline layers are affected (Bronze/Silver/Gold)
   - Which DAGs are modified
   - Which dbt models are added/changed
   - Whether Docker/infra files are touched

3. Generate the PR description using this template:

```markdown
## Summary
[1-3 sentences: what this PR does and why]

## Pipeline Impact
- Layers affected: [staging / core / mart / orchestration]
- New/modified models: [list]
- DAG changes: [yes/no — describe if yes]

## Data Flow
[ASCII diagram or description of the affected data path]

## Review Strategy
Suggested review order (follow commit layering):
1. Start with: [first layer]
2. Then: [second layer]
3. Finally: [last layer]

## Testing
- [ ] dbt build passes (`just dbt-run`)
- [ ] Quality checks pass (GX + Soda)
- [ ] DAG parses without error
- [ ] Docker images build if infra changed (`just build-all`)
- [ ] Unit tests pass (`just test`)

## Breaking Changes
None / [describe impact and migration steps]
```

4. Also indicate:
   - Whether the PR should be split (if > 1500 lines changed, split by pipeline layer)
   - Ideal merge strategy: merge commit (preserve layering) vs squash (single trivial commit)

5. Output the PR description as a ready-to-paste Markdown block.
