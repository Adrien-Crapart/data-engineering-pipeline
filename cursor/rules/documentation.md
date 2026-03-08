# Documentation Standards

## Language

- All documentation files (README.md, docstrings, comments) must be written in **English**.
- Git commit messages must be in English using conventional commit format.

## Folder Documentation

- **Every root-level folder** must contain a `README.md` explaining:
  - Purpose of the folder.
  - Key files and their roles.
  - How to use or run the contents.
  - Dependencies or prerequisites.

## Code Documentation

- All Python modules must have a module-level docstring explaining purpose.
- All public functions and classes must have docstrings.
- Airflow DAGs must have `doc_md` set with a clear description of the pipeline.

## Architecture Documentation

- `docs/architecture.md` must be updated whenever a new service or component is added.
- `docs/pipeline.md` must be updated whenever DAG tasks change.
- `docs/lineage.md` must be updated whenever data flow changes.

## Documentation Maintenance

- When merging a feature branch, documentation updates are **mandatory**.
- The root `README.md` must reflect the current state of the project at all times.
- Add or update ADRs in `docs/decisions/` for significant architectural choices.
