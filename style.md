# CohortDiagnostics Style Guide

## Naming Conventions

- Use `lower_snake_case` for all database objects:
  - table names
  - column names
  - SQL aliases
  - CSV export column names
- Use `camelCase` for all R identifiers:
  - function names
  - variable names
  - argument names

## R Code Style

- Prefer explicit namespaces for non-base calls in package code (`pkg::fn`).
- Prefer `||` and `&&` for scalar logical checks; reserve `|` and `&` for vectorized operations.
- Avoid implicit side effects in helpers; return values explicitly.
- Handle empty data frames explicitly and preserve expected schema (column names and types) whenever possible.
- Keep public API behavior backward-compatible unless explicitly versioned.

## SQL and Data Handling

- Keep SQL object references schema-qualified when possible.
- Keep exported table schemas aligned with `getResultsDataModelSpecifications()`.
- Allow zero-row outputs as valid outcomes; empty output is not an error when schema is correct.

## Testing Standards

- Keep fast unit tests independent from external data/services.
- Gate integration tests requiring large fixtures or external systems behind environment variables.
- Add tests for edge cases (empty outputs, missing optional rows, default/fallback configs).
- For Shiny tests, verify both startup and empty-state behavior, not just rendering success.
