<!--
  Thanks for the PR! This template mirrors the checklist in CONTRIBUTING.md
  so a reviewer can confirm at a glance that the gating checks ran. Where
  the templates diverge, update CONTRIBUTING.md in the same PR — one source
  of truth.
-->

## Summary

<!-- One or two sentences. What does this PR change and why? -->

Closes # <!-- REQUIRED for issue-driven work. One or more "Closes #N" lines; the linked issues close automatically when this lands on `main`. Use "Refs #N" for context-only references that shouldn't auto-close. -->

## Type of change

<!-- Pick all that apply. -->

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to change)
- [ ] Documentation / templates only

## Checklist

<!-- Mirror of the gating checks in CONTRIBUTING.md. Reviewers reject PRs that don't tick every required item. -->

### Build & tests

- [ ] `go build ./...` passes
- [ ] `make test` passes (the unit suite)
- [ ] `make lint` passes (`golangci-lint run`)
- [ ] For schema or DB changes: `make test-db` passes against a throwaway `TEST_DATABASE_URL`
- [ ] New behavior is covered by tests; coverage follows the existing patterns (`table-driven` where practical)

### Docs & config

- [ ] `README.md` updated if API endpoints, request/response shape, or env vars changed (the `API reference` and config table)
- [ ] `docs/*.md` updated if architecture, replay, backfill, or logging flows changed
- [ ] `CONTRIBUTING.md` updated if conventions changed in a way this PR template no longer reflects (so the two stay in sync — one source of truth)

### Operator-facing

- [ ] If a decoder change is included, the PR description notes that operators must run `sorotrail replay --from-ledger N` after upgrading, otherwise the change only applies to events ingested from then on
- [ ] If a migration is included, both the `.up.sql` and `.down.sql` files are checked in; no applied migration is edited

## Out-of-scope / follow-ups

<!-- Anything intentionally not addressed in this PR. Helps reviewers see what you noticed but punted. -->

- n/a

## Testing notes

<!-- Anything reviewers need to know to reproduce your test results: manual steps, an obscure fixture, a flaky-network workaround. -->

- n/a
