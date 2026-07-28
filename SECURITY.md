# Security policy

## Reporting a vulnerability

**Please report security issues privately.** Open a private vulnerability
report via GitHub:

> **https://github.com/sorotrail/SoroTrail/security/advisories/new**

GitHub's private vulnerability reporting hides the report from public view
until a maintainer decides to disclose it. Filing in any other channel —
public issues, Discussions, or social media — risks disclosure before a
fix is shipped and is **strongly discouraged**.

If you cannot use GitHub's private reporting flow for any reason, open
a regular issue and reference the security tag so a maintainer can
follow up privately. Public issues are visible to everyone, so do not
include exploit details in that fallback path.

## What to include

The faster a maintainer can reproduce and reason about a report, the
faster a fix ships. A good report includes:

- The SoroTrail version (`sorotrail --version`) and commit SHA
  (`git rev-parse HEAD`) you reproduced against.
- The deployment shape (single binary, Docker compose, Helm) and the
  environment variables that affect the affected code path
  (`DATABASE_URL`, `RPC_URL`, retention/sweep windows, etc.). **Redact
  secrets** — the RPC URL's basic-auth password and `API_KEY` in
  particular. The maintainer doesn't need them to reproduce and won't
  accept reports that contain credentials in plaintext.
- A minimum reproduction sequence — numbered API calls, a script, or
  the RPC traffic captured in a tool like `mitmproxy`.
- The observed behavior and the expected behavior; the impact (data
  loss, ingestion halt, RCE, etc.).
- Logs, but redacted as above.

## Supported versions

| Version | Supported          |
| ------- | ------------------ |
| `main`  | :white_check_mark: |
| latest tagged release | :white_check_mark: |
| older tags and `main`-N | :x: |

Only the latest tagged release and `main` receive security fixes.
Older tags are not patched; please upgrade before reporting.

## Response

> **Note — the specific timelines below are proposed and await maintainer
> sign-off on issue #60 before they take effect.** They are included
> here as a starting point for the policy review, not as published
> commitments. Until the maintainers confirm, the actual response time
> is best-effort and case-by-case.

A maintainer will acknowledge the report within **5 business days** and
work with you to confirm the report, decide on a fix, and coordinate
disclosure. The actual fix timeline depends on severity and complexity:

| Severity | Target fix-by (proposed) |
| -------- | ------------------------- |
| Critical (RCE, data loss, unauthenticated admin access) | within 7 days of confirmation |
| High     | within 30 days of confirmation |
| Medium / Low | next minor release |

These targets are best-effort and may slip for issues that require
coordination with upstream dependencies. The maintainer will keep you
posted throughout.

## Disclosure policy

Once a fix is available the maintainer will:

1. Tag a release that contains the fix.
2. Publish a GitHub Security Advisory describing the vulnerability, the
   impact, the affected versions, and the fix.
3. Credit the reporter (unless they prefer anonymity).

Please do not publish details of the vulnerability yourself until the
advisory is out — coordinated disclosure protects downstream operators
who haven't upgraded yet.
