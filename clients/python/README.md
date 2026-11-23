# SoroTrail Python Client (`sorotrail`)

Typed, zero-dependency Python client for the [SoroTrail](https://github.com/sorotrail/SoroTrail) Stellar/Soroban contract event indexer HTTP API, generated directly from [`api/openapi.yaml`](../../api/openapi.yaml).

## Packaging & Distribution Decision

The `sorotrail` Python client is **versioned alongside the HTTP specification in this repository and consumed directly from the repository** (rather than published as a standalone package on PyPI). Keeping the client in the monorepo guarantees that every commit and release tag carries a client that is byte-for-byte verified against `api/openapi.yaml` by CI (`pkg/client/python_drift_test.go`).

Install from a release tag or commit with `pip` or `uv`:

```bash
pip install "git+https://github.com/sorotrail/SoroTrail.git@v0.2.0#subdirectory=clients/python"
```

Or for local development from a checkout of this repository:

```bash
pip install -e ./clients/python
```

### Pinned Toolchain

- **Minimum Python version**: Python `>=3.10` (pinned to `3.12` in `.python-version` for local/CI workflows).
- **Type checker**: `mypy==1.15.0` (`strict = true` configured in `pyproject.toml`), plus the structural type-annotation and runtime contract suite in `clients/python/tests/test_client.py` executed automatically by `go test ./pkg/client/...`.
- **Regeneration**: Run `make client` (or `go run ./cmd/clientgen`) after editing `api/openapi.yaml`.

---

## Features

1. **Full OpenAPI Surface Parity**: Covers all 50 documented operations and all component/response schemas in `api/openapi.yaml` on parity with `pkg/client` (Go).
2. **Cursor-Pagination Iterators**: Built-in lazy generators (`iter_events`, `iter_contract_events`, `iter_address_events`, `iter_contracts`, `iter_dead_letters`, `iter_contract_spec_override_history`, and generic `paginate`) stream multi-page result sets without manual cursor bookkeeping.
3. **Structured Machine-Readable Errors (`#1270`)**: Raises `SoroTrailAPIError` (and status/code-specific subclasses such as `BadRequestError`, `UnauthorizedError`, `ForbiddenError`, `NotFoundError`, `ConflictError`, `PayloadTooLargeError`, `QuotaExceededError`, `RateLimitedError`, `UnsupportedOperationError`, `ServiceUnavailableError`, `InternalServerError`) carrying `status_code`, `code` (`ErrorCode`), `message`, `field`, and `retry_after`.
4. **Exact Soroban Wide-Integer (`i128` / `u128` / `i256` / `u256`) Preservation**:
   - All JSON responses are decoded with `json.loads(..., parse_float=Decimal, parse_int=int)` so numeric values never pass through IEEE-754 `float`.
   - String-encoded Soroban wide integers (`i128`, `u128`, `i256`, `u256`) remain exact strings on wire payloads and can be converted with `parse_wide_int(val)` or scaled with `parse_wide_decimal(val, decimals=7)`—both of which reject `float` inputs with `TypeError` to prevent silent 53-bit precision loss.

---

## Quickstart

```python
from decimal import Decimal
from sorotrail import (
    ErrorCode,
    ListEventsParams,
    NotFoundError,
    RateLimitedError,
    SoroTrailAPIError,
    SoroTrailClient,
    parse_wide_decimal,
    parse_wide_int,
)

with SoroTrailClient("http://localhost:8080", api_key="st_live_...") as client:
    # 1. Stream cursor-paginated events across pages automatically:
    for event in client.iter_events(ListEventsParams(type="contract", limit=100)):
        print(event.id, event.contract_id, event.ledger)

        # 2. Safely parse Soroban i128/u128/i256/u256 token amounts without float coercion:
        if isinstance(event.sep41_event, dict):
            raw_amount = event.sep41_event.get("amount")
            if isinstance(raw_amount, (str, int, Decimal)):
                stroops = parse_wide_int(raw_amount)
                tokens = parse_wide_decimal(raw_amount, decimals=7)
                print(f"  amount={stroops} raw ({tokens} tokens)")

    # 3. Branch on stable machine-readable error codes (#1270):
    try:
        client.get_event("0000000000000000-0000000000")
    except NotFoundError as err:
        assert err.code == ErrorCode.NOT_FOUND
    except RateLimitedError as err:
        print(f"Rate limited ({err.code}), retry after {err.retry_after}s")
    except SoroTrailAPIError as err:
        print(f"API error {err.status_code} [{err.code}] field={err.field}: {err.message}")
```

## Running Tests & Type Checks

```bash
# Unit & strict annotation tests
python3 -m unittest discover -s clients/python/tests -v

# Strict static type checking (when mypy is installed)
mypy --config-file clients/python/pyproject.toml clients/python/sorotrail clients/python/tests clients/python/examples

# Full Go + Python drift and end-to-end live server test
go test -v ./pkg/client -run TestPythonClient
```
