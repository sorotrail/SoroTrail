#!/usr/bin/env python3
"""End-to-end example exercising the SoroTrail Python client against a live server."""

from __future__ import annotations

import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import sys

# Allow running directly from a repository checkout without a prior pip install.
_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_ROOT))

from sorotrail import (  # noqa: E402
    BadRequestError,
    CountEventsParams,
    ErrorCode,
    ListContractsParams,
    ListEventsParams,
    NotFoundError,
    SoroTrailClient,
    parse_wide_decimal,
    parse_wide_int,
)


def run_example(base_url: str, api_key: str | None = None) -> dict[str, object]:
    """Run an end-to-end workflow against ``base_url`` and return a summary."""
    with SoroTrailClient(base_url, api_key=api_key, timeout=10.0) as client:
        health = client.health()
        if health.status not in ("ok", "degraded"):
            raise RuntimeError(f"unexpected health status: {health.status!r}")

        version = client.version()

        # Stream all events across cursor-paginated pages (page size = 2).
        streamed_events = list(client.iter_events(ListEventsParams(limit=2)))
        if not streamed_events:
            raise RuntimeError("expected at least one event from iter_events")

        # Verify wide-integer (u128 / i128 / u256) preservation without float loss.
        first_event = client.get_event(streamed_events[0].id)
        wide_int_checked = 0
        wide_decimal_str = ""
        if isinstance(first_event.value, dict):
            raw_amount = first_event.value.get("amount")
            if isinstance(raw_amount, (str, int, Decimal)):
                wide_int_checked = parse_wide_int(raw_amount)
                wide_decimal_str = str(parse_wide_decimal(raw_amount, decimals=7))

        # Stream contracts across cursor-paginated pages (page size = 1).
        contracts = list(client.iter_contracts(ListContractsParams(limit=1)))

        # Count contract events.
        total = client.count_events(CountEventsParams(type="contract"))

        # Exercise structured machine-readable error handling (#1270).
        not_found_code = ""
        try:
            client.get_event("9999999999999999-9999999999")
        except NotFoundError as err:
            not_found_code = err.code

        bad_request_code = ""
        try:
            client.list_events(ListEventsParams(limit=-5))
        except BadRequestError as err:
            bad_request_code = err.code

        if not_found_code != ErrorCode.NOT_FOUND.value:
            raise RuntimeError(f"expected NOT_FOUND code, got {not_found_code!r}")
        if bad_request_code not in (
            ErrorCode.INVALID_PARAMETER.value,
            ErrorCode.LIMIT_EXCEEDED.value,
        ):
            raise RuntimeError(f"unexpected 400 error code: {bad_request_code!r}")

        return {
            "health_status": health.status,
            "server_version": version.version,
            "streamed_event_ids": [ev.id for ev in streamed_events],
            "contract_ids": [c.contract_id for c in contracts],
            "counted_contract_events": total.count,
            "wide_int": str(wide_int_checked),
            "wide_decimal": wide_decimal_str,
            "not_found_code": not_found_code,
            "bad_request_code": bad_request_code,
        }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=os.environ.get("SOROTRAIL_URL", "http://127.0.0.1:8080"),
        help="Base URL of the running SoroTrail server",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("SOROTRAIL_API_KEY"),
        help="Optional API key for authenticated endpoints",
    )
    args = parser.parse_args(argv)
    summary = run_example(str(args.base_url), args.api_key)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
