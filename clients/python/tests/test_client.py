"""Unit, strict type-annotation, pagination, wide-int, and HTTP tests for sorotrail."""

from __future__ import annotations

import ast
from collections.abc import Callable
from decimal import Decimal
import http.server
import json
from pathlib import Path
import shutil
import subprocess
import sys
import threading
import unittest
import urllib.parse

_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_ROOT))

import sorotrail  # noqa: E402
from sorotrail import (  # noqa: E402
    AddressEventsParams,
    BadRequestError,
    ConflictError,
    ContractEventsParams,
    CreateSubscriptionRequest,
    ErrorCode,
    ForbiddenError,
    I128_MAX,
    I128_MIN,
    I256_MAX,
    I256_MIN,
    InternalServerError,
    JSONObject,
    ListContractSpecOverrideHistoryParams,
    ListContractsParams,
    ListDeadLettersParams,
    ListEventsParams,
    NotFoundError,
    PayloadTooLargeError,
    QuotaExceededError,
    RateLimitedError,
    SPEC_ROUTES,
    SPEC_VERSION,
    ServiceUnavailableError,
    SoroTrailAPIError,
    SoroTrailClient,
    SoroTrailError,
    SubscriptionFilter,
    U128_MAX,
    U256_MAX,
    UnauthorizedError,
    UnsupportedOperationError,
    parse_wide_decimal,
    parse_wide_int,
)


class _MockRouteHandler(http.server.BaseHTTPRequestHandler):
    routes: dict[
        tuple[str, str],
        Callable[[dict[str, list[str]], bytes, dict[str, str]], tuple[int, dict[str, str], bytes]],
    ] = {}

    def log_message(self, format: str, *args: object) -> None:
        return

    def _dispatch(self, method: str) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length > 0 else b""
        req_headers = {k.lower(): v for k, v in self.headers.items()}
        handler = self.routes.get((method, parsed.path))
        if handler is None:
            payload = json.dumps(
                {"error": "resource not found", "code": "NOT_FOUND"}
            ).encode("utf-8")
            self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        status, resp_headers, resp_body = handler(query, body, req_headers)
        self.send_response(status)
        for k, v in resp_headers.items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(resp_body)))
        self.end_headers()
        if resp_body:
            self.wfile.write(resp_body)

    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def do_PUT(self) -> None:
        self._dispatch("PUT")

    def do_PATCH(self) -> None:
        self._dispatch("PATCH")

    def do_DELETE(self) -> None:
        self._dispatch("DELETE")


class TestTypeAnnotationsAndSpecParity(unittest.TestCase):
    """Verify full type-hint coverage, zero Any usage, and OpenAPI spec parity."""

    def test_spec_metadata_and_exports(self) -> None:
        self.assertEqual(SPEC_VERSION, "0.2.0")
        self.assertGreaterEqual(len(SPEC_ROUTES), 58)
        for name in sorotrail.__all__:
            self.assertTrue(hasattr(sorotrail, name), f"missing export {name}")

    def test_every_function_and_method_has_complete_annotations_and_no_any(self) -> None:
        py_files = sorted(
            list((_PACKAGE_ROOT / "sorotrail").glob("*.py"))
            + list((_PACKAGE_ROOT / "examples").glob("*.py"))
            + list((_PACKAGE_ROOT / "tests").glob("*.py"))
        )
        self.assertGreaterEqual(len(py_files), 5)

        for path in py_files:
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    self.assertIsNotNone(
                        node.returns,
                        f"{path.name}:{node.lineno} function {node.name} missing return annotation",
                    )
                    all_args = (
                        list(node.args.posonlyargs)
                        + list(node.args.args)
                        + list(node.args.kwonlyargs)
                    )
                    if node.args.vararg is not None:
                        all_args.append(node.args.vararg)
                    if node.args.kwarg is not None:
                        all_args.append(node.args.kwarg)
                    for idx, arg in enumerate(all_args):
                        if idx == 0 and arg.arg in ("self", "cls"):
                            continue
                        self.assertIsNotNone(
                            arg.annotation,
                            f"{path.name}:{node.lineno} arg '{arg.arg}' in {node.name} missing type annotation",
                        )
                if isinstance(node, ast.Name) and node.id == "Any":
                    self.fail(f"{path.name}:{node.lineno} uses forbidden typing.Any")

    def test_mypy_strict_when_installed(self) -> None:
        mypy_bin = shutil.which("mypy")
        if mypy_bin is None:
            return
        proc = subprocess.run(
            [
                mypy_bin,
                "--config-file",
                str(_PACKAGE_ROOT / "pyproject.toml"),
                str(_PACKAGE_ROOT / "sorotrail"),
                str(_PACKAGE_ROOT / "examples"),
                str(_PACKAGE_ROOT / "tests"),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            proc.returncode,
            0,
            f"mypy --strict failed:\n{proc.stdout}\n{proc.stderr}",
        )


class TestWideIntegers(unittest.TestCase):
    """Verify i128/u128/i256/u256 preservation without IEEE-754 float coercion."""

    def test_parse_wide_int_boundaries(self) -> None:
        for bound in (I128_MIN, I128_MAX, U128_MAX, I256_MIN, I256_MAX, U256_MAX):
            self.assertEqual(parse_wide_int(str(bound)), bound)
            self.assertEqual(parse_wide_int(bound), bound)
            self.assertEqual(parse_wide_int(Decimal(bound)), bound)

    def test_parse_wide_int_rejects_float_and_bool(self) -> None:
        with self.assertRaises(TypeError):
            parse_wide_int(1.5)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            parse_wide_int(True)
        with self.assertRaises(ValueError):
            parse_wide_int(Decimal("12.34"))
        with self.assertRaises(ValueError):
            parse_wide_int("")
        with self.assertRaises(ValueError):
            parse_wide_int("not-a-number")
        with self.assertRaises(ValueError):
            parse_wide_int(U256_MAX + 1)
        with self.assertRaises(ValueError):
            parse_wide_int(I256_MIN - 1)

    def test_parse_wide_decimal_scaling_and_rejection(self) -> None:
        # 128-bit integer amount scaled by 7 and 18 decimals without float loss.
        raw_u128 = "340282366920938463463374607431768211455"
        self.assertEqual(
            parse_wide_decimal(raw_u128, decimals=7),
            Decimal("34028236692093846346337460743176.8211455"),
        )
        self.assertEqual(
            parse_wide_decimal(raw_u128, decimals=18),
            Decimal("340282366920938463463.374607431768211455"),
        )
        with self.assertRaises(TypeError):
            parse_wide_decimal(3.14)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            parse_wide_decimal(False)
        with self.assertRaises(ValueError):
            parse_wide_decimal("100", decimals=-1)


class TestClientPaginationAndErrors(unittest.TestCase):
    """Verify HTTP transport, cursor iterators, and structured error codes."""

    server: http.server.HTTPServer
    thread: threading.Thread
    base_url: str

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = http.server.HTTPServer(("127.0.0.1", 0), _MockRouteHandler)
        port = cls.server.server_port
        cls.base_url = f"http://127.0.0.1:{port}"
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self) -> None:
        _MockRouteHandler.routes = {}

    def test_invalid_base_url_rejected(self) -> None:
        with self.assertRaises(ValueError):
            SoroTrailClient("ftp://example.com")
        with self.assertRaises(ValueError):
            SoroTrailClient("not-a-url")

    def test_iter_events_streams_multiple_pages_and_preserves_wide_numbers(self) -> None:
        seen_headers: list[dict[str, str]] = []
        u128_str = str(U128_MAX)
        u256_int = U256_MAX

        def handle_events(
            query: dict[str, list[str]],
            _body: bytes,
            headers: dict[str, str],
        ) -> tuple[int, dict[str, str], bytes]:
            seen_headers.append(headers)
            cursor = query.get("cursor", [""])[0]
            if cursor == "":
                page: JSONObject = {
                    "events": [
                        {
                            "id": "0001-0001",
                            "contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
                            "ledger": 100,
                            "type": "contract",
                            "tx_hash": "tx1",
                            "tx_index": 0,
                            "op_index": 0,
                            "in_successful_call": True,
                            "topics": ["transfer"],
                            "value": {
                                "i128_str": u128_str,
                                "u256_num": u256_int,
                                "exact_dec": Decimal("0.12345678901234567890123456789"),
                            },
                            "created_at": "2026-07-21T00:00:00Z",
                            "network": "testnet",
                        }
                    ],
                    "cursor": "cursor-page-2",
                }
            elif cursor == "cursor-page-2":
                page = {
                    "events": [
                        {
                            "id": "0001-0002",
                            "contract_id": "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC",
                            "ledger": 101,
                            "type": "contract",
                            "tx_hash": "tx2",
                            "tx_index": 0,
                            "op_index": 0,
                            "in_successful_call": True,
                            "topics": ["mint"],
                            "value": {"i128_str": "42"},
                            "created_at": "2026-07-21T00:00:05Z",
                            "network": "testnet",
                        }
                    ],
                    "cursor": "",
                }
            else:
                raise AssertionError(f"unexpected cursor {cursor!r}")
            # Emit high-precision decimal literal directly in JSON wire bytes.
            raw = (
                json.dumps(page, default=str)
                .replace(
                    '"0.12345678901234567890123456789"',
                    "0.12345678901234567890123456789",
                )
                .encode("utf-8")
            )
            return 200, {"Content-Type": "application/json"}, raw

        _MockRouteHandler.routes[("GET", "/events")] = handle_events

        params = ListEventsParams(type="contract", limit=1)
        client = SoroTrailClient(
            self.base_url,
            api_key="st_test_key",
            bearer_token="jwt_tok",
        )
        events = list(client.iter_events(params))

        # Caller's params dataclass must not be mutated.
        self.assertEqual(params.cursor, "")
        self.assertEqual([e.id for e in events], ["0001-0001", "0001-0002"])
        self.assertEqual(seen_headers[0].get("x-api-key"), "st_test_key")
        self.assertEqual(seen_headers[0].get("authorization"), "Bearer jwt_tok")

        val = events[0].value
        self.assertIsInstance(val, dict)
        if isinstance(val, dict):
            self.assertEqual(parse_wide_int(str(val["i128_str"])), U128_MAX)
            self.assertEqual(val["u256_num"], U256_MAX)
            self.assertIsInstance(val["exact_dec"], Decimal)
            self.assertEqual(
                val["exact_dec"],
                Decimal("0.12345678901234567890123456789"),
            )

    def test_other_cursor_iterators_and_loop_guard(self) -> None:
        _MockRouteHandler.routes[("GET", "/contracts")] = lambda q, _b, _h: (
            200,
            {"Content-Type": "application/json"},
            json.dumps(
                {
                    "contracts": [
                        {
                            "contract_id": "C1",
                            "event_count": 5,
                            "first_ledger": 1,
                            "last_ledger": 5,
                            "last_seen": "2026-07-21T00:00:00Z",
                        }
                    ],
                    "count": 1,
                    "cursor": "",
                }
            ).encode("utf-8"),
        )
        _MockRouteHandler.routes[("GET", "/contracts/C1/events")] = lambda q, _b, _h: (
            200,
            {"Content-Type": "application/json"},
            json.dumps({"events": [], "cursor": ""}).encode("utf-8"),
        )
        _MockRouteHandler.routes[("GET", "/addresses/G1/events")] = lambda q, _b, _h: (
            200,
            {"Content-Type": "application/json"},
            json.dumps({"events": [], "cursor": ""}).encode("utf-8"),
        )
        _MockRouteHandler.routes[("GET", "/dead-letters")] = lambda q, _b, _h: (
            200,
            {"Content-Type": "application/json"},
            json.dumps({"dead_letters": [], "count": 0, "cursor": ""}).encode("utf-8"),
        )
        _MockRouteHandler.routes[("GET", "/contracts/C1/spec/history")] = lambda q, _b, _h: (
            200,
            {"Content-Type": "application/json"},
            json.dumps({"versions": [], "count": 0, "cursor": ""}).encode("utf-8"),
        )

        client = SoroTrailClient(self.base_url)
        self.assertEqual(len(list(client.iter_contracts(ListContractsParams()))), 1)
        self.assertEqual(len(list(client.iter_contract_events("C1", ContractEventsParams()))), 0)
        self.assertEqual(len(list(client.iter_address_events("G1", AddressEventsParams()))), 0)
        self.assertEqual(len(list(client.iter_dead_letters(ListDeadLettersParams()))), 0)
        self.assertEqual(
            len(
                list(
                    client.iter_contract_spec_override_history(
                        "C1", ListContractSpecOverrideHistoryParams()
                    )
                )
            ),
            0,
        )

        # Verify cursor loop guard raises SoroTrailError if server repeats cursor.
        _MockRouteHandler.routes[("GET", "/contracts")] = lambda q, _b, _h: (
            200,
            {"Content-Type": "application/json"},
            json.dumps(
                {
                    "contracts": [
                        {
                            "contract_id": "C1",
                            "event_count": 1,
                            "first_ledger": 1,
                            "last_ledger": 1,
                            "last_seen": "2026-07-21T00:00:00Z",
                        }
                    ],
                    "count": 1,
                    "cursor": "stuck-cursor",
                }
            ).encode("utf-8"),
        )
        with self.assertRaises(SoroTrailError):
            list(client.iter_contracts())

    def test_structured_error_codes_and_typed_exceptions(self) -> None:
        cases: list[tuple[int, str, str | None, type[SoroTrailAPIError]]] = [
            (400, ErrorCode.INVALID_PARAMETER.value, "from_ledger", BadRequestError),
            (400, ErrorCode.INVALID_CURSOR.value, "cursor", BadRequestError),
            (401, ErrorCode.UNAUTHORIZED.value, None, UnauthorizedError),
            (403, ErrorCode.FORBIDDEN.value, None, ForbiddenError),
            (404, ErrorCode.NOT_FOUND.value, None, NotFoundError),
            (409, ErrorCode.CONFLICT.value, None, ConflictError),
            (413, ErrorCode.PAYLOAD_TOO_LARGE.value, None, PayloadTooLargeError),
            (429, ErrorCode.QUOTA_EXCEEDED.value, None, QuotaExceededError),
            (429, ErrorCode.RATE_LIMITED.value, None, RateLimitedError),
            (500, ErrorCode.INTERNAL_ERROR.value, None, InternalServerError),
            (501, ErrorCode.UNSUPPORTED_OPERATION.value, None, UnsupportedOperationError),
            (503, ErrorCode.SERVICE_UNAVAILABLE.value, None, ServiceUnavailableError),
        ]
        client = SoroTrailClient(self.base_url)
        for status, code, err_field, expected_cls in cases:
            payload: dict[str, str] = {"error": f"error for {code}", "code": code}
            if err_field is not None:
                payload["field"] = err_field
            encoded_payload = json.dumps(payload).encode("utf-8")
            status_code = status

            def _err_route(
                _q: dict[str, list[str]],
                _b: bytes,
                _h: dict[str, str],
                s: int = status_code,
                raw: bytes = encoded_payload,
            ) -> tuple[int, dict[str, str], bytes]:
                return (
                    s,
                    {"Content-Type": "application/json", "Retry-After": "7"},
                    raw,
                )

            _MockRouteHandler.routes[("GET", "/health")] = _err_route
            with self.assertRaises(expected_cls) as ctx:
                client.health()
            err = ctx.exception
            self.assertEqual(err.status_code, status)
            self.assertEqual(err.code, code)
            self.assertEqual(err.field, err_field)
            self.assertEqual(err.retry_after, 7)

    def test_create_and_delete_subscription_and_raw_bytes(self) -> None:
        _MockRouteHandler.routes[("POST", "/subscriptions")] = lambda _q, body, _h: (
            201,
            {"Content-Type": "application/json"},
            json.dumps(
                {
                    "id": 9,
                    "url": json.loads(body.decode("utf-8"))["url"],
                    "secret": "whsec_1234567890abcdef",
                    "enabled": True,
                    "failure_count": 0,
                    "filters": {"type": "contract"},
                    "created_at": "2026-07-21T00:00:00Z",
                }
            ).encode("utf-8"),
        )
        _MockRouteHandler.routes[("DELETE", "/subscriptions/9")] = lambda _q, _b, _h: (
            204,
            {},
            b"",
        )
        _MockRouteHandler.routes[("GET", "/metrics")] = lambda _q, _b, _h: (
            200,
            {"Content-Type": "text/plain"},
            b"sorotrail_events_total 12\n",
        )

        client = SoroTrailClient(self.base_url)
        sub = client.create_subscription(
            CreateSubscriptionRequest(
                url="https://example.com/hook",
                secret="whsec_1234567890abcdef",
                enabled=True,
                filters=SubscriptionFilter(type="contract"),
            )
        )
        self.assertEqual(sub.id, 9)
        self.assertEqual(sub.filters.type, "contract")

        client.delete_subscription(9)
        metrics_bytes = client.metrics()
        self.assertEqual(metrics_bytes, b"sorotrail_events_total 12\n")


if __name__ == "__main__":
    unittest.main()
