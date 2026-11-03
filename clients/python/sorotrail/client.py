"""Runtime transport, pagination iterators, wide-integer helpers, and error types."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from enum import Enum
import http.client
import json
from typing import Final, TypeVar
import urllib.error
import urllib.parse
import urllib.request

from ._generated import (
    AddressEventsParams,
    ContractEventsParams,
    ContractSpecOverrideVersion,
    ContractSummary,
    DeadLetter,
    ErrorResponse,
    Event,
    JSONObject,
    JSONValue,
    ListContractSpecOverrideHistoryParams,
    ListContractsParams,
    ListDeadLettersParams,
    ListEventsParams,
    SPEC_VERSION,
    _GeneratedClientBase,
    _as_json_obj,
    _as_json_val,
)

PageT = TypeVar("PageT")
ItemT = TypeVar("ItemT")

I128_MIN: Final[int] = -(1 << 127)
I128_MAX: Final[int] = (1 << 127) - 1
U128_MAX: Final[int] = (1 << 128) - 1
I256_MIN: Final[int] = -(1 << 255)
I256_MAX: Final[int] = (1 << 255) - 1
U256_MAX: Final[int] = (1 << 256) - 1


class ErrorCode(str, Enum):
    """Stable machine-readable error codes returned by the SoroTrail API."""

    INVALID_PARAMETER = "INVALID_PARAMETER"
    INVALID_CURSOR = "INVALID_CURSOR"
    RANGE_TOO_WIDE = "RANGE_TOO_WIDE"
    LIMIT_EXCEEDED = "LIMIT_EXCEEDED"
    INVALID_BODY = "INVALID_BODY"
    CONFIRMATION_REQUIRED = "CONFIRMATION_REQUIRED"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    RATE_LIMITED = "RATE_LIMITED"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    UNSUPPORTED_OPERATION = "UNSUPPORTED_OPERATION"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"


class SoroTrailError(Exception):
    """Base exception raised by the SoroTrail Python client."""


class SoroTrailAPIError(SoroTrailError):
    """Structured HTTP error returned by a SoroTrail server."""

    status_code: int
    code: str
    message: str
    field: str | None
    retry_after: int | None

    def __init__(
        self,
        status_code: int,
        message: str,
        *,
        code: str = "",
        field: str | None = None,
        retry_after: int | None = None,
    ) -> None:
        resolved_code = code if code != "" else _default_code_for_status(status_code)
        resolved_field = field if (field is not None and field != "") else None
        super().__init__(self._format(status_code, resolved_code, message, resolved_field))
        self.status_code = status_code
        self.code = resolved_code
        self.message = message
        self.field = resolved_field
        self.retry_after = retry_after

    @staticmethod
    def _format(status_code: int, code: str, message: str, field: str | None) -> str:
        prefix = f"sorotrail: status {status_code} ({code})"
        if field is not None:
            return f"{prefix} [field={field}]: {message}"
        return f"{prefix}: {message}"


class BadRequestError(SoroTrailAPIError):
    """Raised on HTTP 400 validation or parameter errors."""


class UnauthorizedError(SoroTrailAPIError):
    """Raised on HTTP 401 authentication errors."""


class ForbiddenError(SoroTrailAPIError):
    """Raised on HTTP 403 authorization errors."""


class NotFoundError(SoroTrailAPIError):
    """Raised on HTTP 404 missing resource errors."""


class ConflictError(SoroTrailAPIError):
    """Raised on HTTP 409 state conflict errors."""


class PayloadTooLargeError(SoroTrailAPIError):
    """Raised on HTTP 413 oversized request body errors."""


class QuotaExceededError(SoroTrailAPIError):
    """Raised on HTTP 429 watch-list quota exhaustion (QUOTA_EXCEEDED)."""


class RateLimitedError(SoroTrailAPIError):
    """Raised on HTTP 429 rate-limit exhaustion (RATE_LIMITED)."""


class InternalServerError(SoroTrailAPIError):
    """Raised on HTTP 500 internal server errors."""


class UnsupportedOperationError(SoroTrailAPIError):
    """Raised on HTTP 501 unsupported operation errors."""


class ServiceUnavailableError(SoroTrailAPIError):
    """Raised on HTTP 503 service unavailable errors."""


def _default_code_for_status(status_code: int) -> str:
    mapping: dict[int, str] = {
        400: ErrorCode.INVALID_PARAMETER.value,
        401: ErrorCode.UNAUTHORIZED.value,
        403: ErrorCode.FORBIDDEN.value,
        404: ErrorCode.NOT_FOUND.value,
        409: ErrorCode.CONFLICT.value,
        413: ErrorCode.PAYLOAD_TOO_LARGE.value,
        429: ErrorCode.RATE_LIMITED.value,
        500: ErrorCode.INTERNAL_ERROR.value,
        501: ErrorCode.UNSUPPORTED_OPERATION.value,
        503: ErrorCode.SERVICE_UNAVAILABLE.value,
    }
    return mapping.get(status_code, ErrorCode.INTERNAL_ERROR.value)


def _build_api_error(
    status_code: int,
    message: str,
    *,
    code: str = "",
    field: str | None = None,
    retry_after: int | None = None,
) -> SoroTrailAPIError:
    resolved_code = code if code != "" else _default_code_for_status(status_code)
    if status_code == 429 and resolved_code == ErrorCode.QUOTA_EXCEEDED.value:
        return QuotaExceededError(
            status_code,
            message,
            code=resolved_code,
            field=field,
            retry_after=retry_after,
        )
    cls_by_status: dict[int, type[SoroTrailAPIError]] = {
        400: BadRequestError,
        401: UnauthorizedError,
        403: ForbiddenError,
        404: NotFoundError,
        409: ConflictError,
        413: PayloadTooLargeError,
        429: RateLimitedError,
        500: InternalServerError,
        501: UnsupportedOperationError,
        503: ServiceUnavailableError,
    }
    err_cls = cls_by_status.get(status_code, SoroTrailAPIError)
    return err_cls(
        status_code,
        message,
        code=resolved_code,
        field=field,
        retry_after=retry_after,
    )


def parse_wide_int(value: str | int | Decimal) -> int:
    """Parse a Soroban i128/u128/i256/u256 value into an exact Python int.

    Rejects IEEE-754 ``float`` and ``bool`` values with ``TypeError`` so wide
    on-chain integers never suffer silent 53-bit mantissa truncation.
    """
    if isinstance(value, bool) or isinstance(value, float):
        raise TypeError("float and bool are not valid wide integers; pass str, int, or Decimal")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            raise ValueError(f"non-integral Decimal cannot be converted to wide int: {value!r}")
        parsed = int(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            raise ValueError("empty string is not a valid wide integer")
        try:
            parsed = int(stripped, 10)
        except ValueError as exc:
            raise ValueError(f"invalid wide integer string: {value!r}") from exc
    else:
        raise TypeError(f"unsupported wide integer type: {type(value).__name__}")

    if parsed < I256_MIN or parsed > U256_MAX:
        raise ValueError(f"value {parsed} exceeds 256-bit integer bounds [{I256_MIN}, {U256_MAX}]")
    return parsed


def parse_wide_decimal(value: str | int | Decimal, decimals: int = 0) -> Decimal:
    """Parse a Soroban i128/u128/i256/u256 value into a high-precision Decimal.

    When ``decimals > 0``, scales the integer value by ``10 ** -decimals``
    using exact base-10 arithmetic (for example ``decimals=7`` for Stellar
    SAC tokens or ``decimals=18`` for 18-decimal SEP-41 tokens).
    """
    if isinstance(value, bool) or isinstance(value, float):
        raise TypeError("float and bool are not valid wide decimals; pass str, int, or Decimal")
    if decimals < 0:
        raise ValueError("decimals must be non-negative")
    if isinstance(value, int):
        dec = Decimal(value)
    elif isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError(f"non-finite Decimal is not a valid wide decimal: {value!r}")
        dec = value
    elif isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            raise ValueError("empty string is not a valid wide decimal")
        try:
            dec = Decimal(stripped)
        except InvalidOperation as exc:
            raise ValueError(f"invalid wide decimal string: {value!r}") from exc
        if not dec.is_finite():
            raise ValueError(f"non-finite Decimal string: {value!r}")
    else:
        raise TypeError(f"unsupported wide decimal type: {type(value).__name__}")

    if decimals == 0:
        return dec
    tup = dec.as_tuple()
    if not isinstance(tup.exponent, int):
        raise ValueError(f"non-finite Decimal is not a valid wide decimal: {value!r}")
    return Decimal((tup.sign, tup.digits, tup.exponent - decimals))


def paginate(
    fetch_page: Callable[[str], PageT],
    extract_items: Callable[[PageT], Sequence[ItemT]],
    extract_cursor: Callable[[PageT], str],
) -> Iterator[ItemT]:
    """Stream items lazily across cursor-paginated pages.

    Stops when a page returns an empty item list or an empty cursor, and raises
    ``SoroTrailError`` if a broken server repeats the same non-empty cursor.
    """
    cursor = ""
    seen_cursors: set[str] = set()
    while True:
        page = fetch_page(cursor)
        items = extract_items(page)
        for item in items:
            yield item
        next_cursor = extract_cursor(page)
        if not items or next_cursor == "":
            break
        if next_cursor in seen_cursors:
            raise SoroTrailError(f"pagination cursor loop detected on cursor {next_cursor!r}")
        seen_cursors.add(next_cursor)
        cursor = next_cursor


class _DecimalEncoder(json.JSONEncoder):
    def default(self, o: object) -> object:
        if isinstance(o, Decimal):
            if o == o.to_integral_value():
                return int(o)
            return float(o)
        return super().default(o)


class SoroTrailClient(_GeneratedClientBase):
    """Synchronous, strictly typed HTTP client for the SoroTrail API."""

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        bearer_token: str | None = None,
        timeout: float = 30.0,
        user_agent: str | None = None,
        headers: Mapping[str, str] | None = None,
        opener: urllib.request.OpenerDirector | None = None,
    ) -> None:
        trimmed = base_url.strip().rstrip("/")
        parsed = urllib.parse.urlparse(trimmed)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError(f"base_url must be an absolute http(s) URL, got {base_url!r}")

        self._base_url: str = trimmed
        self._api_key: str | None = api_key
        self._bearer_token: str | None = bearer_token
        self._timeout: float = timeout
        self._user_agent: str = user_agent or f"sorotrail-python/{SPEC_VERSION}"
        self._extra_headers: dict[str, str] = dict(headers) if headers is not None else {}
        self._opener: urllib.request.OpenerDirector = (
            opener if opener is not None else urllib.request.build_opener()
        )

    @property
    def base_url(self) -> str:
        """Return the normalized base URL of the SoroTrail server."""
        return self._base_url

    def close(self) -> None:
        """Close the client (no-op for urllib transport; present for context-manager parity)."""

    def __enter__(self) -> SoroTrailClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        self.close()

    def iter_events(
        self,
        params: ListEventsParams | None = None,
    ) -> Iterator[Event]:
        """Yield events lazily across cursor-paginated ``GET /events`` pages."""
        base = replace(params) if params is not None else ListEventsParams()

        def _fetch(cursor: str) -> tuple[list[Event], str]:
            page_params = replace(base, cursor=cursor) if cursor != "" else replace(base)
            resp = self.list_events(page_params)
            return (resp.events, resp.cursor)

        return paginate(_fetch, lambda p: p[0], lambda p: p[1])

    def iter_contract_events(
        self,
        id: str,
        params: ContractEventsParams | None = None,
    ) -> Iterator[Event]:
        """Yield events lazily across ``GET /contracts/{id}/events`` pages."""
        base = replace(params) if params is not None else ContractEventsParams()

        def _fetch(cursor: str) -> tuple[list[Event], str]:
            page_params = replace(base, cursor=cursor) if cursor != "" else replace(base)
            resp = self.contract_events(id, page_params)
            return (resp.events, resp.cursor)

        return paginate(_fetch, lambda p: p[0], lambda p: p[1])

    def iter_address_events(
        self,
        address: str,
        params: AddressEventsParams | None = None,
    ) -> Iterator[Event]:
        """Yield events lazily across ``GET /addresses/{address}/events`` pages."""
        base = replace(params) if params is not None else AddressEventsParams()

        def _fetch(cursor: str) -> tuple[list[Event], str]:
            page_params = replace(base, cursor=cursor) if cursor != "" else replace(base)
            resp = self.address_events(address, page_params)
            return (resp.events, resp.cursor)

        return paginate(_fetch, lambda p: p[0], lambda p: p[1])

    def iter_contracts(
        self,
        params: ListContractsParams | None = None,
    ) -> Iterator[ContractSummary]:
        """Yield contract summaries lazily across ``GET /contracts`` pages."""
        base = replace(params) if params is not None else ListContractsParams()

        def _fetch(cursor: str) -> tuple[list[ContractSummary], str]:
            page_params = replace(base, cursor=cursor) if cursor != "" else replace(base)
            resp = self.list_contracts(page_params)
            return (resp.contracts, resp.cursor)

        return paginate(_fetch, lambda p: p[0], lambda p: p[1])

    def iter_dead_letters(
        self,
        params: ListDeadLettersParams | None = None,
    ) -> Iterator[DeadLetter]:
        """Yield dead-letter rows lazily across ``GET /dead-letters`` pages."""
        base = replace(params) if params is not None else ListDeadLettersParams()

        def _fetch(cursor: str) -> tuple[list[DeadLetter], str]:
            page_params = replace(base, cursor=cursor) if cursor != "" else replace(base)
            resp = self.list_dead_letters(page_params)
            return (resp.dead_letters, resp.cursor)

        return paginate(_fetch, lambda p: p[0], lambda p: p[1])

    def iter_contract_spec_override_history(
        self,
        id: str,
        params: ListContractSpecOverrideHistoryParams | None = None,
    ) -> Iterator[ContractSpecOverrideVersion]:
        """Yield spec override history entries across ``GET /contracts/{id}/spec/history`` pages."""
        base = (
            replace(params)
            if params is not None
            else ListContractSpecOverrideHistoryParams()
        )

        def _fetch(cursor: str) -> tuple[list[ContractSpecOverrideVersion], str]:
            page_params = replace(base, cursor=cursor) if cursor != "" else replace(base)
            resp = self.list_contract_spec_override_history(id, page_params)
            return (resp.versions, resp.cursor)

        return paginate(_fetch, lambda p: p[0], lambda p: p[1])

    def _build_url(self, path: str, query: Mapping[str, str] | None) -> str:
        url = f"{self._base_url}{path}"
        if query:
            encoded = urllib.parse.urlencode(sorted(query.items()))
            if encoded:
                url = f"{url}?{encoded}"
        return url

    def _build_headers(self, accept: str, has_body: bool) -> dict[str, str]:
        hdrs: dict[str, str] = {
            "Accept": accept,
            "User-Agent": self._user_agent,
        }
        if has_body:
            hdrs["Content-Type"] = "application/json"
        if self._api_key is not None and self._api_key != "":
            hdrs["X-API-Key"] = self._api_key
        if self._bearer_token is not None and self._bearer_token != "":
            hdrs["Authorization"] = f"Bearer {self._bearer_token}"
        hdrs.update(self._extra_headers)
        return hdrs

    def _execute(
        self,
        method: str,
        path: str,
        query: Mapping[str, str] | None,
        body: JSONObject | None,
        accept: str,
    ) -> tuple[int, bytes, Mapping[str, str]]:
        url = self._build_url(path, query)
        data: bytes | None = None
        if body is not None:
            data = json.dumps(body, cls=_DecimalEncoder).encode("utf-8")
        headers = self._build_headers(accept, data is not None)
        req = urllib.request.Request(url=url, data=data, headers=headers, method=method)
        try:
            with self._opener.open(req, timeout=self._timeout) as resp:
                if not isinstance(resp, http.client.HTTPResponse):
                    raw_bytes = bytes(resp.read())
                    status = int(getattr(resp, "status", 200))
                    return status, raw_bytes, {}
                raw_bytes = resp.read()
                resp_headers = {k.lower(): v for k, v in resp.headers.items()}
                return resp.status, raw_bytes, resp_headers
        except urllib.error.HTTPError as exc:
            err_body = exc.read() if exc.fp is not None else b""
            err_headers: dict[str, str] = {}
            if exc.headers is not None:
                err_headers = {k.lower(): v for k, v in exc.headers.items()}
            raise self._decode_error(exc.code, exc.reason, err_body, err_headers) from None
        except urllib.error.URLError as exc:
            raise SoroTrailError(f"request {method} {path} failed: {exc.reason}") from exc

    @staticmethod
    def _decode_error(
        status_code: int,
        reason: object,
        body: bytes,
        headers: Mapping[str, str],
    ) -> SoroTrailAPIError:
        retry_after: int | None = None
        ra_raw = headers.get("retry-after", "").strip()
        if ra_raw.isdigit():
            retry_after = int(ra_raw)

        text = body.decode("utf-8", errors="replace").strip()
        if text != "":
            try:
                parsed: object = json.loads(text, parse_float=Decimal, parse_int=int)
                if isinstance(parsed, dict):
                    env = ErrorResponse.from_dict(_as_json_obj(parsed))
                    msg = env.error if env.error != "" else text
                    return _build_api_error(
                        status_code,
                        msg,
                        code=env.code,
                        field=env.field if env.field != "" else None,
                        retry_after=retry_after,
                    )
            except Exception:
                pass
            return _build_api_error(status_code, text, retry_after=retry_after)

        fallback = str(reason) if reason else f"HTTP {status_code}"
        return _build_api_error(status_code, fallback, retry_after=retry_after)

    def _request_json(
        self,
        method: str,
        path: str,
        query: Mapping[str, str] | None = None,
        body: JSONObject | None = None,
    ) -> JSONValue:
        _, raw_bytes, _ = self._execute(method, path, query, body, "application/json")
        if not raw_bytes:
            return {}
        text = raw_bytes.decode("utf-8")
        try:
            loaded: object = json.loads(text, parse_float=Decimal, parse_int=int)
        except ValueError as exc:
            raise SoroTrailError(f"decoding {method} {path} JSON response: {exc}") from exc
        return _as_json_val(loaded)

    def _request_bytes(
        self,
        method: str,
        path: str,
        query: Mapping[str, str] | None = None,
        body: JSONObject | None = None,
    ) -> bytes:
        _, raw_bytes, _ = self._execute(method, path, query, body, "*/*")
        return raw_bytes

    def _request_no_content(
        self,
        method: str,
        path: str,
        query: Mapping[str, str] | None = None,
        body: JSONObject | None = None,
    ) -> None:
        self._execute(method, path, query, body, "application/json")
