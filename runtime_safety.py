"""Central fail-closed authorization for every live order submission."""

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
import fcntl
import hashlib
import hmac
import json
import math
from pathlib import Path
import os
import secrets
import stat
import tempfile
import threading
from typing import Optional


DEFAULT_STATE_PATH = Path(__file__).resolve().parent / "data" / "runtime_safety.json"
DEFAULT_CHECKPOINT_PATH = Path(__file__).resolve().parent / "data" / "runtime_safety.revision.json"
_VALID_SIDES = frozenset({"BUY", "SELL"})
_MARKET_ORDER_TYPES = frozenset({"MARKET", "MOC", "MOO"})
_MAX_MARKET_QUOTE_AGE_SECONDS = 3600
_MAX_MARKET_SLIPPAGE_BUFFER_PERCENT = Decimal("25")
_MAX_QUOTE_FUTURE_SKEW_SECONDS = Decimal("5")
OVERSEAS_ORDER_TYPE_CODES = {
    "LIMIT": "00",
    "MOO": "31",
    "LOO": "32",
    "MOC": "33",
    "LOC": "34",
}
_OVERSEAS_ORDER_TYPES_BY_CODE = {
    code: order_type for order_type, code in OVERSEAS_ORDER_TYPE_CODES.items()
}
_NEW_ORDER_WIRE_SCHEMAS = {
    "/uapi/overseas-stock/v1/trading/order": {
        "tr_ids": {"TTTT1002U": "BUY", "TTTT1006U": "SELL"},
        "quantity_field": "ORD_QTY",
        "price_field": "OVRS_ORD_UNPR",
    },
    "/uapi/overseas-stock/v1/trading/daytime-order": {
        "tr_ids": {"TTTS6036U": "BUY", "TTTS6037U": "SELL"},
        "quantity_field": "ORD_QTY",
        "price_field": "OVRS_ORD_UNPR",
    },
    "/uapi/overseas-stock/v1/trading/order-resv": {
        "tr_ids": {"TTTT3014U": "BUY", "TTTT3016U": "SELL"},
        "quantity_field": "FT_ORD_QTY",
        "price_field": "FT_ORD_UNPR3",
    },
}
_CHECKPOINT_LOCKS = {}
_CHECKPOINT_LOCKS_GUARD = threading.Lock()
_MAX_AUTHORIZATION_TTL_SECONDS = 30


def canonical_order_values(side, order_type="LIMIT"):
    """Return the one canonical representation used by policy and KIS wiring."""
    return (
        str(side or "").strip().upper(),
        str(order_type or "").strip().upper(),
    )


def _validate_json_request_value(value):
    """Reject values outside the exact JSON data model before serialization."""
    if value is None or type(value) in (bool, int):
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("request body contains a non-finite float")
        return value
    if type(value) is str:
        return value
    if type(value) is list:
        return [_validate_json_request_value(item) for item in value]
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise ValueError("request body object keys must be strings")
        return {key: _validate_json_request_value(item) for key, item in value.items()}
    raise ValueError("request body contains an unsupported value")


def serialize_request_body_bytes(body):
    """Serialize a JSON request body once with deterministic type-preserving bytes."""
    return json.dumps(
        _validate_json_request_value(body),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def canonical_request_digest(method, tr_id, path, body=None, *, body_bytes=None):
    """Return a deterministic digest of an immutable HTTP order request."""
    if body_bytes is None:
        if isinstance(body, (bytes, bytearray)):
            body_bytes = body
        else:
            body_bytes = serialize_request_body_bytes(body)
    if not isinstance(body_bytes, (bytes, bytearray)):
        raise ValueError("request body bytes are required")
    envelope = {
        "method": str(method or "").strip().upper(),
        "tr_id": str(tr_id or "").strip(),
        "path": str(path or "").strip(),
        "body_sha256": hashlib.sha256(bytes(body_bytes)).hexdigest(),
        "body_length": len(body_bytes),
    }
    if not envelope["method"] or not envelope["tr_id"] or not envelope["path"]:
        raise ValueError("method, tr_id, and path are required")
    canonical = json.dumps(
        envelope,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _canonical_account_bytes(cano, product_code):
    canonical = f"{str(cano or '').strip()}:{str(product_code or '').strip()}"
    return canonical.encode("utf-8")


def account_fingerprint(cano, product_code, *, key):
    """Return a keyed account identifier without exposing raw account data."""
    if not isinstance(key, (bytes, bytearray)) or len(key) < 32:
        raise ValueError("account fingerprint key must contain at least 32 bytes")
    return hmac.new(bytes(key), _canonical_account_bytes(cano, product_code), hashlib.sha256).hexdigest()


def legacy_account_fingerprint(cano, product_code):
    """Compatibility-only legacy digest; never use this value for authorization."""
    return hashlib.sha256(_canonical_account_bytes(cano, product_code)).hexdigest()


def resolve_account_fingerprint_key(injected=None):
    """Resolve an injected key first, then the runtime-only environment fallback."""
    candidate = injected
    if candidate is None:
        candidate = os.environ.get("ACCOUNT_FINGERPRINT_HMAC_KEY")
    if isinstance(candidate, str):
        candidate = candidate.encode("utf-8")
    if not isinstance(candidate, (bytes, bytearray)) or len(candidate) < 32:
        return None
    return bytes(candidate)


@dataclass(frozen=True)
class TrustedMarketQuote:
    price: Decimal
    as_of: datetime
    source: str
    ticker: str


@dataclass(frozen=True)
class SafetyDecision:
    code: str
    reason: str
    can_submit: bool
    shadow_only: bool = True
    revision: Optional[int] = None
    ticker: str = ""
    side: str = ""
    quantity: Optional[Decimal] = None
    notional: Optional[Decimal] = None

    def as_dict(self):
        result = asdict(self)
        for field in ("quantity", "notional"):
            value = result[field]
            if value is not None:
                result[field] = str(value)
        return result


@dataclass(frozen=True)
class OrderAuthorization:
    """Opaque, request-bound, process-local, one-shot order capability."""

    nonce: str
    revision: int
    request_digest: str
    expires_at: datetime
    seal: str


class AuthorizationError(RuntimeError):
    """Raised when an order authorization cannot be safely consumed."""

    def __init__(self, code, reason):
        super().__init__(f"{code}: {reason}")
        self.code = code
        self.reason = reason


class RuntimeSafetyGate:
    """Reload and validate the safety state for each authorization decision."""

    def __init__(
        self,
        state_path=DEFAULT_STATE_PATH,
        *,
        checkpoint_path=None,
        clock=None,
        authorization_ttl_seconds=5,
    ):
        self.state_path = Path(state_path)
        if checkpoint_path is None:
            checkpoint_path = (
                DEFAULT_CHECKPOINT_PATH
                if self.state_path == DEFAULT_STATE_PATH
                else self.state_path.with_name("runtime_safety.revision.json")
            )
        self.checkpoint_path = Path(checkpoint_path)
        self.checkpoint_lock_path = self.checkpoint_path.with_name(
            f"{self.checkpoint_path.name}.lock"
        )
        self._highest_revision = 0
        self._lock = threading.Lock()
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        if (
            isinstance(authorization_ttl_seconds, bool)
            or not isinstance(authorization_ttl_seconds, (int, float))
            or authorization_ttl_seconds <= 0
            or authorization_ttl_seconds > _MAX_AUTHORIZATION_TTL_SECONDS
        ):
            raise ValueError("authorization TTL must be positive and at most 30 seconds")
        self._authorization_ttl = timedelta(seconds=authorization_ttl_seconds)
        self._authorization_secret = os.urandom(32)
        self._authorizations = {}
        self._ambiguous_halt_reason = None
        key = str(self.checkpoint_path.resolve())
        with _CHECKPOINT_LOCKS_GUARD:
            self._checkpoint_lock = _CHECKPOINT_LOCKS.setdefault(key, threading.Lock())

    @staticmethod
    def denied(code, reason, **context):
        return SafetyDecision(code=code, reason=reason, can_submit=False, **context)

    def latch_ambiguous_submission(self, reason):
        """Fail closed in this process after an indeterminate order submission."""
        with self._lock:
            if self._ambiguous_halt_reason is None:
                self._ambiguous_halt_reason = str(
                    reason or "order submission status is unknown"
                )

    @staticmethod
    def _decimal(value):
        if isinstance(value, bool):
            raise ValueError("boolean values are forbidden")
        try:
            result = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("invalid decimal") from exc
        if not result.is_finite():
            raise ValueError("non-finite decimal")
        return result

    @staticmethod
    def _reject_constant(value):
        raise ValueError(f"non-finite JSON number: {value}")

    def _load_state(self):
        fd = None
        try:
            flags = (
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            fd = os.open(self.state_path, flags)
            file_stat = os.fstat(fd)
            if not stat.S_ISREG(file_stat.st_mode):
                raise OSError("runtime safety state is not a regular file")
            mode = stat.S_IMODE(file_stat.st_mode)
            chunks = []
            total = 0
            while True:
                chunk = os.read(fd, 65536)
                if not chunk:
                    break
                total += len(chunk)
                if total > 1024 * 1024:
                    raise OSError("runtime safety state is too large")
                chunks.append(chunk)
            raw = b"".join(chunks).decode("utf-8")
        except (FileNotFoundError, OSError):
            return None, self.denied(
                "SAFETY_STATE_MISSING",
                "runtime safety state is missing or unreadable",
            )
        except UnicodeError:
            return None, self.denied(
                "SAFETY_STATE_INVALID_JSON",
                "runtime safety state is not valid JSON",
            )
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass

        try:
            state = json.loads(
                raw,
                parse_float=Decimal,
                parse_constant=self._reject_constant,
            )
        except (json.JSONDecodeError, UnicodeError, ValueError):
            return None, self.denied(
                "SAFETY_STATE_INVALID_JSON",
                "runtime safety state is not valid JSON",
            )

        try:
            if not isinstance(state, dict):
                raise ValueError("state must be an object")
            for field in ("operator_halt", "live_armed", "shadow_only"):
                if type(state.get(field)) is not bool:
                    raise ValueError(f"{field} must be boolean")
            revision = state.get("revision")
            if type(revision) is not int or revision <= 0:
                raise ValueError("revision must be a positive integer")
            for field in ("reason", "updated_by"):
                if not isinstance(state.get(field), str) or not state[field].strip():
                    raise ValueError(f"{field} must be a non-empty string")
            updated_at = state.get("updated_at")
            if not isinstance(updated_at, str) or not updated_at.endswith("Z"):
                raise ValueError("updated_at must be a UTC ISO timestamp")
            parsed_updated_at = datetime.fromisoformat(updated_at[:-1] + "+00:00")
            if parsed_updated_at.tzinfo != timezone.utc or "T" not in updated_at:
                raise ValueError("updated_at must be a UTC ISO timestamp")
            allowed = state.get("allowed_tickers")
            if (
                not isinstance(allowed, list)
                or not allowed
                or any(not isinstance(item, str) or not item.strip() for item in allowed)
            ):
                raise ValueError("allowed_tickers must be a non-empty string list")
            allowed = [item.strip().upper() for item in allowed]
            if len(allowed) != len(set(allowed)):
                raise ValueError("allowed_tickers must be unique")
            allowed_accounts = state.get("allowed_account_fingerprints")
            if (
                not isinstance(allowed_accounts, list)
                or not allowed_accounts
                or any(not isinstance(item, str) or not item.strip() for item in allowed_accounts)
            ):
                raise ValueError("allowed_account_fingerprints must be a non-empty string list")
            allowed_accounts = [item.strip().lower() for item in allowed_accounts]
            if len(allowed_accounts) != len(set(allowed_accounts)):
                raise ValueError("allowed_account_fingerprints must be unique")
            for item in allowed_accounts:
                if item != "unconfigured" and (
                    len(item) != 64 or any(char not in "0123456789abcdef" for char in item)
                ):
                    raise ValueError("account fingerprints must be SHA-256 hex digests")
            has_configured_account = any(
                item != "unconfigured" for item in allowed_accounts
            )
            if mode != 0o600 and (state["live_armed"] or has_configured_account):
                return None, self.denied(
                    "SAFETY_STATE_INSECURE_PERMISSIONS",
                    "armed or account-configured safety state must have owner-only 0600 permissions",
                )
            max_quantity = self._decimal(state.get("max_order_quantity"))
            max_notional = self._decimal(state.get("max_order_notional"))
            if max_quantity <= 0 or max_quantity != max_quantity.to_integral_value():
                raise ValueError("max_order_quantity must be a positive integer")
            if max_notional <= 0:
                raise ValueError("max_order_notional must be positive")
            quote_max_age = state.get("market_quote_max_age_seconds")
            if (
                type(quote_max_age) is not int
                or quote_max_age <= 0
                or quote_max_age > _MAX_MARKET_QUOTE_AGE_SECONDS
            ):
                raise ValueError("market_quote_max_age_seconds is outside safe bounds")
            slippage_text = state.get("market_slippage_buffer_percent")
            if not isinstance(slippage_text, str) or not slippage_text.strip():
                raise ValueError("market_slippage_buffer_percent must be a Decimal string")
            slippage_percent = self._decimal(slippage_text)
            if (
                slippage_percent < 0
                or slippage_percent > _MAX_MARKET_SLIPPAGE_BUFFER_PERCENT
            ):
                raise ValueError("market_slippage_buffer_percent is outside safe bounds")
        except (KeyError, TypeError, ValueError):
            return None, self.denied(
                "SAFETY_STATE_INVALID_SCHEMA",
                "runtime safety state schema or field type is invalid",
            )

        return {
            **state,
            "allowed_tickers": allowed,
            "allowed_account_fingerprints": allowed_accounts,
            "max_order_quantity": max_quantity,
            "max_order_notional": max_notional,
            "market_quote_max_age_seconds": quote_max_age,
            "market_slippage_buffer_percent": slippage_percent,
        }, None

    def _load_checkpoint(self):
        if not self.checkpoint_path.exists():
            return None, self.denied(
                "REVISION_CHECKPOINT_MISSING",
                "runtime safety revision checkpoint is missing",
            )
        try:
            value = json.loads(self.checkpoint_path.read_text(encoding="utf-8"))
            if not isinstance(value, dict) or type(value.get("revision")) is not int:
                raise ValueError("invalid checkpoint schema")
            if value["revision"] <= 0 or set(value) != {"revision"}:
                raise ValueError("invalid checkpoint revision")
            return value["revision"], None
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError):
            return None, self.denied(
                "REVISION_CHECKPOINT_INVALID",
                "runtime safety revision checkpoint is missing integrity",
            )

    def _store_checkpoint(self, revision):
        parent = self.checkpoint_path.parent
        try:
            parent.mkdir(parents=True, exist_ok=True)
            fd, temp_name = tempfile.mkstemp(prefix=f".{self.checkpoint_path.name}.", dir=parent)
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as handle:
                    json.dump({"revision": revision}, handle, separators=(",", ":"))
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp_name, self.checkpoint_path)
                dir_fd = os.open(parent, os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            finally:
                if os.path.exists(temp_name):
                    os.unlink(temp_name)
        except OSError:
            return self.denied(
                "REVISION_CHECKPOINT_IO_ERROR",
                "runtime safety revision checkpoint could not be persisted",
            )
        return None

    def _authorization_seal(self, nonce, revision, request_digest, expires_at, pid):
        payload = "|".join(
            (
                nonce,
                str(revision),
                request_digest,
                expires_at.isoformat(timespec="microseconds"),
                str(pid),
            )
        ).encode("utf-8")
        return hmac.new(self._authorization_secret, payload, hashlib.sha256).hexdigest()

    @staticmethod
    def _exact_body_string(body, field):
        value = body.get(field)
        if type(value) is not str or not value:
            raise ValueError(f"{field} must be a JSON string")
        return value

    def _parse_new_order_wire(
        self,
        *,
        method,
        tr_id,
        path,
        body,
        account_fingerprint_key=None,
    ):
        if str(method or "").strip().upper() != "POST":
            raise ValueError("new order wire method must be POST")
        path_text = str(path or "").strip()
        tr_id_text = str(tr_id or "").strip()
        schema = _NEW_ORDER_WIRE_SCHEMAS.get(path_text)
        if schema is None:
            raise ValueError("unsupported new order path")
        side = schema["tr_ids"].get(tr_id_text)
        if side is None:
            raise ValueError("unsupported new order TR id")
        if not isinstance(body, dict):
            raise ValueError("new order body must be a JSON object")

        cano = self._exact_body_string(body, "CANO")
        product_code = self._exact_body_string(body, "ACNT_PRDT_CD")
        ticker = self._exact_body_string(body, "PDNO")
        quantity_text = self._exact_body_string(body, schema["quantity_field"])
        price_text = self._exact_body_string(body, schema["price_field"])
        order_code = self._exact_body_string(body, "ORD_DVSN")
        order_type = _OVERSEAS_ORDER_TYPES_BY_CODE.get(order_code)
        if order_type is None:
            raise ValueError("unsupported order division code")
        quantity = self._decimal(quantity_text)
        price = self._decimal(price_text)
        if quantity <= 0 or quantity != quantity.to_integral_value():
            raise ValueError("wire quantity must be a positive integer string")
        if price < 0:
            raise ValueError("wire price must be a non-negative decimal string")

        wire_account_fingerprint = None
        if account_fingerprint_key is not None:
            wire_account_fingerprint = account_fingerprint(
                cano,
                product_code,
                key=account_fingerprint_key,
            )
        return {
            "account_fingerprint": wire_account_fingerprint,
            "ticker": ticker,
            "side": side,
            "quantity": quantity,
            "price": price,
            "order_type": order_type,
        }

    def _wire_values_match_metadata(
        self,
        wire,
        *,
        ticker,
        side,
        quantity,
        price,
        order_type,
        account_fingerprint,
    ):
        try:
            metadata_quantity = self._decimal(quantity)
            metadata_price = self._decimal(price)
        except ValueError:
            return False
        if wire["ticker"] != ticker:
            return False
        if wire["side"] != side:
            return False
        if wire["order_type"] != order_type:
            return False
        if wire["quantity"] != metadata_quantity:
            return False
        if wire["price"] != metadata_price:
            return False
        if (
            wire["account_fingerprint"] is not None
            and wire["account_fingerprint"] != account_fingerprint
        ):
            return False
        return True

    def authorize_request(
        self,
        ticker,
        side,
        quantity,
        price,
        *,
        method,
        tr_id,
        path,
        body,
        account_fingerprint=None,
        account_fingerprint_key=None,
        account_fingerprint_key_available=True,
        order_type="LIMIT",
        risk_reference_price=None,
        trusted_market_quote=None,
        market_quote_preflight=False,
    ):
        """Authorize policy and mint one request capability in one lock scope."""
        ticker_text = str(ticker or "").strip().upper()
        side_text, order_type_text = canonical_order_values(side, order_type)
        if str(method or "").strip().upper() != "POST":
            return self.denied(
                "ORDER_REQUEST_METHOD_INVALID",
                "order authorization is restricted to POST",
                ticker=ticker_text,
                side=side_text,
            ), None
        try:
            body_bytes = serialize_request_body_bytes(body)
        except (TypeError, ValueError):
            return self.denied(
                "ORDER_REQUEST_INVALID",
                "order request cannot be canonically digested",
                ticker=ticker_text,
                side=side_text,
            ), None
        try:
            wire = self._parse_new_order_wire(
                method=method,
                tr_id=tr_id,
                path=path,
                body=body,
                account_fingerprint_key=account_fingerprint_key,
            )
        except (TypeError, ValueError):
            return self.denied(
                "ORDER_REQUEST_INVALID",
                "order request does not match a supported KIS new-order wire schema",
                ticker=ticker_text,
                side=side_text,
            ), None
        if not self._wire_values_match_metadata(
            wire,
            ticker=ticker_text,
            side=side_text,
            quantity=quantity,
            price=price,
            order_type=order_type_text,
            account_fingerprint=account_fingerprint,
        ):
            return self.denied(
                "WIRE_REQUEST_MISMATCH",
                "caller order metadata differs from the KIS wire request",
                shadow_only=False,
                ticker=ticker_text,
                side=side_text,
            ), None
        try:
            request_digest = canonical_request_digest(
                method, tr_id, path, body_bytes=body_bytes
            )
        except (TypeError, ValueError):
            return self.denied(
                "ORDER_REQUEST_INVALID",
                "order request cannot be canonically digested",
                ticker=ticker_text,
                side=side_text,
            ), None
        policy_account_fingerprint = (
            wire["account_fingerprint"]
            if wire["account_fingerprint"] is not None
            else account_fingerprint
        )
        policy_values = (
            wire["ticker"],
            wire["side"],
            wire["order_type"],
            wire["quantity"],
            wire["price"],
            policy_account_fingerprint,
            account_fingerprint_key_available,
            risk_reference_price,
            trusted_market_quote,
            market_quote_preflight,
        )

        with self._lock, self._checkpoint_lock:
            lock_fd = None
            locked = False
            try:
                self.checkpoint_lock_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    lock_fd = os.open(
                        self.checkpoint_lock_path, os.O_RDWR | os.O_CREAT, 0o600
                    )
                except OSError:
                    lock_fd = os.open(self.checkpoint_lock_path, os.O_RDONLY)
                fcntl.flock(lock_fd, fcntl.LOCK_EX)
                locked = True
                decision = self._authorize_under_checkpoint_lock(*policy_values)
                if not decision.can_submit or decision.code != "LIVE_AUTHORIZED":
                    return decision, None
                now = self._clock()
                if (
                    not isinstance(now, datetime)
                    or now.tzinfo is None
                    or now.utcoffset() != timezone.utc.utcoffset(None)
                ):
                    raise AuthorizationError(
                        "ORDER_AUTHORIZATION_CLOCK_INVALID",
                        "authorization clock must be timezone-aware UTC",
                    )
                nonce = secrets.token_urlsafe(32)
                expires_at = now + self._authorization_ttl
                pid = os.getpid()
                authorization = OrderAuthorization(
                    nonce=nonce,
                    revision=decision.revision,
                    request_digest=request_digest,
                    expires_at=expires_at,
                    seal=self._authorization_seal(
                        nonce, decision.revision, request_digest, expires_at, pid
                    ),
                )
                self._authorizations[nonce] = {
                    "authorization": authorization,
                    "pid": pid,
                    "policy_values": policy_values,
                }
                return decision, authorization
            except OSError:
                return self.denied(
                    "REVISION_LOCK_IO_ERROR",
                    "runtime safety revision lock could not be acquired or released",
                    ticker=ticker_text,
                    side=side_text,
                ), None
            finally:
                if locked:
                    try:
                        fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    except OSError:
                        pass
                if lock_fd is not None:
                    try:
                        os.close(lock_fd)
                    except OSError:
                        pass

    @contextmanager
    def authorized_post(self, authorization, *, method, tr_id, path, body):
        """Consume a capability and hold the checkpoint lock through one POST."""
        with self._lock, self._checkpoint_lock:
            lock_fd = None
            locked = False
            try:
                try:
                    self.checkpoint_lock_path.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        lock_fd = os.open(
                            self.checkpoint_lock_path, os.O_RDWR | os.O_CREAT, 0o600
                        )
                    except OSError:
                        lock_fd = os.open(self.checkpoint_lock_path, os.O_RDONLY)
                    fcntl.flock(lock_fd, fcntl.LOCK_EX)
                    locked = True
                except OSError as exc:
                    raise AuthorizationError(
                        "REVISION_LOCK_IO_ERROR",
                        "runtime safety revision lock could not be acquired",
                    ) from exc

                if not isinstance(authorization, OrderAuthorization):
                    raise AuthorizationError(
                        "ORDER_AUTHORIZATION_INVALID", "authorization is not gate-issued"
                    )
                digest_chars = "0123456789abcdef"
                fields_valid = (
                    isinstance(authorization.nonce, str)
                    and bool(authorization.nonce)
                    and type(authorization.revision) is int
                    and authorization.revision > 0
                    and isinstance(authorization.request_digest, str)
                    and len(authorization.request_digest) == 64
                    and all(char in digest_chars for char in authorization.request_digest)
                    and isinstance(authorization.expires_at, datetime)
                    and authorization.expires_at.tzinfo is not None
                    and authorization.expires_at.utcoffset()
                    == timezone.utc.utcoffset(None)
                    and isinstance(authorization.seal, str)
                    and len(authorization.seal) == 64
                    and all(char in digest_chars for char in authorization.seal)
                )
                if not fields_valid:
                    raise AuthorizationError(
                        "ORDER_AUTHORIZATION_INVALID",
                        "authorization fields are malformed",
                    )
                record = self._authorizations.get(authorization.nonce)
                if record is None or record["pid"] != os.getpid():
                    raise AuthorizationError(
                        "ORDER_AUTHORIZATION_INVALID", "authorization is unknown or consumed"
                    )
                expected_seal = self._authorization_seal(
                    authorization.nonce,
                    authorization.revision,
                    authorization.request_digest,
                    authorization.expires_at,
                    record["pid"],
                )
                if (
                    authorization != record["authorization"]
                    or not hmac.compare_digest(authorization.seal, expected_seal)
                ):
                    raise AuthorizationError(
                        "ORDER_AUTHORIZATION_INVALID", "authorization integrity check failed"
                    )

                # A valid nonce is consumed before any request/state check. Every
                # attempted use is one-shot, including fail-closed attempts.
                del self._authorizations[authorization.nonce]
                try:
                    current_digest = canonical_request_digest(method, tr_id, path, body)
                except (TypeError, ValueError) as exc:
                    raise AuthorizationError(
                        "ORDER_AUTHORIZATION_REQUEST_MISMATCH",
                        "current request cannot match the authorized request",
                    ) from exc
                if not hmac.compare_digest(current_digest, authorization.request_digest):
                    raise AuthorizationError(
                        "ORDER_AUTHORIZATION_REQUEST_MISMATCH",
                        "current request differs from the authorized request",
                    )
                now = self._clock()
                if (
                    not isinstance(now, datetime)
                    or now.tzinfo is None
                    or now.utcoffset() != timezone.utc.utcoffset(None)
                ):
                    raise AuthorizationError(
                        "ORDER_AUTHORIZATION_CLOCK_INVALID",
                        "authorization clock must be timezone-aware UTC",
                    )
                if now >= authorization.expires_at:
                    raise AuthorizationError(
                        "ORDER_AUTHORIZATION_EXPIRED", "authorization has expired"
                    )

                decision = self._authorize_under_checkpoint_lock(
                    *record["policy_values"]
                )
                if not decision.can_submit or decision.code != "LIVE_AUTHORIZED":
                    raise AuthorizationError(decision.code, decision.reason)
                if decision.revision != authorization.revision:
                    raise AuthorizationError(
                        "ORDER_AUTHORIZATION_REVISION_MISMATCH",
                        "runtime safety revision changed after authorization",
                    )
                yield authorization
            finally:
                if locked:
                    try:
                        fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    except OSError:
                        pass
                if lock_fd is not None:
                    try:
                        os.close(lock_fd)
                    except OSError:
                        pass

    def authorize(
        self,
        ticker,
        side,
        quantity,
        price,
        *,
        account_fingerprint=None,
        account_fingerprint_key_available=True,
        order_type="LIMIT",
        risk_reference_price=None,
        trusted_market_quote=None,
        market_quote_preflight=False,
    ):
        ticker_text = str(ticker or "").strip().upper()
        side_text, order_type_text = canonical_order_values(side, order_type)

        with self._lock, self._checkpoint_lock:
            if self._ambiguous_halt_reason is not None:
                return self.denied(
                    "ORDER_SUBMISSION_AMBIGUOUS",
                    self._ambiguous_halt_reason,
                    shadow_only=False,
                    ticker=ticker_text,
                    side=side_text,
                )
            lock_fd = None
            locked = False
            decision = None
            try:
                self.checkpoint_lock_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    lock_fd = os.open(
                        self.checkpoint_lock_path, os.O_RDWR | os.O_CREAT, 0o600
                    )
                except OSError:
                    # Read-only deployments may provide a pre-created lock inode.
                    lock_fd = os.open(self.checkpoint_lock_path, os.O_RDONLY)
                fcntl.flock(lock_fd, fcntl.LOCK_EX)
                locked = True
                decision = self._authorize_under_checkpoint_lock(
                    ticker_text,
                    side_text,
                    order_type_text,
                    quantity,
                    price,
                    account_fingerprint,
                    account_fingerprint_key_available,
                    risk_reference_price,
                    trusted_market_quote,
                    market_quote_preflight,
                )
            except OSError:
                decision = self.denied(
                    "REVISION_LOCK_IO_ERROR",
                    "runtime safety revision lock could not be acquired or released",
                    ticker=ticker_text,
                    side=side_text,
                )
            finally:
                if locked:
                    try:
                        fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    except OSError:
                        decision = self.denied(
                            "REVISION_LOCK_IO_ERROR",
                            "runtime safety revision lock could not be acquired or released",
                            ticker=ticker_text,
                            side=side_text,
                        )
                if lock_fd is not None:
                    try:
                        os.close(lock_fd)
                    except OSError:
                        decision = self.denied(
                            "REVISION_LOCK_IO_ERROR",
                            "runtime safety revision lock could not be acquired or released",
                            ticker=ticker_text,
                            side=side_text,
                        )
            return decision

    def _authorize_under_checkpoint_lock(
        self,
        ticker_text,
        side_text,
        order_type_text,
        quantity,
        price,
        account_fingerprint,
        account_fingerprint_key_available,
        risk_reference_price,
        trusted_market_quote,
        market_quote_preflight,
    ):
            if self._ambiguous_halt_reason is not None:
                return self.denied(
                    "ORDER_SUBMISSION_AMBIGUOUS",
                    self._ambiguous_halt_reason,
                    shadow_only=False,
                    ticker=ticker_text,
                    side=side_text,
                )
            state, load_error = self._load_state()
            if load_error is not None:
                return load_error

            revision = state["revision"]
            context = {
                "shadow_only": state["shadow_only"],
                "revision": revision,
                "ticker": ticker_text,
                "side": side_text,
            }
            checkpoint_revision, checkpoint_error = self._load_checkpoint()
            if checkpoint_error is not None:
                return checkpoint_error
            highest_revision = max(self._highest_revision, checkpoint_revision or 0)
            if revision < highest_revision:
                return self.denied(
                    "REVISION_ROLLBACK",
                    "runtime safety revision moved backwards",
                    **context,
                )
            self._highest_revision = revision
            if revision > checkpoint_revision:
                checkpoint_error = self._store_checkpoint(revision)
                if checkpoint_error is not None:
                    return checkpoint_error

            if state["operator_halt"]:
                return self.denied("OPERATOR_HALT", state.get("reason") or "operator halt", **context)
            if not state["live_armed"]:
                return self.denied("LIVE_NOT_ARMED", "live order submission is not armed", **context)
            if state["shadow_only"]:
                return self.denied("SHADOW_ONLY", "shadow mode forbids KIS submission", **context)
            if ticker_text not in state["allowed_tickers"]:
                return self.denied("TICKER_NOT_ALLOWED", "ticker is not allow-listed", **context)
            if side_text not in _VALID_SIDES:
                return self.denied("INVALID_SIDE", "side must be BUY or SELL", **context)
            if not account_fingerprint_key_available:
                return self.denied(
                    "ACCOUNT_FINGERPRINT_KEY_UNAVAILABLE",
                    "account fingerprint HMAC key is missing or too short",
                    **context,
                )
            submitted_fingerprint = str(account_fingerprint or "").strip().lower()
            if submitted_fingerprint not in state["allowed_account_fingerprints"]:
                return self.denied(
                    "ACCOUNT_NOT_ALLOWED",
                    "account fingerprint is missing or not allow-listed",
                    **context,
                )

            try:
                order_quantity = self._decimal(quantity)
            except ValueError:
                return self.denied("INVALID_QUANTITY", "quantity is not a valid Decimal", **context)
            if order_quantity <= 0 or order_quantity != order_quantity.to_integral_value():
                return self.denied("INVALID_QUANTITY", "quantity must be a positive integer", **context)

            if order_type_text in _MARKET_ORDER_TYPES:
                try:
                    caller_price = self._decimal(risk_reference_price)
                except ValueError:
                    return self.denied(
                        "INVALID_RISK_REFERENCE_PRICE",
                        "market order requires a finite positive risk reference price",
                        **context,
                    )
                if caller_price <= 0:
                    return self.denied(
                        "INVALID_RISK_REFERENCE_PRICE",
                        "market order requires a finite positive risk reference price",
                        **context,
                    )
                if trusted_market_quote is None:
                    code = (
                        "MARKET_QUOTE_REQUIRED"
                        if market_quote_preflight
                        else "TRUSTED_MARKET_QUOTE_UNAVAILABLE"
                    )
                    return self.denied(
                        code,
                        "a fresh structured KIS quote is required for market risk",
                        **context,
                    )
                if not isinstance(trusted_market_quote, TrustedMarketQuote):
                    return self.denied(
                        "TRUSTED_MARKET_QUOTE_INVALID",
                        "trusted market quote has an invalid structure",
                        **context,
                    )
                quote = trusted_market_quote
                if str(quote.source or "").strip().upper() != "KIS":
                    return self.denied(
                        "TRUSTED_MARKET_QUOTE_SOURCE_INVALID",
                        "trusted market quote source is not KIS",
                        **context,
                    )
                if str(quote.ticker or "").strip().upper() != ticker_text:
                    return self.denied(
                        "TRUSTED_MARKET_QUOTE_TICKER_MISMATCH",
                        "trusted market quote ticker does not match the order",
                        **context,
                    )
                try:
                    quote_price = self._decimal(quote.price)
                except ValueError:
                    return self.denied(
                        "TRUSTED_MARKET_QUOTE_PRICE_INVALID",
                        "trusted market quote price must be finite and positive",
                        **context,
                    )
                if quote_price <= 0:
                    return self.denied(
                        "TRUSTED_MARKET_QUOTE_PRICE_INVALID",
                        "trusted market quote price must be finite and positive",
                        **context,
                    )
                if (
                    not isinstance(quote.as_of, datetime)
                    or quote.as_of.tzinfo is None
                    or quote.as_of.utcoffset() != timezone.utc.utcoffset(None)
                ):
                    return self.denied(
                        "TRUSTED_MARKET_QUOTE_TIMESTAMP_INVALID",
                        "trusted market quote timestamp must be timezone-aware UTC",
                        **context,
                    )
                now = self._clock()
                if (
                    not isinstance(now, datetime)
                    or now.tzinfo is None
                    or now.utcoffset() != timezone.utc.utcoffset(None)
                ):
                    return self.denied(
                        "TRUSTED_MARKET_QUOTE_TIMESTAMP_INVALID",
                        "runtime quote clock must be timezone-aware UTC",
                        **context,
                    )
                age_seconds = Decimal(str((now - quote.as_of).total_seconds()))
                if age_seconds < -_MAX_QUOTE_FUTURE_SKEW_SECONDS:
                    return self.denied(
                        "TRUSTED_MARKET_QUOTE_FROM_FUTURE",
                        "trusted market quote timestamp exceeds future-skew tolerance",
                        **context,
                    )
                if age_seconds > state["market_quote_max_age_seconds"]:
                    return self.denied(
                        "TRUSTED_MARKET_QUOTE_STALE",
                        "trusted market quote exceeds configured maximum age",
                        **context,
                    )
                conservative_price = max(caller_price, quote_price) * (
                    Decimal("1")
                    + state["market_slippage_buffer_percent"] / Decimal("100")
                )
                order_price = conservative_price.quantize(
                    Decimal("0.01"), rounding=ROUND_CEILING
                )
            else:
                try:
                    order_price = self._decimal(price)
                except ValueError:
                    return self.denied("INVALID_NOTIONAL", "price is not a valid Decimal", **context)
            notional = order_quantity * order_price
            amount_context = {**context, "quantity": order_quantity, "notional": notional}
            if order_price <= 0 or notional <= 0:
                return self.denied("INVALID_NOTIONAL", "order notional must be positive", **amount_context)
            if order_quantity > state["max_order_quantity"]:
                return self.denied(
                    "QUANTITY_LIMIT_EXCEEDED",
                    "order quantity exceeds the configured limit",
                    **amount_context,
                )
            if notional > state["max_order_notional"]:
                return self.denied(
                    "NOTIONAL_LIMIT_EXCEEDED",
                    "order notional exceeds the configured limit",
                    **amount_context,
                )

            return SafetyDecision(
                code="LIVE_AUTHORIZED",
                reason="live order submission authorized",
                can_submit=True,
                **amount_context,
            )


def safety_block_result(decision):
    """Return a broker-compatible, structured fail-closed result."""
    result = {
        "rt_cd": "999",
        "msg1": f"runtime safety blocked order: {decision.code}",
        "odno": "",
        "shadow": decision.shadow_only,
        "safety_decision": decision.as_dict(),
    }
    if decision.code == "ORDER_SUBMISSION_AMBIGUOUS":
        result["halt_required"] = True
        result["reconciliation_required"] = True
    return result


def order_submission_ambiguous_result(reason, *, ticker="", side=""):
    """Return the common no-order-number HALT/reconciliation contract."""
    decision = RuntimeSafetyGate.denied(
        "ORDER_SUBMISSION_AMBIGUOUS",
        str(reason or "order submission status is unknown"),
        shadow_only=False,
        ticker=str(ticker or "").strip().upper(),
        side=str(side or "").strip().upper(),
    )
    result = safety_block_result(decision)
    result["halt_required"] = True
    result["reconciliation_required"] = True
    return result


def shadow_record_failure_decision(decision, error):
    """Convert recorder failures to one stable fail-closed decision contract."""
    code = getattr(error, "code", "SHADOW_INTENT_RECORD_FAILED")
    reason = (
        "shadow idempotency key conflicts with an existing intent"
        if code == "IDEMPOTENCY_CONFLICT"
        else "shadow intent could not be durably recorded"
    )
    return RuntimeSafetyGate.denied(
        code,
        reason,
        shadow_only=True,
        revision=decision.revision,
        ticker=decision.ticker,
        side=decision.side,
    )
