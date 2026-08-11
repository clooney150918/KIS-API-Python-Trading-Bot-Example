"""Central fail-closed authorization for every live order submission."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import os
import tempfile
import threading
from typing import Optional


DEFAULT_STATE_PATH = Path(__file__).resolve().parent / "data" / "runtime_safety.json"
DEFAULT_CHECKPOINT_PATH = Path(__file__).resolve().parent / "data" / "runtime_safety.revision.json"
_VALID_SIDES = frozenset({"BUY", "SELL"})
_MARKET_ORDER_TYPES = frozenset({"MARKET", "MOC", "MOO"})
_CHECKPOINT_LOCKS = {}
_CHECKPOINT_LOCKS_GUARD = threading.Lock()


def account_fingerprint(cano, product_code):
    """Return the canonical account identifier without exposing raw account data."""
    canonical = f"{str(cano or '').strip()}:{str(product_code or '').strip()}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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


class RuntimeSafetyGate:
    """Reload and validate the safety state for each authorization decision."""

    def __init__(self, state_path=DEFAULT_STATE_PATH, *, checkpoint_path=None):
        self.state_path = Path(state_path)
        if checkpoint_path is None:
            checkpoint_path = (
                DEFAULT_CHECKPOINT_PATH
                if self.state_path == DEFAULT_STATE_PATH
                else self.state_path.with_name("runtime_safety.revision.json")
            )
        self.checkpoint_path = Path(checkpoint_path)
        self._highest_revision = 0
        self._lock = threading.Lock()
        key = str(self.checkpoint_path.resolve())
        with _CHECKPOINT_LOCKS_GUARD:
            self._checkpoint_lock = _CHECKPOINT_LOCKS.setdefault(key, threading.Lock())

    @staticmethod
    def denied(code, reason, **context):
        return SafetyDecision(code=code, reason=reason, can_submit=False, **context)

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
        try:
            raw = self.state_path.read_text(encoding="utf-8")
        except (FileNotFoundError, OSError):
            return None, self.denied(
                "SAFETY_STATE_MISSING",
                "runtime safety state is missing or unreadable",
            )

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
            max_quantity = self._decimal(state.get("max_order_quantity"))
            max_notional = self._decimal(state.get("max_order_notional"))
            if max_quantity <= 0 or max_quantity != max_quantity.to_integral_value():
                raise ValueError("max_order_quantity must be a positive integer")
            if max_notional <= 0:
                raise ValueError("max_order_notional must be positive")
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

    def authorize(
        self,
        ticker,
        side,
        quantity,
        price,
        *,
        account_fingerprint=None,
        order_type="LIMIT",
        risk_reference_price=None,
    ):
        ticker_text = str(ticker or "").strip().upper()
        side_text = str(side or "").strip().upper()

        with self._lock, self._checkpoint_lock:
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

            order_type_text = str(order_type or "").strip().upper()
            if order_type_text in _MARKET_ORDER_TYPES:
                try:
                    order_price = self._decimal(risk_reference_price)
                except ValueError:
                    return self.denied(
                        "INVALID_RISK_REFERENCE_PRICE",
                        "market order requires a finite positive risk reference price",
                        **context,
                    )
                if order_price <= 0:
                    return self.denied(
                        "INVALID_RISK_REFERENCE_PRICE",
                        "market order requires a finite positive risk reference price",
                        **context,
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
    return {
        "rt_cd": "999",
        "msg1": f"runtime safety blocked order: {decision.code}",
        "odno": "",
        "shadow": decision.code == "SHADOW_ONLY",
        "safety_decision": decision.as_dict(),
    }
