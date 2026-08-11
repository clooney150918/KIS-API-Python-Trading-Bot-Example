"""Central fail-closed authorization for every live order submission."""

from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
import threading
from typing import Optional


DEFAULT_STATE_PATH = Path(__file__).resolve().parent / "data" / "runtime_safety.json"
_VALID_SIDES = frozenset({"BUY", "SELL"})


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

    def __init__(self, state_path=DEFAULT_STATE_PATH):
        self.state_path = Path(state_path)
        self._highest_revision = 0
        self._lock = threading.Lock()

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
            "max_order_quantity": max_quantity,
            "max_order_notional": max_notional,
        }, None

    def authorize(self, ticker, side, quantity, price):
        ticker_text = str(ticker or "").strip().upper()
        side_text = str(side or "").strip().upper()

        with self._lock:
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
            if revision < self._highest_revision:
                return self.denied(
                    "REVISION_ROLLBACK",
                    "runtime safety revision moved backwards",
                    **context,
                )
            self._highest_revision = revision

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

            try:
                order_quantity = self._decimal(quantity)
            except ValueError:
                return self.denied("INVALID_QUANTITY", "quantity is not a valid Decimal", **context)
            if order_quantity <= 0 or order_quantity != order_quantity.to_integral_value():
                return self.denied("INVALID_QUANTITY", "quantity must be a positive integer", **context)

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
