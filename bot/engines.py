from typing import Dict, Any

# ─────────────────────────────────────────────────────────────────────────────
#  Entry engine: 5m HA trend + fresh 1m HA momentum + RSI(50) confirm pick the
#  DIRECTION; EV (fair vs market ask) finds the price. See decide_entry below.
# ─────────────────────────────────────────────────────────────────────────────


def _no_ev(reason: str) -> Dict[str, Any]:
    return {"action": "NO_TRADE", "side": None, "phase": "TREND_EV", "strength": "EV", "reason": reason}


def decide_entry(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Trend (5m HA) + fresh momentum (1m HA) set the DIRECTION; EV finds the price.

    - 5m HA colour = the trend. Red -> only DOWN, green -> only UP.
    - 1m HA must be the SAME colour (momentum aligned with the trend) and its streak
      must be FRESH: between freshMin and freshMax bars (a new move, 1..6 by default —
      not a stale, over-extended run).
    - RSI confirms the trend at the 50 line: >= 50 = uptrend (allows UP), < 50 =
      downtrend (allows DOWN).
    - Then EV on that side (fair - ask price) must clear evThreshold, i.e. the book is
      giving a good price to enter the trend. fair prob must also be >= minProb.

    HA, RSI(50) and EV are EQUAL, MANDATORY gates — all three must agree to enter; none
    overrides another. HA picks WHICH way, RSI(50) confirms it, EV picks WHEN/at what price.
    """
    ha5 = inputs.get("ha5Color")          # "green" / "red" / None  (trend)
    ha1 = inputs.get("ha1Color")          # "green" / "red" / None  (momentum)
    streak1 = inputs.get("ha1Streak") or 0
    p_up = inputs.get("mcProbUp")
    price_up = inputs.get("priceUp")
    price_down = inputs.get("priceDown")
    fresh_min = inputs.get("freshMin", 1)
    fresh_max = inputs.get("freshMax", 6)
    min_prob = inputs.get("minProb", 0.55)
    ev_threshold = inputs.get("evThreshold", 0.04)

    if p_up is None:
        return _no_ev("missing_model_data")
    if price_up is None or price_down is None:
        return _no_ev("missing_prices")

    # ── TREND (5m HA) ──
    if ha5 not in ("green", "red"):
        return _no_ev("no_5m_trend")
    # ── MOMENTUM (1m HA aligned + fresh) ──
    if ha1 != ha5:
        return _no_ev("momentum_not_aligned")
    if not (fresh_min <= streak1 <= fresh_max):
        return _no_ev(f"not_fresh_{streak1}")

    side = "UP" if ha5 == "green" else "DOWN"
    p = p_up if side == "UP" else (1.0 - p_up)
    price = price_up if side == "UP" else price_down
    ev = p - price

    # ── RSI trend confirmation at the 50 line (>=50 up, <50 down) — REQUIRED ──
    rsi = inputs.get("rsi")
    if rsi is None:
        return _no_ev("rsi_unavailable")
    if side == "UP" and rsi < 50:
        return _no_ev(f"rsi_{rsi:.0f}_not_uptrend")
    if side == "DOWN" and rsi >= 50:
        return _no_ev(f"rsi_{rsi:.0f}_not_downtrend")

    # ── EV gate: good price to enter the trend ──
    if p < min_prob:
        return _no_ev(f"prob_{p:.2f}_below_{min_prob:.2f}")
    if ev < ev_threshold:
        return _no_ev(f"ev_{ev:.3f}_below_{ev_threshold:.3f}")

    strength = "HIGH_CONVICTION" if p >= 0.70 else "STRONG"
    return {
        "action": "ENTER", "side": side, "phase": "TREND_EV", "strength": strength,
        "prob": p, "price": price, "ev": ev, "streak": streak1, "reason": "trend_momentum_ev"
    }
