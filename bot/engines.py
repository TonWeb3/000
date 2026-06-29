from typing import Dict, Any

# ─────────────────────────────────────────────────────────────────────────────
#  Entry engine: 5m HA trend + 1m HA momentum + Awesome Oscillator (5m & 1m) +
#  RSI(50) confirm the DIRECTION; a PRICE CAP on the Polymarket odds is the only
#  price gate (no EV / fair-probability gate). See decide_entry below.
# ─────────────────────────────────────────────────────────────────────────────


def _no_trade(reason: str) -> Dict[str, Any]:
    return {"action": "NO_TRADE", "side": None, "phase": "TREND", "strength": "-", "reason": reason}


def decide_entry(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Trend (5m HA) + momentum (1m HA) + AO (5m & 1m) + RSI(50) confirm the DIRECTION;
    a PRICE CAP on the Polymarket odds is the only price gate. There is NO EV /
    fair-probability gate on entries.

    - 5m HA colour = the trend. Red -> only DOWN, green -> only UP.
    - 1m HA must be the SAME colour as the 5m (momentum, colour only — no streak).
    - Awesome Oscillator confirms by BAR COLOUR on BOTH 5m and 1m: green = rising bar
      (diff > 0), red = falling/flat (diff <= 0). UP needs both AO green, DOWN both red.
    - RSI(14) confirms at the 50 line: >= 50 = uptrend (UP), < 50 = downtrend (DOWN).
    - PRICE CAP: the chosen side's Polymarket ask price must be BELOW `maxPrice`
      (default 0.60) — only buy when the odds are cheap enough.

    All gates are EQUAL and MANDATORY; none overrides another.
    """
    ha5 = inputs.get("ha5Color")          # "green" / "red" / None  (trend)
    ha1 = inputs.get("ha1Color")          # "green" / "red" / None  (momentum, colour only)
    price_up = inputs.get("priceUp")
    price_down = inputs.get("priceDown")
    max_price = inputs.get("maxPrice", 0.60)

    if price_up is None or price_down is None:
        return _no_trade("missing_prices")

    # ── TREND (5m HA) ──
    if ha5 not in ("green", "red"):
        return _no_trade("no_5m_trend")
    # ── MOMENTUM (1m HA aligned with the 5m trend — colour only, no streak) ──
    if ha1 != ha5:
        return _no_trade("momentum_not_aligned")

    side = "UP" if ha5 == "green" else "DOWN"
    price = price_up if side == "UP" else price_down

    # ── AWESOME OSCILLATOR confirmation (5m + 1m) by BAR COLOUR — REQUIRED ──
    # Standard AO histogram: green = rising bar (diff > 0), red = falling/flat (diff <= 0).
    # Both timeframes must match the side, exactly like the HA colour does.
    ao5 = inputs.get("ao5")  # "green" / "red" / None
    ao1 = inputs.get("ao1")  # "green" / "red" / None
    if ao5 is None or ao1 is None:
        return _no_trade("ao_unavailable")
    if side == "UP":
        if ao5 != "green":
            return _no_trade("ao5_not_green")
        if ao1 != "green":
            return _no_trade("ao1_not_green")
    else:  # DOWN
        if ao5 != "red":
            return _no_trade("ao5_not_red")
        if ao1 != "red":
            return _no_trade("ao1_not_red")

    # ── RSI trend confirmation at the 50 line (>=50 up, <50 down) — REQUIRED ──
    rsi = inputs.get("rsi")
    if rsi is None:
        return _no_trade("rsi_unavailable")
    if side == "UP" and rsi < 50:
        return _no_trade(f"rsi_{rsi:.0f}_not_uptrend")
    if side == "DOWN" and rsi >= 50:
        return _no_trade(f"rsi_{rsi:.0f}_not_downtrend")

    # ── PRICE CAP (replaces EV): only enter when the odds are below the cap ──
    if price is None:
        return _no_trade("no_price")
    if price >= max_price:
        return _no_trade(f"price_{price:.2f}_above_{max_price:.2f}")

    strength = "HIGH_CONVICTION" if price <= 0.50 else "STRONG"
    return {
        "action": "ENTER", "side": side, "phase": "TREND", "strength": strength,
        "price": price, "reason": "trend_confirmed"
    }
