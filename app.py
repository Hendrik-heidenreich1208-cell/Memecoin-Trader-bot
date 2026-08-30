import os
import time
import asyncio
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request, HTTPException

app = FastAPI(title="Memecoin V11 Opportunity Score Bot")

BUILD_VERSION = "FIXED-2026-08-30-V12-INSTANT-BUY-ALERT"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

tracked_wallets_env = os.getenv("TRACKED_WALLETS", "")
TRACKED_WALLETS = {
    w.strip()
    for w in tracked_wallets_env.split(",")
    if w.strip()
}

# ------------------------------------------------------------
# V11: ONE STRONG TRADER IS ENOUGH
# The 15 tracked wallets are treated as curated strong traders.
# A signal is sent only when the market score is high enough.
# ------------------------------------------------------------

MIN_SCORE = int(os.getenv("MIN_SCORE", "72"))

MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "35000"))
MIN_VOLUME_M5_USD = float(os.getenv("MIN_VOLUME_M5_USD", "12000"))
MIN_M5_BUYS = int(os.getenv("MIN_M5_BUYS", "7"))
MIN_BUY_SELL_RATIO = float(os.getenv("MIN_BUY_SELL_RATIO", "1.25"))

MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD", "60000"))
MAX_MARKET_CAP_USD = float(os.getenv("MAX_MARKET_CAP_USD", "12000000"))

# Avoid entering after an extreme 5-minute pump.
MAX_PRICE_CHANGE_M5 = float(os.getenv("MAX_PRICE_CHANGE_M5", "35"))

# Prefer coins that are not literally seconds old, but still early.
MIN_PAIR_AGE_MINUTES = float(os.getenv("MIN_PAIR_AGE_MINUTES", "2"))
MAX_PAIR_AGE_HOURS = float(os.getenv("MAX_PAIR_AGE_HOURS", "72"))

# Basic liquidity-vs-market-cap sanity rule.
MAX_MC_TO_LIQ_RATIO = float(os.getenv("MAX_MC_TO_LIQ_RATIO", "80"))

ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "120"))

WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
QUOTE_MINTS = {WSOL_MINT, USDC_MINT, USDT_MINT}

_seen_signatures: Dict[str, float] = {}
_alerted_mints: Dict[str, float] = {}
SEEN_TTL_SECONDS = 7200

STATS = {
    "webhook_requests": 0,
    "transactions_received": 0,
    "swap_events_received": 0,
    "tracked_wallet_matches": 0,
    "real_buys_detected": 0,
    "market_checks": 0,
    "signals_sent": 0,
    "filtered_low_score": 0,
    "filtered_hard_rule": 0,
    "telegram_rate_limits": 0,
    "market_api_failures": 0,
    "ignored_non_swap": 0,
    "ignored_not_buy": 0,
    "last_trader": None,
    "last_signature": None,
    "last_buy_mint": None,
    "last_score": None,
    "last_grade": None,
    "last_filter_reason": None,
    "last_market_data": None,
    "last_error": None,
}


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def raw_token_amount(item: Dict[str, Any]) -> float:
    raw = item.get("rawTokenAmount")
    if isinstance(raw, dict):
        token_amount = safe_float(raw.get("tokenAmount"))
        decimals = int(safe_float(raw.get("decimals")))
        try:
            return token_amount / (10 ** decimals)
        except Exception:
            return token_amount

    for key in ("tokenAmount", "amount", "uiAmount"):
        if key in item:
            return safe_float(item.get(key))

    return 0.0


def cleanup_state() -> None:
    now = time.time()

    for signature, ts in list(_seen_signatures.items()):
        if now - ts > SEEN_TTL_SECONDS:
            _seen_signatures.pop(signature, None)

    cooldown = ALERT_COOLDOWN_MINUTES * 60
    for mint, ts in list(_alerted_mints.items()):
        if now - ts > cooldown:
            _alerted_mints.pop(mint, None)


def normalize_events(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]

    if isinstance(payload, dict):
        for key in ("data", "events", "transactions", "result"):
            value = payload.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]
            if isinstance(value, dict):
                return [value]
        return [payload]

    return []


def walk(obj: Any):
    yield obj
    if isinstance(obj, dict):
        for value in obj.values():
            yield from walk(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from walk(value)


def find_tracked_wallet(event: Dict[str, Any]) -> Optional[str]:
    fee_payer = event.get("feePayer")
    if fee_payer in TRACKED_WALLETS:
        return fee_payer

    for node in walk(event):
        if isinstance(node, str) and node in TRACKED_WALLETS:
            return node

    return None


def find_signature(event: Dict[str, Any]) -> Optional[str]:
    value = event.get("signature")
    if value:
        return str(value)

    for node in walk(event):
        if isinstance(node, dict):
            for key in ("signature", "transactionSignature", "txSignature"):
                value = node.get(key)
                if value:
                    return str(value)

    return None


def is_swap_event(event: Dict[str, Any]) -> bool:
    if str(event.get("type", "")).upper() == "SWAP":
        return True

    events = event.get("events")
    return isinstance(events, dict) and isinstance(events.get("swap"), dict)


def detect_buy_from_swap_event(
    event: Dict[str, Any],
    trader_wallet: str,
) -> Optional[Tuple[str, float]]:
    events = event.get("events")
    if not isinstance(events, dict):
        return None

    swap = events.get("swap")
    if not isinstance(swap, dict):
        return None

    token_inputs = swap.get("tokenInputs") or []
    token_outputs = swap.get("tokenOutputs") or []
    native_input = swap.get("nativeInput") or {}

    quote_spent = False

    if isinstance(native_input, dict) and native_input.get("account") == trader_wallet:
        if safe_float(native_input.get("amount")) > 0:
            quote_spent = True

    for item in token_inputs:
        if not isinstance(item, dict):
            continue
        if item.get("userAccount") == trader_wallet and item.get("mint") in QUOTE_MINTS:
            quote_spent = True

    outputs: List[Tuple[str, float]] = []

    for item in token_outputs:
        if not isinstance(item, dict):
            continue
        if item.get("userAccount") != trader_wallet:
            continue

        mint = item.get("mint")
        if not mint or mint in QUOTE_MINTS:
            continue

        outputs.append((str(mint), raw_token_amount(item)))

    if quote_spent and outputs:
        outputs.sort(key=lambda x: x[1], reverse=True)
        return outputs[0]

    return None


def transfer_token_amount(transfer: Dict[str, Any]) -> float:
    value = transfer.get("tokenAmount")
    if isinstance(value, (int, float, str)):
        return safe_float(value)

    if isinstance(value, dict):
        for key in ("uiAmount", "uiAmountString", "amount"):
            if key in value:
                return safe_float(value.get(key))

    for key in ("amount", "uiAmount"):
        if key in transfer:
            return safe_float(transfer.get(key))

    return 0.0


def get_mint(transfer: Dict[str, Any]) -> Optional[str]:
    value = transfer.get("mint") or transfer.get("tokenMint") or transfer.get("mintAddress")
    return str(value) if value else None


def get_from(transfer: Dict[str, Any]) -> Optional[str]:
    value = transfer.get("fromUserAccount") or transfer.get("from") or transfer.get("sourceOwner")
    return str(value) if value else None


def get_to(transfer: Dict[str, Any]) -> Optional[str]:
    value = transfer.get("toUserAccount") or transfer.get("to") or transfer.get("destinationOwner")
    return str(value) if value else None


def detect_buy_from_transfers(
    event: Dict[str, Any],
    trader_wallet: str,
) -> Optional[Tuple[str, float]]:
    token_transfers = event.get("tokenTransfers") or []
    native_transfers = event.get("nativeTransfers") or []

    net_by_mint: Dict[str, float] = {}
    quote_spent = False

    for transfer in token_transfers:
        if not isinstance(transfer, dict):
            continue

        mint = get_mint(transfer)
        if not mint:
            continue

        amount = transfer_token_amount(transfer)
        if amount <= 0:
            continue

        from_wallet = get_from(transfer)
        to_wallet = get_to(transfer)

        if to_wallet == trader_wallet:
            net_by_mint[mint] = net_by_mint.get(mint, 0.0) + amount

        if from_wallet == trader_wallet:
            net_by_mint[mint] = net_by_mint.get(mint, 0.0) - amount
            if mint in QUOTE_MINTS:
                quote_spent = True

    for transfer in native_transfers:
        if isinstance(transfer, dict):
            if get_from(transfer) == trader_wallet and safe_float(transfer.get("amount")) > 0:
                quote_spent = True
                break

    if not quote_spent:
        return None

    candidates = [
        (mint, amount)
        for mint, amount in net_by_mint.items()
        if mint not in QUOTE_MINTS and amount > 0
    ]

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0]


def detect_real_buy(
    event: Dict[str, Any],
    trader_wallet: str,
) -> Optional[Tuple[str, float]]:
    buy = detect_buy_from_swap_event(event, trader_wallet)
    if buy:
        return buy
    return detect_buy_from_transfers(event, trader_wallet)


async def fetch_market_data(mint: str) -> Optional[Dict[str, Any]]:
    url = f"https://api.dexscreener.com/token-pairs/v1/solana/{mint}"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(url, headers={"Accept": "application/json"})

        if response.status_code != 200:
            STATS["market_api_failures"] += 1
            STATS["last_error"] = f"DexScreener HTTP {response.status_code}"
            return None

        data = response.json()
        pairs = data.get("pairs") if isinstance(data, dict) else data if isinstance(data, list) else []

        solana_pairs = [
            pair for pair in (pairs or [])
            if isinstance(pair, dict)
            and str(pair.get("chainId", "")).lower() == "solana"
        ]

        if not solana_pairs:
            return None

        solana_pairs.sort(
            key=lambda pair: safe_float((pair.get("liquidity") or {}).get("usd")),
            reverse=True,
        )

        pair = solana_pairs[0]
        txns_m5 = (pair.get("txns") or {}).get("m5") or {}

        market_cap = safe_float(pair.get("marketCap"))
        if market_cap <= 0:
            market_cap = safe_float(pair.get("fdv"))

        created_ms = safe_float(pair.get("pairCreatedAt"))
        age_minutes = 0.0
        if created_ms > 0:
            age_minutes = max(0.0, (time.time() - created_ms / 1000) / 60)

        base_token = pair.get("baseToken") or {}

        return {
            "name": base_token.get("name") or "Unbekannt",
            "symbol": base_token.get("symbol") or "?",
            "liquidity_usd": safe_float((pair.get("liquidity") or {}).get("usd")),
            "volume_m5_usd": safe_float((pair.get("volume") or {}).get("m5")),
            "buys_m5": int(safe_float(txns_m5.get("buys"))),
            "sells_m5": int(safe_float(txns_m5.get("sells"))),
            "price_change_m5": safe_float((pair.get("priceChange") or {}).get("m5")),
            "market_cap_usd": market_cap,
            "pair_age_minutes": age_minutes,
            "dex": pair.get("dexId") or "?",
        }

    except Exception as exc:
        STATS["market_api_failures"] += 1
        STATS["last_error"] = f"DexScreener error: {str(exc)[:200]}"
        return None


def hard_filter(market: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    if not market:
        return False, "Keine Marktdaten"

    liquidity = market["liquidity_usd"]
    volume = market["volume_m5_usd"]
    buys = market["buys_m5"]
    sells = market["sells_m5"]
    ratio = buys / max(sells, 1)
    mc = market["market_cap_usd"]
    age_min = market["pair_age_minutes"]
    change = market["price_change_m5"]

    if liquidity < MIN_LIQUIDITY_USD:
        return False, f"LiquiditÃ¤t zu niedrig (${liquidity:,.0f})"
    if volume < MIN_VOLUME_M5_USD:
        return False, f"5m-Volumen zu niedrig (${volume:,.0f})"
    if buys < MIN_M5_BUYS:
        return False, f"Zu wenige KÃ¤ufe ({buys})"
    if ratio < MIN_BUY_SELL_RATIO:
        return False, f"Buy/Sell zu schwach ({ratio:.2f}x)"
    if mc <= 0:
        return False, "Market Cap unbekannt"
    if mc < MIN_MARKET_CAP_USD:
        return False, f"Market Cap zu klein (${mc:,.0f})"
    if mc > MAX_MARKET_CAP_USD:
        return False, f"Market Cap zu groÃ (${mc:,.0f})"
    if change > MAX_PRICE_CHANGE_M5:
        return False, f"Schon zu stark gepumpt (+{change:.1f}% / 5m)"
    if age_min > 0 and age_min < MIN_PAIR_AGE_MINUTES:
        return False, f"Pair extrem neu ({age_min:.1f} Min.)"
    if age_min > MAX_PAIR_AGE_HOURS * 60:
        return False, f"Pair zu alt ({age_min/60:.1f} Std.)"
    if liquidity > 0 and mc / liquidity > MAX_MC_TO_LIQ_RATIO:
        return False, f"MarketCap/LiquiditÃ¤t zu hoch ({mc/liquidity:.1f}x)"

    return True, "PASS"


def calculate_score(market: Dict[str, Any]) -> Tuple[int, str]:
    """
    0-100 Opportunity Score.
    One tracked strong trader is already the trigger.
    Market structure decides whether the alert is good enough.
    """
    score = 30  # strong tracked trader signal

    liquidity = market["liquidity_usd"]
    volume = market["volume_m5_usd"]
    buys = market["buys_m5"]
    sells = market["sells_m5"]
    ratio = buys / max(sells, 1)
    change = market["price_change_m5"]
    mc = market["market_cap_usd"]
    age_min = market["pair_age_minutes"]

    # Liquidity: max +18
    if liquidity >= 150000:
        score += 18
    elif liquidity >= 80000:
        score += 15
    elif liquidity >= 50000:
        score += 12
    else:
        score += 8

    # 5m volume: max +14
    if volume >= 100000:
        score += 14
    elif volume >= 50000:
        score += 12
    elif volume >= 25000:
        score += 9
    else:
        score += 6

    # Buy pressure: max +14
    if ratio >= 3.0:
        score += 14
    elif ratio >= 2.0:
        score += 11
    elif ratio >= 1.5:
        score += 8
    else:
        score += 5

    # Momentum sweet spot: max +12
    # We reward positive movement, but not a huge chase.
    if 3 <= change <= 15:
        score += 12
    elif 1 <= change < 3:
        score += 9
    elif 15 < change <= 25:
        score += 7
    elif 25 < change <= MAX_PRICE_CHANGE_M5:
        score += 3
    elif change < 0:
        score += 1

    # Market cap: max +6
    if 100000 <= mc <= 3000000:
        score += 6
    elif 60000 <= mc <= 6000000:
        score += 4
    else:
        score += 2

    # Pair age: max +6
    if 5 <= age_min <= 360:
        score += 6
    elif 2 <= age_min < 5:
        score += 4
    elif 360 < age_min <= 1440:
        score += 3
    elif age_min == 0:
        score += 2

    score = min(score, 100)

    if score >= 88:
        grade = "ð¥ VERY STRONG"
    elif score >= 80:
        grade = "ð STRONG"
    elif score >= MIN_SCORE:
        grade = "â¡ EARLY GOOD"
    else:
        grade = "WATCH"

    return score, grade


async def send_instant_buy_alert(
    mint: str,
    trader_wallet: str,
) -> bool:
    """
    V12 core behavior:
    As soon as a real BUY by any tracked wallet is detected,
    send Telegram immediately. No DexScreener request first.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        STATS["last_error"] = "Telegram-Konfiguration fehlt."
        return False

    short_wallet = (
        f"{trader_wallet[:5]}...{trader_wallet[-5:]}"
        if len(trader_wallet) > 12
        else trader_wallet
    )

    message = (
        "ð¨ INSTANT BUY ALERT\n\n"
        "ð¤ Einer deiner beobachteten Trader hat gerade gekauft.\n"
        f"ð Wallet: {short_wallet}\n\n"
        "ðª CONTRACT ADDRESS (CA):\n"
        f"{mint}\n\n"
        "ð Unten auf âCA kopierenâ tippen und in Phantom prÃ¼fen.\n\n"
        "â ï¸ Das ist eine KaufaktivitÃ¤ts-Meldung, keine Gewinnprognose. "
        "CA, LiquiditÃ¤t, Preis und Slippage vor einem Kauf selbst prÃ¼fen."
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
        "reply_markup": {
            "inline_keyboard": [[{
                "text": "ð CA kopieren",
                "copy_text": {"text": mint},
            }]]
        },
    }

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    async with httpx.AsyncClient(timeout=20) as client:
        for attempt in range(2):
            try:
                response = await client.post(url, json=payload)
            except Exception as exc:
                STATS["last_error"] = f"Telegram network error: {str(exc)[:200]}"
                return False

            if response.status_code == 429:
                STATS["telegram_rate_limits"] += 1
                try:
                    retry_after = int(
                        response.json().get("parameters", {}).get("retry_after", 2)
                    )
                except Exception:
                    retry_after = 2

                if attempt == 0:
                    await asyncio.sleep(min(max(retry_after, 1), 5))
                    continue

                STATS["last_error"] = "Telegram 429 Too Many Requests"
                return False

            if response.is_error:
                STATS["last_error"] = (
                    f"Telegram HTTP {response.status_code}: {response.text[:200]}"
                )
                return False

            STATS["last_error"] = None
            return True

    return False


async def send_telegram_signal(
    mint: str,
    market: Dict[str, Any],
    score: int,
    grade: str,
) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        STATS["last_error"] = "Telegram-Konfiguration fehlt."
        return False

    buys = market["buys_m5"]
    sells = market["sells_m5"]
    ratio = buys / max(sells, 1)
    age_min = market["pair_age_minutes"]

    age_text = (
        f"{age_min:.0f} Min."
        if age_min < 120
        else f"{age_min/60:.1f} Std."
    )

    message = (
        f"{grade} â {score}/100\n\n"
        "ð¤ 1 starker beobachteter Trader hat gekauft\n"
        "â Kein Warten auf einen zweiten Trader\n\n"
        f"ðª {market['name']} ({market['symbol']})\n"
        f"â± Pair-Alter: {age_text}\n"
        f"ð§ LiquiditÃ¤t: ${market['liquidity_usd']:,.0f}\n"
        f"ð¥ Volumen 5m: ${market['volume_m5_usd']:,.0f}\n"
        f"ð¢ KÃ¤ufe 5m: {buys}\n"
        f"ð´ VerkÃ¤ufe 5m: {sells}\n"
        f"ð Buy/Sell: {ratio:.2f}x\n"
        f"â¡ Kurs 5m: {market['price_change_m5']:+.1f}%\n"
        f"ð° Market Cap: ${market['market_cap_usd']:,.0f}\n"
        f"ð¦ DEX: {market['dex']}\n\n"
        "ðª CONTRACT ADDRESS (CA):\n"
        f"{mint}\n\n"
        "â ï¸ Score ist nur ein Filter, keine Gewinnprognose. "
        "CA, LiquiditÃ¤t und Slippage vor einem Kauf prÃ¼fen."
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
        "reply_markup": {
            "inline_keyboard": [[{
                "text": "ð CA kopieren",
                "copy_text": {"text": mint},
            }]]
        },
    }

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    async with httpx.AsyncClient(timeout=20) as client:
        for attempt in range(2):
            try:
                response = await client.post(url, json=payload)
            except Exception as exc:
                STATS["last_error"] = f"Telegram network error: {str(exc)[:200]}"
                return False

            if response.status_code == 429:
                STATS["telegram_rate_limits"] += 1

                try:
                    retry_after = int(
                        response.json().get("parameters", {}).get("retry_after", 2)
                    )
                except Exception:
                    retry_after = 2

                if attempt == 0:
                    await asyncio.sleep(min(max(retry_after, 1), 5))
                    continue

                STATS["last_error"] = "Telegram 429 Too Many Requests"
                return False

            if response.is_error:
                STATS["last_error"] = (
                    f"Telegram HTTP {response.status_code}: {response.text[:200]}"
                )
                return False

            STATS["last_error"] = None
            return True

    return False


@app.get("/")
async def health():
    return {
        "ok": True,
        "version": BUILD_VERSION,
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
        "tracked_wallets": len(TRACKED_WALLETS),
        "mode": "INSTANT_BUY_ALERT",
        "min_score": MIN_SCORE,
    }


@app.get("/stats")
async def stats():
    return {
        "version": BUILD_VERSION,
        **STATS,
        "tracked_wallets": len(TRACKED_WALLETS),
        "filters": {
            "min_score": MIN_SCORE,
            "min_liquidity_usd": MIN_LIQUIDITY_USD,
            "min_volume_m5_usd": MIN_VOLUME_M5_USD,
            "min_m5_buys": MIN_M5_BUYS,
            "min_buy_sell_ratio": MIN_BUY_SELL_RATIO,
            "min_market_cap_usd": MIN_MARKET_CAP_USD,
            "max_market_cap_usd": MAX_MARKET_CAP_USD,
            "max_price_change_m5": MAX_PRICE_CHANGE_M5,
            "min_pair_age_minutes": MIN_PAIR_AGE_MINUTES,
            "max_pair_age_hours": MAX_PAIR_AGE_HOURS,
            "max_mc_to_liq_ratio": MAX_MC_TO_LIQ_RATIO,
        },
    }


@app.get("/test-telegram")
async def test_telegram():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {"ok": False, "error": "Telegram not configured"}

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": (
            "â V12 INSTANT BUY ALERT ist aktiv.\n"
            "ð¨ Jeder erkannte Kauf eines Ã¼berwachten Traders lÃ¶st sofort einen Alarm aus.\n"
            "ð CA-Kopierbutton ist aktiv."
        ),
    }

    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(url, json=payload)

    return {
        "ok": response.status_code == 200,
        "version": BUILD_VERSION,
        "telegram_status": response.status_code,
    }


@app.post("/helius")
async def helius_webhook(request: Request):
    STATS["webhook_requests"] += 1

    try:
        if WEBHOOK_SECRET:
            supplied = (
                request.headers.get("x-webhook-secret")
                or request.headers.get("authorization")
                or ""
            )
            if supplied != WEBHOOK_SECRET:
                raise HTTPException(status_code=401, detail="Bad webhook secret")

        payload = await request.json()
        events = normalize_events(payload)
        STATS["transactions_received"] += len(events)

        cleanup_state()
        alerts_sent = 0

        for event in events:
            if not is_swap_event(event):
                STATS["ignored_non_swap"] += 1
                continue

            STATS["swap_events_received"] += 1

            trader_wallet = find_tracked_wallet(event)
            if not trader_wallet:
                continue

            STATS["tracked_wallet_matches"] += 1
            STATS["last_trader"] = trader_wallet

            signature = find_signature(event)
            STATS["last_signature"] = signature

            dedupe_key = signature or f"{trader_wallet}:{hash(str(event))}"
            if dedupe_key in _seen_signatures:
                continue
            _seen_signatures[dedupe_key] = time.time()

            buy = detect_real_buy(event, trader_wallet)
            if not buy:
                STATS["ignored_not_buy"] += 1
                continue

            mint, _amount = buy
            STATS["real_buys_detected"] += 1
            STATS["last_buy_mint"] = mint

            # V12: INSTANT alert. No second trader, score or market filter can block it.
            # We deliberately send before calling any external market-data API.
            if mint in _alerted_mints:
                continue

            sent = await send_instant_buy_alert(
                mint=mint,
                trader_wallet=trader_wallet,
            )

            if sent:
                _alerted_mints[mint] = time.time()
                STATS["signals_sent"] += 1
                STATS["last_filter_reason"] = "INSTANT BUY ALERT SENT"
                alerts_sent += 1

        return {
            "ok": True,
            "version": BUILD_VERSION,
            "alerts_sent": alerts_sent,
        }

    except HTTPException:
        raise

    except Exception as exc:
        STATS["last_error"] = str(exc)[:500]
        return {
            "ok": False,
            "version": BUILD_VERSION,
            "error": "event_processing_failed",
        }
