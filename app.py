import os
import time
import asyncio
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request, HTTPException

app = FastAPI(title="Memecoin High-Potential Buy Alert Bot")

BUILD_VERSION = "FIXED-2026-08-30-V9-HIGH-POTENTIAL"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

tracked_wallets_env = os.getenv("TRACKED_WALLETS", "")
TRACKED_WALLETS: Dict[str, Dict[str, Any]] = {}

for i, wallet in enumerate(tracked_wallets_env.split(","), start=1):
    wallet = wallet.strip()
    if wallet:
        TRACKED_WALLETS[wallet] = {"name": f"Trader {i}"}

# ---------- HIGH-POTENTIAL FILTER ----------
# These can later be changed in Render Environment without editing code.
MIN_TRADERS = int(os.getenv("MIN_TRADERS", "2"))
TRADER_WINDOW_MINUTES = int(os.getenv("TRADER_WINDOW_MINUTES", "15"))

MIN_LIQUIDITY_USD = float(os.getenv("MIN_LIQUIDITY_USD", "50000"))
MIN_VOLUME_M5_USD = float(os.getenv("MIN_VOLUME_M5_USD", "20000"))
MIN_M5_BUYS = int(os.getenv("MIN_M5_BUYS", "10"))
MIN_BUY_SELL_RATIO = float(os.getenv("MIN_BUY_SELL_RATIO", "1.5"))
MIN_PRICE_CHANGE_M5 = float(os.getenv("MIN_PRICE_CHANGE_M5", "3"))

MIN_MARKET_CAP_USD = float(os.getenv("MIN_MARKET_CAP_USD", "100000"))
MAX_MARKET_CAP_USD = float(os.getenv("MAX_MARKET_CAP_USD", "10000000"))

ALERT_COOLDOWN_MINUTES = int(os.getenv("ALERT_COOLDOWN_MINUTES", "120"))

WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"

QUOTE_MINTS = {WSOL_MINT, USDC_MINT, USDT_MINT}

_seen_signatures: Dict[str, float] = {}
_buy_signals: Dict[str, List[Tuple[str, float]]] = {}
_alerted_mints: Dict[str, float] = {}

SEEN_TTL_SECONDS = 7200

STATS = {
    "webhook_requests": 0,
    "transactions_received": 0,
    "tracked_wallet_matches": 0,
    "real_buys_detected": 0,
    "multi_trader_candidates": 0,
    "market_checks": 0,
    "high_potential_passes": 0,
    "telegram_alerts_sent": 0,
    "telegram_rate_limits": 0,
    "filtered_not_enough_traders": 0,
    "filtered_market_conditions": 0,
    "last_trader": None,
    "last_signature": None,
    "last_buy_mint": None,
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


def cleanup_state() -> None:
    now = time.time()

    for signature, timestamp in list(_seen_signatures.items()):
        if now - timestamp > SEEN_TTL_SECONDS:
            _seen_signatures.pop(signature, None)

    window_seconds = TRADER_WINDOW_MINUTES * 60

    for mint, signals in list(_buy_signals.items()):
        recent = [
            (wallet, ts)
            for wallet, ts in signals
            if now - ts <= window_seconds
        ]
        if recent:
            _buy_signals[mint] = recent
        else:
            _buy_signals.pop(mint, None)

    cooldown_seconds = ALERT_COOLDOWN_MINUTES * 60

    for mint, timestamp in list(_alerted_mints.items()):
        if now - timestamp > cooldown_seconds:
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
    tracked = set(TRACKED_WALLETS.keys())

    for node in walk(event):
        if isinstance(node, str) and node in tracked:
            return node

    return None


def find_signature(event: Dict[str, Any]) -> Optional[str]:
    for node in walk(event):
        if not isinstance(node, dict):
            continue

        for key in ("signature", "transactionSignature", "txSignature"):
            value = node.get(key)
            if value:
                return str(value)

    return None


def token_amount(transfer: Dict[str, Any]) -> float:
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
    value = (
        transfer.get("mint")
        or transfer.get("tokenMint")
        or transfer.get("mintAddress")
    )
    return str(value) if value else None


def get_from(transfer: Dict[str, Any]) -> Optional[str]:
    value = (
        transfer.get("fromUserAccount")
        or transfer.get("from")
        or transfer.get("sourceOwner")
    )
    return str(value) if value else None


def get_to(transfer: Dict[str, Any]) -> Optional[str]:
    value = (
        transfer.get("toUserAccount")
        or transfer.get("to")
        or transfer.get("destinationOwner")
    )
    return str(value) if value else None


def collect_token_transfers(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    transfers: List[Dict[str, Any]] = []

    for node in walk(event):
        if not isinstance(node, dict):
            continue

        mint = get_mint(node)
        if not mint:
            continue

        if get_from(node) or get_to(node):
            transfers.append(node)

    result = []
    seen = set()

    for transfer in transfers:
        marker = (
            get_mint(transfer),
            get_from(transfer),
            get_to(transfer),
            str(transfer.get("tokenAmount")),
            str(transfer.get("amount")),
        )

        if marker not in seen:
            seen.add(marker)
            result.append(transfer)

    return result


def collect_native_transfers(event: Dict[str, Any]) -> List[Dict[str, Any]]:
    transfers: List[Dict[str, Any]] = []

    for node in walk(event):
        if not isinstance(node, dict):
            continue

        if get_mint(node):
            continue

        if not (get_from(node) and get_to(node)):
            continue

        if "amount" not in node:
            continue

        transfers.append(node)

    return transfers


def detect_real_buy(
    event: Dict[str, Any],
    trader_wallet: str,
) -> Optional[Tuple[str, float]]:
    """
    BUY only:
    - tracked wallet receives a non-quote token
    - same transaction shows tracked wallet spending SOL/WSOL/USDC/USDT
    """
    token_transfers = collect_token_transfers(event)
    native_transfers = collect_native_transfers(event)

    net_by_mint: Dict[str, float] = {}
    quote_spent = False

    for transfer in token_transfers:
        mint = get_mint(transfer)
        if not mint:
            continue

        amount = token_amount(transfer)
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
        if get_from(transfer) == trader_wallet:
            if safe_float(transfer.get("amount")) > 0:
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

    candidates.sort(key=lambda item: item[1], reverse=True)
    return candidates[0]


def register_buy_signal(mint: str, trader_wallet: str) -> int:
    now = time.time()
    window_seconds = TRADER_WINDOW_MINUTES * 60

    signals = _buy_signals.setdefault(mint, [])

    # Keep only recent buys.
    signals[:] = [
        (wallet, ts)
        for wallet, ts in signals
        if now - ts <= window_seconds
    ]

    # One signal per trader per token within the window.
    if not any(wallet == trader_wallet for wallet, _ in signals):
        signals.append((trader_wallet, now))

    return len({wallet for wallet, _ in signals})


async def fetch_market_data(mint: str) -> Optional[Dict[str, Any]]:
    """
    Uses DexScreener's public token endpoint and selects the Solana pair
    with the highest USD liquidity.
    """
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"

    async with httpx.AsyncClient(timeout=12) as client:
        response = await client.get(url)

    if response.status_code != 200:
        STATS["last_error"] = (
            f"Market API HTTP {response.status_code}"
        )
        return None

    data = response.json()
    pairs = data.get("pairs") or []

    solana_pairs = [
        pair
        for pair in pairs
        if str(pair.get("chainId", "")).lower() == "solana"
    ]

    if not solana_pairs:
        return None

    solana_pairs.sort(
        key=lambda pair: safe_float(
            (pair.get("liquidity") or {}).get("usd")
        ),
        reverse=True,
    )

    pair = solana_pairs[0]

    txns_m5 = (pair.get("txns") or {}).get("m5") or {}
    volume_m5 = (pair.get("volume") or {}).get("m5")
    price_change_m5 = (pair.get("priceChange") or {}).get("m5")

    liquidity = safe_float(
        (pair.get("liquidity") or {}).get("usd")
    )
    volume = safe_float(volume_m5)
    buys = int(safe_float(txns_m5.get("buys")))
    sells = int(safe_float(txns_m5.get("sells")))
    price_change = safe_float(price_change_m5)

    market_cap = safe_float(pair.get("marketCap"))
    if market_cap <= 0:
        market_cap = safe_float(pair.get("fdv"))

    base_token = pair.get("baseToken") or {}

    return {
        "name": base_token.get("name") or "Unbekannt",
        "symbol": base_token.get("symbol") or "?",
        "liquidity_usd": liquidity,
        "volume_m5_usd": volume,
        "buys_m5": buys,
        "sells_m5": sells,
        "price_change_m5": price_change,
        "market_cap_usd": market_cap,
        "dex": pair.get("dexId") or "?",
    }


def evaluate_market(market: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    if not market:
        return False, "Keine Marktdaten gefunden"

    liquidity = market["liquidity_usd"]
    volume = market["volume_m5_usd"]
    buys = market["buys_m5"]
    sells = market["sells_m5"]
    price_change = market["price_change_m5"]
    market_cap = market["market_cap_usd"]

    if liquidity < MIN_LIQUIDITY_USD:
        return False, f"LiquiditÃ¤t zu niedrig: ${liquidity:,.0f}"

    if volume < MIN_VOLUME_M5_USD:
        return False, f"5m Volumen zu niedrig: ${volume:,.0f}"

    if buys < MIN_M5_BUYS:
        return False, f"Zu wenige 5m KÃ¤ufe: {buys}"

    ratio = buys / max(sells, 1)

    if ratio < MIN_BUY_SELL_RATIO:
        return False, f"Kaufdruck zu schwach: {ratio:.2f}x"

    if price_change < MIN_PRICE_CHANGE_M5:
        return False, f"5m Momentum zu schwach: {price_change:.1f}%"

    if market_cap <= 0:
        return False, "Market Cap unbekannt"

    if market_cap < MIN_MARKET_CAP_USD:
        return False, f"Market Cap zu klein: ${market_cap:,.0f}"

    if market_cap > MAX_MARKET_CAP_USD:
        return False, f"Market Cap zu groÃ: ${market_cap:,.0f}"

    return True, "PASS"


async def send_high_potential_alert(
    mint: str,
    trader_count: int,
    market: Dict[str, Any],
) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        STATS["last_error"] = "Telegram-Konfiguration fehlt."
        return False

    buys = market["buys_m5"]
    sells = market["sells_m5"]
    ratio = buys / max(sells, 1)

    message = (
        "ð HIGH-POTENTIAL MEMECOIN\n\n"
        f"ðª {market['name']} ({market['symbol']})\n"
        f"ð¥ {trader_count} beobachtete Trader haben gekauft\n"
        f"â± Zeitfenster: {TRADER_WINDOW_MINUTES} Min.\n\n"
        f"ð§ LiquiditÃ¤t: ${market['liquidity_usd']:,.0f}\n"
        f"ð¥ Volumen 5m: ${market['volume_m5_usd']:,.0f}\n"
        f"ð¢ KÃ¤ufe 5m: {buys}\n"
        f"ð´ VerkÃ¤ufe 5m: {sells}\n"
        f"ð Buy/Sell: {ratio:.2f}x\n"
        f"â¡ Kurs 5m: {market['price_change_m5']:+.1f}%\n"
        f"ð° Market Cap: ${market['market_cap_usd']:,.0f}\n\n"
        "ðª CONTRACT ADDRESS (CA):\n"
        f"{mint}\n\n"
        "ð Unten auf âCA kopierenâ tippen und in Phantom suchen.\n\n"
        "â ï¸ Kein Gewinn ist garantiert. CA, LiquiditÃ¤t und Preis "
        "vor dem Kauf selbst prÃ¼fen."
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
        "reply_markup": {
            "inline_keyboard": [
                [
                    {
                        "text": "ð CA kopieren",
                        "copy_text": {"text": mint},
                    }
                ]
            ]
        },
    }

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    async with httpx.AsyncClient(timeout=20) as client:
        for attempt in range(2):
            response = await client.post(url, json=payload)

            if response.status_code == 429:
                STATS["telegram_rate_limits"] += 1

                try:
                    retry_after = int(
                        response.json()
                        .get("parameters", {})
                        .get("retry_after", 2)
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
                    f"Telegram HTTP {response.status_code}: "
                    f"{response.text[:200]}"
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
        "telegram_configured": bool(
            TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
        ),
        "tracked_wallets": len(TRACKED_WALLETS),
        "mode": "BUY_ONLY_HIGH_POTENTIAL",
    }


@app.get("/stats")
async def stats():
    return {
        "version": BUILD_VERSION,
        **STATS,
        "tracked_wallets": len(TRACKED_WALLETS),
        "filter": {
            "min_traders": MIN_TRADERS,
            "window_minutes": TRADER_WINDOW_MINUTES,
            "min_liquidity_usd": MIN_LIQUIDITY_USD,
            "min_volume_m5_usd": MIN_VOLUME_M5_USD,
            "min_m5_buys": MIN_M5_BUYS,
            "min_buy_sell_ratio": MIN_BUY_SELL_RATIO,
            "min_price_change_m5": MIN_PRICE_CHANGE_M5,
            "min_market_cap_usd": MIN_MARKET_CAP_USD,
            "max_market_cap_usd": MAX_MARKET_CAP_USD,
        },
    }


@app.get("/test-telegram")
async def test_telegram():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return {"ok": False, "error": "Telegram not configured"}

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": "â V9 HIGH-POTENTIAL ist aktiv. Nur gefilterte KÃ¤ufe.",
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
                raise HTTPException(
                    status_code=401,
                    detail="Bad webhook secret",
                )

        payload = await request.json()
        events = normalize_events(payload)

        STATS["transactions_received"] += len(events)
        cleanup_state()

        alerts_sent = 0

        for event in events:
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

            # No Telegram message for sells or unrelated activity.
            if not buy:
                continue

            mint, _amount = buy

            STATS["real_buys_detected"] += 1
            STATS["last_buy_mint"] = mint

            trader_count = register_buy_signal(mint, trader_wallet)

            if trader_count < MIN_TRADERS:
                STATS["filtered_not_enough_traders"] += 1
                STATS["last_filter_reason"] = (
                    f"Nur {trader_count}/{MIN_TRADERS} Trader"
                )
                continue

            STATS["multi_trader_candidates"] += 1

            # Do not alert the same token repeatedly during cooldown.
            if mint in _alerted_mints:
                continue

            STATS["market_checks"] += 1
            market = await fetch_market_data(mint)

            passed, reason = evaluate_market(market)
            STATS["last_market_data"] = market
            STATS["last_filter_reason"] = reason

            if not passed:
                STATS["filtered_market_conditions"] += 1
                continue

            STATS["high_potential_passes"] += 1

            sent = await send_high_potential_alert(
                mint=mint,
                trader_count=trader_count,
                market=market,
            )

            if sent:
                STATS["telegram_alerts_sent"] += 1
                alerts_sent += 1
                _alerted_mints[mint] = time.time()

        return {
            "ok": True,
            "version": BUILD_VERSION,
            "alerts_sent": alerts_sent,
        }

    except HTTPException:
        raise

    except Exception as exc:
        STATS["last_error"] = str(exc)[:500]

        # Acknowledge the webhook to avoid Helius retry storms.
        return {
            "ok": False,
            "version": BUILD_VERSION,
            "error": "event_processing_failed",
        }
