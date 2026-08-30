import os
import time
import asyncio
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, Request, HTTPException

app = FastAPI(title="Memecoin V10 Early Signal Bot")

BUILD_VERSION = "FIXED-2026-08-30-V10-EARLY-SIGNAL"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

tracked_wallets_env = os.getenv("TRACKED_WALLETS", "")
TRACKED_WALLETS: Dict[str, Dict[str, Any]] = {}

for i, wallet in enumerate(tracked_wallets_env.split(","), start=1):
    wallet = wallet.strip()
    if wallet:
        TRACKED_WALLETS[wallet] = {"name": f"Trader {i}"}

TRADER_WINDOW_MINUTES = int(os.getenv("TRADER_WINDOW_MINUTES", "15"))

# Early signal: 1 tracked trader + strong momentum
EARLY_MIN_LIQUIDITY_USD = float(os.getenv("EARLY_MIN_LIQUIDITY_USD", "40000"))
EARLY_MIN_VOLUME_M5_USD = float(os.getenv("EARLY_MIN_VOLUME_M5_USD", "15000"))
EARLY_MIN_M5_BUYS = int(os.getenv("EARLY_MIN_M5_BUYS", "8"))
EARLY_MIN_BUY_SELL_RATIO = float(os.getenv("EARLY_MIN_BUY_SELL_RATIO", "1.4"))
EARLY_MIN_PRICE_CHANGE_M5 = float(os.getenv("EARLY_MIN_PRICE_CHANGE_M5", "1.5"))
EARLY_MIN_MARKET_CAP_USD = float(os.getenv("EARLY_MIN_MARKET_CAP_USD", "75000"))
EARLY_MAX_MARKET_CAP_USD = float(os.getenv("EARLY_MAX_MARKET_CAP_USD", "8000000"))

# Confirmed signal: 2+ distinct tracked traders + stricter filters
CONFIRMED_MIN_TRADERS = int(os.getenv("CONFIRMED_MIN_TRADERS", "2"))
CONFIRMED_MIN_LIQUIDITY_USD = float(os.getenv("CONFIRMED_MIN_LIQUIDITY_USD", "50000"))
CONFIRMED_MIN_VOLUME_M5_USD = float(os.getenv("CONFIRMED_MIN_VOLUME_M5_USD", "20000"))
CONFIRMED_MIN_M5_BUYS = int(os.getenv("CONFIRMED_MIN_M5_BUYS", "10"))
CONFIRMED_MIN_BUY_SELL_RATIO = float(os.getenv("CONFIRMED_MIN_BUY_SELL_RATIO", "1.5"))
CONFIRMED_MIN_PRICE_CHANGE_M5 = float(os.getenv("CONFIRMED_MIN_PRICE_CHANGE_M5", "3"))
CONFIRMED_MIN_MARKET_CAP_USD = float(os.getenv("CONFIRMED_MIN_MARKET_CAP_USD", "100000"))
CONFIRMED_MAX_MARKET_CAP_USD = float(os.getenv("CONFIRMED_MAX_MARKET_CAP_USD", "10000000"))

EARLY_ALERT_COOLDOWN_MINUTES = int(os.getenv("EARLY_ALERT_COOLDOWN_MINUTES", "120"))
CONFIRMED_ALERT_COOLDOWN_MINUTES = int(os.getenv("CONFIRMED_ALERT_COOLDOWN_MINUTES", "180"))

WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
QUOTE_MINTS = {WSOL_MINT, USDC_MINT, USDT_MINT}

_seen_signatures: Dict[str, float] = {}
_buy_signals: Dict[str, List[Tuple[str, float]]] = {}
_early_alerted_mints: Dict[str, float] = {}
_confirmed_alerted_mints: Dict[str, float] = {}

SEEN_TTL_SECONDS = 7200

STATS = {
    "webhook_requests": 0,
    "transactions_received": 0,
    "swap_events_received": 0,
    "tracked_wallet_matches": 0,
    "real_buys_detected": 0,
    "early_market_checks": 0,
    "early_alerts_sent": 0,
    "confirmed_candidates": 0,
    "confirmed_market_checks": 0,
    "confirmed_alerts_sent": 0,
    "telegram_rate_limits": 0,
    "market_api_failures": 0,
    "ignored_non_swap": 0,
    "ignored_not_buy": 0,
    "last_trader": None,
    "last_signature": None,
    "last_buy_mint": None,
    "last_signal_level": None,
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

    window_seconds = TRADER_WINDOW_MINUTES * 60
    for mint, signals in list(_buy_signals.items()):
        recent = [(wallet, ts) for wallet, ts in signals if now - ts <= window_seconds]
        if recent:
            _buy_signals[mint] = recent
        else:
            _buy_signals.pop(mint, None)

    early_cd = EARLY_ALERT_COOLDOWN_MINUTES * 60
    for mint, ts in list(_early_alerted_mints.items()):
        if now - ts > early_cd:
            _early_alerted_mints.pop(mint, None)

    confirmed_cd = CONFIRMED_ALERT_COOLDOWN_MINUTES * 60
    for mint, ts in list(_confirmed_alerted_mints.items()):
        if now - ts > confirmed_cd:
            _confirmed_alerted_mints.pop(mint, None)


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

    fee_payer = event.get("feePayer")
    if fee_payer in tracked:
        return fee_payer

    for node in walk(event):
        if isinstance(node, str) and node in tracked:
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


def register_buy_signal(mint: str, trader_wallet: str) -> int:
    now = time.time()
    window_seconds = TRADER_WINDOW_MINUTES * 60
    signals = _buy_signals.setdefault(mint, [])

    signals[:] = [(wallet, ts) for wallet, ts in signals if now - ts <= window_seconds]

    if not any(wallet == trader_wallet for wallet, _ in signals):
        signals.append((trader_wallet, now))

    return len({wallet for wallet, _ in signals})


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
            "dex": pair.get("dexId") or "?",
        }

    except Exception as exc:
        STATS["market_api_failures"] += 1
        STATS["last_error"] = f"DexScreener error: {str(exc)[:200]}"
        return None


def evaluate_early(market: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    if not market:
        return False, "Keine Marktdaten"

    buys = market["buys_m5"]
    sells = market["sells_m5"]
    ratio = buys / max(sells, 1)

    if market["liquidity_usd"] < EARLY_MIN_LIQUIDITY_USD:
        return False, "Early: LiquiditÃ¤t zu niedrig"
    if market["volume_m5_usd"] < EARLY_MIN_VOLUME_M5_USD:
        return False, "Early: Volumen zu niedrig"
    if buys < EARLY_MIN_M5_BUYS:
        return False, "Early: zu wenige KÃ¤ufe"
    if ratio < EARLY_MIN_BUY_SELL_RATIO:
        return False, "Early: Kaufdruck zu schwach"
    if market["price_change_m5"] < EARLY_MIN_PRICE_CHANGE_M5:
        return False, "Early: Momentum zu schwach"
    if market["market_cap_usd"] < EARLY_MIN_MARKET_CAP_USD:
        return False, "Early: Market Cap zu klein"
    if market["market_cap_usd"] > EARLY_MAX_MARKET_CAP_USD:
        return False, "Early: Market Cap zu groÃ"

    return True, "EARLY PASS"


def evaluate_confirmed(market: Optional[Dict[str, Any]]) -> Tuple[bool, str]:
    if not market:
        return False, "Keine Marktdaten"

    buys = market["buys_m5"]
    sells = market["sells_m5"]
    ratio = buys / max(sells, 1)

    if market["liquidity_usd"] < CONFIRMED_MIN_LIQUIDITY_USD:
        return False, "Confirmed: LiquiditÃ¤t zu niedrig"
    if market["volume_m5_usd"] < CONFIRMED_MIN_VOLUME_M5_USD:
        return False, "Confirmed: Volumen zu niedrig"
    if buys < CONFIRMED_MIN_M5_BUYS:
        return False, "Confirmed: zu wenige KÃ¤ufe"
    if ratio < CONFIRMED_MIN_BUY_SELL_RATIO:
        return False, "Confirmed: Kaufdruck zu schwach"
    if market["price_change_m5"] < CONFIRMED_MIN_PRICE_CHANGE_M5:
        return False, "Confirmed: Momentum zu schwach"
    if market["market_cap_usd"] < CONFIRMED_MIN_MARKET_CAP_USD:
        return False, "Confirmed: Market Cap zu klein"
    if market["market_cap_usd"] > CONFIRMED_MAX_MARKET_CAP_USD:
        return False, "Confirmed: Market Cap zu groÃ"

    return True, "CONFIRMED PASS"


async def send_telegram_signal(
    level: str,
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

    if level == "EARLY":
        headline = "â¡ EARLY SIGNAL"
        explanation = (
            "1 beobachteter Trader hat gekauft + Markt-Momentum ist stark.\n"
            "â ï¸ FrÃ¼hwarnung: noch nicht durch einen zweiten Trader bestÃ¤tigt."
        )
    else:
        headline = "ð CONFIRMED HIGH-POTENTIAL"
        explanation = (
            f"{trader_count} verschiedene beobachtete Trader haben "
            f"innerhalb von {TRADER_WINDOW_MINUTES} Min. gekauft."
        )

    message = (
        f"{headline}\n\n"
        f"ðª {market['name']} ({market['symbol']})\n"
        f"ð¥ Trader-Signale: {trader_count}\n"
        f"{explanation}\n\n"
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
        "ð Unten auf âCA kopierenâ tippen und in Phantom prÃ¼fen.\n\n"
        "â ï¸ Kein Gewinn ist garantiert. CA, LiquiditÃ¤t, Preis und Slippage "
        "vor einem Kauf selbst prÃ¼fen."
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
        "mode": "SWAP_BUY_ONLY_EARLY_PLUS_CONFIRMED",
    }


@app.get("/stats")
async def stats():
    return {
        "version": BUILD_VERSION,
        **STATS,
        "tracked_wallets": len(TRACKED_WALLETS),
        "strategy": {
            "trader_window_minutes": TRADER_WINDOW_MINUTES,
            "early_traders": 1,
            "confirmed_min_traders": CONFIRMED_MIN_TRADERS,
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
            "â V10 EARLY SIGNAL ist aktiv.\n"
            "â¡ Early nach 1 Trader + starken Marktdaten\n"
            "ð Confirmed nach 2+ verschiedenen Tradern"
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

            trader_count = register_buy_signal(mint, trader_wallet)

            if trader_count == 1 and mint not in _early_alerted_mints:
                STATS["early_market_checks"] += 1
                market = await fetch_market_data(mint)
                STATS["last_market_data"] = market

                passed, reason = evaluate_early(market)
                STATS["last_filter_reason"] = reason

                if passed and market:
                    sent = await send_telegram_signal(
                        level="EARLY",
                        mint=mint,
                        trader_count=1,
                        market=market,
                    )
                    if sent:
                        _early_alerted_mints[mint] = time.time()
                        STATS["early_alerts_sent"] += 1
                        STATS["last_signal_level"] = "EARLY"
                        alerts_sent += 1

            if trader_count >= CONFIRMED_MIN_TRADERS:
                STATS["confirmed_candidates"] += 1

                if mint not in _confirmed_alerted_mints:
                    STATS["confirmed_market_checks"] += 1
                    market = await fetch_market_data(mint)
                    STATS["last_market_data"] = market

                    passed, reason = evaluate_confirmed(market)
                    STATS["last_filter_reason"] = reason

                    if passed and market:
                        sent = await send_telegram_signal(
                            level="CONFIRMED",
                            mint=mint,
                            trader_count=trader_count,
                            market=market,
                        )
                        if sent:
                            _confirmed_alerted_mints[mint] = time.time()
                            STATS["confirmed_alerts_sent"] += 1
                            STATS["last_signal_level"] = "CONFIRMED"
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
