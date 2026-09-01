import os
import time
import json
import asyncio
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

import httpx
import websockets
from fastapi import FastAPI, Request, HTTPException

BUILD_VERSION = "FIXED-2026-09-01-V14-FREE-FAST-STREAM"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_TARGET = TELEGRAM_CHANNEL_ID or TELEGRAM_CHAT_ID

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "").strip()
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "").strip()

TRACKED_WALLETS = {
    w.strip()
    for w in os.getenv("TRACKED_WALLETS", "").split(",")
    if w.strip()
}

WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
QUOTE_MINTS = {WSOL_MINT, USDC_MINT, USDT_MINT}

# Nur echte Transaktionen deduplizieren. Gleicher Coin darf spÃ¤ter erneut alarmieren.
SEEN_TTL_SECONDS = int(os.getenv("SEEN_TTL_SECONDS", "7200"))
MIN_FAST_SOL_SPEND = float(os.getenv("MIN_FAST_SOL_SPEND", "0.00005"))

_seen_signatures: Dict[str, float] = {}
_ws_task: Optional[asyncio.Task] = None

STATS: Dict[str, Any] = {
    "webhook_requests": 0,
    "webhook_transactions": 0,
    "webhook_buys": 0,
    "ws_connected": False,
    "ws_reconnects": 0,
    "ws_notifications": 0,
    "ws_transactions_fetched": 0,
    "ws_fetch_misses": 0,
    "ws_buys": 0,
    "signals_sent": 0,
    "telegram_rate_limits": 0,
    "duplicates_ignored": 0,
    "ignored_not_buy": 0,
    "last_source": None,
    "last_trader": None,
    "last_signature": None,
    "last_buy_mint": None,
    "last_error": None,
}


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def cleanup_seen() -> None:
    now = time.time()
    for sig, ts in list(_seen_signatures.items()):
        if now - ts > SEEN_TTL_SECONDS:
            _seen_signatures.pop(sig, None)


def claim_signature(signature: Optional[str]) -> bool:
    if not signature:
        return True
    cleanup_seen()
    if signature in _seen_signatures:
        STATS["duplicates_ignored"] += 1
        return False
    _seen_signatures[signature] = time.time()
    return True


def unclaim_signature(signature: Optional[str]) -> None:
    # Wenn wir die Transaktion noch nicht lesen konnten, darf der Webhook sie spÃ¤ter Ã¼bernehmen.
    if signature:
        _seen_signatures.pop(signature, None)


def walk(obj: Any):
    yield obj
    if isinstance(obj, dict):
        for value in obj.values():
            yield from walk(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from walk(value)


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


def raw_token_amount(item: Dict[str, Any]) -> float:
    raw = item.get("rawTokenAmount")
    if isinstance(raw, dict):
        token_amount = safe_float(raw.get("tokenAmount"))
        decimals = int(safe_float(raw.get("decimals")))
        return token_amount / (10 ** decimals) if decimals >= 0 else token_amount
    for key in ("tokenAmount", "amount", "uiAmount"):
        if key in item:
            return safe_float(item.get(key))
    return 0.0


def detect_buy_from_enhanced_event(
    event: Dict[str, Any],
    trader_wallet: str,
) -> Optional[Tuple[str, float]]:
    events = event.get("events")
    if isinstance(events, dict) and isinstance(events.get("swap"), dict):
        swap = events["swap"]
        quote_spent = False

        native_input = swap.get("nativeInput") or {}
        if (
            isinstance(native_input, dict)
            and native_input.get("account") == trader_wallet
            and safe_float(native_input.get("amount")) > 0
        ):
            quote_spent = True

        for item in swap.get("tokenInputs") or []:
            if (
                isinstance(item, dict)
                and item.get("userAccount") == trader_wallet
                and item.get("mint") in QUOTE_MINTS
            ):
                quote_spent = True

        outputs: List[Tuple[str, float]] = []
        for item in swap.get("tokenOutputs") or []:
            if not isinstance(item, dict):
                continue
            mint = item.get("mint")
            if (
                item.get("userAccount") == trader_wallet
                and mint
                and mint not in QUOTE_MINTS
            ):
                outputs.append((str(mint), raw_token_amount(item)))

        if quote_spent and outputs:
            outputs.sort(key=lambda x: x[1], reverse=True)
            return outputs[0]

    # Fallback auf Enhanced Transfers
    net: Dict[str, float] = {}
    quote_spent = False

    for tr in event.get("tokenTransfers") or []:
        if not isinstance(tr, dict):
            continue
        mint = tr.get("mint") or tr.get("tokenMint") or tr.get("mintAddress")
        if not mint:
            continue
        amount = safe_float(tr.get("tokenAmount") or tr.get("amount"))
        src = tr.get("fromUserAccount") or tr.get("from")
        dst = tr.get("toUserAccount") or tr.get("to")
        if dst == trader_wallet:
            net[str(mint)] = net.get(str(mint), 0.0) + amount
        if src == trader_wallet:
            net[str(mint)] = net.get(str(mint), 0.0) - amount
            if mint in QUOTE_MINTS:
                quote_spent = True

    for tr in event.get("nativeTransfers") or []:
        if not isinstance(tr, dict):
            continue
        src = tr.get("fromUserAccount") or tr.get("from")
        if src == trader_wallet and safe_float(tr.get("amount")) > 0:
            quote_spent = True
            break

    if quote_spent:
        candidates = [
            (mint, amount)
            for mint, amount in net.items()
            if mint not in QUOTE_MINTS and amount > 0
        ]
        if candidates:
            candidates.sort(key=lambda x: x[1], reverse=True)
            return candidates[0]

    return None


def token_ui_amount(balance: Dict[str, Any]) -> float:
    ui = balance.get("uiTokenAmount") or {}
    value = ui.get("uiAmountString")
    if value is None:
        value = ui.get("uiAmount")
    return safe_float(value)


def account_key_string(key: Any) -> str:
    if isinstance(key, str):
        return key
    if isinstance(key, dict):
        return str(key.get("pubkey") or "")
    return ""


def detect_buy_from_rpc_transaction(
    tx: Dict[str, Any],
    trader_wallet: str,
) -> Optional[Tuple[str, float]]:
    meta = tx.get("meta")
    transaction = tx.get("transaction")
    if not isinstance(meta, dict) or not isinstance(transaction, dict):
        return None
    if meta.get("err") is not None:
        return None

    message = transaction.get("message") or {}
    keys = [account_key_string(k) for k in message.get("accountKeys") or []]

    # Token-Deltas des Traders nach Mint
    pre_by_mint: Dict[str, float] = {}
    post_by_mint: Dict[str, float] = {}

    for bal in meta.get("preTokenBalances") or []:
        if isinstance(bal, dict) and bal.get("owner") == trader_wallet and bal.get("mint"):
            mint = str(bal["mint"])
            pre_by_mint[mint] = pre_by_mint.get(mint, 0.0) + token_ui_amount(bal)

    for bal in meta.get("postTokenBalances") or []:
        if isinstance(bal, dict) and bal.get("owner") == trader_wallet and bal.get("mint"):
            mint = str(bal["mint"])
            post_by_mint[mint] = post_by_mint.get(mint, 0.0) + token_ui_amount(bal)

    all_mints = set(pre_by_mint) | set(post_by_mint)
    deltas = {
        mint: post_by_mint.get(mint, 0.0) - pre_by_mint.get(mint, 0.0)
        for mint in all_mints
    }

    quote_spent = any(
        deltas.get(mint, 0.0) < -1e-12
        for mint in QUOTE_MINTS
    )

    # Native SOL-Ausgabe bestimmen und Fee herausrechnen.
    if trader_wallet in keys:
        idx = keys.index(trader_wallet)
        pre_bal = meta.get("preBalances") or []
        post_bal = meta.get("postBalances") or []
        if idx < len(pre_bal) and idx < len(post_bal):
            spent_lamports = safe_float(pre_bal[idx]) - safe_float(post_bal[idx])
            if idx == 0:
                spent_lamports -= safe_float(meta.get("fee"))
            sol_spent = max(0.0, spent_lamports / 1_000_000_000)
            if sol_spent >= MIN_FAST_SOL_SPEND:
                quote_spent = True

    if not quote_spent:
        return None

    candidates = [
        (mint, delta)
        for mint, delta in deltas.items()
        if mint not in QUOTE_MINTS and delta > 1e-12
    ]
    if not candidates:
        return None

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0]


async def telegram_buy_alert(
    mint: str,
    trader_wallet: str,
    signature: Optional[str],
    source: str,
) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_TARGET:
        STATS["last_error"] = "Telegram-Konfiguration fehlt."
        return False

    short_wallet = (
        f"{trader_wallet[:5]}...{trader_wallet[-5:]}"
        if len(trader_wallet) > 12
        else trader_wallet
    )

    speed_text = "â¡ FAST STREAM" if source == "WSS" else "ð¨ WEBHOOK"
    message = (
        f"ð¨ TRADER BUY â {speed_text}\n\n"
        "ð¤ Einer deiner beobachteten Trader hat gekauft.\n"
        f"ð Wallet: {short_wallet}\n\n"
        "ðª CONTRACT ADDRESS (CA):\n"
        f"{mint}\n\n"
        "ð CA direkt unten kopieren.\n\n"
        "â ï¸ On-Chain-Signal, keine Gewinnprognose. "
        "Preis, LiquiditÃ¤t und Slippage vor einem eigenen Kauf prÃ¼fen."
    )

    payload: Dict[str, Any] = {
        "chat_id": TELEGRAM_TARGET,
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

    async with httpx.AsyncClient(timeout=10) as client:
        for attempt in range(2):
            try:
                response = await client.post(url, json=payload)
            except Exception as exc:
                STATS["last_error"] = f"Telegram network error: {str(exc)[:200]}"
                return False

            if response.status_code == 429:
                STATS["telegram_rate_limits"] += 1
                retry_after = 2
                try:
                    retry_after = int(
                        response.json().get("parameters", {}).get("retry_after", 2)
                    )
                except Exception:
                    pass
                if attempt == 0:
                    await asyncio.sleep(min(max(retry_after, 1), 5))
                    continue
                STATS["last_error"] = "Telegram 429 Too Many Requests"
                return False

            if response.status_code != 200:
                STATS["last_error"] = (
                    f"Telegram HTTP {response.status_code}: {response.text[:200]}"
                )
                return False

            STATS["signals_sent"] += 1
            STATS["last_source"] = source
            STATS["last_trader"] = trader_wallet
            STATS["last_signature"] = signature
            STATS["last_buy_mint"] = mint
            STATS["last_error"] = None
            return True

    return False


async def fetch_transaction_fast(signature: str) -> Optional[Dict[str, Any]]:
    if not HELIUS_API_KEY:
        return None

    url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            },
        ],
    }

    # processed-Log kommt oft vor getTransaction(confirmed).
    # Kurze Retries, damit wir trotzdem schneller als der normale Webhook sein kÃ¶nnen.
    delays = [0.05, 0.10, 0.15, 0.20, 0.30, 0.40, 0.50]

    async with httpx.AsyncClient(timeout=4) as client:
        for delay in delays:
            try:
                response = await client.post(url, json=body)
                if response.status_code == 200:
                    data = response.json()
                    result = data.get("result")
                    if isinstance(result, dict):
                        STATS["ws_transactions_fetched"] += 1
                        return result
            except Exception:
                pass
            await asyncio.sleep(delay)

    STATS["ws_fetch_misses"] += 1
    return None


async def process_ws_signature(signature: str, trader_wallet: str) -> None:
    if not claim_signature(signature):
        return

    tx = await fetch_transaction_fast(signature)
    if not tx:
        unclaim_signature(signature)
        return

    buy = detect_buy_from_rpc_transaction(tx, trader_wallet)
    if not buy:
        STATS["ignored_not_buy"] += 1
        # Gelesen und kein Kauf: Signatur bleibt dedupliziert.
        return

    mint, _amount = buy
    STATS["ws_buys"] += 1
    await telegram_buy_alert(
        mint=mint,
        trader_wallet=trader_wallet,
        signature=signature,
        source="WSS",
    )


async def websocket_loop() -> None:
    if not HELIUS_API_KEY or not TRACKED_WALLETS:
        STATS["last_error"] = (
            "FAST STREAM aus: HELIUS_API_KEY oder TRACKED_WALLETS fehlt."
        )
        return

    url = f"wss://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    backoff = 2

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=30,
                ping_timeout=20,
                close_timeout=5,
                max_size=2_000_000,
            ) as ws:
                STATS["ws_connected"] = True
                STATS["last_error"] = None
                backoff = 2

                request_to_wallet: Dict[int, str] = {}
                subscription_to_wallet: Dict[int, str] = {}

                request_id = 1000
                for wallet in sorted(TRACKED_WALLETS):
                    request_id += 1
                    request_to_wallet[request_id] = wallet
                    await ws.send(json.dumps({
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "logsSubscribe",
                        "params": [
                            {"mentions": [wallet]},
                            {"commitment": "processed"},
                        ],
                    }))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    # Antwort auf Subscription
                    if "id" in msg and "result" in msg and isinstance(msg.get("result"), int):
                        req_id = msg.get("id")
                        wallet = request_to_wallet.get(req_id)
                        if wallet:
                            subscription_to_wallet[int(msg["result"])] = wallet
                        continue

                    if msg.get("method") != "logsNotification":
                        continue

                    params = msg.get("params") or {}
                    sub_id = params.get("subscription")
                    result = params.get("result") or {}
                    value = result.get("value") or {}

                    if value.get("err") is not None:
                        continue

                    signature = value.get("signature")
                    trader_wallet = subscription_to_wallet.get(sub_id)
                    if not signature or not trader_wallet:
                        continue

                    STATS["ws_notifications"] += 1
                    asyncio.create_task(
                        process_ws_signature(str(signature), trader_wallet)
                    )

        except asyncio.CancelledError:
            STATS["ws_connected"] = False
            raise
        except Exception as exc:
            STATS["ws_connected"] = False
            STATS["ws_reconnects"] += 1
            STATS["last_error"] = f"WSS reconnect: {str(exc)[:200]}"
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _ws_task
    if HELIUS_API_KEY and TRACKED_WALLETS:
        _ws_task = asyncio.create_task(websocket_loop())
    yield
    if _ws_task:
        _ws_task.cancel()
        try:
            await _ws_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="Memecoin Trader Bot V14 Free Fast Stream",
    lifespan=lifespan,
)


@app.get("/")
async def health():
    return {
        "ok": True,
        "version": BUILD_VERSION,
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_TARGET),
        "telegram_destination": (
            "channel" if TELEGRAM_CHANNEL_ID
            else "chat" if TELEGRAM_CHAT_ID
            else "not_configured"
        ),
        "tracked_wallets": len(TRACKED_WALLETS),
        "mode": "FREE_FAST_WSS_PLUS_WEBHOOK",
        "fast_stream_configured": bool(HELIUS_API_KEY),
        "fast_stream_connected": bool(STATS["ws_connected"]),
    }


@app.get("/stats")
async def stats():
    return {
        "version": BUILD_VERSION,
        **STATS,
        "tracked_wallets": len(TRACKED_WALLETS),
        "fast_stream_configured": bool(HELIUS_API_KEY),
    }


@app.get("/test-telegram")
async def test_telegram():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_TARGET:
        return {"ok": False, "error": "Telegram not configured"}

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_TARGET,
        "text": (
            "â V14 FREE FAST STREAM ist aktiv.\n"
            "â¡ Standard Helius WebSocket + bestehender SWAP-Webhook.\n"
            "ð Kaufalarme enthalten weiterhin den CA-Kopierbutton."
        ),
    }
    async with httpx.AsyncClient(timeout=10) as client:
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
        STATS["webhook_transactions"] += len(events)

        for event in events:
            if not is_swap_event(event):
                continue

            trader_wallet = find_tracked_wallet(event)
            if not trader_wallet:
                continue

            signature = find_signature(event)
            if not claim_signature(signature):
                continue

            buy = detect_buy_from_enhanced_event(event, trader_wallet)
            if not buy:
                STATS["ignored_not_buy"] += 1
                continue

            mint, _amount = buy
            STATS["webhook_buys"] += 1
            await telegram_buy_alert(
                mint=mint,
                trader_wallet=trader_wallet,
                signature=signature,
                source="WEBHOOK",
            )

        # Helius immer schnell mit 200 beantworten.
        return {"ok": True, "version": BUILD_VERSION}

    except HTTPException:
        raise
    except Exception as exc:
        STATS["last_error"] = str(exc)[:500]
        # Auch bei Parserfehler 200, damit keine Retry-Flut entsteht.
        return {
            "ok": False,
            "version": BUILD_VERSION,
            "error": "event_processing_failed",
        }
