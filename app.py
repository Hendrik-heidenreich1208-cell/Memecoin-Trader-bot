import os
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Request, HTTPException

app = FastAPI(title="Memecoin Wallet Alert Bot")

BUILD_VERSION = "FIXED-2026-08-30-V5"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

tracked_wallets_env = os.getenv("TRACKED_WALLETS", "")
TRACKED_WALLETS: Dict[str, Dict[str, Any]] = {}

for i, wallet in enumerate(tracked_wallets_env.split(","), start=1):
    wallet = wallet.strip()
    if wallet:
        TRACKED_WALLETS[wallet] = {"name": f"Trader {i}"}

IGNORE_MINTS = {
    "So11111111111111111111111111111111111111112",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
}

_seen: Dict[str, float] = {}
SEEN_TTL_SECONDS = 3600

STATS = {
    "webhook_requests": 0,
    "transactions_received": 0,
    "tracked_wallet_matches": 0,
    "alerts_sent": 0,
    "token_mints_found": 0,
    "last_trader": None,
    "last_signature": None,
    "last_type": None,
    "last_mint": None,
    "last_description": None,
    "last_error": None,
}


async def send_telegram(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("Telegram-Konfiguration fehlt.")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
    }

    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()


def cleanup_seen() -> None:
    now = time.time()
    for key, timestamp in list(_seen.items()):
        if now - timestamp > SEEN_TTL_SECONDS:
            _seen.pop(key, None)


def walk(obj: Any):
    yield obj
    if isinstance(obj, dict):
        for value in obj.values():
            yield from walk(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from walk(value)


def find_tracked_wallet(obj: Any) -> Optional[str]:
    tracked = set(TRACKED_WALLETS.keys())

    for value in walk(obj):
        if isinstance(value, str) and value in tracked:
            return value

    return None


def find_first_key(obj: Any, keys: set[str]) -> Any:
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in keys and value not in (None, ""):
                return value

        for value in obj.values():
            result = find_first_key(value, keys)
            if result not in (None, ""):
                return result

    elif isinstance(obj, list):
        for value in obj:
            result = find_first_key(value, keys)
            if result not in (None, ""):
                return result

    return None


def find_signature(obj: Any) -> Optional[str]:
    value = find_first_key(
        obj,
        {"signature", "transactionSignature", "txSignature"},
    )
    return str(value) if value else None


def find_tx_type(obj: Any) -> str:
    value = find_first_key(
        obj,
        {"type", "transactionType", "txType"},
    )
    return str(value).upper() if value else "UNKNOWN"


def find_description(obj: Any) -> str:
    value = find_first_key(
        obj,
        {"description", "summary", "message"},
    )
    if value:
        return str(value)[:800]

    return "On-Chain-AktivitÃ¤t der Ã¼berwachten Wallet erkannt."


def find_mint(obj: Any, trader_wallet: str) -> Optional[str]:
    candidates: List[str] = []

    for node in walk(obj):
        if not isinstance(node, dict):
            continue

        mint = (
            node.get("mint")
            or node.get("tokenMint")
            or node.get("mintAddress")
        )

        if not isinstance(mint, str):
            continue

        if mint in IGNORE_MINTS:
            continue

        node_text = str(node)

        if trader_wallet in node_text:
            candidates.append(mint)

    return candidates[0] if candidates else None


def phantom_links(mint: str) -> Tuple[str, str]:
    token_page = f"https://phantom.com/tokens/solana/{mint}"
    caip19 = f"solana:101/address:{mint}"
    encoded = quote(caip19, safe="")
    swap_link = f"https://phantom.app/ul/v1/swap/?buy={encoded}"
    return token_page, swap_link


def normalize_events(payload: Any) -> List[Any]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for key in ("data", "events", "transactions", "result"):
            value = payload.get(key)

            if isinstance(value, list):
                return value

            if isinstance(value, dict):
                return [value]

        return [payload]

    return [payload]


@app.get("/")
async def health():
    return {
        "ok": True,
        "version": BUILD_VERSION,
        "telegram_configured": bool(
            TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID
        ),
        "tracked_wallets": len(TRACKED_WALLETS),
    }


@app.get("/stats")
async def stats():
    return {
        "version": BUILD_VERSION,
        **STATS,
        "tracked_wallets": len(TRACKED_WALLETS),
    }


@app.get("/test-telegram")
async def test_telegram():
    await send_telegram(
        "â Telegram-Test erfolgreich! V5 ist aktiv."
    )
    return {
        "ok": True,
        "version": BUILD_VERSION,
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

        cleanup_seen()
        alerts_sent = 0

        for event in events:
            trader_wallet = find_tracked_wallet(event)

            if not trader_wallet:
                continue

            signature = find_signature(event)
            dedupe_key = (
                signature
                or f"{trader_wallet}:{hash(str(event))}"
            )

            if dedupe_key in _seen:
                continue

            tx_type = find_tx_type(event)
            description = find_description(event)
            mint = find_mint(event, trader_wallet)

            STATS["tracked_wallet_matches"] += 1
            STATS["last_trader"] = trader_wallet
            STATS["last_signature"] = signature
            STATS["last_type"] = tx_type
            STATS["last_description"] = description
            STATS["last_mint"] = mint

            trader_name = TRACKED_WALLETS[trader_wallet]["name"]

            message = (
                "ð¨ WALLET-AKTIVITÃT ERKANNT\n\n"
                f"Trader: {trader_name}\n"
                f"Wallet: {trader_wallet}\n"
                f"Typ: {tx_type}\n\n"
                f"{description}\n"
            )

            if mint:
                STATS["token_mints_found"] += 1

                token_page, swap_link = phantom_links(mint)

                message += (
                    f"\nðª Token-Mint:\n{mint}\n"
                    f"\nð» Phantom Token:\n{token_page}\n"
                    f"\nâ¡ Phantom Swap:\n{swap_link}\n"
                )

            if signature:
                message += (
                    f"\nð Solscan:\n"
                    f"https://solscan.io/tx/{signature}\n"
                )

            message += (
                "\nâ ï¸ PrÃ¼fe Token-Mint, LiquiditÃ¤t und Preis "
                "immer selbst vor einem Kauf."
            )

            await send_telegram(message)

            STATS["alerts_sent"] += 1
            alerts_sent += 1
            _seen[dedupe_key] = time.time()

        STATS["last_error"] = None

        return {
            "ok": True,
            "version": BUILD_VERSION,
            "alerts_sent": alerts_sent,
        }

    except HTTPException:
        raise

    except Exception as exc:
        STATS["last_error"] = str(exc)[:500]
        raise
