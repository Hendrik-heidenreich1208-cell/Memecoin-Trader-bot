import os
import time
from typing import Any, Dict, List
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Request, HTTPException
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Memecoin Wallet Alert Bot")

BUILD_VERSION = "FIXED-2026-08-30-V3"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
SOLANA_EXPLORER = "https://solscan.io/tx/"

tracked_wallets_env = os.getenv("TRACKED_WALLETS", "")

TRACKED_WALLETS: Dict[str, Dict[str, Any]] = {}

for i, wallet in enumerate(tracked_wallets_env.split(","), start=1):
    wallet = wallet.strip()
    if wallet:
        TRACKED_WALLETS[wallet] = {"name": f"Trader {i}"}

IGNORE_MINTS = {
    "So11111111111111111111111111111111111111112",  # WSOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4rA4nZJDKjVwQ7PkhTNo",  # USDT
}

_seen: Dict[str, float] = {}
SEEN_TTL_SECONDS = 3600

STATS = {
    "webhook_requests": 0,
    "transactions_received": 0,
    "tracked_wallet_matches": 0,
    "swaps_detected": 0,
    "tokens_identified": 0,
    "telegram_alerts_sent": 0,
    "last_type": None,
    "last_trader": None,
    "last_signature": None,
    "last_mint": None,
    "last_description": None,
    "last_error": None,
}


def cleanup_seen() -> None:
    now = time.time()
    for signature, timestamp in list(_seen.items()):
        if now - timestamp > SEEN_TTL_SECONDS:
            _seen.pop(signature, None)


async def send_telegram(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise RuntimeError("Telegram credentials are missing.")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
    }

    async with httpx.AsyncClient(timeout=15) as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()


def get_all_addresses(tx: Dict[str, Any]) -> set[str]:
    addresses: set[str] = set()

    fee_payer = tx.get("feePayer")
    if fee_payer:
        addresses.add(fee_payer)

    for item in tx.get("accountData", []) or []:
        account = item.get("account")
        if account:
            addresses.add(account)

    for transfer in tx.get("tokenTransfers", []) or []:
        for key in ("fromUserAccount", "toUserAccount"):
            address = transfer.get(key)
            if address:
                addresses.add(address)

    for transfer in tx.get("nativeTransfers", []) or []:
        for key in ("fromUserAccount", "toUserAccount"):
            address = transfer.get(key)
            if address:
                addresses.add(address)

    return addresses


def identify_trader(
    tx: Dict[str, Any],
) -> tuple[str | None, Dict[str, Any] | None]:
    addresses = get_all_addresses(tx)

    for wallet, meta in TRACKED_WALLETS.items():
        if wallet in addresses:
            return wallet, meta

    return None, None


def identify_bought_token(
    tx: Dict[str, Any],
    trader_wallet: str,
) -> Dict[str, Any] | None:
    candidates: List[Dict[str, Any]] = []

    for transfer in tx.get("tokenTransfers", []) or []:
        if transfer.get("toUserAccount") != trader_wallet:
            continue

        mint = transfer.get("mint")
        if not mint or mint in IGNORE_MINTS:
            continue

        candidates.append(
            {
                "mint": mint,
                "amount": transfer.get("tokenAmount"),
            }
        )

    return candidates[0] if candidates else None


def phantom_links(mint: str) -> tuple[str, str]:
    token_page = f"https://phantom.com/tokens/solana/{mint}"
    caip19 = f"solana:101/address:{mint}"
    buy = quote(caip19, safe="")
    swap_link = f"https://phantom.app/ul/v1/swap/?buy={buy}"
    return token_page, swap_link


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
        "â Telegram-Test erfolgreich! Dein Memecoin-Bot funktioniert."
    )
    return {"ok": True}


@app.post("/helius")
async def helius_webhook(request: Request):
    STATS["webhook_requests"] += 1

    try:
        if WEBHOOK_SECRET:
            supplied = request.headers.get("x-webhook-secret", "")
            if supplied != WEBHOOK_SECRET:
                raise HTTPException(
                    status_code=401,
                    detail="Bad webhook secret",
                )

        payload = await request.json()
        txs: List[Dict[str, Any]] = (
            payload if isinstance(payload, list) else [payload]
        )

        STATS["transactions_received"] += len(txs)
        cleanup_seen()
        alerts = 0

        for tx in txs:
            signature = tx.get("signature") or tx.get(
                "transactionSignature"
            )

            if signature and signature in _seen:
                continue

            tx_type = str(tx.get("type") or "UNKNOWN").upper()
            description = str(
                tx.get("description") or "Keine Beschreibung verfÃ¼gbar."
            )

            STATS["last_type"] = tx_type
            STATS["last_signature"] = signature
            STATS["last_description"] = description[:500]

            trader_wallet, meta = identify_trader(tx)
            if not trader_wallet:
                continue

            STATS["tracked_wallet_matches"] += 1
            STATS["last_trader"] = trader_wallet

            if tx_type != "SWAP":
                continue

            STATS["swaps_detected"] += 1

            bought = identify_bought_token(tx, trader_wallet)
            if not bought:
                continue

            mint = bought["mint"]
            amount = bought.get("amount")
            STATS["tokens_identified"] += 1
            STATS["last_mint"] = mint

            name = meta.get("name", "Tracked trader")
            tx_link = (
                f"{SOLANA_EXPLORER}{signature}"
                if signature
                else "keine Signatur"
            )
            phantom_token, phantom_swap = phantom_links(mint)

            message = (
                "ð¨ MEMECOIN SWAP ALERT\n\n"
                f"Trader: {name}\n"
                f"Wallet: {trader_wallet}\n"
                f"Token-Mint: {mint}\n"
                f"Menge: {amount if amount is not None else 'unbekannt'}\n\n"
                f"{description}\n\n"
                f"ð» In Phantom Ã¶ffnen:\n{phantom_token}\n\n"
                f"â¡ Direkt in Phantom zum Swap:\n{phantom_swap}\n\n"
                f"ð Solscan:\n{tx_link}\n\n"
                "â ï¸ PrÃ¼fe Mint, LiquiditÃ¤t und Preis vor dem Kauf. "
                "Der Kauf wird NICHT automatisch ausgefÃ¼hrt."
            )

            await send_telegram(message)
            alerts += 1
            STATS["telegram_alerts_sent"] += 1

            if signature:
                _seen[signature] = time.time()

        STATS["last_error"] = None
        return {"ok": True, "alerts_sent": alerts}

    except HTTPException:
        raise
    except Exception as exc:
        STATS["last_error"] = str(exc)[:500]
        raise
