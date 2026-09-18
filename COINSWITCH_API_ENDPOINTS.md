# CoinSwitch API Endpoints Reference

## HFT API

### Base URL
https://dma.coinswitch.co

### Server Time
GET /v5/market/time

Full endpoint:
https://dma.coinswitch.co/v5/market/time

Status: VERIFIED LIVE

---

## HFT Unified Wallet Balance

GET /v5/account/wallet-balance?accountType=UNIFIED&coin=USDT

Full endpoint:
https://dma.coinswitch.co/v5/account/wallet-balance?accountType=UNIFIED&coin=USDT

Status: VERIFIED LIVE

Verified account state:

- accountType: UNIFIED
- coin: USDT
- walletBalance: 2.232
- equity: 2.232
- usdValue: 2.2302077
- totalEquity: 2.2302077
- totalMarginBalance: 2.2302077
- totalAvailableBalance: 2.2302077
- totalPerpUPL: 0
- unrealisedPnl: 0
- locked: 0
- totalPositionIM: 0
- totalPositionMM: 0
- totalOrderIM: 0

IMPORTANT:
HFT balance और Direct Futures balance अलग account surfaces हैं।
इन दोनों balances को आपस में mix नहीं करना है।

---

## Direct Futures Wallet Balance

GET /trade/api/v2/futures/wallet_balance

Status: VERIFIED LIVE

Verified Direct Futures USDT state:

- total_balance: 0
- total_available_balance: 0
- total_blocked_balance: 0
- total_position_margin: 0
- total_open_order_margin: 0

IMPORTANT:
यह HFT account के 2.232 USDT wallet balance से अलग account surface है।

---

## Authentication

Direct Futures authentication headers:

- X-AUTH-APIKEY
- X-AUTH-SIGNATURE
- X-AUTH-EPOCH

Signature mechanism:

Ed25519

API key environment variable:

COINSWITCH_API_KEY

IMPORTANT:
API keys, secrets और private signing keys को इस repository में commit नहीं करना है।

---

## Funds Transfer

POST /dma/api/v1/funds/transfer

Status:
DOCUMENTATION REFERENCE

इस endpoint को इस investigation में live transfer request से test नहीं किया गया है।

इसलिए इसे LIVE VERIFIED endpoint नहीं माना जाए जब तक वास्तविक request सफलतापूर्वक test न हो।

---

## Account Surface Rules

### HFT

Base URL:
https://dma.coinswitch.co

Wallet:
GET /v5/account/wallet-balance

### Direct Futures

Wallet:
GET /trade/api/v2/futures/wallet_balance

RULE:
HFT और Direct Futures balances को एक ही balance समझकर calculation नहीं करनी है।

---

## Current Verified Account Situation

HFT Unified USDT wallet balance:
2.232 USDT

HFT total equity:
2.2302077 USD

Direct Futures USDT balance:
0

---

## Trading Bot Integration Rules

Existing working trading infrastructure को MASTER BASELINE माना जाए।

नई strategy या account integration करते समय common infrastructure preserve करना है:

- WebSocket / market data
- Scanner
- Execution framework
- Capital management
- Risk management
- Leverage logic
- Fees handling
- Funding handling
- PnL calculation
- ROI calculation
- Closed Trades
- Dashboard
- Dashboard layout

नई strategy में मुख्य बदलाव Strategy Logic / Strategy Layer में किए जाएँ।

---

## Dashboard Rules

Leverage कम या ज्यादा होने पर ROI calculation उसी leverage के अनुसार dynamically बदलनी चाहिए।

Live trading में जहां exchange-confirmed actual fee उपलब्ध हो वहां अनुमानित fee का उपयोग नहीं करना है।

Closed Trades में हर trade की actual exchange-confirmed fee दिखनी चाहिए।

Dashboard में:

Fees = सभी closed trades की actual confirmed fees का total

Paper trading में configured paper-trading fee इस्तेमाल की जा सकती है।

---

## Discovery Log

Verified:

1. HFT server time
GET https://dma.coinswitch.co/v5/market/time

2. HFT Unified wallet
GET https://dma.coinswitch.co/v5/account/wallet-balance?accountType=UNIFIED&coin=USDT

3. Direct Futures wallet
GET /trade/api/v2/futures/wallet_balance

4. Funds transfer
POST /dma/api/v1/funds/transfer

Status:
Documentation reference only.
Not live-tested.

IMPORTANT:
Order-book endpoint का exact path इस reference में जानबूझकर नहीं लिखा गया है क्योंकि exact path को सुरक्षित रूप से verify नहीं किया गया है।

Guess करके endpoint को VERIFIED नहीं लिखना है।

---

## Security Rules

इस repository में कभी भी commit नहीं करना है:

- API keys
- API secrets
- Private keys
- Ed25519 private signing keys
- Passwords
- Access tokens
- Authentication credentials

---

## Endpoint Verification Rules

जब कोई नया CoinSwitch endpoint discover हो:

1. HTTP method verify करें।
2. Exact path verify करें।
3. Account surface identify करें।
4. Live request से response verify करें।
5. Response structure record करें।
6. तभी इस file में VERIFIED status दें।

अनुमानित endpoint को VERIFIED न लिखें।

---

## Repository

GitHub repository:
nildhane009/Coinswitch

Default branch:
main

Reference file:
COINSWITCH_API_ENDPOINTS.md

---

## Last Updated

2026-09-18
