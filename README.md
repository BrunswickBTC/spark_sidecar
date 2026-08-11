<a href="https://lnbits.com" target="_blank" rel="noopener noreferrer">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://i.imgur.com/QE6SIrs.png">
    <img src="https://i.imgur.com/fyKPgVT.png" alt="LNbits" style="width:280px">
  </picture>
</a>

[![License: MIT](https://img.shields.io/badge/License-MIT-success?logo=open-source-initiative&logoColor=white)](./LICENSE)
[![Built for LNbits](https://img.shields.io/badge/Built%20for-LNbits-4D4DFF?logo=lightning&logoColor=white)](https://github.com/lnbits/lnbits)

# Spark L2 sidecar

This sidecar exposes a small HTTP API for LNbits and administrators to use the Spark L2 SDK.
https://www.spark.money/

## Install

```bash
git clone https://github.com/lnbits/spark_sidecar.git
cd spark_sidecar
npm install
```

## Run

```bash
chmod +x server.mjs spark-cli.mjs

SPARK_MNEMONIC="bottom bottom bottom bottom bottom bottom bottom bottom bottom bottom bottom bottom" \
SPARK_NETWORK=MAINNET \
SPARK_SIDECAR_PORT=8765 \
SPARK_PAY_WAIT_MS=20000 \
node server.mjs
```

The administrator CLI is `spark-cli.mjs`. It talks to the running sidecar over HTTP and uses the same API surface as other clients; it does not initialize a second Spark wallet.

**Spark Multiplicity Setting**

Optional multiplicity tuning for [Spark leaf optimization](https://docs.spark.money/api-reference/wallet/initialize#multiplicity-levels).

Default multiplicity is 3:

```bash
SPARK_MULTIPLICITY=3
```

**Optional API Key**

```bash
SPARK_SIDECAR_API_KEY="mykey"
```

Set the same key in LNbits as `SPARK_L2_API_KEY`. The CLI reads the key from `SPARK_SIDECAR_API_KEY`.

If you prefer to provide the mnemonic after startup, omit `SPARK_MNEMONIC` and POST it to the sidecar:

```bash
curl -X POST http://127.0.0.1:8765/v1/mnemonic \
  -H "Content-Type: application/json" \
  -d '{"mnemonic":"bottom bottom bottom bottom bottom bottom bottom bottom bottom bottom bottom bottom"}'
```

## Command-line administrator interface

Run the CLI locally beside the sidecar:

```bash
./spark-cli.mjs health
./spark-cli.mjs balance
```

For a sidecar on another URL, set `SPARK_SIDECAR_URL`:

```bash
SPARK_SIDECAR_URL=http://127.0.0.1:8765 \
SPARK_SIDECAR_API_KEY="mykey" \
./spark-cli.mjs balance --json
```

Every command supports `--json` for machine-readable output. Commands that spend funds require either an interactive confirmation or the explicit `--yes` flag. Do not put mnemonics, API keys, or payment secrets in shell history or process arguments.

### CLI examples

#### Health, identity, balance, and settings

```bash
./spark-cli.mjs health
./spark-cli.mjs identity --json
./spark-cli.mjs balance --json
./spark-cli.mjs tokens balance --json
./spark-cli.mjs settings --json
./spark-cli.mjs status optimization --json
```

The balance response includes available, owned, and incoming sats, together with token balances.

#### Lightning invoices and payments

Create and inspect a Lightning receive invoice:

```bash
./spark-cli.mjs invoice create \
  --amount-sats 1000 \
  --memo "Administrator test" \
  --expiry-seconds 3600 \
  --json

./spark-cli.mjs invoice get 'SparkLightningReceiveRequest:REQUEST_ID' --json
```

Follow paid-invoice events through the SSE stream:

```bash
./spark-cli.mjs invoice stream --json
```

Send a Lightning payment. Interactive confirmation is used by default:

```bash
./spark-cli.mjs payment send \
  --bolt11 'lnbc...' \
  --max-fee-sats 10
```

For automation, explicitly acknowledge the spend:

```bash
./spark-cli.mjs payment send \
  --bolt11 'lnbc...' \
  --max-fee-sats 10 \
  --yes \
  --json

./spark-cli.mjs payment get 'SparkLightningSendRequest:REQUEST_ID' --json
```

For a zero-amount invoice, specify the amount:

```bash
./spark-cli.mjs payment send \
  --bolt11 'lnbc...' \
  --amount-sats 1000 \
  --max-fee-sats 10 \
  --yes
```

#### Spark identity and Bitcoin deposit addresses

```bash
./spark-cli.mjs identity
./spark-cli.mjs deposit single-use
./spark-cli.mjs deposit static
./spark-cli.mjs deposit static-addresses --json
```

A single-use address must not be reused. A static address is intended for repeated deposits.

#### Spark-to-Spark transfers

```bash
./spark-cli.mjs transfer send \
  --to 'spark...' \
  --amount-sats 1000
```

For noninteractive administration:

```bash
./spark-cli.mjs transfer send \
  --to 'spark...' \
  --amount-sats 1000 \
  --yes \
  --json
```

Inspect a transfer or list transfers:

```bash
./spark-cli.mjs transfer get TRANSFER_ID --json
./spark-cli.mjs transfer list --limit 20 --offset 0 --json
```

#### On-chain withdrawals

First request a fee quote:

```bash
./spark-cli.mjs withdraw quote \
  --address 'bc1...' \
  --amount-sats 10000 \
  --json
```

Submit the withdrawal using values from the quote:

```bash
./spark-cli.mjs withdraw send \
  --address 'bc1...' \
  --amount-sats 10000 \
  --exit-speed FAST \
  --fee-quote-id QUOTE_ID \
  --fee-amount-sats 100 \
  --yes \
  --json
```

Check a cooperative-exit request:

```bash
./spark-cli.mjs withdraw get REQUEST_ID --json
```

Withdrawal operations spend funds and require `--yes` when run non-interactively.

#### Token operations

Get the wallet's token L1 address:

```bash
./spark-cli.mjs tokens l1-address --json
```

Transfer tokens:

```bash
./spark-cli.mjs tokens transfer \
  --token-id 'btkn...' \
  --amount 100 \
  --to 'spark...' \
  --yes \
  --json
```

Query token transactions by token identifier or transaction hash:

```bash
./spark-cli.mjs tokens transactions \
  --token-id 'btkn...' \
  --json

./spark-cli.mjs tokens transactions \
  --hash TXID \
  --json
```

#### Runtime mnemonic setup

The mnemonic is accepted only on standard input and is never printed by the CLI:

```bash
printf '%s\n' "$SPARK_MNEMONIC" | \
  ./spark-cli.mjs mnemonic set --stdin --json
```

## HTTP API

All endpoints require the `X-Api-Key` header when `SPARK_SIDECAR_API_KEY` is configured.

### General and wallet state

- `GET /health`
- `POST /v1/mnemonic`
- `GET /v1/identity`
- `POST /v1/balance`
- `GET /v1/settings`
- `POST /v1/status/optimization`

`POST /v1/balance` returns immediately spendable sats plus the `sats_balance` breakdown and `token_balances`.

### Lightning

- `POST /v1/invoices`
- `GET /v1/invoices/stream` (SSE stream of paid Lightning receive requests)
- `GET /v1/invoices/{id}`
- `POST /v1/payments`
- `GET /v1/payments/{id}`

Invoice and payment responses include the raw Spark `status` and a normalized `status_class` where available: `pending`, `success`, `failed`, or `unknown`.

### Bitcoin deposits

- `GET /v1/deposit/single-use`
- `GET /v1/deposit/static`
- `GET /v1/deposit/static/addresses`
- `POST /v1/deposit/utxos`

### Spark transfers

- `POST /v1/transfer`
- `POST /v1/transfer/get`
- `POST /v1/transfer/ssp`
- `POST /v1/transfers/list`

### On-chain withdrawals

- `POST /v1/withdraw/quote`
- `POST /v1/withdraw`
- `POST /v1/withdraw/get`

### Tokens and Spark invoices

- `GET /v1/tokens/l1-address`
- `POST /v1/tokens/transfer`
- `POST /v1/tokens/transactions`
- `POST /v1/tokens/invoice`
- `POST /v1/sats/invoice`

## Invoice stream

The stream endpoint emits Server-Sent Events when a Lightning invoice is paid.

Example:

```bash
curl -N http://127.0.0.1:8765/v1/invoices/stream
```

Each event payload is a JSON object similar to:

```json
{
  "checking_id": "<receive_request_id>",
  "payment_hash": "<hash>",
  "status": "LIGHTNING_PAYMENT_RECEIVED"
}
```

Optional tuning:

- `SPARK_STREAM_KEEPALIVE_MS` (default `15000`)
- `SPARK_STREAM_HEARTBEAT_MS` (default `30000`)
- `SPARK_TRANSFER_LOOKUP_CONCURRENCY` (default `20`)
- `SPARK_TRANSFER_QUEUE_MAX` (default `5000`)
- `SPARK_INVOICE_POLL_MS` (default `2000`)
- `SPARK_INVOICE_POLL_LIMIT` (default `100`)
- `SPARK_INVOICE_CACHE_TTL_MS` (default `3600000`)

## Nix (flake)

Build:

```bash
nix build
```

Run:

```bash
SPARK_MNEMONIC="bottom bottom bottom bottom bottom bottom bottom bottom bottom bottom bottom bottom" \
SPARK_NETWORK=MAINNET \
SPARK_SIDECAR_PORT=8765 \
SPARK_PAY_WAIT_MS=20000 \
nix run
```

Notes:

- The flake includes `flake.nix` and `flake.lock`. Commit both.
- The `result` symlink from `nix build` should not be committed.

## Powered by LNbits

[LNbits](https://lnbits.com) is a free and open-source lightning accounts system.

[![Visit LNbits Shop](https://img.shields.io/badge/Visit-LNbits%20Shop-7C3AED?logo=shopping-cart&logoColor=white&labelColor=5B21B6)](https://shop.lnbits.com/)
[![Try myLNbits SaaS](https://img.shields.io/badge/Try-myLNbits%20SaaS-2563EB?logo=lightning&logoColor=white&labelColor=1E40AF)](https://my.lnbits.com/login)
