import fs from 'node:fs'
import http from 'node:http'
import path from 'node:path'

import {SparkWallet, SparkWalletEvent} from '@buildonspark/spark-sdk'

const PORT = parseInt(process.env.SPARK_SIDECAR_PORT || '8765', 10)
const HOST = process.env.SPARK_SIDECAR_HOST || '127.0.0.1'
const API_KEY = process.env.SPARK_SIDECAR_API_KEY || ''
const SIDECAR_API_VERSION = 1
const SIDECAR_VERSION = process.env.SPARK_SIDECAR_VERSION || '0.2.0'
let SDK_VERSION = process.env.SPARK_SDK_VERSION || 'unknown'
try {
  SDK_VERSION = JSON.parse(fs.readFileSync(new URL('./node_modules/@buildonspark/spark-sdk/package.json', import.meta.url), 'utf8')).version || SDK_VERSION
} catch (_) {}
const SIDECAR_CAPABILITIES = [
  'identity', 'settings', 'balance', 'optimization', 'static_addresses',
  'transfers', 'deposit_utxos', 'deposit_claim', 'transfer',
  'withdrawal_quote', 'withdrawal'
]
let mnemonic = process.env.SPARK_MNEMONIC || ''
const NETWORK = process.env.SPARK_NETWORK || 'MAINNET'
const ELECTRS_URL = process.env.SPARK_ELECTRS_URL || (NETWORK === 'REGTEST' ? 'https://regtest-mempool.us-west-2.sparkinfra.net/api' : 'https://mempool.space/api')
const MULTIPLICITY = parseInt(process.env.SPARK_MULTIPLICITY || '3', 10)
const PAY_WAIT_MS = parseInt(process.env.SPARK_PAY_WAIT_MS || '4000', 10)
const PAY_POLL_MS = parseInt(process.env.SPARK_PAY_POLL_MS || '500', 10)
const STREAM_KEEPALIVE_MS = parseInt(
  process.env.SPARK_STREAM_KEEPALIVE_MS || '15000',
  10
)
const STREAM_HEARTBEAT_MS = parseInt(
  process.env.SPARK_STREAM_HEARTBEAT_MS || '30000',
  10
)
const INVOICE_POLL_MS = parseInt(
  process.env.SPARK_INVOICE_POLL_MS || '2000',
  10
)
const INVOICE_POLL_LIMIT = parseInt(
  process.env.SPARK_INVOICE_POLL_LIMIT || '100',
  10
)
const INVOICE_CACHE_TTL_MS = parseInt(
  process.env.SPARK_INVOICE_CACHE_TTL_MS || '3600000',
  10
)
const TRANSFER_LOOKUP_CONCURRENCY = parseInt(
  process.env.SPARK_TRANSFER_LOOKUP_CONCURRENCY || '20',
  10
)
const TRANSFER_QUEUE_MAX = Math.max(
  1,
  parseInt(process.env.SPARK_TRANSFER_QUEUE_MAX || '5000', 10)
)
const ACCOUNT_NUMBER = process.env.SPARK_ACCOUNT_NUMBER
  ? parseInt(process.env.SPARK_ACCOUNT_NUMBER, 10)
  : undefined
const STATE_PATH =
  process.env.SPARK_SIDECAR_STATE_PATH ||
  path.join(process.cwd(), 'spark-sidecar-state.json')
const STATE_PERSIST_DEBOUNCE_MS = parseInt(
  process.env.SPARK_STATE_PERSIST_DEBOUNCE_MS || '1000',
  10
)

let mnemonicReadyResolve
const mnemonicReady = new Promise(resolve => {
  mnemonicReadyResolve = resolve
})
if (mnemonic) {
  mnemonicReadyResolve()
}

let walletPromise
let walletInstance
const paymentHashToRequestId = new Map()
const sseClients = new Set()
const sseKeepaliveTimers = new Map()
const sseHeartbeatTimers = new Map()
const pendingTransferIds = new Set()
const transferQueue = []
let activeTransferLookups = 0
let walletListenersAttached = false
let droppedTransfers = 0
let lastDropLog = 0
const emittedInvoiceIds = new Map()
let invoicePollTimer = null
let invoicePollInFlight = false
let lastSeenUpdatedAtMs = 0
let statePersistTimer = null
let baselineInitialized = false
let stateLoaded = false

const DROP_LOG_INTERVAL_MS = 10000

loadState()

function loadState() {
  try {
    if (!fs.existsSync(STATE_PATH)) {
      return
    }
    const raw = fs.readFileSync(STATE_PATH, 'utf8')
    const parsed = JSON.parse(raw)
    stateLoaded = true
    if (Number.isFinite(parsed?.lastSeenUpdatedAtMs)) {
      lastSeenUpdatedAtMs = parsed.lastSeenUpdatedAtMs
    }
  } catch (error) {
    console.error('Error loading Spark sidecar state:', error)
  }
}

async function persistState() {
  try {
    await fs.promises.writeFile(
      STATE_PATH,
      JSON.stringify({lastSeenUpdatedAtMs}),
      'utf8'
    )
  } catch (error) {
    console.error('Error persisting Spark sidecar state:', error)
  }
}

function scheduleStatePersist() {
  if (statePersistTimer) {
    return
  }
  statePersistTimer = setTimeout(
    () => {
      statePersistTimer = null
      void persistState()
    },
    Math.max(0, STATE_PERSIST_DEBOUNCE_MS)
  )
}

function getRequestUpdatedAtMs(request) {
  const stamp = request?.updatedAt || request?.createdAt
  if (!stamp) {
    return 0
  }
  const parsed = Date.parse(stamp)
  return Number.isFinite(parsed) ? parsed : 0
}

function rememberInvoiceEmitted(requestId, now = Date.now()) {
  if (!requestId) {
    return false
  }
  if (emittedInvoiceIds.has(requestId)) {
    return true
  }
  emittedInvoiceIds.set(requestId, now)
  return false
}

function attachWalletListeners(wallet) {
  if (walletListenersAttached) {
    return
  }
  walletListenersAttached = true

  wallet.on(SparkWalletEvent.TransferClaimed, transferId => {
    if (!transferId) {
      return
    }
    enqueueTransferLookup(transferId)
  })
}

async function getWallet() {
  await mnemonicReady
  if (!walletPromise) {
    console.log('Initializing Spark wallet...')
    walletPromise = SparkWallet.initialize({
      mnemonicOrSeed: mnemonic,
      accountNumber: ACCOUNT_NUMBER,
      options: {
        network: NETWORK,
        optimizationOptions: {
          auto: true,
          multiplicity: MULTIPLICITY
        }
      }
    }).then(({wallet}) => {
      walletInstance = wallet
      attachWalletListeners(wallet)
      console.log('Spark wallet initialized.')
      return wallet
    })
  }
  const wallet = await walletPromise

  await wallet.setPrivacyEnabled(true)

  if (wallet && !walletListenersAttached) {
    attachWalletListeners(wallet)
  }
  return wallet
}

async function shutdown() {
  try {
    console.log('Shutting down Spark sidecar...')
    if (walletPromise) {
      const wallet = await walletPromise
      if (wallet && typeof wallet.cleanupConnections === 'function') {
        await wallet.cleanupConnections()
      } else if (wallet && typeof wallet.cleanup === 'function') {
        wallet.cleanup()
      }
    }
  } catch (error) {
    console.error('Error during Spark sidecar shutdown:', error)
  } finally {
    process.exit(0)
  }
}

process.on('SIGINT', shutdown)
process.on('SIGTERM', shutdown)

function bytesToHex(value) {
  if (value instanceof Uint8Array) return Buffer.from(value).toString('hex')
  if (Buffer.isBuffer(value)) return value.toString('hex')
  return value
}

function normalizeTransfer(transfer) {
  if (!transfer || typeof transfer !== 'object') return transfer
  return {
    ...transfer,
    senderIdentityPublicKey: bytesToHex(transfer.senderIdentityPublicKey),
    receiverIdentityPublicKey: bytesToHex(transfer.receiverIdentityPublicKey),
    createdTime: transfer.createdTime instanceof Date ? transfer.createdTime.toISOString() : transfer.createdTime,
    updatedTime: transfer.updatedTime instanceof Date ? transfer.updatedTime.toISOString() : transfer.updatedTime,
    totalValue: transfer.totalValue === undefined ? undefined : Number(transfer.totalValue)
  }
}

function jsonSafe(value) {
  if (typeof value === 'bigint') return value.toString()
  if (value instanceof Map) return Object.fromEntries([...value.entries()].map(([k, v]) => [k, jsonSafe(v)]))
  if (Array.isArray(value)) return value.map(jsonSafe)
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, jsonSafe(v)]))
  return value
}
function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, {'content-type': 'application/json'})
  res.end(JSON.stringify(jsonSafe(payload)))
}
async function requireWallet() {
  if (!mnemonic) throw new Error('missing_mnemonic')
  return await getWallet()
}
async function enrichDepositUtxos(utxos) {
  return await Promise.all(utxos.map(async utxo => {
    const response = await fetch(`${ELECTRS_URL}/tx/${utxo.txid}`)
    if (!response.ok) throw new Error(`Unable to retrieve transaction ${utxo.txid}: HTTP ${response.status}`)
    const transaction = await response.json()
    const value = transaction?.vout?.[Number(utxo.vout)]?.value
    if (!Number.isFinite(value) || value <= 0) throw new Error(`Transaction ${utxo.txid} output ${utxo.vout} has no positive value`)
    // Electrs/mempool returns vout.value in satoshis, not BTC.
    return {...utxo, amount_sats: Math.round(value), confirmed: true}
  }))
}

function parseDate(value, field) {
  if (value === undefined || value === null) return undefined
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) throw new Error(`Invalid ${field}`)
  return date
}

async function readJson(req) {
  const chunks = []
  for await (const chunk of req) {
    chunks.push(chunk)
  }
  if (chunks.length === 0) {
    return {}
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'))
}

function feeToMsat(fee) {
  if (!fee || fee.originalValue === undefined || !fee.originalUnit) {
    return null
  }
  const value = Number(fee.originalValue)
  if (!Number.isFinite(value)) {
    return null
  }
  switch (fee.originalUnit) {
    case 'MILLISATOSHI':
      return BigInt(Math.round(value)).toString()
    case 'SATOSHI':
      return BigInt(Math.round(value * 1000)).toString()
    case 'BITCOIN':
      return BigInt(Math.round(value * 100_000_000_000)).toString()
    default:
      return BigInt(Math.round(value * 1000)).toString()
  }
}

function setMnemonic(nextMnemonic) {
  if (!nextMnemonic) {
    return {status: 'missing'}
  }
  if (mnemonic) {
    if (mnemonic === nextMnemonic) {
      return {status: 'already_set'}
    }
    return {status: 'conflict'}
  }
  mnemonic = nextMnemonic
  mnemonicReadyResolve()
  return {status: 'set'}
}

const SEND_SUCCESS_STATUSES = new Set([
  'LIGHTNING_PAYMENT_SUCCEEDED',
  'TRANSFER_COMPLETED',
  'PREIMAGE_PROVIDED'
])
const SEND_FAILURE_STATUSES = new Set([
  'LIGHTNING_PAYMENT_FAILED',
  'TRANSFER_FAILED',
  'PREIMAGE_PROVIDING_FAILED',
  'USER_TRANSFER_VALIDATION_FAILED',
  'USER_SWAP_RETURN_FAILED'
])
const SEND_PENDING_STATUSES = new Set([
  'CREATED',
  'REQUEST_VALIDATED',
  'LIGHTNING_PAYMENT_INITIATED',
  'PENDING_USER_SWAP_RETURN',
  'USER_SWAP_RETURNED'
])
const RECEIVE_SUCCESS_STATUSES = new Set([
  'LIGHTNING_PAYMENT_RECEIVED',
  'TRANSFER_COMPLETED',
  'PAYMENT_PREIMAGE_RECOVERED'
])
const RECEIVE_FAILURE_STATUSES = new Set([
  'TRANSFER_FAILED',
  'PAYMENT_PREIMAGE_RECOVERING_FAILED',
  'REFUND_SIGNING_FAILED',
  'REFUND_SIGNING_COMMITMENTS_QUERYING_FAILED',
  'TRANSFER_CREATION_FAILED'
])
const RECEIVE_PENDING_STATUSES = new Set([
  'INVOICE_CREATED',
  'TRANSFER_CREATED'
])

function classifySendStatus(status) {
  if (SEND_SUCCESS_STATUSES.has(status)) return 'success'
  if (SEND_FAILURE_STATUSES.has(status)) return 'failed'
  if (SEND_PENDING_STATUSES.has(status)) return 'pending'
  console.warn(`Unknown Spark send payment status: ${status}`)
  return 'unknown'
}

function classifyReceiveStatus(status) {
  if (RECEIVE_SUCCESS_STATUSES.has(status)) return 'success'
  if (RECEIVE_FAILURE_STATUSES.has(status)) return 'failed'
  if (RECEIVE_PENDING_STATUSES.has(status)) return 'pending'
  console.warn(`Unknown Spark receive invoice status: ${status}`)
  return 'unknown'
}

function isSendTerminal(status) {
  return SEND_SUCCESS_STATUSES.has(status) || SEND_FAILURE_STATUSES.has(status)
}

async function waitForSendStatus(wallet, requestId, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const payment = await wallet.getLightningSendRequest(requestId)
    if (payment && isSendTerminal(payment.status)) {
      return payment
    }
    await new Promise(resolve => setTimeout(resolve, PAY_POLL_MS))
  }
  return null
}

function enqueueTransferLookup(transferId) {
  if (pendingTransferIds.has(transferId)) {
    return
  }
  pendingTransferIds.add(transferId)
  if (transferQueue.length >= TRANSFER_QUEUE_MAX) {
    const dropped = transferQueue.shift()
    if (dropped) {
      pendingTransferIds.delete(dropped)
      droppedTransfers += 1
      const now = Date.now()
      if (now - lastDropLog > DROP_LOG_INTERVAL_MS) {
        console.warn(
          `Dropping transfer events due to queue pressure: ${droppedTransfers}`
        )
        lastDropLog = now
      }
    }
  }
  transferQueue.push(transferId)
  processTransferQueue()
}

function processTransferQueue() {
  while (
    activeTransferLookups < TRANSFER_LOOKUP_CONCURRENCY &&
    transferQueue.length > 0
  ) {
    const transferId = transferQueue.shift()
    activeTransferLookups += 1
    void handleTransferLookup(transferId).finally(() => {
      activeTransferLookups -= 1
      pendingTransferIds.delete(transferId)
      processTransferQueue()
    })
  }
}

function pruneEmittedInvoiceCache(now) {
  if (INVOICE_CACHE_TTL_MS <= 0) {
    return
  }
  for (const [invoiceId, timestamp] of emittedInvoiceIds) {
    if (now - timestamp > INVOICE_CACHE_TTL_MS) {
      emittedInvoiceIds.delete(invoiceId)
    }
  }
}

async function pollInvoiceUpdates() {
  if (invoicePollInFlight || sseClients.size === 0) {
    return
  }
  invoicePollInFlight = true
  try {
    const now = Date.now()
    pruneEmittedInvoiceCache(now)
    const wallet = walletInstance || (await getWallet())
    let maxSeenUpdatedAtMs = lastSeenUpdatedAtMs
    let hasEntity = false
    let cursor = undefined
    let reachedKnown = false
    let isFirstPage = true
    while (true) {
      const response = await wallet.getUserRequests({
        first: INVOICE_POLL_LIMIT,
        after: cursor,
        types: ['LIGHTNING_RECEIVE'],
        statuses: ['SUCCEEDED']
      })
      const entities = response?.entities || []
      console.log(
        `Polled ${entities.length} lightning receive requests (cursor: ${cursor})`
      )
      for (const request of entities) {
        if (!request || request.typename !== 'LightningReceiveRequest') {
          continue
        }
        if (!RECEIVE_SUCCESS_STATUSES.has(request.status)) {
          continue
        }
        const updatedAtMs = getRequestUpdatedAtMs(request)
        hasEntity = true
        if (updatedAtMs > maxSeenUpdatedAtMs) {
          maxSeenUpdatedAtMs = updatedAtMs
        }
        if (!baselineInitialized && !stateLoaded && lastSeenUpdatedAtMs === 0) {
          continue
        }
        if (updatedAtMs && updatedAtMs <= lastSeenUpdatedAtMs) {
          reachedKnown = true
          continue
        }
        if (rememberInvoiceEmitted(request.id, now)) {
          continue
        }
        const invoice = request.invoice || {}
        sendSseEvent({
          checking_id: request.id,
          payment_hash: invoice.paymentHash || null,
          status: request.status
        })
      }

      if (
        isFirstPage &&
        !baselineInitialized &&
        !stateLoaded &&
        lastSeenUpdatedAtMs === 0
      ) {
        baselineInitialized = true
        if (hasEntity && maxSeenUpdatedAtMs > lastSeenUpdatedAtMs) {
          lastSeenUpdatedAtMs = maxSeenUpdatedAtMs
          scheduleStatePersist()
        }
        return
      }

      const pageInfo = response?.pageInfo || {}
      cursor = pageInfo.endCursor
      if (!pageInfo.hasNextPage || !cursor || reachedKnown) {
        break
      }
      isFirstPage = false
    }

    if (maxSeenUpdatedAtMs > lastSeenUpdatedAtMs) {
      lastSeenUpdatedAtMs = maxSeenUpdatedAtMs
      scheduleStatePersist()
    }
  } catch (error) {
    console.error('Error polling lightning invoices:', error)
  } finally {
    invoicePollInFlight = false
  }
}

function stopInvoicePolling() {
  if (!invoicePollTimer) {
    return
  }
  clearInterval(invoicePollTimer)
  invoicePollTimer = null
}

async function handleTransferLookup(transferId) {
  try {
    const wallet = walletInstance || (await getWallet())
    const transfer = await wallet.getTransferFromSsp(transferId)
    const userRequest = transfer?.userRequest
    if (!userRequest || userRequest.typename !== 'LightningReceiveRequest') {
      return
    }
    if (!RECEIVE_SUCCESS_STATUSES.has(userRequest.status)) {
      return
    }
    const updatedAtMs = getRequestUpdatedAtMs(userRequest)
    if (updatedAtMs && updatedAtMs <= lastSeenUpdatedAtMs) {
      return
    }
    if (rememberInvoiceEmitted(userRequest.id)) {
      return
    }
    const invoice = userRequest.invoice || {}
    sendSseEvent({
      checking_id: userRequest.id,
      payment_hash: invoice.paymentHash || null,
      status: userRequest.status
    })
    if (updatedAtMs > lastSeenUpdatedAtMs) {
      lastSeenUpdatedAtMs = updatedAtMs
      scheduleStatePersist()
    }
  } catch (error) {
    console.error('Error handling transfer event:', error)
  }
}

function sendSseEvent(payload) {
  console.log('Sending SSE event')
  const data = `data: ${JSON.stringify(payload)}\n\n`
  for (const res of sseClients) {
    try {
      res.write(data)
    } catch (error) {
      removeSseClient(res)
    }
  }
}

function addSseClient(res) {
  res.writeHead(200, {
    'content-type': 'text/event-stream',
    'cache-control': 'no-cache',
    connection: 'keep-alive',
    'x-accel-buffering': 'no'
  })
  res.write(':\n\n')
  sseClients.add(res)

  if (STREAM_KEEPALIVE_MS > 0) {
    const timer = setInterval(() => {
      try {
        res.write(':\n\n')
      } catch (error) {
        removeSseClient(res)
      }
    }, STREAM_KEEPALIVE_MS)
    sseKeepaliveTimers.set(res, timer)
  }

  if (STREAM_HEARTBEAT_MS > 0) {
    const timer = setInterval(() => {
      try {
        res.write(
          `data: ${JSON.stringify({type: 'heartbeat', ts: Date.now()})}\n\n`
        )
      } catch (error) {
        removeSseClient(res)
      }
    }, STREAM_HEARTBEAT_MS)
    sseHeartbeatTimers.set(res, timer)
  }

  res.on('close', () => {
    removeSseClient(res)
  })
}

function removeSseClient(res) {
  if (!sseClients.has(res)) {
    return
  }
  sseClients.delete(res)
  const timer = sseKeepaliveTimers.get(res)
  if (timer) {
    clearInterval(timer)
  }
  sseKeepaliveTimers.delete(res)
  const heartbeatTimer = sseHeartbeatTimers.get(res)
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer)
  }
  sseHeartbeatTimers.delete(res)

  if (sseClients.size === 0) {
    stopInvoicePolling()
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(
    req.url || '/',
    `http://${req.headers.host || 'localhost'}`
  )

  if (API_KEY && req.headers['x-api-key'] !== API_KEY) {
    console.log('Unauthorized request with invalid API key')
    return sendJson(res, 401, {error: 'Unauthorized'})
  }

  console.log(`${req.method} ${url.pathname}`)
  try {
    if (req.method === 'GET' && url.pathname === '/v1/capabilities') {
      return sendJson(res, 200, {
        service: 'spark-sidecar',
        sidecar_version: SIDECAR_VERSION,
        api_contract: 'spark-sidecar-v1',
        api_version: SIDECAR_API_VERSION,
        sdk: {name: '@buildonspark/spark-sdk', version: SDK_VERSION},
        network: NETWORK,
        capabilities: SIDECAR_CAPABILITIES
      })
    }

    if (req.method === 'GET' && url.pathname === '/health') {
      return sendJson(res, 200, {status: 'ok'})
    }

    if (req.method === 'POST' && url.pathname === '/v1/mnemonic') {
      const body = await readJson(req)
      const provided = body.mnemonic || body.mnemonic_or_seed || ''
      const result = setMnemonic(provided)
      if (result.status === 'missing') {
        return sendJson(res, 400, {error: 'Missing mnemonic'})
      }
      if (result.status === 'conflict') {
        return sendJson(res, 409, {error: 'Mnemonic already set'})
      }
      return sendJson(res, 200, {status: result.status})
    }

    if (req.method === 'GET' && url.pathname === '/v1/invoices/stream') {
      await getWallet()
      addSseClient(res)
      return
    }

    if (req.method === 'POST' && url.pathname === '/v1/balance') {
      if (!mnemonic) {
        return sendJson(res, 200, {status: 'missing_mnemonic'})
      }
      const wallet = await getWallet()
      const balance = await wallet.getBalance()
      const sats = BigInt(balance.satsBalance?.available ?? balance.balance)
      return sendJson(res, 200, {
        balance_sats: sats.toString(),
        balance_msat: (sats * 1000n).toString(),
        sats_balance: balance.satsBalance,
        token_balances: balance.tokenBalances,
        status: 'ok'
      })
    }

    if (req.method === 'POST' && url.pathname === '/v1/invoices') {
      const wallet = await getWallet()
      const body = await readJson(req)
      const amountSats = Number(body.amount_sats)
      if (!Number.isFinite(amountSats) || amountSats < 0) {
        return sendJson(res, 400, {error: 'Invalid amount_sats'})
      }
      const invoice = await wallet.createLightningInvoice({
        amountSats,
        memo: body.memo || undefined,
        descriptionHash: body.description_hash || undefined,
        expirySeconds: body.expiry_seconds || undefined
      })
      return sendJson(res, 200, {
        checking_id: invoice.id,
        payment_request: invoice.invoice.encodedInvoice,
        payment_hash: invoice.invoice.paymentHash,
        status: invoice.status,
        status_class: classifyReceiveStatus(invoice.status),
        preimage: invoice.paymentPreimage || null
      })
    }

    if (req.method === 'POST' && url.pathname === '/v1/payments') {
      const wallet = await getWallet()
      const body = await readJson(req)
      const bolt11 = body.bolt11
      if (!bolt11) {
        return sendJson(res, 400, {error: 'Missing bolt11'})
      }
      const maxFeeSats = Number(body.max_fee_sats || 0)
      const amountSatsToSend = body.amount_sats
        ? Number(body.amount_sats)
        : undefined
      const paymentHash = body.payment_hash || null
      try {
        let payment = await wallet.payLightningInvoice({
          invoice: bolt11,
          maxFeeSats,
          amountSatsToSend
        })
        if (
          PAY_WAIT_MS > 0 &&
          payment &&
          payment.id &&
          !isSendTerminal(payment.status)
        ) {
          const refreshed = await waitForSendStatus(
            wallet,
            payment.id,
            PAY_WAIT_MS
          )
          if (refreshed) {
            payment = refreshed
          }
        }
        if (paymentHash && payment?.id) {
          paymentHashToRequestId.set(paymentHash, payment.id)
        }
        return sendJson(res, 200, {
          checking_id: payment.id,
          payment_hash: paymentHash,
          status: payment.status,
          status_class: classifySendStatus(payment.status),
          fee_msat: feeToMsat(payment.fee),
          preimage: payment.paymentPreimage || null
        })
      } catch (error) {
        console.error('Error processing payment:', error)
        const message =
          error && typeof error === 'object' && 'initialMessage' in error
            ? error.initialMessage
            : error instanceof Error
              ? error.message
              : String(error)
        message == '' && (message = 'Payment failed')

        return sendJson(res, 500, {error: message})
      }
    }

    if (req.method === 'GET' && url.pathname === '/v1/identity') {
      const wallet = await requireWallet()
      return sendJson(res, 200, {identity_public_key: await wallet.getIdentityPublicKey(), spark_address: await wallet.getSparkAddress()})
    }
    if (req.method === 'GET' && url.pathname === '/v1/deposit/single-use') {
      const wallet = await requireWallet(); return sendJson(res, 200, {address: await wallet.getSingleUseDepositAddress()})
    }
    if (req.method === 'GET' && url.pathname === '/v1/deposit/static') {
      const wallet = await requireWallet(); return sendJson(res, 200, {address: await wallet.getStaticDepositAddress()})
    }
    if (req.method === 'GET' && url.pathname === '/v1/deposit/static/addresses') {
      const wallet = await requireWallet(); return sendJson(res, 200, {addresses: await wallet.queryStaticDepositAddresses()})
    }
    if (req.method === 'POST' && url.pathname === '/v1/deposit/utxos') {
      const wallet = await requireWallet(); const body = await readJson(req)
      if (Array.isArray(body.addresses)) {
        const result = await wallet.getUtxosForDepositAddresses(body)
        return sendJson(res, 200, {...result, utxos: await enrichDepositUtxos(result.utxos)})
      }
      const utxos = await wallet.getUtxosForDepositAddress(body.address, body.limit, body.offset, body.exclude_claimed)
      return sendJson(res, 200, {utxos: await enrichDepositUtxos(utxos)})
    }
    if (req.method === 'POST' && url.pathname === '/v1/deposit/claim') {
      const wallet = await requireWallet(); const body = await readJson(req)
      if (!body.txid) return sendJson(res, 400, {error: 'Missing txid'})
      return sendJson(res, 200, await wallet.claimDeposit(body.txid))
    }
    if (req.method === 'POST' && url.pathname === '/v1/transfer') {
      const wallet = await requireWallet(); const body = await readJson(req)
      return sendJson(res, 200, await wallet.transfer({amountSats: Number(body.amount_sats), receiverSparkAddress: body.receiver_spark_address}))
    }
    if (req.method === 'POST' && url.pathname === '/v1/transfers/list') {
      const wallet = await requireWallet(); const body = await readJson(req)
      const result = await wallet.getTransfers(body.limit, body.offset, parseDate(body.created_after, 'created_after'), parseDate(body.created_before, 'created_before'))
      return sendJson(res, 200, {...result, transfers: (result?.transfers || []).map(normalizeTransfer)})
    }
    if (req.method === 'POST' && url.pathname === '/v1/transfer/get') {
      const wallet = await requireWallet(); const body = await readJson(req); return sendJson(res, 200, await wallet.getTransfer(body.id))
    }
    if (req.method === 'POST' && url.pathname === '/v1/transfer/ssp') {
      const wallet = await requireWallet(); const body = await readJson(req); return sendJson(res, 200, await wallet.getTransferFromSsp(body.id))
    }
    if (req.method === 'POST' && url.pathname === '/v1/withdraw/quote') {
      const wallet = await requireWallet(); const body = await readJson(req)
      return sendJson(res, 200, await wallet.getWithdrawalFeeQuote({amountSats: Number(body.amount_sats), withdrawalAddress: body.withdrawal_address}))
    }
    if (req.method === 'POST' && url.pathname === '/v1/withdraw') {
      const wallet = await requireWallet(); const body = await readJson(req)
      return sendJson(res, 200, await wallet.withdraw({onchainAddress: body.onchain_address, amountSats: body.amount_sats === undefined ? undefined : Number(body.amount_sats), exitSpeed: body.exit_speed, feeQuote: body.fee_quote, feeAmountSats: body.fee_amount_sats === undefined ? undefined : Number(body.fee_amount_sats), feeQuoteId: body.fee_quote_id, deductFeeFromWithdrawalAmount: body.deduct_fee_from_withdrawal_amount ?? true}))
    }
    if (req.method === 'POST' && url.pathname === '/v1/withdraw/get') {
      const wallet = await requireWallet(); const body = await readJson(req); return sendJson(res, 200, await wallet.getCoopExitRequest(body.id))
    }
    if (req.method === 'GET' && url.pathname === '/v1/tokens/l1-address') {
      const wallet = await requireWallet(); return sendJson(res, 200, {address: await wallet.getTokenL1Address()})
    }
    if (req.method === 'POST' && url.pathname === '/v1/tokens/transfer') {
      const wallet = await requireWallet(); const body = await readJson(req)
      const txid = await wallet.transferTokens({tokenIdentifier: body.token_identifier, tokenAmount: BigInt(body.token_amount), receiverSparkAddress: body.receiver_spark_address, outputSelectionStrategy: body.output_selection_strategy})
      return sendJson(res, 200, {transaction_id: txid, status: 'submitted'})
    }
    if (req.method === 'POST' && url.pathname === '/v1/tokens/transactions') {
      const wallet = await requireWallet(); const body = await readJson(req)
      if (Array.isArray(body.transaction_hashes)) return sendJson(res, 200, await wallet.queryTokenTransactionsByTxHashes(body.transaction_hashes))
      return sendJson(res, 200, await wallet.queryTokenTransactionsWithFilters(body))
    }
    if (req.method === 'POST' && url.pathname === '/v1/tokens/invoice') {
      const wallet = await requireWallet(); const body = await readJson(req)
      return sendJson(res, 200, {spark_invoice: await wallet.createTokensInvoice({amount: body.amount === undefined ? undefined : BigInt(body.amount), tokenIdentifier: body.token_identifier, memo: body.memo, senderSparkAddress: body.sender_spark_address, expiryTime: parseDate(body.expiry_time, 'expiry_time')})})
    }
    if (req.method === 'POST' && url.pathname === '/v1/sats/invoice') {
      const wallet = await requireWallet(); const body = await readJson(req)
      return sendJson(res, 200, {spark_invoice: await wallet.createSatsInvoice({amount: body.amount === undefined ? undefined : Number(body.amount), memo: body.memo, senderSparkAddress: body.sender_spark_address, expiryTime: parseDate(body.expiry_time, 'expiry_time')})})
    }
    if (req.method === 'POST' && url.pathname === '/v1/status/optimization') {
      const wallet = await requireWallet(); return sendJson(res, 200, {optimization_in_progress: await wallet.isOptimizationInProgress(), token_optimization_in_progress: await wallet.isTokenOptimizationInProgress()})
    }
    if (req.method === 'GET' && url.pathname === '/v1/settings') {
      const wallet = await requireWallet(); return sendJson(res, 200, await wallet.getWalletSettings())
    }

    const parts = url.pathname.split('/').filter(Boolean)
    if (parts.length === 3 && parts[0] === 'v1' && parts[1] === 'invoices') {
      const wallet = await getWallet()
      const invoice = await wallet.getLightningReceiveRequest(parts[2])
      if (!invoice) {
        return sendJson(res, 404, {error: 'Not found'})
      }
      return sendJson(res, 200, {
        checking_id: invoice.id,
        status: invoice.status,
        status_class: classifyReceiveStatus(invoice.status),
        payment_hash: invoice.invoice.paymentHash,
        preimage: invoice.paymentPreimage || null
      })
    }

    if (parts.length === 3 && parts[0] === 'v1' && parts[1] === 'payments') {
      const wallet = await getWallet()
      const requestedId = parts[2]
      const lookupId = paymentHashToRequestId.get(requestedId)

      if (!lookupId && /^[0-9a-fA-F]{64}$/.test(requestedId)) {
        return sendJson(res, 404, {
          error: 'Payment hash not mapped to Spark payment request ID',
          checking_id: requestedId
        })
      }

      const payment = await wallet.getLightningSendRequest(lookupId || requestedId)
      if (!payment) {
        return sendJson(res, 404, {error: 'Not found'})
      }

      return sendJson(res, 200, {
        checking_id: requestedId,
        status: payment.status,
        status_class: classifySendStatus(payment.status),
        fee_msat: feeToMsat(payment.fee),
        preimage: payment.paymentPreimage || null
      })
    }

    return sendJson(res, 404, {error: 'Not found'})
  } catch (error) {
    console.error('Error handling request:', error)
    const message = error instanceof Error ? error.message : String(error)
    return sendJson(res, 500, {error: message})
  }
})

server.listen(PORT, HOST, () => {
  console.log(`Spark sidecar listening on ${HOST}:${PORT}`)
})

server.on('error', err => {
  if (err && err.code === 'EADDRINUSE') {
    console.error(`Spark sidecar port ${HOST}:${PORT} already in use.`)
    process.exit(1)
  }
  throw err
})
