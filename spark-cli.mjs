#!/usr/bin/env node
import readline from 'node:readline'

const BASE_URL = (process.env.SPARK_SIDECAR_URL || 'http://127.0.0.1:8765').replace(/\/$/, '')
const API_KEY = process.env.SPARK_SIDECAR_API_KEY || ''

function usage() {
  console.error(`Usage:
  spark-cli health [--json]
  spark-cli balance [--json]
  spark-cli invoice create --amount-sats N [--memo TEXT] [--expiry-seconds N] [--json]
  spark-cli invoice get ID [--json]
  spark-cli invoice stream [--json]
  spark-cli payment send --bolt11 INVOICE [--max-fee-sats N] [--amount-sats N] [--yes] [--json]
  spark-cli payment get ID [--json]
  spark-cli mnemonic set --stdin [--json]

Environment:
  SPARK_SIDECAR_URL       Sidecar base URL (default: http://127.0.0.1:8765)
  SPARK_SIDECAR_API_KEY   Sidecar API key

The payment send command requires --yes when stdin is not a terminal.`)
  process.exit(2)
}
function fail(message, code = 2) { console.error(`error: ${message}`); process.exit(code) }
function value(args, flag, required = false) {
  const i = args.indexOf(flag)
  if (i < 0) { if (required) fail(`missing ${flag}`); return undefined }
  if (i + 1 >= args.length || args[i + 1].startsWith('--')) fail(`missing value for ${flag}`)
  return args[i + 1]
}
function has(args, flag) { return args.includes(flag) }
function jsonMode(args) { return has(args, '--json') }
function output(payload, args) {
  if (jsonMode(args)) { console.log(JSON.stringify(payload, null, 2)); return }
  if (payload && typeof payload === 'object') for (const [key, val] of Object.entries(payload)) if (val !== undefined && val !== null) console.log(`${key}: ${typeof val === 'object' ? JSON.stringify(val) : val}`)
  else console.log(payload)
}
async function request(method, path, body) {
  const headers = {accept: 'application/json'}
  if (API_KEY) headers['x-api-key'] = API_KEY
  if (body !== undefined) headers['content-type'] = 'application/json'
  const response = await fetch(`${BASE_URL}${path}`, {method, headers, body: body === undefined ? undefined : JSON.stringify(body)})
  const text = await response.text()
  let payload
  try { payload = text ? JSON.parse(text) : {} } catch { payload = {raw: text} }
  if (!response.ok) { const error = new Error(payload?.error || `${response.status} ${response.statusText}`); error.status = response.status; error.payload = payload; throw error }
  return payload
}
function numberArg(args, flag, fallback, required = false) {
  const raw = value(args, flag)
  if (raw === undefined) { if (required) fail(`missing ${flag}`); return fallback }
  const parsed = Number(raw)
  if (!Number.isFinite(parsed) || !Number.isInteger(parsed) || parsed < 0) fail(`invalid ${flag}: ${raw}`)
  return parsed
}
async function readStdin() { const chunks = []; for await (const chunk of process.stdin) chunks.push(chunk); return Buffer.concat(chunks).toString().trim() }
async function confirmSend(args) {
  if (has(args, '--yes')) return
  if (!process.stdin.isTTY || !process.stdout.isTTY) fail('payment send requires --yes when stdin/stdout is not a terminal')
  const rl = readline.createInterface({input: process.stdin, output: process.stdout})
  const answer = await new Promise(resolve => rl.question('Send this Lightning payment? Type yes to continue: ', resolve)); rl.close()
  if (answer.trim().toLowerCase() !== 'yes') fail('payment not sent', 1)
}
async function streamInvoices(args) {
  const headers = {}; if (API_KEY) headers['x-api-key'] = API_KEY
  const response = await fetch(`${BASE_URL}/v1/invoices/stream`, {headers})
  if (!response.ok || !response.body) fail(`stream failed: ${response.status} ${response.statusText}`, 1)
  const decoder = new TextDecoder(); let buffer = ''
  for await (const chunk of response.body) {
    buffer += decoder.decode(chunk, {stream: true})
    const records = buffer.split('\n\n'); buffer = records.pop() || ''
    for (const record of records) {
      const data = record.split('\n').filter(line => line.startsWith('data:')).map(line => line.slice(5).trim()).join('\n')
      if (!data) continue
      try { output(JSON.parse(data), args) } catch { console.log(data) }
    }
  }
}
async function main() {
  const args = process.argv.slice(2); const resource = args.shift(); let action = args.shift()
  if (action && action.startsWith('--')) { args.unshift(action); action = undefined }
  if (!resource || has(args, '--help') || has(args, '-h')) usage()
  let result
  if (resource === 'health' && action === undefined) result = await request('GET', '/health')
  else if (resource === 'balance' && action === undefined) result = await request('POST', '/v1/balance', {})
  else if (resource === 'invoice' && action === 'create') result = await request('POST', '/v1/invoices', {amount_sats: numberArg(args, '--amount-sats', undefined, true), memo: value(args, '--memo'), expiry_seconds: numberArg(args, '--expiry-seconds')})
  else if (resource === 'invoice' && action === 'get') { const id = args.shift() || fail('missing invoice ID'); result = await request('GET', `/v1/invoices/${encodeURIComponent(id)}`) }
  else if (resource === 'invoice' && action === 'stream') { await streamInvoices(args); return }
  else if (resource === 'payment' && action === 'send') { await confirmSend(args); result = await request('POST', '/v1/payments', {bolt11: value(args, '--bolt11', true), max_fee_sats: numberArg(args, '--max-fee-sats', 0), amount_sats: numberArg(args, '--amount-sats')}) }
  else if (resource === 'payment' && action === 'get') { const id = args.shift() || fail('missing payment ID'); result = await request('GET', `/v1/payments/${encodeURIComponent(id)}`) }
  else if (resource === 'mnemonic' && action === 'set' && has(args, '--stdin')) { const mnemonic = await readStdin(); if (!mnemonic) fail('empty mnemonic on stdin'); result = await request('POST', '/v1/mnemonic', {mnemonic}) }
  else usage()
  output(result, args)
}
main().catch(error => { if (error.payload && jsonMode(process.argv.slice(2))) console.error(JSON.stringify(error.payload, null, 2)); else console.error(`error: ${error.message}`); process.exit(1) })
