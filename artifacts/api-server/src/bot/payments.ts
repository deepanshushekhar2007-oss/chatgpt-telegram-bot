import { createHmac } from "crypto";
import { getCollection } from "./mongodb";

// ─── Types ────────────────────────────────────────────────────────────────────

export interface PaymentPlan {
  _id: string;
  name: string;
  days: number;
  priceUsdt: number;
  active: boolean;
  createdAt: number;
}

export interface PaymentSettings {
  binanceUid: string;
  binanceApiKey: string;
  binanceApiSecret: string;
  bep20Address: string;
}

export interface PaymentTransaction {
  _id: string;
  userId: number;
  planId: string;
  planName: string;
  days: number;
  priceUsdt: number;
  txId: string;
  status: "verified" | "failed";
  createdAt: number;
  accessGrantedUntil: number;
}

// ─── Plans ────────────────────────────────────────────────────────────────────

function docToPlan(doc: any): PaymentPlan {
  return {
    _id: String(doc._id),
    name: doc.name,
    days: doc.days,
    priceUsdt: doc.priceUsdt,
    active: doc.active ?? true,
    createdAt: doc.createdAt ?? Date.now(),
  };
}

export async function getActivePlans(): Promise<PaymentPlan[]> {
  try {
    const col = await getCollection("payment_plans");
    const docs = await col.find({ active: true }).sort({ priceUsdt: 1 }).toArray();
    return docs.map(docToPlan);
  } catch { return []; }
}

export async function getAllPlans(): Promise<PaymentPlan[]> {
  try {
    const col = await getCollection("payment_plans");
    const docs = await col.find({}).sort({ priceUsdt: 1 }).toArray();
    return docs.map(docToPlan);
  } catch { return []; }
}

export async function getPlan(planId: string): Promise<PaymentPlan | null> {
  try {
    const col = await getCollection("payment_plans");
    const doc = await col.findOne({ _id: planId as any });
    return doc ? docToPlan(doc) : null;
  } catch { return null; }
}

export async function createPlan(name: string, days: number, priceUsdt: number): Promise<PaymentPlan> {
  const col = await getCollection("payment_plans");
  const id = `plan_${Date.now()}`;
  const plan: PaymentPlan = { _id: id, name, days, priceUsdt, active: true, createdAt: Date.now() };
  await col.insertOne({ ...plan, _id: id as any });
  return plan;
}

export async function updatePlan(planId: string, updates: Partial<Omit<PaymentPlan, "_id" | "createdAt">>): Promise<boolean> {
  try {
    const col = await getCollection("payment_plans");
    const res = await col.updateOne({ _id: planId as any }, { $set: updates });
    return (res.modifiedCount ?? 0) > 0;
  } catch { return false; }
}

export async function deletePlan(planId: string): Promise<boolean> {
  try {
    const col = await getCollection("payment_plans");
    const res = await col.deleteOne({ _id: planId as any });
    return (res.deletedCount ?? 0) > 0;
  } catch { return false; }
}

// ─── Settings ─────────────────────────────────────────────────────────────────

const DEFAULT_SETTINGS: PaymentSettings = {
  binanceUid: "",
  binanceApiKey: "",
  binanceApiSecret: "",
  bep20Address: "",
};

export async function getPaymentSettings(): Promise<PaymentSettings> {
  try {
    const col = await getCollection("payment_settings");
    const doc = await col.findOne({ _id: "main" as any });
    if (!doc) return { ...DEFAULT_SETTINGS };
    return {
      binanceUid: doc["binanceUid"] ?? "",
      binanceApiKey: doc["binanceApiKey"] ?? "",
      binanceApiSecret: doc["binanceApiSecret"] ?? "",
      bep20Address: doc["bep20Address"] ?? "",
    };
  } catch { return { ...DEFAULT_SETTINGS }; }
}

export async function savePaymentSettings(updates: Partial<PaymentSettings>): Promise<void> {
  try {
    const col = await getCollection("payment_settings");
    await col.updateOne({ _id: "main" as any }, { $set: updates }, { upsert: true });
  } catch (err: any) {
    console.error("[PAY] savePaymentSettings error:", err?.message);
  }
}

// ─── Transactions ─────────────────────────────────────────────────────────────

export async function isTxIdUsed(txId: string): Promise<boolean> {
  try {
    const col = await getCollection("payment_transactions");
    const doc = await col.findOne({ txId, status: "verified" });
    return !!doc;
  } catch { return false; }
}

export async function saveTransaction(tx: Omit<PaymentTransaction, "_id">): Promise<string> {
  try {
    const col = await getCollection("payment_transactions");
    const id = `tx_${Date.now()}_${tx.userId}`;
    await col.insertOne({ ...tx, _id: id as any });
    return id;
  } catch { return ""; }
}

export async function getTransactions(limit = 50): Promise<PaymentTransaction[]> {
  try {
    const col = await getCollection("payment_transactions");
    const docs = await col.find({ status: "verified" }).sort({ createdAt: -1 }).limit(limit).toArray();
    return docs.map((d: any) => ({
      _id: String(d._id),
      userId: d.userId,
      planId: d.planId,
      planName: d.planName,
      days: d.days,
      priceUsdt: d.priceUsdt,
      txId: d.txId,
      status: d.status,
      createdAt: d.createdAt,
      accessGrantedUntil: d.accessGrantedUntil,
    }));
  } catch { return []; }
}

export async function getTotalIncome(): Promise<number> {
  try {
    const col = await getCollection("payment_transactions");
    const docs = await col.find({ status: "verified" }).toArray();
    return docs.reduce((sum: number, d: any) => sum + (d.priceUsdt ?? 0), 0);
  } catch { return 0; }
}

export async function getTransactionCount(): Promise<number> {
  try {
    const col = await getCollection("payment_transactions");
    return await col.countDocuments({ status: "verified" });
  } catch { return 0; }
}

// ─── USDT BEP20 Verification (BSC RPC — no API key needed) ───────────────────

const BSC_RPC_URLS = [
  "https://bsc-dataseed.binance.org/",
  "https://bsc-dataseed1.binance.org/",
  "https://bsc-dataseed2.binance.org/",
];

// USDT contract on BSC (18 decimals)
const USDT_BEP20_CONTRACT = "0x55d398326f99059ff775485246999027b3197955";
// ERC-20 Transfer(address indexed from, address indexed to, uint256 value)
const TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

export interface Bep20VerifyResult {
  valid: boolean;
  amount?: number;
  error?: string;
}

async function rpcCall(method: string, params: any[]): Promise<any> {
  let lastErr: any;
  for (const url of BSC_RPC_URLS) {
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
        signal: AbortSignal.timeout(10_000),
      });
      if (!res.ok) { lastErr = new Error(`HTTP ${res.status}`); continue; }
      const json = await res.json() as any;
      if (json.error) { lastErr = new Error(json.error.message ?? "RPC error"); continue; }
      return json.result;
    } catch (err: any) {
      lastErr = err;
    }
  }
  throw lastErr ?? new Error("All BSC RPC nodes failed");
}

export async function verifyBep20TxHash(
  txHash: string,
  expectedToAddress: string,
  expectedUsdt: number
): Promise<Bep20VerifyResult> {
  try {
    if (!/^0x[0-9a-fA-F]{64}$/.test(txHash)) {
      return { valid: false, error: "Invalid transaction hash format. It must start with 0x and be 66 characters long." };
    }

    const receipt = await rpcCall("eth_getTransactionReceipt", [txHash]);
    if (!receipt) {
      return { valid: false, error: "Transaction not found on BSC. Make sure you used the BNB Smart Chain (BEP20) network and the hash is correct." };
    }
    if (receipt.status !== "0x1") {
      return { valid: false, error: "Transaction failed on blockchain (status not successful). Please check your transaction on BSCScan." };
    }

    const toLower = expectedToAddress.toLowerCase();
    const logs: any[] = receipt.logs ?? [];

    // Find a Transfer log from USDT contract to our wallet
    const transferLog = logs.find((log: any) => {
      if (!log.address || log.address.toLowerCase() !== USDT_BEP20_CONTRACT) return false;
      if (!Array.isArray(log.topics) || log.topics.length < 3) return false;
      if (log.topics[0]?.toLowerCase() !== TRANSFER_TOPIC) return false;
      // topics[2] = to address padded to 32 bytes → last 40 hex chars = address
      const toAddr = "0x" + log.topics[2].slice(-40).toLowerCase();
      return toAddr === toLower;
    });

    if (!transferLog) {
      return { valid: false, error: `No USDT (BEP20) transfer to your wallet address found in this transaction. Make sure you sent to the correct address on BNB Smart Chain.` };
    }

    // Decode amount: data field is uint256 in hex (18 decimals)
    const rawAmount = BigInt(transferLog.data);
    const divisor = BigInt("1000000000000000000"); // 10^18
    const amount = Number(rawAmount * 10000n / divisor) / 10000;

    const tolerance = 0.011;
    if (Math.abs(amount - expectedUsdt) > tolerance) {
      return { valid: false, error: `Wrong amount: received ${amount.toFixed(4)} USDT but plan requires ${expectedUsdt} USDT. Send the exact amount.` };
    }

    return { valid: true, amount };
  } catch (err: any) {
    return { valid: false, error: `Verification error: ${err?.message ?? "BSC network unreachable, please try again."}` };
  }
}

// ─── Binance Pay Verification ─────────────────────────────────────────────────

export interface BinanceVerifyResult {
  valid: boolean;
  amount?: number;
  currency?: string;
  error?: string;
}

export async function verifyBinanceTxId(
  apiKey: string,
  apiSecret: string,
  txId: string,
  expectedUsdt: number
): Promise<BinanceVerifyResult> {
  try {
    const timestamp = Date.now();
    const recvWindow = 60000;
    const endTime = timestamp;
    const startTime = timestamp - 90 * 24 * 60 * 60 * 1000;

    const params = `startTime=${startTime}&endTime=${endTime}&limit=100&timestamp=${timestamp}&recvWindow=${recvWindow}`;
    const signature = createHmac("sha256", apiSecret).update(params).digest("hex");
    const url = `https://api.binance.com/sapi/v1/pay/transactions?${params}&signature=${signature}`;

    const res = await fetch(url, { headers: { "X-MBX-APIKEY": apiKey } });

    if (!res.ok) {
      const errBody = await res.text().catch(() => "");
      return { valid: false, error: `Binance API error ${res.status}: ${errBody.slice(0, 100)}` };
    }

    const json = await res.json() as any;
    if (json.status !== "SUCCESS" || !Array.isArray(json.data)) {
      return { valid: false, error: json.message || "Binance API returned an error. Check your API keys." };
    }

    const tx = json.data.find((t: any) =>
      String(t.transactionId) === String(txId)
    );

    if (!tx) {
      return { valid: false, error: "Transaction ID not found in your recent payments. Make sure you copied the correct TxID from Binance Pay history." };
    }

    if (tx.orderStatus !== "SUCCESS") {
      return { valid: false, error: `Transaction status is "${tx.orderStatus}" — only SUCCESS transactions are accepted.` };
    }

    const amount = parseFloat(tx.amount ?? "0");
    const tolerance = 0.01;
    if (Math.abs(amount - expectedUsdt) > tolerance) {
      return { valid: false, error: `Wrong amount: you sent ${amount} USDT but the plan requires ${expectedUsdt} USDT.` };
    }

    return { valid: true, amount, currency: tx.currency ?? "USDT" };
  } catch (err: any) {
    return { valid: false, error: `Verification error: ${err?.message}` };
  }
}
