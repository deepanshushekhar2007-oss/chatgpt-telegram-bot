/**
 * file-tools.ts
 * RAM-efficient file processing utilities for the VCF File Tools feature.
 * Extracts phone numbers immediately and discards raw buffers — keeping
 * only string[] in memory (~12 bytes per contact vs raw file bytes).
 */

import * as https from "https";
import * as http from "http";
import * as path from "path";

// ─── Phone cleaning ───────────────────────────────────────────────────────────

export function cleanPhone(raw: string): string {
  const s = raw.trim();
  if (!s) return "";
  let cleaned = s.replace(/[\s\-().]/g, "");
  cleaned = cleaned.replace(/[^0-9+]/g, "");
  if (!cleaned) return "";
  const digits = cleaned.replace(/^\+/, "");
  if (digits.length < 7 || digits.length > 15) return "";
  return cleaned;
}

// ─── Parsers ──────────────────────────────────────────────────────────────────

export function extractPhonesFromText(text: string): string[] {
  const seen = new Set<string>();
  const phones: string[] = [];
  for (const line of text.split(/[\r\n]+/)) {
    for (const part of line.split(/[,;\t|]/)) {
      const p = cleanPhone(part.trim());
      if (p && !seen.has(p)) { seen.add(p); phones.push(p); }
    }
  }
  return phones;
}

export function extractPhonesFromVCF(content: string): string[] {
  const seen = new Set<string>();
  const phones: string[] = [];
  for (const line of content.split(/\r?\n/)) {
    if (!line.toUpperCase().startsWith("TEL")) continue;
    const colonIdx = line.lastIndexOf(":");
    if (colonIdx < 0) continue;
    const raw = line.slice(colonIdx + 1).trim().replace(/^waid:/i, "");
    const p = cleanPhone(raw);
    if (p && !seen.has(p)) { seen.add(p); phones.push(p); }
  }
  return phones;
}

export function extractPhonesFromCSV(content: string): string[] {
  const seen = new Set<string>();
  const phones: string[] = [];
  const lines = content.split(/[\r\n]+/).filter(l => l.trim());
  for (const line of lines) {
    for (const col of line.split(/[,;\t]/)) {
      const trimmed = col.replace(/^["'\s]+|["'\s]+$/g, "");
      const p = cleanPhone(trimmed);
      if (p && !seen.has(p)) { seen.add(p); phones.push(p); }
    }
  }
  return phones;
}

export async function extractPhonesFromXLSX(buffer: Buffer): Promise<string[]> {
  const XLSX = await import("xlsx");
  const wb = XLSX.read(buffer, { type: "buffer" });
  const seen = new Set<string>();
  const phones: string[] = [];
  for (const sheetName of wb.SheetNames) {
    const ws = wb.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(ws, { header: 1 }) as unknown[][];
    for (const row of rows) {
      for (const cell of row) {
        const p = cleanPhone(String(cell ?? "").trim());
        if (p && !seen.has(p)) { seen.add(p); phones.push(p); }
      }
    }
  }
  return phones;
}

/** Parse any supported file format and extract phone numbers. */
export async function extractPhonesFromBuffer(buffer: Buffer, fileName: string): Promise<string[]> {
  const ext = path.extname(fileName).toLowerCase();
  if (ext === ".vcf") return extractPhonesFromVCF(buffer.toString("utf8"));
  if (ext === ".txt") return extractPhonesFromText(buffer.toString("utf8"));
  if (ext === ".csv" || ext === ".tsv") return extractPhonesFromCSV(buffer.toString("utf8"));
  if (ext === ".xlsx" || ext === ".xls" || ext === ".xlsm") return extractPhonesFromXLSX(buffer);
  // Fallback: treat as text
  return extractPhonesFromText(buffer.toString("utf8"));
}

// ─── VCF generation ───────────────────────────────────────────────────────────

function makeVCard(contactName: string, phone: string): string {
  const p = phone.startsWith("+") ? phone : `+${phone}`;
  return `BEGIN:VCARD\r\nVERSION:3.0\r\nFN:${contactName}\r\nTEL;TYPE=CELL:${p}\r\nEND:VCARD\r\n`;
}

/**
 * Build a full VCF file content from a list of phone numbers.
 * contactStartNum: the number of the FIRST contact (e.g. 1 → "Name 01").
 */
export function buildVCFContent(
  phones: string[],
  contactBaseName: string,
  contactStartNum: number
): string {
  return phones
    .map((p, i) => {
      const seq = contactStartNum + i;
      const pad = seq < 10 ? `0${seq}` : `${seq}`;
      return makeVCard(`${contactBaseName} ${pad}`, p);
    })
    .join("");
}

/** Rebuild split content in the original file format. */
export function buildSplitContent(phones: string[], ext: string): string {
  if (ext === ".vcf") {
    return phones
      .map((p, i) => makeVCard(`Contact ${String(i + 1).padStart(2, "0")}`, p))
      .join("");
  }
  return phones.join("\n");
}

// ─── Utilities ────────────────────────────────────────────────────────────────

/** Split an array into fixed-size chunks. */
export function chunkArray<T>(arr: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < arr.length; i += size) chunks.push(arr.slice(i, i + size));
  return chunks;
}

/** Detect the output extension for merge output. */
export function detectMergeExt(exts: string[]): string {
  const unique = [...new Set(exts.map(e => e.toLowerCase()))];
  if (unique.length === 1) return unique[0];
  return ""; // mixed — user must choose
}

/** Download a Telegram file URL and return Buffer (streaming, RAM-safe). */
export async function downloadBuffer(url: string, maxBytes = 20 * 1024 * 1024): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const mod = url.startsWith("https:") ? https : http;
    const req = mod.get(url, (res) => {
      if (res.statusCode && res.statusCode >= 400) {
        reject(new Error(`HTTP ${res.statusCode}`));
        return;
      }
      const chunks: Buffer[] = [];
      let total = 0;
      res.on("data", (d: Buffer) => {
        total += d.length;
        if (total > maxBytes) { req.destroy(); reject(new Error("File too large (max 20 MB)")); return; }
        chunks.push(d);
      });
      res.on("end", () => resolve(Buffer.concat(chunks)));
      res.on("error", reject);
    });
    req.on("error", reject);
    req.setTimeout(30_000, () => { req.destroy(); reject(new Error("Download timeout")); });
  });
}

/** Supported file extensions for upload. */
export const SUPPORTED_EXTS = new Set([".vcf", ".txt", ".csv", ".tsv", ".xlsx", ".xls", ".xlsm"]);

export function isSupportedExt(fileName: string): boolean {
  return SUPPORTED_EXTS.has(path.extname(fileName).toLowerCase());
}

/** Get display name for extension. */
export function extLabel(ext: string): string {
  const map: Record<string, string> = {
    ".vcf": "VCF", ".txt": "TXT", ".csv": "CSV", ".tsv": "TSV",
    ".xlsx": "XLSX", ".xls": "XLS", ".xlsm": "XLSM",
  };
  return map[ext.toLowerCase()] ?? ext.toUpperCase();
}
