export interface VCFContact {
  name: string;
  phone: string;
}

// Priority order: CELL/MOBILE phone numbers preferred over HOME/WORK/VOICE
const MOBILE_TYPE_KEYWORDS = ["cell", "mobile", "iphone", "android"];
const SKIP_TYPE_KEYWORDS = ["fax", "pager"];

// Decode a Quoted-Printable encoded string
function decodeQuotedPrintable(str: string): string {
  return str
    .replace(/=\r?\n/g, "") // soft line breaks
    .replace(/=([0-9A-Fa-f]{2})/g, (_, hex) => String.fromCharCode(parseInt(hex, 16)));
}

// Unfold RFC 6350 folded lines: CRLF or LF followed by a single space/tab
// means the next logical line is a continuation of this one.
function unfoldVCFLines(content: string): string[] {
  // Normalize line endings to \n, then unfold
  return content
    .replace(/\r\n/g, "\n")
    .replace(/\n[ \t]/g, "") // continuation line — join with previous
    .split("\n");
}

export function parseVCF(content: string): VCFContact[] {
  const contacts: VCFContact[] = [];
  const lines = unfoldVCFLines(content);

  let inCard = false;
  let name = "";
  // Store all phones found with their type priority
  let phones: Array<{ number: string; priority: number }> = [];

  for (const raw of lines) {
    const line = raw.trim();
    if (!line) continue;

    if (line.toUpperCase() === "BEGIN:VCARD") {
      inCard = true;
      name = "";
      phones = [];
      continue;
    }

    if (line.toUpperCase() === "END:VCARD") {
      inCard = false;
      if (phones.length > 0) {
        // Pick highest-priority phone (lowest priority number = best match)
        phones.sort((a, b) => a.priority - b.priority);
        const phone = phones[0].number;
        if (phone) {
          contacts.push({ name: name || "Unknown", phone });
        }
      }
      continue;
    }

    if (!inCard) continue;

    const upperLine = line.toUpperCase();

    // ── FN (Full Name) ──────────────────────────────────────────────────────
    if (upperLine.startsWith("FN:") || upperLine.startsWith("FN;")) {
      const colonIdx = line.indexOf(":");
      if (colonIdx !== -1) {
        let raw = line.slice(colonIdx + 1).trim();
        // Check for QP encoding
        if (upperLine.includes("ENCODING=QUOTED-PRINTABLE") || upperLine.includes("ENCODING=QP")) {
          raw = decodeQuotedPrintable(raw);
        }
        name = raw;
      }
      continue;
    }

    // ── N (Structured Name — fallback if FN missing) ──────────────────────
    if (!name && (upperLine.startsWith("N:") || upperLine.startsWith("N;"))) {
      const colonIdx = line.indexOf(":");
      if (colonIdx !== -1) {
        const parts = line.slice(colonIdx + 1).split(";");
        const combined = parts
          .map((p) => p.trim())
          .filter(Boolean)
          .join(" ");
        if (combined) name = combined;
      }
      continue;
    }

    // ── TEL (Phone Number) ─────────────────────────────────────────────────
    if (upperLine.startsWith("TEL") && (upperLine[3] === ":" || upperLine[3] === ";")) {
      const colonIdx = line.indexOf(":");
      if (colonIdx === -1) continue;

      const paramStr = line.slice(0, colonIdx).toUpperCase(); // before colon
      let numStr = line.slice(colonIdx + 1).trim();           // after colon

      // Skip fax / pager
      if (SKIP_TYPE_KEYWORDS.some((kw) => paramStr.includes(kw.toUpperCase()))) continue;

      // Handle Quoted-Printable encoded phone numbers (rare but exists)
      if (paramStr.includes("ENCODING=QUOTED-PRINTABLE") || paramStr.includes("ENCODING=QP")) {
        numStr = decodeQuotedPrintable(numStr);
      }

      // Clean the number: keep only digits and leading +
      const cleaned = numStr.replace(/[\s\-().]/g, "");
      // Extract actual digits
      const digits = cleaned.replace(/[^0-9]/g, "");
      if (digits.length < 7) continue; // too short to be a real number

      const normalized = cleaned.startsWith("+") ? "+" + digits : digits;
      const withPlus = normalized.startsWith("+") ? normalized : "+" + normalized;

      // Priority: CELL/MOBILE = 1 (best), untyped = 2, VOICE = 3, others = 4
      let priority = 2;
      if (MOBILE_TYPE_KEYWORDS.some((kw) => paramStr.includes(kw.toUpperCase()))) {
        priority = 1;
      } else if (paramStr.includes("VOICE")) {
        priority = 3;
      } else if (!paramStr.includes("TYPE")) {
        priority = 2; // no type specified — treat as mobile
      }

      phones.push({ number: withPlus, priority });
    }
  }

  // Flush any card that didn't have END:VCARD (malformed VCF)
  if (inCard && phones.length > 0) {
    phones.sort((a, b) => a.priority - b.priority);
    const phone = phones[0].number;
    if (phone) {
      contacts.push({ name: name || "Unknown", phone });
    }
  }

  return contacts;
}

export function normalizePhone(phone: string): string {
  return phone.replace(/[^0-9]/g, "");
}
