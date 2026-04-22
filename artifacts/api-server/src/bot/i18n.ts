import { getCollection } from "./mongodb";
import https from "https";

export type LangCode = "default" | "en" | "hi" | "id" | "zh";

const VALID_LANGS: LangCode[] = ["default", "en", "hi", "id", "zh"];

export const LANG_LABELS: Record<LangCode, string> = {
  default: "🌐 Default (English+Hindi)",
  en: "🇬🇧 English",
  hi: "🇮🇳 Hindi",
  id: "🇮🇩 Indonesian",
  zh: "🇨🇳 Chinese",
};

export const LANG_NAMES: Record<LangCode, string> = {
  default: "Default",
  en: "English",
  hi: "हिन्दी",
  id: "Bahasa Indonesia",
  zh: "中文",
};

const userLangCache = new Map<number, LangCode>();

export function isValidLang(v: string): v is LangCode {
  return (VALID_LANGS as string[]).includes(v);
}

export async function getUserLang(userId: number): Promise<LangCode> {
  if (userLangCache.has(userId)) return userLangCache.get(userId)!;
  try {
    const col = await getCollection("user_lang");
    const doc = await col.findOne({ _id: userId as any });
    const lang = doc && isValidLang(doc.lang) ? (doc.lang as LangCode) : "default";
    userLangCache.set(userId, lang);
    return lang;
  } catch {
    userLangCache.set(userId, "default");
    return "default";
  }
}

export function getUserLangSync(userId: number): LangCode {
  return userLangCache.get(userId) || "default";
}

export async function setUserLang(userId: number, lang: LangCode): Promise<void> {
  userLangCache.set(userId, lang);
  try {
    const col = await getCollection("user_lang");
    await col.updateOne(
      { _id: userId as any },
      { $set: { lang, updatedAt: new Date() } },
      { upsert: true }
    );
  } catch (err: any) {
    console.error("[i18n] setUserLang error:", err?.message);
  }
}

// Preload some users' languages into the cache at startup
export async function preloadLangCache(): Promise<void> {
  try {
    const col = await getCollection("user_lang");
    const docs = await col.find({}).limit(5000).toArray();
    for (const d of docs) {
      const id = Number((d as any)._id);
      const lang = (d as any).lang;
      if (Number.isFinite(id) && isValidLang(lang)) {
        userLangCache.set(id, lang as LangCode);
      }
    }
    console.log(`[i18n] Preloaded ${userLangCache.size} user language preferences`);
  } catch (err: any) {
    console.error("[i18n] preloadLangCache error:", err?.message);
  }
}

// ---------------- Translation ----------------

const translateCache = new Map<string, string>();
const MAX_CACHE = 5000;

function cachePut(key: string, value: string) {
  if (translateCache.size >= MAX_CACHE) {
    const firstKey = translateCache.keys().next().value;
    if (firstKey !== undefined) translateCache.delete(firstKey);
  }
  translateCache.set(key, value);
}

function googleTranslate(text: string, target: string): Promise<string> {
  return new Promise((resolve) => {
    const params = new URLSearchParams({
      client: "gtx",
      sl: "auto",
      tl: target,
      dt: "t",
      q: text,
    });
    const url = `https://translate.googleapis.com/translate_a/single?${params.toString()}`;
    const req = https.get(
      url,
      {
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
          Accept: "*/*",
        },
      },
      (res) => {
        let body = "";
        res.on("data", (c) => (body += c));
        res.on("end", () => {
          try {
            const data = JSON.parse(body);
            const segments: string[] = [];
            if (Array.isArray(data) && Array.isArray(data[0])) {
              for (const seg of data[0]) {
                if (Array.isArray(seg) && typeof seg[0] === "string") {
                  segments.push(seg[0]);
                }
              }
            }
            resolve(segments.join("") || text);
          } catch {
            resolve(text);
          }
        });
      }
    );
    req.on("error", () => resolve(text));
    req.setTimeout(8000, () => {
      try {
        req.destroy();
      } catch {}
      resolve(text);
    });
  });
}

// HTML-aware translator: preserves Telegram HTML tags and <code>/<pre> contents.
// Strategy:
//   1. Extract <pre>...</pre> and <code>...</code> blocks → placeholders (content preserved verbatim).
//   2. Extract remaining HTML tags → placeholders.
//   3. Translate the resulting plain text.
//   4. Restore placeholders.
async function translateHtml(text: string, target: string): Promise<string> {
  if (!text || !text.trim()) return text;

  const blocks: string[] = [];
  const tags: string[] = [];

  // Use ASCII-safe placeholders that Google Translate is unlikely to alter.
  const blockPh = (i: number) => `[[BB${i}BB]]`;
  const tagPh = (i: number) => `[[TT${i}TT]]`;

  // 1) Preserve <pre>...</pre> and <code>...</code> verbatim
  let processed = text.replace(
    /<pre[\s\S]*?<\/pre>|<code[\s\S]*?<\/code>/gi,
    (m) => {
      blocks.push(m);
      return blockPh(blocks.length - 1);
    }
  );

  // 2) Replace remaining HTML tags
  processed = processed.replace(/<\/?[a-zA-Z][^>]*>/g, (m) => {
    tags.push(m);
    return tagPh(tags.length - 1);
  });

  // Quick check: if nothing meaningful to translate
  const stripped = processed
    .replace(/\[\[BB\d+BB\]\]/g, "")
    .replace(/\[\[TT\d+TT\]\]/g, "")
    .trim();
  if (!stripped) return text;

  // 3) Translate
  let translated: string;
  const cacheKey = `${target}|${processed}`;
  const cached = translateCache.get(cacheKey);
  if (cached !== undefined) {
    translated = cached;
  } else {
    translated = await googleTranslate(processed, target);
    cachePut(cacheKey, translated);
  }

  // 4) Restore placeholders. Google sometimes adds spaces/case-changes inside brackets.
  translated = translated.replace(/\[\s*\[\s*BB\s*(\d+)\s*BB\s*\]\s*\]/gi, (_m, i) => {
    const idx = Number(i);
    return blocks[idx] !== undefined ? blocks[idx] : _m;
  });
  translated = translated.replace(/\[\s*\[\s*TT\s*(\d+)\s*TT\s*\]\s*\]/gi, (_m, i) => {
    const idx = Number(i);
    return tags[idx] !== undefined ? tags[idx] : _m;
  });

  return translated;
}

export async function translateForUser(text: string, userId: number | undefined): Promise<string> {
  if (!text || userId === undefined) return text;
  const lang = await getUserLang(userId);
  if (lang === "default") return text;
  try {
    return await translateHtml(text, lang);
  } catch {
    return text;
  }
}

// ---------------- grammY API transformer ----------------

const TRANSLATABLE_METHODS = new Set([
  "sendMessage",
  "editMessageText",
  "sendPhoto",
  "sendDocument",
  "sendVideo",
  "sendAudio",
  "sendAnimation",
  "sendVoice",
  "editMessageCaption",
  "answerCallbackQuery",
]);

function extractUserId(payload: any): number | undefined {
  if (!payload) return undefined;
  if (typeof payload.chat_id === "number") return payload.chat_id;
  if (typeof payload.chat_id === "string") {
    const n = Number(payload.chat_id);
    if (Number.isFinite(n)) return n;
  }
  if (typeof payload.user_id === "number") return payload.user_id;
  // For answerCallbackQuery, we don't know the user from payload; the caller
  // wraps the text manually.
  return undefined;
}

export function makeTranslateTransformer() {
  return async function translateTransformer(
    prev: (method: string, payload: any, signal?: AbortSignal) => Promise<any>,
    method: string,
    payload: any,
    signal?: AbortSignal
  ) {
    if (TRANSLATABLE_METHODS.has(method) && payload && typeof payload === "object") {
      const userId = extractUserId(payload);
      if (userId !== undefined) {
        const lang = await getUserLang(userId);
        if (lang !== "default") {
          if (typeof payload.text === "string") {
            payload = { ...payload, text: await translateHtml(payload.text, lang) };
          }
          if (typeof payload.caption === "string") {
            payload = { ...payload, caption: await translateHtml(payload.caption, lang) };
          }
        }
      }
    }
    return prev(method, payload, signal);
  };
}

// ---------------- Localized strings for the /language UI ----------------

export const LANG_PROMPT: Record<LangCode, string> = {
  default:
    "🌐 <b>Language Settings</b>\n\nApni preferred language choose karo:\n\n<i>Default = English + Hindi (jo abhi hai waisa hi)</i>",
  en: "🌐 <b>Language Settings</b>\n\nChoose your preferred language:\n\n<i>Default keeps the bot's original mix of English &amp; Hindi.</i>",
  hi: "🌐 <b>भाषा सेटिंग्स</b>\n\nअपनी पसंदीदा भाषा चुनें:\n\n<i>Default = अंग्रेज़ी + हिन्दी (जैसा अभी है)</i>",
  id: "🌐 <b>Pengaturan Bahasa</b>\n\nPilih bahasa yang Anda inginkan:\n\n<i>Default mempertahankan campuran asli Bahasa Inggris &amp; Hindi.</i>",
  zh: "🌐 <b>语言设置</b>\n\n请选择您偏好的语言：\n\n<i>默认保留原始的英语和印地语混合。</i>",
};

export const LANG_CONFIRM: Record<LangCode, string> = {
  default: "✅ Language set to Default (English + Hindi). Bot ab original text use karega.",
  en: "✅ Language set to English. The bot will reply in English from now on.",
  hi: "✅ भाषा हिन्दी पर सेट हो गई। अब बॉट हिन्दी में जवाब देगा।",
  id: "✅ Bahasa diatur ke Bahasa Indonesia. Bot akan membalas dalam Bahasa Indonesia mulai sekarang.",
  zh: "✅ 语言已设置为中文。机器人现在将以中文回复。",
};
