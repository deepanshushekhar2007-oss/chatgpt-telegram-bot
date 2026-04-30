import { Bot, InlineKeyboard, InputFile } from "grammy";
import {
  connectWhatsApp,
  connectWhatsAppQr,
  isConnected,
  disconnectWhatsApp,
  idleDisconnectWhatsApp,
  refreshWhatsAppSession,
  createWhatsAppGroup,
  applyGroupSettings,
  setGroupIcon,
  joinGroupWithLink,
  getGroupPendingRequests,
  getGroupPendingRequestsJids,
  getGroupPendingRequestsDetailed,
  checkContactsInGroup,
  getGroupIdFromLink,
  GroupPermissions,
  getAllGroups,
  getGroupInviteLink,
  leaveGroup,
  getGroupParticipants,
  removeGroupParticipant,
  getGroupPendingList,
  makeGroupAdmin,
  approveGroupParticipant,
  rejectGroupParticipantsBulk,
  setGroupApprovalMode,
  findParticipantByPhone,
  addGroupParticipant,
  addGroupParticipantsBulk,
  isUserInGroup,
  getConnectedWhatsAppNumber,
  sendGroupMessage,
  getAutoUserId,
  isAutoConnected,
  getAutoConnectedNumber,
  getActiveSessionUserIds,
  setDisconnectNotifier,
  setGroupDisappearingMessages,
  setGroupName,
  ensureSessionLoaded,
  hasStoredWhatsAppSession,
  waitForWhatsAppConnected,
  sweepIdleSessions,
  getGroupPendingInviteLinkJoins,
} from "./whatsapp";
import { parseVCF, normalizePhone } from "./vcf-parser";
import QRCode from "qrcode";
import https from "https";
import http from "http";
import { AsyncLocalStorage } from "async_hooks";
import {
  loadBotData,
  saveBotData,
  trackUser as trackUserMongo,
  isUserBanned,
  hasUserAccess,
  getUserAccessState,
  ensureFreeTrial,
  recordReferral,
  setReferMode,
  getReferralStats,
  findAndMarkTrialsToWarn,
  AccessState,
} from "./mongo-bot-data";
import { getSessionStats, cleanupStaleSessions, clearMongoSession } from "./mongo-auth-state";
import {
  Language,
  LANGUAGES,
  getUserLang,
  hasUserLang,
  setUserLanguage,
  loadUserLanguages,
  translate,
  translateInlineKeyboard,
  warmUpLanguage,
  notr,
  isNotr,
  stripNotr,
  clearTranslationCaches,
} from "./i18n";

const token = process.env["TELEGRAM_BOT_TOKEN"] || "";

const ADMIN_USER_ID = Number(process.env["ADMIN_USER_ID"] || "0");
const FORCE_SUB_CHANNEL = process.env["FORCE_SUB_CHANNEL"] || "";
const OWNER_USERNAME = "@SPIDYWS";
const BOT_DISPLAY_NAME = "ᴡꜱ ᴀᴜᴛᴏᴍᴀᴛɪᴏɴ";

// ── Referral mode tunables ───────────────────────────────────────────────────
// Free trial length given to every new user when refer mode is ON. After it
// expires, the user must either refer someone else or buy premium from the
// owner. One referral grants exactly one extra day of access (stacking).
const FREE_TRIAL_MS = 24 * 60 * 60 * 1000;
const REFERRAL_REWARD_MS = 24 * 60 * 60 * 1000;
// How long before trial expiry we send the "your trial is ending soon"
// warning. The scheduler checks every TRIAL_WARNING_INTERVAL_MS, so the
// warning may land anywhere inside [warnBefore - tickInterval, warnBefore].
const TRIAL_WARNING_BEFORE_MS = 30 * 60 * 1000;
const TRIAL_WARNING_INTERVAL_MS = 60 * 1000;

const bot = new Bot(token || "placeholder");

// Cached bot username for building referral deep links. Populated lazily on
// first access so we don't have to await getMe() during startup.
let cachedBotUsername: string | null = null;

// ── Pending referrals (force-sub aware) ─────────────────────────────────────
// When a user opens the bot via "/start ref_<referrerId>" but is NOT yet
// joined to FORCE_SUB_CHANNEL, the original /start handler returns early
// (force-sub guard) and the referral payload is lost — the referrer never
// gets credit. This map stashes the referrer-id keyed by the new user's
// telegram-id so the `check_joined` callback can credit the referral once
// the user actually joins the channel.
//
// Entries are dropped after PENDING_REFERRAL_TTL_MS or after they're consumed
// by check_joined / clearUserMemoryState. Cap is small (one entry per user
// per /start) so unbounded growth is not a real concern, but the TTL sweep
// keeps it tidy if a user opens the link and never joins.
const pendingReferrals: Map<number, { referrerId: number; createdAt: number }> = new Map();
const PENDING_REFERRAL_TTL_MS = 60 * 60 * 1000; // 1 hour
setInterval(() => {
  const cutoff = Date.now() - PENDING_REFERRAL_TTL_MS;
  for (const [uid, entry] of pendingReferrals) {
    if (entry.createdAt < cutoff) pendingReferrals.delete(uid);
  }
}, 15 * 60 * 1000);

// Shared referral-award helper. Called from /start when the user is already
// joined to the channel AND from `check_joined` when the user joins via the
// force-sub flow. Idempotent: recordReferral() in db.ts dedupes — a user can
// only ever earn one referrer credit, no matter how many times this runs.
async function processReferralAward(newUserId: number, referrerId: number): Promise<void> {
  if (!referrerId || !Number.isFinite(referrerId)) return;
  if (referrerId === newUserId) return; // can't refer yourself
  try {
    const data = await loadBotData();
    if (!data.referMode) return;
    const result = await recordReferral(
      newUserId, referrerId, REFERRAL_REWARD_MS, ADMIN_USER_ID
    );
    if (result.success && referrerId !== ADMIN_USER_ID) {
      const totalText = result.totalReferred
        ? `\n👥 <b>Total people you've referred:</b> ${result.totalReferred}`
        : "";
      const remaining = result.referrerExpiresAt
        ? `\n⏰ <b>Your access now lasts:</b> ${formatRemaining(result.referrerExpiresAt)}`
        : "";
      bot.api.sendMessage(
        referrerId,
        `🎉 <b>New referral!</b>\n\n` +
        `User <code>${newUserId}</code> just started the bot through your link.\n\n` +
        `✅ <b>You've earned 1 extra day of free access.</b>${remaining}${totalText}`,
        { parse_mode: "HTML" }
      ).catch((err: any) => {
        console.error(`[REFER] Failed to notify referrer ${referrerId}:`, err?.message);
      });
    }
  } catch (err: any) {
    console.error(`[REFER] processReferralAward error:`, err?.message);
  }
}

async function getBotUsername(): Promise<string> {
  if (cachedBotUsername) return cachedBotUsername;
  try {
    const me = await bot.api.getMe();
    cachedBotUsername = me.username || "";
  } catch (err: any) {
    console.error("[REFER] getMe failed:", err?.message);
    cachedBotUsername = "";
  }
  return cachedBotUsername;
}

function buildReferLink(userId: number, botUsername: string): string {
  // tg deep-link format: https://t.me/<bot>?start=ref_<userId>
  return `https://t.me/${botUsername}?start=ref_${userId}`;
}

function formatRemaining(expiresAt: number): string {
  const ms = expiresAt - Date.now();
  if (ms <= 0) return "expired";
  const totalMin = Math.floor(ms / 60000);
  if (totalMin < 60) return `${totalMin} minute${totalMin === 1 ? "" : "s"}`;
  const totalHrs = Math.floor(totalMin / 60);
  if (totalHrs < 24) {
    const mins = totalMin - totalHrs * 60;
    return mins
      ? `${totalHrs} hour${totalHrs === 1 ? "" : "s"} ${mins} min`
      : `${totalHrs} hour${totalHrs === 1 ? "" : "s"}`;
  }
  const days = Math.floor(totalHrs / 24);
  const hrs = totalHrs - days * 24;
  return hrs
    ? `${days} day${days === 1 ? "" : "s"} ${hrs} hour${hrs === 1 ? "" : "s"}`
    : `${days} day${days === 1 ? "" : "s"}`;
}

// ─────────────────────────────────────────────────────────────────────────────
// i18n API transformer: auto-translate every outgoing message + button label
// based on the destination user's language preference. Single chokepoint so
// no individual call site needs to change.
//
// Coverage:
//   • sendMessage / sendPhoto / sendDocument / sendVideo / sendAnimation
//     → translates `text` and `caption` body, plus inline keyboard buttons.
//   • editMessageText / editMessageCaption / editMessageMedia
//     → same as above, including caption inside `media`.
//   • editMessageReplyMarkup → translates inline keyboard button labels even
//     when only the markup changes (no text edit).
//   • answerCallbackQuery → translates the alert/toast `text` field.
//
// Language resolution priority (to support every grammy call style):
//   1. payload.chat_id when it is a number (or a numeric string).
//   2. AsyncLocalStorage user-id captured by the per-update middleware below.
//      This is what lets answerCallbackQuery (which has no chat_id) and any
//      other non-chat-bound method still pick up the right user language.
// ─────────────────────────────────────────────────────────────────────────────

// Per-update store of the active user's Telegram ID. Set by `bot.use` below,
// read by the api transformer when no chat_id is available on the payload.
const updateUserCtx = new AsyncLocalStorage<number>();

const TRANSLATABLE_METHODS = new Set([
  "sendMessage",
  "editMessageText",
  "editMessageCaption",
  "editMessageMedia",
  "editMessageReplyMarkup",
  "sendPhoto",
  "sendDocument",
  "sendVideo",
  "sendAnimation",
  "answerCallbackQuery",
]);

function resolveLangFromPayload(payload: any): Language {
  const chatId = payload?.chat_id;
  if (typeof chatId === "number") return getUserLang(chatId);
  if (typeof chatId === "string") {
    const n = Number(chatId);
    if (Number.isFinite(n)) return getUserLang(n);
  }
  // Fallback: use the user-id captured by the per-update middleware.
  const uid = updateUserCtx.getStore();
  if (typeof uid === "number") return getUserLang(uid);
  return "default";
}

bot.api.config.use(async (prev, method, payload, signal) => {
  try {
    if (!TRANSLATABLE_METHODS.has(method)) {
      // Still strip the no-translate marker if present, even when we don't translate.
      if (payload && typeof (payload as any).text === "string" && isNotr((payload as any).text)) {
        const newPayload: any = { ...payload, text: stripNotr((payload as any).text) };
        return prev(method, newPayload, signal);
      }
      return prev(method, payload, signal);
    }

    const lang: Language = resolveLangFromPayload(payload);

    // Fast path: default language → no translation overhead at all.
    if (lang === "default") {
      // Even on default, strip the no-translate marker so it never reaches Telegram.
      const text = (payload as any).text;
      const caption = (payload as any).caption;
      if ((typeof text === "string" && isNotr(text)) || (typeof caption === "string" && isNotr(caption))) {
        const newPayload: any = { ...payload };
        if (typeof text === "string" && isNotr(text)) newPayload.text = stripNotr(text);
        if (typeof caption === "string" && isNotr(caption)) newPayload.caption = stripNotr(caption);
        return prev(method, newPayload, signal);
      }
      return prev(method, payload, signal);
    }

    const newPayload: any = { ...payload };

    // Translate text body (unless explicitly marked no-translate).
    // `text` covers sendMessage / editMessageText / answerCallbackQuery alerts.
    if (typeof newPayload.text === "string") {
      if (isNotr(newPayload.text)) {
        newPayload.text = stripNotr(newPayload.text);
      } else {
        newPayload.text = await translate(newPayload.text, lang);
      }
    }
    // `caption` covers sendPhoto / sendDocument / sendVideo / sendAnimation /
    // editMessageCaption.
    if (typeof newPayload.caption === "string") {
      if (isNotr(newPayload.caption)) {
        newPayload.caption = stripNotr(newPayload.caption);
      } else {
        newPayload.caption = await translate(newPayload.caption, lang);
      }
    }
    // editMessageMedia carries caption inside the `media` object.
    if (newPayload.media && typeof newPayload.media.caption === "string") {
      if (isNotr(newPayload.media.caption)) {
        newPayload.media = { ...newPayload.media, caption: stripNotr(newPayload.media.caption) };
      } else {
        newPayload.media = { ...newPayload.media, caption: await translate(newPayload.media.caption, lang) };
      }
    }
    // Translate inline keyboard button labels (works for both edit-text and
    // edit-only-reply-markup paths).
    if (newPayload.reply_markup && Array.isArray(newPayload.reply_markup.inline_keyboard)) {
      newPayload.reply_markup = await translateInlineKeyboard(newPayload.reply_markup, lang);
    }

    return prev(method, newPayload, signal);
  } catch (err: any) {
    console.error(`[i18n] transformer error on ${method}:`, err?.message);
    return prev(method, payload, signal);
  }
});

// Run every incoming update inside an AsyncLocalStorage scope tagged with the
// triggering user's ID. This lets the API transformer above resolve the right
// language even for methods that carry no chat_id (e.g. answerCallbackQuery)
// and for places that send messages indirectly (timers, post-await flows).
bot.use(async (ctx, next) => {
  const userId = ctx.from?.id;
  if (typeof userId === "number") {
    // Refresh the user's "active" timestamp on every interaction (commands,
    // button presses, text messages). This is what keeps long flows like
    // group creation alive past the old aggressive cleanup, AND prevents the
    // "✅ WhatsApp connected" toast from re-appearing on every /start.
    const startedNewSession = markUserActive(userId);
    newSessionFlag.set(userId, startedNewSession);
    // If WhatsApp got disconnected (idle timer or process restart) but the
    // user has a saved Mongo session, kick off a silent restore in the
    // background so it's ready by the time they tap a feature button.
    void ensureWhatsAppRestored(userId);

    // Auto-reconnect-and-resume when a feature button is tapped while
    // WhatsApp is disconnected (typical 30-min idle case). We edit the
    // message in-place to a "🔄 Reconnecting..." status, silently wait
    // for the background restore (already kicked off above) to finish,
    // and then let the original handler run normally — so the user does
    // NOT have to re-tap the button. Connect / menu / language /
    // force-sub callbacks are exempted because they handle the
    // disconnected state on purpose. We deliberately do NOT pre-answer
    // the callback query — the handler will answer it itself once it
    // runs. Telegram keeps the per-button spinner visible until then,
    // which is exactly the loading feedback we want.
    const cbData = ctx.callbackQuery?.data;
    const skipReconnect = !cbData
      || cbData === "connect_wa"
      || cbData === "main_menu"
      || cbData.startsWith("connect_")
      || cbData.startsWith("disconnect_")
      || cbData.startsWith("logout_")
      || cbData.startsWith("lang_")
      || cbData.startsWith("force_sub_");
    if (cbData && !skipReconnect && !isConnected(String(userId))) {
      let hasStored = false;
      try { hasStored = await hasStoredWhatsAppSession(String(userId)); } catch {}
      if (hasStored) {
        // Edit text only (no reply_markup specified → original inline
        // keyboard is preserved). The handler that runs after reconnect
        // will overwrite this text with its real UI.
        let statusEdited = false;
        try {
          await ctx.editMessageText(
            `🔄 <b>WhatsApp reconnecting...</b>\n\n` +
            `<i>Aap idle the to disconnect ho gaya tha. ` +
            `5-15 second mein apne aap button kaam karega.</i>`,
            { parse_mode: "HTML" }
          );
          statusEdited = true;
        } catch {}

        let connected = false;
        try {
          // 20s timeout per user request. WhatsApp Baileys cold reconnect
          // on Render free tier normally takes 5-15 sec when the session
          // credentials are still valid. If 20 sec pass without the
          // socket reaching `open` state, the user's WhatsApp device is
          // almost certainly truly disconnected (logged out from phone,
          // device unlinked, or session creds invalidated by WhatsApp)
          // — not a slow-network case. So we tell the user clearly
          // instead of silently spinning.
          connected = await waitForWhatsAppConnected(String(userId), {
            timeoutMs: 20_000,
            pollMs: 500,
          });
        } catch {}

        if (!connected) {
          // 20 sec elapsed without socket open → treat as a real
          // disconnection. Show the user a clear, actionable message so
          // they know what happened and how to fix it (re-link via the
          // QR / pairing-code flow inside the WhatsApp menu).
          try { await ctx.answerCallbackQuery(); } catch {}
          try {
            await ctx.editMessageText(
              `❌ <b>WhatsApp disconnected</b>\n\n` +
              `Aapka WhatsApp session disconnect ho gaya hai. Phone me ` +
              `WhatsApp → Linked Devices kholo, bot wala device check ` +
              `karo. Agar wahan se hata diya gaya hai to bot me dobara ` +
              `link karna hoga:\n\n` +
              `📱 Menu → <b>Connect WhatsApp</b> → QR ya Pairing Code se ` +
              `re-link karo.`,
              { parse_mode: "HTML" }
            );
          } catch {
            try {
              await ctx.reply(
                `❌ <b>WhatsApp disconnected</b>\n\n` +
                `Aapka WhatsApp session disconnect ho gaya hai. Menu se ` +
                `<b>Connect WhatsApp</b> dabake QR ya Pairing Code se ` +
                `dobara link karo.`,
                { parse_mode: "HTML" }
              );
            } catch {}
          }
          return;
        }
        // Connected — fall through to the normal handler dispatch below.
        // The handler will edit this same message with its real UI and
        // call answerCallbackQuery itself.
      }
    }

    try {
      await updateUserCtx.run(userId, next);
    } finally {
      newSessionFlag.delete(userId);
    }
  } else {
    await next();
  }
});

// ─────────────────────────────────────────────────────────────────────────────
// Refer-mode gate for callback queries.
//
// When refer mode is ON and the user has run out of access (no admin grant,
// no active 24h trial, no remaining referral days) every button press is
// intercepted here and replaced with the "refer or buy premium" message.
// Admin and the language / channel-join callbacks are exempted so the user
// can always pick a language and confirm the channel join even if their
// trial has just expired.
// ─────────────────────────────────────────────────────────────────────────────
const REFER_GATE_EXEMPT_PREFIXES = ["lang_", "force_sub_"];
const REFER_GATE_EXEMPT_EXACT = new Set(["check_joined"]);
function isReferGateExempt(cbData: string): boolean {
  if (REFER_GATE_EXEMPT_EXACT.has(cbData)) return true;
  return REFER_GATE_EXEMPT_PREFIXES.some((p) => cbData.startsWith(p));
}

bot.use(async (ctx, next) => {
  const cbData = ctx.callbackQuery?.data;
  if (!cbData) return next();
  const userId = ctx.from?.id;
  if (typeof userId !== "number") return next();
  if (isAdmin(userId)) return next();
  if (isReferGateExempt(cbData)) return next();

  const data = await loadBotData();
  if (!data.referMode) return next();

  const state = await getAccessState(userId);
  if (state.kind !== "none") return next();

  // Out of access — block the button and surface the refer-required UI.
  try { await ctx.answerCallbackQuery({ text: "🔒 Free access ended", show_alert: false }); } catch {}
  const { text, keyboard } = await buildReferRequiredMessage(userId);
  try {
    await ctx.editMessageText(text, { parse_mode: "HTML", reply_markup: keyboard });
  } catch {
    try {
      await ctx.reply(text, { parse_mode: "HTML", reply_markup: keyboard });
    } catch {}
  }
  // Stop here — do NOT call next(), the original handler must not run.
});

type TelegramButtonStyle = "primary" | "success" | "danger";

function getButtonStyle(text: string, callbackData?: string): TelegramButtonStyle {
  const value = `${text} ${callbackData || ""}`.toLowerCase();

  if (
    value.includes("cancel") ||
    value.includes("delete") ||
    value.includes("remove") ||
    value.includes("leave") ||
    value.includes("disconnect") ||
    value.includes("ban") ||
    value.includes("reject") ||
    value.includes("clear") ||
    value.includes("stop") ||
    value.includes("❌") ||
    value.includes("🗑")
  ) {
    return "danger";
  }

  if (
    value.includes("confirm") ||
    value.includes("create") ||
    value.includes("join") ||
    value.includes("connect") ||
    value.includes("approve") ||
    value.includes("add") ||
    value.includes("select") ||
    value.includes("save") ||
    value.includes("done") ||
    value.includes("start") ||
    value.includes("continue") ||
    value.includes("retry") ||
    value.includes("proceed") ||
    value.includes("copy") ||
    value.includes("yes") ||
    value.includes("✅") ||
    value.includes("☑️") ||
    value.includes("💾") ||
    value.includes("➕")
  ) {
    return "success";
  }

  return "primary";
}

function setLatestButtonStyle(keyboard: InlineKeyboard, style: TelegramButtonStyle): void {
  const markup = keyboard as unknown as { inline_keyboard?: Array<Array<Record<string, unknown>>> };
  const rows = markup.inline_keyboard;
  const latestRow = rows?.[rows.length - 1];
  const latestButton = latestRow?.[latestRow.length - 1];
  if (latestButton && !latestButton.style) {
    latestButton.style = style;
  }
}

const originalInlineText = InlineKeyboard.prototype.text;
const originalInlineUrl = InlineKeyboard.prototype.url;

(InlineKeyboard.prototype as any).text = function (this: InlineKeyboard, ...args: any[]) {
  const result = (originalInlineText as any).apply(this, args);
  if (typeof args[0] === "string") {
    setLatestButtonStyle(this, getButtonStyle(args[0], typeof args[1] === "string" ? args[1] : undefined));
  }
  return result;
};

(InlineKeyboard.prototype as any).url = function (this: InlineKeyboard, ...args: any[]) {
  const result = (originalInlineUrl as any).apply(this, args);
  if (typeof args[0] === "string") {
    setLatestButtonStyle(this, "primary");
  }
  return result;
};

function esc(text: string): string {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function connectedStatusText(userId: number): string {
  const mainConnected = isConnected(String(userId));
  const autoConnected = isAutoConnected(String(userId));
  let text = "";

  if (!mainConnected) {
    text += "📱 <b>Status:</b> WhatsApp not connected\n";
  } else {
    const number = getConnectedWhatsAppNumber(String(userId));
    text += "✅ <b>Status:</b> WhatsApp connected\n" +
      (number ? `📞 <b>Connected Number:</b> <code>${esc(number)}</code>\n` : "📞 <b>Connected Number:</b> Detecting from session\n");
  }

  if (autoConnected) {
    const autoNumber = getAutoConnectedNumber(String(userId));
    text += "🤖 <b>Auto Chat WA:</b> Connected\n" +
      (autoNumber ? `📞 <b>Auto Number:</b> <code>${esc(autoNumber)}</code>\n` : "");
  }

  return text;
}

function mainMenuText(userId: number, mode: "welcome" | "menu" = "menu"): string {
  const greeting = mode === "welcome" ? "👋 <b>Welcome!</b>" : "✨ <b>Main Menu</b>";
  return (
    `🤖 <b>${BOT_DISPLAY_NAME}</b>\n\n` +
    `${greeting}\n` +
    connectedStatusText(userId) +
    "\nChoose an option below:"
  );
}

function whatsappConnectedText(userId: number, detail: string): string {
  return (
    `🤖 <b>${BOT_DISPLAY_NAME}</b>\n\n` +
    `✅ <b>WhatsApp Connected!</b>\n` +
    connectedStatusText(userId) +
    `\n${detail}`
  );
}

function generateGroupNames(baseName: string, count: number): string[] {
  if (count === 1) return [baseName];
  const match = baseName.match(/^(.*?)(\s*)(\d+)$/);
  if (match) {
    const prefix = match[1];
    const sep = match[2] || " ";
    const startNum = parseInt(match[3]);
    return Array.from({ length: count }, (_, i) => `${prefix}${sep}${startNum + i}`);
  }
  return Array.from({ length: count }, (_, i) => `${baseName} ${i + 1}`);
}

function cleanWALink(raw: string): string {
  const trimmed = raw.trim();
  const withoutQuery = trimmed.split("?")[0].replace(/\/$/, "");
  return withoutQuery
    .replace("https://chat.whatsapp.com/", "")
    .replace("http://chat.whatsapp.com/", "")
    .trim();
}

function buildCleanLink(raw: string): string {
  return `https://chat.whatsapp.com/${cleanWALink(raw)}`;
}

function extractLinksFromText(text: string): string[] {
  const regex = /https?:\/\/chat\.whatsapp\.com\/[A-Za-z0-9]+/gi;
  const matches = text.match(regex);
  if (!matches) return [];
  return [...new Set(matches.map(buildCleanLink))];
}

function isAdmin(userId: number): boolean {
  return userId === ADMIN_USER_ID;
}

async function isBanned(userId: number): Promise<boolean> {
  return isUserBanned(userId);
}

async function hasAccess(userId: number): Promise<boolean> {
  return hasUserAccess(userId, ADMIN_USER_ID);
}

async function getAccessState(userId: number): Promise<AccessState> {
  return getUserAccessState(userId, ADMIN_USER_ID);
}

// Build the "your free time is over — refer or buy premium" reply that is
// shown to users when refer mode is on and their trial + referral access
// have both expired. The message includes the user's personal referral
// deep link so they can share it directly. All copy is in English.
async function buildReferRequiredMessage(userId: number): Promise<{
  text: string;
  keyboard: InlineKeyboard;
}> {
  const username = await getBotUsername();
  const link = username ? buildReferLink(userId, username) : "";
  const stats = await getReferralStats(userId);
  const referredText = stats.totalReferred > 0
    ? `\n👥 <b>Total people referred so far:</b> ${stats.totalReferred}`
    : "";

  const text =
    `🔒 <b>Your free access has ended.</b>\n\n` +
    `To keep using the bot you have two options:\n\n` +
    `1️⃣ <b>Refer a friend</b> — every new person who starts the bot through your link gives you <b>1 day of free access</b>.\n` +
    `2️⃣ <b>Don't want to refer?</b> Message ${OWNER_USERNAME} on Telegram to buy premium access.\n\n` +
    `🔗 <b>Your personal referral link:</b>\n` +
    (link ? `<code>${esc(link)}</code>` : `<i>(link unavailable, please try again later)</i>`) +
    `${referredText}\n\n` +
    `Share this link with friends — as soon as someone starts the bot through it, you'll get a notification and 1 extra day will be added to your access.`;

  const kb = new InlineKeyboard();
  if (link) {
    const shareText = encodeURIComponent(
      `Try this Telegram bot — start through my link to get a 24-hour free trial:`
    );
    kb.url("📤 Share My Referral Link", `https://t.me/share/url?url=${encodeURIComponent(link)}&text=${shareText}`).row();
  }
  kb.url(`💎 Buy Premium (${OWNER_USERNAME})`, `https://t.me/${OWNER_USERNAME.replace(/^@/, "")}`);
  return { text, keyboard: kb };
}

// Send the refer-required message as a fresh reply (used for /start and
// other text commands).
async function sendReferRequired(ctx: any, userId: number): Promise<void> {
  const { text, keyboard } = await buildReferRequiredMessage(userId);
  try {
    await ctx.reply(text, { parse_mode: "HTML", reply_markup: keyboard });
  } catch (err: any) {
    console.error("[REFER] sendReferRequired failed:", err?.message);
  }
}

// Build the friendly "trial just started" notification used by /start
// when refer mode is ON and we just created a 24h window for the user.
function trialStartedMessage(expiresAt: number): string {
  return (
    `🎁 <b>Welcome! You've unlocked a 24-hour free trial.</b>\n\n` +
    `For the next 24 hours you can enjoy free access to the bot.\n\n` +
    `⏰ <b>Trial ends in:</b> ${formatRemaining(expiresAt)}\n\n` +
    `When the trial ends, you can either refer a friend (1 referral = 1 day free) or buy premium from ${OWNER_USERNAME}.`
  );
}

// Build the "your trial ends in 30 minutes" reminder. Includes the user's
// personal referral link + a Buy Premium button so they can act
// immediately without having to wait for the trial to expire. All copy in
// English.
async function buildTrialEndingMessage(userId: number, expiresAt: number): Promise<{
  text: string;
  keyboard: InlineKeyboard;
}> {
  const username = await getBotUsername();
  const link = username ? buildReferLink(userId, username) : "";
  const text =
    `⏰ <b>Heads up — your free trial is ending soon.</b>\n\n` +
    `Your 24-hour free trial will end in about <b>${formatRemaining(expiresAt)}</b>.\n\n` +
    `To keep using the bot without a break, you can:\n\n` +
    `1️⃣ <b>Refer a friend now</b> — every new person who starts the bot through your link gives you <b>1 extra day</b> of free access.\n` +
    `2️⃣ <b>Don't want to refer?</b> Message ${OWNER_USERNAME} on Telegram to buy premium access.\n\n` +
    (link
      ? `🔗 <b>Your personal referral link:</b>\n<code>${esc(link)}</code>\n\n`
      : ``) +
    `If you do nothing, the bot will stop responding to your buttons once the trial ends.`;

  const kb = new InlineKeyboard();
  if (link) {
    const shareText = encodeURIComponent(
      `Try this Telegram bot — start through my link to get a 24-hour free trial:`
    );
    kb.url("📤 Share My Referral Link", `https://t.me/share/url?url=${encodeURIComponent(link)}&text=${shareText}`).row();
  }
  kb.url(`💎 Buy Premium (${OWNER_USERNAME})`, `https://t.me/${OWNER_USERNAME.replace(/^@/, "")}`);
  return { text, keyboard: kb };
}

// Background scheduler: every minute, scan for trials ending within the
// next 30 minutes that we haven't warned about yet, and ping each user
// once with the reminder. Atomic "mark as warned" lives in
// findAndMarkTrialsToWarn() so the same user is never double-pinged
// even if multiple ticks overlap or the process restarts mid-window.
setInterval(async () => {
  try {
    const due = await findAndMarkTrialsToWarn(TRIAL_WARNING_BEFORE_MS);
    for (const { userId, expiresAt } of due) {
      try {
        // Skip the warning if the user already has access that outlasts
        // the trial (admin grant, referral days). Their trial expiring
        // is irrelevant to them.
        const state = await getUserAccessState(userId, ADMIN_USER_ID);
        if (state.kind !== "trial") continue;

        const { text, keyboard } = await buildTrialEndingMessage(userId, expiresAt);
        await bot.api.sendMessage(userId, text, {
          parse_mode: "HTML",
          reply_markup: keyboard,
        });
      } catch (err: any) {
        // Most likely the user blocked the bot — nothing useful to do.
        console.error(`[TRIAL-WARN] notify ${userId} failed:`, err?.message);
      }
    }
  } catch (err: any) {
    console.error("[TRIAL-WARN] scheduler tick failed:", err?.message);
  }
}, TRIAL_WARNING_INTERVAL_MS);

async function trackUser(userId: number): Promise<void> {
  return trackUserMongo(userId);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function broadcastProgressText(total: number, sent: number, failed: number): string {
  const processed = sent + failed;
  const percent = total > 0 ? Math.floor((processed / total) * 100) : 0;
  return (
    "📢 <b>Broadcast in Progress</b>\n\n" +
    `👥 <b>Total Users:</b> ${total}\n` +
    `✅ <b>Sent:</b> ${sent}\n` +
    `❌ <b>Failed:</b> ${failed}\n` +
    `⏳ <b>Processed:</b> ${processed}/${total} (${percent}%)\n\n` +
    "Please wait..."
  );
}

function broadcastFinalText(total: number, sent: number, failed: number, failedUsers: number[]): string {
  const failedPreview = failedUsers.length
    ? "\n\n<b>Failed User IDs:</b>\n" + failedUsers.slice(0, 20).map((id) => `• <code>${id}</code>`).join("\n") + (failedUsers.length > 20 ? `\n...and ${failedUsers.length - 20} more` : "")
    : "";
  return (
    "✅ <b>Broadcast Completed</b>\n\n" +
    `👥 <b>Total Users:</b> ${total}\n` +
    `✅ <b>Successfully Sent:</b> ${sent}\n` +
    `❌ <b>Failed:</b> ${failed}\n\n` +
    "The broadcast message has been sent to all reachable users." +
    failedPreview
  );
}

async function sendBroadcastToUsers(adminId: number, progressMessageId: number, users: number[], message: string): Promise<void> {
  let sent = 0;
  let failed = 0;
  const failedUsers: number[] = [];
  const chunks = splitMessage(message, 4000);

  await bot.api.editMessageText(adminId, progressMessageId, broadcastProgressText(users.length, sent, failed), { parse_mode: "HTML" }).catch(() => {});

  for (let i = 0; i < users.length; i++) {
    const userId = users[i];
    try {
      for (const chunk of chunks) {
        await bot.api.sendMessage(userId, chunk);
      }
      sent++;
    } catch (err: any) {
      failed++;
      failedUsers.push(userId);
      console.error(`[BROADCAST] Failed for ${userId}:`, err?.message);
    }

    if ((i + 1) % 5 === 0 || i === users.length - 1) {
      await bot.api.editMessageText(adminId, progressMessageId, broadcastProgressText(users.length, sent, failed), { parse_mode: "HTML" }).catch(() => {});
    }
    await sleep(50);
  }

  const finalText = broadcastFinalText(users.length, sent, failed, failedUsers);
  await bot.api.editMessageText(adminId, progressMessageId, finalText, {
    parse_mode: "HTML",
    reply_markup: new InlineKeyboard().text("🏠 Menu", "main_menu"),
  }).catch(async () => {
    await bot.api.sendMessage(adminId, finalText, { parse_mode: "HTML" }).catch(() => {});
  });
}

async function checkForceSub(ctx: any): Promise<boolean> {
  if (!FORCE_SUB_CHANNEL) return true;
  const userId = ctx.from?.id;
  if (!userId) return false;
  if (isAdmin(userId)) return true;

  try {
    const member = await bot.api.getChatMember(FORCE_SUB_CHANNEL, userId);
    if (["member", "administrator", "creator"].includes(member.status)) return true;
  } catch (err: any) {
    console.error("[FORCE_SUB] Check error:", err?.message);
  }

  const channelName = FORCE_SUB_CHANNEL.replace(/^@/, "");
  const kb = new InlineKeyboard()
    .url("📢 Join Channel", `https://t.me/${channelName}`).text("✅ I Joined", "check_joined");
  try {
    await ctx.reply(
      "⛔ <b>Channel Subscription Required!</b>\n\n" +
      `📢 Join our channel to use this bot!\n\nChannel: @${esc(channelName)}\n\n` +
      "After joining click <b>✅ I Joined</b>",
      { parse_mode: "HTML", reply_markup: kb }
    );
  } catch {
    try {
      await ctx.editMessageText(
        "⛔ <b>Channel Subscription Required!</b>\n\n" +
        `📢 Join our channel to use this bot!\n\nChannel: @${esc(channelName)}\n\n` +
        "After joining click <b>✅ I Joined</b>",
        { parse_mode: "HTML", reply_markup: kb }
      );
    } catch {}
  }
  return false;
}

interface GroupSettings {
  name: string;
  description: string;
  count: number;
  finalNames: string[];
  namingMode: "auto" | "custom";
  dpBuffers: Buffer[];
  editGroupInfo: boolean;
  sendMessages: boolean;
  addMembers: boolean;
  approveJoin: boolean;
  disappearingMessages: number;
  friendNumbers: string[];
  makeFriendAdmin: boolean;
}

interface CtcPair {
  link: string;
  vcfContacts: Array<{ name: string; phone: string; vcfFileName: string }>;
}

interface SimilarGroup {
  base: string;
  groups: Array<{ id: string; subject: string }>;
}

interface UserState {
  step: string;
  groupSettings?: GroupSettings;
  groupCreationCancel?: boolean;
  // True while the cancel-confirmation dialog ("Are you sure?") is shown to
  // the user. The background creation loop must NOT edit the message while
  // this flag is on, otherwise the dialog gets overwritten by the next
  // progress update and the user can never tap "Yes, Cancel".
  groupCreationCancelPending?: boolean;
  ctcData?: {
    groupLinks: string[];
    pairs: CtcPair[];
    currentPairIndex: number;
  };
  joinData?: { links: string[] };
  leaveData?: {
    groups: Array<{ id: string; subject: string; isAdmin: boolean }>;
    mode: "member" | "admin" | "all";
    patterns?: SimilarGroup[];
    selectedGroups?: Array<{ id: string; subject: string; isAdmin: boolean }>;
    selectedIndices?: Set<number>;
    page?: number;
  };
  arData?: {
    allGroups: Array<{ id: string; subject: string }>;
    patterns: SimilarGroup[];
    selectedIndices: Set<number>;
    page: number;
  };
  removeData?: {
    allGroups: Array<{ id: string; subject: string }>;
    selectedIndices: Set<number>;
    page: number;
  };
  removeExcludeData?: {
    selectedGroups: Array<{ id: string; subject: string }>;
    excludeNumbers: Set<string>;
    excludePrefixes: Set<string>;
  };
  similarData?: {
    patterns: SimilarGroup[];
    allGroups: Array<{ id: string; subject: string }>;
  };
  pendingListData?: {
    patterns: SimilarGroup[];
    allPending: Array<{ groupId: string; groupName: string; pendingCount: number }>;
    selectedIndices?: Set<number>;
    page?: number;
  };
  makeAdminData?: {
    allGroups: Array<{ id: string; subject: string }>;
    patterns: SimilarGroup[];
    selectedIndices: Set<number>;
    page?: number;
  };
  approvalData?: {
    allGroups: Array<{ id: string; subject: string }>;
    patterns: SimilarGroup[];
    selectedIndices: Set<number>;
    page?: number;
    // Admin Approval flow extension:
    mode?: "all" | "admin_specific";
    targetPhones?: string[];
    makeAdminAfter?: boolean;
  };
  addMembersData?: {
    groupLink: string;
    groupId: string;
    groupName: string;
    groups: Array<{ link: string; id: string; name: string }>;
    multiGroup: boolean;
    friendNumbers: string[];
    adminContacts: Array<{ name: string; phone: string }>;
    navyContacts: Array<{ name: string; phone: string }>;
    memberContacts: Array<{ name: string; phone: string }>;
    totalToAdd: number;
    mode: "one_by_one" | "together" | "custom" | "";
    delaySeconds: number;
    cancelled: boolean;
    customBatchFriend?: number;
    customBatchAdmin?: number;
    customBatchNavy?: number;
    customBatchMember?: number;
    customStep?: "friend" | "admin" | "navy" | "member" | "done";
  };
  editSettingsData?: {
    allGroups: Array<{ id: string; subject: string }>;
    patterns: SimilarGroup[];
    selectedIndices: Set<number>;
    page: number;
    settings: GroupSettings;
    cancelled: boolean;
  };
  broadcastData?: {
    message: string;
    users: number[];
  };
  chatInGroupData?: {
    allGroups: Array<{ id: string; subject: string }>;
    selectedIndices: Set<number>;
    page: number;
    message: string;
    delaySeconds: number;
    cancelled: boolean;
    botMode?: "single" | "both";
  };
  autoConnectStep?: string;
  // ── Change Group Name feature ────────────────────────────────────────────
  // Two sub-flows share this state:
  //   "manual"  → user picks groups, then types names (auto-numbered or custom)
  //   "auto"    → user picks pending-only groups, uploads one VCF per group,
  //               bot matches each VCF to the group whose pending list contains
  //               it, then user chooses "same as VCF name" or "custom prefix"
  changeGroupNameData?: {
    mode: "manual" | "auto";
    // Manual: pool of admin groups the user is selecting from
    allGroups?: Array<{ id: string; subject: string }>;
    patterns?: SimilarGroup[];
    // Manual: which subset is currently being shown (similar pattern or all)
    selectionPool?: Array<{ id: string; subject: string }>;
    selectionPoolLabel?: string;
    // Insertion-ordered selection (so the user sees 1️⃣, 2️⃣, …)
    selectedGroupIds?: string[];
    page?: number;
    // Manual naming
    namingMode?: "auto" | "custom";
    baseName?: string;
    finalNames?: string[];
    // Auto: pool of groups that have pending requests
    pendingPool?: Array<{ groupId: string; groupName: string; pendingCount: number }>;
    pendingSelectedIds?: string[];
    pendingPage?: number;
    // Auto: collected VCF files (one per selected group, in upload order)
    vcfFiles?: Array<{ fileName: string; phones: string[] }>;
    // Auto naming
    autoNameMode?: "same_vcf" | "custom_vcf";
    customPrefix?: string;
    // Final review: list of {groupId, oldName, newName, vcfFileName?}
    renamePlan?: Array<{ groupId: string; oldName: string; newName: string; vcfFileName?: string }>;
    // Cancel signal for the background rename loop
    cancel?: boolean;
  };
}

interface AutoChatSession {
  running: boolean;
  cancelled: boolean;
  chatId: number;
  msgId: number;
  groups: Array<{ id: string; subject: string }>;
  message: string;
  delaySeconds: number;
  repeatCount: number;
  sent: number;
  failed: number;
  currentRound: number;
  rotationIndex: number;
}

interface CigSession {
  running: boolean;
  cancelled: boolean;
  chatId: number;
  msgId: number;
  groups: Array<{ id: string; subject: string }>;
  message: string;
  sent: number;
  failed: number;
  sentByAccount1: number;
  sentByAccount2: number;
  botMode: "single" | "both";
  currentGroupIndex: number;
  cycle: number;
  nextDelayMs: number;
  rotationIndex: number;
}

interface AcfSession {
  running: boolean;
  cancelled: boolean;
  chatId: number;
  msgId: number;
  primaryJid: string;
  autoJid: string;
  sent: number;
  failed: number;
  currentPair: number;
  totalPairs: number;
  cycle: number;
  nextDelayMs: number;
  rotationIndex: number;
}

const CHAT_FRIEND_PAIRS: [string, string][] = [
  ["Yaar, kal ka test tha kaisa gaya?", "Bilkul bekar 😭 Tu bata?"],
  ["Main sab bhool gaya tha 😂", "Hahaha mujhe bhi! Chalo saath mein rone wale hain 😂"],
  ["Kal physics padh le yaar seriously", "Haan yaar, aaj raat 11 baje call karte hain group mein"],
  ["Bhai tune notes liye the class mein?", "Nahi yaar main so gaya tha 🙈 Tu de de please"],
  ["Assignment submit ho gaya tera?", "Abhi nahi yaar, 2 ghante bacha hai deadline mein 😰"],
  ["Canteen ka khana aaj kaisa tha?", "Ekdum bekar! Ghar ka khana yaad aa gaya 😭"],
  ["Weekend pe kya plan hai?", "Bas ghar pe padhai... ya shayad nahi bhi 😄"],
  ["Bhai exam me kitna aaya?", "Puchh mat yaar... dard hota hai yaad karke 😂"],
  ["Tu serious kyun rehta hai har waqt?", "Serious nahi hoon yaar, bas aaj neend nahi aayi 😪"],
  ["Chal coffee peete hain baad mein?", "Haan bilkul! 3 baje canteen chalte hain ✅"],
  ["Bhai teacher ne aaj class mein kya padha?", "Pata nahi yaar, main phone pe tha 😬"],
  ["Tera homework hua kya?", "Homework? Wo toh kal subah 5 baje karenge jaise hamesha 😅"],
  ["Yaar kitna bada syllabus hai is baar!", "Haan bhai, rona aa raha hai dekh ke 😭"],
  ["Bhai galti se teacher ki aankhon mein dekh liya!", "Phir? Sun li lecture wali sirf tujhe hi? 😂"],
  ["Kal result aane wala hai yaar...", "Main toh kal school nahi aaunga 😂 Chhup jaunga ghar pe"],
  ["Yaar mera pen kho gaya phir se!", "Tera pen kho gaya ya tune diya kisi ko aur bhool gaya? 😏"],
  ["Physics ka formula yaad nahi ho raha", "Tension mat le, exam mein bhi nahi hoga yaad 😂"],
  ["Bhai library mein padhai hoti hai kya?", "Hoti toh hai... mujhe toh neend aati hai wahan 😴"],
  ["Yaar group project mein mera koi kaam nahi kiya!", "Welcome to team work 😂"],
  ["Teacher ne merit list nikaali, tera naam nahi tha!", "Iska matlab mujhe vacation ki zaroorat hai 😂"],
  ["Bhai aaj phir bunk maara tune?", "Yaar attendance ki fikr mat kar, marks bhi nahi aate toh bhi 😂"],
  ["Exam ke baad kya plan hai?", "Bhool ja sab aur so jaao teen din tak 😴"],
  ["Yaar notes share kar na please!", "Mere notes? Main khud copy karta hoon tere notes se 😂"],
  ["Bhai iss baar padhna hai seriously", "Haan same last baar bhi kaha tha, aur usse pehle bhi 😂"],
  ["Canteen mein aaj noodles the kaafi acche!", "Tu canteen gaya? Mujhe bata toh deta yaar 😤"],
  ["Yaar maths class mein so gaya tha", "Acha toh uss waqt main akela nahi tha 😴"],
  ["Teacher ne mujhe pakad liya mobile pe!", "Mujhe bhi kal hi pakda... solidarity yaar 😂"],
  ["Yaar padhai mein man nahi lagta", "Man kisi ka bhi nahi lagta, phir bhi karte hain 😅"],
  ["Bhai principal office mein kyon bula rahe hain?", "Pray kar yaar aur sach mat bolna 😂"],
  ["Teri girlfriend hai kya school mein?", "Haan, merī books... unse hi pyaar hai 😂"],
  ["Yaar kal presentation hai, ready hai tu?", "Presentation? Kaun sa topic tha yaar 😅"],
  ["Bhai aaj phir baarish mein bheega?", "Haan yaar, umbrella ghar pe hi reh gaya jaisa hamesha 😭"],
  ["Yaar tere marks kitne aaye iss baar?", "Itne kam ki calculator se bhi nahi ginne 😂"],
  ["Bhai chemistry experiment mein kuch jalaya tune!", "Sirf thoda sa... science toh yahi hota hai na 😂"],
];

// Sequential delay rotation: 1min → 2min → 3min → 4min → 5min → repeat
const CHAT_DELAY_ROTATION_MS = [
  1 * 60 * 1000,
  2 * 60 * 1000,
  3 * 60 * 1000,
  4 * 60 * 1000,
  5 * 60 * 1000,
];

const AUTO_GROUP_MESSAGES = CHAT_FRIEND_PAIRS.flat();

function getSequentialDelayMs(rotationIndex: number): number {
  return CHAT_DELAY_ROTATION_MS[rotationIndex % CHAT_DELAY_ROTATION_MS.length];
}

function formatDelay(ms: number): string {
  if (ms < 60 * 1000) return `${Math.round(ms / 1000)} sec`;
  if (ms < 60 * 60 * 1000) return `${Math.round(ms / (60 * 1000))} min`;
  const hours = ms / (60 * 60 * 1000);
  return `${Number.isInteger(hours) ? hours : hours.toFixed(1)} hour${hours === 1 ? "" : "s"}`;
}

function isSessionActive(session: { cancelled: boolean; running: boolean }): boolean {
  return !session.cancelled && session.running;
}

async function waitWithCancel(session: { cancelled: boolean; running: boolean }, delayMs: number): Promise<void> {
  const stepMs = 5000;
  let waited = 0;
  while (!session.cancelled && session.running && waited < delayMs) {
    const remaining = delayMs - waited;
    const next = Math.min(stepMs, remaining);
    await sleep(next);
    waited += next;
  }
}

const autoChatSessions: Map<number, AutoChatSession> = new Map();
const cigSessions: Map<number, CigSession> = new Map();
const acfSessions: Map<number, AcfSession> = new Map();
const userStates: Map<number, UserState> = new Map();

// ─────────────────────────────────────────────────────────────────────────────
// User-activity tracking (in-memory). Drives three behaviours:
//   1. The "✅ WhatsApp connected +XXX" celebration message on /start is only
//      shown the first time per session window (i.e. when the user has been
//      idle for >= USER_IDLE_DISCONNECT_MS, or has never used the bot since
//      the process started). On subsequent /start calls within the active
//      window, the menu appears without the connection toast.
//   2. Any button press or text message refreshes lastActivityAt — the user
//      is "active" for another 30 minutes from that point.
//   3. A background timer disconnects WhatsApp for users idle >=
//      USER_IDLE_DISCONNECT_MS and remembers that we did so (idleDisconnected
//      flag). On the next interaction, the connection is restored silently
//      from the stored Mongo session if available.
// ─────────────────────────────────────────────────────────────────────────────
const USER_IDLE_DISCONNECT_MS = Number(
  process.env.USER_IDLE_DISCONNECT_MS || String(30 * 60 * 1000)
);
const USER_IDLE_CHECK_INTERVAL_MS = Number(
  process.env.USER_IDLE_CHECK_INTERVAL_MS || String(5 * 60 * 1000)
);

interface UserActivity {
  lastActivityAt: number;
  // Set to true after the background idle timer disconnects this user. Reset
  // to false the next time they interact with the bot.
  idleDisconnected: boolean;
}

const userActivity: Map<number, UserActivity> = new Map();

function isUserActive(userId: number): boolean {
  const a = userActivity.get(userId);
  if (!a) return false;
  return Date.now() - a.lastActivityAt < USER_IDLE_DISCONNECT_MS;
}

// Returns true when this interaction is the first one in a new active
// window (i.e. the user was previously idle/never seen). Callers like the
// /start handler use this to decide whether to show the WhatsApp
// "connected" toast — we only want it once per session window, not on
// every /start tap.
function markUserActive(userId: number): boolean {
  const now = Date.now();
  const existing = userActivity.get(userId);
  if (!existing) {
    userActivity.set(userId, { lastActivityAt: now, idleDisconnected: false });
    return true;
  }
  const wasIdle = existing.idleDisconnected || (now - existing.lastActivityAt >= USER_IDLE_DISCONNECT_MS);
  existing.lastActivityAt = now;
  existing.idleDisconnected = false;
  return wasIdle;
}

// True if the most recent markUserActive call started a new active window.
// We can't simply re-check on demand because the middleware updates the
// timestamp on every update — once /start runs, the user is already
// "active". Cache the result per-update via a tiny in-memory flag.
const newSessionFlag: Map<number, boolean> = new Map();

// Silent reconnect: if the user has a stored WhatsApp session but the in-memory
// socket has been evicted (process restart, idle disconnect, etc.), trigger a
// background reload. Returns immediately — the menu/button flow continues
// without waiting. The connect handlers in connectWhatsApp itself will set the
// connected flag once the socket is up.
async function ensureWhatsAppRestored(userId: number): Promise<void> {
  const uid = String(userId);
  if (isConnected(uid)) return;
  try {
    const stored = await hasStoredWhatsAppSession(uid);
    if (!stored) return;
    // Fire-and-forget — ensureSessionLoaded handles its own concurrency guards.
    ensureSessionLoaded(uid).catch((err) => {
      console.error(`[BOT] silent restore failed for ${userId}:`, err?.message);
    });
  } catch {}
}
const joinCancelRequests: Set<number> = new Set();
const getLinkCancelRequests: Set<number> = new Set();
const addMembersCancelRequests: Set<number> = new Set();
const removeMembersCancelRequests: Set<number> = new Set();
// Same cancel pattern, but for the "Approve 1 by 1" flow. The background
// loop checks this each iteration so the user can stop mid-run after
// confirming the cancel-dialog (Yes/No).
const approvalCancelRequests: Set<number> = new Set();

// ── Cancel-dialog protection ────────────────────────────────────────────────
// When a user taps a "❌ Cancel" button on a long-running flow, the bot shows
// an "Are you sure?" confirmation by changing only the inline keyboard. The
// underlying message text is still the in-progress status. Without protection
// the next progress update from the background task would call
// editMessageText(...) with a fresh "❌ Cancel" reply_markup — which wipes
// the Yes/No dialog the user is staring at and makes it look like cancel
// silently failed. This Set tracks "cancel dialog currently open for this
// user". Background tasks check it with safeBackgroundEdit() below and skip
// the edit until the dialog is dismissed (No) or confirmed (Yes).
const cancelDialogActiveFor: Set<number> = new Set();

async function safeBackgroundEdit(
  userId: number,
  chatId: number,
  msgId: number,
  text: string,
  options?: any,
): Promise<void> {
  if (cancelDialogActiveFor.has(userId)) return; // dialog open — don't clobber
  try {
    await bot.api.editMessageText(chatId, msgId, text, options);
  } catch {}
}

let autoChatGlobalEnabled: boolean = true;
const autoChatAccessSet: Set<number> = new Set();

function canUserSeeAutoChat(userId: number): boolean {
  if (isAdmin(userId)) return true;
  if (autoChatGlobalEnabled) return true;
  return autoChatAccessSet.has(userId);
}

async function syncAutoChatSettings(): Promise<void> {
  try {
    const data = await loadBotData();
    autoChatGlobalEnabled = data.autoChatEnabled ?? true;
    autoChatAccessSet.clear();
    for (const id of data.autoChatAccessList ?? []) {
      autoChatAccessSet.add(id);
    }
  } catch (err: any) {
    console.error("[AutoChat] syncAutoChatSettings error:", err?.message);
  }
}

const MA_PAGE_SIZE = 20;
const PL_PAGE_SIZE = 20;
const AP_PAGE_SIZE = 20;

function pendingSumExpression(items: Array<{ pendingCount: number }>): string {
  const counts = items.map((g) => g.pendingCount).filter((count) => count > 0);
  const total = counts.reduce((sum, count) => sum + count, 0);
  return counts.length ? `${counts.join("+")} = ${total}` : "0";
}

function pendingCopyText(title: string, items: Array<{ groupName: string; pendingCount: number }>): string {
  // Natural numeric sort so "SK 1, 2, 3, ... 14, 15" comes in correct order
  // (instead of plain alphabetic which gives "SK 1, 10, 11, 12, 14, 2, 20, 3...")
  const sorted = [...items].sort((a, b) => a.groupName.localeCompare(b.groupName, undefined, { numeric: true, sensitivity: "base" }));
  let text = `📋 <b>${esc(title)}</b>\n\n<pre>`;
  for (const g of sorted) {
    text += `${g.groupName} ✅ ${g.pendingCount}\n`;
  }
  text += `\nTotal sum = ${pendingSumExpression(sorted)}`;
  text += `</pre>`;
  return text;
}

function buildPendingListKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const allPending = state.pendingListData!.allPending;
  const selected = state.pendingListData!.selectedIndices || new Set<number>();
  const page = state.pendingListData!.page || 0;
  const totalPages = Math.max(1, Math.ceil(allPending.length / PL_PAGE_SIZE));
  const start = page * PL_PAGE_SIZE;
  const end = Math.min(start + PL_PAGE_SIZE, allPending.length);

  for (let i = start; i < end; i++) {
    const g = allPending[i];
    const isSelected = selected.has(i);
    kb.text(`${isSelected ? "✅" : "☐"} ${g.groupName} (${g.pendingCount})`, `pl_tog_${i}`).row();
  }

  if (totalPages > 1) {
    const prev = page > 0 ? "⬅️ Prev" : " ";
    const next = page < totalPages - 1 ? "Next ➡️" : " ";
    kb.text(prev, "pl_prev_page").text(`📄 ${page + 1}/${totalPages}`, "pl_page_info").text(next, "pl_next_page").row();
  }

  kb.text("☑️ Select All", "pl_select_all").text("🧹 Clear All", "pl_clear_all").row();
  if (selected.size > 0) kb.text(`📋 Show Copy Format (${selected.size})`, "pl_proceed").row();
  kb.text("🏠 Main Menu", "main_menu");
  return kb;
}

interface QrPairingState {
  chatId: number;
  statusMessageId?: number;
  qrMessageId?: number;
  interval?: ReturnType<typeof setInterval>;
  expired?: boolean;
  qrLocked?: boolean;
}

const qrPairings: Map<number, QrPairingState> = new Map();

// Cleanup interval (15 min) — keeps RAM footprint tight on low-memory hosts
// (Render free 512MB) when 500-1000 concurrent users are connected.
const MEMORY_CLEANUP_INTERVAL_MS = Number(process.env.MEMORY_CLEANUP_INTERVAL_MS || String(15 * 60 * 1000));

// Snapshot of RSS at module load — used by /memory to show "growth since
// startup" so admin can see at a glance whether RAM is creeping up over
// uptime or staying flat. Captured here (not inside the handler) so the
// reading is the actual baseline, not the post-warmup value.
const STARTUP_RSS_MB = process.memoryUsage().rss / 1024 / 1024;
const STARTUP_TIMESTAMP_MS = Date.now();
// Drop /help pagination state for users idle longer than this. Each entry
// can hold ~10–20KB of HTML chunks; if 1000 users press /help we'd be
// keeping 10–20MB live forever without this.
const HELP_PAGES_STALE_MS = 30 * 60 * 1000;
const helpPagesLastTouched: Map<number, number> = new Map();
setInterval(() => {
  const activeUserIds = new Set([
    ...autoChatSessions.keys(),
    ...cigSessions.keys(),
    ...acfSessions.keys(),
  ]);
  for (const [userId, state] of userStates) {
    // Keep state for users currently in a long-running session (auto chat,
    // CIG, ACF) AND for users whose last interaction was within the active
    // window. Without this, multi-step flows like "Create Groups → wait 15
    // min → enter group name" lose their step and silently drop the input.
    if (activeUserIds.has(userId) || isUserActive(userId)) continue;
    // Eagerly drop any large Buffers (group DPs / edit-settings DPs) so
    // they become GC-able immediately, not at the next heap pressure.
    if (state.groupSettings) state.groupSettings.dpBuffers = [];
    if (state.editSettingsData) state.editSettingsData.settings.dpBuffers = [];
    userStates.delete(userId);
  }
  // Drop activity entries for users idle far longer than the disconnect
  // window so the map doesn't grow unbounded across days.
  const STALE_ACTIVITY_MS = USER_IDLE_DISCONNECT_MS * 4;
  for (const [userId, a] of userActivity) {
    if (Date.now() - a.lastActivityAt > STALE_ACTIVITY_MS) {
      userActivity.delete(userId);
    }
  }
  for (const [userId, state] of qrPairings) {
    if (state.expired) {
      if (state.interval) clearInterval(state.interval);
      qrPairings.delete(userId);
    }
  }
  // Drop stale /help pagination state — keeps ~10-20KB per entry from
  // accumulating forever when users open /help and never come back.
  const now = Date.now();
  for (const [userId, touchedAt] of helpPagesLastTouched) {
    if (now - touchedAt > HELP_PAGES_STALE_MS) {
      helpPages.delete(userId);
      helpPagesLastTouched.delete(userId);
    }
  }
  // newSessionFlag is a per-update flag; if anything stuck in there from
  // dead users, drop it. It's tiny but bounded == bounded.
  if (newSessionFlag.size > 1000) newSessionFlag.clear();
  joinCancelRequests.clear();
  getLinkCancelRequests.clear();
  addMembersCancelRequests.clear();
  removeMembersCancelRequests.clear();
  // Double-pass GC with a small wait between passes. A single gc() leaves
  // partially-promoted objects in the old generation; doing a second pass
  // after a 50ms gap lets V8 finish the sweep and gives glibc malloc (which
  // we've capped to 2 arenas via MALLOC_ARENA_MAX=2) a chance to actually
  // return freed pages to the OS — which is what makes RSS visibly drop
  // instead of climbing forever as uptime grows.
  if (typeof (global as any).gc === "function") {
    try { (global as any).gc(); } catch {}
    setTimeout(() => {
      try { (global as any).gc(); } catch {}
    }, 50);
  }
  const mem = process.memoryUsage();
  const rssMb = Math.round(mem.rss / 1024 / 1024);
  const heapMb = Math.round(mem.heapUsed / 1024 / 1024);
  console.log(
    `[MEMORY] Cleanup: rss=${rssMb}MB heap=${heapMb}MB userStates=${userStates.size} autoChat=${autoChatSessions.size} cig=${cigSessions.size} acf=${acfSessions.size} qr=${qrPairings.size} helpPages=${helpPages.size}`
  );
}, MEMORY_CLEANUP_INTERVAL_MS);

// ── Idle-disconnect timer ─────────────────────────────────────────────────
// Walk every connected WhatsApp session and disconnect users who have been
// idle for >= USER_IDLE_DISCONNECT_MS. Long-running flows that imply the
// user is still working in the background (auto chat, chat-in-group, auto
// chat friend) are exempt — we don't want to kill a user's CIG run just
// because they walked away from Telegram. The next time the user taps any
// button or sends a message, the bot.use middleware silently restores the
// session from Mongo and the connection toast appears once (showing the
// user the freshly restored connection).
setInterval(async () => {
  try {
    const liveSessions = getActiveSessionUserIds();
    const longRunning = new Set<number>([
      ...autoChatSessions.keys(),
      ...cigSessions.keys(),
      ...acfSessions.keys(),
    ]);
    for (const uidStr of liveSessions) {
      const uid = Number(uidStr);
      if (!Number.isFinite(uid)) continue;
      if (longRunning.has(uid)) continue;
      const a = userActivity.get(uid);
      // No recorded activity OR activity older than the window → disconnect.
      const idleFor = a ? Date.now() - a.lastActivityAt : Number.POSITIVE_INFINITY;
      if (idleFor < USER_IDLE_DISCONNECT_MS) continue;
      try {
        // IMPORTANT: use idleDisconnectWhatsApp (memory-only eviction).
        // disconnectWhatsApp() would call socket.logout() — which unlinks
        // the device on WhatsApp servers — AND clear MongoDB creds, so
        // the user would have to re-pair from scratch on next interaction.
        // We only want to free RAM and let ensureSessionLoaded() silently
        // restore the socket from saved creds when the user comes back.
        await idleDisconnectWhatsApp(uidStr);
        if (a) a.idleDisconnected = true;
        else userActivity.set(uid, { lastActivityAt: Date.now() - USER_IDLE_DISCONNECT_MS, idleDisconnected: true });
        console.log(`[BOT] Idle disconnect for user ${uid} after ${Math.round(idleFor / 60000)} min`);
      } catch (err: any) {
        console.error(`[BOT] Idle disconnect error for ${uid}:`, err?.message);
      }
    }
  } catch (err: any) {
    console.error(`[BOT] Idle-disconnect sweep error:`, err?.message);
  }
}, USER_IDLE_CHECK_INTERVAL_MS);

// ── High-memory alert: ping admin on Telegram when RSS crosses threshold ──
// Checks every 1 min. Sends alert when RSS >= MEMORY_ALERT_THRESHOLD_PCT of
// MEMORY_ALERT_LIMIT_MB. Cooldown prevents spam — once alerted, won't alert
// again until either RAM drops below threshold OR cooldown expires.
const MEMORY_ALERT_LIMIT_MB = Number(process.env.MEMORY_ALERT_LIMIT_MB || "512");
const MEMORY_ALERT_THRESHOLD_PCT = Number(process.env.MEMORY_ALERT_THRESHOLD_PCT || "85");
const MEMORY_ALERT_COOLDOWN_MS = Number(process.env.MEMORY_ALERT_COOLDOWN_MS || String(30 * 60 * 1000));
let memoryAlertLastSentAt = 0;
let memoryAlertActive = false;
setInterval(() => {
  try {
    const mem = process.memoryUsage();
    const rssMb = mem.rss / 1024 / 1024;
    const heapUsedMb = mem.heapUsed / 1024 / 1024;
    const heapTotalMb = mem.heapTotal / 1024 / 1024;
    const rssPct = (rssMb / MEMORY_ALERT_LIMIT_MB) * 100;
    const now = Date.now();

    if (rssPct >= MEMORY_ALERT_THRESHOLD_PCT) {
      const cooldownOver = now - memoryAlertLastSentAt >= MEMORY_ALERT_COOLDOWN_MS;
      // Send only on the first crossing OR after cooldown — avoids spamming
      // the admin every minute while RAM stays high.
      if (!memoryAlertActive || cooldownOver) {
        memoryAlertActive = true;
        memoryAlertLastSentAt = now;
        const text =
          `⚠️ <b>High RAM Alert</b>\n\n` +
          `📦 RSS: <b>${rssMb.toFixed(1)} MB</b> / ${MEMORY_ALERT_LIMIT_MB} MB ` +
          `(<b>${rssPct.toFixed(0)}%</b>)\n` +
          `🔵 Heap: ${heapUsedMb.toFixed(1)} MB / ${heapTotalMb.toFixed(1)} MB\n\n` +
          `👥 Active Sessions:\n` +
          `  📱 WhatsApp: ${getActiveSessionUserIds().size}\n` +
          `  🤖 Auto Chat: ${autoChatSessions.size} / ${MAX_CONCURRENT_AUTOCHAT}\n` +
          `  💬 Chat-In-Group: ${cigSessions.size}\n` +
          `  🔁 Auto Chat Friend: ${acfSessions.size}\n\n` +
          `💡 Use /memory for full details.`;
        bot.api.sendMessage(ADMIN_USER_ID, text, { parse_mode: "HTML" }).catch((err) => {
          console.error(`[MEMORY_ALERT] Failed to notify admin:`, err?.message);
        });
        console.log(`[MEMORY_ALERT] Triggered: rss=${rssMb.toFixed(1)}MB (${rssPct.toFixed(0)}%)`);
      }
    } else if (memoryAlertActive && rssPct < MEMORY_ALERT_THRESHOLD_PCT - 5) {
      // Reset only after RAM drops at least 5% below threshold (hysteresis) so
      // we don't flap if RSS hovers right at the boundary.
      memoryAlertActive = false;
      console.log(`[MEMORY_ALERT] Cleared: rss=${rssMb.toFixed(1)}MB (${rssPct.toFixed(0)}%)`);
    }
  } catch (err: any) {
    console.error(`[MEMORY_ALERT] check error:`, err?.message);
  }
}, 60 * 1000);

function qrActiveKeyboard(): InlineKeyboard {
  return new InlineKeyboard().text("❌ Cancel", "connect_pair_qr_cancel").text("🔙 Back", "connect_wa");
}

function qrExpiredKeyboard(): InlineKeyboard {
  return new InlineKeyboard().text("🔄 Retry", "connect_pair_qr_retry").text("🔙 Back", "connect_wa");
}

function qrCaption(remainingSeconds: number): string {
  return (
    "📷 <b>Pair WhatsApp with QR</b>\n\n" +
    "1️⃣ WhatsApp open karo\n" +
    "2️⃣ Settings → Linked Devices\n" +
    "3️⃣ Link a Device tap karo\n" +
    "4️⃣ Ye QR scan karo\n\n" +
    `⏳ QR expires in: <b>${remainingSeconds}s</b>`
  );
}

function clearQrPairing(userId: number): void {
  const existing = qrPairings.get(userId);
  if (existing?.interval) clearInterval(existing.interval);
  qrPairings.delete(userId);
}

async function safeDeleteMessage(chatId: number, messageId?: number): Promise<void> {
  if (!messageId) return;
  try { await bot.api.deleteMessage(chatId, messageId); } catch {}
}

async function startQrPairing(ctx: any, userId: number): Promise<void> {
  if (!(await checkAccessMiddleware(ctx))) return;
  if (isConnected(String(userId))) {
    await ctx.editMessageText(
      "✅ <b>WhatsApp already connected!</b>\n\nYou can use all features.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
    return;
  }

  clearQrPairing(userId);
  userStates.delete(userId);

  const chatId = ctx.chat?.id;
  if (!chatId) return;

  let statusMessageId = ctx.callbackQuery?.message?.message_id;
  try {
    if (statusMessageId) {
      await ctx.editMessageText(
        "⏳ <b>Generating WhatsApp QR...</b>\n\nPlease wait a few seconds.",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔙 Back", "connect_wa") }
      );
    } else {
      const sent = await ctx.reply(
        "⏳ <b>Generating WhatsApp QR...</b>\n\nPlease wait a few seconds.",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔙 Back", "connect_wa") }
      );
      statusMessageId = sent.message_id;
    }
  } catch {
    const sent = await ctx.reply(
      "⏳ <b>Generating WhatsApp QR...</b>\n\nPlease wait a few seconds.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔙 Back", "connect_wa") }
    );
    statusMessageId = sent.message_id;
  }

  qrPairings.set(userId, { chatId, statusMessageId });

  await connectWhatsAppQr(
    String(userId),
    async (qr, expiresAt) => {
      const active = qrPairings.get(userId);
      if (!active || active.expired || active.qrLocked) return;
      active.qrLocked = true;
      if (active.interval) clearInterval(active.interval);
      await safeDeleteMessage(active.chatId, active.qrMessageId);

      const remaining = Math.max(0, Math.ceil((expiresAt - Date.now()) / 1000));
      const buffer = await QRCode.toBuffer(qr, {
        type: "png",
        width: 420,
        margin: 2,
        errorCorrectionLevel: "M",
      });

      await safeDeleteMessage(active.chatId, active.statusMessageId);
      const sent = await bot.api.sendPhoto(
        active.chatId,
        new InputFile(buffer, "whatsapp-qr.png"),
        { caption: qrCaption(remaining), parse_mode: "HTML", reply_markup: qrActiveKeyboard() }
      );

      active.qrMessageId = sent.message_id;
      active.statusMessageId = undefined;
      active.interval = setInterval(async () => {
        const current = qrPairings.get(userId);
        if (!current || current.qrMessageId !== sent.message_id) return;
        const left = Math.max(0, Math.ceil((expiresAt - Date.now()) / 1000));
        if (left <= 0) {
          if (current.interval) clearInterval(current.interval);
          current.expired = true;
          if (!isConnected(String(userId))) {
            await safeDeleteMessage(current.chatId, current.qrMessageId);
            current.qrMessageId = undefined;
            await disconnectWhatsApp(String(userId));
            await bot.api.sendMessage(
              current.chatId,
              "⌛ <b>Your QR code has expired.</b>\n\nIf you are unable to connect via QR, please try linking with a pair code instead.\n\nClick <b>Retry</b> to generate a new QR code.",
              { parse_mode: "HTML", reply_markup: qrExpiredKeyboard() }
            );
          }
          return;
        }
        try {
          await bot.api.editMessageCaption(current.chatId, current.qrMessageId!, {
            caption: qrCaption(left),
            parse_mode: "HTML",
            reply_markup: qrActiveKeyboard(),
          });
        } catch {}
      }, 1000);
    },
    async () => {
      const active = qrPairings.get(userId);
      clearQrPairing(userId);
      if (active) await safeDeleteMessage(active.chatId, active.qrMessageId || active.statusMessageId);
      await bot.api.sendMessage(chatId, whatsappConnectedText(userId, "🎉 QR scan successful. All features are now available."), {
        parse_mode: "HTML",
        reply_markup: mainMenu(userId),
      });
    },
    async (reason) => {
      const active = qrPairings.get(userId);
      clearQrPairing(userId);
      if (active) await safeDeleteMessage(active.chatId, active.qrMessageId);
      await bot.api.sendMessage(chatId, `⚠️ <b>WhatsApp Disconnected</b>\n\nReason: ${esc(reason)}\n\n🔄 Try QR pairing again.`, {
        parse_mode: "HTML",
        reply_markup: qrExpiredKeyboard(),
      });
    }
  );
}

function mainMenu(userId?: number): InlineKeyboard {
  const connected = userId !== undefined && isConnected(String(userId));
  const kb = new InlineKeyboard();
  if (!connected) {
    kb.text("📱 Connect WhatsApp", "connect_wa").row();
  }
  kb
    .text("👥 Create Groups", "create_groups").text("🔗 Join Groups", "join_groups").row()
    .text("🔍 CTC Checker", "ctc_checker").text("🔗 Get Link", "get_link").row()
    .text("🚪 Leave Group", "leave_group").text("🗑️ Remove Members", "remove_members").row()
    .text("👑 Make Admin", "make_admin").text("✅ Approval", "approval").row()
    .text("📋 Get Pending List", "pending_list").text("➕ Add Members", "add_members").row()
    .text("⚙️ Edit Settings", "edit_settings").text("🏷️ Change Name", "change_group_name").row()
    .text("🛡️ Auto Accepter", "auto_accepter").row();
  if (userId !== undefined && canUserSeeAutoChat(userId)) {
    kb.text("🤖 Auto Chat", "auto_chat_menu").row();
  }
  if (connected) {
    kb.text("🔄 Session Refresh", "session_refresh").text("🔌 Disconnect", "disconnect_wa");
  } else {
    kb.text("🔌 Disconnect", "disconnect_wa");
  }
  return kb;
}

bot.callbackQuery("check_joined", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!FORCE_SUB_CHANNEL) {
    await ctx.editMessageText("✅ Bot is ready! Use /start to begin.");
    return;
  }
  try {
    const member = await bot.api.getChatMember(FORCE_SUB_CHANNEL, userId);
    if (["member", "administrator", "creator"].includes(member.status)) {
      const data = await loadBotData();

      // ── Award any pending referral now that the user has joined the
      // required channel. The referrer-id was stashed by /start when the
      // user first opened "/start ref_<id>" but failed the force-sub
      // guard. Awarding here means a user who joins the channel during
      // the force-sub flow still earns the referrer their +1 day —
      // previously this was silently dropped. Idempotent (recordReferral
      // dedupes), and we delete the pending entry to free the map.
      const pending = pendingReferrals.get(userId);
      if (pending) {
        pendingReferrals.delete(userId);
        await processReferralAward(userId, pending.referrerId);
      }

      // First-time users (no language picked yet) → show language picker.
      // Trial creation + trial banner are deferred until after language
      // selection (handled in applyLanguageSelection), matching the /start
      // flow so the trial banner never appears stacked on the language picker.
      if (!hasUserLang(userId)) {
        try { await ctx.deleteMessage(); } catch {}
        await sendLanguagePicker(ctx, true);
        return;
      }

      // Returning users (lang already set): start trial if not yet created.
      let trialJustStarted: { expiresAt: number } | null = null;
      if (!isAdmin(userId)) {
        const trial = await ensureFreeTrial(userId, FREE_TRIAL_MS);
        if (trial.created) trialJustStarted = { expiresAt: trial.expiresAt };
      }

      // Even in refer mode, if the user somehow has no access at all
      // (e.g. trial already expired and no referral), surface the refer
      // message instead of the menu.
      if (!(await hasAccess(userId))) {
        if (data.referMode) {
          const { text, keyboard } = await buildReferRequiredMessage(userId);
          await ctx.editMessageText(text, { parse_mode: "HTML", reply_markup: keyboard });
        } else {
          await ctx.editMessageText(
            `🔒 <b>Subscription Required!</b>\n\n👤 Contact owner: <b>${OWNER_USERNAME}</b>`,
            { parse_mode: "HTML" }
          );
        }
        return;
      }

      await ctx.editMessageText(
        mainMenuText(userId, "welcome"),
        { parse_mode: "HTML", reply_markup: mainMenu(userId) }
      );
      if (trialJustStarted) {
        await ctx.reply(trialStartedMessage(trialJustStarted.expiresAt), { parse_mode: "HTML" });
      }
      return;
    }
  } catch {}
  await ctx.answerCallbackQuery({ text: "❌ You haven't joined the channel yet!", show_alert: true });
});

// Render a 10-segment progress bar as text: e.g. [█████░░░░░] 50%
function renderProgressBar(pct: number): string {
  const clamped = Math.max(0, Math.min(100, Math.floor(pct)));
  const filled = Math.round(clamped / 10);
  return `[${"█".repeat(filled)}${"░".repeat(10 - filled)}] ${clamped}%`;
}

// On /start, if the user has a saved WhatsApp session that isn't currently
// connected (e.g. the socket was evicted from memory or the bot just
// restarted), show a live progress bar that ticks while we restore the
// session in the background. Once connected, the message updates to a
// "✅ WhatsApp connected" confirmation. If restoration fails or times out,
// it gracefully falls through so the main menu still appears.
async function showWhatsAppConnectingProgress(ctx: any, userId: number): Promise<void> {
  const uid = String(userId);

  // Only surface the connection toast/progress bar when this /start kicks
  // off a brand-new active window (first /start of the session, or first
  // /start after a 30-min idle gap). Otherwise the user sees the same
  // "✅ WhatsApp connected +XXX" message every time they tap /start,
  // which is exactly what the user reported. If the user is mid-session,
  // the menu appears immediately with no toast.
  const isNewSession = newSessionFlag.get(userId) === true;
  if (!isNewSession) return;

  // Already live? Just show a quick confirmation, no progress bar needed.
  if (isConnected(uid)) {
    try {
      const phone = getConnectedWhatsAppNumber(uid);
      const phoneTxt = phone ? ` <code>+${phone}</code>` : "";
      const msg = await ctx.reply(`✅ <b>WhatsApp connected${phoneTxt}</b>`, { parse_mode: "HTML" });
      // Auto-delete after 5s so the chat stays clean (matches the post-
      // restore confirmation behaviour below).
      setTimeout(() => {
        ctx.api.deleteMessage(msg.chat.id, msg.message_id).catch(() => {});
      }, 5000);
    } catch {}
    return;
  }

  // No saved session at all — nothing to wait for, the menu's "Connect
  // WhatsApp" button will handle pairing.
  let hasStored = false;
  try { hasStored = await hasStoredWhatsAppSession(uid); } catch {}
  if (!hasStored) return;

  // Send the initial progress message; if it fails, abort silently — the
  // menu will still be shown by the caller.
  let msg: any;
  try {
    msg = await ctx.reply(
      `⏳ <b>Connecting your WhatsApp...</b>\n${renderProgressBar(0)}\n\n<i>This usually takes 5–15 seconds.</i>`,
      { parse_mode: "HTML" }
    );
  } catch {
    return;
  }

  const TOTAL_MS = 30_000;
  const TICK_MS = 1_500;
  const startedAt = Date.now();
  let lastPct = -1;
  let stopped = false;

  // Background ticker — edits the message every TICK_MS until either we're
  // connected or the timeout hits. Skips edits when % hasn't changed (avoids
  // Telegram's "message is not modified" error).
  const ticker = setInterval(async () => {
    if (stopped) return;
    if (isConnected(uid)) return; // final edit handled below
    const elapsed = Date.now() - startedAt;
    // Cap visible progress at 95% until truly connected so it doesn't
    // mislead the user when something stalls.
    const pct = Math.min(95, Math.floor((elapsed / TOTAL_MS) * 100));
    if (pct === lastPct) return;
    lastPct = pct;
    try {
      await ctx.api.editMessageText(
        msg.chat.id,
        msg.message_id,
        `⏳ <b>Connecting your WhatsApp...</b>\n${renderProgressBar(pct)}\n\n<i>This usually takes 5–15 seconds.</i>`,
        { parse_mode: "HTML" }
      );
    } catch {}
  }, TICK_MS);

  let connected = false;
  try {
    connected = await waitForWhatsAppConnected(uid, { timeoutMs: TOTAL_MS, pollMs: 500 });
  } catch {}
  stopped = true;
  clearInterval(ticker);

  // Final message: success or graceful fallback.
  try {
    if (connected) {
      const phone = getConnectedWhatsAppNumber(uid);
      const phoneTxt = phone ? ` <code>+${phone}</code>` : "";
      await ctx.api.editMessageText(
        msg.chat.id,
        msg.message_id,
        `✅ <b>WhatsApp connected${phoneTxt}</b>\n${renderProgressBar(100)}`,
        { parse_mode: "HTML" }
      );
      // Auto-delete the success message after 5s so the chat stays clean —
      // user already sees the menu right below it.
      setTimeout(() => {
        ctx.api.deleteMessage(msg.chat.id, msg.message_id).catch(() => {});
      }, 5000);
    } else {
      await ctx.api.editMessageText(
        msg.chat.id,
        msg.message_id,
        `⚠️ <b>WhatsApp not connected yet.</b>\n\n` +
        `It might still be reconnecting in the background, or you may need ` +
        `to reconnect manually from the menu.`,
        { parse_mode: "HTML" }
      );
    }
  } catch {}
}

// Parse /start payload — supports plain "/start" and deep links such as
// "/start ref_12345" used by the referral system.
function parseStartPayload(text: string | undefined): string {
  if (!text) return "";
  const m = text.match(/^\/start(?:@\w+)?(?:\s+(.+))?$/i);
  return (m?.[1] || "").trim();
}

bot.command("start", async (ctx) => {
  const userId = ctx.from!.id;
  await trackUser(userId);
  if (await isBanned(userId)) {
    await ctx.reply("🚫 You are banned from using this bot.");
    return;
  }

  // ── Parse referral payload FIRST (before the force-sub guard) ─────────
  // We need the referrer-id captured even if checkForceSub returns false,
  // otherwise users who join the channel via the force-sub flow lose the
  // referral credit (the original /start ref_<id> message is not available
  // inside the check_joined callback). If force-sub fails below, we stash
  // it in pendingReferrals; check_joined consumes it after a successful
  // join. If force-sub passes (already joined), we award immediately so
  // the existing fast-path behaviour is preserved.
  const payload = parseStartPayload(ctx.message?.text);
  const refMatch = payload.match(/^ref_(\d+)$/i);
  const referrerId = refMatch ? Number(refMatch[1]) : 0;

  if (!(await checkForceSub(ctx))) {
    // User is being asked to join the channel. Remember the referrer so
    // we can credit it once they tap "✅ I Joined".
    if (referrerId && Number.isFinite(referrerId) && referrerId !== userId) {
      pendingReferrals.set(userId, { referrerId, createdAt: Date.now() });
    }
    return;
  }

  // ── Referral award (channel-already-joined fast path) ─────────────────
  // User is already a channel member, so award the referral right now.
  if (referrerId && Number.isFinite(referrerId)) {
    await processReferralAward(userId, referrerId);
  }

  // ── First-time users (no language set yet) → language picker FIRST.
  // We deliberately do NOT start the free trial or show the trial message
  // here. The trial is created right after the user picks a language in
  // applyLanguageSelection(), so the trial countdown only starts once the
  // user has actually entered the bot and the trial banner appears AFTER
  // the language is set (not bundled with the language picker).
  //
  // No access gate here. Per UX request, /start always shows the user the
  // language picker (and then the menu). The "your free access has ended"
  // / "subscription required" gate is enforced inside each feature handler
  // — so a user without access will see all the buttons, but tapping any
  // feature button will show the gate inside that feature. This avoids
  // the confusing case where a user with an ACTIVE trial saw the
  // access-ended message just because of a stale check on /start.
  if (!hasUserLang(userId)) {
    userStates.delete(userId);
    await sendLanguagePicker(ctx, true);
    return;
  }

  // ── Returning users (language already set).
  // Start their one-and-only 24h free trial if they don't have one yet.
  // The trial entry is permanent in MongoDB (`freeTrials` map keyed by
  // userId) so the same user can never receive a second free trial.
  // Admin is skipped — admin already has unlimited access, the trial UI
  // would be misleading for them.
  let trialJustStarted: { expiresAt: number } | null = null;
  if (!isAdmin(userId)) {
    const trial = await ensureFreeTrial(userId, FREE_TRIAL_MS);
    if (trial.created) trialJustStarted = { expiresAt: trial.expiresAt };
  }

  // No access gate here either. The gate lives inside each feature
  // handler (see hasAccess() / sendReferRequired() call sites). This
  // means /start always shows the menu with all buttons — even for
  // users whose trial has expired. They see the gate only when they
  // actually try to USE a feature.
  userStates.delete(userId);

  // Show live "connecting WhatsApp" progress bar before the menu, so
  // users immediately see the status of their saved WhatsApp session.
  // Skip this for users who have no access — for them WhatsApp is
  // unusable until they renew, so spending time/RAM on a connection
  // attempt is wasteful and could trigger needless reconnects.
  const userHasAccess = isAdmin(userId) || (await hasAccess(userId));
  if (userHasAccess) {
    await showWhatsAppConnectingProgress(ctx, userId);
  }
  if (trialJustStarted) {
    await ctx.reply(trialStartedMessage(trialJustStarted.expiresAt), { parse_mode: "HTML" });
  }
  await ctx.reply(
    mainMenuText(userId, "welcome"),
    { parse_mode: "HTML", reply_markup: mainMenu(userId) }
  );
});

// ─────────────────────────────────────────────────────────────────────────────
// /language command — pick UI language. Shows 5 options:
//   1. Default (current Hindi+English mix, no translation)
//   2. English
//   3. हिन्दी (Hindi)
//   4. Bahasa Indonesia
//   5. 中文 (Chinese)
// The picker UI itself is wrapped in notr() so its text/buttons are never
// translated — language names should always show in their native scripts.
// ─────────────────────────────────────────────────────────────────────────────
function languagePickerKeyboard(): InlineKeyboard {
  const kb = new InlineKeyboard();
  kb.text(notr("🌐 Default (Hindi + English)"), "lang_set_default").row();
  kb.text(notr(`${LANGUAGES.en.flag} ${LANGUAGES.en.nativeName}`), "lang_set_en").row();
  kb.text(notr(`${LANGUAGES.hi.flag} ${LANGUAGES.hi.nativeName}`), "lang_set_hi").row();
  kb.text(notr(`${LANGUAGES.id.flag} ${LANGUAGES.id.nativeName}`), "lang_set_id").row();
  kb.text(notr(`${LANGUAGES.zh.flag} ${LANGUAGES.zh.nativeName}`), "lang_set_zh").row();
  return kb;
}

async function sendLanguagePicker(ctx: any, isFirstRun: boolean): Promise<void> {
  const heading = isFirstRun
    ? "👋 <b>Welcome!</b>\n\n🌐 <b>Choose your language</b> / भाषा चुनें / Pilih bahasa / 选择语言"
    : "🌐 <b>Choose your language</b> / भाषा चुनें / Pilih bahasa / 选择语言";
  const body =
    `${heading}\n\n` +
    `• <b>Default</b> — Hindi + English (current)\n` +
    `• <b>English</b> — full English UI\n` +
    `• <b>हिन्दी</b> — pure Hindi UI\n` +
    `• <b>Bahasa Indonesia</b> — Indonesian UI\n` +
    `• <b>中文</b> — Chinese UI\n\n` +
    `<i>Tip: you can change this anytime with /language</i>`;
  await ctx.reply(notr(body), {
    parse_mode: "HTML",
    reply_markup: languagePickerKeyboard(),
  });
}

bot.command("language", async (ctx) => {
  const userId = ctx.from!.id;
  await trackUser(userId);
  if (await isBanned(userId)) return;
  await sendLanguagePicker(ctx, false);
});

// ─────────────────────────────────────────────────────────────────────────────
// /myaccess — anyone can ask "what's my current access status?".
//
// Shows the user, in plain English, exactly which window is active for
// them, when it expires, and their referral stats + personal link if
// refer mode is on. Admin sees a special "unlimited access" line.
// ─────────────────────────────────────────────────────────────────────────────
bot.command("myaccess", async (ctx) => {
  const userId = ctx.from!.id;
  await trackUser(userId);
  if (await isBanned(userId)) {
    await ctx.reply("🚫 You are banned from using this bot.");
    return;
  }

  const data = await loadBotData();
  const state = await getUserAccessState(userId, ADMIN_USER_ID);
  const stats = await getReferralStats(userId);
  const username = await getBotUsername();
  const link = username && !isAdmin(userId) ? buildReferLink(userId, username) : "";

  let header = "";
  switch (state.kind) {
    case "admin":
      header = `👑 <b>Admin</b> — unlimited access.`;
      break;
    case "admin_grant":
      header =
        `💎 <b>Premium access (granted by admin)</b>\n` +
        `⏰ Time left: <b>${formatRemaining(state.expiresAt!)}</b>\n` +
        `📅 Expires (UTC): ${new Date(state.expiresAt!).toUTCString()}`;
      break;
    case "trial":
      header =
        `🎁 <b>Free 24-hour trial</b>\n` +
        `⏰ Time left: <b>${formatRemaining(state.expiresAt!)}</b>\n` +
        `📅 Ends (UTC): ${new Date(state.expiresAt!).toUTCString()}`;
      break;
    case "referral":
      header =
        `🤝 <b>Referral access</b>\n` +
        `⏰ Time left: <b>${formatRemaining(state.expiresAt!)}</b>\n` +
        `📅 Expires (UTC): ${new Date(state.expiresAt!).toUTCString()}`;
      break;
    case "subscription_open":
      header = `🆓 <b>Free for everyone right now</b> — the bot is open to all users.`;
      break;
    case "none":
      header =
        `🔒 <b>No active access.</b>\n` +
        `Refer a friend (1 referral = 1 day free) or buy premium from ${OWNER_USERNAME}.`;
      break;
  }

  // Referral stats are only meaningful for non-admin users when refer
  // mode is on (or has historical data).
  let referralBlock = "";
  if (!isAdmin(userId) && (data.referMode || stats.totalReferred > 0)) {
    referralBlock =
      `\n\n📊 <b>Your referral stats</b>\n` +
      `👥 People you've referred: <b>${stats.totalReferred}</b>\n` +
      (link ? `🔗 Your referral link:\n<code>${esc(link)}</code>\n` : ``) +
      `<i>Each new person who starts the bot through your link gives you 1 extra day of free access.</i>`;
  }

  const text = `${header}${referralBlock}`;

  // Add a "Share my link" button when we have a link to share.
  const kb = new InlineKeyboard();
  if (link) {
    const shareText = encodeURIComponent(
      `Try this Telegram bot — start through my link to get a 24-hour free trial:`
    );
    kb.url("📤 Share My Referral Link", `https://t.me/share/url?url=${encodeURIComponent(link)}&text=${shareText}`).row();
  }
  if (state.kind === "none" || state.kind === "trial" || state.kind === "referral") {
    kb.url(`💎 Buy Premium (${OWNER_USERNAME})`, `https://t.me/${OWNER_USERNAME.replace(/^@/, "")}`);
  }

  await ctx.reply(text, {
    parse_mode: "HTML",
    reply_markup: kb,
  });
});

async function applyLanguageSelection(ctx: any, lang: Language): Promise<void> {
  const userId = ctx.from.id;
  await ctx.answerCallbackQuery();

  // Was this the user's very first language pick? If yes, this is the
  // moment we kick off their one-and-only 24h free trial — NOT on /start.
  // The trial banner is shown AFTER the menu so it doesn't get bundled
  // with the language picker reply.
  const isFirstPick = !hasUserLang(userId);

  // Persist the choice immediately so all subsequent messages use it.
  await setUserLanguage(userId, lang);

  // Create the trial right after the very first language pick (admin
  // skipped — they have unlimited access).
  let trialJustStarted: { expiresAt: number } | null = null;
  if (isFirstPick && !isAdmin(userId)) {
    try {
      const trial = await ensureFreeTrial(userId, FREE_TRIAL_MS);
      if (trial.created) trialJustStarted = { expiresAt: trial.expiresAt };
    } catch (err: any) {
      console.error(`[TRIAL] ensureFreeTrial after lang pick failed for ${userId}:`, err?.message);
    }
  }

  // For "default" there's nothing to warm up — go straight to the menu.
  if (lang === "default") {
    try {
      await ctx.editMessageText(
        notr("✅ <b>Language set:</b> Default (Hindi + English)\n\nLoading menu..."),
        { parse_mode: "HTML" }
      );
    } catch {}
    await new Promise((r) => setTimeout(r, 300));
    try {
      await ctx.editMessageText(
        mainMenuText(userId, "welcome"),
        { parse_mode: "HTML", reply_markup: mainMenu(userId) }
      );
    } catch {
      await ctx.reply(mainMenuText(userId, "welcome"), {
        parse_mode: "HTML", reply_markup: mainMenu(userId),
      });
    }
    if (trialJustStarted) {
      await ctx.reply(trialStartedMessage(trialJustStarted.expiresAt), { parse_mode: "HTML" });
    }
    return;
  }

  // For non-default langs: show progress bar while we warm up the cache.
  const meta = LANGUAGES[lang as Exclude<Language, "default">];
  const renderBar = (done: number, total: number): string => {
    const pct = total > 0 ? Math.floor((done / total) * 100) : 0;
    const filled = Math.floor(pct / 5);
    const bar = "█".repeat(filled) + "░".repeat(20 - filled);
    return (
      `${meta.flag} <b>Switching to ${meta.nativeName}...</b>\n\n` +
      `<code>[${bar}] ${pct}%</code>\n` +
      `${done}/${total} translated`
    );
  };

  let lastEditAt = 0;
  try {
    await ctx.editMessageText(notr(renderBar(0, 1)), { parse_mode: "HTML" });
  } catch {}

  await warmUpLanguage(lang, async (done, total) => {
    // Throttle Telegram edits to avoid 429s; update every ~700ms or on completion.
    const now = Date.now();
    if (now - lastEditAt < 700 && done < total) return;
    lastEditAt = now;
    try {
      await ctx.editMessageText(notr(renderBar(done, total)), { parse_mode: "HTML" });
    } catch {}
  });

  // Done — show the main menu in the new language. The transformer auto-translates.
  try {
    await ctx.editMessageText(
      mainMenuText(userId, "welcome"),
      { parse_mode: "HTML", reply_markup: mainMenu(userId) }
    );
  } catch {
    await ctx.reply(mainMenuText(userId, "welcome"), {
      parse_mode: "HTML", reply_markup: mainMenu(userId),
    });
  }
  if (trialJustStarted) {
    await ctx.reply(trialStartedMessage(trialJustStarted.expiresAt), { parse_mode: "HTML" });
  }
}

bot.callbackQuery("lang_set_default", (ctx) => applyLanguageSelection(ctx, "default"));
bot.callbackQuery("lang_set_en", (ctx) => applyLanguageSelection(ctx, "en"));
bot.callbackQuery("lang_set_hi", (ctx) => applyLanguageSelection(ctx, "hi"));
bot.callbackQuery("lang_set_id", (ctx) => applyLanguageSelection(ctx, "id"));
bot.callbackQuery("lang_set_zh", (ctx) => applyLanguageSelection(ctx, "zh"));

bot.command("help", async (ctx) => {
  const userId = ctx.from!.id;
  await trackUser(userId);
  if (await isBanned(userId)) return;

  const codeBlock =
    `🤖 WhatsApp Bot Manager — Help Guide\n\n` +

    `━━━━━━━━━━━━━━━━━━\n\n` +
    `📌 All Features:\n\n` +

    `📱 1. Connect WhatsApp\n` +
    `• Bot se apna WhatsApp link karo\n` +
    `• Phone number do → Pairing code milega (koi bhi format chalega, jaise +91 9999-999999)\n` +
    `• WhatsApp → Linked Devices → Link with phone number → code daalo\n` +
    `• Ek baar connect hone ke baad sab features use karo\n\n` +

    `🏗️ 2. Create Groups\n` +
    `• Ek saath kaafi saare WhatsApp groups banao\n` +
    `• Custom ya auto-numbered names (e.g. Group 1, Group 2...)\n` +
    `• Group description set kar sakte ho\n` +
    `• 🖼️ Multiple Group DPs (max 50): 1 DP do to sab groups mein same lagega.\n` +
    `  Multiple DPs do to 1st DP→1st group, 2nd DP→2nd group...\n` +
    `  Groups DPs se zyada hue to DPs rotate ho jayenge.\n` +
    `• Permissions: kaun message, kaun add kar sakta hai, approval mode\n` +
    `• ⏳ Disappearing Messages: 24 Hours / 7 Days / 90 Days / Off\n` +
    `• 👫 Friends Add: Group bante waqt seedha friends ko add karo\n` +
    `  (koi bhi number format — +919999999999, +91 9999-999999, 919999999999)\n` +
    `• Live progress dikhta hai jaise groups bante hain\n\n` +

    `🔗 3. Get Group Links\n` +
    `• Apne sabhi WhatsApp groups ke invite links lo\n` +
    `• Sabhi ya similar name ke groups filter karke\n` +
    `• Links copy karke kahin bhi paste kar sakte ho\n\n` +

    `🔗 4. Join Groups\n` +
    `• Multiple invite links paste karo\n` +
    `• Bot automatically sabhi groups join kar leta hai\n` +
    `• Live progress dikhta hai\n\n` +

    `🚪 5. Leave Groups\n` +
    `• Sirf member wale, sirf admin wale, ya sabhi ek saath\n` +
    `• Similar name wale groups batch mein leave\n\n` +

    `📊 6. CTC Checker\n` +
    `• Group links do → VCF files do → bot check karta hai:\n` +
    `  ✅ Pehle se group mein hai\n` +
    `  ⏳ Pending approval mein hai\n` +
    `  ❌ Group mein nahi mila\n` +
    `  ⚠️ Wrong add — group mein hai par VCF mein nahi\n` +
    `  🔁 Duplicate pending — ek contact multiple groups mein\n` +
    `• Multiple VCF files ek saath bhej sakte ho\n\n` +

    `🗑️ 7. Remove Members\n` +
    `• Ek ya zyada groups select karo\n` +
    `• Optionally kuch numbers exclude karo\n` +
    `• Baki sabhi non-admin members remove ho jayenge\n\n` +

    `👑 8. Make Admin\n` +
    `• Admin groups select karo\n` +
    `• Phone numbers bhejo\n` +
    `• Bot dhundhke unhe admin promote kar dega\n\n` +

    `✅ 9. Approval\n` +
    `• Admin groups select karo → pending members approve karo:\n` +
    `  ☝️ 1 by 1: Har pending member individually approve\n` +
    `  👥 Together: Approval OFF phir ON — sabhi ek saath approve\n` +
    `• Similar name wale groups ek saath select kar sakte ho\n\n` +

    `📋 10. Get Pending List\n` +
    `• Sabhi admin groups ka pending members count dikhata hai\n` +
    `• Similar name wale groups grouped dikhate hain\n` +
    `• Pata chal jata hai kaun se group mein kitne log pending\n\n` +

    `➕ 11. Add Members\n` +
    `• Single group: Link do → Friend numbers + Admin/Navy/Member VCF do\n` +
    `• Multiple groups: Ek se zyada links ek per line do → sirf Friend numbers bhejo\n` +
    `  → Sabhi groups mein ek saath add ho jayenge\n` +
    `• 3 modes:\n` +
    `   👆 Add 1 by 1 (safe, with delay)\n` +
    `   👥 Add Together (fast, ek baar mein)\n` +
    `   🎯 Custom — har category ke liye apni pace (1-1, 2-2, 3-3, 4-4, 5-5, 6-6, 7-7, 8-8, 9-9, 10-10, 15-15, 20-20 ya All)\n` +
    `• Sirf wahi categories show hoti hain jinka VCF ya numbers diya ho\n` +
    `  (e.g. Admin VCF nahi diya to Admin option nahi dikhega)\n` +
    `• Fail hone par specific reason dikhta hai:\n` +
    `   • Privacy block / invite required\n` +
    `   • Number not on WhatsApp\n` +
    `   • Already in group / Recently left\n` +
    `   • Rate limit hit\n` +
    `   • WhatsApp ban / restricted\n` +
    `   • Group/account limit reached\n` +
    `• Live progress dikhta hai, beech mein cancel kar sakte ho\n\n` +

    `⚙️ 12. Edit Settings\n` +
    `• Admin groups scan hote hain → Similar Groups ya All Groups choose karo\n` +
    `• Multiple groups ek saath select karo (pagination + Select All)\n` +
    `• Permissions toggle karo (message, add members, approval mode)\n` +
    `• ⏳ Disappearing Messages set karo: 24h / 7 Days / 90 Days / Off\n` +
    `• Group DP change karo ya skip karo\n` +
    `• Description update karo ya skip karo\n` +
    `• Review karke Apply — har group ka live progress dikhega\n` +
    `• Beech mein cancel bhi kar sakte ho\n\n` +

    `🏷️ 13. Change Group Name\n` +
    `• Rename multiple groups in one go. Two modes:\n` +
    `  ✏️ Manual (by name):\n` +
    `   • Pick Similar Groups or All Groups (like Get Link)\n` +
    `   • Tap groups to select — buttons show 1, 2, 3… in tap order\n` +
    `   • Choose Auto-numbered (e.g. "Spidy 1, Spidy 2…") or Custom Names (one per line)\n` +
    `   • Review and confirm — bot renames in your tap order with live progress + Cancel\n` +
    `  📁 Auto (VCF + name):\n` +
    `   • Only groups with pending requests are shown (like Pending List)\n` +
    `   • Select groups, then upload one VCF file per selected group (any order)\n` +
    `   • Bot matches each VCF to a group by checking pending phone numbers\n` +
    `   • Choose name source:\n` +
    `      ◦ Same as VCF name → group name = VCF filename without .vcf\n` +
    `        (e.g. "SPIDY 酒店回饋活動FL_61.vcf" → "SPIDY 酒店回饋活動FL_61")\n` +
    `      ◦ Customize name → you give a prefix template; bot keeps the trailing number from the VCF\n` +
    `        (e.g. prefix "SPIDY 酒店EMPIRE動FL_" + VCF "..._61.vcf" → "SPIDY 酒店EMPIRE動FL_61")\n` +
    `   • Review and confirm — live progress + Cancel\n\n` +

    (canUserSeeAutoChat(userId) ?
    `🤖 14. Auto Chat  ⭐ Paid Service\n` +
    `• Auto Chat ke liye 2nd WhatsApp connect karo\n` +
    `• Chat Friend: funny/study messages auto send hote rahenge jab tak Stop na dabao\n` +
    `• Chat In Group: selected common groups mein funny/study messages rotate hote rahenge\n` +
    `• Messages fast-fast nahi jaate; random delay rotation use hota hai\n` +
    `• Delay rotation: 10 sec, 1 min, 10 min, 20 min, 30 min, 1 hour, 2 hours\n` +
    `• Live status, sent/failed count, refresh aur stop controls milte hain\n\n`
    :
    `🤖 14. Auto Chat  ⭐ Paid Service\n` +
    `• Automatically send messages to friends or groups on WhatsApp\n` +
    `• Random delay rotation keeps it natural and safe\n` +
    `• To buy Auto Chat access, message ${OWNER_USERNAME} on Telegram\n\n`) +

    `🛡️ 15. Auto Request Accepter\n` +
    `• Automatically accept pending join requests in selected groups\n` +
    `• Only accepts users who joined via invite link (NOT direct admin-adds)\n` +
    `• How to use:\n` +
    `   1. Tap "Auto Accepter" in main menu\n` +
    `   2. Select groups — choose Similar Groups or All Groups\n` +
    `   3. Pick duration: 15 min, 30 min, 1 hr, or 2 hrs\n` +
    `   4. Review selected groups and confirm to start\n` +
    `   5. Bot will poll every 30 seconds and auto-accept invite-link joiners\n` +
    `   6. Tap "Cancel" button to stop early at any time\n` +
    `• When the timer ends, you get a notification\n` +
    `• Group must have "Approval required" mode ON\n` +
    `• You must be admin in the group\n\n` +

    `━━━━━━━━━━━━━━━━━━\n\n` +
    `💬 Commands:\n` +
    `/start — Bot start karo & main menu dekho\n` +
    `/help  — Yeh help message dekho\n\n` +

    `━━━━━━━━━━━━━━━━━━\n\n` +
    `⚠️ Important Notes:\n` +
    `• CTC Pending ke liye aap group admin hone chahiye\n` +
    `• Group mein "Approval required" mode ON hona chahiye\n` +
    `• 1 by 1 Approval ke liye bhi admin hona zaroori hai\n` +
    `• Connect WhatsApp mein number kisi bhi format mein de sakte ho\n` +
    `  (+91 9999-999999, +919999999999 — sab chalega)\n` +
    `• 🔌 Agar aapka WhatsApp disconnect ho jaye to aapko ek alert message milega\n` +
    `  (English mein, aapke WhatsApp number ke saath)`;

  // Telegram has a 4096-character limit per message. The full help guide
  // exceeds that when wrapped in <pre>, so we split it into chunks on
  // section boundaries (double newlines) and show one page at a time
  // with Next / Previous buttons. Content stays in <pre> (copy-code) format.
  const chunks = splitHelpIntoChunks(codeBlock);
  helpPages.set(userId, chunks);
  helpPagesLastTouched.set(userId, Date.now());
  await ctx.reply(renderHelpPage(chunks, 0), {
    parse_mode: "HTML",
    reply_markup: buildHelpKeyboard(0, chunks.length),
  });
});

// ─── Help pagination ──────────────────────────────────────────────────────────
const helpPages: Map<number, string[]> = new Map();
const HELP_MAX_CHUNK = 3500; // safe budget under Telegram's 4096-char limit

function splitHelpIntoChunks(codeBlock: string): string[] {
  const sections = codeBlock.split("\n\n");
  const chunks: string[] = [];
  let current = "";
  for (const sec of sections) {
    const piece = current ? current + "\n\n" + sec : sec;
    if (piece.length > HELP_MAX_CHUNK && current) {
      chunks.push(current);
      current = sec;
    } else {
      current = piece;
    }
  }
  if (current) chunks.push(current);
  return chunks;
}

function renderHelpPage(chunks: string[], page: number): string {
  const ownerLine = `👤 <b>Owner:</b> ${OWNER_USERNAME}`;
  const pageInfo = `📄 <b>Page ${page + 1} / ${chunks.length}</b>`;
  return `${ownerLine}\n${pageInfo}\n\n<pre>${chunks[page]}</pre>`;
}

function buildHelpKeyboard(page: number, total: number): InlineKeyboard {
  const kb = new InlineKeyboard();
  if (total > 1) {
    if (page > 0) kb.text("⬅️ Previous", `help_pg_${page - 1}`);
    if (page < total - 1) kb.text("Next ➡️", `help_pg_${page + 1}`);
    kb.row();
  }
  kb.text("🏠 Main Menu", "main_menu");
  return kb;
}

bot.callbackQuery(/^help_pg_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from!.id;
  const page = Number(ctx.match![1]);
  const chunks = helpPages.get(userId);
  if (!chunks || page < 0 || page >= chunks.length) {
    try { await ctx.answerCallbackQuery({ text: "Help session expired. Send /help again.", show_alert: true }); } catch {}
    return;
  }
  helpPagesLastTouched.set(userId, Date.now());
  try {
    await ctx.editMessageText(renderHelpPage(chunks, page), {
      parse_mode: "HTML",
      reply_markup: buildHelpKeyboard(page, chunks.length),
    });
  } catch {}
});

async function checkAccessMiddleware(ctx: any): Promise<boolean> {
  const userId = ctx.from?.id;
  if (!userId) return false;
  if (await isBanned(userId)) {
    try { await ctx.answerCallbackQuery({ text: "🚫 You are banned from this bot.", show_alert: true }); } catch {
      await ctx.reply("🚫 You are banned from using this bot.");
    }
    return false;
  }
  if (!(await checkForceSub(ctx))) return false;
  if (!(await hasAccess(userId))) {
    try {
      await ctx.answerCallbackQuery({
        text: `🔒 Subscription required! Contact ${OWNER_USERNAME}`,
        show_alert: true,
      });
    } catch {
      await ctx.reply(`🔒 <b>Subscription Required!</b>\n\nContact owner: ${OWNER_USERNAME}`, { parse_mode: "HTML" });
    }
    return false;
  }
  // Lazy-restore WhatsApp session if user has stored creds but the live
  // socket was evicted (idle/memory pressure). No-op (<1ms) if already loaded.
  // ~5s on cold restore but only happens once per eviction cycle.
  try {
    await ensureSessionLoaded(String(userId));
  } catch (err: any) {
    console.warn(`[WA][LAZY-RESTORE][${userId}] failed:`, err?.message);
  }
  return true;
}

bot.callbackQuery("main_menu", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  userStates.delete(userId);
  await ctx.editMessageText(
    mainMenuText(userId, "menu"),
    { parse_mode: "HTML", reply_markup: mainMenu(userId) }
  );
});

// ─── Get Pending List ────────────────────────────────────────────────────────

bot.callbackQuery("pending_list", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ WhatsApp not connected.", {
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    });
    return;
  }
  await ctx.editMessageText("⏳ <b>Fetching pending requests for all admin groups...</b>\n\nPlease wait...", { parse_mode: "HTML" });

  const list = await getGroupPendingList(String(userId));

  const pendingOnly = list.filter((g) => g.pendingCount > 0);

  if (!pendingOnly.length) {
    await ctx.editMessageText(
      "📋 <b>Pending List</b>\n\nNo pending requests found in any group.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
    return;
  }

  // Natural numeric sort: SK 1, SK 2, SK 3 ... SK 14, SK 15 (not SK 1, SK 10, SK 11, SK 2)
  pendingOnly.sort((a, b) => a.groupName.localeCompare(b.groupName, undefined, { numeric: true, sensitivity: "base" }));

  // Detect similar patterns from admin group names
  const groupsForPattern = pendingOnly.map((g) => ({ id: g.groupId, subject: g.groupName }));
  const patterns = detectSimilarGroups(groupsForPattern);

  userStates.set(userId, {
    step: "pending_list_select",
    pendingListData: { patterns, allPending: pendingOnly, selectedIndices: new Set(), page: 0 },
  });

  await ctx.editMessageText(
    `📋 <b>Pending List</b>\n\n` +
    `📊 Groups with pending: ${pendingOnly.length}\n` +
    `⏳ Total Pending: ${pendingOnly.reduce((s, g) => s + g.pendingCount, 0)}\n` +
    (patterns.length > 0 ? `🔍 Similar Patterns: ${patterns.length}\n` : "") +
    `\n📌 Select groups to show copy-format pending list:`,
    { parse_mode: "HTML", reply_markup: buildPendingListKeyboard(userStates.get(userId)!) }
  );
});

bot.callbackQuery(/^pl_tog_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.pendingListData?.selectedIndices) return;
  const idx = parseInt(ctx.match![1]);
  if (idx < 0 || idx >= state.pendingListData.allPending.length) return;
  if (state.pendingListData.selectedIndices.has(idx)) state.pendingListData.selectedIndices.delete(idx);
  else state.pendingListData.selectedIndices.add(idx);
  await ctx.editMessageText(
    `📋 <b>Pending List</b>\n\n📊 Groups with pending: ${state.pendingListData.allPending.length}\n\n📌 Select groups to show copy-format pending list:\n<i>${state.pendingListData.selectedIndices.size || "None"} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildPendingListKeyboard(state) }
  );
});

bot.callbackQuery("pl_prev_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.pendingListData) return;
  if ((state.pendingListData.page || 0) > 0) state.pendingListData.page = (state.pendingListData.page || 0) - 1;
  await ctx.editMessageText(
    `📋 <b>Pending List</b>\n\n📊 Groups with pending: ${state.pendingListData.allPending.length}\n\n📌 Select groups to show copy-format pending list:\n<i>${state.pendingListData.selectedIndices?.size || "None"} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildPendingListKeyboard(state) }
  );
});

bot.callbackQuery("pl_next_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.pendingListData) return;
  const totalPages = Math.ceil(state.pendingListData.allPending.length / PL_PAGE_SIZE);
  if ((state.pendingListData.page || 0) < totalPages - 1) state.pendingListData.page = (state.pendingListData.page || 0) + 1;
  await ctx.editMessageText(
    `📋 <b>Pending List</b>\n\n📊 Groups with pending: ${state.pendingListData.allPending.length}\n\n📌 Select groups to show copy-format pending list:\n<i>${state.pendingListData.selectedIndices?.size || "None"} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildPendingListKeyboard(state) }
  );
});

bot.callbackQuery("pl_page_info", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" });
});

bot.callbackQuery("pl_select_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.pendingListData?.selectedIndices) return;
  for (let i = 0; i < state.pendingListData.allPending.length; i++) state.pendingListData.selectedIndices.add(i);
  await ctx.editMessageText(
    `📋 <b>Pending List</b>\n\n✅ All <b>${state.pendingListData.allPending.length}</b> groups selected.`,
    { parse_mode: "HTML", reply_markup: buildPendingListKeyboard(state) }
  );
});

bot.callbackQuery("pl_clear_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.pendingListData?.selectedIndices) return;
  state.pendingListData.selectedIndices.clear();
  await ctx.editMessageText(
    `📋 <b>Pending List</b>\n\n📊 Groups with pending: ${state.pendingListData.allPending.length}\n\n📌 Select groups to show copy-format pending list:\n<i>None selected</i>`,
    { parse_mode: "HTML", reply_markup: buildPendingListKeyboard(state) }
  );
});

bot.callbackQuery("pl_proceed", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.pendingListData?.selectedIndices || state.pendingListData.selectedIndices.size === 0) return;
  const selected = Array.from(state.pendingListData.selectedIndices)
    .sort((a, b) => a - b)
    .map((i) => state.pendingListData!.allPending[i])
    .filter(Boolean);
  const text = pendingCopyText("Selected Groups — Pending List", selected);
  const chunks = splitMessage(text, 4000);
  const kb = new InlineKeyboard().text("🔙 Back", "pending_list").text("🏠 Menu", "main_menu");
  try {
    await ctx.editMessageText(chunks[0], { parse_mode: "HTML", reply_markup: chunks.length === 1 ? kb : undefined });
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(ctx.chat!.id, chunks[i], {
      parse_mode: "HTML",
      reply_markup: i === chunks.length - 1 ? kb : undefined,
    });
  }
});

bot.callbackQuery("pl_similar", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.pendingListData) return;

  const { patterns } = state.pendingListData;
  if (!patterns.length) {
    await ctx.editMessageText("⚠️ No similar group patterns found.", {
      reply_markup: new InlineKeyboard().text("🔙 Back", "pending_list").text("🏠 Menu", "main_menu"),
    }); return;
  }

  const kb = new InlineKeyboard();
  for (let i = 0; i < patterns.length; i++) {
    const p = patterns[i];
    const totalPending = p.groups.reduce((s, g) => {
      const found = state.pendingListData!.allPending.find((ap) => ap.groupId === g.id);
      return s + (found?.pendingCount || 0);
    }, 0);
    kb.text(`📌 ${p.base} (${p.groups.length} groups) ⏳${totalPending}`, `pl_sim_${i}`).row();
  }
  kb.text("🔙 Back", "pending_list").text("🏠 Menu", "main_menu");

  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to see pending count:",
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery(/^pl_sim_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.pendingListData) return;

  const idx = parseInt(ctx.match![1]);
  const pattern = state.pendingListData.patterns[idx];
  if (!pattern) return;

  const selectedItems: Array<{ groupName: string; pendingCount: number }> = [];
  for (const g of pattern.groups) {
    const found = state.pendingListData.allPending.find((ap) => ap.groupId === g.id);
    const count = found?.pendingCount ?? 0;
    if (count > 0) selectedItems.push({ groupName: g.subject, pendingCount: count });
  }
  const text = pendingCopyText(`"${pattern.base}" — Pending List`, selectedItems);

  const chunks = splitMessage(text, 4000);
  const backKb = new InlineKeyboard().text("🔙 Back", "pl_similar").text("🏠 Menu", "main_menu");
  try {
    await ctx.editMessageText(chunks[0], { parse_mode: "HTML", reply_markup: chunks.length === 1 ? backKb : undefined });
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(ctx.chat!.id, chunks[i], {
      parse_mode: "HTML",
      reply_markup: i === chunks.length - 1 ? backKb : undefined,
    });
  }
});

bot.callbackQuery("pl_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.pendingListData) return;

  const { allPending } = state.pendingListData;
  const text = pendingCopyText("All Groups — Pending List", allPending);

  const chunks = splitMessage(text, 4000);
  const backKb = new InlineKeyboard().text("🔙 Back", "pending_list").text("🏠 Menu", "main_menu");
  try {
    await ctx.editMessageText(chunks[0], { parse_mode: "HTML", reply_markup: chunks.length === 1 ? backKb : undefined });
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(ctx.chat!.id, chunks[i], {
      parse_mode: "HTML",
      reply_markup: i === chunks.length - 1 ? backKb : undefined,
    });
  }
});

// ─── Admin Commands ──────────────────────────────────────────────────────────

bot.command("admin", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  await ctx.reply(
    "🛡️ <b>Admin Panel</b>\n\n" +
    "📋 <b>Commands:</b>\n\n" +
    "🟢 <code>/access on</code> — Enable subscription mode\n" +
    "🔴 <code>/access off</code> — Disable subscription mode\n" +
    "✅ <code>/access [id] [days]</code> — Give user access\n" +
    "❌ <code>/revoke [id]</code> — Revoke user access\n" +
    "🚫 <code>/ban [id]</code> — Ban a user\n" +
    "✅ <code>/unban [id]</code> — Unban a user\n" +
    "📢 <code>/broadcast [message]</code> — Send message to all users\n" +
    "📊 <code>/status</code> — View bot statistics\n" +
    "📱 <code>/sessions</code> — WhatsApp sessions list\n" +
    "🧠 <code>/memory</code> — Server RAM usage\n" +
    "🧽 <code>/cleanram</code> — Force-clear all caches and free RAM now\n" +
    "🧹 <code>/cleansessions [num]</code> — Delete session by number\n\n" +
    "🎁 <b>Refer Mode:</b>\n" +
    "🟢 <code>/refermode on</code> — Enable refer mode (24h trial + referrals)\n" +
    "🔴 <code>/refermode off</code> — Disable refer mode (back to normal)\n\n" +
    "🤖 <b>Auto Chat Controls:</b>\n" +
    "🟢 <code>/autochat on</code> — Auto Chat sabhi users ke liye ON\n" +
    "🔴 <code>/autochat off</code> — Auto Chat sabhi users ke liye OFF\n" +
    "✅ <code>/accessautochat [id]</code> — Specific user ke liye Auto Chat ON\n" +
    "❌ <code>/revokeautochat [id]</code> — Specific user ka Auto Chat OFF",

    { parse_mode: "HTML" }
  );
});

// ─── /refermode on|off ──────────────────────────────────────────────────────
// Enables / disables the referral system globally. When ON:
//   • Every new user gets a 24-hour free trial (all features except Auto
//     Chat, which still follows /autochat + /accessautochat).
//   • When the trial ends, the user is shown their personal refer link and
//     is told they can earn 1 day per referred friend, or buy premium.
//   • Admin-granted users (/access [id] [days]) are exempt from referral
//     requirements.
// When OFF, the bot reverts to the original behaviour — every user can use
// every feature for free (subject to existing /access subscription mode if
// admin enabled it separately).
bot.command("refermode", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const arg = (ctx.message?.text || "").split(/\s+/)[1]?.toLowerCase();
  if (arg !== "on" && arg !== "off") {
    const data = await loadBotData();
    await ctx.reply(
      `🎁 <b>Refer Mode: ${data.referMode ? "ON 🟢" : "OFF 🔴"}</b>\n\n` +
      `❓ <b>Usage:</b>\n` +
      `<code>/refermode on</code> — Enable 24h trial + referral system\n` +
      `<code>/refermode off</code> — Disable referrals (free for all again)\n\n` +
      `<b>How it works when ON:</b>\n` +
      `• New users get a 24-hour free trial after joining the channel\n` +
      `• Auto Chat is still admin-controlled (unchanged)\n` +
      `• When trial ends, users must refer friends (1 referral = 1 day) or buy premium from ${OWNER_USERNAME}\n` +
      `• Each user can only be referred once (stored in MongoDB)\n` +
      `• Users you grant access to with <code>/access [id] [days]</code> do NOT need to refer`,
      { parse_mode: "HTML" }
    );
    return;
  }
  await setReferMode(arg === "on");
  if (arg === "on") {
    await ctx.reply(
      `🎁 <b>Refer Mode: ON 🟢</b>\n\n` +
      `✅ New users will now get a 24-hour free trial (all features except Auto Chat).\n` +
      `✅ When the trial ends, users will be asked to refer friends (1 referral = 1 day) or buy premium from ${OWNER_USERNAME}.\n\n` +
      `💡 Users you grant access to with <code>/access [id] [days]</code> are exempt from referral requirements.`,
      { parse_mode: "HTML" }
    );
  } else {
    await ctx.reply(
      `🎁 <b>Refer Mode: OFF 🔴</b>\n\n` +
      `✅ Referral system disabled. Bot behaves like before — all users can use every feature for free (subject to <code>/access on</code> subscription mode if enabled).\n\n` +
      `📦 Existing trial / referral records are kept in the database; if you turn refer mode back on, leftover days will still count.`,
      { parse_mode: "HTML" }
    );
  }
});

bot.command("autochat", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const arg = (ctx.message?.text || "").split(/\s+/)[1]?.toLowerCase();
  if (arg !== "on" && arg !== "off") {
    await ctx.reply("❓ Usage:\n<code>/autochat on</code> — Sabhi users ke liye ON\n<code>/autochat off</code> — Sabhi users ke liye OFF", { parse_mode: "HTML" });
    return;
  }
  const data = await loadBotData();
  data.autoChatEnabled = arg === "on";
  await saveBotData(data);
  autoChatGlobalEnabled = data.autoChatEnabled;
  await ctx.reply(
    arg === "on"
      ? "✅ <b>Auto Chat: ON</b>\n\n🤖 Sabhi users ko Auto Chat button dikhega." 
      : "🔴 <b>Auto Chat: OFF</b>\n\n🚫 Kisi bhi user ko Auto Chat button nahi dikhega.\n💡 Specific user ke liye: <code>/accessautochat [user_id]</code>",
    { parse_mode: "HTML" }
  );
});

bot.command("accessautochat", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const id = parseInt((ctx.message?.text || "").split(/\s+/)[1]);
  if (isNaN(id)) { await ctx.reply("❓ Usage: <code>/accessautochat [user_id]</code>", { parse_mode: "HTML" }); return; }
  const data = await loadBotData();
  if (!data.autoChatAccessList.includes(id)) {
    data.autoChatAccessList.push(id);
    await saveBotData(data);
    autoChatAccessSet.add(id);
  }
  await ctx.reply(`✅ <b>Auto Chat Access Granted!</b>\n\n👤 User: <code>${id}</code>\n🤖 Is user ko ab Auto Chat button dikhega (chahe global OFF ho).`, { parse_mode: "HTML" });
});

bot.command("revokeautochat", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const id = parseInt((ctx.message?.text || "").split(/\s+/)[1]);
  if (isNaN(id)) { await ctx.reply("❓ Usage: <code>/revokeautochat [user_id]</code>", { parse_mode: "HTML" }); return; }
  const data = await loadBotData();
  data.autoChatAccessList = data.autoChatAccessList.filter((u) => u !== id);
  await saveBotData(data);
  autoChatAccessSet.delete(id);
  await ctx.reply(`❌ <b>Auto Chat Access Revoked!</b>\n\n👤 User: <code>${id}</code>\n🚫 Is user ko ab Auto Chat button nahi dikhega.`, { parse_mode: "HTML" });
});

bot.command("access", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const args = (ctx.message?.text || "").split(/\s+/).slice(1);
  if (!args.length) { await ctx.reply("❓ Usage:\n/access on\n/access off\n/access [user_id] [days]"); return; }

  if (args[0] === "on") {
    const data = await loadBotData(); data.subscriptionMode = true; await saveBotData(data);
    await ctx.reply(`🔒 <b>Subscription Mode: ON</b>\n\nOnly users with access can use the bot.\n👤 Owner: <b>${OWNER_USERNAME}</b>`, { parse_mode: "HTML" });
    return;
  }
  if (args[0] === "off") {
    const data = await loadBotData(); data.subscriptionMode = false; await saveBotData(data);
    await ctx.reply("🔓 <b>Subscription Mode: OFF</b>\n\nAll users can use the bot for free.", { parse_mode: "HTML" });
    return;
  }
  if (args.length >= 2) {
    const targetId = parseInt(args[0]), days = parseInt(args[1]);
    if (isNaN(targetId) || isNaN(days) || days <= 0) { await ctx.reply("❓ Example: /access 123456789 30"); return; }
    const data = await loadBotData();
    data.accessList[String(targetId)] = { expiresAt: Date.now() + days * 86400000, grantedBy: ctx.from!.id };
    await saveBotData(data);
    const exp = new Date(data.accessList[String(targetId)].expiresAt).toUTCString();
    await ctx.reply(`✅ <b>Access Granted!</b>\n\n👤 User: <code>${targetId}</code>\n📅 Days: ${days}\n⏰ Expires: ${exp}`, { parse_mode: "HTML" });

    // Notify the user that admin has granted them access. Lists every
    // feature that's unlocked so they know exactly what they got. Auto
    // Chat is mentioned conditionally, depending on whether the user
    // already has Auto Chat permission via /accessautochat (or global
    // /autochat on).
    const autoChatOn = data.autoChatEnabled === true
      || (Array.isArray(data.autoChatAccessList) && data.autoChatAccessList.includes(targetId));
    const features = [
      "• ✅ Create Groups",
      "• ✅ Join Groups",
      "• ✅ CTC (Number) Checker",
      "• ✅ Get Group Link",
      "• ✅ Leave Group",
      "• ✅ Remove Members",
      "• ✅ Make Admin",
      "• ✅ Pending Approvals",
      "• ✅ Pending Members List",
      "• ✅ Add Members",
      "• ✅ Edit Group Settings",
      autoChatOn
        ? "• ✅ Auto Chat (already enabled for you)"
        : "• ❌ Auto Chat (admin permission required separately — contact owner)",
    ].join("\n");
    bot.api.sendMessage(
      targetId,
      `🎉 <b>Premium Access Granted!</b>\n\n` +
      `Admin has unlocked premium access on your account.\n\n` +
      `📅 <b>Duration:</b> ${days} day${days === 1 ? "" : "s"}\n` +
      `⏰ <b>Expires (UTC):</b> ${exp}\n\n` +
      `🔓 <b>Features unlocked:</b>\n${features}\n\n` +
      `💡 You don't need to refer anyone — refer mode does not apply to you while this access is active.\n\n` +
      `Send /start to open the menu.`,
      { parse_mode: "HTML" }
    ).catch((err: any) => {
      console.error(`[ACCESS] Failed to notify user ${targetId}:`, err?.message);
    });
    return;
  }
  await ctx.reply("❓ Usage:\n/access on\n/access off\n/access [user_id] [days]");
});

bot.command("revoke", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const id = parseInt((ctx.message?.text || "").split(/\s+/)[1]);
  if (isNaN(id)) { await ctx.reply("❓ Usage: /revoke [user_id]"); return; }
  const data = await loadBotData();
  if (data.accessList[String(id)]) { delete data.accessList[String(id)]; await saveBotData(data); await ctx.reply(`❌ <b>Access Revoked!</b>\n\n👤 User: <code>${id}</code>`, { parse_mode: "HTML" }); }
  else await ctx.reply("⚠️ User does not have access.");
});

bot.command("ban", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const id = parseInt((ctx.message?.text || "").split(/\s+/)[1]);
  if (isNaN(id)) { await ctx.reply("❓ Usage: /ban [user_id]"); return; }
  const data = await loadBotData();
  if (!data.bannedUsers.includes(id)) { data.bannedUsers.push(id); await saveBotData(data); }
  await ctx.reply(`🚫 <b>User Banned!</b>\n\n👤 User: <code>${id}</code>`, { parse_mode: "HTML" });
});

bot.command("unban", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const id = parseInt((ctx.message?.text || "").split(/\s+/)[1]);
  if (isNaN(id)) { await ctx.reply("❓ Usage: /unban [user_id]"); return; }
  const data = await loadBotData();
  data.bannedUsers = data.bannedUsers.filter((u) => u !== id);
  await saveBotData(data);
  await ctx.reply(`✅ <b>User Unbanned!</b>\n\n👤 User: <code>${id}</code>`, { parse_mode: "HTML" });
});

bot.command("broadcast", async (ctx) => {
  const adminId = ctx.from!.id;
  if (!isAdmin(adminId)) { await ctx.reply("🚫 You are not an admin."); return; }

  const rawText = ctx.message?.text || "";
  const message = rawText.replace(/^\/broadcast(?:@\w+)?\s*/i, "").trim();
  if (!message) {
    await ctx.reply(
      "❓ <b>Usage:</b>\n<code>/broadcast Hello guys</code>\n\nSend a message after /broadcast to deliver it to all users.",
      { parse_mode: "HTML" }
    );
    return;
  }

  const data = await loadBotData();
  const users = [...new Set(data.totalUsers.filter((id) => Number.isFinite(id) && id > 0))];
  if (!users.length) {
    await ctx.reply("⚠️ No users found for broadcast.");
    return;
  }

  userStates.set(adminId, { step: "broadcast_confirm", broadcastData: { message, users } });
  const preview = esc(message.length > 1000 ? `${message.slice(0, 1000)}...` : message);
  const kb = new InlineKeyboard()
    .text("✅ Confirm Broadcast", "broadcast_confirm")
    .text("❌ Cancel", "broadcast_cancel");

  await ctx.reply(
    "📢 <b>Broadcast Confirmation</b>\n\n" +
    `👥 <b>Total Users:</b> ${users.length}\n\n` +
    "<b>Message Preview:</b>\n" +
    `<blockquote>${preview}</blockquote>\n\n` +
    "Do you want to send this message to all users?",
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.command("status", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const data = await loadBotData();
  const now = Date.now();
  let accessText = "";
  for (const [uid, info] of Object.entries(data.accessList)) {
    const rem = info.expiresAt - now;
    const dLeft = Math.ceil(rem / 86400000);
    accessText += rem > 0 ? `  ✅ <code>${uid}</code> — ${dLeft} days\n` : `  ⚠️ <code>${uid}</code> — EXPIRED\n`;
  }
  const bannedText = data.bannedUsers.length ? data.bannedUsers.map((id) => `  🚫 <code>${id}</code>`).join("\n") + "\n" : "  None\n";

  const autoChatEnabled = data.autoChatEnabled ?? true;
  const autoChatAccessList = data.autoChatAccessList ?? [];
  let autoChatAccessText = autoChatAccessList.length
    ? autoChatAccessList.map((id) => `  🤖 <code>${id}</code>`).join("\n") + "\n"
    : "  None\n";

  await ctx.reply(
    "📊 <b>Bot Status</b>\n\n" +
    `🔒 <b>Subscription Mode:</b> ${data.subscriptionMode ? "ON 🟢" : "OFF 🔴"}\n` +
    `🤖 <b>Auto Chat:</b> ${autoChatEnabled ? "ON 🟢 (Sabhi users ke liye)" : "OFF 🔴 (Sirf selected users ke liye)"}\n` +
    `👑 <b>Owner:</b> ${OWNER_USERNAME}\n` +
    `👥 <b>Total Users:</b> ${data.totalUsers.length}\n\n` +
    `✅ <b>Access List (${Object.keys(data.accessList).length}):</b>\n${accessText || "  None\n"}\n` +
    `🤖 <b>Auto Chat Access (${autoChatAccessList.length}):</b>\n${autoChatAccessText}\n` +
    `🚫 <b>Banned (${data.bannedUsers.length}):</b>\n${bannedText}`,
    { parse_mode: "HTML" }
  );
});


bot.command("sessions", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }

  await ctx.reply("⏳ <b>Fetching session info...</b>", { parse_mode: "HTML" });

  try {
    const stats = await getSessionStats();
    const activeIds = getActiveSessionUserIds();

    if (!stats.length) {
      await ctx.reply("📭 <b>No WhatsApp sessions in MongoDB.</b>", { parse_mode: "HTML" });
      return;
    }

    const nums = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"];
    let text = `📱 <b>WhatsApp Sessions (${stats.length})</b>\n\n`;

    for (let i = 0; i < stats.length; i++) {
      const s = stats[i];
      const isLive = activeIds.has(s.userId);
      const statusIcon = isLive ? "🟢" : s.registered ? "🔴" : "⚪";
      const statusLabel = isLive ? "Live" : s.registered ? "Disconnected" : "Unpaired";
      const num = i < nums.length ? nums[i] : `[${i+1}]`;
      text += `${num} ${statusIcon} <b>${esc(s.phoneNumber)}</b>\n`;
      text += `   Status: ${statusLabel} | Last: ${esc(s.lastSeen)}\n\n`;
    }

    const liveCount = stats.filter(s => activeIds.has(s.userId)).length;
    const disconnectedCount = stats.filter(s => !activeIds.has(s.userId) && s.registered).length;
    const unpairedCount = stats.filter(s => !s.registered).length;

    text += `📊 <b>Summary:</b> 🟢 ${liveCount} Live | 🔴 ${disconnectedCount} Off | ⚪ ${unpairedCount} Unpaired\n\n`;
    text += `💡 <code>/cleansessions [number]</code> to delete a specific session`;

    await ctx.reply(text, { parse_mode: "HTML" });
  } catch (err: any) {
    await ctx.reply(`❌ Error: ${esc(err?.message || "Unknown error")}`, { parse_mode: "HTML" });
  }
});

bot.command("cleansessions", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }

  const args = (ctx.message?.text || "").split(/\s+/).slice(1);
  const targetNum = args[0] ? parseInt(args[0]) : NaN;

  // --- Delete specific session by number ---
  if (!isNaN(targetNum) && targetNum > 0) {
    await ctx.reply(`🔍 <b>Fetching session #${targetNum}...</b>`, { parse_mode: "HTML" });
    try {
      const stats = await getSessionStats();
      if (targetNum > stats.length) {
        await ctx.reply(`❌ Session #${targetNum} not found. Use /sessions to see the list (total: ${stats.length}).`, { parse_mode: "HTML" });
        return;
      }
      const session = stats[targetNum - 1];
      const activeIds = getActiveSessionUserIds();
      const wasLive = activeIds.has(session.userId);

      // Disconnect from WhatsApp if live
      if (wasLive) {
        try { await disconnectWhatsApp(session.userId); } catch {}
      }

      // Delete from MongoDB
      await clearMongoSession(session.userId);

      // Force GC if available
      if (typeof (global as any).gc === "function") (global as any).gc();

      const memAfter = process.memoryUsage();
      const heapMB = (memAfter.heapUsed / 1024 / 1024).toFixed(1);
      const rssMB = (memAfter.rss / 1024 / 1024).toFixed(1);

      const nums = ["1️⃣","2️⃣","3️⃣","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"];
      const numIcon = (targetNum - 1) < nums.length ? nums[targetNum - 1] : `#${targetNum}`;

      await ctx.reply(
        `✅ <b>Session Deleted!</b>\n\n` +
        `${numIcon} 📱 <b>${esc(session.phoneNumber)}</b>\n` +
        `🔌 Was Live: ${wasLive ? "Yes (disconnected)" : "No"}\n` +
        `🗑 MongoDB: Cleaned\n\n` +
        `🧠 <b>Memory after:</b> RSS ${rssMB} MB | Heap ${heapMB} MB`,
        { parse_mode: "HTML" }
      );
    } catch (err: any) {
      await ctx.reply(`❌ Error: ${esc(err?.message || "Unknown error")}`, { parse_mode: "HTML" });
    }
    return;
  }

  // --- Bulk cleanup: delete stale sessions ---
  await ctx.reply("🧹 <b>Running bulk cleanup...</b>\n\nDeleting sessions inactive for 7+ days...", { parse_mode: "HTML" });

  try {
    const activeIds = getActiveSessionUserIds();
    const result = await cleanupStaleSessions(activeIds, 7);

    if (typeof (global as any).gc === "function") (global as any).gc();

    const memAfter = process.memoryUsage();
    const heapMB = (memAfter.heapUsed / 1024 / 1024).toFixed(1);
    const rssMB = (memAfter.rss / 1024 / 1024).toFixed(1);

    if (result.deletedSessions === 0) {
      await ctx.reply(
        `✅ <b>Cleanup Done!</b>\n\nNo stale sessions found. MongoDB is clean.\n\n` +
        `🧠 Memory: RSS ${rssMB} MB | Heap ${heapMB} MB`,
        { parse_mode: "HTML" }
      );
    } else {
      await ctx.reply(
        `✅ <b>Bulk Cleanup Done!</b>\n\n` +
        `🗑 Sessions deleted: <b>${result.deletedSessions}</b>\n` +
        `⚪ Unpaired deleted: <b>${result.deletedUnpaired}</b>\n` +
        `🔑 Keys freed: <b>${result.deletedKeys}</b>\n\n` +
        `🧠 <b>Memory after:</b> RSS ${rssMB} MB | Heap ${heapMB} MB`,
        { parse_mode: "HTML" }
      );
    }
  } catch (err: any) {
    await ctx.reply(`❌ Error: ${esc(err?.message || "Unknown error")}`, { parse_mode: "HTML" });
  }
});

// Per-user memory consumption estimator. We can't get exact per-user RSS
// from Node, but we CAN approximate it by summing the byte cost of every
// per-user data structure we own, plus a fixed estimate per WhatsApp socket
// (Baileys keeps signal sessions, message store, and pre-key cache in RAM
// per connected user — ~6 MB measured average). This is an estimate, not a
// guaranteed exact figure, but it surfaces the actual top consumers reliably
// (the user with 10 active flows + giant userState always rises to the top).
const WA_SOCKET_EST_MB = Number(process.env.WA_SOCKET_EST_MB || "6");

interface UserMemEntry {
  userId: number;
  estBytes: number;
  parts: string[];
}

function safeJsonSize(obj: unknown): number {
  try {
    const seen = new WeakSet();
    const s = JSON.stringify(obj, (_k, v) => {
      if (typeof v === "object" && v !== null) {
        if (seen.has(v as object)) return undefined;
        seen.add(v as object);
      }
      // Drop non-serialisable heavy refs (sockets, streams) to avoid throwing.
      if (typeof v === "function") return undefined;
      return v;
    });
    return s ? s.length : 0;
  } catch {
    return 0;
  }
}

function computePerUserMemory(): UserMemEntry[] {
  const map = new Map<number, UserMemEntry>();
  const ensure = (uid: number): UserMemEntry => {
    let e = map.get(uid);
    if (!e) {
      e = { userId: uid, estBytes: 0, parts: [] };
      map.set(uid, e);
    }
    return e;
  };

  // 1. Live WhatsApp sockets — by far the biggest per-user cost.
  for (const uidStr of getActiveSessionUserIds()) {
    const uid = Number(uidStr);
    if (!Number.isFinite(uid)) continue;
    const e = ensure(uid);
    e.estBytes += WA_SOCKET_EST_MB * 1024 * 1024;
    e.parts.push("WA");
  }

  // 2. userStates — flow state machines (group lists, VCF data, etc.)
  for (const [uid, state] of userStates) {
    const bytes = safeJsonSize(state);
    if (bytes === 0) continue;
    const e = ensure(uid);
    e.estBytes += bytes;
    e.parts.push(`state:${(bytes / 1024).toFixed(0)}KB`);
  }

  // 3. Long-running flows — each holds queues, schedules, group caches.
  for (const [uid, s] of autoChatSessions) {
    const bytes = safeJsonSize(s) + 1.5 * 1024 * 1024; // +1.5 MB runtime
    const e = ensure(uid);
    e.estBytes += bytes;
    e.parts.push("AutoChat");
  }
  for (const [uid, s] of cigSessions) {
    const bytes = safeJsonSize(s) + 2 * 1024 * 1024; // +2 MB (group msg cache)
    const e = ensure(uid);
    e.estBytes += bytes;
    e.parts.push("CIG");
  }
  for (const [uid, s] of acfSessions) {
    const bytes = safeJsonSize(s) + 1.5 * 1024 * 1024;
    const e = ensure(uid);
    e.estBytes += bytes;
    e.parts.push("ACF");
  }

  // 4. QR pairing screens — small but counted for completeness.
  for (const uid of qrPairings.keys()) {
    const e = ensure(uid);
    e.estBytes += 100 * 1024; // ~100 KB QR state
    e.parts.push("QR");
  }

  // 5. /help pagination — pre-rendered HTML chunks per user.
  for (const [uid, chunks] of helpPages) {
    const bytes = chunks.reduce((sum, c) => sum + c.length, 0);
    if (bytes === 0) continue;
    const e = ensure(uid);
    e.estBytes += bytes;
    e.parts.push(`help:${(bytes / 1024).toFixed(0)}KB`);
  }

  return [...map.values()].sort((a, b) => b.estBytes - a.estBytes);
}

function fmtMB(bytes: number): string {
  return (bytes / 1024 / 1024).toFixed(1);
}

function fmtUptime(ms: number): string {
  const totalMin = Math.floor(ms / 60000);
  const days = Math.floor(totalMin / 1440);
  const hours = Math.floor((totalMin % 1440) / 60);
  const mins = totalMin % 60;
  if (days > 0) return `${days}d ${hours}h ${mins}m`;
  if (hours > 0) return `${hours}h ${mins}m`;
  return `${mins}m`;
}

bot.command("memory", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }

  // Force a quick GC pass before sampling so the numbers reflect actual
  // live memory, not garbage waiting to be collected. Cheap (<10ms) and
  // gives a much more honest reading.
  if (typeof (global as any).gc === "function") {
    try { (global as any).gc(); } catch {}
  }

  const mem = process.memoryUsage();
  const heapUsedMB = mem.heapUsed / 1024 / 1024;
  const heapTotalMB = mem.heapTotal / 1024 / 1024;
  const rssMB = mem.rss / 1024 / 1024;
  const externalMB = mem.external / 1024 / 1024;
  const arrayBuffersMB = mem.arrayBuffers / 1024 / 1024;

  // Compare heapUsed against the actual --max-old-space-size limit (380MB
  // in package.json), NOT against heapTotal — heapTotal is just whatever
  // Node has lazily allocated so far, which makes the % reading useless.
  const HEAP_LIMIT_MB = Number(process.env.NODE_HEAP_LIMIT_MB || "380");
  const heapPct = Math.min(100, Math.round((heapUsedMB / HEAP_LIMIT_MB) * 100));
  const RENDER_LIMIT_MB = Number(process.env.RENDER_RAM_LIMIT_MB || "512");
  const rssPct = Math.min(100, Math.round((rssMB / RENDER_LIMIT_MB) * 100));

  const heapBar = buildMemBar(heapPct);
  const rssBar = buildMemBar(rssPct);
  const heapStatus = heapPct >= 85 ? "🔴 Critical" : heapPct >= 65 ? "🟡 High" : "🟢 Normal";
  const rssStatus = rssPct >= 85 ? "🔴 Critical" : rssPct >= 65 ? "🟡 High" : "🟢 Normal";

  const rssGrowthMB = rssMB - STARTUP_RSS_MB;
  const growthSign = rssGrowthMB >= 0 ? "+" : "";
  const growthEmoji = rssGrowthMB > 50 ? "📈" : rssGrowthMB > 10 ? "↗️" : rssGrowthMB < -10 ? "📉" : "➡️";

  const waActiveIds = getActiveSessionUserIds();
  const uptimeMs = Date.now() - STARTUP_TIMESTAMP_MS;
  const uptimeStr = fmtUptime(uptimeMs);

  // Per-user memory breakdown — top 5 consumers.
  const perUser = computePerUserMemory();
  const top5 = perUser.slice(0, 5);
  const totalTrackedMB = perUser.reduce((s, e) => s + e.estBytes, 0) / 1024 / 1024;

  let topUsersBlock = "";
  if (top5.length === 0) {
    topUsersBlock = "  <i>No active users</i>\n";
  } else {
    for (let i = 0; i < top5.length; i++) {
      const u = top5[i];
      const medal = i === 0 ? "🥇" : i === 1 ? "🥈" : i === 2 ? "🥉" : `${i + 1}.`;
      const partsStr = u.parts.length > 0 ? u.parts.join(", ") : "—";
      topUsersBlock += `  ${medal} <code>${u.userId}</code> — <b>${fmtMB(u.estBytes)} MB</b>\n`;
      topUsersBlock += `      └ ${esc(partsStr)}\n`;
    }
  }

  const text =
    `🧠 <b>Server Memory — Live</b>\n` +
    `<i>Uptime: ${uptimeStr}</i>\n` +
    `─────────────────────────────\n\n` +
    `📦 <b>RSS (Total RAM):</b> ${fmtMB(mem.rss)} MB / ${RENDER_LIMIT_MB} MB\n` +
    `${rssBar} ${rssPct}%  ${rssStatus}\n` +
    `${growthEmoji} Since startup: <b>${growthSign}${rssGrowthMB.toFixed(1)} MB</b> ` +
    `(boot: ${STARTUP_RSS_MB.toFixed(0)} MB)\n\n` +
    `🔵 <b>JS Heap (used / limit):</b>\n` +
    `${heapBar} ${heapPct}%  ${heapStatus}\n` +
    `   ${fmtMB(mem.heapUsed)} MB used / ${HEAP_LIMIT_MB} MB limit\n` +
    `   ${fmtMB(mem.heapTotal)} MB allocated by V8\n\n` +
    `🧩 <b>Off-heap (C++/Buffers):</b>\n` +
    `   External: ${externalMB.toFixed(1)} MB\n` +
    `   ArrayBuffers: ${arrayBuffersMB.toFixed(1)} MB\n\n` +
    `👥 <b>Active Sessions:</b>\n` +
    `  📱 WhatsApp connected: <b>${waActiveIds.size}</b>\n` +
    `  🤖 Auto Chat: <b>${autoChatSessions.size}</b> / ${MAX_CONCURRENT_AUTOCHAT}\n` +
    `  💬 Chat-In-Group: <b>${cigSessions.size}</b>\n` +
    `  🔁 Auto Chat Friend: <b>${acfSessions.size}</b>\n` +
    `  🗂️ User states: <b>${userStates.size}</b>\n` +
    `  📷 QR pairings: <b>${qrPairings.size}</b>\n` +
    `  📖 Help pages cached: <b>${helpPages.size}</b>\n\n` +
    `🔥 <b>Top RAM Consumers (Top 5):</b>\n` +
    topUsersBlock +
    `  ─────────────────\n` +
    `  📊 Tracked total: ~<b>${totalTrackedMB.toFixed(1)} MB</b> across <b>${perUser.length}</b> user(s)\n\n` +
    `⚙️ <b>Config:</b>\n` +
    `  • Heap limit: ${HEAP_LIMIT_MB} MB\n` +
    `  • RSS limit: ${RENDER_LIMIT_MB} MB\n` +
    `  • Cleanup: every ${Math.round(MEMORY_CLEANUP_INTERVAL_MS / 60000)} min\n` +
    `  • WA socket est: ${WA_SOCKET_EST_MB} MB/user\n\n` +
    `💡 <i>Tap /cleanram to force a manual purge.</i>`;

  await ctx.reply(text, { parse_mode: "HTML" });
});

function buildMemBar(pct: number): string {
  const filled = Math.round(pct / 10);
  const empty = 10 - filled;
  return `[${"█".repeat(filled)}${"░".repeat(empty)}]`;
}

// runMemoryPurge — shared implementation behind both the admin /cleanram
// command and the automatic memory watchdog (see index.ts). Clears every
// cache that's safe to drop without breaking active users:
//   • i18n translation cache + negative cache (will re-translate on demand)
//   • /help pagination state (users will re-paginate)
//   • Expired QR pairings (active QR scans untouched)
//   • Stale userActivity entries (anyone idle > USER_IDLE_DISCONNECT_MS)
//   • All cancel-request flag sets
//   • Idle WhatsApp sockets via sweepIdleSessions (doesn't kick live users)
//   • newSessionFlag (per-update flag, safe to clear)
// Then forces 3 GC passes (V8 needs multiple to reclaim across heap regions)
// and returns before/after RSS plus per-bucket counts so the caller can
// log/display them.
//
// SAFE: Does NOT touch userStates of users with ongoing flows, in-flight
// translation promises, autoChat/cig/acf sessions, or live WA sockets that
// have been active recently. Those would break user experience.
//
// `reason` is only used for the [MEM-PURGE] log line so we can tell apart
// admin-triggered runs (`/cleanram`) from automatic ones (`auto-watchdog
// rss=4XX`).
export interface MemoryPurgeResult {
  rssBefore: number;
  rssAfter: number;
  rssDelta: number;
  heapBefore: number;
  heapAfter: number;
  heapDelta: number;
  i18nMemCleared: number;
  i18nNegCleared: number;
  helpPagesCleared: number;
  qrCleared: number;
  userStatesCleared: number;
  activityCleared: number;
  cancelCleared: number;
  newSessionCleared: number;
  waEvicted: number;
  waTotal: number;
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-user state purge — call this whenever a user disconnects WhatsApp so
// their slice of every in-memory Map/Set is dropped right away. Without this,
// even after disconnectWhatsApp() releases the Baileys socket, all the
// follow-on per-user objects (state machines, activity timestamps, paginated
// help pages, QR intervals, cancellation flags, auto-chat session objects)
// keep their share of RAM until the next global purge — which is the bug the
// user is seeing on their 512MB Render dyno.
// ─────────────────────────────────────────────────────────────────────────────
function clearUserMemoryState(telegramUserId: number): void {
  // 1. State machine + transient form state
  userStates.delete(telegramUserId);

  // 2. Per-user activity / cooldown bookkeeping
  userActivity.delete(telegramUserId);

  // 3. /help paginated message buffers (can hold large translated strings)
  helpPages.delete(telegramUserId);
  helpPagesLastTouched.delete(telegramUserId);

  // 4. QR pairing UI — interval refers to the (now stale) socket, must be cleared
  const qr = qrPairings.get(telegramUserId);
  if (qr?.interval) {
    try { clearInterval(qr.interval); } catch {}
  }
  qrPairings.delete(telegramUserId);

  // 5. Long-running auto-chat / chat-in-group / add-chat-friends sessions
  const ac = autoChatSessions.get(telegramUserId);
  if (ac) { ac.cancelled = true; ac.running = false; }
  autoChatSessions.delete(telegramUserId);

  const cig = cigSessions.get(telegramUserId);
  if (cig) { (cig as any).cancelled = true; (cig as any).running = false; }
  cigSessions.delete(telegramUserId);

  const acf = acfSessions.get(telegramUserId);
  if (acf) { (acf as any).cancelled = true; (acf as any).running = false; }
  acfSessions.delete(telegramUserId);

  // 6. One-shot cancellation flag sets
  joinCancelRequests.delete(telegramUserId);
  getLinkCancelRequests.delete(telegramUserId);
  addMembersCancelRequests.delete(telegramUserId);
  removeMembersCancelRequests.delete(telegramUserId);
  approvalCancelRequests.delete(telegramUserId);

  // 7. New-session flag
  newSessionFlag.delete(telegramUserId);
}

export async function runMemoryPurge(reason: string): Promise<MemoryPurgeResult> {
  const memBefore = process.memoryUsage();
  const rssBefore = memBefore.rss / 1024 / 1024;
  const heapBefore = memBefore.heapUsed / 1024 / 1024;

  // 1. Translation caches (biggest non-session leak source)
  const i18nCleared = clearTranslationCaches();

  // 2. /help pagination
  const helpPagesCleared = helpPages.size;
  helpPages.clear();
  helpPagesLastTouched.clear();

  // 3. Expired QR pairings (keep active scans alive)
  let qrCleared = 0;
  for (const [userId, state] of qrPairings) {
    if (state.expired) {
      if (state.interval) clearInterval(state.interval);
      qrPairings.delete(userId);
      qrCleared++;
    }
  }

  // 4. Stale userStates — only ones not in a long-running session AND
  //    idle past the disconnect window. Safe because the user has clearly
  //    walked away; they'll start fresh on next /start.
  const longRunning = new Set<number>([
    ...autoChatSessions.keys(),
    ...cigSessions.keys(),
    ...acfSessions.keys(),
  ]);
  let userStatesCleared = 0;
  for (const [userId, state] of userStates) {
    if (longRunning.has(userId)) continue;
    if (isUserActive(userId)) continue;
    if (state.groupSettings) state.groupSettings.dpBuffers = [];
    if (state.editSettingsData) state.editSettingsData.settings.dpBuffers = [];
    userStates.delete(userId);
    userStatesCleared++;
  }

  // 5. Stale userActivity entries
  let activityCleared = 0;
  const activityCutoff = Date.now() - USER_IDLE_DISCONNECT_MS * 2;
  for (const [userId, a] of userActivity) {
    if (a.lastActivityAt < activityCutoff) {
      userActivity.delete(userId);
      activityCleared++;
    }
  }

  // 6. Cancel-request flag sets
  const cancelCleared = joinCancelRequests.size + getLinkCancelRequests.size +
    addMembersCancelRequests.size + removeMembersCancelRequests.size +
    approvalCancelRequests.size;
  joinCancelRequests.clear();
  getLinkCancelRequests.clear();
  addMembersCancelRequests.clear();
  removeMembersCancelRequests.clear();
  approvalCancelRequests.clear();

  // 7. newSessionFlag
  const newSessionCleared = newSessionFlag.size;
  newSessionFlag.clear();

  // 8. Idle WhatsApp sockets (does not kick recently-active users)
  let waEvicted = 0;
  let waTotal = 0;
  try {
    const sweep = sweepIdleSessions();
    waEvicted = sweep.evicted;
    waTotal = sweep.total;
  } catch {}

  // 9. Force GC multiple times. V8 collects in regions; one pass often
  //    leaves freshly-orphaned objects un-reclaimed. 3 passes with a tiny
  //    yield in between gives the collector time to compact.
  if (typeof (global as any).gc === "function") {
    for (let i = 0; i < 3; i++) {
      (global as any).gc();
      await new Promise((r) => setTimeout(r, 100));
    }
  }

  const memAfter = process.memoryUsage();
  const rssAfter = memAfter.rss / 1024 / 1024;
  const heapAfter = memAfter.heapUsed / 1024 / 1024;
  const rssDelta = rssBefore - rssAfter;
  const heapDelta = heapBefore - heapAfter;

  console.log(
    `[MEM-PURGE] ${reason}: rss ${rssBefore.toFixed(0)}MB -> ${rssAfter.toFixed(0)}MB ` +
    `(freed ${rssDelta.toFixed(0)}MB), heap ${heapBefore.toFixed(0)}MB -> ${heapAfter.toFixed(0)}MB, ` +
    `i18n=${i18nCleared.memCleared}+${i18nCleared.negCleared} states=${userStatesCleared} ` +
    `wa=${waEvicted}evicted/${waTotal}live`
  );

  return {
    rssBefore, rssAfter, rssDelta,
    heapBefore, heapAfter, heapDelta,
    i18nMemCleared: i18nCleared.memCleared,
    i18nNegCleared: i18nCleared.negCleared,
    helpPagesCleared, qrCleared, userStatesCleared, activityCleared,
    cancelCleared, newSessionCleared, waEvicted, waTotal,
  };
}

// /cleanram — admin-only manual trigger for runMemoryPurge.
// Replies with a before/after breakdown so admin can see exactly what was
// freed. Underlying logic is the same as the automatic watchdog.
bot.command("cleanram", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }

  const statusMsg = await ctx.reply("🧹 <b>Cleaning RAM...</b>\n\nClearing caches and running garbage collection...", { parse_mode: "HTML" });

  const r = await runMemoryPurge("admin /cleanram");
  const i18nCleared = { memCleared: r.i18nMemCleared, negCleared: r.i18nNegCleared };
  const helpPagesCleared = r.helpPagesCleared;
  const qrCleared = r.qrCleared;
  const userStatesCleared = r.userStatesCleared;
  const activityCleared = r.activityCleared;
  const cancelCleared = r.cancelCleared;
  const newSessionCleared = r.newSessionCleared;
  const waEvicted = r.waEvicted;
  const waTotal = r.waTotal;
  const rssBefore = r.rssBefore;
  const rssAfter = r.rssAfter;
  const heapBefore = r.heapBefore;
  const heapAfter = r.heapAfter;
  const rssDelta = r.rssDelta;
  const heapDelta = r.heapDelta;

  const fmt = (n: number) => n.toFixed(1);
  const sign = (n: number) => (n >= 0 ? "−" : "+"); // we report freed as positive
  const totalEntries = i18nCleared.memCleared + i18nCleared.negCleared +
    helpPagesCleared + qrCleared + userStatesCleared + activityCleared +
    cancelCleared + newSessionCleared;

  const text =
    `✅ <b>RAM Cleanup Done!</b>\n\n` +
    `📦 <b>RAM (RSS):</b>\n` +
    `  Before: ${fmt(rssBefore)} MB\n` +
    `  After:  ${fmt(rssAfter)} MB\n` +
    `  Freed:  <b>${sign(rssDelta)}${fmt(Math.abs(rssDelta))} MB</b>\n\n` +
    `🔵 <b>Heap:</b>\n` +
    `  Before: ${fmt(heapBefore)} MB\n` +
    `  After:  ${fmt(heapAfter)} MB\n` +
    `  Freed:  <b>${sign(heapDelta)}${fmt(Math.abs(heapDelta))} MB</b>\n\n` +
    `🗑 <b>Cache entries cleared:</b> ${totalEntries}\n` +
    `  • Translation cache: ${i18nCleared.memCleared}\n` +
    `  • Translation neg-cache: ${i18nCleared.negCleared}\n` +
    `  • /help pagination: ${helpPagesCleared}\n` +
    `  • Idle user states: ${userStatesCleared}\n` +
    `  • Stale activity: ${activityCleared}\n` +
    `  • Expired QR pairings: ${qrCleared}\n` +
    `  • Cancel flags: ${cancelCleared}\n` +
    `  • New-session flags: ${newSessionCleared}\n\n` +
    `📱 <b>WhatsApp sockets:</b> ${waEvicted} idle evicted (${waTotal} live remain)\n\n` +
    `💡 <i>Active users, ongoing flows, and live WhatsApp sessions were not touched.</i>`;

  try {
    await ctx.api.editMessageText(ctx.chat!.id, statusMsg.message_id, text, { parse_mode: "HTML" });
  } catch {
    await ctx.reply(text, { parse_mode: "HTML" });
  }
});

bot.callbackQuery("broadcast_cancel", async (ctx) => {
  await ctx.answerCallbackQuery("Broadcast cancelled.");
  const adminId = ctx.from.id;
  if (!isAdmin(adminId)) return;
  userStates.delete(adminId);
  await ctx.editMessageText("❌ <b>Broadcast Cancelled</b>\n\nNo message was sent.", {
    parse_mode: "HTML",
    reply_markup: new InlineKeyboard().text("🏠 Menu", "main_menu"),
  }).catch(() => {});
});

bot.callbackQuery("broadcast_confirm", async (ctx) => {
  await ctx.answerCallbackQuery("Broadcast started.");
  const adminId = ctx.from.id;
  if (!isAdmin(adminId)) return;
  const state = userStates.get(adminId);
  const data = state?.broadcastData;
  if (!data) {
    await ctx.editMessageText("⚠️ Broadcast request expired. Please send /broadcast again.", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("🏠 Menu", "main_menu"),
    }).catch(() => {});
    return;
  }

  userStates.delete(adminId);
  const progressMessageId = ctx.callbackQuery.message?.message_id;
  if (!progressMessageId) {
    const msg = await ctx.reply(broadcastProgressText(data.users.length, 0, 0), { parse_mode: "HTML" });
    void sendBroadcastToUsers(adminId, msg.message_id, data.users, data.message);
    return;
  }

  await ctx.editMessageText(broadcastProgressText(data.users.length, 0, 0), { parse_mode: "HTML" }).catch(() => {});
  void sendBroadcastToUsers(adminId, progressMessageId, data.users, data.message);
});

// ─── Connect WhatsApp ────────────────────────────────────────────────────────

bot.callbackQuery("connect_wa", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  clearQrPairing(userId);
  await disconnectWhatsApp(String(userId)).catch(() => {});

  const connectedText = "✅ <b>WhatsApp already connected!</b>\n\nYou can use all features.";
  const connectedKb = new InlineKeyboard().text("🏠 Main Menu", "main_menu");
  const connectText = "📱 <b>Connect WhatsApp</b>\n\nChoose pairing method:";
  const connectKb = new InlineKeyboard()
    .text("🔑 Pair Code", "connect_pair_code")
    .text("📷 Pair QR", "connect_pair_qr")
    .row()
    .text("🔙 Back", "main_menu");

  userStates.delete(userId);

  const isPhoto = !!ctx.callbackQuery.message?.photo;

  if (isPhoto) {
    try { await ctx.deleteMessage(); } catch {}
    if (isConnected(String(userId))) {
      await ctx.reply(connectedText, { parse_mode: "HTML", reply_markup: connectedKb });
    } else {
      await ctx.reply(connectText, { parse_mode: "HTML", reply_markup: connectKb });
    }
    return;
  }

  if (isConnected(String(userId))) {
    await ctx.editMessageText(connectedText, { parse_mode: "HTML", reply_markup: connectedKb });
    return;
  }
  await ctx.editMessageText(connectText, { parse_mode: "HTML", reply_markup: connectKb });
});

bot.callbackQuery("connect_pair_code", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  clearQrPairing(userId);
  if (isConnected(String(userId))) {
    await ctx.editMessageText(
      "✅ <b>WhatsApp already connected!</b>\n\nYou can use all features.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
    return;
  }
  userStates.set(userId, { step: "awaiting_phone" });
  await ctx.editMessageText(
    "🔑 <b>Pair Code</b>\n\nEnter your phone number with country code:\n\nExample: <code>+919942222222</code>",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔙 Back", "connect_wa").text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("connect_pair_qr", async (ctx) => {
  await ctx.answerCallbackQuery();
  await startQrPairing(ctx, ctx.from.id);
});

bot.callbackQuery("connect_pair_qr_retry", async (ctx) => {
  await ctx.answerCallbackQuery();
  await startQrPairing(ctx, ctx.from.id);
});

bot.callbackQuery("connect_pair_qr_cancel", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const active = qrPairings.get(userId);
  if (active) {
    if (active.interval) clearInterval(active.interval);
    active.expired = true;
  }
  clearQrPairing(userId);
  await disconnectWhatsApp(String(userId)).catch(() => {});
  try { await ctx.deleteMessage(); } catch {}
  await ctx.reply(
    "📱 <b>Connect WhatsApp</b>\n\nChoose pairing method:",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🔑 Pair Code", "connect_pair_code")
        .text("📷 Pair QR", "connect_pair_qr")
        .row()
        .text("🔙 Back", "main_menu"),
    }
  );
});

// ─── Create Groups ───────────────────────────────────────────────────────────

function defaultGroupSettings(): GroupSettings {
  return { name: "", description: "", count: 1, finalNames: [], namingMode: "auto", dpBuffers: [], editGroupInfo: true, sendMessages: true, addMembers: true, approveJoin: false, disappearingMessages: 0, friendNumbers: [], makeFriendAdmin: false };
}

function settingsKeyboard(gs: GroupSettings): InlineKeyboard {
  const on = (v: boolean) => v ? "✅ ON" : "❌ OFF";
  return new InlineKeyboard()
    .text(`📝 Edit Info: ${on(gs.editGroupInfo)}`, "tog_editInfo").text(`💬 Send Msgs: ${on(gs.sendMessages)}`, "tog_sendMsg").row()
    .text(`➕ Add Members: ${on(gs.addMembers)}`, "tog_addMembers").text(`🔐 Approve: ${on(gs.approveJoin)}`, "tog_approveJoin").row()
    .text("💾 Save Settings", "settings_done");
}

function settingsText(gs: GroupSettings): string {
  const on = (v: boolean) => v ? "✅ ON" : "❌ OFF";
  return (
    "⚙️ <b>Group Permissions</b>\n\n" +
    "<b>👥 Members can:</b>\n" +
    `📝 Edit Group Info: ${on(gs.editGroupInfo)}\n` +
    `💬 Send Messages: ${on(gs.sendMessages)}\n` +
    `➕ Add Members: ${on(gs.addMembers)}\n\n` +
    "<b>👑 Admins:</b>\n" +
    `🔐 Approve New Members: ${on(gs.approveJoin)}\n\n` +
    "Tap to toggle each setting:"
  );
}

bot.callbackQuery("create_groups", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>\n\nPlease connect first.", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    }); return;
  }
  userStates.set(userId, { step: "group_enter_name", groupSettings: defaultGroupSettings() });
  await ctx.editMessageText(
    "👥 <b>Create WhatsApp Groups</b>\n\n✏️ Enter the group name:",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

for (const [cb, field] of [
  ["tog_editInfo", "editGroupInfo"], ["tog_sendMsg", "sendMessages"],
  ["tog_addMembers", "addMembers"], ["tog_approveJoin", "approveJoin"],
] as const) {
  bot.callbackQuery(cb, async (ctx) => {
    await ctx.answerCallbackQuery();
    const state = userStates.get(ctx.from.id);
    if (!state?.groupSettings) return;
    (state.groupSettings as any)[field] = !(state.groupSettings as any)[field];
    await ctx.editMessageText(settingsText(state.groupSettings), { parse_mode: "HTML", reply_markup: settingsKeyboard(state.groupSettings) });
  });
}

bot.callbackQuery("settings_done", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.groupSettings) return;
  state.step = "group_disappearing";
  const dmLabel = (v: number) => {
    if (v === 86400) return "✅ 24 Hours";
    if (v === 604800) return "✅ 7 Days";
    if (v === 7776000) return "✅ 90 Days";
    return "✅ Off";
  };
  const cur = state.groupSettings.disappearingMessages;
  await ctx.editMessageText(
    "⏳ <b>Disappearing Messages</b>\n\nGroup mein messages kitne time baad automatically delete hone chahiye?\n\n" +
    `Current: <b>${cur === 0 ? "Off" : cur === 86400 ? "24 Hours" : cur === 604800 ? "7 Days" : "90 Days"}</b>`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text(cur === 86400 ? "✅ 24 Hours" : "🕐 24 Hours", "gdm_24h").text(cur === 604800 ? "✅ 7 Days" : "📅 7 Days", "gdm_7d").row()
        .text(cur === 7776000 ? "✅ 90 Days" : "📆 90 Days", "gdm_90d").text(cur === 0 ? "✅ Off" : "🔕 Off", "gdm_off").row()
        .text("❌ Cancel", "main_menu"),
    }
  );
});

for (const [cb, dur] of [["gdm_24h", 86400], ["gdm_7d", 604800], ["gdm_90d", 7776000], ["gdm_off", 0]] as const) {
  bot.callbackQuery(cb, async (ctx) => {
    await ctx.answerCallbackQuery();
    const state = userStates.get(ctx.from.id);
    if (!state?.groupSettings) return;
    state.groupSettings.disappearingMessages = dur;
    state.step = "group_dp";
    const maxDps = state.groupSettings.count;
    await ctx.editMessageText(
      "🖼️ <b>Group Profile Photo(s)</b>\n\n" +
      `Ek ya zyada photos bhejo (max ${maxDps}).\n\n` +
      "• 1 photo bhejoge → sab groups mein wahi DP lagega\n" +
      `• N photos bhejoge → 1st DP → 1st group, 2nd DP → 2nd group, ... (max ${maxDps} kyunki tum ${maxDps} group bana rahe ho)\n\n` +
      "Photos ek ek karke bhejo. Saare bhej do to <b>✅ Done</b> dabao.\n" +
      "DP nahi lagana to <b>⏭️ Skip</b> karo.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "group_dp_skip").text("❌ Cancel", "main_menu") }
    );
  });
}

bot.callbackQuery("group_dp_skip", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.groupSettings) return;
  state.groupSettings.dpBuffers = [];
  await showGroupFriendsStep(ctx);
});

bot.callbackQuery("group_dp_done", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.groupSettings) return;
  await showGroupFriendsStep(ctx);
});

async function showGroupFriendsStep(ctx: any) {
  const userId = ctx.from?.id ?? ctx.chat?.id;
  const state = userStates.get(userId);
  if (!state?.groupSettings) return;
  state.step = "group_enter_friends";
  const friendsText =
    "👫 <b>Add Friends While Creating Group</b>\n\n" +
    "⚠️ <b>Important:</b> The friend's number must be saved in your contact list on WhatsApp. If the number is not saved, it may not be added.\n\n" +
    "Send friend numbers, one per line (with country code):\n" +
    "<code>919912345678\n919898765432</code>\n\n" +
    "You can also send with + prefix:\n" +
    "<code>+919912345678\n+91 9898 765432</code>\n\n" +
    "If you don't want to add any friend, tap Skip.";
  const friendsMarkup = new InlineKeyboard().text("⏭️ Skip", "group_skip_friends").text("❌ Cancel", "main_menu");
  try {
    await ctx.editMessageText(friendsText, { parse_mode: "HTML", reply_markup: friendsMarkup });
  } catch {
    await ctx.reply(friendsText, { parse_mode: "HTML", reply_markup: friendsMarkup });
  }
}

async function showGroupFriendAdminStep(ctx: any) {
  const userId = ctx.from?.id ?? ctx.chat?.id;
  const state = userStates.get(userId);
  if (!state?.groupSettings) return;
  state.step = "group_confirm_friend_admin";
  const count = state.groupSettings.friendNumbers.length;
  const text =
    `👑 <b>Make Friend Admin?</b>\n\n` +
    `You have added <b>${count}</b> friend number(s).\n\n` +
    `Do you want to make the friend(s) <b>Admin</b> in the group after they are added?\n\n` +
    `• <b>Yes</b> → Friends will be added to the group AND made admin\n` +
    `• <b>No</b> → Friends will only be added as members (not admin)`;
  const markup = new InlineKeyboard()
    .text("✅ Yes, Make Admin", "group_friend_admin_yes")
    .text("❌ No, Just Add", "group_friend_admin_no");
  try {
    await ctx.editMessageText(text, { parse_mode: "HTML", reply_markup: markup });
  } catch {
    await ctx.reply(text, { parse_mode: "HTML", reply_markup: markup });
  }
}

bot.callbackQuery("group_skip_friends", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.groupSettings) return;
  state.groupSettings.friendNumbers = [];
  state.groupSettings.makeFriendAdmin = false;
  await showGroupSummary(ctx);
});

bot.callbackQuery("group_friend_admin_yes", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.groupSettings) return;
  state.groupSettings.makeFriendAdmin = true;
  await showGroupSummary(ctx);
});

bot.callbackQuery("group_friend_admin_no", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.groupSettings) return;
  state.groupSettings.makeFriendAdmin = false;
  await showGroupSummary(ctx);
});

bot.callbackQuery("naming_auto", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.groupSettings) return;
  state.groupSettings.namingMode = "auto";
  state.groupSettings.finalNames = generateGroupNames(state.groupSettings.name, state.groupSettings.count);
  state.step = "group_enter_description";
  const preview = state.groupSettings.finalNames.slice(0, 5).map((n, i) => `${i + 1}. ${esc(n)}`).join("\n");
  await ctx.editMessageText(
    `✅ <b>Names Preview:</b>\n${preview}${state.groupSettings.count > 5 ? `\n... +${state.groupSettings.count - 5} more` : ""}\n\n` +
    "📄 <b>Group Description</b>\n\nSend description or type <code>skip</code>:",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("naming_custom", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.groupSettings) return;
  state.groupSettings.namingMode = "custom";
  state.groupSettings.finalNames = [];
  state.step = "group_enter_custom_names";
  await ctx.editMessageText(
    `✏️ <b>Custom Names</b>\n\nSend all <b>${state.groupSettings.count}</b> names, one per line:\n\n<i>Example:\nSpidy Squad\nSpidy Gang\nSpidy Army</i>`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

async function showGroupSummary(ctx: any) {
  const userId = ctx.from?.id ?? ctx.chat?.id;
  const state = userStates.get(userId);
  if (!state?.groupSettings) return;
  const gs = state.groupSettings;
  const namesList = gs.finalNames.map((n, i) => `${i + 1}. ${esc(n)}`).join("\n");
  state.step = "group_confirm";
  const dmText = gs.disappearingMessages === 86400 ? "24 Hours" : gs.disappearingMessages === 604800 ? "7 Days" : gs.disappearingMessages === 7776000 ? "90 Days" : "Off";
  const text =
    "📋 <b>Group Creation Summary</b>\n\n" +
    `📝 <b>Names (${gs.finalNames.length}):</b>\n${namesList}\n\n` +
    `📄 <b>Description:</b> ${gs.description ? esc(gs.description) : "None"}\n` +
    `🖼️ <b>Group DPs:</b> ${gs.dpBuffers.length > 0 ? `${gs.dpBuffers.length} photo(s)${gs.dpBuffers.length === 1 ? " (sab groups mein same)" : " (rotate honge)"}` : "❌ None"}\n` +
    `⏳ <b>Disappearing Msgs:</b> ${dmText}\n` +
    `👫 <b>Friends to add:</b> ${gs.friendNumbers.length > 0 ? `${gs.friendNumbers.length} numbers` : "None"}\n` +
    (gs.friendNumbers.length > 0 ? `👑 <b>Make Friend Admin:</b> ${gs.makeFriendAdmin ? "✅ Yes" : "❌ No"}\n` : "") +
    `\n` +
    "⚙️ <b>Permissions:</b>\n" +
    `${gs.editGroupInfo ? "✅" : "❌"} Edit Group Info | ${gs.sendMessages ? "✅" : "❌"} Send Messages\n` +
    `${gs.addMembers ? "✅" : "❌"} Add Members | ${gs.approveJoin ? "✅" : "❌"} Approve Join\n\n` +
    "🚀 Ready to create?";
  const markup = new InlineKeyboard().text("✅ Create Now", "group_create_start").text("❌ Cancel", "main_menu");
  try { await ctx.editMessageText(text, { parse_mode: "HTML", reply_markup: markup }); }
  catch { await ctx.reply(text, { parse_mode: "HTML", reply_markup: markup }); }
}

bot.callbackQuery("group_create_start", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.groupSettings) return;

  const gs = { ...state.groupSettings };
  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  state.step = "group_creating";
  state.groupCreationCancel = false;

  await ctx.editMessageText(
    `⏳ <b>Creating ${gs.finalNames.length} group(s)...</b>\n\n🔄 0/${gs.finalNames.length} done...`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel Creation", "group_cancel_creation") }
  );

  void createGroupsBackground(String(userId), userId, gs, chatId, msgId);
});

bot.callbackQuery("group_cancel_creation", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  // Set the "pending" flag immediately so the background loop's next
  // progress update doesn't overwrite the confirmation dialog.
  const state = userStates.get(userId);
  if (state) state.groupCreationCancelPending = true;
  await ctx.editMessageText(
    "⚠️ <b>Cancel Group Creation?</b>\n\nGroups already created will remain. Only remaining groups won't be created.\n\nAre you sure?",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Cancel", "group_cancel_confirm")
        .text("▶️ Continue", "group_cancel_dismiss"),
    }
  );
});

bot.callbackQuery("group_cancel_confirm", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "🛑 Creation cancelled!" });
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (state) {
    state.groupCreationCancel = true;
    // Keep the pending flag set as well — we don't want the background
    // loop to overwrite the "cancelled" message with a stale progress
    // update from a group that was already mid-creation when the user
    // confirmed. The background loop checks both flags before editing.
    state.groupCreationCancelPending = true;
  }
  await ctx.editMessageText(
    "🛑 <b>Group creation cancelled.</b>\n\nGroups already created will remain.",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
  );
});

bot.callbackQuery("group_cancel_dismiss", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "▶️ Continuing..." });
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (state) state.groupCreationCancelPending = false;
  // The background loop will resume editing on its next iteration. To
  // restore the progress UI immediately (instead of leaving the "Are you
  // sure?" dialog visible until the next group finishes), put back the
  // "❌ Cancel Creation" button now.
  try {
    await ctx.editMessageReplyMarkup({
      reply_markup: new InlineKeyboard().text("❌ Cancel Creation", "group_cancel_creation"),
    });
  } catch {}
});

async function createGroupsBackground(userId: string, numericUserId: number, gs: GroupSettings, chatId: number, msgId: number) {
  const perms: GroupPermissions = { editGroupInfo: gs.editGroupInfo, sendMessages: gs.sendMessages, addMembers: gs.addMembers, approveJoin: gs.approveJoin };
  const results: Array<{ name: string; link: string | null; error?: string; friendsAdded?: number; friendsFailed?: boolean; friendAdmin?: boolean }> = [];
  const total = gs.finalNames.length;

  for (let i = 0; i < total; i++) {
    const state = userStates.get(numericUserId);
    if (state?.groupCreationCancel) {
      results.push({ name: gs.finalNames[i], link: null, error: "Cancelled by user" });
      for (let j = i + 1; j < total; j++) {
        results.push({ name: gs.finalNames[j], link: null, error: "Cancelled by user" });
      }
      break;
    }

    const groupName = gs.finalNames[i];
    try {
      // Pass friendNumbers at creation time — bypasses WhatsApp privacy restrictions on non-contacts
      const result = await createWhatsAppGroup(userId, groupName, gs.friendNumbers);
      if (result) {
        await new Promise((r) => setTimeout(r, 1500));
        await applyGroupSettings(userId, result.id, perms, gs.description);
        if (gs.disappearingMessages > 0) {
          await new Promise((r) => setTimeout(r, 1000));
          await setGroupDisappearingMessages(userId, result.id, gs.disappearingMessages);
        }
        if (gs.dpBuffers.length > 0) {
          const dpBuf = gs.dpBuffers[i % gs.dpBuffers.length];
          await new Promise((r) => setTimeout(r, 2000));
          await setGroupIcon(userId, result.id, dpBuf);
          // Memory: when each DP is one-shot (i.e. dpBuffers.length >= total
          // groups, so no rotation), free that buffer right after use so the
          // heap doesn't carry hundreds of KB per user across the entire loop.
          if (gs.dpBuffers.length >= total) {
            gs.dpBuffers[i] = Buffer.alloc(0);
          }
        }

        let finalFriendsAdded = 0;
        let finalFriendsFailed = false;

        if (gs.friendNumbers.length > 0) {
          if (result.participantsFailed) {
            // Creation with participants failed — try adding separately as fallback
            await new Promise((r) => setTimeout(r, 3000));
            const addResults = await addGroupParticipantsBulk(userId, result.id, gs.friendNumbers);
            finalFriendsAdded = addResults.filter(r => r.success).length;
            finalFriendsFailed = finalFriendsAdded < gs.friendNumbers.length;
          } else {
            finalFriendsAdded = result.addedParticipants ?? 0;
          }

          // Promote friends to admin if user chose Yes
          if (gs.makeFriendAdmin && finalFriendsAdded > 0) {
            await new Promise((r) => setTimeout(r, 2000));
            for (const num of gs.friendNumbers) {
              const jid = `${num.replace(/[^0-9]/g, "")}@s.whatsapp.net`;
              try { await makeGroupAdmin(userId, result.id, jid); } catch {}
              await new Promise((r) => setTimeout(r, 800));
            }
          }
        }

        results.push({
          name: groupName,
          link: result.inviteCode,
          friendsAdded: gs.friendNumbers.length > 0 ? finalFriendsAdded : undefined,
          friendsFailed: finalFriendsFailed,
          friendAdmin: gs.makeFriendAdmin && finalFriendsAdded > 0,
        });
      } else {
        results.push({ name: groupName, link: null, error: "Failed to create" });
      }
    } catch (err: any) {
      results.push({ name: groupName, link: null, error: err?.message || "Unknown error" });
    }

    const done = i + 1;
    const lines = results.map((r) => r.link ? `✅ ${esc(r.name)}` : `❌ ${esc(r.name)}`).join("\n");

    // Re-read state right before editing — if the user has just cancelled
    // (or is mid-confirmation), DO NOT overwrite the cancel/dialog screen
    // with a stale progress update. Without this guard, a group that was
    // already in flight when the user tapped Cancel would push the
    // "Creating Groups: X/Y..." UI back on screen and make it look like
    // the cancel didn't work.
    const stateNow = userStates.get(numericUserId);
    const skipEdit = !!(stateNow?.groupCreationCancel || stateNow?.groupCreationCancelPending);
    if (!skipEdit) {
      try {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Creating Groups: ${done}/${total}</b>\n\n${lines}${done < total ? "\n\n⌛ Processing..." : ""}`,
          { parse_mode: "HTML", reply_markup: done < total ? new InlineKeyboard().text("❌ Cancel Creation", "group_cancel_creation") : undefined }
        );
      } catch {}
    }

    // If cancel was confirmed during this iteration, mark the remaining
    // groups as cancelled and break out — the final summary will be sent
    // by the post-loop block below.
    if (stateNow?.groupCreationCancel) {
      for (let j = i + 1; j < total; j++) {
        results.push({ name: gs.finalNames[j], link: null, error: "Cancelled by user" });
      }
      break;
    }

    if (i < total - 1) await new Promise((r) => setTimeout(r, 4000));
  }

  // Memory: drop all DP buffers as soon as the whole creation flow is done.
  // userStates.delete() below will eventually GC the state, but explicit clear
  // here lets the buffers be freed before any further async work in this fn.
  gs.dpBuffers = [];

  userStates.delete(numericUserId);

  const cancelled = results.some((r) => r.error === "Cancelled by user");
  const created = results.filter((r) => r.link).length;
  let message = cancelled
    ? `🛑 <b>Cancelled! (${created}/${total} created before cancel)</b>\n\n`
    : `🎉 <b>Done! (${created}/${total} created)</b>\n\n`;
  for (const r of results) {
    if (r.error === "Cancelled by user") {
      message += `🛑 <b>${esc(r.name)}</b>\n⚠️ Cancelled\n\n`;
    } else if (r.link) {
      let line = `✅ <b>${esc(r.name)}</b>\n🔗 ${r.link}`;
      if (r.friendsAdded !== undefined) {
        if (r.friendsFailed) {
          line += `\n👫 Friends: ${r.friendsAdded} added (some were not added — rejected by WhatsApp)`;
        } else if (r.friendsAdded > 0) {
          line += `\n👫 Friends: ${r.friendsAdded} added ✅`;
        }
        if (r.friendAdmin) {
          line += ` 👑 Made Admin`;
        }
      }
      message += line + "\n\n";
    } else {
      message += `❌ <b>${esc(r.name)}</b>\n⚠️ ${esc(r.error || "")}\n\n`;
    }
  }

  const chunks = splitMessage(message, 4000);
  try {
    await bot.api.editMessageText(chatId, msgId, chunks[0], {
      parse_mode: "HTML",
      link_preview_options: { is_disabled: true },
      reply_markup: chunks.length === 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
    });
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(chatId, chunks[i], {
      parse_mode: "HTML",
      link_preview_options: { is_disabled: true },
      reply_markup: i === chunks.length - 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
    });
  }
}

// ─── Join Groups ─────────────────────────────────────────────────────────────

bot.callbackQuery("join_groups", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    }); return;
  }
  userStates.set(userId, { step: "join_enter_links", joinData: { links: [] } });
  await ctx.editMessageText(
    "🔗 <b>Join Groups</b>\n\nSend WhatsApp group link(s), one per line:\n\n<code>https://chat.whatsapp.com/ABC123\nhttps://chat.whatsapp.com/XYZ456</code>",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("join_cancel_request", async (ctx) => {
  await ctx.answerCallbackQuery();
  cancelDialogActiveFor.add(ctx.from.id);
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard()
      .text("✅ Yes, Stop Joining", "join_cancel_confirm")
      .text("↩️ Continue", "join_cancel_no"),
  });
});

bot.callbackQuery("join_cancel_no", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Joining continued" });
  const userId = ctx.from.id;
  cancelDialogActiveFor.delete(userId);
  if (joinCancelRequests.has(userId)) return;
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard().text("❌ Cancel", "join_cancel_request"),
  });
});

bot.callbackQuery("join_cancel_confirm", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Stopping after current group..." });
  joinCancelRequests.add(ctx.from.id);
  // Keep the dialog flag on so the in-flight progress edit doesn't pop
  // the "❌ Cancel" button back. The background task clears the flag in
  // its finally cleanup.
  await ctx.editMessageReplyMarkup({ reply_markup: undefined });
});

// ─── CTC Checker ─────────────────────────────────────────────────────────────

bot.callbackQuery("ctc_checker", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    }); return;
  }
  userStates.set(userId, { step: "ctc_enter_links", ctcData: { groupLinks: [], pairs: [], currentPairIndex: 0 } });
  await ctx.editMessageText(
    "🔍 <b>CTC Checker</b>\n\nStep 1: Send all WhatsApp group links, one per line:\n\n<code>https://chat.whatsapp.com/ABC123\nhttps://chat.whatsapp.com/XYZ456</code>",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("ctc_start_check", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.ctcData) return;
  const activePairs = state.ctcData.pairs.filter((p) => p.vcfContacts.length > 0);
  if (!activePairs.length) { await ctx.editMessageText("⚠️ No VCF files provided. Please send VCF files first."); return; }

  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  userStates.delete(userId);
  await ctx.editMessageText(`⏳ <b>Checking ${activePairs.length} group(s)...</b>\n\n⌛ Please wait...`, { parse_mode: "HTML" });

  void ctcCheckBackground(String(userId), activePairs, chatId, msgId);
});

// Fix Wrong Pending: cached per-user data so the user can tap the
// "🛠 Fix Wrong Pending" button after a CTC check completes. We store
// only what's needed to re-fetch the live pending list and reject the
// JIDs whose phone number is NOT in the VCF for that group.
interface CtcFixData {
  groups: Array<{
    groupId: string;
    groupName: string;
    link: string;
    // last-10-digit phone numbers from this group's VCF — used to decide
    // which pending JIDs are "wrong" (= not in VCF) at fix time.
    vcfLast10Set: Set<string>;
    // Snapshot count from the check, just for the confirmation prompt.
    wrongCount: number;
  }>;
  totalWrong: number;
  createdAt: number;
}

const ctcFixDataStore: Map<number, CtcFixData> = new Map();

// Drop stale fix-data after 30 min so the map can't grow unbounded.
setInterval(() => {
  const cutoff = Date.now() - 30 * 60 * 1000;
  for (const [uid, data] of ctcFixDataStore) {
    if (data.createdAt < cutoff) ctcFixDataStore.delete(uid);
  }
}, 10 * 60 * 1000);

async function ctcCheckBackground(userId: string, activePairs: CtcPair[], chatId: number, msgId: number) {
  // Collect all VCF phone numbers across all pairs for duplicate detection
  // Map: phone number → list of group names it appears as pending
  const pendingPhoneToGroups = new Map<string, string[]>();

  // First pass: collect results per group
  const groupResults: Array<{
    groupId: string;
    groupName: string;
    link: string;
    vcfContacts: Array<{ name: string; phone: string; vcfFileName: string }>;
    inMembers: string[];
    inPending: string[];
    notFoundPhones: string[];
    allMemberPhones: Set<string>;
    allPendingPhones: Set<string>;
    pendingAvailable: boolean;
    couldNotAccess: boolean;
  }> = [];

  for (let i = 0; i < activePairs.length; i++) {
    const pair = activePairs[i];
    const cleanLink = buildCleanLink(pair.link);

    try {
      await bot.api.editMessageText(chatId, msgId,
        `⏳ <b>Checking group ${i + 1}/${activePairs.length}...</b>`,
        { parse_mode: "HTML" }
      );
    } catch {}

    const groupInfo = await getGroupIdFromLink(userId, cleanLink);
    if (!groupInfo) {
      groupResults.push({
        groupId: "",
        groupName: `Group ${i + 1}`,
        link: cleanLink,
        vcfContacts: pair.vcfContacts,
        inMembers: [],
        inPending: [],
        notFoundPhones: pair.vcfContacts.map(c => c.phone),
        allMemberPhones: new Set(),
        allPendingPhones: new Set(),
        pendingAvailable: false,
        couldNotAccess: true,
      });
      continue;
    }

    const phones = pair.vcfContacts.map((c) => c.phone);
    const checkResult = await checkContactsInGroup(userId, groupInfo.id, phones);
    const { inMembers, inPending, notFound: notFoundPhones, pendingAvailable, allMemberPhones, allPendingPhones } = checkResult;

    // Track ALL pending phones for duplicate detection (not just VCF matches)
    for (const phone of allPendingPhones) {
      if (!pendingPhoneToGroups.has(phone)) pendingPhoneToGroups.set(phone, []);
      pendingPhoneToGroups.get(phone)!.push(groupInfo.subject);
    }

    groupResults.push({
      groupId: groupInfo.id,
      groupName: groupInfo.subject,
      link: cleanLink,
      vcfContacts: pair.vcfContacts,
      inMembers,
      inPending,
      notFoundPhones,
      allMemberPhones,
      allPendingPhones,
      pendingAvailable,
      couldNotAccess: false,
    });
  }

  // ── Compact, scannable result format ────────────────────────────────────
  // Top: one-line totals so the user sees the headline numbers without
  // scrolling. Then per-group summary lines (no full pending phone dumps).
  // Wrong-pending phones are still surfaced but trimmed to the first 10
  // and counted, since dumping every wrong number is what made the old
  // output unreadable.
  let totalCorrect = 0;
  let totalWrong = 0;
  let groupsAccessed = 0;
  let groupsFailed = 0;

  // Per-group computed summary (used both for the message and the fix data)
  type Summary = {
    gr: typeof groupResults[number];
    correctPendingCount: number;
    correctMembersCount: number;
    notInVcfCount: number;
    wrongPending: string[];          // "+91XXXXXXXXXX" formatted
    wrongPendingFull: number;        // actual count (may exceed shown list)
    vcfLast10Set: Set<string>;
  };
  const summaries: Summary[] = [];

  for (const gr of groupResults) {
    if (gr.couldNotAccess) {
      groupsFailed++;
      summaries.push({
        gr,
        correctPendingCount: 0,
        correctMembersCount: 0,
        wrongPending: [],
        wrongPendingFull: 0,
        vcfLast10Set: new Set(),
      });
      continue;
    }
    groupsAccessed++;

    const inMembersSet = new Set(gr.inMembers.map(p => p.replace(/[^0-9]/g, "")));
    const inPendingSet = new Set(gr.inPending.map(p => p.replace(/[^0-9]/g, "")));
    const correctMembersCount = gr.vcfContacts.filter((c) => inMembersSet.has(c.phone.replace(/[^0-9]/g, ""))).length;
    const correctPendingCount = gr.vcfContacts.filter((c) => inPendingSet.has(c.phone.replace(/[^0-9]/g, ""))).length;

    const vcfLast10Set = new Set(gr.vcfContacts.map(c => c.phone.replace(/[^0-9]/g, "").slice(-10)));

    const wrongPending: string[] = [];
    for (const pendingPhone of gr.allPendingPhones) {
      const pLast10 = pendingPhone.slice(-10);
      if (pLast10.length >= 7 && !vcfLast10Set.has(pLast10)) {
        wrongPending.push("+" + pendingPhone);
      }
    }
    const wrongPendingFull = wrongPending.length;

    totalCorrect += correctPendingCount;
    totalWrong += wrongPendingFull;

    summaries.push({
      gr,
      correctPendingCount,
      correctMembersCount,
      wrongPending,
      wrongPendingFull,
      vcfLast10Set,
    });
  }

  // Headline summary
  let result = "📊 <b>CTC Check — Summary</b>\n";
  result += `📁 Groups: <b>${groupsAccessed}</b>${groupsFailed ? ` ❌ ${groupsFailed} failed` : ""}\n`;
  result += `✅ Correct Pending: <b>${totalCorrect}</b>\n`;
  result += `⚠️ Wrong Pending: <b>${totalWrong}</b>\n`;
  result += "━━━━━━━━━━━━━━━━━━\n\n";

  // Per-group block — kept short. Wrong pending phones limited to 10 lines.
  for (let i = 0; i < summaries.length; i++) {
    const s = summaries[i];
    const gr = s.gr;
    if (gr.couldNotAccess) {
      result += `❌ <b>Group ${i + 1}</b>: Could not access\n   ${esc(gr.link)}\n\n`;
      continue;
    }
    result += `📋 <b>${esc(gr.groupName)}</b>\n`;
    // Show the group invite link right under the title so the user can copy it.
    result += `   🔗 ${esc(gr.link)}\n`;
    // Show the unique VCF file name(s) supplied for this group. Usually just
    // one file per group, but we de-dupe in case the user sent multiple VCFs
    // and they all got attached to the same pair.
    const vcfNames = Array.from(new Set(gr.vcfContacts.map(c => c.vcfFileName).filter(Boolean)));
    if (vcfNames.length > 0) {
      for (const vn of vcfNames) result += `   📁 ${esc(vn)}\n`;
    }
    if (!gr.pendingAvailable) {
      result += `   ⚠️ <i>Pending detection off — need admin + "Approval required" ON</i>\n`;
    }
    result += `   ✅ Correct Pending: <b>${s.correctPendingCount}</b>`;
    if (s.correctMembersCount) result += `   👥 Already In: <b>${s.correctMembersCount}</b>`;
    result += "\n";
    if (s.wrongPendingFull > 0) {
      result += `   ⚠️ Wrong Pending: <b>${s.wrongPendingFull}</b>\n`;
      const SHOW = 10;
      const slice = s.wrongPending.slice(0, SHOW);
      for (const p of slice) result += `      • ${esc(p)}\n`;
      if (s.wrongPendingFull > SHOW) result += `      … +${s.wrongPendingFull - SHOW} more\n`;
    }
    result += "\n";
  }

  // Duplicate pending detection: contacts in pending of multiple groups
  const duplicates: Array<{ phone: string; groups: string[] }> = [];
  for (const [phone, groups] of pendingPhoneToGroups.entries()) {
    if (groups.length > 1) duplicates.push({ phone: "+" + phone, groups });
  }
  if (duplicates.length > 0) {
    result += `🔁 <b>Duplicate Pending (${duplicates.length}):</b>\n`;
    const SHOW = 8;
    // How many group names to print per phone before collapsing to "+N more".
    // Most duplicates are in 2 groups; cap at 3 so the message stays under
    // Telegram's 4096-char limit even when 8 duplicates each list groups.
    const NAMES_PER_PHONE = 3;
    const slice = duplicates.slice(0, SHOW);
    for (const d of slice) {
      result += `   • ${esc(d.phone)} — in <b>${d.groups.length}</b> groups:\n`;
      const namesShown = d.groups.slice(0, NAMES_PER_PHONE);
      for (const g of namesShown) result += `      ↳ ${esc(g)}\n`;
      if (d.groups.length > NAMES_PER_PHONE) {
        result += `      ↳ … +${d.groups.length - NAMES_PER_PHONE} more\n`;
      }
    }
    if (duplicates.length > SHOW) result += `   … +${duplicates.length - SHOW} more\n`;
    result += "\n";
  }

  // Stash fix data so the user can run "Fix Wrong Pending" right from the
  // result message. Only groups that we successfully accessed AND have at
  // least one wrong pending entry are eligible.
  const fixGroups = summaries
    .filter((s) => !s.gr.couldNotAccess && s.wrongPendingFull > 0)
    .map((s) => ({
      groupId: s.gr.groupId,
      groupName: s.gr.groupName,
      link: s.gr.link,
      vcfLast10Set: s.vcfLast10Set,
      wrongCount: s.wrongPendingFull,
    }));
  const uidNum = Number(userId);
  if (fixGroups.length > 0 && Number.isFinite(uidNum)) {
    ctcFixDataStore.set(uidNum, {
      groups: fixGroups,
      totalWrong,
      createdAt: Date.now(),
    });
  } else if (Number.isFinite(uidNum)) {
    ctcFixDataStore.delete(uidNum);
  }

  const finalKb = new InlineKeyboard();
  if (totalWrong > 0) {
    finalKb.text(`🛠 Fix Wrong Pending (${totalWrong})`, "ctc_fix_wrong").row();
  }
  finalKb.text("🏠 Main Menu", "main_menu");

  const chunks = splitMessage(result, 4000);
  try {
    await bot.api.editMessageText(chatId, msgId, chunks[0], {
      parse_mode: "HTML",
      reply_markup: chunks.length === 1 ? finalKb : undefined,
    });
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(chatId, chunks[i], {
      parse_mode: "HTML",
      reply_markup: i === chunks.length - 1 ? finalKb : undefined,
    });
  }
}

// ── Fix Wrong Pending ────────────────────────────────────────────────────
// Confirmation step: explain what will happen and wait for the user to tap
// "Yes, Cancel them". We don't want a single accidental tap to reject
// dozens of join requests with no second chance.
bot.callbackQuery("ctc_fix_wrong", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const data = ctcFixDataStore.get(userId);
  if (!data || !data.groups.length) {
    await ctx.editMessageText(
      "⚠️ <b>Fix data expired</b>\n\nPlease run the CTC check again.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
    return;
  }
  if (!isConnected(String(userId))) {
    await ctx.editMessageText(
      "❌ <b>WhatsApp not connected!</b>",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }
    );
    return;
  }
  const groupList = data.groups
    .slice(0, 8)
    .map((g) => `• ${esc(g.groupName)} — <b>${g.wrongCount}</b>`)
    .join("\n");
  const more = data.groups.length > 8 ? `\n… +${data.groups.length - 8} more groups` : "";
  await ctx.editMessageText(
    `🛠 <b>Fix Wrong Pending Requests</b>\n\n` +
    `Total: <b>${data.totalWrong}</b> wrong pending requests across <b>${data.groups.length}</b> group(s).\n\n` +
    `${groupList}${more}\n\n` +
    `<i>This will REJECT (cancel) every pending request whose number is NOT in your VCF for that group.</i>\n\n` +
    `Sure?`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Cancel them", "ctc_fix_wrong_confirm")
        .text("❌ No", "main_menu"),
    }
  );
});

bot.callbackQuery("ctc_fix_wrong_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const data = ctcFixDataStore.get(userId);
  if (!data || !data.groups.length) {
    await ctx.editMessageText(
      "⚠️ <b>Fix data expired</b>\n\nPlease run the CTC check again.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
    return;
  }
  if (!isConnected(String(userId))) {
    await ctx.editMessageText(
      "❌ <b>WhatsApp not connected!</b>",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }
    );
    return;
  }

  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  // Clear so the user can't double-trigger by re-tapping
  ctcFixDataStore.delete(userId);

  await ctx.editMessageText(
    `⏳ <b>Cancelling wrong pending requests...</b>`,
    { parse_mode: "HTML" }
  );

  let totalRejected = 0;
  let totalAttempted = 0;
  const perGroupReport: string[] = [];

  for (let i = 0; i < data.groups.length; i++) {
    const g = data.groups[i];
    try {
      await bot.api.editMessageText(chatId, msgId,
        `⏳ <b>Cancelling wrong pending...</b>\n\nGroup ${i + 1}/${data.groups.length}: <b>${esc(g.groupName)}</b>`,
        { parse_mode: "HTML" }
      );
    } catch {}

    // Re-fetch live pending list right before rejecting so we don't act on
    // stale data and accidentally reject someone who already got approved.
    let pending: Array<{ jid: string; phone: string }> = [];
    try {
      pending = await getGroupPendingRequestsDetailed(String(userId), g.groupId);
    } catch (err: any) {
      perGroupReport.push(`• ${esc(g.groupName)} — failed: ${esc(err?.message || "fetch error")}`);
      continue;
    }

    const wrongJids: string[] = [];
    for (const p of pending) {
      const last10 = p.phone.replace(/[^0-9]/g, "").slice(-10);
      // If we couldn't resolve a phone (rare, @lid edge case), skip — too
      // risky to reject without confirming the contact identity.
      if (!last10 || last10.length < 7) continue;
      if (!g.vcfLast10Set.has(last10)) wrongJids.push(p.jid);
    }

    if (!wrongJids.length) {
      perGroupReport.push(`• ${esc(g.groupName)} — nothing to reject`);
      continue;
    }

    totalAttempted += wrongJids.length;
    const rejected = await rejectGroupParticipantsBulk(String(userId), g.groupId, wrongJids);
    totalRejected += rejected;
    perGroupReport.push(`• ${esc(g.groupName)} — <b>${rejected}</b>/${wrongJids.length} cancelled`);
  }

  const finalText =
    `✅ <b>Wrong Pending Fixed</b>\n\n` +
    `Cancelled: <b>${totalRejected}</b> / ${totalAttempted}\n\n` +
    perGroupReport.join("\n");

  const chunks = splitMessage(finalText, 4000);
  try {
    await bot.api.editMessageText(chatId, msgId, chunks[0], {
      parse_mode: "HTML",
      reply_markup: chunks.length === 1
        ? new InlineKeyboard().text("🏠 Main Menu", "main_menu")
        : undefined,
    });
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(chatId, chunks[i], {
      parse_mode: "HTML",
      reply_markup: i === chunks.length - 1
        ? new InlineKeyboard().text("🏠 Main Menu", "main_menu")
        : undefined,
    });
  }
}); 

// ─── Get Link ────────────────────────────────────────────────────────────────

function detectSimilarGroups(groups: Array<{ id: string; subject: string }>): SimilarGroup[] {
  const map = new Map<string, Array<{ id: string; subject: string }>>();
  for (const g of groups) {
    const name = g.subject.trim();
    const match = name.match(/^(.*?)\s*\d+\s*$/);
    if (match && match[1].trim().length > 0) {
      const base = match[1].trim().toLowerCase();
      if (!map.has(base)) map.set(base, []);
      map.get(base)!.push(g);
    }
  }
  return Array.from(map.entries())
    .filter(([, items]) => items.length >= 2)
    .map(([, items]) => {
      const sorted = items.sort((a, b) => a.subject.localeCompare(b.subject, undefined, { numeric: true, sensitivity: "base" }));
      return { base: sorted[0].subject.replace(/\s*\d+\s*$/, "").trim(), groups: sorted };
    })
    .sort((a, b) => a.base.localeCompare(b.base));
}

bot.callbackQuery("get_link", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    }); return;
  }
  await ctx.editMessageText("🔍 <b>Scanning your WhatsApp groups...</b>\n\n⌛ Please wait...", { parse_mode: "HTML" });

  const groups = await getAllGroups(String(userId));
  if (!groups.length) {
    await ctx.editMessageText("📭 No groups found on your WhatsApp.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    }); return;
  }

  const adminGroups = groups.filter((g) => g.isAdmin);
  const allGroupsSimple = adminGroups.map((g) => ({ id: g.id, subject: g.subject })).sort((a, b) => a.subject.localeCompare(b.subject, undefined, { numeric: true, sensitivity: "base" }));
  const patterns = detectSimilarGroups(allGroupsSimple);

  userStates.set(userId, {
    step: "get_link_menu",
    similarData: { patterns, allGroups: allGroupsSimple },
  });

  const kb = new InlineKeyboard();
  if (patterns.length > 0) kb.text("🔗 Similar Groups", "gl_similar").text("📋 Get All Links", "gl_all").row();
  else kb.text("📋 Get All Links", "gl_all").row();
  kb.text("🏠 Main Menu", "main_menu");

  await ctx.editMessageText(
    `📱 <b>Admin Groups Found: ${adminGroups.length}</b> (Total: ${groups.length})\n\n` +
    (patterns.length > 0 ? `🔍 <b>Similar Patterns Detected: ${patterns.length}</b>\n` : "⚠️ No similar group patterns found.\n") +
    "\n📌 Choose an option:",
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery("gl_similar", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.similarData) return;

  const { patterns } = state.similarData;
  if (!patterns.length) {
    await ctx.editMessageText("⚠️ No similar group patterns found.", {
      reply_markup: new InlineKeyboard().text("🔙 Back", "get_link").text("🏠 Menu", "main_menu"),
    }); return;
  }

  const kb = new InlineKeyboard();
  for (let i = 0; i < patterns.length; i++) {
    const p = patterns[i];
    kb.text(`🔗 ${p.base} (${p.groups.length})`, `gl_sim_${i}`).row();
  }
  kb.text("🔙 Back", "get_link").text("🏠 Menu", "main_menu");

  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\n" +
    "Tap a pattern to get all its group links:",
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery(/^gl_sim_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.similarData) return;

  const idx = parseInt(ctx.match![1]);
  const pattern = state.similarData.patterns[idx];
  if (!pattern) return;

  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  await ctx.editMessageText(
    `⏳ <b>Fetching links for "${esc(pattern.base)}" groups...</b>\n\n📊 0/${pattern.groups.length} fetched...`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "gl_cancel_request") }
  );

  getLinkCancelRequests.delete(userId);
  void fetchGroupLinksBackground(String(userId), pattern.groups, chatId, msgId, "similar", pattern.base);
});

bot.callbackQuery("gl_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.similarData) return;

  const { allGroups } = state.similarData;
  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  await ctx.editMessageText(
    `⏳ <b>Fetching all group links...</b>\n\n📊 0/${allGroups.length} fetched...`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "gl_cancel_request") }
  );

  getLinkCancelRequests.delete(userId);
  void fetchGroupLinksBackground(String(userId), allGroups, chatId, msgId, "all");
});

bot.callbackQuery("gl_cancel_request", async (ctx) => {
  cancelDialogActiveFor.add(ctx.from.id);
  await ctx.answerCallbackQuery();
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard()
      .text("✅ Yes, Stop Fetch", "gl_cancel_confirm")
      .text("↩️ Continue", "gl_cancel_no"),
  });
});

bot.callbackQuery("gl_cancel_no", async (ctx) => {
  cancelDialogActiveFor.delete(ctx.from.id);
  await ctx.answerCallbackQuery({ text: "Fetching continued" });
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard().text("❌ Cancel", "gl_cancel_request"),
  });
});

bot.callbackQuery("gl_cancel_confirm", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Stopping after current group..." });
  getLinkCancelRequests.add(ctx.from.id);
  await ctx.editMessageReplyMarkup({ reply_markup: undefined });
});

const GL_BATCH_SIZE = 1;
const GL_BATCH_DELAY_MS = 800;
// After a fetch failure, wait a little longer before the next group so we
// don't pile up calls during a WhatsApp throttle window.
const GL_AFTER_FAIL_DELAY_MS = 2500;
// How long to wait before the manual retry pass — gives WA a brief cool-down.
// Was 5s; lowered to 1.5s because the user had to sit and wait staring at a
// blank "retrying..." screen before anything happened.
const GL_RETRY_PASS_PRE_DELAY_MS = 1500;
// Spacing between retries during the manual retry pass. Was 2s; lowered to
// 600ms — the same pacing the initial fetch uses on success.
const GL_RETRY_PASS_DELAY_MS = 600;
// Per-group cap for the retry pass. The initial fetch already burned the
// full 5-attempt budget on these groups; doing 5 more attempts each makes
// the retry feel completely frozen (5×30s = 2.5 min for just 5 groups).
// 2 quick attempts is plenty to catch a transient WA throttle window.
const GL_RETRY_PER_GROUP_ATTEMPTS = 2;
// How long we keep the per-user retry state in memory after the result
// is sent. After this window the "🔄 Retry" button becomes a no-op
// with a friendly "session expired" message.
const GL_RETRY_STATE_TTL_MS = 60 * 60 * 1000; // 1 hour

// Per-user state for the manual retry button. The user can press the
// retry button at most ONCE — we delete the entry as soon as the retry
// callback consumes it. We also auto-cleanup after TTL to bound memory.
type GetLinkRetryState = {
  results: Array<{ subject: string; link: string | null; groupId: string }>;
  mode: "all" | "similar";
  patternBase?: string;
  chatId: number;
  msgId: number;
  cleanupTimer: NodeJS.Timeout;
};
const getLinkRetryState = new Map<number, GetLinkRetryState>();

function clearGetLinkRetryState(userId: number): void {
  const s = getLinkRetryState.get(userId);
  if (s) {
    clearTimeout(s.cleanupTimer);
    getLinkRetryState.delete(userId);
  }
}

// Renders the final get-link result (success links + pending list) and
// posts it to the user. If `canRetry` is true AND there are failed
// groups, a "🔄 Retry Pending" button is added; the caller is
// responsible for storing the matching retry state in
// `getLinkRetryState` BEFORE invoking this with canRetry=true.
async function sendGetLinkResult(
  results: Array<{ subject: string; link: string | null; groupId: string }>,
  mode: "all" | "similar",
  patternBase: string | undefined,
  chatId: number,
  msgId: number,
  wasCancelled: boolean,
  canRetry: boolean,
): Promise<void> {
  const totalCount = results.length;
  const successCount = results.filter((r) => r.link).length;
  const failedResults = results
    .filter((r) => !r.link)
    .sort((a, b) => a.subject.localeCompare(b.subject, undefined, { numeric: true, sensitivity: "base" }));
  const successResults = results
    .filter((r) => r.link)
    .sort((a, b) => a.subject.localeCompare(b.subject, undefined, { numeric: true, sensitivity: "base" }));

  let result: string;
  if (mode === "similar") {
    result = `🔗 <b>"${esc(patternBase!)}" Pattern</b>\n`;
    result += `📊 <b>Total: ${totalCount} groups | ✅ ${successCount} links fetched</b>\n\n`;
  } else {
    result = `📋 <b>All Group Links</b>\n📊 <b>Total: ${totalCount} groups | ✅ ${successCount} links fetched</b>\n\n`;
  }
  if (wasCancelled) result += "⛔ <b>Fetch stopped by user.</b>\n\n";

  for (const r of successResults) {
    result += `📌 ${esc(r.subject)}\n${r.link}\n\n`;
  }

  if (failedResults.length) {
    result += "⚠️ <b>Links Not Fetched</b>\n";
    for (const r of failedResults) result += `• ${esc(r.subject)}\n`;
    if (canRetry && !wasCancelled) {
      result += `\n💡 <i>Tap below to retry the ${failedResults.length} pending link(s). You can retry only once.</i>`;
    }
  }

  // Build the action keyboard.
  const kb = new InlineKeyboard();
  if (canRetry && failedResults.length > 0 && !wasCancelled) {
    kb.text(`🔄 Retry ${failedResults.length} Pending`, "gl_retry_pending").row();
  }
  if (mode === "similar") {
    kb.text("🔙 Back", "gl_similar").text("🏠 Menu", "main_menu");
  } else {
    kb.text("🏠 Main Menu", "main_menu");
  }

  const chunks = splitMessage(result, 4000);
  try {
    await bot.api.editMessageText(chatId, msgId, chunks[0], {
      parse_mode: "HTML",
      link_preview_options: { is_disabled: true },
      reply_markup: chunks.length === 1 ? kb : undefined,
    });
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(chatId, chunks[i], {
      parse_mode: "HTML",
      link_preview_options: { is_disabled: true },
      reply_markup: i === chunks.length - 1 ? kb : undefined,
    });
  }
}

async function fetchGroupLinksBackground(
  userId: string,
  groups: Array<{ id: string; subject: string }>,
  chatId: number,
  msgId: number,
  mode: "all" | "similar",
  patternBase?: string
) {
  const results: Array<{ subject: string; link: string | null; groupId: string }> =
    groups.map(g => ({ subject: g.subject, link: null, groupId: g.id }));
  let fetchedCount = 0;
  let successCount = 0;
  let consecutiveFailures = 0;

  const updateProgress = async (extra?: string) => {
    // Skip if user is currently looking at the cancel-confirm dialog —
    // overwriting would wipe the Yes/No buttons and look like cancel failed.
    if (cancelDialogActiveFor.has(Number(userId))) return;
    try {
      const label = mode === "similar" ? `Fetching links for "${esc(patternBase!)}" groups` : "Fetching all group links";
      await bot.api.editMessageText(chatId, msgId,
        `⏳ <b>${label}...</b>\n\n📊 ${fetchedCount}/${groups.length} fetched | ✅ ${successCount} links found${extra ? `\n\n${extra}` : ""}`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "gl_cancel_request") }
      );
    } catch {}
  };

  // ── Single fetch pass: try each group once. We DO NOT auto-retry
  // failed groups anymore. Per user request, the result (with all
  // successful links) is sent immediately, and a "🔄 Retry Pending"
  // button is attached so the user can manually trigger the retry
  // for the failed ones — but only once. ──
  for (let i = 0; i < groups.length; i += GL_BATCH_SIZE) {
    if (getLinkCancelRequests.has(Number(userId))) break;
    const batch = groups.slice(i, i + GL_BATCH_SIZE);
    const batchResults = await Promise.allSettled(
      batch.map((g) => getGroupInviteLink(userId, g.id, 5))
    );

    let batchHadFailure = false;
    for (let j = 0; j < batch.length; j++) {
      const res = batchResults[j];
      const link = res.status === "fulfilled" ? res.value : null;
      results[i + j].link = link;
      fetchedCount++;
      if (link) {
        successCount++;
        consecutiveFailures = 0;
      } else {
        batchHadFailure = true;
        consecutiveFailures++;
      }
    }

    await updateProgress();
    if (i + GL_BATCH_SIZE < groups.length) {
      // Adaptive backpressure: if WA is throttling (3+ consecutive fails)
      // back off harder so we don't drown the connection.
      let delay = batchHadFailure ? GL_AFTER_FAIL_DELAY_MS : GL_BATCH_DELAY_MS;
      if (consecutiveFailures >= 3) delay = Math.max(delay, 5000);
      await new Promise((r) => setTimeout(r, delay));
    }
  }

  const wasCancelled = getLinkCancelRequests.has(Number(userId));
  getLinkCancelRequests.delete(Number(userId));
  cancelDialogActiveFor.delete(Number(userId));

  // If there are pending (failed) links and the user didn't cancel,
  // store the retry state so the "🔄 Retry Pending" button has data
  // to operate on. The state is single-use (consumed by the retry
  // handler) and auto-expires after GL_RETRY_STATE_TTL_MS.
  const failedCount = results.filter((r) => !r.link).length;
  const numUserId = Number(userId);
  clearGetLinkRetryState(numUserId);
  let canRetry = false;
  if (failedCount > 0 && !wasCancelled) {
    const cleanupTimer = setTimeout(() => {
      getLinkRetryState.delete(numUserId);
    }, GL_RETRY_STATE_TTL_MS);
    getLinkRetryState.set(numUserId, {
      results,
      mode,
      patternBase,
      chatId,
      msgId,
      cleanupTimer,
    });
    canRetry = true;
  }

  await sendGetLinkResult(results, mode, patternBase, chatId, msgId, wasCancelled, canRetry);
}

// ── "🔄 Retry Pending" — manual single-use retry for failed links. ──
bot.callbackQuery("gl_retry_pending", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;

  // Consume the retry state immediately so a double-tap can't fire
  // the retry twice. If there's no state, it was already consumed
  // (or expired) — tell the user instead of silently doing nothing.
  const state = getLinkRetryState.get(userId);
  if (state) {
    clearTimeout(state.cleanupTimer);
    getLinkRetryState.delete(userId);
  }
  if (!state) {
    try {
      await ctx.editMessageReplyMarkup({
        reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
      });
    } catch {}
    try {
      await ctx.reply(
        "⚠️ <b>Retry session expired</b>\n\n" +
        "Aap ek hi baar retry kar sakte the, ya 1 hour ka window khatam ho gaya. " +
        "Naye se link fetch karne ke liye menu se Get Link dobara dabao.",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
    return;
  }

  const failedIndexes: number[] = [];
  for (let i = 0; i < state.results.length; i++) {
    if (!state.results[i].link) failedIndexes.push(i);
  }
  if (failedIndexes.length === 0) {
    // Nothing to retry — just resend the result with no retry button.
    await sendGetLinkResult(
      state.results, state.mode, state.patternBase,
      state.chatId, state.msgId, false, false,
    );
    return;
  }

  // Bail out early if WhatsApp isn't connected — retrying without a
  // socket would just produce another wave of failures.
  if (!isConnected(String(userId))) {
    try {
      await bot.api.editMessageText(state.chatId, state.msgId,
        "❌ <b>WhatsApp not connected</b>\n\n" +
        "Retry nahi ho sakta — pehle WhatsApp connect karo, phir Get Link dobara dabao.",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }
      );
    } catch {}
    return;
  }

  // Reset any stale cancel flag from a previous run so the retry
  // pass starts cleanly, and wire the cancel button onto progress.
  getLinkCancelRequests.delete(userId);
  const cancelKb = new InlineKeyboard().text("❌ Cancel", "gl_cancel_request");

  // Show a fresh progress message for the retry pass. We try to
  // edit the existing result message; if it's gone (deleted/too
  // old), send a new one and switch chatId/msgId to it for the
  // final result render.
  let workChatId = state.chatId;
  let workMsgId = state.msgId;
  const retryProgress = (k: number) =>
    `🔄 <b>Retrying pending link(s)...</b>\n\n` +
    `📊 ${k}/${failedIndexes.length} retried`;
  try {
    await bot.api.editMessageText(workChatId, workMsgId, retryProgress(0), {
      parse_mode: "HTML",
      reply_markup: cancelKb,
    });
  } catch {
    try {
      const fresh = await ctx.reply(retryProgress(0), {
        parse_mode: "HTML",
        reply_markup: cancelKb,
      });
      workChatId = fresh.chat.id;
      workMsgId = fresh.message_id;
    } catch {}
  }

  // Brief cool-down before retrying so WA's throttle window clears.
  // Honor cancel even during this initial wait.
  const preDelayStart = Date.now();
  while (Date.now() - preDelayStart < GL_RETRY_PASS_PRE_DELAY_MS) {
    if (getLinkCancelRequests.has(userId)) break;
    await new Promise((r) => setTimeout(r, 200));
  }

  let cancelled = false;
  for (let k = 0; k < failedIndexes.length; k++) {
    if (getLinkCancelRequests.has(userId)) { cancelled = true; break; }
    const idx = failedIndexes[k];
    try {
      // Use a tight per-group attempt cap during the retry pass —
      // the initial fetch already burned the full retry budget on
      // these groups, so doing 5 more attempts each makes the user
      // wait minutes for nothing. 2 quick attempts catches transient
      // WA throttle without making the UI feel frozen.
      const link = await getGroupInviteLink(
        String(userId),
        state.results[idx].groupId,
        GL_RETRY_PER_GROUP_ATTEMPTS,
      );
      if (link) state.results[idx].link = link;
    } catch {}
    try {
      await bot.api.editMessageText(workChatId, workMsgId, retryProgress(k + 1), {
        parse_mode: "HTML",
        reply_markup: cancelKb,
      });
    } catch {}
    if (k < failedIndexes.length - 1) {
      // Cancel-aware delay so a tap on Cancel breaks out of the
      // wait instead of forcing the user to sit through it.
      const waitStart = Date.now();
      while (Date.now() - waitStart < GL_RETRY_PASS_DELAY_MS) {
        if (getLinkCancelRequests.has(userId)) { cancelled = true; break; }
        await new Promise((r) => setTimeout(r, 100));
      }
      if (cancelled) break;
    }
  }

  // Always clear the cancel flag so the next /get_link run starts fresh.
  getLinkCancelRequests.delete(userId);

  // Send the final, combined result. canRetry=false so the retry
  // button is NOT shown again — single-use as requested. wasCancelled
  // is forwarded so the result message reflects the user's choice.
  await sendGetLinkResult(
    state.results, state.mode, state.patternBase,
    workChatId, workMsgId, cancelled, false,
  );
});

// ─── Help Button (from main menu) ────────────────────────────────────────────

bot.callbackQuery("help_button", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (await isBanned(userId)) return;

  const codeBlock =
    `🤖 WhatsApp Bot Manager — Help Guide\n\n` +
    `Use /help command to see the full detailed guide.\n\n` +
    `📋 Quick Feature List:\n\n` +
    `1. Create Groups — Create multiple WA groups at once\n` +
    `2. Join Groups — Join groups via invite links\n` +
    `3. CTC Checker — Check if contacts are in group or pending\n` +
    `4. Get Link — Get invite links for your groups\n` +
    `5. Leave Group — Leave selected groups\n` +
    `6. Remove Members — Remove members from groups\n` +
    `7. Make Admin — Promote members to admin\n` +
    `8. Approval — Approve/reject pending join requests\n` +
    `9. Get Pending List — View all pending join requests\n` +
    `10. Add Members — Add members to your groups\n` +
    `11. Edit Settings — Change group settings/permissions\n` +
    `12. Change Name — Rename your groups\n` +
    `13. Auto Chat ⭐ — Auto send messages to friends/groups\n` +
    `14. Auto Accepter — Auto-accept invite-link join requests\n\n` +
    `💬 Commands:\n` +
    `/start — Open main menu\n` +
    `/help  — Full detailed help guide\n\n` +
    `👤 Owner: ${OWNER_USERNAME}`;

  await ctx.reply(
    `<pre>${codeBlock}</pre>`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    }
  );
});

// ─── Auto Request Accepter ────────────────────────────────────────────────────

interface AutoAccepterJob {
  userId: number;
  groupIds: string[];
  groupNames: string[];
  durationMs: number;
  endsAt: number;
  chatId: number;
  statusMsgId: number;
  pollTimer: ReturnType<typeof setInterval>;
  endTimer: ReturnType<typeof setTimeout>;
  totalAccepted: number;
  seenJids: Set<string>;
}

const autoAccepterJobs: Map<number, AutoAccepterJob> = new Map();

async function runAutoAccepterPoll(job: AutoAccepterJob): Promise<void> {
  const { userId, groupIds, groupNames, chatId, statusMsgId } = job;
  const userIdStr = String(userId);
  let newCount = 0;

  for (let i = 0; i < groupIds.length; i++) {
    const groupId = groupIds[i];
    try {
      const jids = await getGroupPendingInviteLinkJoins(userIdStr, groupId);
      for (const jid of jids) {
        if (!job.seenJids.has(jid)) {
          const ok = await approveGroupParticipant(userIdStr, groupId, jid);
          if (ok) {
            job.seenJids.add(jid);
            job.totalAccepted++;
            newCount++;
          }
        }
      }
    } catch (err: any) {
      console.error(`[AutoAccepter][${userId}] Poll error for group ${groupNames[i]}:`, err?.message);
    }
  }

  const remaining = Math.max(0, job.endsAt - Date.now());
  const remainMins = Math.ceil(remaining / 60000);
  const statusLines = groupNames.slice(0, 5).map(n => `• ${esc(n)}`).join("\n");
  const moreText = groupNames.length > 5 ? `\n... +${groupNames.length - 5} more` : "";

  try {
    await bot.api.editMessageText(
      chatId,
      statusMsgId,
      `🛡️ <b>Auto Request Accepter — Running</b>\n\n` +
      `📋 <b>Groups (${groupNames.length}):</b>\n${statusLines}${moreText}\n\n` +
      `✅ <b>Total Accepted:</b> ${job.totalAccepted}\n` +
      (newCount > 0 ? `🆕 <b>Just Accepted:</b> ${newCount}\n` : "") +
      `⏰ <b>Time Remaining:</b> ~${remainMins} min\n\n` +
      `<i>Polls every 30 seconds. Only accepts invite-link joiners.</i>`,
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("⛔ Cancel", "ar_stop_job"),
      }
    );
  } catch {}
}

async function stopAutoAccepterJob(userId: number, reason: "done" | "cancelled"): Promise<void> {
  const job = autoAccepterJobs.get(userId);
  if (!job) return;

  clearInterval(job.pollTimer);
  clearTimeout(job.endTimer);
  autoAccepterJobs.delete(userId);

  const isDone = reason === "done";
  const msg = isDone
    ? `🛡️ <b>Auto Request Accepter — Finished</b>\n\n` +
      `✅ <b>Total Accepted:</b> ${job.totalAccepted}\n` +
      `⏱️ <b>Duration:</b> ${Math.round(job.durationMs / 60000)} min\n\n` +
      `<b>Time is up! The Auto Request Accepter has been stopped.</b>\n` +
      `Your selected groups will no longer auto-accept join requests.`
    : `⛔ <b>Auto Request Accepter — Cancelled</b>\n\n` +
      `✅ <b>Total Accepted:</b> ${job.totalAccepted}\n\n` +
      `You cancelled the Auto Request Accepter. No more requests will be auto-accepted.`;

  try {
    await bot.api.editMessageText(
      job.chatId,
      job.statusMsgId,
      msg,
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
      }
    );
  } catch {}

  // Also send a separate notification message
  if (isDone) {
    try {
      await bot.api.sendMessage(
        job.chatId,
        `🔔 <b>Notification: Auto Request Accepter Stopped</b>\n\n` +
        `The Auto Request Accepter has been turned off — your selected time duration has expired.\n\n` +
        `✅ <b>Total requests accepted:</b> ${job.totalAccepted}`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
  }
}

bot.callbackQuery("auto_accepter", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText(
      `❌ <b>WhatsApp not connected!</b>\n\nPlease connect WhatsApp first.`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }
    ); return;
  }

  // If already running, show status
  const existingJob = autoAccepterJobs.get(userId);
  if (existingJob) {
    const remaining = Math.max(0, existingJob.endsAt - Date.now());
    const remainMins = Math.ceil(remaining / 60000);
    await ctx.editMessageText(
      `🛡️ <b>Auto Request Accepter</b>\n\n` +
      `⚡ A job is already running!\n\n` +
      `✅ Accepted so far: <b>${existingJob.totalAccepted}</b>\n` +
      `⏰ Time remaining: <b>~${remainMins} min</b>\n\n` +
      `Stop the current job first to start a new one.`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Stop Current Job", "ar_stop_job").text("🏠 Menu", "main_menu") }
    ); return;
  }

  await ctx.editMessageText("🔍 <b>Scanning your WhatsApp groups...</b>\n\n⌛ Please wait...", { parse_mode: "HTML" });

  const groups = await getAllGroups(String(userId));
  if (!groups.length) {
    await ctx.editMessageText("📭 No groups found on your WhatsApp.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    }); return;
  }

  const adminGroups = groups.filter((g) => g.isAdmin);
  if (!adminGroups.length) {
    await ctx.editMessageText("❌ You are not an admin in any WhatsApp group.\n\nYou need to be admin to use this feature.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    }); return;
  }

  const allGroupsSimple = adminGroups
    .map((g) => ({ id: g.id, subject: g.subject }))
    .sort((a, b) => a.subject.localeCompare(b.subject, undefined, { numeric: true, sensitivity: "base" }));
  const patterns = detectSimilarGroups(allGroupsSimple);

  userStates.set(userId, {
    step: "ar_menu",
    arData: { patterns, allGroups: allGroupsSimple, selectedIndices: new Set(), page: 0 },
  });

  const kb = new InlineKeyboard();
  if (patterns.length > 0) kb.text("🔍 Similar Groups", "ar_similar").text("📋 All Groups", "ar_show_all").row();
  else kb.text("📋 All Groups", "ar_show_all").row();
  kb.text("🏠 Main Menu", "main_menu");

  await ctx.editMessageText(
    `🛡️ <b>Auto Request Accepter</b>\n\n` +
    `📱 <b>Admin Groups Found: ${adminGroups.length}</b>\n` +
    (patterns.length > 0 ? `🔍 <b>Similar Patterns: ${patterns.length}</b>\n` : `⚠️ No similar patterns found.\n`) +
    `\n📌 Select which groups to monitor:\n\n` +
    `<i>Bot will auto-accept all pending join requests in selected groups.</i>`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

function buildArKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const allGroups = state.arData!.allGroups;
  const selected = state.arData!.selectedIndices;
  const page = state.arData!.page || 0;
  const totalPages = Math.max(1, Math.ceil(allGroups.length / MA_PAGE_SIZE));
  const start = page * MA_PAGE_SIZE;
  const end = Math.min(start + MA_PAGE_SIZE, allGroups.length);

  for (let i = start; i < end; i++) {
    const g = allGroups[i];
    const label = selected.has(i) ? `✅ ${g.subject}` : `☐ ${g.subject}`;
    kb.text(label, `ar_tog_${i}`).row();
  }

  if (totalPages > 1) {
    const prev = page > 0 ? "⬅️ Prev" : " ";
    const next = page < totalPages - 1 ? "Next ➡️" : " ";
    kb.text(prev, "ar_prev_page").text(`📄 ${page + 1}/${totalPages}`, "ar_page_info").text(next, "ar_next_page").row();
  }

  if (allGroups.length > 1) {
    kb.text("☑️ Select All", "ar_select_all").text("🧹 Clear All", "ar_clear_all").row();
  }

  if (selected.size > 0) {
    kb.text(`▶️ Continue (${selected.size} selected)`, "ar_proceed").row();
  }

  kb.text("🔙 Back", "auto_accepter").text("🏠 Menu", "main_menu");
  return kb;
}

bot.callbackQuery("ar_similar", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.arData) return;

  const { patterns } = state.arData;
  if (!patterns.length) {
    await ctx.editMessageText("⚠️ No similar group patterns found.", {
      reply_markup: new InlineKeyboard().text("🔙 Back", "auto_accepter").text("🏠 Menu", "main_menu"),
    }); return;
  }

  const kb = new InlineKeyboard();
  for (let i = 0; i < patterns.length; i++) {
    kb.text(`📌 ${patterns[i].base} (${patterns[i].groups.length} groups)`, `ar_sim_${i}`).row();
  }
  kb.text("🔙 Back", "auto_accepter").text("🏠 Menu", "main_menu");

  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select those groups:",
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery(/^ar_sim_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.arData) return;

  const idx = parseInt(ctx.match![1]);
  const pattern = state.arData.patterns[idx];
  if (!pattern) return;

  const patternIds = new Set(pattern.groups.map((g) => g.id));
  state.arData.selectedIndices = new Set();
  for (let i = 0; i < state.arData.allGroups.length; i++) {
    if (patternIds.has(state.arData.allGroups[i].id)) state.arData.selectedIndices.add(i);
  }
  state.step = "ar_select";
  state.arData.page = 0;

  await ctx.editMessageText(
    `🛡️ <b>Auto Request Accepter</b>\n\n📱 <b>${state.arData.allGroups.length} admin group(s)</b>\n\nSelect groups to monitor:\n<i>${state.arData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildArKeyboard(state) }
  );
});

bot.callbackQuery("ar_show_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.arData) return;
  state.step = "ar_select";
  state.arData.page = 0;
  await ctx.editMessageText(
    `🛡️ <b>Auto Request Accepter</b>\n\n📱 <b>${state.arData.allGroups.length} admin group(s)</b>\n\nSelect groups to monitor:\n<i>Tap to select/deselect</i>`,
    { parse_mode: "HTML", reply_markup: buildArKeyboard(state) }
  );
});

bot.callbackQuery(/^ar_tog_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.arData) return;
  const idx = parseInt(ctx.match![1]);
  if (idx < 0 || idx >= state.arData.allGroups.length) return;
  if (state.arData.selectedIndices.has(idx)) state.arData.selectedIndices.delete(idx);
  else state.arData.selectedIndices.add(idx);
  const cnt = state.arData.selectedIndices.size;
  await ctx.editMessageText(
    `🛡️ <b>Auto Request Accepter</b>\n\n📱 <b>${state.arData.allGroups.length} admin group(s)</b>\n\nSelect groups to monitor:\n<i>${cnt > 0 ? `${cnt} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildArKeyboard(state) }
  );
});

bot.callbackQuery("ar_prev_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.arData) return;
  if ((state.arData.page || 0) > 0) state.arData.page--;
  const cnt = state.arData.selectedIndices.size;
  await ctx.editMessageText(
    `🛡️ <b>Auto Request Accepter</b>\n\n📱 <b>${state.arData.allGroups.length} admin group(s)</b>\n\nSelect groups:\n<i>${cnt > 0 ? `${cnt} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildArKeyboard(state) }
  );
});

bot.callbackQuery("ar_next_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.arData) return;
  const totalPages = Math.ceil(state.arData.allGroups.length / MA_PAGE_SIZE);
  if ((state.arData.page || 0) < totalPages - 1) state.arData.page++;
  const cnt = state.arData.selectedIndices.size;
  await ctx.editMessageText(
    `🛡️ <b>Auto Request Accepter</b>\n\n📱 <b>${state.arData.allGroups.length} admin group(s)</b>\n\nSelect groups:\n<i>${cnt > 0 ? `${cnt} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildArKeyboard(state) }
  );
});

bot.callbackQuery("ar_page_info", async (ctx) => { await ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" }); });

bot.callbackQuery("ar_select_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.arData) return;
  for (let i = 0; i < state.arData.allGroups.length; i++) state.arData.selectedIndices.add(i);
  await ctx.editMessageText(
    `🛡️ <b>Auto Request Accepter</b>\n\nAll <b>${state.arData.allGroups.length} groups selected</b>`,
    { parse_mode: "HTML", reply_markup: buildArKeyboard(state) }
  );
});

bot.callbackQuery("ar_clear_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.arData) return;
  state.arData.selectedIndices.clear();
  await ctx.editMessageText(
    `🛡️ <b>Auto Request Accepter</b>\n\n📱 <b>${state.arData.allGroups.length} admin group(s)</b>\n\nSelect groups:\n<i>None selected yet</i>`,
    { parse_mode: "HTML", reply_markup: buildArKeyboard(state) }
  );
});

bot.callbackQuery("ar_proceed", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.arData || !state.arData.selectedIndices.size) return;

  const selectedGroups = Array.from(state.arData.selectedIndices).map((i) => state.arData!.allGroups[i]);
  (state as any).arGroups = selectedGroups;
  state.step = "ar_time_select";

  const previewGroups = selectedGroups.slice(0, 8).map((g) => `• ${esc(g.subject)}`).join("\n");
  const moreText = selectedGroups.length > 8 ? `\n... +${selectedGroups.length - 8} more` : "";

  const kb = new InlineKeyboard()
    .text("⏱️ 15 min", "ar_time_15").text("⏱️ 30 min", "ar_time_30").row()
    .text("⏱️ 1 hour", "ar_time_60").text("⏱️ 2 hours", "ar_time_120").row()
    .text("🔙 Back", "auto_accepter").text("🏠 Menu", "main_menu");

  await ctx.editMessageText(
    `🛡️ <b>Auto Request Accepter</b>\n\n` +
    `📋 <b>Selected Groups (${selectedGroups.length}):</b>\n${previewGroups}${moreText}\n\n` +
    `⏰ <b>How long should it run?</b>`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery(/^ar_time_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  const arGroups: Array<{ id: string; subject: string }> = (state as any)?.arGroups;
  if (!arGroups || !arGroups.length) { await ctx.answerCallbackQuery({ text: "Session expired. Please try again.", show_alert: true }); return; }

  const minutes = parseInt(ctx.match![1]);
  const durationMs = minutes * 60 * 1000;
  const durationLabel = minutes < 60 ? `${minutes} min` : `${minutes / 60} hour${minutes / 60 > 1 ? "s" : ""}`;

  (state as any).arDurationMs = durationMs;
  (state as any).arDurationLabel = durationLabel;

  const previewGroups = arGroups.slice(0, 8).map((g) => `• ${esc(g.subject)}`).join("\n");
  const moreText = arGroups.length > 8 ? `\n... +${arGroups.length - 8} more` : "";

  const kb = new InlineKeyboard()
    .text("✅ Start Auto Accepter", "ar_confirm").row()
    .text("❌ Cancel", "main_menu");

  await ctx.editMessageText(
    `🛡️ <b>Auto Request Accepter — Review</b>\n\n` +
    `📋 <b>Groups to Monitor (${arGroups.length}):</b>\n${previewGroups}${moreText}\n\n` +
    `⏱️ <b>Duration:</b> ${durationLabel}\n\n` +
    `ℹ️ <b>What will happen:</b>\n` +
    `• Bot polls every 30 seconds\n` +
    `• Only users who joined via invite link will be accepted\n` +
    `• Admin-added pending requests will NOT be accepted\n` +
    `• You will get a notification when time is up\n\n` +
    `Tap <b>Start</b> to begin or <b>Cancel</b> to go back.`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery("ar_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  const arGroups: Array<{ id: string; subject: string }> = (state as any)?.arGroups;
  const durationMs: number = (state as any)?.arDurationMs;
  if (!arGroups || !durationMs) { await ctx.answerCallbackQuery({ text: "Session expired. Please try again.", show_alert: true }); return; }
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ WhatsApp not connected!", {
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    }); return;
  }

  // Check if already running
  if (autoAccepterJobs.has(userId)) {
    await ctx.answerCallbackQuery({ text: "A job is already running! Stop it first.", show_alert: true }); return;
  }

  userStates.delete(userId);

  const groupIds = arGroups.map((g) => g.id);
  const groupNames = arGroups.map((g) => g.subject);
  const durationLabel = durationMs < 3600000 ? `${durationMs / 60000} min` : `${durationMs / 3600000} hour${durationMs / 3600000 > 1 ? "s" : ""}`;
  const endsAt = Date.now() + durationMs;
  const chatId = ctx.chat!.id;

  await ctx.editMessageText(
    `🛡️ <b>Auto Request Accepter — Starting...</b>\n\n` +
    `📋 <b>Groups (${groupNames.length}):</b>\n` +
    groupNames.slice(0, 5).map((n) => `• ${esc(n)}`).join("\n") +
    (groupNames.length > 5 ? `\n... +${groupNames.length - 5} more` : "") +
    `\n\n⏱️ Duration: ${durationLabel}\n\n⌛ Starting first poll...`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "ar_stop_job") }
  );

  const statusMsgId = ctx.callbackQuery.message!.message_id;

  const job: AutoAccepterJob = {
    userId,
    groupIds,
    groupNames,
    durationMs,
    endsAt,
    chatId,
    statusMsgId,
    totalAccepted: 0,
    seenJids: new Set(),
    pollTimer: null as any,
    endTimer: null as any,
  };

  autoAccepterJobs.set(userId, job);

  // Start polling every 30 seconds
  job.pollTimer = setInterval(() => {
    void runAutoAccepterPoll(job);
  }, 30_000);

  // End timer
  job.endTimer = setTimeout(() => {
    void stopAutoAccepterJob(userId, "done");
  }, durationMs);

  // Run immediately on start
  void runAutoAccepterPoll(job);
});

bot.callbackQuery("ar_stop_job", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Stopping Auto Request Accepter..." });
  const userId = ctx.from.id;
  if (!autoAccepterJobs.has(userId)) {
    try {
      await ctx.editMessageText(
        `⚠️ No Auto Request Accepter is currently running.`,
        { reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
    return;
  }
  await stopAutoAccepterJob(userId, "cancelled");
});

// ─── Leave Group ─────────────────────────────────────────────────────────────

const leaveJobCancel = new Set<number>();
const LV_PAGE_SIZE = 20;

function buildLeaveKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const allGroups = state.leaveData!.groups;
  const selected = state.leaveData!.selectedIndices!;
  const page = state.leaveData!.page || 0;
  const totalPages = Math.max(1, Math.ceil(allGroups.length / LV_PAGE_SIZE));
  const start = page * LV_PAGE_SIZE;
  const end = Math.min(start + LV_PAGE_SIZE, allGroups.length);

  for (let i = start; i < end; i++) {
    const g = allGroups[i];
    const label = selected.has(i) ? `✅ ${g.subject}` : `☐ ${g.subject}`;
    kb.text(label, `lv_tog_${i}`).row();
  }

  if (totalPages > 1) {
    const prev = page > 0 ? "⬅️ Prev" : " ";
    const next = page < totalPages - 1 ? "Next ➡️" : " ";
    kb.text(prev, "lv_prev_page").text(`📄 ${page + 1}/${totalPages}`, "lv_page_info").text(next, "lv_next_page").row();
  }

  if (allGroups.length > 1) {
    kb.text("☑️ Select All", "lv_select_all").text("🧹 Clear All", "lv_clear_all").row();
  }

  if (selected.size > 0) {
    kb.text(`▶️ Continue (${selected.size} selected)`, "lv_proceed").row();
  }

  kb.text("🔙 Back", "leave_group").text("🏠 Menu", "main_menu");
  return kb;
}

bot.callbackQuery("leave_group", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    }); return;
  }

  await ctx.editMessageText("🔍 <b>Scanning groups...</b>", { parse_mode: "HTML" });
  const allGroups = await getAllGroups(String(userId));
  if (!allGroups.length) {
    await ctx.editMessageText("📭 No groups found.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    }); return;
  }

  const simple = allGroups.map((g) => ({ id: g.id, subject: g.subject }));
  const patterns = detectSimilarGroups(simple);

  userStates.set(userId, {
    step: "lv_menu",
    leaveData: {
      groups: allGroups.map((g) => ({ id: g.id, subject: g.subject, isAdmin: g.isAdmin })),
      mode: "all",
      patterns,
      selectedIndices: new Set(),
      page: 0,
    },
  });

  const kb = new InlineKeyboard();
  if (patterns.length > 0) kb.text("🔍 Similar Groups", "lv_similar").text("📋 All Groups", "lv_show_all").row();
  else kb.text("📋 All Groups", "lv_show_all").row();
  kb.text("🏠 Main Menu", "main_menu");

  await ctx.editMessageText(
    `🚪 <b>Select Groups to Leave</b>\n\n` +
    `📊 Found <b>${allGroups.length}</b> groups\n` +
    (patterns.length > 0 ? `🔍 <b>${patterns.length}</b> similar patterns detected\n` : "") +
    `\nChoose how to select groups:`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery("lv_similar", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.leaveData?.patterns) return;

  const { patterns } = state.leaveData;
  if (!patterns.length) {
    await ctx.editMessageText("⚠️ No similar group patterns found.", {
      reply_markup: new InlineKeyboard().text("🔙 Back", "leave_group").text("🏠 Menu", "main_menu"),
    }); return;
  }

  const kb = new InlineKeyboard();
  for (let i = 0; i < patterns.length; i++) {
    kb.text(`📌 ${patterns[i].base} (${patterns[i].groups.length} groups)`, `lv_sim_${i}`).row();
  }
  kb.text("🔙 Back", "leave_group").text("🏠 Menu", "main_menu");

  await ctx.editMessageText("🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select those groups:", {
    parse_mode: "HTML", reply_markup: kb,
  });
});

bot.callbackQuery(/^lv_sim_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.leaveData?.patterns) return;

  const idx = parseInt(ctx.match![1]);
  const pattern = state.leaveData.patterns![idx];
  if (!pattern) return;

  const patternIds = new Set(pattern.groups.map((g) => g.id));
  state.leaveData.selectedIndices = new Set();
  for (let i = 0; i < state.leaveData.groups.length; i++) {
    if (patternIds.has(state.leaveData.groups[i].id)) state.leaveData.selectedIndices.add(i);
  }
  state.step = "lv_select";
  state.leaveData.page = 0;

  const cnt = state.leaveData.selectedIndices.size;
  await ctx.editMessageText(
    `🚪 <b>Select Groups to Leave</b>\n\n📊 <b>${state.leaveData.groups.length}</b> groups total\n\nTap to select/deselect:\n<i>${cnt} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildLeaveKeyboard(state) }
  );
});

bot.callbackQuery("lv_show_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.leaveData) return;
  state.step = "lv_select";
  state.leaveData.page = 0;
  await ctx.editMessageText(
    `🚪 <b>Select Groups to Leave</b>\n\n📊 <b>${state.leaveData.groups.length}</b> groups total\n\nTap to select/deselect:\n<i>None selected yet</i>`,
    { parse_mode: "HTML", reply_markup: buildLeaveKeyboard(state) }
  );
});

bot.callbackQuery(/^lv_tog_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.leaveData?.selectedIndices) return;
  const idx = parseInt(ctx.match![1]);
  if (idx < 0 || idx >= state.leaveData.groups.length) return;
  if (state.leaveData.selectedIndices.has(idx)) state.leaveData.selectedIndices.delete(idx);
  else state.leaveData.selectedIndices.add(idx);
  const cnt = state.leaveData.selectedIndices.size;
  await ctx.editMessageText(
    `🚪 <b>Select Groups to Leave</b>\n\n📊 <b>${state.leaveData.groups.length}</b> groups total\n\nTap to select/deselect:\n<i>${cnt > 0 ? `${cnt} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildLeaveKeyboard(state) }
  );
});

bot.callbackQuery("lv_prev_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.leaveData) return;
  if ((state.leaveData.page || 0) > 0) state.leaveData.page!--;
  const cnt = state.leaveData.selectedIndices?.size || 0;
  await ctx.editMessageText(
    `🚪 <b>Select Groups to Leave</b>\n\n📊 <b>${state.leaveData.groups.length}</b> groups total\n\nTap to select/deselect:\n<i>${cnt > 0 ? `${cnt} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildLeaveKeyboard(state) }
  );
});

bot.callbackQuery("lv_next_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.leaveData) return;
  const totalPages = Math.ceil(state.leaveData.groups.length / LV_PAGE_SIZE);
  if ((state.leaveData.page || 0) < totalPages - 1) state.leaveData.page!++;
  const cnt = state.leaveData.selectedIndices?.size || 0;
  await ctx.editMessageText(
    `🚪 <b>Select Groups to Leave</b>\n\n📊 <b>${state.leaveData.groups.length}</b> groups total\n\nTap to select/deselect:\n<i>${cnt > 0 ? `${cnt} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildLeaveKeyboard(state) }
  );
});

bot.callbackQuery("lv_page_info", async (ctx) => { await ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" }); });

bot.callbackQuery("lv_select_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.leaveData?.selectedIndices) return;
  for (let i = 0; i < state.leaveData.groups.length; i++) state.leaveData.selectedIndices.add(i);
  await ctx.editMessageText(
    `🚪 <b>Select Groups to Leave</b>\n\nAll <b>${state.leaveData.groups.length} groups selected</b>`,
    { parse_mode: "HTML", reply_markup: buildLeaveKeyboard(state) }
  );
});

bot.callbackQuery("lv_clear_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.leaveData?.selectedIndices) return;
  state.leaveData.selectedIndices.clear();
  await ctx.editMessageText(
    `🚪 <b>Select Groups to Leave</b>\n\n📊 <b>${state.leaveData.groups.length}</b> groups total\n\nTap to select/deselect:\n<i>None selected yet</i>`,
    { parse_mode: "HTML", reply_markup: buildLeaveKeyboard(state) }
  );
});

bot.callbackQuery("lv_proceed", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.leaveData?.selectedIndices?.size) return;

  const selectedGroups = Array.from(state.leaveData.selectedIndices)
    .map((i) => state.leaveData!.groups[i]);
  state.leaveData.selectedGroups = selectedGroups;

  let text = `🚪 <b>Leave Groups — Confirm</b>\n\n`;
  text += `📊 <b>${selectedGroups.length} group(s) will be left:</b>\n\n`;
  for (const g of selectedGroups) text += `• ${esc(g.subject)} ${g.isAdmin ? "👑" : "👤"}\n`;
  text += `\n⚠️ <b>Are you sure you want to leave these groups?</b>`;

  const chunks = splitMessage(text, 4000);
  for (let i = 0; i < chunks.length; i++) {
    const isLast = i === chunks.length - 1;
    const kb = isLast
      ? new InlineKeyboard().text("✅ Yes, Leave", "lv_confirm").text("❌ Cancel", "leave_group")
      : undefined;
    if (i === 0) await ctx.editMessageText(chunks[i], { parse_mode: "HTML", reply_markup: kb });
    else await ctx.reply(chunks[i], { parse_mode: "HTML", reply_markup: kb });
  }
});

bot.callbackQuery("lv_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.leaveData?.selectedGroups?.length) return;
  const groups = state.leaveData.selectedGroups;

  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  userStates.delete(userId);
  leaveJobCancel.delete(userId);

  await ctx.editMessageText(
    `⏳ <b>Leaving ${groups.length} group(s)...</b>\n\n🔄 0/${groups.length} done...`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "lv_cancel") }
  );

  void (async () => {
    const lines: string[] = [];
    let success = 0, failed = 0, cancelled = false;
    for (let li = 0; li < groups.length; li++) {
      if (leaveJobCancel.has(userId)) { cancelled = true; break; }
      const g = groups[li];
      const ok = await leaveGroup(String(userId), g.id);
      if (ok) { lines.push(`✅ Left: ${esc(g.subject)}`); success++; }
      else { lines.push(`❌ Failed: ${esc(g.subject)}`); failed++; }
      try {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Leaving: ${li + 1}/${groups.length}</b>\n\n${lines.join("\n")}`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "lv_cancel") }
        );
      } catch {}
      if (li < groups.length - 1) await new Promise((r) => setTimeout(r, 1000));
    }
    leaveJobCancel.delete(userId);
    const summary = cancelled
      ? `\n\n⛔ <b>Cancelled! ✅ ${success} left | ❌ ${failed} failed</b>`
      : `\n\n📊 <b>Done! ✅ ${success} left | ❌ ${failed} failed</b>`;
    const result = `🚪 <b>Leave Groups Result</b>\n\n${lines.join("\n")}${summary}`;
    const chunks = splitMessage(result, 4000);
    try {
      await bot.api.editMessageText(chatId, msgId, chunks[0], {
        parse_mode: "HTML",
        reply_markup: chunks.length === 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
      });
    } catch {}
    for (let i = 1; i < chunks.length; i++) {
      await bot.api.sendMessage(chatId, chunks[i], {
        parse_mode: "HTML",
        reply_markup: i === chunks.length - 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
      });
    }
  })();
});

bot.callbackQuery("lv_cancel", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "⛔ Cancelling...", show_alert: false });
  leaveJobCancel.add(ctx.from.id);
});

// ─── Remove Members ──────────────────────────────────────────────────────────

const RM_PAGE_SIZE = 20;

function buildRemoveMembersKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const allGroups = state.removeData!.allGroups;
  const selected = state.removeData!.selectedIndices;
  const page = state.removeData!.page || 0;
  const totalPages = Math.ceil(allGroups.length / RM_PAGE_SIZE);
  const start = page * RM_PAGE_SIZE;
  const end = Math.min(start + RM_PAGE_SIZE, allGroups.length);

  for (let i = start; i < end; i++) {
    const g = allGroups[i];
    const isSelected = selected.has(i);
    const label = isSelected ? `✅ ${g.subject}` : `☐ ${g.subject}`;
    kb.text(label, `rm_tog_${i}`).row();
  }

  if (totalPages > 1) {
    if (page > 0) kb.text("⬅️ Previous", "rm_page_prev");
    kb.text(`📄 ${page + 1}/${totalPages}`, "rm_page_info");
    if (page < totalPages - 1) kb.text("➡️ Next", "rm_page_next");
    kb.row();
  }

  if (allGroups.length > 1) {
    kb.text("🗑️ Remove from ALL Groups", "rm_select_all").row();
  }

  if (selected.size > 0) {
    kb.text(`▶️ Continue (${selected.size} selected)`, "rm_proceed").row();
  }

  kb.text("🏠 Back", "main_menu");
  return kb;
}

bot.callbackQuery("remove_members", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    }); return;
  }
  await ctx.editMessageText("🔍 <b>Scanning your admin groups...</b>", { parse_mode: "HTML" });
  const allGroups = await getAllGroups(String(userId));
  const adminGroups = allGroups.filter((g) => g.isAdmin);

  if (!adminGroups.length) {
    await ctx.editMessageText("📭 You are not an admin in any group.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    }); return;
  }

  userStates.set(userId, {
    step: "remove_members_select",
    removeData: {
      allGroups: adminGroups.map((g) => ({ id: g.id, subject: g.subject })),
      selectedIndices: new Set(),
      page: 0,
    },
  });

  const state = userStates.get(userId)!;
  await ctx.editMessageText(
    `🗑️ <b>Remove Members</b>\n\n👑 <b>${adminGroups.length} admin group(s) found</b>\n\nSelect the group(s) from which you want to remove members:\n<i>Tap to select/deselect</i>`,
    { parse_mode: "HTML", reply_markup: buildRemoveMembersKeyboard(state) }
  );
});

bot.callbackQuery(/^rm_tog_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeData) return;

  const idx = parseInt(ctx.match![1]);
  if (idx < 0 || idx >= state.removeData.allGroups.length) return;

  if (state.removeData.selectedIndices.has(idx)) {
    state.removeData.selectedIndices.delete(idx);
  } else {
    state.removeData.selectedIndices.add(idx);
  }

  const selectedCount = state.removeData.selectedIndices.size;
  await ctx.editMessageText(
    `🗑️ <b>Remove Members</b>\n\n👑 <b>${state.removeData.allGroups.length} admin group(s)</b>\n\nSelect group(s) to remove members from:\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildRemoveMembersKeyboard(state) }
  );
});

bot.callbackQuery("rm_select_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeData) return;

  // Select all groups
  for (let i = 0; i < state.removeData.allGroups.length; i++) {
    state.removeData.selectedIndices.add(i);
  }

  await ctx.editMessageText(
    `🗑️ <b>Remove Members</b>\n\n👑 All <b>${state.removeData.allGroups.length} groups selected</b>\n\nSelect group(s) to remove members from:`,
    { parse_mode: "HTML", reply_markup: buildRemoveMembersKeyboard(state) }
  );
});

bot.callbackQuery("rm_page_prev", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeData) return;
  if (state.removeData.page > 0) state.removeData.page--;
  const selectedCount = state.removeData.selectedIndices.size;
  await ctx.editMessageText(
    `🗑️ <b>Remove Members</b>\n\n👑 <b>${state.removeData.allGroups.length} admin group(s)</b>\n\nSelect group(s) to remove members from:\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildRemoveMembersKeyboard(state) }
  );
});

bot.callbackQuery("rm_page_next", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeData) return;
  const totalPages = Math.ceil(state.removeData.allGroups.length / RM_PAGE_SIZE);
  if (state.removeData.page < totalPages - 1) state.removeData.page++;
  const selectedCount = state.removeData.selectedIndices.size;
  await ctx.editMessageText(
    `🗑️ <b>Remove Members</b>\n\n👑 <b>${state.removeData.allGroups.length} admin group(s)</b>\n\nSelect group(s) to remove members from:\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildRemoveMembersKeyboard(state) }
  );
});

bot.callbackQuery("rm_page_info", async (ctx) => {
  await ctx.answerCallbackQuery();
});

bot.callbackQuery("rm_proceed", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeData || state.removeData.selectedIndices.size === 0) return;

  const selectedGroups = Array.from(state.removeData.selectedIndices).map(i => state.removeData!.allGroups[i]);

  // Move to exclude numbers step
  userStates.set(userId, {
    step: "remove_exclude_numbers",
    removeExcludeData: {
      selectedGroups,
      excludeNumbers: new Set(),
      excludePrefixes: new Set(),
    },
  });

  const groupList = selectedGroups.map(g => `• ${esc(g.subject)}`).join("\n");
  await ctx.editMessageText(
    `✅ <b>${selectedGroups.length} group(s) selected:</b>\n\n${groupList}\n\n` +
    `📱 <b>Exclude Numbers</b>\n\n` +
    `🛡️ <b>Admins hamesha safe rahenge</b> — unhe kabhi remove nahi karta, chahe aap exclude karo ya na karo.\n\n` +
    `Aap do tarah se exclude kar sakte ho (ek per line, dono mix bhi kar sakte ho):\n\n` +
    `1️⃣ <b>Pura number</b> — sirf wahi number exclude hoga.\n` +
    `   Example:\n   <code>+919912345678\n   +919998887777</code>\n\n` +
    `2️⃣ <b>Sirf country code</b> (1-4 digits, + optional) — uss country ke <i>saare</i> numbers exclude honge.\n` +
    `   Example:\n   <code>+91\n   +92</code>\n   (India aur Pakistan ke saare numbers safe rahenge)\n\n` +
    `Agar kuch bhi exclude nahi karna to <b>Skip</b> dabao:`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("⏭️ Skip", "rm_skip_exclude")
        .text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("rm_skip_exclude", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeExcludeData) return;

  await startRemoveMembersProcess(ctx, userId, state.removeExcludeData.selectedGroups, new Set(), new Set());
});

bot.callbackQuery("rm_cancel_request", async (ctx) => {
  await ctx.answerCallbackQuery();
  cancelDialogActiveFor.add(ctx.from.id);
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard()
      .text("✅ Yes, Stop Removing", "rm_cancel_confirm")
      .text("↩️ Continue", "rm_cancel_no"),
  });
});

bot.callbackQuery("rm_cancel_no", async (ctx) => {
  cancelDialogActiveFor.delete(ctx.from.id);
  await ctx.answerCallbackQuery({ text: "Removing continued" });
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard().text("❌ Cancel", "rm_cancel_request"),
  });
});

bot.callbackQuery("rm_cancel_confirm", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Stopping after current member..." });
  removeMembersCancelRequests.add(ctx.from.id);
  // Keep the dialog flag on; it gets cleared in the background task's
  // finally cleanup so the in-flight progress edit can't pop the
  // "❌ Cancel" button back.
  await ctx.editMessageReplyMarkup({ reply_markup: undefined });
});

async function startRemoveMembersProcess(
  ctx: any,
  userId: number,
  selectedGroups: Array<{ id: string; subject: string }>,
  excludeNumbers: Set<string>,
  excludePrefixes: Set<string>
) {
  const chatId = ctx.callbackQuery?.message?.chat.id || ctx.chat?.id;
  const msgId = ctx.callbackQuery?.message?.message_id;

  userStates.delete(userId);

  const excludeList = Array.from(excludeNumbers).map(n => n.replace(/[^0-9]/g, ""));
  const prefixList = Array.from(excludePrefixes).map(p => p.replace(/[^0-9]/g, ""));

  const groupList = selectedGroups.map(g => `• ${esc(g.subject)}`).join("\n");
  const excludeBits: string[] = [];
  if (excludeList.length > 0) excludeBits.push(`🚫 <b>Excluding ${excludeList.length} number(s)</b>`);
  if (prefixList.length > 0) excludeBits.push(`🌐 <b>Excluding country code(s):</b> ${prefixList.map(p => "+" + p).join(", ")}`);
  const excludeText = excludeBits.length > 0 ? "\n" + excludeBits.join("\n") : "";

  const statusText = `⏳ <b>Removing members from ${selectedGroups.length} group(s)...</b>\n\n${groupList}${excludeText}\n\n⌛ Please wait...`;

  try {
    if (msgId) {
      await ctx.editMessageText(statusText, {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("❌ Cancel", "rm_cancel_request"),
      });
    } else {
      await ctx.reply(statusText, {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("❌ Cancel", "rm_cancel_request"),
      });
    }
  } catch {}

  removeMembersCancelRequests.delete(userId);
  void removeAllGroupMembersBackground(String(userId), selectedGroups, excludeList, prefixList, chatId, msgId);
}

async function removeAllGroupMembersBackground(
  userId: string,
  groups: Array<{ id: string; subject: string }>,
  excludeNumbers: string[],
  excludePrefixes: string[],
  chatId: number,
  msgId: number | undefined
) {
  let fullResult = "🗑️ <b>Remove Members Result</b>\n\n";
  const excludeSet = new Set(excludeNumbers);
  // Pre-compute the digit-only prefix list once (already stripped, but be safe).
  const prefixDigitsList = excludePrefixes
    .map(p => p.replace(/[^0-9]/g, ""))
    .filter(p => p.length >= 1 && p.length <= 4);

  for (let gi = 0; gi < groups.length; gi++) {
    const group = groups[gi];

    try {
      if (msgId) {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Processing group ${gi + 1}/${groups.length}:</b>\n${esc(group.subject)}\n\n⌛ Fetching members...`,
          { parse_mode: "HTML" }
        );
      }
    } catch {}

    if (removeMembersCancelRequests.has(Number(userId))) break;

    const participants = await getGroupParticipants(userId, group.id);

    // Build last-10-digit set of excluded numbers for robust matching
    const excludeLast10Set = new Set<string>();
    for (const excl of excludeSet) {
      const digits = excl.replace(/[^0-9]/g, "");
      if (digits.length >= 7) excludeLast10Set.add(digits.slice(-10));
    }

    const nonAdmins = participants.filter((p) => {
      if (p.isAdmin) return false;
      // Use p.phone (populated even in LID mode) for reliable number comparison
      const pNum = (p.phone || p.jid.replace(/:\d+@/, "@").split("@")[0]).replace(/[^0-9]/g, "");
      const pLast10 = pNum.slice(-10);
      if (pLast10.length >= 7 && excludeLast10Set.has(pLast10)) return false;
      // Country-code prefix exclusion: if the participant's full number
      // starts with any excluded prefix, skip them. pNum is digits only
      // (no leading +), e.g. "919912345678" — so prefix "91" matches.
      if (prefixDigitsList.length > 0) {
        for (const pref of prefixDigitsList) {
          if (pNum.startsWith(pref)) return false;
        }
      }
      return true;
    });

    if (!nonAdmins.length) {
      fullResult += `📋 <b>${esc(group.subject)}</b>\n`;
      fullResult += `✅ No members to remove (all are admins or excluded)\n\n`;
      continue;
    }

    let removed = 0, failed = 0;
    let cancelledEarly = false;
    for (let pi = 0; pi < nonAdmins.length; pi++) {
      if (removeMembersCancelRequests.has(Number(userId))) { cancelledEarly = true; break; }
      const p = nonAdmins[pi];
      const ok = await removeGroupParticipant(userId, group.id, p.jid);
      if (ok) removed++;
      else failed++;

      // Update progress every 5 removals
      if (pi % 5 === 0 || pi === nonAdmins.length - 1) {
        // Skip overwrite if user is staring at the cancel-confirm dialog.
        if (msgId && !cancelDialogActiveFor.has(Number(userId))) {
          try {
            await bot.api.editMessageText(chatId, msgId,
              `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n🗑️ Removing: ${pi + 1}/${nonAdmins.length}\n✅ Removed: ${removed} | ❌ Failed: ${failed}`,
              { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "rm_cancel_request") }
            );
          } catch {}
        }
      }

      await new Promise((r) => setTimeout(r, 500));
    }

    fullResult += `📋 <b>${esc(group.subject)}</b>\n`;
    fullResult += `🗑️ Removed: ${removed} | ❌ Failed: ${failed}\n\n`;
    if (cancelledEarly) break;
  }

  const wasCancelled = removeMembersCancelRequests.has(Number(userId));
  removeMembersCancelRequests.delete(Number(userId));
  cancelDialogActiveFor.delete(Number(userId));

  if (wasCancelled) fullResult += `⛔ <b>Stopped by user.</b>\n\n`;
  fullResult += `━━━━━━━━━━━━━━━━━━\n✅ <b>Done processing group(s)!</b>`;

  const chunks = splitMessage(fullResult, 4000);
  try {
    if (msgId) {
      await bot.api.editMessageText(chatId, msgId, chunks[0], {
        parse_mode: "HTML",
        reply_markup: chunks.length === 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
      });
    } else {
      await bot.api.sendMessage(chatId, chunks[0], {
        parse_mode: "HTML",
        reply_markup: chunks.length === 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
      });
    }
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(chatId, chunks[i], {
      parse_mode: "HTML",
      reply_markup: i === chunks.length - 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
    });
  }
}

// ─── Make Admin ──────────────────────────────────────────────────────────────

function buildMakeAdminKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const allGroups = state.makeAdminData!.allGroups;
  const selected = state.makeAdminData!.selectedIndices;
  const page = state.makeAdminData!.page || 0;
  const totalPages = Math.max(1, Math.ceil(allGroups.length / MA_PAGE_SIZE));
  const start = page * MA_PAGE_SIZE;
  const end = Math.min(start + MA_PAGE_SIZE, allGroups.length);

  for (let i = start; i < end; i++) {
    const g = allGroups[i];
    const isSelected = selected.has(i);
    const label = isSelected ? `✅ ${g.subject}` : `☐ ${g.subject}`;
    kb.text(label, `ma_tog_${i}`).row();
  }

  if (totalPages > 1) {
    const prev = page > 0 ? "⬅️ Prev" : " ";
    const next = page < totalPages - 1 ? "Next ➡️" : " ";
    kb.text(prev, "ma_prev_page").text(`📄 ${page + 1}/${totalPages}`, "ma_page_info").text(next, "ma_next_page").row();
  }

  if (allGroups.length > 1) {
    kb.text("☑️ Select All", "ma_select_all").text("🧹 Clear All", "ma_clear_all").row();
  }

  if (selected.size > 0) {
    kb.text(`▶️ Continue (${selected.size} selected)`, "ma_proceed").row();
  }

  kb.text("🔙 Back", "make_admin").text("🏠 Menu", "main_menu");
  return kb;
}

bot.callbackQuery("make_admin", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    }); return;
  }
  await ctx.editMessageText("🔍 <b>Scanning your admin groups...</b>", { parse_mode: "HTML" });
  const allGroups = await getAllGroups(String(userId));
  const adminGroups = allGroups.filter((g) => g.isAdmin);

  if (!adminGroups.length) {
    await ctx.editMessageText("📭 You are not an admin in any group.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    }); return;
  }

  const adminGroupsSimple = adminGroups.map((g) => ({ id: g.id, subject: g.subject }));
  const patterns = detectSimilarGroups(adminGroupsSimple);

  userStates.set(userId, {
    step: "make_admin_menu",
    makeAdminData: {
      allGroups: adminGroupsSimple,
      patterns,
      selectedIndices: new Set(),
      page: 0,
    },
  });

  const kb = new InlineKeyboard();
  if (patterns.length > 0) kb.text("🔍 Similar Groups", "ma_similar").text("📋 All Groups", "ma_show_all").row();
  else kb.text("📋 All Groups", "ma_show_all").row();
  kb.text("🏠 Main Menu", "main_menu");

  await ctx.editMessageText(
    `👑 <b>Make Admin</b>\n\n` +
    `📊 Admin Groups: ${adminGroups.length} (Total: ${allGroups.length})\n` +
    (patterns.length > 0 ? `🔍 Similar Patterns: ${patterns.length}\n` : "") +
    `\n📌 Choose an option:`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery("ma_similar", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.makeAdminData) return;

  const { patterns } = state.makeAdminData;
  if (!patterns.length) {
    await ctx.editMessageText("⚠️ No similar group patterns found.", {
      reply_markup: new InlineKeyboard().text("🔙 Back", "make_admin").text("🏠 Menu", "main_menu"),
    }); return;
  }

  const kb = new InlineKeyboard();
  for (let i = 0; i < patterns.length; i++) {
    const p = patterns[i];
    kb.text(`📌 ${p.base} (${p.groups.length} groups)`, `ma_sim_${i}`).row();
  }
  kb.text("🔙 Back", "make_admin").text("🏠 Menu", "main_menu");

  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select those groups:",
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery(/^ma_sim_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.makeAdminData) return;

  const idx = parseInt(ctx.match![1]);
  const pattern = state.makeAdminData.patterns[idx];
  if (!pattern) return;

  const patternIds = new Set(pattern.groups.map(g => g.id));
  state.makeAdminData.selectedIndices = new Set();
  for (let i = 0; i < state.makeAdminData.allGroups.length; i++) {
    if (patternIds.has(state.makeAdminData.allGroups[i].id)) {
      state.makeAdminData.selectedIndices.add(i);
    }
  }

  state.step = "make_admin_select";
  state.makeAdminData.page = 0;
  await ctx.editMessageText(
    `👑 <b>Make Admin</b>\n\n👑 <b>${state.makeAdminData.allGroups.length} admin group(s)</b>\n\nSelect group(s):\n<i>${state.makeAdminData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildMakeAdminKeyboard(state) }
  );
});

bot.callbackQuery("ma_show_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.makeAdminData) return;

  state.step = "make_admin_select";
  state.makeAdminData.page = 0;
  await ctx.editMessageText(
    `👑 <b>Make Admin</b>\n\n👑 <b>${state.makeAdminData.allGroups.length} admin group(s)</b>\n\nSelect group(s) in which to make admin:\n<i>Tap to select/deselect</i>`,
    { parse_mode: "HTML", reply_markup: buildMakeAdminKeyboard(state) }
  );
});

bot.callbackQuery(/^ma_tog_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.makeAdminData) return;

  const idx = parseInt(ctx.match![1]);
  if (idx < 0 || idx >= state.makeAdminData.allGroups.length) return;

  if (state.makeAdminData.selectedIndices.has(idx)) {
    state.makeAdminData.selectedIndices.delete(idx);
  } else {
    state.makeAdminData.selectedIndices.add(idx);
  }

  const selectedCount = state.makeAdminData.selectedIndices.size;
  await ctx.editMessageText(
    `👑 <b>Make Admin</b>\n\n👑 <b>${state.makeAdminData.allGroups.length} admin group(s)</b>\n\nSelect group(s):\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildMakeAdminKeyboard(state) }
  );
});

bot.callbackQuery("ma_prev_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.makeAdminData) return;
  if ((state.makeAdminData.page || 0) > 0) state.makeAdminData.page = (state.makeAdminData.page || 0) - 1;
  const selectedCount = state.makeAdminData.selectedIndices.size;
  await ctx.editMessageText(
    `👑 <b>Make Admin</b>\n\n👑 <b>${state.makeAdminData.allGroups.length} admin group(s)</b>\n\nSelect group(s):\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildMakeAdminKeyboard(state) }
  );
});

bot.callbackQuery("ma_next_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.makeAdminData) return;
  const totalPages = Math.ceil(state.makeAdminData.allGroups.length / MA_PAGE_SIZE);
  if ((state.makeAdminData.page || 0) < totalPages - 1) state.makeAdminData.page = (state.makeAdminData.page || 0) + 1;
  const selectedCount = state.makeAdminData.selectedIndices.size;
  await ctx.editMessageText(
    `👑 <b>Make Admin</b>\n\n👑 <b>${state.makeAdminData.allGroups.length} admin group(s)</b>\n\nSelect group(s):\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildMakeAdminKeyboard(state) }
  );
});

bot.callbackQuery("ma_page_info", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" });
});

bot.callbackQuery("ma_select_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.makeAdminData) return;

  for (let i = 0; i < state.makeAdminData.allGroups.length; i++) {
    state.makeAdminData.selectedIndices.add(i);
  }

  await ctx.editMessageText(
    `👑 <b>Make Admin</b>\n\nAll <b>${state.makeAdminData.allGroups.length} groups selected</b>`,
    { parse_mode: "HTML", reply_markup: buildMakeAdminKeyboard(state) }
  );
});

bot.callbackQuery("ma_clear_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.makeAdminData) return;

  state.makeAdminData.selectedIndices.clear();

  await ctx.editMessageText(
    `👑 <b>Make Admin</b>\n\n👑 <b>${state.makeAdminData.allGroups.length} admin group(s)</b>\n\nSelect group(s):\n<i>None selected yet</i>`,
    { parse_mode: "HTML", reply_markup: buildMakeAdminKeyboard(state) }
  );
});

bot.callbackQuery("ma_proceed", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.makeAdminData || state.makeAdminData.selectedIndices.size === 0) return;

  state.step = "make_admin_enter_numbers";
  const selectedGroups = Array.from(state.makeAdminData.selectedIndices).map(i => state.makeAdminData!.allGroups[i]);
  const groupList = selectedGroups.slice(0, 60).map(g => `• ${esc(g.subject)}`).join("\n");
  const moreText = selectedGroups.length > 60 ? `\n... +${selectedGroups.length - 60} more group(s)` : "";

  await ctx.editMessageText(
    `✅ <b>${selectedGroups.length} group(s) selected:</b>\n\n${groupList}${moreText}\n\n` +
    `📱 <b>Send phone number(s)</b>\n\n` +
    `Send the phone numbers (with country code) of people you want to make admin, one per line:\n\n` +
    `Example:\n<code>+919912345678\n+919998887777</code>`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

// ─── Approval ────────────────────────────────────────────────────────────────

function buildApprovalKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const allGroups = state.approvalData!.allGroups;
  const selected = state.approvalData!.selectedIndices;
  const page = state.approvalData!.page || 0;
  const totalPages = Math.max(1, Math.ceil(allGroups.length / AP_PAGE_SIZE));
  const start = page * AP_PAGE_SIZE;
  const end = Math.min(start + AP_PAGE_SIZE, allGroups.length);

  for (let i = start; i < end; i++) {
    const g = allGroups[i];
    const isSelected = selected.has(i);
    const label = isSelected ? `✅ ${g.subject}` : `☐ ${g.subject}`;
    kb.text(label, `ap_tog_${i}`).row();
  }

  if (totalPages > 1) {
    const prev = page > 0 ? "⬅️ Previous 20" : " ";
    const next = page < totalPages - 1 ? "Next 20 ➡️" : " ";
    kb.text(prev, "ap_prev_page").text(`📄 ${page + 1}/${totalPages}`, "ap_page_info").text(next, "ap_next_page").row();
  }

  if (allGroups.length > 1) {
    kb.text("☑️ Select All", "ap_select_all").row();
  }

  if (selected.size > 0) {
    kb.text(`▶️ Continue (${selected.size} selected)`, "ap_proceed").row();
  }

  kb.text("🏠 Back", "main_menu");
  return kb;
}

bot.callbackQuery("approval", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    }); return;
  }
  await ctx.editMessageText("🔍 <b>Scanning your admin groups...</b>", { parse_mode: "HTML" });
  const allGroups = await getAllGroups(String(userId));
  const adminGroups = allGroups.filter((g) => g.isAdmin);

  if (!adminGroups.length) {
    await ctx.editMessageText("📭 You are not an admin in any group.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    }); return;
  }

  const adminGroupsSimple = adminGroups.map((g) => ({ id: g.id, subject: g.subject }));
  const patterns = detectSimilarGroups(adminGroupsSimple);

  userStates.set(userId, {
    step: "approval_menu",
    approvalData: {
      allGroups: adminGroupsSimple,
      patterns,
      selectedIndices: new Set(),
      page: 0,
    },
  });

  const kb = new InlineKeyboard();
  if (patterns.length > 0) kb.text("🔍 Similar Groups", "ap_similar").text("📋 All Groups", "ap_show_all").row();
  else kb.text("📋 All Groups", "ap_show_all").row();
  kb.text("🏠 Main Menu", "main_menu");

  await ctx.editMessageText(
    `✅ <b>Approval</b>\n\n` +
    `📊 Admin Groups: ${adminGroups.length} (Total: ${allGroups.length})\n` +
    (patterns.length > 0 ? `🔍 Similar Patterns: ${patterns.length}\n` : "") +
    `\n📌 Choose an option:`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery("ap_similar", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData) return;

  const { patterns } = state.approvalData;
  if (!patterns.length) {
    await ctx.editMessageText("⚠️ No similar group patterns found.", {
      reply_markup: new InlineKeyboard().text("🔙 Back", "approval").text("🏠 Menu", "main_menu"),
    }); return;
  }

  const kb = new InlineKeyboard();
  for (let i = 0; i < patterns.length; i++) {
    const p = patterns[i];
    kb.text(`📌 ${p.base} (${p.groups.length} groups)`, `ap_sim_${i}`).row();
  }
  kb.text("🔙 Back", "approval").text("🏠 Menu", "main_menu");

  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select those groups:",
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery(/^ap_sim_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData) return;

  const idx = parseInt(ctx.match![1]);
  const pattern = state.approvalData.patterns[idx];
  if (!pattern) return;

  const patternIds = new Set(pattern.groups.map(g => g.id));
  state.approvalData.selectedIndices = new Set();
  for (let i = 0; i < state.approvalData.allGroups.length; i++) {
    if (patternIds.has(state.approvalData.allGroups[i].id)) {
      state.approvalData.selectedIndices.add(i);
    }
  }

  state.step = "approval_select";
  state.approvalData.page = 0;
  await ctx.editMessageText(
    `✅ <b>Approval</b>\n\n👑 <b>${state.approvalData.allGroups.length} admin group(s)</b>\n\nSelect group(s):\n<i>${state.approvalData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildApprovalKeyboard(state) }
  );
});

bot.callbackQuery("ap_show_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData) return;

  state.step = "approval_select";
  state.approvalData.page = 0;
  await ctx.editMessageText(
    `✅ <b>Approval</b>\n\n👑 <b>${state.approvalData.allGroups.length} admin group(s)</b>\n\nSelect group(s) to approve pending members:\n<i>Tap to select/deselect</i>`,
    { parse_mode: "HTML", reply_markup: buildApprovalKeyboard(state) }
  );
});

bot.callbackQuery(/^ap_tog_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData) return;

  const idx = parseInt(ctx.match![1]);
  if (idx < 0 || idx >= state.approvalData.allGroups.length) return;

  if (state.approvalData.selectedIndices.has(idx)) {
    state.approvalData.selectedIndices.delete(idx);
  } else {
    state.approvalData.selectedIndices.add(idx);
  }

  const selectedCount = state.approvalData.selectedIndices.size;
  await ctx.editMessageText(
    `✅ <b>Approval</b>\n\n👑 <b>${state.approvalData.allGroups.length} admin group(s)</b>\n\nSelect group(s):\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildApprovalKeyboard(state) }
  );
});

bot.callbackQuery("ap_prev_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData) return;
  if ((state.approvalData.page || 0) > 0) state.approvalData.page = (state.approvalData.page || 0) - 1;
  const selectedCount = state.approvalData.selectedIndices.size;
  await ctx.editMessageText(
    `✅ <b>Approval</b>\n\n👑 <b>${state.approvalData.allGroups.length} admin group(s)</b>\n\nSelect group(s):\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildApprovalKeyboard(state) }
  );
});

bot.callbackQuery("ap_next_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData) return;
  const totalPages = Math.ceil(state.approvalData.allGroups.length / AP_PAGE_SIZE);
  if ((state.approvalData.page || 0) < totalPages - 1) state.approvalData.page = (state.approvalData.page || 0) + 1;
  const selectedCount = state.approvalData.selectedIndices.size;
  await ctx.editMessageText(
    `✅ <b>Approval</b>\n\n👑 <b>${state.approvalData.allGroups.length} admin group(s)</b>\n\nSelect group(s):\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildApprovalKeyboard(state) }
  );
});

bot.callbackQuery("ap_page_info", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Previous/Next se 20 group per page dekhein" });
});

bot.callbackQuery("ap_select_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData) return;

  for (let i = 0; i < state.approvalData.allGroups.length; i++) {
    state.approvalData.selectedIndices.add(i);
  }

  await ctx.editMessageText(
    `✅ <b>Approval</b>\n\nAll <b>${state.approvalData.allGroups.length} groups selected</b>`,
    { parse_mode: "HTML", reply_markup: buildApprovalKeyboard(state) }
  );
});

bot.callbackQuery("ap_proceed", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData || state.approvalData.selectedIndices.size === 0) return;

  const selectedGroups = Array.from(state.approvalData.selectedIndices).map(i => state.approvalData!.allGroups[i]);
  const preview = selectedGroups.slice(0, 30).map(g => `• ${esc(g.subject)}`).join("\n");
  const moreText = selectedGroups.length > 30 ? `\n... +${selectedGroups.length - 30} more group(s)` : "";

  await ctx.editMessageText(
    `✅ <b>${selectedGroups.length} group(s) selected:</b>\n\n${preview}${moreText}\n\n` +
    `📌 <b>Choose approval type:</b>\n\n` +
    `• <b>👥 All Approval</b> — Approve every pending member in the selected groups (1 by 1 or all together)\n` +
    `• <b>👑 Admin Approval</b> — Approve only specific numbers (from a VCF or a list) and optionally also make them admin`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("👥 All Approval", "ap_type_all")
        .text("👑 Admin Approval", "ap_type_admin")
        .row()
        .text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("ap_type_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData || state.approvalData.selectedIndices.size === 0) return;
  state.approvalData.mode = "all";

  const selectedGroups = Array.from(state.approvalData.selectedIndices).map(i => state.approvalData!.allGroups[i]);
  const preview = selectedGroups.slice(0, 30).map(g => `• ${esc(g.subject)}`).join("\n");
  const moreText = selectedGroups.length > 30 ? `\n... +${selectedGroups.length - 30} more group(s)` : "";

  await ctx.editMessageText(
    `👥 <b>All Approval — ${selectedGroups.length} group(s):</b>\n\n${preview}${moreText}\n\n` +
    `📌 <b>Choose approval method:</b>\n\n` +
    `• <b>Approve 1 by 1</b> — Approve each pending member one at a time\n` +
    `• <b>Approve Together</b> — Turn off approval setting, then turn it back on to approve all at once`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("☝️ Approve 1 by 1", "ap_one_by_one")
        .text("👥 Approve Together", "ap_together")
        .row()
        .text("🔙 Back", "ap_proceed").text("❌ Cancel", "main_menu"),
    }
  );
});

// ─── Admin Approval (specific numbers, optional make-admin) ──────────────────
bot.callbackQuery("ap_type_admin", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData || state.approvalData.selectedIndices.size === 0) return;

  state.approvalData.mode = "admin_specific";
  state.approvalData.targetPhones = undefined;
  state.approvalData.makeAdminAfter = undefined;
  state.step = "approval_admin_input";

  const selectedGroups = Array.from(state.approvalData.selectedIndices).map(i => state.approvalData!.allGroups[i]);
  const preview = selectedGroups.slice(0, 30).map(g => `• ${esc(g.subject)}`).join("\n");
  const moreText = selectedGroups.length > 30 ? `\n... +${selectedGroups.length - 30} more group(s)` : "";

  await ctx.editMessageText(
    `👑 <b>Admin Approval — ${selectedGroups.length} group(s):</b>\n\n${preview}${moreText}\n\n` +
    `📁 <b>Send a VCF file</b> OR <b>send phone numbers</b> (one per line, with country code).\n\n` +
    `Only these numbers will be approved across the selected groups.\n\n` +
    `Example:\n<code>+919912345678\n+919998887777</code>`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🔙 Back", "ap_proceed").text("❌ Cancel", "main_menu"),
    }
  );
});

async function showAdminApprovalChoice(ctx: any, userId: number) {
  const state = userStates.get(userId);
  if (!state?.approvalData?.targetPhones?.length) return;
  state.step = "approval_admin_choice";
  const phones = state.approvalData.targetPhones;
  const phonePreview = phones.slice(0, 10).map(p => `• +${p}`).join("\n");
  const phoneMore = phones.length > 10 ? `\n... +${phones.length - 10} more` : "";

  await ctx.reply(
    `✅ <b>${phones.length} number(s) received</b>\n\n${phonePreview}${phoneMore}\n\n` +
    `📌 <b>After approval, what should I do?</b>\n\n` +
    `• <b>Approve only</b> — Just approve these numbers in the selected groups\n` +
    `• <b>Approve + Make Admin</b> — Approve them, then also promote them to admin in those groups`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Approve only", "ap_admin_no_make")
        .text("👑 Approve + Make Admin", "ap_admin_make")
        .row()
        .text("❌ Cancel", "main_menu"),
    }
  );
}

bot.callbackQuery("ap_admin_no_make", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData?.targetPhones?.length) return;
  state.approvalData.makeAdminAfter = false;
  await showAdminApprovalReview(ctx, userId);
});

bot.callbackQuery("ap_admin_make", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData?.targetPhones?.length) return;
  state.approvalData.makeAdminAfter = true;
  await showAdminApprovalReview(ctx, userId);
});

async function showAdminApprovalReview(ctx: any, userId: number) {
  const state = userStates.get(userId);
  if (!state?.approvalData?.targetPhones?.length) return;
  state.step = "approval_admin_review";

  const selectedGroups = Array.from(state.approvalData.selectedIndices).map(i => state.approvalData!.allGroups[i]);
  const groupPreview = selectedGroups.slice(0, 20).map(g => `• ${esc(g.subject)}`).join("\n");
  const groupMore = selectedGroups.length > 20 ? `\n... +${selectedGroups.length - 20} more group(s)` : "";

  const phones = state.approvalData.targetPhones;
  const phonePreview = phones.slice(0, 15).map(p => `• +${p}`).join("\n");
  const phoneMore = phones.length > 15 ? `\n... +${phones.length - 15} more` : "";

  const actionLine = state.approvalData.makeAdminAfter
    ? "✅ Approve <b>and</b> 👑 make admin"
    : "✅ Approve only";

  await ctx.editMessageText(
    `📋 <b>Review — Admin Approval</b>\n\n` +
    `<b>Groups (${selectedGroups.length}):</b>\n${groupPreview}${groupMore}\n\n` +
    `<b>Numbers (${phones.length}):</b>\n${phonePreview}${phoneMore}\n\n` +
    `<b>Action:</b> ${actionLine}\n\n` +
    `Tap <b>Confirm</b> to start.`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Confirm & Start", "ap_admin_confirm")
        .text("❌ Cancel", "main_menu"),
    }
  );
}

bot.callbackQuery("ap_admin_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData?.targetPhones?.length) return;

  const selectedGroups = Array.from(state.approvalData.selectedIndices).map(i => state.approvalData!.allGroups[i]);
  const phones = state.approvalData.targetPhones.slice();
  const makeAdminAfter = !!state.approvalData.makeAdminAfter;

  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  userStates.delete(userId);
  await ctx.editMessageText(
    `⏳ <b>${makeAdminAfter ? "Approving + making admin" : "Approving"} ${phones.length} number(s) in ${selectedGroups.length} group(s)...</b>\n\n⌛ Please wait...`,
    { parse_mode: "HTML" }
  );
  void approveAdminSpecificBackground(String(userId), selectedGroups, phones, makeAdminAfter, chatId, msgId);
});

async function approveAdminSpecificBackground(
  userId: string,
  groups: Array<{ id: string; subject: string }>,
  phones: string[],
  makeAdminAfter: boolean,
  chatId: number,
  msgId: number,
) {
  const normalizedTargets = new Set(phones.map(p => p.replace(/[^0-9]/g, "")));
  const titleLabel = makeAdminAfter ? "✅ Admin Approval (Approve + Make Admin) Result" : "✅ Admin Approval (Approve only) Result";
  let fullResult = `<b>${titleLabel}</b>\n\n`;
  const lines: string[] = [];

  for (let gi = 0; gi < groups.length; gi++) {
    const group = groups[gi];

    try {
      await bot.api.editMessageText(chatId, msgId,
        `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n⌛ Fetching pending list...`,
        { parse_mode: "HTML" }
      );
    } catch {}

    const pending = await getGroupPendingRequestsDetailed(userId, group.id);
    // Build a last-10-digits index so we tolerate missing/extra country codes on either side.
    const targetByLast10 = new Map<string, string>();
    for (const t of normalizedTargets) {
      const last10 = t.slice(-10);
      if (last10.length >= 7) targetByLast10.set(last10, t);
    }
    const matched: Array<{ jid: string; phone: string }> = [];
    const matchedTargets = new Set<string>();
    for (const p of pending) {
      const phone = (p.phone || "").replace(/[^0-9]/g, "");
      if (!phone) continue; // LID without resolvable phone — skip (will be reported below)
      let hitTarget = "";
      if (normalizedTargets.has(phone)) hitTarget = phone;
      else {
        const last10 = phone.slice(-10);
        if (last10.length >= 7 && targetByLast10.has(last10)) hitTarget = targetByLast10.get(last10)!;
      }
      if (hitTarget) {
        matched.push({ jid: p.jid, phone });
        matchedTargets.add(hitTarget);
      }
    }
    const notFound = Array.from(normalizedTargets).filter(p => !matchedTargets.has(p));
    const unresolvedLidCount = pending.filter(p => !p.phone && p.jid.endsWith("@lid")).length;

    const groupLines: string[] = [];
    let approved = 0, approveFailed = 0;

    if (matched.length === 0) {
      groupLines.push(`  ⚠️ None of the supplied numbers were in this group's pending list`);
    } else {
      for (let mi = 0; mi < matched.length; mi++) {
        const { jid, phone } = matched[mi];
        const ok = await approveGroupParticipant(userId, group.id, jid);
        if (ok) {
          approved++;
          groupLines.push(`  ✅ +${phone} — Approved`);
        } else {
          approveFailed++;
          groupLines.push(`  ❌ +${phone} — Approval failed`);
        }
        if (mi % 3 === 0 || mi === matched.length - 1) {
          try {
            await bot.api.editMessageText(chatId, msgId,
              `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n` +
              `Approving: ${mi + 1}/${matched.length}\n` +
              `✅ Approved: ${approved} | ❌ Failed: ${approveFailed}`,
              { parse_mode: "HTML" }
            );
          } catch {}
        }
        await new Promise((r) => setTimeout(r, 1000));
      }
    }

    for (const np of notFound) {
      groupLines.push(`  ⚠️ +${np} — Not in pending list`);
    }
    if (unresolvedLidCount > 0) {
      groupLines.push(`  ℹ️ ${unresolvedLidCount} pending member(s) hidden their phone (LID-only) — could not match by number`);
    }

    let madeAdmin = 0, adminFailed = 0;
    if (makeAdminAfter && matched.length > 0) {
      try {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n👑 Promoting approved members to admin...`,
          { parse_mode: "HTML" }
        );
      } catch {}
      // Small wait so the participant lookup picks up newly-approved members
      await new Promise((r) => setTimeout(r, 1500));

      for (const { phone } of matched) {
        const participantJid = await findParticipantByPhone(userId, group.id, phone);
        if (!participantJid) {
          adminFailed++;
          groupLines.push(`  ⚠️ +${phone} — Approved, but not found for admin promotion`);
          continue;
        }
        const ok = await makeGroupAdmin(userId, group.id, participantJid);
        if (ok) {
          madeAdmin++;
          groupLines.push(`  👑 +${phone} — Admin granted`);
        } else {
          adminFailed++;
          groupLines.push(`  ❌ +${phone} — Failed to make admin`);
        }
        await new Promise((r) => setTimeout(r, 500));
      }
    }

    const summary = makeAdminAfter
      ? `✅ Approved: ${approved} | 👑 Admin: ${madeAdmin} | ❌ Failed: ${approveFailed + adminFailed} | ⚠️ Not found: ${notFound.length}`
      : `✅ Approved: ${approved} | ❌ Failed: ${approveFailed} | ⚠️ Not found: ${notFound.length}`;
    lines.push(`📋 <b>${esc(group.subject)}</b>\n${groupLines.join("\n")}\n${summary}`);
  }

  fullResult += lines.join("\n\n");
  fullResult += `\n\n━━━━━━━━━━━━━━━━━━\n✅ <b>Done processing ${groups.length} group(s)!</b>`;

  const chunks = splitMessage(fullResult, 4000);
  try {
    await bot.api.editMessageText(chatId, msgId, chunks[0], {
      parse_mode: "HTML",
      reply_markup: chunks.length === 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
    });
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(chatId, chunks[i], {
      parse_mode: "HTML",
      reply_markup: i === chunks.length - 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
    });
  }
}

bot.callbackQuery("ap_one_by_one", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData) return;

  const selectedGroups = Array.from(state.approvalData.selectedIndices).map(i => state.approvalData!.allGroups[i]);
  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  userStates.delete(userId);
  // Clear any leftover cancel state from a previous run.
  approvalCancelRequests.delete(userId);
  cancelDialogActiveFor.delete(userId);

  await ctx.editMessageText(
    `⏳ <b>Approving pending members 1 by 1...</b>\n\n⌛ Please wait...`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("❌ Cancel", "ap_cancel_request"),
    }
  );

  void approveOneByOneBackground(userId, String(userId), selectedGroups, chatId, msgId);
});

// Cancel-confirm dialog for the 1-by-1 approval loop. Same protected pattern
// used by Join / Get Links / Remove Members so the in-flight progress edit
// can't wipe the Yes/No buttons before the user answers.
bot.callbackQuery("ap_cancel_request", async (ctx) => {
  await ctx.answerCallbackQuery();
  cancelDialogActiveFor.add(ctx.from.id);
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard()
      .text("✅ Yes, Stop Approving", "ap_cancel_confirm")
      .text("↩️ Continue", "ap_cancel_no"),
  });
});

bot.callbackQuery("ap_cancel_no", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Approval continued" });
  const userId = ctx.from.id;
  cancelDialogActiveFor.delete(userId);
  // If somehow the user already confirmed, don't put the Cancel button back.
  if (approvalCancelRequests.has(userId)) return;
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard().text("❌ Cancel", "ap_cancel_request"),
  });
});

bot.callbackQuery("ap_cancel_confirm", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Stopping after current member..." });
  const userId = ctx.from.id;
  approvalCancelRequests.add(userId);
  // Keep the dialog flag on; the background loop's cleanup clears both flags
  // once it actually stops, which prevents a racing progress-edit between
  // confirm-tap and the loop's next iteration check.
  await ctx.editMessageReplyMarkup({ reply_markup: undefined });
});

// Fetch a group's current total member count after an approval pass.
// Returns "?" if the metadata fetch fails or returns an empty list, so the
// result message stays readable instead of dropping the line.
async function getGroupMemberCountSafe(userId: string, groupId: string): Promise<string> {
  try {
    const parts = await getGroupParticipants(userId, groupId);
    return parts.length > 0 ? String(parts.length) : "?";
  } catch {
    return "?";
  }
}

async function approveOneByOneBackground(
  userIdNum: number,
  userId: string,
  groups: Array<{ id: string; subject: string }>,
  chatId: number,
  msgId: number
) {
  const progressMarkup = new InlineKeyboard().text("❌ Cancel", "ap_cancel_request");
  let fullResult = "✅ <b>Approve 1 by 1 Result</b>\n\n";
  const lines: string[] = [];
  let cancelled = false;

  outer: for (let gi = 0; gi < groups.length; gi++) {
    if (approvalCancelRequests.has(userIdNum)) { cancelled = true; break outer; }
    const group = groups[gi];

    await safeBackgroundEdit(userIdNum, chatId, msgId,
      `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n⌛ Fetching pending members...`,
      { parse_mode: "HTML", reply_markup: progressMarkup }
    );

    // Use raw JIDs from the pending list — do NOT reconstruct from phone number.
    // In LID-mode groups the JID may be @lid format; reconstructing as @s.whatsapp.net
    // causes the approval API call to fail silently.
    const pendingJids = await getGroupPendingRequestsJids(userId, group.id);

    if (!pendingJids.length) {
      lines.push(`📋 <b>${esc(group.subject)}</b>\n✅ No pending members`);
      continue;
    }

    let approved = 0, failed = 0;
    for (let pi = 0; pi < pendingJids.length; pi++) {
      if (approvalCancelRequests.has(userIdNum)) {
        // Record what we did for this group so far before bailing. Fetch
        // the live total so the user knows the group's current size after
        // the partial approval.
        const total = await getGroupMemberCountSafe(userId, group.id);
        lines.push(`📋 <b>${esc(group.subject)}</b>\n✅ Approved: ${approved} | ❌ Failed: ${failed} | 🛑 Stopped at ${pi}/${pendingJids.length} | 👥 Total: ${total}`);
        cancelled = true;
        break outer;
      }
      const jid = pendingJids[pi];
      const ok = await approveGroupParticipant(userId, group.id, jid);
      if (ok) approved++;
      else failed++;

      if (pi % 3 === 0 || pi === pendingJids.length - 1) {
        await safeBackgroundEdit(userIdNum, chatId, msgId,
          `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n` +
          `✅ Approving: ${pi + 1}/${pendingJids.length}\n` +
          `Approved: ${approved} | Failed: ${failed}`,
          { parse_mode: "HTML", reply_markup: progressMarkup }
        );
      }

      // 1s delay between approvals to avoid WhatsApp rate limiting
      await new Promise((r) => setTimeout(r, 1000));
    }

    // Show the live total member count so the user sees how big the group
    // is now (post-approval). Stays as "?" if metadata fetch fails.
    const total = await getGroupMemberCountSafe(userId, group.id);
    lines.push(`📋 <b>${esc(group.subject)}</b>\n✅ Approved: ${approved} | ❌ Failed: ${failed} | 👥 Total: ${total}`);
  }

  // Cleanup flags so the next run starts clean (and so any racing dialog
  // confirmation after this point is a no-op).
  approvalCancelRequests.delete(userIdNum);
  cancelDialogActiveFor.delete(userIdNum);

  if (cancelled) {
    fullResult = `🛑 <b>Approve 1 by 1 — Cancelled</b>\n\n`;
  }
  fullResult += lines.join("\n\n");
  fullResult += cancelled
    ? `\n\n━━━━━━━━━━━━━━━━━━\n🛑 <b>Stopped after ${lines.length} group(s).</b>`
    : `\n\n━━━━━━━━━━━━━━━━━━\n✅ <b>Done processing ${groups.length} group(s)!</b>`;

  const chunks = splitMessage(fullResult, 4000);
  try {
    if (msgId) {
      await bot.api.editMessageText(chatId, msgId, chunks[0], {
        parse_mode: "HTML",
        reply_markup: chunks.length === 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
      });
    }
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(chatId, chunks[i], {
      parse_mode: "HTML",
      reply_markup: i === chunks.length - 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
    });
  }
}

bot.callbackQuery("ap_together", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData) return;

  const selectedGroups = Array.from(state.approvalData.selectedIndices).map(i => state.approvalData!.allGroups[i]);
  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  userStates.delete(userId);
  await ctx.editMessageText(`⏳ <b>Approving all pending members together...</b>\n\n⌛ Please wait...`, { parse_mode: "HTML" });

  void approveTogetherBackground(String(userId), selectedGroups, chatId, msgId);
});

async function approveTogetherBackground(
  userId: string,
  groups: Array<{ id: string; subject: string }>,
  chatId: number,
  msgId: number
) {
  let fullResult = "✅ <b>Approve Together Result</b>\n\n";
  const lines: string[] = [];

  for (let gi = 0; gi < groups.length; gi++) {
    const group = groups[gi];

    try {
      if (msgId) {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n` +
          `🔄 Step 1: Turning OFF approval mode...`,
          { parse_mode: "HTML" }
        );
      }
    } catch {}

    const offOk = await setGroupApprovalMode(userId, group.id, "off");
    if (!offOk) {
      lines.push(`📋 <b>${esc(group.subject)}</b>\n❌ Failed to turn off approval mode`);
      continue;
    }

    await new Promise((r) => setTimeout(r, 2000));

    try {
      if (msgId) {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n` +
          `🔄 Step 2: Turning ON approval mode...\n` +
          `✅ All pending members will be approved!`,
          { parse_mode: "HTML" }
        );
      }
    } catch {}

    const onOk = await setGroupApprovalMode(userId, group.id, "on");
    if (!onOk) {
      lines.push(`📋 <b>${esc(group.subject)}</b>\n⚠️ Turned off approval but failed to turn it back on`);
      continue;
    }

    // Give the server a moment to update group state, then fetch the live
    // total. "Approve Together" works by toggling the approval mode off→on,
    // which triggers the server to auto-approve everyone — so the metadata
    // we read here reflects the post-approval member count.
    await new Promise((r) => setTimeout(r, 1000));
    const total = await getGroupMemberCountSafe(userId, group.id);
    lines.push(`📋 <b>${esc(group.subject)}</b>\n✅ All pending members approved! | 👥 Total: ${total}`);
  }

  fullResult += lines.join("\n\n");
  fullResult += `\n\n━━━━━━━━━━━━━━━━━━\n✅ <b>Done processing ${groups.length} group(s)!</b>`;

  const chunks = splitMessage(fullResult, 4000);
  try {
    if (msgId) {
      await bot.api.editMessageText(chatId, msgId, chunks[0], {
        parse_mode: "HTML",
        reply_markup: chunks.length === 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
      });
    }
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(chatId, chunks[i], {
      parse_mode: "HTML",
      reply_markup: i === chunks.length - 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
    });
  }
}

async function makeAdminBackground(
  userId: string,
  groups: Array<{ id: string; subject: string }>,
  phoneNumbers: string[],
  chatId: number,
  msgId: number
) {
  let fullResult = "👑 <b>Make Admin Result</b>\n\n";
  const lines: string[] = [];

  for (let gi = 0; gi < groups.length; gi++) {
    const group = groups[gi];
    const groupLines: string[] = [];
    let madeAdmin = 0, notFound = 0, failed = 0;

    try {
      if (msgId) {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n⌛ Processing ${phoneNumbers.length} number(s)...`,
          { parse_mode: "HTML" }
        );
      }
    } catch {}

    for (let pi = 0; pi < phoneNumbers.length; pi++) {
      const phone = phoneNumbers[pi].replace(/[^0-9]/g, "");
      const participantJid = await findParticipantByPhone(userId, group.id, phone);

      if (!participantJid) {
        groupLines.push(`  ❌ +${phone} — Not found in group`);
        notFound++;
      } else {
        const ok = await makeGroupAdmin(userId, group.id, participantJid);
        if (ok) {
          groupLines.push(`  ✅ +${phone} — Admin granted`);
          madeAdmin++;
        } else {
          groupLines.push(`  ❌ +${phone} — Failed to make admin`);
          failed++;
        }
      }

      if (pi % 3 === 0 || pi === phoneNumbers.length - 1) {
        try {
          if (msgId) {
            await bot.api.editMessageText(chatId, msgId,
              `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n` +
              `Processing: ${pi + 1}/${phoneNumbers.length}\n` +
              `✅ Admin: ${madeAdmin} | ❌ Not found: ${notFound} | ❌ Failed: ${failed}`,
              { parse_mode: "HTML" }
            );
          }
        } catch {}
      }

      await new Promise((r) => setTimeout(r, 500));
    }

    lines.push(`📋 <b>${esc(group.subject)}</b>\n${groupLines.join("\n")}\n✅ Admin: ${madeAdmin} | ❌ Not found: ${notFound} | ❌ Failed: ${failed}`);
  }

  fullResult += lines.join("\n\n");
  fullResult += `\n\n━━━━━━━━━━━━━━━━━━\n✅ <b>Done processing ${groups.length} group(s)!</b>`;

  const chunks = splitMessage(fullResult, 4000);
  try {
    await bot.api.editMessageText(chatId, msgId, chunks[0], {
      parse_mode: "HTML",
      reply_markup: chunks.length === 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
    });
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(chatId, chunks[i], {
      parse_mode: "HTML",
      reply_markup: i === chunks.length - 1 ? new InlineKeyboard().text("🏠 Main Menu", "main_menu") : undefined,
    });
  }
}

// ─── Session Refresh ─────────────────────────────────────────────────────────

bot.callbackQuery("session_refresh", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("ℹ️ WhatsApp is not connected.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }
  await ctx.editMessageText(
    "🔄 <b>Session Refresh</b>\n\n" +
    "This will reconnect your WhatsApp session and reload the <b>LATEST</b> data from WhatsApp:\n\n" +
    "• 👥 Latest groups (including new ones where you just became admin)\n" +
    "• 👑 Latest admin status in every group\n" +
    "• 🔗 Latest invite links\n" +
    "• 📋 Latest pending requests\n" +
    "• 📞 Latest contacts\n\n" +
    "⚠️ Your saved login is <b>NOT</b> deleted — you do <b>NOT</b> need to re-pair. " +
    "The bot will be paused for ~10–30 seconds while it refreshes.\n\n" +
    "Do you want to continue?",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Refresh Now", "session_refresh_confirm")
        .text("❌ Cancel", "main_menu"),
    }
  );
});

const REFRESH_PHASES = [
  "🔌 Closing existing socket...",
  "🔐 Loading saved credentials...",
  "🌐 Reconnecting to WhatsApp servers...",
  "📥 Syncing latest groups & metadata...",
  "👑 Refreshing admin status...",
  "✨ Almost ready...",
];

function progressBar(percent: number, width = 14): string {
  const filled = Math.max(0, Math.min(width, Math.round((percent / 100) * width)));
  return "▰".repeat(filled) + "▱".repeat(width - filled);
}

bot.callbackQuery("session_refresh_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("ℹ️ WhatsApp is not connected.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }

  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  const startedAt = Date.now();
  let done = false;
  let phaseIdx = 0;
  let lastRendered = "";

  const renderProgress = async (phase: string, percent: number, extra = "") => {
    const elapsed = Math.floor((Date.now() - startedAt) / 1000);
    const text =
      `🔄 <b>Refreshing WhatsApp Session...</b>\n\n` +
      `${progressBar(percent)} <b>${percent}%</b>\n\n` +
      `${phase}\n` +
      `⏱️ Elapsed: ${elapsed}s${extra ? `\n\n${extra}` : ""}`;
    if (text === lastRendered) return;
    lastRendered = text;
    try {
      await bot.api.editMessageText(chatId, msgId, text, { parse_mode: "HTML" });
    } catch {}
  };

  await renderProgress(REFRESH_PHASES[0], 5);

  // Animate the progress bar while we wait for the reconnect to complete.
  // Caps at 90% until onConnected/onError fires; then we jump to 100% / final state.
  const ticker = setInterval(async () => {
    if (done) return;
    const elapsed = (Date.now() - startedAt) / 1000;
    // Map elapsed time to percent: ~3s per 10%, capped at 90.
    const percent = Math.min(90, 5 + Math.floor(elapsed * 3));
    if (percent >= 15 && phaseIdx < REFRESH_PHASES.length - 1) {
      phaseIdx = Math.min(REFRESH_PHASES.length - 1, Math.floor(percent / 15));
    }
    await renderProgress(REFRESH_PHASES[phaseIdx], percent);
  }, 1500);

  await refreshWhatsAppSession(
    String(userId),
    async () => {
      done = true;
      clearInterval(ticker);
      const elapsed = Math.floor((Date.now() - startedAt) / 1000);
      try {
        await bot.api.editMessageText(chatId, msgId,
          `✅ <b>Session Refreshed Successfully!</b>\n\n` +
          `${progressBar(100)} <b>100%</b>\n\n` +
          `🎉 All the LATEST WhatsApp data has been loaded:\n` +
          `• 👥 Groups\n• 👑 Admin status\n• 🔗 Invite links\n• 📋 Pending requests\n\n` +
          `⏱️ Took: ${elapsed}s\n\n` +
          `You can now use any feature with the latest data.`,
          { parse_mode: "HTML", reply_markup: mainMenu(userId) }
        );
      } catch {}
    },
    async (reason) => {
      done = true;
      clearInterval(ticker);
      try {
        await bot.api.editMessageText(chatId, msgId,
          `❌ <b>Session Refresh Failed</b>\n\nReason: ${esc(reason)}\n\n` +
          `Please try again, or use 🔌 Disconnect and reconnect manually.`,
          {
            parse_mode: "HTML",
            reply_markup: new InlineKeyboard()
              .text("🔄 Try Again", "session_refresh_confirm")
              .text("🏠 Main Menu", "main_menu"),
          }
        );
      } catch {}
    },
  );

  // Safety timeout — if neither callback fires in 60s, surface a timeout message.
  setTimeout(async () => {
    if (done) return;
    done = true;
    clearInterval(ticker);
    try {
      await bot.api.editMessageText(chatId, msgId,
        `⚠️ <b>Refresh is taking longer than expected</b>\n\n` +
        `The reconnect is still running in the background. Try the action again in a few seconds, or use the menu below.`,
        {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("🔄 Try Again", "session_refresh_confirm")
            .text("🏠 Main Menu", "main_menu"),
        }
      );
    } catch {}
  }, 60_000);
});

// ─── Disconnect ──────────────────────────────────────────────────────────────

bot.callbackQuery("disconnect_wa", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("ℹ️ WhatsApp is not connected.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    }); return;
  }
  await ctx.editMessageText(
    "⚠️ <b>Disconnect WhatsApp?</b>\n\nAre you sure you want to disconnect your WhatsApp session? You will need to reconnect again to use the bot.",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Disconnect", "disconnect_confirm")
        .text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("disconnect_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("ℹ️ WhatsApp is not connected.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    }); return;
  }
  // 1. Drop the live Baileys socket + auth state and pending reconnect timers.
  await disconnectWhatsApp(String(userId));
  // 2. Drop this user's slice of every per-user in-memory Map/Set so RAM
  //    actually returns to baseline instead of being held by orphaned state.
  clearUserMemoryState(userId);
  // 3. Also clear the Auto-Chat WhatsApp socket if it's open under the
  //    derived auto-userId — otherwise that second socket keeps ~5-10MB.
  try { await disconnectWhatsApp(getAutoUserId(String(userId))); } catch {}
  // 4. Run a global purge to flush translation caches + nudge V8/glibc to
  //    actually release pages back to the OS so RSS visibly drops.
  void runMemoryPurge("user disconnect");
  await ctx.editMessageText("✅ <b>WhatsApp disconnected!</b>", {
    parse_mode: "HTML", reply_markup: mainMenu(userId),
  });
});

// ─── Connect Auto Chat WhatsApp ───────────────────────────────────────────────

bot.callbackQuery("connect_auto_wa", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;

  if (isAutoConnected(String(userId))) {
    await ctx.editMessageText(
      "✅ <b>Auto Chat WhatsApp already connected!</b>\n\n" + connectedStatusText(userId),
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
    return;
  }

  userStates.set(userId, { step: "auto_connect_phone", autoConnectStep: "phone" });
  await ctx.editMessageText(
    "🤖 <b>Connect Auto Chat WhatsApp</b>\n\n" +
    "Yeh alag WhatsApp number Auto Chat ke liye connect hoga.\n\n" +
    "📱 Apna phone number bhejo (country code ke saath):\n" +
    "Example: <code>919876543210</code>",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

// ─── Auto Chat Menu ───────────────────────────────────────────────────────────

bot.callbackQuery("auto_chat_menu", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;

  if (!canUserSeeAutoChat(userId)) {
    await ctx.editMessageText(
      "🚫 <b>Auto Chat Access Nahi Hai</b>\n\nYe feature abhi aapke liye available nahi hai.\nAdmin se contact karo.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
    return;
  }

  if (!isConnected(String(userId))) {
    await ctx.editMessageText(
      "🤖 <b>Auto Chat</b>\n\n" +
      "Primary WhatsApp is not connected yet.\n\n" +
      "Please connect your 1st WhatsApp first. After that, you can connect the 2nd WhatsApp for Auto Chat.",
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("📱 Connect 1st WhatsApp", "connect_wa").row()
          .text("🏠 Main Menu", "main_menu"),
      }
    );
    return;
  }

  if (!isAutoConnected(String(userId))) {
    await ctx.editMessageText(
      "🤖 <b>Auto Chat</b>\n\n" +
      "Primary WhatsApp is connected.\n\n" +
      "Now connect your 2nd WhatsApp number for Auto Chat:",
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("📱 Connect 2nd WhatsApp", "connect_auto_wa").row()
          .text("🏠 Main Menu", "main_menu"),
      }
    );
    return;
  }

  const cigSess = cigSessions.get(userId);
  if (cigSess?.running) {
    await ctx.editMessageText(cigProgressText(cigSess), {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🔄 Refresh", "cig_refresh")
        .text("⏹️ Stop", "cig_stop_btn").row()
        .text("🏠 Main Menu", "main_menu"),
    });
    return;
  }

  const acfSess = acfSessions.get(userId);
  if (acfSess?.running) {
    await ctx.editMessageText(acfProgressText(acfSess), {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🔄 Refresh", "acf_refresh")
        .text("⏹️ Stop", "acf_stop_btn").row()
        .text("🏠 Main Menu", "main_menu"),
    });
    return;
  }

  const autoNumber = getAutoConnectedNumber(String(userId));
  const mainNumber = getConnectedWhatsAppNumber(String(userId));
  await ctx.editMessageText(
    "🤖 <b>Auto Chat Menu</b>\n\n" +
    (mainNumber ? `📞 Primary WA: <code>${esc(mainNumber)}</code>\n` : "") +
    (autoNumber ? `🤖 Auto WA: <code>${esc(autoNumber)}</code>\n` : "") +
    "\nKya karna chahte ho?",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("👥 Chat In Group", "acig_start").row()
        .text("👫 Chat Friend", "acf_start").row()
        .text("🔌 Disconnect Auto WA", "auto_disconnect_wa").row()
        .text("🏠 Main Menu", "main_menu"),
    }
  );
});

bot.callbackQuery("auto_chat_refresh", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const session = autoChatSessions.get(userId);
  if (!session?.running) {
    await ctx.editMessageText(
      "✅ <b>Auto Chat has stopped.</b>",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
    return;
  }
  const progressText = autoChatProgressText(session);
  try {
    await ctx.editMessageText(progressText, {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🔄 Refresh", "auto_chat_refresh")
        .text("⏹️ Stop", "auto_chat_stop").row()
        .text("🏠 Main Menu", "main_menu"),
    });
  } catch {}
});

bot.callbackQuery("auto_chat_stop", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const session = autoChatSessions.get(userId);
  if (!session?.running) {
    await ctx.editMessageText("ℹ️ Auto Chat already stopped.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }
  await ctx.editMessageText(
    "⚠️ <b>Stop Auto Chat?</b>\n\nDo you want to stop auto chat?",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Stop", "auto_chat_stop_confirm")
        .text("❌ Go Back", "auto_chat_refresh"),
    }
  );
});

bot.callbackQuery("auto_chat_stop_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const session = autoChatSessions.get(userId);
  if (session) {
    session.cancelled = true;
    session.running = false;
  }
  await ctx.editMessageText("⏹️ <b>Auto Chat stopped!</b>", {
    parse_mode: "HTML",
    reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
  });
});

bot.callbackQuery("auto_disconnect_wa", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const autoUserId = getAutoUserId(String(userId));
  if (!isAutoConnected(String(userId))) {
    await ctx.editMessageText("ℹ️ Auto Chat WhatsApp already disconnected.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    }); return;
  }
  await ctx.editMessageText(
    "⚠️ <b>Disconnect Auto Chat WA?</b>",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes", "auto_disconnect_confirm")
        .text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("auto_disconnect_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const autoUserId = getAutoUserId(String(userId));
  // Drop the auto-chat Baileys socket + its pending reconnect timers.
  await disconnectWhatsApp(autoUserId);
  // Stop and forget the auto-chat session object.
  const session = autoChatSessions.get(userId);
  if (session) { session.cancelled = true; session.running = false; }
  autoChatSessions.delete(userId);
  // Also drop CIG/ACF sessions which run on top of the auto socket and the
  // matching cancellation flags, so RAM actually returns to baseline.
  const cig = cigSessions.get(userId);
  if (cig) { (cig as any).cancelled = true; (cig as any).running = false; }
  cigSessions.delete(userId);
  const acf = acfSessions.get(userId);
  if (acf) { (acf as any).cancelled = true; (acf as any).running = false; }
  acfSessions.delete(userId);
  joinCancelRequests.delete(userId);
  getLinkCancelRequests.delete(userId);
  addMembersCancelRequests.delete(userId);
  removeMembersCancelRequests.delete(userId);
  // Nudge GC so RSS drops promptly on the 512MB free-tier dyno.
  void runMemoryPurge("auto-chat disconnect");
  await ctx.editMessageText("✅ <b>Auto Chat WhatsApp disconnected!</b>", {
    parse_mode: "HTML", reply_markup: mainMenu(userId),
  });
});

const CIG_PAGE_SIZE = 15;

// ─── Chat In Group (Auto Chat) — ACIG ─────────────────────────────────────────

bot.callbackQuery("acig_start", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isAutoConnected(String(userId))) {
    await ctx.editMessageText("❌ Auto Chat WA connected nahi hai.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }

  const autoUserId = getAutoUserId(String(userId));
  let primaryGroups: Array<{ id: string; subject: string }> = [];
  let autoGroups: Array<{ id: string; subject: string }> = [];
  try {
    [primaryGroups, autoGroups] = await Promise.all([
      getAllGroups(String(userId)),
      getAllGroups(autoUserId),
    ]);
  } catch {}

  const autoGroupIds = new Set(autoGroups.map(g => g.id));
  const commonGroups = primaryGroups.filter(g => autoGroupIds.has(g.id));

  if (!commonGroups.length) {
    await ctx.editMessageText(
      "❌ <b>Koi common group nahi mila!</b>\n\n" +
      "Dono WhatsApp numbers jo groups me hain unme se koi common group nahi hai.\n\n" +
      `Primary WA groups: ${primaryGroups.length}\nAuto WA groups: ${autoGroups.length}`,
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("🔙 Back", "auto_chat_menu").text("🏠 Menu", "main_menu"),
      }
    );
    return;
  }

  userStates.set(userId, {
    step: "acig_select_groups",
    chatInGroupData: {
      allGroups: commonGroups,
      selectedIndices: new Set(),
      page: 0,
      message: "",
      delaySeconds: 5,
      cancelled: false,
      botMode: "both",
    },
  });

  await ctx.editMessageText(
    "👥 <b>Chat In Group — Groups Select Karo</b>\n\n" +
    `📋 ${commonGroups.length} common groups mile (dono WA me hain).\n\n` +
    "Jin groups me dono numbers se msg bhejnha hai unhe select karo:",
    {
      parse_mode: "HTML",
      reply_markup: buildAcigKeyboard(userStates.get(userId)!),
    }
  );
});

function buildAcigKeyboard(state: UserState): InlineKeyboard {
  const data = state.chatInGroupData!;
  const kb = new InlineKeyboard();
  const groups = data.allGroups;
  const selected = data.selectedIndices;
  const page = data.page;
  const totalPages = Math.max(1, Math.ceil(groups.length / CIG_PAGE_SIZE));
  const start = page * CIG_PAGE_SIZE;
  const end = Math.min(start + CIG_PAGE_SIZE, groups.length);

  for (let i = start; i < end; i++) {
    const g = groups[i];
    const isSelected = selected.has(i);
    kb.text(`${isSelected ? "✅" : "☐"} ${g.subject.substring(0, 28)}`, `acig_tog_${i}`).row();
  }

  if (totalPages > 1) {
    const prev = page > 0 ? "⬅️ Prev" : " ";
    const next = page < totalPages - 1 ? "Next ➡️" : " ";
    kb.text(prev, "acig_prev_page").text(`📄 ${page + 1}/${totalPages}`, "acig_page_info").text(next, "acig_next_page").row();
  }

  kb.text("☑️ Select All", "acig_select_all").text("🧹 Clear", "acig_clear_all").row();
  if (selected.size > 0) {
    kb.text(`✅ Continue (${selected.size} groups)`, "acig_proceed").row();
  }
  kb.text("🔙 Back", "auto_chat_menu").text("🏠 Menu", "main_menu");
  return kb;
}

bot.callbackQuery(/^acig_tog_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.step !== "acig_select_groups") return;
  const idx = parseInt(ctx.match[1]);
  const selected = state.chatInGroupData.selectedIndices;
  if (selected.has(idx)) selected.delete(idx); else selected.add(idx);
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildAcigKeyboard(state) }); } catch {}
});

bot.callbackQuery("acig_select_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.step !== "acig_select_groups") return;
  for (let i = 0; i < state.chatInGroupData.allGroups.length; i++) state.chatInGroupData.selectedIndices.add(i);
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildAcigKeyboard(state) }); } catch {}
});

bot.callbackQuery("acig_clear_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.step !== "acig_select_groups") return;
  state.chatInGroupData.selectedIndices.clear();
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildAcigKeyboard(state) }); } catch {}
});

bot.callbackQuery("acig_prev_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.chatInGroupData.page <= 0) return;
  state.chatInGroupData.page--;
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildAcigKeyboard(state) }); } catch {}
});

bot.callbackQuery("acig_next_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData) return;
  const data = state.chatInGroupData;
  const totalPages = Math.ceil(data.allGroups.length / CIG_PAGE_SIZE);
  if (data.page < totalPages - 1) data.page++;
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildAcigKeyboard(state) }); } catch {}
});

bot.callbackQuery("acig_page_info", async (ctx) => { await ctx.answerCallbackQuery(); });

bot.callbackQuery("acig_proceed", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.chatInGroupData.selectedIndices.size === 0) return;
  const data = state.chatInGroupData;
  const selectedGroups = [...data.selectedIndices].map(i => data.allGroups[i]);
  const autoUserId = getAutoUserId(String(userId));

  const statusMsg = await ctx.editMessageText(
    "👥 <b>Chat In Group Started!</b>\n\n" +
    "Funny/study messages will rotate across all selected groups until you press Stop.",
    { parse_mode: "HTML" }
  );
  const msgId = (statusMsg as any).message_id;
  const chatId = ctx.chat!.id;
  userStates.delete(userId);
  void runGroupChatDualBackground(userId, String(userId), autoUserId, chatId, msgId, selectedGroups);
});

function cigProgressText(session: CigSession): string {
  const currentGroup = session.groups[session.currentGroupIndex]?.subject || session.groups[0]?.subject || "group";
  return (
    "🤖 <b>Auto Chat Running</b>\n\n" +
    `📍 Mode: <b>Chat in Group</b>\n` +
    `🎯 Target: <b>Group: ${esc(currentGroup)}</b>\n\n` +
    `📊 <b>Messages Sent:</b>\n` +
    `📱 Account 1: <b>${session.sentByAccount1} messages</b>\n` +
    `📱 Account 2: <b>${session.sentByAccount2} messages</b>\n` +
    `📩 Total: <b>${session.sent} messages</b>\n\n` +
    (session.nextDelayMs > 0 ? `⏰ Sending every ~${formatDelay(session.nextDelayMs)}...\n` : "") +
    "Press <b>Stop</b> to stop the chat."
  );
}

async function runGroupChatDualBackground(
  userId: number,
  primaryUserId: string,
  autoUserId: string,
  chatId: number,
  msgId: number,
  groups: Array<{ id: string; subject: string }>
): Promise<void> {
  const session: CigSession = {
    running: true,
    cancelled: false,
    chatId,
    msgId,
    groups,
    message: "Auto funny/study rotation",
    sent: 0,
    failed: 0,
    sentByAccount1: 0,
    sentByAccount2: 0,
    botMode: "both",
    currentGroupIndex: 0,
    cycle: 1,
    nextDelayMs: 0,
    rotationIndex: 0,
  };
  cigSessions.set(userId, session);

  try {
    let groupIndex = 0;
    let messageIndex = 0;
    let senderIndex = 0;

    while (!session.cancelled && session.running) {
      if (!groups.length) break;
      const group = groups[groupIndex];
      session.currentGroupIndex = groupIndex;
      session.cycle = Math.floor(messageIndex / (groups.length * 2)) + 1;
      if (session.cancelled) break;

      // Send 2 messages per group before rotating to next group
      for (let msgInGroup = 0; msgInGroup < 2; msgInGroup++) {
        if (!isSessionActive(session)) break;

        const isAccount1 = senderIndex % 2 === 0;
        const senderUserId = isAccount1 ? primaryUserId : autoUserId;
        const message = AUTO_GROUP_MESSAGES[messageIndex % AUTO_GROUP_MESSAGES.length];
        const ok = await sendGroupMessage(senderUserId, group.id, message);
        if (ok) {
          session.sent++;
          if (isAccount1) session.sentByAccount1++; else session.sentByAccount2++;
        } else session.failed++;

        messageIndex++;
        senderIndex++;
        session.nextDelayMs = getSequentialDelayMs(session.rotationIndex);
        session.rotationIndex++;

        try {
          await bot.api.editMessageText(chatId, msgId, cigProgressText(session), {
            parse_mode: "HTML",
            reply_markup: new InlineKeyboard()
              .text("🔄 Refresh", "cig_refresh")
              .text("⏹️ Stop", "cig_stop_btn").row()
              .text("🏠 Main Menu", "main_menu"),
          });
        } catch {}

        if (!isSessionActive(session)) break;
        await waitWithCancel(session, session.nextDelayMs);
        if (!isSessionActive(session)) break;
      }

      if (!isSessionActive(session)) break;

      groupIndex = (groupIndex + 1) % groups.length;
      session.currentGroupIndex = groupIndex;
    }
  } catch (err: any) {
    console.error(`[ACIG][${userId}] Error:`, err?.message);
  }

  session.running = false;
  session.nextDelayMs = 0;
  if (!session.cancelled) {
    try {
      await bot.api.editMessageText(chatId, msgId,
        `✅ <b>Chat In Group Complete!</b>\n\n📤 Sent: ${session.sent}\n❌ Failed: ${session.failed}\n📋 Groups: ${groups.length}`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
  }
}

bot.callbackQuery("cig_refresh", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const session = cigSessions.get(userId);
  if (!session?.running) {
    await ctx.editMessageText("✅ <b>Chat In Group band ho gaya.</b>", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }
  try {
    await ctx.editMessageText(cigProgressText(session), {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🔄 Refresh", "cig_refresh")
        .text("⏹️ Stop", "cig_stop_btn").row()
        .text("🏠 Main Menu", "main_menu"),
    });
  } catch {}
});

bot.callbackQuery("cig_stop_btn", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const session = cigSessions.get(userId);
  if (!session?.running) {
    await ctx.editMessageText("ℹ️ Chat In Group already band hai.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }
  await ctx.editMessageText(
    "⚠️ <b>Stop Chat In Group?</b>\n\nDo you want to stop sending messages?",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Stop", "cig_stop_confirm")
        .text("❌ Go Back", "cig_refresh"),
    }
  );
});

bot.callbackQuery("cig_stop_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const session = cigSessions.get(userId);
  if (session) {
    session.cancelled = true;
    session.running = false;
  }
  await ctx.editMessageText("⏹️ <b>Chat In Group stopped!</b>", {
    parse_mode: "HTML",
    reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
  });
});

// ─── Chat Friend Feature ────────────────────────────────────────────────────────

bot.callbackQuery("acf_start", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  const existingSession = acfSessions.get(userId);
  if (existingSession?.running) {
    await ctx.editMessageText(acfProgressText(existingSession), {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🔄 Refresh", "acf_refresh")
        .text("⏹️ Stop", "acf_stop_btn").row()
        .text("🏠 Main Menu", "main_menu"),
    });
    return;
  }
  if (existingSession) acfSessions.delete(userId);
  if (!isAutoConnected(String(userId))) {
    await ctx.editMessageText("❌ Auto Chat WA connected nahi hai.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }

  const primaryNumber = getConnectedWhatsAppNumber(String(userId));
  const autoNumber = getAutoConnectedNumber(String(userId));

  if (!primaryNumber || !autoNumber) {
    await ctx.editMessageText("❌ Dono WhatsApp numbers detect nahi hue. Reconnect karo.", {
      reply_markup: new InlineKeyboard().text("🔙 Back", "auto_chat_menu"),
    });
    return;
  }

  const primaryJid = primaryNumber.replace(/[^0-9]/g, "") + "@s.whatsapp.net";
  const autoJid = autoNumber.replace(/[^0-9]/g, "") + "@s.whatsapp.net";
  const totalPairs = CHAT_FRIEND_PAIRS.length;

  const statusMsg = await ctx.editMessageText(
    "👫 <b>Chat Friend Started!</b>\n\n" +
    `📞 Primary: <code>${esc(primaryNumber)}</code>\n` +
    `🤖 Auto: <code>${esc(autoNumber)}</code>\n\n` +
    "⏳ Auto funny/study messages will continue until you press Stop.",
    { parse_mode: "HTML" }
  );
  const msgId = (statusMsg as any).message_id;
  const chatId = ctx.chat!.id;

  void runChatFriendBackground(userId, String(userId), getAutoUserId(String(userId)), chatId, msgId, primaryJid, autoJid, totalPairs);
});

function acfProgressText(session: AcfSession): string {
  return (
    "👫 <b>Chat Friend Running...</b>\n\n" +
    `🔁 Cycle: <b>${session.cycle}</b>\n` +
    `💬 Pair: <b>${session.currentPair}/${session.totalPairs}</b>\n` +
    `📤 Sent: <b>${session.sent}</b>\n` +
    `❌ Failed: <b>${session.failed}</b>\n` +
    (session.nextDelayMs > 0 ? `⏱️ Next Delay: <b>${formatDelay(session.nextDelayMs)}</b>\n` : "") +
    "\nPress Stop to end it."
  );
}

async function runChatFriendBackground(
  userId: number,
  primaryUserId: string,
  autoUserId: string,
  chatId: number,
  msgId: number,
  primaryJid: string,
  autoJid: string,
  totalPairs: number
): Promise<void> {
  const session: AcfSession = {
    running: true,
    cancelled: false,
    chatId,
    msgId,
    primaryJid,
    autoJid,
    sent: 0,
    failed: 0,
    currentPair: 0,
    totalPairs,
    cycle: 1,
    nextDelayMs: 0,
    rotationIndex: 0,
  };
  acfSessions.set(userId, session);

  try {
    let i = 0;
    while (!session.cancelled && session.running) {
      if (session.cancelled) break;
      session.currentPair = (i % CHAT_FRIEND_PAIRS.length) + 1;
      session.cycle = Math.floor(i / CHAT_FRIEND_PAIRS.length) + 1;

      const [msg1, msg2] = CHAT_FRIEND_PAIRS[i % CHAT_FRIEND_PAIRS.length];

      const ok1 = await sendGroupMessage(primaryUserId, autoJid, msg1);
      if (ok1) session.sent++; else session.failed++;
      session.nextDelayMs = getSequentialDelayMs(session.rotationIndex);
      session.rotationIndex++;

      try {
        await bot.api.editMessageText(chatId, msgId, acfProgressText(session), {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("🔄 Refresh", "acf_refresh")
            .text("⏹️ Stop", "acf_stop_btn").row()
            .text("🏠 Main Menu", "main_menu"),
        });
      } catch {}

      if (!isSessionActive(session)) break;
      await waitWithCancel(session, session.nextDelayMs);
      if (!isSessionActive(session)) break;

      const ok2 = await sendGroupMessage(autoUserId, primaryJid, msg2);
      if (ok2) session.sent++; else session.failed++;
      session.nextDelayMs = getSequentialDelayMs(session.rotationIndex);
      session.rotationIndex++;

      try {
        await bot.api.editMessageText(chatId, msgId, acfProgressText(session), {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("🔄 Refresh", "acf_refresh")
            .text("⏹️ Stop", "acf_stop_btn").row()
            .text("🏠 Main Menu", "main_menu"),
        });
      } catch {}

      if (isSessionActive(session)) {
        await waitWithCancel(session, session.nextDelayMs);
      }
      if (!isSessionActive(session)) break;
      i++;
    }
  } catch (err: any) {
    console.error(`[ACF][${userId}] Error:`, err?.message);
  }

  session.running = false;
  session.nextDelayMs = 0;
  if (!session.cancelled) {
    try {
      await bot.api.editMessageText(chatId, msgId,
        `✅ <b>Chat Friend Complete!</b>\n\n📤 Sent: ${session.sent}\n❌ Failed: ${session.failed}\n💬 Pairs: ${session.totalPairs}`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
  }
}

bot.callbackQuery("acf_refresh", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const session = acfSessions.get(userId);
  if (!session?.running) {
    await ctx.editMessageText("✅ <b>Chat Friend band ho gaya.</b>", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }
  try {
    await ctx.editMessageText(acfProgressText(session), {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🔄 Refresh", "acf_refresh")
        .text("⏹️ Stop", "acf_stop_btn").row()
        .text("🏠 Main Menu", "main_menu"),
    });
  } catch {}
});

bot.callbackQuery("acf_stop_btn", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const session = acfSessions.get(userId);
  if (!session?.running) {
    await ctx.editMessageText("ℹ️ Chat Friend already band hai.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }
  await ctx.editMessageText(
    "⚠️ <b>Stop Chat Friend?</b>",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Stop", "acf_stop_confirm")
        .text("❌ Go Back", "acf_refresh"),
    }
  );
});

bot.callbackQuery("acf_stop_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const session = acfSessions.get(userId);
  if (session) {
    session.cancelled = true;
    session.running = false;
  }
  await ctx.editMessageText("⏹️ <b>Chat Friend stopped!</b>", {
    parse_mode: "HTML",
    reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
  });
});

// ─── Old Auto Chat background (kept for compatibility) ────────────────────────

function autoChatProgressText(session: AutoChatSession): string {
  const total = session.groups.length;
  const processed = session.sent + session.failed;
  const percent = total > 0 ? Math.floor((processed / total) * 100) : 0;
  return (
    "🤖 <b>Auto Chat Chal Raha Hai...</b>\n\n" +
    `🔁 Round: <b>${session.currentRound}/${session.repeatCount === 0 ? "∞" : session.repeatCount}</b>\n` +
    `📤 Sent: <b>${session.sent}</b>\n` +
    `❌ Failed: <b>${session.failed}</b>\n` +
    `📊 Progress: <b>${percent}%</b>\n\n` +
    "Roknay ke liye Stop dabao."
  );
}

// ── Memory & concurrency tuning for low-RAM hosts (e.g. Render free 512MB) ──
// Targeted to handle 500-1000 concurrent Auto Chat sessions safely.
// All limits can be tuned via env vars without code changes.
const MAX_CONCURRENT_AUTOCHAT = Number(process.env.MAX_CONCURRENT_AUTOCHAT || "1000");
const MAX_GROUPS_PER_AUTOCHAT = Number(process.env.MAX_GROUPS_PER_AUTOCHAT || "300");
const AUTOCHAT_PROGRESS_THROTTLE_MS = Number(process.env.AUTOCHAT_PROGRESS_THROTTLE_MS || "20000");
let activeAutoChatCount = 0;

async function runAutoChatBackground(userId: number, autoUserId: string, chatId: number, msgId: number, groups: Array<{ id: string; subject: string }>, message: string, delaySeconds: number, repeatCount: number): Promise<void> {
  // Backpressure: if too many auto-chats are already running, refuse politely
  // instead of pushing the host into OOM.
  if (activeAutoChatCount >= MAX_CONCURRENT_AUTOCHAT) {
    try {
      await bot.api.editMessageText(chatId, msgId,
        `⏳ <b>Server is busy</b>\n\n` +
        `Abhi <b>${activeAutoChatCount}</b> users ka Auto Chat chal raha hai (max <b>${MAX_CONCURRENT_AUTOCHAT}</b> ek saath allowed).\n\n` +
        `Thodi der baad firse try karein. 🙏`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
    return;
  }

  // Safety cap + memory trim: keep only group IDs in the long-lived session
  // (subjects are not read by the send loop or progress text). For 300 users
  // × 300 groups, this saves ~5–10MB of string heap that would otherwise
  // sit around for hours during repeat-forever sessions.
  const slice = groups.length > MAX_GROUPS_PER_AUTOCHAT
    ? groups.slice(0, MAX_GROUPS_PER_AUTOCHAT)
    : groups;
  const cappedGroups: Array<{ id: string; subject: string }> = slice.map((g) => ({ id: g.id, subject: "" }));

  const session: AutoChatSession = {
    running: true,
    cancelled: false,
    chatId,
    msgId,
    groups: cappedGroups,
    message,
    delaySeconds,
    repeatCount,
    sent: 0,
    failed: 0,
    currentRound: 1,
    rotationIndex: 0,
  };
  autoChatSessions.set(userId, session);
  activeAutoChatCount++;

  // Throttled progress updater — reduces Telegram API calls dramatically when
  // many users are running simultaneously. Always edits on `force=true`
  // (round changes, completion, errors) and otherwise at most once per
  // AUTOCHAT_PROGRESS_THROTTLE_MS.
  let lastProgressAt = 0;
  const progressKb = new InlineKeyboard()
    .text("🔄 Refresh", "auto_chat_refresh")
    .text("⏹️ Stop", "auto_chat_stop").row()
    .text("🏠 Main Menu", "main_menu");
  const tryUpdateProgress = async (force = false): Promise<void> => {
    const now = Date.now();
    if (!force && now - lastProgressAt < AUTOCHAT_PROGRESS_THROTTLE_MS) return;
    lastProgressAt = now;
    try {
      await bot.api.editMessageText(chatId, msgId, autoChatProgressText(session), {
        parse_mode: "HTML",
        reply_markup: progressKb,
      });
    } catch {}
  };

  const maxRounds = repeatCount === 0 ? Infinity : repeatCount;

  try {
    for (let round = 1; round <= maxRounds; round++) {
      if (session.cancelled) break;
      session.currentRound = round;
      await tryUpdateProgress(true); // force update at the start of every round

      for (const group of cappedGroups) {
        if (session.cancelled) break;

        let ok = false;
        try {
          ok = await sendGroupMessage(autoUserId, group.id, message);
        } catch (err: any) {
          // Never let a single send crash the whole loop.
          console.error(`[AUTO_CHAT][${userId}] sendGroupMessage error:`, err?.message);
          ok = false;
        }
        if (ok) session.sent++; else session.failed++;

        const delayMs = getSequentialDelayMs(session.rotationIndex);
        session.rotationIndex++;

        await tryUpdateProgress(); // throttled — won't spam Telegram API

        if (!session.cancelled) {
          await waitWithCancel(session, delayMs);
        }
      }

      if (!session.cancelled && round < maxRounds) {
        const delayMs = getSequentialDelayMs(session.rotationIndex);
        session.rotationIndex++;
        await waitWithCancel(session, delayMs);
      }
    }
  } catch (err: any) {
    console.error(`[AUTO_CHAT][${userId}] Error:`, err?.message);
  } finally {
    session.running = false;
    activeAutoChatCount = Math.max(0, activeAutoChatCount - 1);

    if (!session.cancelled) {
      try {
        await bot.api.editMessageText(chatId, msgId,
          `✅ <b>Auto Chat Complete!</b>\n\n📤 Sent: ${session.sent}\n❌ Failed: ${session.failed}`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
        );
      } catch {}
    }

    // Free per-user state once the loop is fully done so the Map doesn't grow
    // unboundedly for long-running deployments.
    setTimeout(() => {
      const cur = autoChatSessions.get(userId);
      if (cur && !cur.running) autoChatSessions.delete(userId);
    }, 30_000); // small grace period so user can still see status if they tap Refresh
  }
}

// ─── Chat In Group Feature ─────────────────────────────────────────────────────

function buildChatGroupKeyboard(state: UserState): InlineKeyboard {
  const data = state.chatInGroupData!;
  const kb = new InlineKeyboard();
  const groups = data.allGroups;
  const selected = data.selectedIndices;
  const page = data.page;
  const totalPages = Math.max(1, Math.ceil(groups.length / CIG_PAGE_SIZE));
  const start = page * CIG_PAGE_SIZE;
  const end = Math.min(start + CIG_PAGE_SIZE, groups.length);

  for (let i = start; i < end; i++) {
    const g = groups[i];
    const isSelected = selected.has(i);
    kb.text(`${isSelected ? "✅" : "☐"} ${g.subject.substring(0, 28)}`, `cig_tog_${i}`).row();
  }

  if (totalPages > 1) {
    const prev = page > 0 ? "⬅️ Prev" : " ";
    const next = page < totalPages - 1 ? "Next ➡️" : " ";
    kb.text(prev, "cig_prev_page").text(`📄 ${page + 1}/${totalPages}`, "cig_page_info").text(next, "cig_next_page").row();
  }

  kb.text("☑️ Select All", "cig_select_all").text("🧹 Clear", "cig_clear_all").row();
  if (selected.size > 0) {
    kb.text(`✅ Continue (${selected.size} groups)`, "cig_proceed").row();
  }
  kb.text("🏠 Main Menu", "main_menu");
  return kb;
}

bot.callbackQuery("chat_in_group", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("📱 <b>WhatsApp not connected!</b>\n\nConnect first to use this feature.", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    });
    return;
  }

  let groups: Array<{ id: string; subject: string }> = [];
  try {
    groups = await getAllGroups(String(userId));
  } catch {}

  if (!groups.length) {
    await ctx.editMessageText("❌ <b>Koi group nahi mila!</b>", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }

  userStates.set(userId, {
    step: "cig_select_groups",
    chatInGroupData: {
      allGroups: groups,
      selectedIndices: new Set(),
      page: 0,
      message: "",
      delaySeconds: 3,
      cancelled: false,
    },
  });

  await ctx.editMessageText(
    `💬 <b>Chat In Group</b>\n\n📋 ${groups.length} groups mile.\nJin groups me msg bhejnha hai unhe select karo:`,
    { parse_mode: "HTML", reply_markup: buildChatGroupKeyboard(userStates.get(userId)!) }
  );
});

bot.callbackQuery(/^cig_tog_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData) return;
  const idx = parseInt(ctx.match[1]);
  const selected = state.chatInGroupData.selectedIndices;
  if (selected.has(idx)) selected.delete(idx); else selected.add(idx);
  try {
    await ctx.editMessageReplyMarkup({ reply_markup: buildChatGroupKeyboard(state) });
  } catch {}
});

bot.callbackQuery("cig_select_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData) return;
  const data = state.chatInGroupData;
  for (let i = 0; i < data.allGroups.length; i++) data.selectedIndices.add(i);
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildChatGroupKeyboard(state) }); } catch {}
});

bot.callbackQuery("cig_clear_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData) return;
  state.chatInGroupData.selectedIndices.clear();
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildChatGroupKeyboard(state) }); } catch {}
});

bot.callbackQuery("cig_prev_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.chatInGroupData.page <= 0) return;
  state.chatInGroupData.page--;
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildChatGroupKeyboard(state) }); } catch {}
});

bot.callbackQuery("cig_next_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData) return;
  const data = state.chatInGroupData;
  const totalPages = Math.ceil(data.allGroups.length / CIG_PAGE_SIZE);
  if (data.page < totalPages - 1) data.page++;
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildChatGroupKeyboard(state) }); } catch {}
});

bot.callbackQuery("cig_page_info", async (ctx) => { await ctx.answerCallbackQuery(); });

bot.callbackQuery("cig_proceed", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.chatInGroupData.selectedIndices.size === 0) return;
  state.step = "cig_enter_message";
  const count = state.chatInGroupData.selectedIndices.size;
  await ctx.editMessageText(
    `✅ <b>${count} groups select kiye!</b>\n\n` +
    "📝 Ab wo message bhejo jo in groups me bhejnha hai:",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("cig_start_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || !state.chatInGroupData.message) return;

  const data = state.chatInGroupData;
  const selectedGroups = [...data.selectedIndices].map(i => data.allGroups[i]);
  const statusMsg = await ctx.editMessageText(
    `⏳ <b>Message bhej raha hun...</b>\n\n📤 0/${selectedGroups.length} done...`,
    { parse_mode: "HTML" }
  );
  const msgId = (statusMsg as any).message_id;
  const chatId = ctx.chat!.id;
  userStates.delete(userId);
  void cigSendBackground(userId, String(userId), chatId, msgId, selectedGroups, data.message, data.delaySeconds);
});

bot.callbackQuery("cig_cancel_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (state?.chatInGroupData) state.chatInGroupData.cancelled = true;
  userStates.delete(userId);
  await ctx.editMessageText("❌ <b>Cancelled.</b>", {
    parse_mode: "HTML",
    reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
  });
});

async function cigSendBackground(userId: number, waUserId: string, chatId: number, msgId: number, groups: Array<{ id: string; subject: string }>, message: string, delaySeconds: number): Promise<void> {
  const session: CigSession = {
    running: true,
    cancelled: false,
    chatId,
    msgId,
    groups,
    message,
    sent: 0,
    failed: 0,
    botMode: "single",
    currentGroupIndex: 0,
    cycle: 1,
    nextDelayMs: 0,
    rotationIndex: 0,
  };
  cigSessions.set(userId, session);

  try {
    let groupIndex = 0;
    while (!session.cancelled && session.running) {
      if (!groups.length) break;
      const group = groups[groupIndex];
      session.currentGroupIndex = groupIndex;
      session.cycle = Math.floor(session.sent / groups.length) + 1;

      const ok = await sendGroupMessage(waUserId, group.id, message);
      if (ok) session.sent++; else session.failed++;

      session.nextDelayMs = getSequentialDelayMs(session.rotationIndex);
      session.rotationIndex++;

      try {
        await bot.api.editMessageText(chatId, msgId,
          `📤 <b>Messages bhej raha hun...</b>\n\n` +
          `✅ Sent: ${session.sent}\n❌ Failed: ${session.failed}\n` +
          `🔁 Cycle: ${session.cycle}\n` +
          `📊 Group: ${groupIndex + 1}/${groups.length}\n` +
          `⏱️ Next Delay: <b>${formatDelay(session.nextDelayMs)}</b>\n` +
          `⏳ Last: ${esc(group.subject)}\n\n` +
          `Press Stop to end it.`,
          {
            parse_mode: "HTML",
            reply_markup: new InlineKeyboard()
              .text("🔄 Refresh", "cig_refresh")
              .text("⏹️ Stop", "cig_stop_btn").row()
              .text("🏠 Main Menu", "main_menu"),
          }
        );
      } catch {}

      if (!isSessionActive(session)) break;
      await waitWithCancel(session, session.nextDelayMs);
      if (!isSessionActive(session)) break;

      groupIndex = (groupIndex + 1) % groups.length;
    }
  } catch (err: any) {
    console.error(`[CIG_SINGLE][${userId}] Error:`, err?.message);
  }

  session.running = false;
  session.nextDelayMs = 0;
  if (!session.cancelled) {
    try {
      await bot.api.editMessageText(chatId, msgId,
        `✅ <b>Chat In Group Band!</b>\n\n📤 Sent: ${session.sent}\n❌ Failed: ${session.failed}\n📊 Groups: ${groups.length}`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
  }
}

// ─── Edit Settings Feature ────────────────────────────────────────────────────

const ES_PAGE_SIZE = 20;

function buildEditSettingsGroupKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const allGroups = state.editSettingsData!.allGroups;
  const selected = state.editSettingsData!.selectedIndices;
  const page = state.editSettingsData!.page;
  const totalPages = Math.max(1, Math.ceil(allGroups.length / ES_PAGE_SIZE));
  const start = page * ES_PAGE_SIZE;
  const end = Math.min(start + ES_PAGE_SIZE, allGroups.length);
  for (let i = start; i < end; i++) {
    const g = allGroups[i];
    const label = selected.has(i) ? `✅ ${g.subject}` : `☐ ${g.subject}`;
    kb.text(label, `es_tog_${i}`).row();
  }
  if (totalPages > 1) {
    const prev = page > 0 ? "⬅️ Prev" : " ";
    const next = page < totalPages - 1 ? "Next ➡️" : " ";
    kb.text(prev, "es_prev_page").text(`📄 ${page + 1}/${totalPages}`, "es_page_info").text(next, "es_next_page").row();
  }
  if (allGroups.length > 1) kb.text("☑️ Select All", "es_select_all").text("🧹 Clear All", "es_clear_all").row();
  if (selected.size > 0) kb.text(`▶️ Continue (${selected.size} selected)`, "es_continue").row();
  kb.text("🔙 Back", "edit_settings").text("🏠 Menu", "main_menu");
  return kb;
}

function editSettingsKeyboard(gs: GroupSettings): InlineKeyboard {
  const on = (v: boolean) => v ? "✅ ON" : "❌ OFF";
  return new InlineKeyboard()
    .text(`📝 Edit Info: ${on(gs.editGroupInfo)}`, "es_tog_editInfo").text(`💬 Send Msgs: ${on(gs.sendMessages)}`, "es_tog_sendMsg").row()
    .text(`➕ Add Members: ${on(gs.addMembers)}`, "es_tog_addMembers").text(`🔐 Approve: ${on(gs.approveJoin)}`, "es_tog_approveJoin").row()
    .text("💾 Save Settings", "es_settings_done");
}

function editSettingsText(gs: GroupSettings): string {
  const on = (v: boolean) => v ? "✅ ON" : "❌ OFF";
  return (
    "⚙️ <b>Edit Group Settings</b>\n\n" +
    "<b>👥 Members can:</b>\n" +
    `📝 Edit Group Info: ${on(gs.editGroupInfo)}\n` +
    `💬 Send Messages: ${on(gs.sendMessages)}\n` +
    `➕ Add Members: ${on(gs.addMembers)}\n\n` +
    "<b>👑 Admins:</b>\n" +
    `🔐 Approve New Members: ${on(gs.approveJoin)}\n\n` +
    "Tap to toggle each setting:"
  );
}

bot.callbackQuery("edit_settings", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    }); return;
  }
  await ctx.editMessageText("🔍 <b>Scanning your admin groups...</b>", { parse_mode: "HTML" });
  const allGroups = await getAllGroups(String(userId));
  const adminGroups = allGroups.filter(g => g.isAdmin);
  if (!adminGroups.length) {
    await ctx.editMessageText("📭 Aap kisi bhi group mein admin nahi hain.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    }); return;
  }
  const simpleGroups = adminGroups.map(g => ({ id: g.id, subject: g.subject }));
  const patterns = detectSimilarGroups(simpleGroups);
  userStates.set(userId, {
    step: "edit_settings_menu",
    editSettingsData: {
      allGroups: simpleGroups, patterns, selectedIndices: new Set(), page: 0,
      settings: defaultGroupSettings(), cancelled: false,
    },
  });
  const kb = new InlineKeyboard();
  if (patterns.length > 0) kb.text("🔍 Similar Groups", "es_similar").text("📋 All Groups", "es_show_all").row();
  else kb.text("📋 All Groups", "es_show_all").row();
  kb.text("🏠 Main Menu", "main_menu");
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\n📊 Admin Groups: ${adminGroups.length}\n` +
    (patterns.length > 0 ? `🔍 Similar Patterns: ${patterns.length}\n` : "") +
    `\n📌 Choose an option:`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery("es_similar", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  const { patterns } = state.editSettingsData;
  if (!patterns.length) {
    await ctx.editMessageText("⚠️ No similar group patterns found.", {
      reply_markup: new InlineKeyboard().text("🔙 Back", "edit_settings").text("🏠 Menu", "main_menu"),
    }); return;
  }
  const kb = new InlineKeyboard();
  for (let i = 0; i < patterns.length; i++) {
    kb.text(`📌 ${patterns[i].base} (${patterns[i].groups.length} groups)`, `es_sim_${i}`).row();
  }
  kb.text("🔙 Back", "edit_settings").text("🏠 Menu", "main_menu");
  await ctx.editMessageText("🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select those groups:", { parse_mode: "HTML", reply_markup: kb });
});

bot.callbackQuery(/^es_sim_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  const idx = parseInt(ctx.match![1]);
  const pattern = state.editSettingsData.patterns[idx];
  if (!pattern) return;
  const patternIds = new Set(pattern.groups.map(g => g.id));
  state.editSettingsData.selectedIndices = new Set();
  for (let i = 0; i < state.editSettingsData.allGroups.length; i++) {
    if (patternIds.has(state.editSettingsData.allGroups[i].id)) state.editSettingsData.selectedIndices.add(i);
  }
  state.step = "edit_settings_select";
  state.editSettingsData.page = 0;
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\n👑 <b>${state.editSettingsData.allGroups.length} admin group(s)</b>\n\nGroup(s) select karo:\n<i>${state.editSettingsData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildEditSettingsGroupKeyboard(state) }
  );
});

bot.callbackQuery("es_show_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  state.step = "edit_settings_select";
  state.editSettingsData.page = 0;
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\n👑 <b>${state.editSettingsData.allGroups.length} admin group(s)</b>\n\nGroup(s) select karo:\n<i>Tap to select/deselect</i>`,
    { parse_mode: "HTML", reply_markup: buildEditSettingsGroupKeyboard(state) }
  );
});

bot.callbackQuery(/^es_tog_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  const idx = parseInt(ctx.match![1]);
  if (idx < 0 || idx >= state.editSettingsData.allGroups.length) return;
  if (state.editSettingsData.selectedIndices.has(idx)) state.editSettingsData.selectedIndices.delete(idx);
  else state.editSettingsData.selectedIndices.add(idx);
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\n👑 <b>${state.editSettingsData.allGroups.length} admin group(s)</b>\n\nGroup(s) select karo:\n<i>${state.editSettingsData.selectedIndices.size > 0 ? `${state.editSettingsData.selectedIndices.size} selected` : "None selected"}</i>`,
    { parse_mode: "HTML", reply_markup: buildEditSettingsGroupKeyboard(state) }
  );
});

bot.callbackQuery("es_select_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  state.editSettingsData.selectedIndices = new Set(state.editSettingsData.allGroups.map((_, i) => i));
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\n${state.editSettingsData.allGroups.length} groups selected.\n\nSab select ho gaye:`,
    { parse_mode: "HTML", reply_markup: buildEditSettingsGroupKeyboard(state) }
  );
});

bot.callbackQuery("es_clear_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  state.editSettingsData.selectedIndices = new Set();
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\nSab clear. Group(s) select karo:`,
    { parse_mode: "HTML", reply_markup: buildEditSettingsGroupKeyboard(state) }
  );
});

bot.callbackQuery("es_prev_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  if (state.editSettingsData.page > 0) state.editSettingsData.page--;
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\n<i>${state.editSettingsData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildEditSettingsGroupKeyboard(state) }
  );
});

bot.callbackQuery("es_next_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  const totalPages = Math.ceil(state.editSettingsData.allGroups.length / ES_PAGE_SIZE);
  if (state.editSettingsData.page < totalPages - 1) state.editSettingsData.page++;
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\n<i>${state.editSettingsData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildEditSettingsGroupKeyboard(state) }
  );
});

bot.callbackQuery("es_page_info", async (ctx) => { await ctx.answerCallbackQuery(); });

bot.callbackQuery("es_continue", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  if (state.editSettingsData.selectedIndices.size === 0) {
    await ctx.answerCallbackQuery({ text: "⚠️ Koi group select nahi!" }); return;
  }
  state.step = "edit_settings_permissions";
  const gs = state.editSettingsData.settings;
  await ctx.editMessageText(editSettingsText(gs), { parse_mode: "HTML", reply_markup: editSettingsKeyboard(gs) });
});

for (const [cb, field] of [
  ["es_tog_editInfo", "editGroupInfo"], ["es_tog_sendMsg", "sendMessages"],
  ["es_tog_addMembers", "addMembers"], ["es_tog_approveJoin", "approveJoin"],
] as const) {
  bot.callbackQuery(cb, async (ctx) => {
    await ctx.answerCallbackQuery();
    const state = userStates.get(ctx.from.id);
    if (!state?.editSettingsData) return;
    (state.editSettingsData.settings as any)[field] = !(state.editSettingsData.settings as any)[field];
    await ctx.editMessageText(editSettingsText(state.editSettingsData.settings), { parse_mode: "HTML", reply_markup: editSettingsKeyboard(state.editSettingsData.settings) });
  });
}

bot.callbackQuery("es_settings_done", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  const cur = state.editSettingsData.settings.disappearingMessages;
  state.step = "edit_settings_disappearing";
  await ctx.editMessageText(
    "⏳ <b>Disappearing Messages</b>\n\nMessages kitne time baad delete honge?\n\n" +
    `Current: <b>${cur === 0 ? "Off" : cur === 86400 ? "24 Hours" : cur === 604800 ? "7 Days" : "90 Days"}</b>`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text(cur === 86400 ? "✅ 24 Hours" : "🕐 24 Hours", "es_dm_24h").text(cur === 604800 ? "✅ 7 Days" : "📅 7 Days", "es_dm_7d").row()
        .text(cur === 7776000 ? "✅ 90 Days" : "📆 90 Days", "es_dm_90d").text(cur === 0 ? "✅ Off" : "🔕 Off", "es_dm_off").row()
        .text("❌ Cancel", "main_menu"),
    }
  );
});

for (const [cb, dur] of [["es_dm_24h", 86400], ["es_dm_7d", 604800], ["es_dm_90d", 7776000], ["es_dm_off", 0]] as const) {
  bot.callbackQuery(cb, async (ctx) => {
    await ctx.answerCallbackQuery();
    const state = userStates.get(ctx.from.id);
    if (!state?.editSettingsData) return;
    state.editSettingsData.settings.disappearingMessages = dur;
    state.step = "edit_settings_dp";
    await ctx.editMessageText(
      "🖼️ <b>Group DP</b>\n\nSare selected groups mein DP lagana hai?\nPhoto bhejo ya skip karo.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "es_dp_skip").text("❌ Cancel", "main_menu") }
    );
  });
}

bot.callbackQuery("es_dp_skip", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  state.editSettingsData.settings.dpBuffers = [];
  state.step = "edit_settings_desc";
  await ctx.editMessageText(
    "📄 <b>Group Description</b>\n\nSare selected groups mein description lagani hai?\nDescription bhejo ya skip karo.",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "es_desc_skip").text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("es_desc_skip", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  state.editSettingsData.settings.description = "";
  await showEditSettingsReview(ctx);
});

async function showEditSettingsReview(ctx: any) {
  const userId = ctx.from?.id ?? ctx.chat?.id;
  const state = userStates.get(userId);
  if (!state?.editSettingsData) return;
  const { settings, allGroups, selectedIndices } = state.editSettingsData;
  state.step = "edit_settings_review";
  const selectedGroups = Array.from(selectedIndices).map(i => allGroups[i]);
  const groupList = selectedGroups.slice(0, 5).map(g => `• ${esc(g.subject)}`).join("\n");
  const moreText = selectedGroups.length > 5 ? `\n... +${selectedGroups.length - 5} more` : "";
  const dmText = settings.disappearingMessages === 86400 ? "24 Hours" : settings.disappearingMessages === 604800 ? "7 Days" : settings.disappearingMessages === 7776000 ? "90 Days" : "Off";
  const on = (v: boolean) => v ? "✅" : "❌";
  const reviewText =
    "📋 <b>Edit Settings — Review</b>\n\n" +
    `📋 <b>Groups (${selectedGroups.length}):</b>\n${groupList}${moreText}\n\n` +
    `📄 Description: ${settings.description ? esc(settings.description) : "Skip"}\n` +
    `🖼️ DP: ${settings.dpBuffers.length > 0 ? "✅ Change" : "❌ Skip"}\n` +
    `⏳ Disappearing: ${dmText}\n\n` +
    "⚙️ <b>Permissions:</b>\n" +
    `${on(settings.editGroupInfo)} Edit Info | ${on(settings.sendMessages)} Send Msgs\n` +
    `${on(settings.addMembers)} Add Members | ${on(settings.approveJoin)} Approve Join\n\n` +
    "✅ Confirm karke sab groups mein apply karo:";
  const kb = new InlineKeyboard().text("✅ Apply to All Groups", "es_apply_confirm").text("❌ Cancel", "main_menu");
  try { await ctx.editMessageText(reviewText, { parse_mode: "HTML", reply_markup: kb }); }
  catch { await ctx.reply(reviewText, { parse_mode: "HTML", reply_markup: kb }); }
}

bot.callbackQuery("es_apply_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.editSettingsData) return;
  const { settings, allGroups, selectedIndices } = state.editSettingsData;
  const selectedGroups = Array.from(selectedIndices).map(i => allGroups[i]);
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;
  state.editSettingsData.cancelled = false;
  state.step = "edit_settings_applying";
  await ctx.editMessageText(
    `⏳ <b>Settings Apply Ho Rahi Hain...</b>\n\n🔄 0/${selectedGroups.length} done`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "es_cancel_apply") }
  );
  void applyEditSettingsBackground(String(userId), userId, settings, selectedGroups, chatId, msgId);
});

bot.callbackQuery("es_cancel_apply", async (ctx) => {
  await ctx.answerCallbackQuery();
  await ctx.editMessageText(
    "⚠️ <b>Cancel karna chahte hain?</b>\n\nJo groups process ho chuke hain unko revert nahi kiya jayega.",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Haan, Cancel", "es_cancel_confirm")
        .text("▶️ Continue", "es_cancel_dismiss"),
    }
  );
});

bot.callbackQuery("es_cancel_confirm", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "🛑 Cancelled!" });
  const state = userStates.get(ctx.from.id);
  if (state?.editSettingsData) state.editSettingsData.cancelled = true;
  await ctx.editMessageText("🛑 <b>Apply cancelled.</b>", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Menu", "main_menu") });
});

bot.callbackQuery("es_cancel_dismiss", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "▶️ Continuing..." });
});

async function applyEditSettingsBackground(
  userId: string, numericUserId: number,
  settings: GroupSettings,
  groups: Array<{ id: string; subject: string }>,
  chatId: number, msgId: number
) {
  const perms: GroupPermissions = { editGroupInfo: settings.editGroupInfo, sendMessages: settings.sendMessages, addMembers: settings.addMembers, approveJoin: settings.approveJoin };
  const results: Array<{ name: string; ok: boolean; error?: string }> = [];
  const total = groups.length;

  for (let i = 0; i < total; i++) {
    const state = userStates.get(numericUserId);
    if (state?.editSettingsData?.cancelled) {
      for (let j = i; j < total; j++) results.push({ name: groups[j].subject, ok: false, error: "Cancelled" });
      break;
    }
    const group = groups[i];
    try {
      await applyGroupSettings(userId, group.id, perms, settings.description);
      if (settings.disappearingMessages >= 0) {
        await new Promise(r => setTimeout(r, 800));
        await setGroupDisappearingMessages(userId, group.id, settings.disappearingMessages);
      }
      if (settings.dpBuffers.length > 0) {
        const dpBuf = settings.dpBuffers[i % settings.dpBuffers.length];
        await new Promise(r => setTimeout(r, 1500));
        await setGroupIcon(userId, group.id, dpBuf);
      }
      results.push({ name: group.subject, ok: true });
    } catch (err: any) {
      results.push({ name: group.subject, ok: false, error: err?.message || "Unknown error" });
    }
    const done = i + 1;
    const lines = results.map(r => r.ok ? `✅ ${esc(r.name)}` : r.error === "Cancelled" ? `⛔ ${esc(r.name)}` : `❌ ${esc(r.name)}`).join("\n");
    try {
      await bot.api.editMessageText(chatId, msgId,
        `⏳ <b>Apply Ho Rahi Hain: ${done}/${total}</b>\n\n${lines}${done < total ? "\n\n⌛ Processing..." : ""}`,
        { parse_mode: "HTML", reply_markup: done < total ? new InlineKeyboard().text("❌ Cancel", "es_cancel_apply") : undefined }
      );
    } catch {}
    if (i < total - 1) await new Promise(r => setTimeout(r, 2000));
  }

  userStates.delete(numericUserId);
  const ok = results.filter(r => r.ok).length;
  const cancelled = results.some(r => r.error === "Cancelled");
  const header = cancelled ? `🛑 <b>Cancelled (${ok}/${total} done)</b>` : `🎉 <b>Done! (${ok}/${total} applied)</b>`;
  const finalLines = results.map(r => r.ok ? `✅ ${esc(r.name)}` : r.error === "Cancelled" ? `⛔ ${esc(r.name)} (skipped)` : `❌ ${esc(r.name)}: ${esc(r.error || "")}`).join("\n");
  try {
    await bot.api.editMessageText(chatId, msgId, `${header}\n\n${finalLines}`, {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
  } catch {}
}

// ─── Change Group Name Feature ────────────────────────────────────────────────
// Two sub-flows:
//   • Manual (by name): scan admin groups → similar/all → user taps groups
//     in order (buttons show 1, 2, 3…) → choose Auto-numbered or Custom
//     names → review → background rename in selection order with live
//     progress + Cancel.
//   • Auto (VCF + name): scan groups with pending requests → user selects
//     groups → user uploads one VCF per selected group → bot matches each
//     VCF to a group by checking which group's pending list contains the
//     VCF's phones → user chooses "same as VCF name" or "custom prefix" →
//     review → background rename + Cancel.
// Cancel-confirm dialog is protected by `cancelDialogActiveFor` (same
// pattern used by Join/Get-Links/Remove-Members).

const CGN_PAGE_SIZE = 20;

// Strip a trailing number from a VCF basename so we can keep just the
// number for the "custom prefix" mode.
//   "Expedia 酒店回饋活動FL_61.vcf" → "61"
//   "SPIDY group 12.vcf"         → "12"
//   "no number here.vcf"         → ""
function extractTrailingNumber(vcfFileName: string): string {
  const base = vcfFileName.replace(/\.vcf$/i, "");
  const m = base.match(/(\d+)\s*$/);
  return m ? m[1] : "";
}

// Strip the .vcf extension to use as a group name directly.
function vcfBasename(vcfFileName: string): string {
  return vcfFileName.replace(/\.vcf$/i, "").trim();
}

// ── Entry: ask user to pick Manual or Auto ──
bot.callbackQuery("change_group_name", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    });
    return;
  }
  userStates.delete(userId);
  const kb = new InlineKeyboard()
    .text("✏️ Manual (by name)", "cgn_manual").row()
    .text("📁 Auto (VCF + name)", "cgn_auto").row()
    .text("🏠 Main Menu", "main_menu");
  await ctx.editMessageText(
    "🏷️ <b>Change Group Name</b>\n\n" +
    "Pick a mode:\n\n" +
    "✏️ <b>Manual (by name)</b>\n" +
    "• Pick groups (Similar / All) by tapping — order matters\n" +
    "• Type names yourself (auto-numbered or one per line)\n" +
    "• Bot renames in your tap order\n\n" +
    "📁 <b>Auto (VCF + name)</b>\n" +
    "• Only groups with pending requests are shown\n" +
    "• Upload one VCF per selected group — bot matches each VCF to its group by checking pending phones\n" +
    "• Group name comes from the VCF filename (same or with your custom prefix)",
    { parse_mode: "HTML", reply_markup: kb }
  );
});

// ═══ MANUAL MODE ════════════════════════════════════════════════════════════

bot.callbackQuery("cgn_manual", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ WhatsApp not connected.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }
  await ctx.editMessageText("🔍 <b>Scanning your WhatsApp groups...</b>\n\n⌛ Please wait...", { parse_mode: "HTML" });

  const groups = await getAllGroups(String(userId));
  const adminGroups = groups.filter((g) => g.isAdmin);
  if (!adminGroups.length) {
    await ctx.editMessageText(
      "📭 <b>No admin groups found.</b>\n\nYou must be an admin in a group to rename it.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
    return;
  }

  const allSimple = adminGroups
    .map((g) => ({ id: g.id, subject: g.subject }))
    .sort((a, b) => a.subject.localeCompare(b.subject, undefined, { numeric: true, sensitivity: "base" }));
  const patterns = detectSimilarGroups(allSimple);

  userStates.set(userId, {
    step: "cgn_manual_menu",
    changeGroupNameData: {
      mode: "manual",
      allGroups: allSimple,
      patterns,
      selectedGroupIds: [],
      page: 0,
    },
  });

  const kb = new InlineKeyboard();
  if (patterns.length > 0) kb.text("🔗 Similar Groups", "cgn_m_similar").text("📋 All Groups", "cgn_m_all").row();
  else kb.text("📋 All Groups", "cgn_m_all").row();
  kb.text("🔙 Back", "change_group_name").text("🏠 Menu", "main_menu");

  await ctx.editMessageText(
    `✏️ <b>Manual Rename</b>\n\n` +
    `📱 Admin groups found: <b>${adminGroups.length}</b>\n` +
    (patterns.length > 0 ? `🔍 Similar patterns: <b>${patterns.length}</b>\n\n` : `\n`) +
    `Pick which set of groups to choose from:`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery("cgn_m_similar", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.patterns) return;
  const patterns = state.changeGroupNameData.patterns;
  if (!patterns.length) {
    await ctx.editMessageText("⚠️ No similar group patterns found.", {
      reply_markup: new InlineKeyboard().text("🔙 Back", "cgn_manual").text("🏠 Menu", "main_menu"),
    });
    return;
  }
  const kb = new InlineKeyboard();
  for (let i = 0; i < patterns.length; i++) {
    kb.text(`🔗 ${patterns[i].base} (${patterns[i].groups.length})`, `cgn_m_sim_${i}`).row();
  }
  kb.text("🔙 Back", "cgn_manual").text("🏠 Menu", "main_menu");
  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\nTap a pattern — its groups will be the selection pool:",
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery(/^cgn_m_sim_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.patterns) return;
  const idx = parseInt(ctx.match![1]);
  const pattern = state.changeGroupNameData.patterns[idx];
  if (!pattern) return;
  state.changeGroupNameData.selectionPool = pattern.groups;
  state.changeGroupNameData.selectionPoolLabel = pattern.base;
  state.changeGroupNameData.selectedGroupIds = [];
  state.changeGroupNameData.page = 0;
  state.step = "cgn_manual_select_pool";
  await renderCgnManualSelect(ctx);
});

bot.callbackQuery("cgn_m_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.allGroups) return;
  state.changeGroupNameData.selectionPool = state.changeGroupNameData.allGroups;
  state.changeGroupNameData.selectionPoolLabel = "All Admin Groups";
  state.changeGroupNameData.selectedGroupIds = [];
  state.changeGroupNameData.page = 0;
  state.step = "cgn_manual_select_pool";
  await renderCgnManualSelect(ctx);
});

function buildCgnManualKeyboard(state: UserState): InlineKeyboard {
  const data = state.changeGroupNameData!;
  const pool = data.selectionPool || [];
  const selectedIds = data.selectedGroupIds || [];
  const page = data.page || 0;
  const totalPages = Math.max(1, Math.ceil(pool.length / CGN_PAGE_SIZE));
  const start = page * CGN_PAGE_SIZE;
  const end = Math.min(start + CGN_PAGE_SIZE, pool.length);

  const kb = new InlineKeyboard();
  for (let i = start; i < end; i++) {
    const g = pool[i];
    const orderIdx = selectedIds.indexOf(g.id);
    const tag = orderIdx >= 0 ? `✅ ${orderIdx + 1}.` : "☐";
    kb.text(`${tag} ${g.subject}`, `cgn_m_tog_${i}`).row();
  }
  if (totalPages > 1) {
    const prev = page > 0 ? "⬅️ Prev" : " ";
    const next = page < totalPages - 1 ? "Next ➡️" : " ";
    kb.text(prev, "cgn_m_prev_page").text(`📄 ${page + 1}/${totalPages}`, "cgn_m_page_info").text(next, "cgn_m_next_page").row();
  }
  kb.text("☑️ Select All", "cgn_m_select_all").text("🧹 Clear", "cgn_m_clear_all").row();
  if (selectedIds.length > 0) kb.text(`▶️ Next: Choose Names (${selectedIds.length})`, "cgn_m_proceed").row();
  kb.text("🔙 Back", "cgn_manual").text("🏠 Menu", "main_menu");
  return kb;
}

async function renderCgnManualSelect(ctx: any) {
  const userId = ctx.from?.id ?? ctx.chat?.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  const data = state.changeGroupNameData;
  const count = (data.selectedGroupIds || []).length;
  await ctx.editMessageText(
    `✏️ <b>Manual Rename — Select Groups</b>\n\n` +
    `📂 Pool: <b>${esc(data.selectionPoolLabel || "")}</b> (${(data.selectionPool || []).length} groups)\n` +
    `📌 Selected: <b>${count}</b>\n\n` +
    `Tap groups in the order you want them renamed. Numbers on the buttons (1, 2, 3…) show your tap order — the bot will use the same order when you pick names.`,
    { parse_mode: "HTML", reply_markup: buildCgnManualKeyboard(state) }
  );
}

bot.callbackQuery(/^cgn_m_tog_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.selectionPool) return;
  const data = state.changeGroupNameData;
  const idx = parseInt(ctx.match![1]);
  const g = data.selectionPool![idx];
  if (!g) return;
  data.selectedGroupIds = data.selectedGroupIds || [];
  const at = data.selectedGroupIds.indexOf(g.id);
  if (at >= 0) data.selectedGroupIds.splice(at, 1);
  else data.selectedGroupIds.push(g.id);
  await renderCgnManualSelect(ctx);
});

bot.callbackQuery("cgn_m_prev_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  const data = state.changeGroupNameData;
  if ((data.page || 0) > 0) data.page = (data.page || 0) - 1;
  await renderCgnManualSelect(ctx);
});

bot.callbackQuery("cgn_m_next_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.selectionPool) return;
  const data = state.changeGroupNameData;
  const totalPages = Math.ceil(data.selectionPool!.length / CGN_PAGE_SIZE);
  if ((data.page || 0) < totalPages - 1) data.page = (data.page || 0) + 1;
  await renderCgnManualSelect(ctx);
});

bot.callbackQuery("cgn_m_page_info", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" });
});

bot.callbackQuery("cgn_m_select_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.selectionPool) return;
  const data = state.changeGroupNameData;
  data.selectedGroupIds = data.selectionPool!.map((g) => g.id);
  await renderCgnManualSelect(ctx);
});

bot.callbackQuery("cgn_m_clear_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  state.changeGroupNameData.selectedGroupIds = [];
  await renderCgnManualSelect(ctx);
});

bot.callbackQuery("cgn_m_proceed", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.selectedGroupIds?.length) return;
  state.step = "cgn_manual_naming_choose";
  const count = state.changeGroupNameData.selectedGroupIds.length;
  await ctx.editMessageText(
    `✏️ <b>Manual Rename — Choose Naming Mode</b>\n\n` +
    `📌 Selected groups: <b>${count}</b>\n\n` +
    `🔢 <b>Auto-numbered:</b> You give one base name, bot generates ${count} numbered names (e.g. "Spidy 1, Spidy 2, Spidy 3…"). If your base ends in a number, bot continues from that number.\n\n` +
    `✏️ <b>Custom Names:</b> You send all ${count} names yourself, one per line, in the same order you tapped the groups.`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🔢 Auto-numbered", "cgn_m_naming_auto")
        .text("✏️ Custom Names", "cgn_m_naming_custom").row()
        .text("🔙 Back", "cgn_m_all").text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("cgn_m_naming_auto", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  state.changeGroupNameData.namingMode = "auto";
  state.step = "cgn_manual_naming_auto_input";
  const count = (state.changeGroupNameData.selectedGroupIds || []).length;
  await ctx.editMessageText(
    `🔢 <b>Auto-numbered Names</b>\n\n` +
    `Send the <b>base name</b> for ${count} group(s).\n\n` +
    `Examples:\n` +
    `• <code>Spidy</code> → Spidy 1, Spidy 2, … Spidy ${count}\n` +
    `• <code>Spidy 5</code> → Spidy 5, Spidy 6, … Spidy ${4 + count} (continues numbering)`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("cgn_m_naming_custom", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  state.changeGroupNameData.namingMode = "custom";
  state.step = "cgn_manual_naming_custom_input";
  const count = (state.changeGroupNameData.selectedGroupIds || []).length;
  await ctx.editMessageText(
    `✏️ <b>Custom Names</b>\n\n` +
    `Send <b>${count}</b> names, one per line, in the order you tapped the groups:\n\n` +
    `<i>Example:\nSpidy Squad\nSpidy Gang\nSpidy Army</i>`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

// Build the rename plan for manual mode and show review
async function showCgnManualReview(ctx: any) {
  const userId = ctx.from?.id ?? ctx.chat?.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  const data = state.changeGroupNameData;
  const ids = data.selectedGroupIds || [];
  const allMap = new Map((data.allGroups || []).map((g) => [g.id, g.subject] as const));
  const plan: Array<{ groupId: string; oldName: string; newName: string }> = [];
  for (let i = 0; i < ids.length; i++) {
    plan.push({
      groupId: ids[i],
      oldName: allMap.get(ids[i]) || "(unknown)",
      newName: (data.finalNames || [])[i] || "(missing)",
    });
  }
  data.renamePlan = plan;
  state.step = "cgn_manual_review";
  const previewLines = plan.slice(0, 10)
    .map((p, i) => `${i + 1}. <code>${esc(p.oldName)}</code>\n   → <code>${esc(p.newName)}</code>`)
    .join("\n\n");
  const more = plan.length > 10 ? `\n\n<i>… +${plan.length - 10} more</i>` : "";
  const text =
    `📋 <b>Rename Review</b>\n\n` +
    `Groups to rename: <b>${plan.length}</b>\n\n${previewLines}${more}\n\n` +
    `🚀 Ready to rename?`;
  const markup = new InlineKeyboard()
    .text("✅ Start Rename", "cgn_confirm")
    .text("❌ Cancel", "main_menu");
  try { await ctx.editMessageText(text, { parse_mode: "HTML", reply_markup: markup }); }
  catch { await ctx.reply(text, { parse_mode: "HTML", reply_markup: markup }); }
}

// ═══ AUTO MODE ══════════════════════════════════════════════════════════════

bot.callbackQuery("cgn_auto", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ WhatsApp not connected.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }
  await ctx.editMessageText("⏳ <b>Fetching groups with pending requests...</b>\n\nPlease wait...", { parse_mode: "HTML" });

  const list = await getGroupPendingList(String(userId));
  const pendingOnly = list.filter((g) => g.pendingCount > 0);
  if (!pendingOnly.length) {
    await ctx.editMessageText(
      "📋 <b>Auto Rename</b>\n\nNo groups with pending requests found.\n\nThis mode only works for groups that have at least one pending member request — that's how the bot matches a VCF to the right group.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔙 Back", "change_group_name").text("🏠 Menu", "main_menu") }
    );
    return;
  }
  pendingOnly.sort((a, b) => a.groupName.localeCompare(b.groupName, undefined, { numeric: true, sensitivity: "base" }));

  userStates.set(userId, {
    step: "cgn_auto_select_pool",
    changeGroupNameData: {
      mode: "auto",
      pendingPool: pendingOnly,
      pendingSelectedIds: [],
      pendingPage: 0,
      vcfFiles: [],
    },
  });
  await renderCgnAutoSelect(ctx);
});

function buildCgnAutoSelectKeyboard(state: UserState): InlineKeyboard {
  const data = state.changeGroupNameData!;
  const pool = data.pendingPool || [];
  const selectedIds = data.pendingSelectedIds || [];
  const page = data.pendingPage || 0;
  const totalPages = Math.max(1, Math.ceil(pool.length / CGN_PAGE_SIZE));
  const start = page * CGN_PAGE_SIZE;
  const end = Math.min(start + CGN_PAGE_SIZE, pool.length);

  const kb = new InlineKeyboard();
  for (let i = start; i < end; i++) {
    const g = pool[i];
    const orderIdx = selectedIds.indexOf(g.groupId);
    const tag = orderIdx >= 0 ? `✅ ${orderIdx + 1}.` : "☐";
    kb.text(`${tag} ${g.groupName} (${g.pendingCount})`, `cgn_a_tog_${i}`).row();
  }
  if (totalPages > 1) {
    const prev = page > 0 ? "⬅️ Prev" : " ";
    const next = page < totalPages - 1 ? "Next ➡️" : " ";
    kb.text(prev, "cgn_a_prev_page").text(`📄 ${page + 1}/${totalPages}`, "cgn_a_page_info").text(next, "cgn_a_next_page").row();
  }
  kb.text("☑️ Select All", "cgn_a_select_all").text("🧹 Clear", "cgn_a_clear_all").row();
  if (selectedIds.length > 0) kb.text(`▶️ Next: Upload VCFs (${selectedIds.length})`, "cgn_a_proceed").row();
  kb.text("🔙 Back", "change_group_name").text("🏠 Menu", "main_menu");
  return kb;
}

async function renderCgnAutoSelect(ctx: any) {
  const userId = ctx.from?.id ?? ctx.chat?.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  const data = state.changeGroupNameData;
  const count = (data.pendingSelectedIds || []).length;
  await ctx.editMessageText(
    `📁 <b>Auto Rename — Select Groups</b>\n\n` +
    `📊 Groups with pending: <b>${(data.pendingPool || []).length}</b>\n` +
    `📌 Selected: <b>${count}</b>\n\n` +
    `Tap groups to select. After this you'll upload one VCF per group — the bot matches each VCF to the group whose pending list contains it.`,
    { parse_mode: "HTML", reply_markup: buildCgnAutoSelectKeyboard(state) }
  );
}

bot.callbackQuery(/^cgn_a_tog_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.pendingPool) return;
  const data = state.changeGroupNameData;
  const idx = parseInt(ctx.match![1]);
  const g = data.pendingPool![idx];
  if (!g) return;
  data.pendingSelectedIds = data.pendingSelectedIds || [];
  const at = data.pendingSelectedIds.indexOf(g.groupId);
  if (at >= 0) data.pendingSelectedIds.splice(at, 1);
  else data.pendingSelectedIds.push(g.groupId);
  await renderCgnAutoSelect(ctx);
});

bot.callbackQuery("cgn_a_prev_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  const data = state.changeGroupNameData;
  if ((data.pendingPage || 0) > 0) data.pendingPage = (data.pendingPage || 0) - 1;
  await renderCgnAutoSelect(ctx);
});

bot.callbackQuery("cgn_a_next_page", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.pendingPool) return;
  const data = state.changeGroupNameData;
  const totalPages = Math.ceil(data.pendingPool!.length / CGN_PAGE_SIZE);
  if ((data.pendingPage || 0) < totalPages - 1) data.pendingPage = (data.pendingPage || 0) + 1;
  await renderCgnAutoSelect(ctx);
});

bot.callbackQuery("cgn_a_page_info", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" });
});

bot.callbackQuery("cgn_a_select_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.pendingPool) return;
  const data = state.changeGroupNameData;
  data.pendingSelectedIds = data.pendingPool!.map((g) => g.groupId);
  await renderCgnAutoSelect(ctx);
});

bot.callbackQuery("cgn_a_clear_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  state.changeGroupNameData.pendingSelectedIds = [];
  await renderCgnAutoSelect(ctx);
});

bot.callbackQuery("cgn_a_proceed", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.pendingSelectedIds?.length) return;
  state.changeGroupNameData.vcfFiles = [];
  state.step = "cgn_auto_collect_vcf";
  const count = state.changeGroupNameData.pendingSelectedIds.length;
  await ctx.editMessageText(
    `📁 <b>Upload VCF Files</b>\n\n` +
    `Send <b>${count}</b> VCF file(s) — one per selected group.\n\n` +
    `📌 You can upload them in any order. The bot will match each VCF to the right group by checking which group's pending list contains the VCF's phone numbers.\n\n` +
    `Progress: <b>0 / ${count}</b> received`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

// Once all VCFs are uploaded, this is called by the document handler.
async function cgnAutoAfterVcfUploaded(ctx: any) {
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  const data = state.changeGroupNameData;
  const need = (data.pendingSelectedIds || []).length;
  const have = (data.vcfFiles || []).length;
  if (have < need) {
    await ctx.reply(
      `✅ VCF received (${have}/${need}). Send ${need - have} more.`,
      { reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
    );
    return;
  }
  // All VCFs collected — ask which naming mode
  state.step = "cgn_auto_name_choose";
  await ctx.reply(
    `✅ <b>All ${need} VCF file(s) received!</b>\n\n` +
    `Choose how the new group names should be built:\n\n` +
    `📁 <b>Same as VCF name</b>\n` +
    `Each group's new name = its VCF filename without ".vcf"\n` +
    `<i>e.g. "SPIDY 酒店回饋活動FL_61.vcf" → "SPIDY 酒店回饋活動FL_61"</i>\n\n` +
    `✏️ <b>Customize name</b>\n` +
    `You give a prefix like <code>SPIDY 酒店EMPIRE動FL_</code>. The bot keeps just the trailing number from each VCF filename and appends it.\n` +
    `<i>e.g. prefix "SPIDY 酒店EMPIRE動FL_" + VCF "..._61.vcf" → "SPIDY 酒店EMPIRE動FL_61"</i>`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("📁 Same as VCF name", "cgn_a_name_same")
        .text("✏️ Customize name", "cgn_a_name_custom").row()
        .text("❌ Cancel", "main_menu"),
    }
  );
}

bot.callbackQuery("cgn_a_name_same", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  state.changeGroupNameData.autoNameMode = "same_vcf";
  await buildAndShowCgnAutoReview(ctx);
});

bot.callbackQuery("cgn_a_name_custom", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  state.changeGroupNameData.autoNameMode = "custom_vcf";
  state.step = "cgn_auto_custom_prefix_input";
  await ctx.editMessageText(
    `✏️ <b>Custom Prefix</b>\n\n` +
    `Send the prefix you want before the trailing number from each VCF filename.\n\n` +
    `Example:\n` +
    `• Prefix: <code>SPIDY 酒店EMPIRE動FL_</code>\n` +
    `• VCF filename: <code>Expedia 酒店回饋活動FL_61.vcf</code>\n` +
    `• Final group name: <code>SPIDY 酒店EMPIRE動FL_61</code>\n\n` +
    `<i>Tip: include a separator (space, _, -) at the end of your prefix if you want one.</i>`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

// Build the rename plan for auto mode (matches each VCF to a group by
// pending-phone overlap) and show the review screen.
async function buildAndShowCgnAutoReview(ctx: any) {
  const userId = ctx.from?.id ?? ctx.chat?.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  const data = state.changeGroupNameData;
  const selectedIds = data.pendingSelectedIds || [];
  const vcfs = data.vcfFiles || [];

  // Tell the user we're matching — could take a few seconds for many groups.
  let matchingMsg: any = null;
  try {
    matchingMsg = await ctx.reply(
      `⏳ <b>Matching ${vcfs.length} VCF(s) to ${selectedIds.length} group(s)...</b>\n\nFetching pending requests for each group.`,
      { parse_mode: "HTML" }
    );
  } catch {}

  // For each selected group, fetch its pending phone numbers.
  // Then for each group, pick the VCF with most overlap (≥1) as its match.
  const poolMap = new Map((data.pendingPool || []).map((g) => [g.groupId, g] as const));
  const groupPendingPhones: Map<string, Set<string>> = new Map(); // last10 set
  for (const gid of selectedIds) {
    try {
      const detailed = await getGroupPendingRequestsDetailed(String(userId), gid);
      const last10s = new Set<string>();
      for (const p of detailed) {
        const cleaned = (p.phone || "").replace(/[^0-9]/g, "");
        if (cleaned.length >= 7) last10s.add(cleaned.slice(-10));
      }
      groupPendingPhones.set(gid, last10s);
    } catch {
      groupPendingPhones.set(gid, new Set());
    }
  }

  // Pre-compute each VCF's last-10 phone set
  const vcfLast10: Array<{ idx: number; fileName: string; last10: Set<string> }> = vcfs.map((v, i) => ({
    idx: i,
    fileName: v.fileName,
    last10: new Set(v.phones.map((p) => p.replace(/[^0-9]/g, "").slice(-10)).filter((s) => s.length >= 7)),
  }));

  // Greedy matching: for each VCF, find best group (most overlap, ≥1).
  // Once a group is taken, it can't be re-used. If two VCFs tie for the
  // same group, the higher-overlap VCF wins; the loser stays unmatched.
  const groupTaken = new Set<string>();
  const vcfToGroup = new Map<number, string>(); // vcfIdx -> groupId
  // Compute all (vcf, group) overlap candidates, sort descending, then assign.
  type Cand = { vcfIdx: number; groupId: string; overlap: number };
  const cands: Cand[] = [];
  for (const v of vcfLast10) {
    for (const gid of selectedIds) {
      const gset = groupPendingPhones.get(gid)!;
      let overlap = 0;
      for (const p of v.last10) if (gset.has(p)) overlap++;
      if (overlap > 0) cands.push({ vcfIdx: v.idx, groupId: gid, overlap });
    }
  }
  cands.sort((a, b) => b.overlap - a.overlap);
  const vcfTaken = new Set<number>();
  for (const c of cands) {
    if (vcfTaken.has(c.vcfIdx) || groupTaken.has(c.groupId)) continue;
    vcfToGroup.set(c.vcfIdx, c.groupId);
    vcfTaken.add(c.vcfIdx);
    groupTaken.add(c.groupId);
  }

  // Build rename plan in selection order. For each selected group:
  //   • find the matched VCF (if any)
  //   • compute new name based on autoNameMode
  const plan: Array<{ groupId: string; oldName: string; newName: string; vcfFileName?: string }> = [];
  // Reverse map: groupId -> matched vcf
  const groupToVcf = new Map<string, { vcfIdx: number; fileName: string }>();
  for (const [vidx, gid] of vcfToGroup.entries()) {
    groupToVcf.set(gid, { vcfIdx: vidx, fileName: vcfs[vidx].fileName });
  }

  for (const gid of selectedIds) {
    const groupName = poolMap.get(gid)?.groupName || "(unknown)";
    const matched = groupToVcf.get(gid);
    if (!matched) {
      plan.push({ groupId: gid, oldName: groupName, newName: "(no matching VCF — will skip)" });
      continue;
    }
    let newName = "";
    if (data.autoNameMode === "same_vcf") {
      newName = vcfBasename(matched.fileName);
    } else {
      const num = extractTrailingNumber(matched.fileName);
      newName = (data.customPrefix || "") + num;
    }
    plan.push({ groupId: gid, oldName: groupName, newName, vcfFileName: matched.fileName });
  }

  // Also note any VCFs that didn't match any group
  const unmatchedVcfs: string[] = [];
  for (const v of vcfLast10) {
    if (!vcfTaken.has(v.idx)) unmatchedVcfs.push(v.fileName);
  }

  data.renamePlan = plan;
  state.step = "cgn_auto_review";

  const previewLines = plan.slice(0, 12).map((p, i) => {
    const vcfTag = p.vcfFileName ? `   📁 ${esc(p.vcfFileName)}\n` : "";
    return `${i + 1}. <code>${esc(p.oldName)}</code>\n${vcfTag}   → <code>${esc(p.newName)}</code>`;
  }).join("\n\n");
  const more = plan.length > 12 ? `\n\n<i>… +${plan.length - 12} more</i>` : "";
  const validCount = plan.filter((p) => !p.newName.startsWith("(no matching")).length;
  const skipCount = plan.length - validCount;

  let warn = "";
  if (skipCount > 0) warn += `\n⚠️ ${skipCount} group(s) had no matching VCF — they will be skipped.`;
  if (unmatchedVcfs.length > 0) {
    warn += `\n⚠️ ${unmatchedVcfs.length} VCF(s) didn't match any group:\n` +
      unmatchedVcfs.slice(0, 3).map((n) => `   • ${esc(n)}`).join("\n");
    if (unmatchedVcfs.length > 3) warn += `\n   … +${unmatchedVcfs.length - 3} more`;
  }

  const text =
    `📋 <b>Auto Rename — Review</b>\n\n` +
    `Will rename: <b>${validCount}</b> / ${plan.length} groups${warn}\n\n` +
    `${previewLines}${more}\n\n` +
    `🚀 Ready to rename?`;
  const markup = new InlineKeyboard();
  if (validCount > 0) markup.text("✅ Start Rename", "cgn_confirm").text("❌ Cancel", "main_menu");
  else markup.text("🔙 Back", "change_group_name").text("🏠 Menu", "main_menu");

  try {
    if (matchingMsg) {
      await ctx.api.editMessageText(matchingMsg.chat.id, matchingMsg.message_id, text, {
        parse_mode: "HTML",
        reply_markup: markup,
      });
    } else {
      await ctx.reply(text, { parse_mode: "HTML", reply_markup: markup });
    }
  } catch {
    try { await ctx.reply(text, { parse_mode: "HTML", reply_markup: markup }); } catch {}
  }
}

// ═══ SHARED: Confirm + Background Rename + Cancel ═══════════════════════════

bot.callbackQuery("cgn_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.renamePlan) return;
  const plan = state.changeGroupNameData.renamePlan.filter((p) => !p.newName.startsWith("(no matching"));
  if (!plan.length) return;
  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  state.step = "cgn_renaming";
  state.changeGroupNameData.cancel = false;

  await ctx.editMessageText(
    `⏳ <b>Renaming ${plan.length} group(s)...</b>\n\n🔄 0/${plan.length} done...`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "cgn_cancel_request") }
  );

  void runChangeGroupNameBackground(userId, chatId, msgId, plan);
});

bot.callbackQuery("cgn_cancel_request", async (ctx) => {
  await ctx.answerCallbackQuery();
  cancelDialogActiveFor.add(ctx.from.id);
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard()
      .text("✅ Yes, Stop Renaming", "cgn_cancel_confirm")
      .text("↩️ Continue", "cgn_cancel_no"),
  });
});

bot.callbackQuery("cgn_cancel_no", async (ctx) => {
  cancelDialogActiveFor.delete(ctx.from.id);
  await ctx.answerCallbackQuery({ text: "Renaming continued" });
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard().text("❌ Cancel", "cgn_cancel_request"),
  });
});

bot.callbackQuery("cgn_cancel_confirm", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Stopping after current group..." });
  const state = userStates.get(ctx.from.id);
  if (state?.changeGroupNameData) state.changeGroupNameData.cancel = true;
  // Keep dialog flag on; background loop's cleanup clears it.
  await ctx.editMessageReplyMarkup({ reply_markup: undefined });
});

async function runChangeGroupNameBackground(
  userId: number,
  chatId: number,
  msgId: number,
  plan: Array<{ groupId: string; oldName: string; newName: string; vcfFileName?: string }>,
) {
  const results: Array<{ groupId: string; oldName: string; newName: string; ok: boolean; error?: string }> = [];
  let done = 0;
  let cancelled = false;

  for (let i = 0; i < plan.length; i++) {
    const state = userStates.get(userId);
    if (state?.changeGroupNameData?.cancel) {
      cancelled = true;
      break;
    }
    const p = plan[i];
    const r = await setGroupName(String(userId), p.groupId, p.newName);
    results.push({ groupId: p.groupId, oldName: p.oldName, newName: p.newName, ok: r.ok, error: r.error });
    done++;

    // Live progress — skip overwrite if user is staring at the cancel-confirm dialog.
    if (!cancelDialogActiveFor.has(userId)) {
      try {
        const last5 = results.slice(-5).map((res) => {
          const tag = res.ok ? "✅" : "❌";
          return `${tag} ${esc(res.oldName)} → ${esc(res.newName)}${res.ok ? "" : ` (${esc(res.error || "fail")})`}`;
        }).join("\n");
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Renaming ${done}/${plan.length}...</b>\n\n${last5}`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "cgn_cancel_request") }
        );
      } catch {}
    }

    // Small delay between renames to avoid hammering WhatsApp.
    if (i < plan.length - 1) await new Promise((r) => setTimeout(r, 800));
  }

  // Cleanup cancel-dialog flag so the next flow starts clean.
  cancelDialogActiveFor.delete(userId);

  const ok = results.filter((r) => r.ok).length;
  const fail = results.length - ok;
  const skipped = plan.length - results.length; // remaining when cancelled
  const header = cancelled
    ? `🛑 <b>Cancelled</b> (${ok} renamed, ${fail} failed, ${skipped} skipped)`
    : `🎉 <b>Done!</b> (${ok} renamed, ${fail} failed)`;
  const lines = results.map((r) => {
    if (r.ok) return `✅ ${esc(r.oldName)} → ${esc(r.newName)}`;
    return `❌ ${esc(r.oldName)} → ${esc(r.newName)} <i>(${esc(r.error || "fail")})</i>`;
  }).join("\n");

  const fullText = `${header}\n\n${lines}`;
  const chunks = splitMessage(fullText, 4000);
  try {
    await bot.api.editMessageText(chatId, msgId, chunks[0], {
      parse_mode: "HTML",
      reply_markup: chunks.length === 1
        ? new InlineKeyboard().text("🏠 Main Menu", "main_menu")
        : undefined,
    });
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    try {
      await bot.api.sendMessage(chatId, chunks[i], {
        parse_mode: "HTML",
        reply_markup: i === chunks.length - 1
          ? new InlineKeyboard().text("🏠 Main Menu", "main_menu")
          : undefined,
      });
    } catch {}
  }

  // Clear the user's state once we're done.
  const finalState = userStates.get(userId);
  if (finalState && finalState.step?.startsWith("cgn_")) {
    userStates.delete(userId);
  }
}


// ─── Add Members Feature ──────────────────────────────────────────────────────

bot.callbackQuery("add_members", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("📱 <b>WhatsApp not connected!</b>\n\nConnect first to use this feature.", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    });
    return;
  }
  userStates.set(userId, {
    step: "add_members_enter_link",
    addMembersData: {
      groupLink: "", groupId: "", groupName: "",
      groups: [], multiGroup: false,
      friendNumbers: [], adminContacts: [], navyContacts: [], memberContacts: [],
      totalToAdd: 0, mode: "", delaySeconds: 15, cancelled: false,
    },
  });
  await ctx.editMessageText(
    "➕ <b>Add Members to Group</b>\n\n" +
    "🔗 <b>Step 1:</b> WhatsApp group link(s) bhejo.\n\n" +
    "✅ <b>Single group:</b> Ek link (Friend + Admin/Navy/Member VCF support)\n" +
    "✅ <b>Multiple groups:</b> Multiple links, ek per line (sirf Friend numbers)\n\n" +
    "Example single:\n<code>https://chat.whatsapp.com/ABC123xyz</code>\n\n" +
    "Example multiple:\n<code>https://chat.whatsapp.com/ABC123\nhttps://chat.whatsapp.com/XYZ456</code>",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("am_skip_friends", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  state.addMembersData.friendNumbers = [];
  if (state.addMembersData.multiGroup) {
    await ctx.editMessageText(
      "❌ <b>Multiple groups mode mein friend numbers zaroori hain!</b>\n\nFriend numbers ke bina kuch add nahi hoga.\n\nFriend numbers bhejo ya feature restart karo.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔄 Restart", "add_members").text("🏠 Menu", "main_menu") }
    );
    return;
  }
  state.step = "add_members_admin_vcf";
  await ctx.editMessageText(
    "👑 <b>Step 3: Admin VCF File</b>\n\n" +
    "📁 Send Admin VCF file (.vcf)\n\n" +
    "Agar admin ka VCF nahi hai to Skip karo.",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "am_skip_admin").text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("am_skip_admin", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  state.addMembersData.adminContacts = [];
  state.step = "add_members_navy_vcf";
  await ctx.editMessageText(
    "⚓ <b>Step 4: Navy VCF File</b>\n\n" +
    "📁 Send Navy VCF file (.vcf)\n\n" +
    "Agar navy ka VCF nahi hai to Skip karo.",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "am_skip_navy").text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("am_skip_navy", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  state.addMembersData.navyContacts = [];
  state.step = "add_members_member_vcf";
  await ctx.editMessageText(
    "👥 <b>Step 5: Member VCF File</b>\n\n" +
    "📁 Send Member VCF file (.vcf)\n\n" +
    "Agar member ka VCF nahi hai to Skip karo.",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "am_skip_members").text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("am_skip_members", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  state.addMembersData.memberContacts = [];
  const d = state.addMembersData;
  const totalAvailable = d.friendNumbers.length + d.adminContacts.length + d.navyContacts.length + d.memberContacts.length;
  if (totalAvailable === 0) {
    await ctx.editMessageText(
      "❌ <b>No contacts provided!</b>\n\nAapne koi bhi friend number ya VCF file nahi diya. Kuch to dena padega add karne ke liye.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔄 Try Again", "add_members").text("🏠 Menu", "main_menu") }
    );
    return;
  }
  state.step = "add_members_total_count";
  const availLines: string[] = [];
  if (d.friendNumbers.length > 0) availLines.push(`👫 Friends: ${d.friendNumbers.length}`);
  if (d.adminContacts.length > 0) availLines.push(`👑 Admin: ${d.adminContacts.length}`);
  if (d.navyContacts.length > 0) availLines.push(`⚓ Navy: ${d.navyContacts.length}`);
  if (d.memberContacts.length > 0) availLines.push(`👥 Members: ${d.memberContacts.length}`);
  await ctx.editMessageText(
    "🔢 <b>Step 6: Total Members to Add</b>\n\n" +
    `📊 Available contacts:\n` +
    `${availLines.join("\n")}\n` +
    `━━━━━━━━━━━━━━━━━━\n` +
    `📋 Total available: <b>${totalAvailable}</b>\n\n` +
    `🔢 Kitna members add karna hai total? (Number bhejo)`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("am_mode_one_by_one", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  state.addMembersData.mode = "one_by_one";
  state.step = "add_members_set_delay";
  await ctx.editMessageText(
    "⏱️ <b>Set Adding Speed</b>\n\n" +
    "1 member add karne ke baad kitna wait karna hai?\n\n" +
    "⚡ Recommended: <b>15 seconds</b> (safe adding)\n\n" +
    "Time in seconds bhejo (e.g. <code>15</code>)\n" +
    "Ya recommended use karo:",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("✅ 15s (Recommended)", "am_delay_15").text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("am_delay_15", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  state.addMembersData.delaySeconds = 15;
  await showAddMembersReview(ctx, userId);
});

bot.callbackQuery("am_mode_together", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  state.addMembersData.mode = "together";
  state.addMembersData.delaySeconds = 0;
  await showAddMembersReview(ctx, userId);
});

// ─── Custom Add Mode ───────────────────────────────────────────────────────
function customCategoryOrder(d: NonNullable<UserState["addMembersData"]>): Array<"friend" | "admin" | "navy" | "member"> {
  const order: Array<"friend" | "admin" | "navy" | "member"> = [];
  if (d.friendNumbers.length > 0) order.push("friend");
  if (d.adminContacts.length > 0) order.push("admin");
  if (d.navyContacts.length > 0) order.push("navy");
  if (d.memberContacts.length > 0) order.push("member");
  return order;
}

function categoryLabel(c: "friend" | "admin" | "navy" | "member"): string {
  return c === "friend" ? "👫 Friend" : c === "admin" ? "👑 Admin" : c === "navy" ? "⚓ Navy" : "👥 Member";
}

function categoryCount(d: NonNullable<UserState["addMembersData"]>, c: "friend" | "admin" | "navy" | "member"): number {
  return c === "friend" ? d.friendNumbers.length : c === "admin" ? d.adminContacts.length : c === "navy" ? d.navyContacts.length : d.memberContacts.length;
}

async function showCustomBatchPrompt(ctx: any, userId: number) {
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  const d = state.addMembersData;
  const order = customCategoryOrder(d);
  // Find next category not yet set
  let nextCat: "friend" | "admin" | "navy" | "member" | null = null;
  for (const c of order) {
    const key = c === "friend" ? "customBatchFriend" : c === "admin" ? "customBatchAdmin" : c === "navy" ? "customBatchNavy" : "customBatchMember";
    if (d[key] === undefined) { nextCat = c; break; }
  }
  if (!nextCat) {
    state.addMembersData.delaySeconds = 5;
    await showAddMembersReview(ctx, userId);
    return;
  }
  state.addMembersData.customStep = nextCat;
  state.step = "add_members_custom_batch";
  const cnt = categoryCount(d, nextCat);
  const text =
    `🎯 <b>Custom Pace — ${categoryLabel(nextCat)}</b>\n\n` +
    `Available: <b>${cnt}</b> contacts\n\n` +
    `Ek baar mein kitne add karein?`;
  const kb = new InlineKeyboard()
    .text("1-1", "am_cb_1").text("2-2", "am_cb_2").text("3-3", "am_cb_3").row()
    .text("4-4", "am_cb_4").text("5-5", "am_cb_5").text("6-6", "am_cb_6").row()
    .text("7-7", "am_cb_7").text("8-8", "am_cb_8").text("9-9", "am_cb_9").row()
    .text("10-10", "am_cb_10").text("15-15", "am_cb_15").text("20-20", "am_cb_20").row()
    .text("✅ All Together", "am_cb_all").text("❌ Cancel", "main_menu");
  try { await ctx.editMessageText(text, { parse_mode: "HTML", reply_markup: kb }); }
  catch { await ctx.reply(text, { parse_mode: "HTML", reply_markup: kb }); }
}

bot.callbackQuery("am_mode_custom", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  state.addMembersData.mode = "custom";
  // reset custom batches
  state.addMembersData.customBatchFriend = undefined;
  state.addMembersData.customBatchAdmin = undefined;
  state.addMembersData.customBatchNavy = undefined;
  state.addMembersData.customBatchMember = undefined;
  await showCustomBatchPrompt(ctx, userId);
});

for (const [cb, val] of [
  ["am_cb_1", 1], ["am_cb_2", 2], ["am_cb_3", 3],
  ["am_cb_4", 4], ["am_cb_5", 5], ["am_cb_6", 6],
  ["am_cb_7", 7], ["am_cb_8", 8], ["am_cb_9", 9],
  ["am_cb_10", 10], ["am_cb_15", 15], ["am_cb_20", 20],
  ["am_cb_all", -1],
] as const) {
  bot.callbackQuery(cb, async (ctx) => {
    await ctx.answerCallbackQuery();
    const userId = ctx.from.id;
    const state = userStates.get(userId);
    if (!state?.addMembersData?.customStep) return;
    const d = state.addMembersData;
    const cat = d.customStep!;
    const total = categoryCount(d, cat);
    const batch = val === -1 ? total : val;
    if (cat === "friend") d.customBatchFriend = batch;
    else if (cat === "admin") d.customBatchAdmin = batch;
    else if (cat === "navy") d.customBatchNavy = batch;
    else d.customBatchMember = batch;
    await showCustomBatchPrompt(ctx, userId);
  });
}

// Map low-level WhatsApp errors to user-friendly English reasons
function formatAddError(errMsg: string): string {
  const msg = errMsg.toLowerCase();
  if (msg.includes("already")) return "Already in group";
  if (msg.includes("not on whatsapp") || msg.includes("not-exist") || msg.includes("404")) return "Number not on WhatsApp";
  if (msg.includes("recently")) return "Recently left the group — can't add right now";
  if (msg.includes("invite") || msg.includes("not-authorized") || msg.includes("403")) return "Privacy block — invite required (contact must allow being added)";
  if (msg.includes("rate") || msg.includes("429") || msg.includes("too many")) return "Rate limit hit — adding too fast";
  if (msg.includes("ban") || msg.includes("forbidden")) return "Action blocked — your WhatsApp may be banned/restricted";
  if (msg.includes("not connected") || msg.includes("disconnected")) return "WhatsApp disconnected";
  if (msg.includes("limit")) return "Group/account limit reached";
  if (msg.includes("timeout")) return "Request timed out";
  return errMsg;
}

function isSkippableError(errMsg: string): boolean {
  const m = errMsg.toLowerCase();
  return m.includes("already") || m.includes("not on whatsapp") || m.includes("recently");
}

async function showAddMembersReview(ctx: any, userId: number) {
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  const d = state.addMembersData;
  state.step = "add_members_confirm";
  const modeText =
    d.mode === "one_by_one" ? `1 by 1 (${d.delaySeconds}s delay)` :
    d.mode === "together" ? "All Together" :
    "Custom (per category pace)";
  let customLines = "";
  if (d.mode === "custom") {
    const order = customCategoryOrder(d);
    const parts: string[] = [];
    for (const c of order) {
      const cnt = categoryCount(d, c);
      const batch = c === "friend" ? d.customBatchFriend : c === "admin" ? d.customBatchAdmin : c === "navy" ? d.customBatchNavy : d.customBatchMember;
      const paceText = !batch ? "?" : batch >= cnt ? "All together" : `${batch}-${batch}`;
      parts.push(`  • ${categoryLabel(c)} (${cnt}) → ${paceText}`);
    }
    customLines = `\n🎯 <b>Custom pace:</b>\n${parts.join("\n")}\n`;
  }
  let reviewText: string;
  if (d.multiGroup) {
    const groupList = d.groups.slice(0, 5).map(g => `• ${esc(g.name)}`).join("\n");
    const moreGroups = d.groups.length > 5 ? `\n... +${d.groups.length - 5} more` : "";
    reviewText =
      "📋 <b>Add Members — Final Review (Multi-Group)</b>\n\n" +
      `📋 <b>Groups (${d.groups.length}):</b>\n${groupList}${moreGroups}\n\n` +
      `👫 Friends: ${d.friendNumbers.length}\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `🔢 Per group: <b>${d.friendNumbers.length}</b> friends\n` +
      `⚙️ Mode: <b>${modeText}</b>${customLines}\n\n` +
      `⚠️ Confirm karke Start karo:`;
  } else {
    const catLines: string[] = [];
    if (d.friendNumbers.length > 0) catLines.push(`👫 Friends: ${d.friendNumbers.length}`);
    if (d.adminContacts.length > 0) catLines.push(`👑 Admin VCF: ${d.adminContacts.length}`);
    if (d.navyContacts.length > 0) catLines.push(`⚓ Navy VCF: ${d.navyContacts.length}`);
    if (d.memberContacts.length > 0) catLines.push(`👥 Member VCF: ${d.memberContacts.length}`);
    reviewText =
      "📋 <b>Add Members — Final Review</b>\n\n" +
      `🔗 Group: <b>${esc(d.groupName)}</b>\n` +
      `📋 Group ID: <code>${esc(d.groupId)}</code>\n\n` +
      `${catLines.join("\n")}\n` +
      `━━━━━━━━━━━━━━━━━━\n` +
      `🔢 Total to add: <b>${d.totalToAdd}</b>\n` +
      `⚙️ Mode: <b>${modeText}</b>${customLines}\n\n` +
      `⚠️ Confirm karke Start karo:`;
  }
  const kb = {
    parse_mode: "HTML" as const,
    reply_markup: new InlineKeyboard()
      .text("✅ Start Adding", "am_start_adding")
      .text("❌ Cancel", "main_menu"),
  };
  try {
    await ctx.editMessageText(reviewText, kb);
  } catch {
    await ctx.reply(reviewText, kb);
  }
}

bot.callbackQuery("am_start_adding", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  const d = state.addMembersData;
  const chatId = ctx.chat!.id;

  addMembersCancelRequests.delete(userId);
  d.cancelled = false;

  if (d.multiGroup) {
    const statusMsg = await ctx.editMessageText(
      `⏳ <b>Multi-Group Adding Shuru...</b>\n\n` +
      `📋 Groups: ${d.groups.length}\n` +
      `👫 Friends per group: ${d.friendNumbers.length}\n\n` +
      `⌛ Starting...`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "am_cancel_adding") }
    );
    void startAddMembersMultiGroup(userId, d.groups, d.friendNumbers, d.delaySeconds, chatId, statusMsg.message_id);
    return;
  }

  const inGroup = await isUserInGroup(String(userId), d.groupId);
  if (!inGroup) {
    await ctx.editMessageText(
      "⏳ <b>Bot is not in this group!</b>\n\n" +
      "🔗 Pehle group join request bhej raha hun...\n" +
      "⌛ Admin approval ka wait kar raha hun...",
      { parse_mode: "HTML" }
    );

    const joinResult = await joinGroupWithLink(String(userId), d.groupLink);
    if (!joinResult.success) {
      await ctx.editMessageText(
        `❌ <b>Group join nahi ho paya!</b>\n\nError: ${esc(joinResult.error || "Unknown")}\n\n` +
        "Group admin se approval lein ya check karein ki link sahi hai.",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔄 Retry", "am_start_adding").text("🏠 Menu", "main_menu") }
      );
      return;
    }
  }

  if (d.mode === "one_by_one") {
    await startAddMembersOneByOne(ctx, userId, chatId);
  } else if (d.mode === "custom") {
    await startAddMembersCustom(ctx, userId, chatId);
  } else {
    await startAddMembersTogether(ctx, userId, chatId);
  }
});

bot.callbackQuery("am_cancel_adding", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "⛔ Adding stopped!" });
  addMembersCancelRequests.add(ctx.from.id);
});

async function startAddMembersMultiGroup(
  userId: number,
  groups: Array<{ link: string; id: string; name: string }>,
  friendNumbers: string[],
  delaySeconds: number,
  chatId: number,
  msgId: number
) {
  const contacts = friendNumbers.map(n => n.replace(/[^0-9]/g, "")).filter(n => n.length >= 7);
  const lines: string[] = [];

  for (const group of groups) {
    if (addMembersCancelRequests.has(userId)) {
      lines.push(`⛔ Cancelled — ${esc(group.name)} aur remaining skip.`);
      break;
    }
    lines.push(`\n⏳ <b>${esc(group.name)}</b> — Adding...`);
    try {
      await bot.api.editMessageText(chatId, msgId,
        `⏳ <b>Multi-Group Adding...</b>\n${lines.join("\n")}`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "am_cancel_adding") }
      );
    } catch {}

    const inGroup = await isUserInGroup(String(userId), group.id);
    if (!inGroup) {
      const joinResult = await joinGroupWithLink(String(userId), group.link);
      if (!joinResult.success) {
        lines[lines.length - 1] = `❌ <b>${esc(group.name)}</b> — Join fail: ${esc(joinResult.error || "Unknown")}`;
        continue;
      }
      await new Promise(r => setTimeout(r, 2000));
    }

    const result = await addGroupParticipantsBulk(String(userId), group.id, contacts);
    const addedCount = Array.isArray(result) ? result.filter(r => r.success).length : contacts.length;
    lines[lines.length - 1] = `✅ <b>${esc(group.name)}</b> — ${addedCount}/${contacts.length} added`;

    if (delaySeconds > 0) await new Promise(r => setTimeout(r, delaySeconds * 1000));
  }

  addMembersCancelRequests.delete(userId);
  userStates.delete(userId);

  const summary = lines.join("\n");
  try {
    await bot.api.editMessageText(chatId, msgId,
      `🎉 <b>Multi-Group Adding Done!</b>\n\n${summary}`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
  } catch {
    await bot.api.sendMessage(chatId,
      `🎉 <b>Multi-Group Adding Done!</b>\n\n${summary}`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
  }
}

function normalizePhoneForJid(raw: string): string {
  // Remove all non-digit chars (strip +, spaces, dashes, etc.)
  const digits = raw.replace(/[^0-9]/g, "");
  // Remove leading zero if present (e.g. 08012345678 → 8012345678)
  // WhatsApp JID uses international format without + or leading 0
  return digits.startsWith("0") && digits.length > 10 ? digits.slice(1) : digits;
}

function buildAddMembersList(d: NonNullable<UserState["addMembersData"]>): Array<{ phone: string; category: string }> {
  const seen = new Set<string>();
  const list: Array<{ phone: string; category: string }> = [];

  function addIfUnique(raw: string, category: string) {
    const phone = normalizePhoneForJid(raw);
    if (phone.length >= 7 && !seen.has(phone)) {
      seen.add(phone);
      list.push({ phone, category });
    }
  }

  for (const num of d.friendNumbers) addIfUnique(num, "Friend");
  for (const c of d.adminContacts) addIfUnique(c.phone, "Admin");
  for (const c of d.navyContacts) addIfUnique(c.phone, "Navy");
  for (const c of d.memberContacts) addIfUnique(c.phone, "Member");

  return list;
}

async function startAddMembersOneByOne(ctx: any, userId: number, chatId: number) {
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  const d = state.addMembersData;

  const allContacts = buildAddMembersList(d);
  const totalToAdd = Math.min(d.totalToAdd, allContacts.length);

  const statusMsg = await ctx.editMessageText(
    `⏳ <b>Adding Members 1 by 1...</b>\n\n` +
    `🔢 0/${totalToAdd} done\n` +
    `✅ Added: 0 | ❌ Skipped: 0\n\n` +
    `⌛ Starting...`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "am_cancel_adding") }
  );
  const msgId = statusMsg.message_id;

  void (async () => {
    let added = 0;
    let skipped = 0;
    let attempted = 0;
    let cancelled = false;
    const results: string[] = [];

    for (let i = 0; i < allContacts.length && added < totalToAdd; i++) {
      if (addMembersCancelRequests.has(userId)) {
        cancelled = true;
        break;
      }
      if (!isConnected(String(userId))) {
        results.push("⚠️ WhatsApp disconnected — stopping.");
        break;
      }

      const contact = allContacts[i];
      attempted++;
      const res = await addGroupParticipant(String(userId), d.groupId, contact.phone);

      if (res.success) {
        added++;
        results.push(`✅ +${contact.phone} (${contact.category})`);
      } else {
        const errMsg = res.error || "Failed";
        const friendly = formatAddError(errMsg);
        if (isSkippableError(errMsg)) {
          skipped++;
          results.push(`⏭️ +${contact.phone} (${contact.category}) — ${friendly}`);
        } else {
          // Real failure — surface specific reason instead of marking as added
          skipped++;
          results.push(`❌ +${contact.phone} (${contact.category}) — ${friendly}`);
        }
      }

      const lastResults = results.slice(-8).join("\n");
      try {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Adding Members 1 by 1...</b>\n\n` +
          `🔢 Progress: ${added}/${totalToAdd} added\n` +
          `✅ Added: ${added} | ⏭️ Skipped: ${skipped} | 📊 Tried: ${attempted}\n\n` +
          `📋 Recent:\n${lastResults}\n\n` +
          `⏱️ Next in ${d.delaySeconds}s...`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "am_cancel_adding") }
        );
      } catch {}

      if (added < totalToAdd && i < allContacts.length - 1 && !addMembersCancelRequests.has(userId)) {
        await new Promise(r => setTimeout(r, d.delaySeconds * 1000));
      }
    }

    addMembersCancelRequests.delete(userId);
    userStates.delete(userId);

    const summary =
      `${cancelled ? "⛔" : "✅"} <b>Add Members ${cancelled ? "Cancelled" : "Complete"}!</b>\n\n` +
      `🔗 Group: <b>${esc(d.groupName)}</b>\n` +
      `✅ Successfully Added: <b>${added}</b>\n` +
      `⏭️ Skipped: <b>${skipped}</b>\n` +
      `📊 Total Attempted: <b>${attempted}</b>\n` +
      (cancelled ? `\n⛔ <b>User ne adding cancel kar diya.</b>` : "");

    try {
      await bot.api.editMessageText(chatId, msgId, summary, {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
      });
    } catch {}
  })();
}

async function startAddMembersTogether(ctx: any, userId: number, chatId: number) {
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  const d = state.addMembersData;

  const allContacts = buildAddMembersList(d);
  const totalToAdd = Math.min(d.totalToAdd, allContacts.length);
  const contactsToAdd = allContacts.slice(0, totalToAdd);
  const phoneNumbers = contactsToAdd.map(c => c.phone);

  const statusMsg = await ctx.editMessageText(
    `⏳ <b>Adding ${totalToAdd} Members Together...</b>\n\n` +
    `🔢 Sending bulk add request...\n` +
    `⌛ Please wait... (background mein chal raha hai)`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "am_cancel_adding") }
  );
  const msgId = statusMsg.message_id;

  // Run in background so other users' bots are not blocked
  void (async () => {
    try {
      if (addMembersCancelRequests.has(userId)) {
        addMembersCancelRequests.delete(userId);
        userStates.delete(userId);
        try {
          await bot.api.editMessageText(chatId, msgId, "⛔ <b>Adding cancelled.</b>", {
            parse_mode: "HTML",
            reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
          });
        } catch {}
        return;
      }

      if (!isConnected(String(userId))) {
        userStates.delete(userId);
        try {
          await bot.api.editMessageText(chatId, msgId,
            "❌ <b>WhatsApp disconnected!</b>\n\nPlease reconnect and try again.",
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }
          );
        } catch {}
        return;
      }

      const results = await addGroupParticipantsBulk(String(userId), d.groupId, phoneNumbers);

      let added = 0;
      let skipped = 0;
      const resultLines: string[] = [];

      for (let i = 0; i < results.length; i++) {
        if (addMembersCancelRequests.has(userId)) break;
        const r = results[i];
        const cat = contactsToAdd[i].category;
        if (r.success) {
          added++;
          resultLines.push(`✅ +${r.phone} (${cat})`);
        } else {
          const errMsg = r.error || "Failed";
          const friendly = formatAddError(errMsg);
          if (isSkippableError(errMsg)) {
            skipped++;
            resultLines.push(`⏭️ +${r.phone} (${cat}) — ${friendly}`);
          } else {
            skipped++;
            resultLines.push(`❌ +${r.phone} (${cat}) — ${friendly}`);
          }
        }
      }

      addMembersCancelRequests.delete(userId);
      userStates.delete(userId);

      const lastLines = resultLines.slice(-15).join("\n");
      const summary =
        `✅ <b>Add Members Together — Complete!</b>\n\n` +
        `🔗 Group: <b>${esc(d.groupName)}</b>\n` +
        `✅ Successfully Added: <b>${added}</b>\n` +
        `⏭️ Skipped: <b>${skipped}</b>\n` +
        `📊 Total: <b>${results.length}</b>\n\n` +
        `📋 Results:\n${lastLines}` +
        (resultLines.length > 15 ? `\n... +${resultLines.length - 15} more` : "");

      try {
        await bot.api.editMessageText(chatId, msgId, summary, {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
        });
      } catch {}
    } catch (err: any) {
      userStates.delete(userId);
      addMembersCancelRequests.delete(userId);
      try {
        await bot.api.editMessageText(chatId, msgId,
          `❌ <b>Error:</b> ${esc(err?.message || "Unknown error")}`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
        );
      } catch {}
    }
  })();
}

async function startAddMembersCustom(ctx: any, userId: number, chatId: number) {
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  const d = state.addMembersData;

  // Build per-category contact lists, preserving uniqueness across the whole flow
  const seen = new Set<string>();
  function buildList(rawList: string[], category: string): Array<{ phone: string; category: string }> {
    const out: Array<{ phone: string; category: string }> = [];
    for (const raw of rawList) {
      const phone = normalizePhoneForJid(raw);
      if (phone.length >= 7 && !seen.has(phone)) {
        seen.add(phone);
        out.push({ phone, category });
      }
    }
    return out;
  }

  const order = customCategoryOrder(d);
  const categoryData: Array<{ cat: string; contacts: Array<{ phone: string; category: string }>; batch: number }> = [];
  for (const c of order) {
    const label = c === "friend" ? "Friend" : c === "admin" ? "Admin" : c === "navy" ? "Navy" : "Member";
    const raws =
      c === "friend" ? d.friendNumbers :
      c === "admin" ? d.adminContacts.map(x => x.phone) :
      c === "navy" ? d.navyContacts.map(x => x.phone) :
      d.memberContacts.map(x => x.phone);
    const contacts = buildList(raws, label);
    const batch =
      c === "friend" ? (d.customBatchFriend ?? contacts.length) :
      c === "admin" ? (d.customBatchAdmin ?? contacts.length) :
      c === "navy" ? (d.customBatchNavy ?? contacts.length) :
      (d.customBatchMember ?? contacts.length);
    categoryData.push({ cat: label, contacts, batch: Math.max(1, batch) });
  }

  const totalAvailable = categoryData.reduce((s, x) => s + x.contacts.length, 0);
  const totalToAdd = Math.min(d.totalToAdd, totalAvailable);

  const statusMsg = await ctx.editMessageText(
    `⏳ <b>Custom Adding Shuru...</b>\n\n` +
    `🔢 Target: 0/${totalToAdd}\n` +
    `⌛ Starting...`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "am_cancel_adding") }
  );
  const msgId = statusMsg.message_id;

  void (async () => {
    let added = 0;
    let skipped = 0;
    let attempted = 0;
    let cancelled = false;
    const results: string[] = [];

    outer: for (const cd of categoryData) {
      if (added >= totalToAdd) break;
      if (cd.contacts.length === 0) continue;
      results.push(`\n🔹 <b>${cd.cat}</b> (${cd.contacts.length}, batch=${cd.batch >= cd.contacts.length ? "all" : cd.batch})`);
      let i = 0;
      while (i < cd.contacts.length && added < totalToAdd) {
        if (addMembersCancelRequests.has(userId)) { cancelled = true; break outer; }
        if (!isConnected(String(userId))) {
          results.push("⚠️ WhatsApp disconnected — stopping.");
          break outer;
        }

        const slice = cd.contacts.slice(i, i + cd.batch);
        const phones = slice.map(s => s.phone);
        attempted += phones.length;

        const batchResults = await addGroupParticipantsBulk(String(userId), d.groupId, phones);
        for (let k = 0; k < batchResults.length; k++) {
          const r = batchResults[k];
          if (r.success) {
            added++;
            results.push(`✅ +${r.phone} (${cd.cat})`);
          } else {
            const errMsg = r.error || "Failed";
            const friendly = formatAddError(errMsg);
            skipped++;
            const icon = isSkippableError(errMsg) ? "⏭️" : "❌";
            results.push(`${icon} +${r.phone} (${cd.cat}) — ${friendly}`);
          }
          if (added >= totalToAdd) break;
        }

        i += cd.batch;

        const lastResults = results.slice(-10).join("\n");
        try {
          await bot.api.editMessageText(chatId, msgId,
            `⏳ <b>Custom Adding...</b>\n\n` +
            `🔢 Progress: ${added}/${totalToAdd}\n` +
            `✅ Added: ${added} | ⏭️/❌ Skipped: ${skipped} | 📊 Tried: ${attempted}\n\n` +
            `📋 Recent:\n${lastResults}\n\n` +
            (i < cd.contacts.length && added < totalToAdd ? `⏱️ Next batch in 5s...` : ""),
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "am_cancel_adding") }
          );
        } catch {}

        if (i < cd.contacts.length && added < totalToAdd && !addMembersCancelRequests.has(userId)) {
          await new Promise(r => setTimeout(r, 5000));
        }
      }
    }

    addMembersCancelRequests.delete(userId);
    userStates.delete(userId);

    const summary =
      `${cancelled ? "⛔" : "✅"} <b>Custom Add ${cancelled ? "Cancelled" : "Complete"}!</b>\n\n` +
      `🔗 Group: <b>${esc(d.groupName)}</b>\n` +
      `✅ Successfully Added: <b>${added}</b>\n` +
      `⏭️/❌ Skipped/Failed: <b>${skipped}</b>\n` +
      `📊 Total Attempted: <b>${attempted}</b>\n` +
      (cancelled ? `\n⛔ <b>User ne adding cancel kar diya.</b>` : "");

    try {
      await bot.api.editMessageText(chatId, msgId, summary, {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
      });
    } catch {}
  })();
}

// ─── Text Handler ─────────────────────────────────────────────────────────────

bot.on("message:text", async (ctx) => {
  const userId = ctx.from.id;
  await trackUser(userId);
  if (await isBanned(userId)) return;
  const state = userStates.get(userId);
  const text = ctx.message.text.trim();

  if (!state) {
    if (text.toLowerCase() === "start") {
      if (await isBanned(userId)) return;
      if (!(await hasAccess(userId))) {
        const data = await loadBotData();
        if (data.referMode) {
          await sendReferRequired(ctx, userId);
        } else {
          await ctx.reply(
            `🔒 <b>Subscription Required!</b>\n\n👤 Contact owner: <b>${OWNER_USERNAME}</b>`,
            { parse_mode: "HTML" }
          );
        }
        return;
      }
      await ctx.reply(
        mainMenuText(userId, "welcome"),
        { parse_mode: "HTML", reply_markup: mainMenu(userId) }
      );
      return;
    }
    // Block free-text interactions when refer mode is on and the user has
    // no access — same UX as button presses.
    const data = await loadBotData();
    if (data.referMode && !isAdmin(userId)) {
      const state2 = await getAccessState(userId);
      if (state2.kind === "none") {
        await sendReferRequired(ctx, userId);
        return;
      }
    }
    await ctx.reply("💬 Use /start to begin.");
    return;
  }

  // ── Change Group Name: text inputs ──
  if (state.step === "cgn_manual_naming_auto_input" && state.changeGroupNameData) {
    const data = state.changeGroupNameData;
    const count = (data.selectedGroupIds || []).length;
    if (!text) {
      await ctx.reply("⚠️ Empty name. Send a base name like <code>Spidy</code>.", { parse_mode: "HTML" });
      return;
    }
    data.baseName = text;
    data.finalNames = generateGroupNames(text, count);
    await showCgnManualReview(ctx);
    return;
  }
  if (state.step === "cgn_manual_naming_custom_input" && state.changeGroupNameData) {
    const data = state.changeGroupNameData;
    const count = (data.selectedGroupIds || []).length;
    const lines = text.split(/\r?\n/).map((l) => l.trim()).filter((l) => l.length > 0);
    if (lines.length !== count) {
      await ctx.reply(
        `⚠️ Got <b>${lines.length}</b> name(s) but selected <b>${count}</b> group(s). Send exactly ${count} names, one per line.`,
        { parse_mode: "HTML" }
      );
      return;
    }
    data.finalNames = lines;
    await showCgnManualReview(ctx);
    return;
  }
  if (state.step === "cgn_auto_custom_prefix_input" && state.changeGroupNameData) {
    const data = state.changeGroupNameData;
    if (!text) {
      await ctx.reply("⚠️ Empty prefix. Send the prefix text (e.g. <code>SPIDY 酒店EMPIRE動FL_</code>).", { parse_mode: "HTML" });
      return;
    }
    data.customPrefix = text;
    await buildAndShowCgnAutoReview(ctx);
    return;
  }

  if (state.step === "acig_enter_message" && state.chatInGroupData) {
    state.chatInGroupData.message = text;
    state.step = "acig_confirm";
    const data = state.chatInGroupData;
    const selectedGroups = [...data.selectedIndices].map(i => data.allGroups[i]);
    const previewGroups = selectedGroups.slice(0, 5).map(g => `• ${esc(g.subject)}`).join("\n");
    const moreText = selectedGroups.length > 5 ? `\n... +${selectedGroups.length - 5} more` : "";
    await ctx.reply(
      `✅ <b>Message Set!</b>\n\n` +
      `📝 Message: <i>${esc(text.substring(0, 100))}${text.length > 100 ? "..." : ""}</i>\n\n` +
      `📋 Groups (${selectedGroups.length}):\n${previewGroups}${moreText}\n\n` +
      `⏱️ Delay: ${data.delaySeconds}s per group\n` +
      `🤖 Dono WhatsApp se bhejnha hai\n\n` +
      `Confirm karo?`,
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("✅ Start", "acig_confirm_start")
          .text("❌ Cancel", "main_menu"),
      }
    );
    return;
  }

  if (state.step === "awaiting_phone") {
    const phone = "+" + text.replace(/[^0-9]/g, "");
    if (!/^\+\d{10,15}$/.test(phone)) {
      await ctx.reply("❌ Invalid phone number.\nExample: <code>+919942222222</code>\nYa: <code>+91 (9999) 222222</code>", { parse_mode: "HTML" }); return;
    }
    userStates.delete(userId);
    const statusMsg = await ctx.reply(
      `⏳ <b>Connecting...</b>\n\n📱 Phone: <code>${esc(phone)}</code>\n\n⌛ Getting pairing code, please wait 10-20 seconds...`,
      { parse_mode: "HTML" }
    );
    try {
      await connectWhatsApp(String(userId), phone,
        async (code) => {
          try {
            await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
              `🔑 <b>Pairing Code:</b>\n\n<code>${esc(code)}</code>\n\n` +
              `📋 <b>Steps:</b>\n1️⃣ Open WhatsApp on your phone\n2️⃣ Settings → Linked Devices\n` +
              `3️⃣ Tap "Link a Device"\n4️⃣ Tap "Link with phone number instead"\n` +
              `5️⃣ Enter code: <code>${esc(code)}</code>\n\n⌛ Waiting for confirmation...`,
              { parse_mode: "HTML" }
            );
          } catch {}
        },
        async () => {
          try {
            await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
              whatsappConnectedText(userId, "🎉 All features are now available."),
              { parse_mode: "HTML", reply_markup: mainMenu(userId) }
            );
          } catch {}
        },
        async (reason) => {
          try {
            await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
              `⚠️ <b>WhatsApp Disconnected</b>\n\nReason: ${esc(reason)}\n\n🔄 Try connecting again.`,
              { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect Again", "connect_wa").text("🏠 Menu", "main_menu") }
            );
          } catch {}
        }
      );
    } catch (err: any) {
      console.error(`[BOT] connectWhatsApp threw for user ${userId}:`, err?.message);
      try {
        await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
          `❌ <b>Connection Failed</b>\n\nError: ${esc(err?.message || "Unknown error")}\n\n🔄 Please try again.`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Try Again", "connect_wa").text("🏠 Menu", "main_menu") }
        );
      } catch {}
    }
    return;
  }

  if (state.step === "auto_connect_phone") {
    const phone = text.replace(/\s/g, "");
    if (!/^\+?\d{10,15}$/.test(phone)) {
      await ctx.reply("❌ Invalid phone number.\nExample: <code>919876543210</code>", { parse_mode: "HTML" }); return;
    }
    userStates.delete(userId);
    const autoUserId = getAutoUserId(String(userId));
    const statusMsg = await ctx.reply(
      `⏳ <b>Auto Chat WA Connecting...</b>\n\n📱 Phone: <code>${esc(phone)}</code>\n\n⌛ Pairing code aa raha hai, 10-20 seconds wait karo...`,
      { parse_mode: "HTML" }
    );
    try {
      await connectWhatsApp(autoUserId, phone,
        async (code) => {
          try {
            await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
              `🔑 <b>Auto Chat WA Pairing Code:</b>\n\n<code>${esc(code)}</code>\n\n` +
              `📋 <b>Steps:</b>\n1️⃣ 2nd WhatsApp open karo\n2️⃣ Settings → Linked Devices\n` +
              `3️⃣ Tap "Link a Device"\n4️⃣ Tap "Link with phone number instead"\n` +
              `5️⃣ Code enter karo: <code>${esc(code)}</code>\n\n⌛ Confirm hone ka wait kar raha hun...`,
              { parse_mode: "HTML" }
            );
          } catch {}
        },
        async () => {
          try {
            const autoNumber = getAutoConnectedNumber(String(userId));
            await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
              `✅ <b>Auto Chat WhatsApp Connected!</b>\n\n` +
              (autoNumber ? `📞 Auto Number: <code>${esc(autoNumber)}</code>\n\n` : "") +
              `🎉 Ab Auto Chat use kar sakte ho!`,
              { parse_mode: "HTML", reply_markup: mainMenu(userId) }
            );
          } catch {}
        },
        async (reason) => {
          try {
            await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
              `⚠️ <b>Auto Chat WA Disconnected</b>\n\nReason: ${esc(reason)}\n\n🔄 Dobara try karo.`,
              { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🤖 Connect Auto WA", "connect_auto_wa").text("🏠 Menu", "main_menu") }
            );
          } catch {}
        }
      );
    } catch (err: any) {
      console.error(`[BOT] auto connectWhatsApp threw for user ${userId}:`, err?.message);
      try {
        await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
          `❌ <b>Connection Failed</b>\n\nError: ${esc(err?.message || "Unknown error")}\n\n🔄 Please try again.`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🤖 Connect Auto WA", "connect_auto_wa").text("🏠 Menu", "main_menu") }
        );
      } catch {}
    }
    return;
  }

  if (state.step === "cig_enter_message" && state.chatInGroupData) {
    state.chatInGroupData.message = text;
    state.step = "cig_confirm";
    const data = state.chatInGroupData;
    const selectedGroups = [...data.selectedIndices].map(i => data.allGroups[i]);
    const previewGroups = selectedGroups.slice(0, 5).map(g => `• ${esc(g.subject)}`).join("\n");
    const moreText = selectedGroups.length > 5 ? `\n... +${selectedGroups.length - 5} more` : "";
    await ctx.reply(
      `✅ <b>Message Set!</b>\n\n` +
      `📝 Message: <i>${esc(text.substring(0, 100))}${text.length > 100 ? "..." : ""}</i>\n\n` +
      `📋 Groups (${selectedGroups.length}):\n${previewGroups}${moreText}\n\n` +
      `⏱️ Delay: ${data.delaySeconds}s per group\n\n` +
      `Message bhejun?`,
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("✅ Yes, Send", "cig_start_confirm")
          .text("❌ Cancel", "cig_cancel_confirm"),
      }
    );
    return;
  }

  if (state.step === "auto_chat_set_message" && state.chatInGroupData) {
    state.chatInGroupData.message = text;
    state.step = "auto_chat_confirm";
    const data = state.chatInGroupData;
    const selectedGroups = [...data.selectedIndices].map(i => data.allGroups[i]);
    const previewGroups = selectedGroups.slice(0, 5).map(g => `• ${esc(g.subject)}`).join("\n");
    const moreText = selectedGroups.length > 5 ? `\n... +${selectedGroups.length - 5} more` : "";
    await ctx.reply(
      `✅ <b>Auto Chat Setup Ready!</b>\n\n` +
      `📝 Message: <i>${esc(text.substring(0, 100))}${text.length > 100 ? "..." : ""}</i>\n\n` +
      `📋 Groups (${selectedGroups.length}):\n${previewGroups}${moreText}\n\n` +
      `⏱️ Delay: ${data.delaySeconds}s\n\n` +
      `Auto Chat shuru karoon?`,
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("✅ Start", "auto_chat_confirm_start")
          .text("❌ Cancel", "main_menu"),
      }
    );
    return;
  }

  if (state.step === "group_enter_name") {
    if (!state.groupSettings) return;
    state.groupSettings.name = text;
    state.step = "group_enter_count";
    await ctx.reply("🔢 <b>How many groups?</b>\n\nEnter number (1-50):", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") });
    return;
  }

  if (state.step === "group_enter_count") {
    if (!state.groupSettings) return;
    const count = parseInt(text);
    if (isNaN(count) || count < 1 || count > 50) { await ctx.reply("❌ Enter a valid number (1-50)."); return; }
    state.groupSettings.count = count;
    if (count === 1) {
      state.groupSettings.finalNames = [state.groupSettings.name];
      state.step = "group_enter_description";
      await ctx.reply("📄 <b>Group Description</b>\n\nSend description or type <code>skip</code>:", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") });
    } else {
      state.step = "group_naming_mode";
      await ctx.reply(
        `🏷️ <b>Naming Mode</b>\n\nCreating <b>${count} groups</b>. How to name them?`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔢 Auto-numbered", "naming_auto").text("✏️ Custom Names", "naming_custom").row().text("❌ Cancel", "main_menu") }
      );
    }
    return;
  }

  if (state.step === "group_enter_custom_names") {
    if (!state.groupSettings) return;
    const names = text.split("\n").map((n) => n.trim()).filter((n) => n.length > 0);
    if (names.length !== state.groupSettings.count) {
      await ctx.reply(`❌ Need <b>${state.groupSettings.count}</b> names, got <b>${names.length}</b>.\n\nSend exactly ${state.groupSettings.count} names, one per line.`, { parse_mode: "HTML" }); return;
    }
    state.groupSettings.finalNames = names;
    state.step = "group_enter_description";
    const preview = names.slice(0, 5).map((n, i) => `${i + 1}. ${esc(n)}`).join("\n");
    await ctx.reply(
      `✅ <b>Names saved:</b>\n${preview}${names.length > 5 ? `\n... +${names.length - 5} more` : ""}\n\n📄 <b>Group Description</b>\n\nSend description or type <code>skip</code>:`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
    );
    return;
  }

  if (state.step === "group_enter_description") {
    if (!state.groupSettings) return;
    state.groupSettings.description = text.toLowerCase() === "skip" ? "" : text;
    state.step = "group_settings";
    await ctx.reply(settingsText(state.groupSettings), { parse_mode: "HTML", reply_markup: settingsKeyboard(state.groupSettings) });
    return;
  }

  if (state.step === "edit_settings_desc") {
    if (!state.editSettingsData) return;
    state.editSettingsData.settings.description = text.toLowerCase() === "skip" ? "" : text;
    await showEditSettingsReview(ctx);
    return;
  }

  if (state.step === "group_enter_friends") {
    if (!state.groupSettings) return;
    const lines = text.split("\n").map(l => l.trim()).filter(l => l.length > 0);
    const numbers: string[] = [];
    for (const line of lines) {
      const cleaned = line.replace(/[^0-9]/g, "");
      if (cleaned.length >= 10) numbers.push(cleaned);
    }
    if (numbers.length === 0) {
      await ctx.reply(
        "❌ No valid number found.\n\nAccepted formats:\n" +
        "<code>919912345678\n+919912345678\n+91 9912 345678\n+91 (9912) 345678</code>\n\n" +
        "Country code (e.g. 91 for India) is required. Or tap Skip.",
        {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard().text("⏭️ Skip", "group_skip_friends").text("❌ Cancel", "main_menu"),
        }
      );
      return;
    }
    state.groupSettings.friendNumbers = numbers;
    await ctx.reply(`✅ <b>${numbers.length} friend number(s) saved!</b>`, { parse_mode: "HTML" });
    await showGroupFriendAdminStep(ctx);
    return;
  }

  if (state.step === "join_enter_links") {
    if (!state.joinData) return;
    const cleanLinks = extractLinksFromText(text);
    if (!cleanLinks.length) { await ctx.reply("❌ No valid WhatsApp links found.\nExample:\n<code>https://chat.whatsapp.com/ABC123</code>", { parse_mode: "HTML" }); return; }
    userStates.delete(userId);
    joinCancelRequests.delete(userId);
    const statusMsg = await ctx.reply(`⏳ <b>Joining ${cleanLinks.length} group(s)...</b>\n\n🔄 0/${cleanLinks.length} done...`, {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("❌ Cancel", "join_cancel_request"),
    });
    const joinChatId = ctx.chat.id;
    const joinMsgId = statusMsg.message_id;
    void (async () => {
      let result = "🔗 <b>Join Groups Result</b>\n\n";
      const results: string[] = [];
      let cancelled = false;
      for (let ji = 0; ji < cleanLinks.length; ji++) {
        if (joinCancelRequests.has(userId)) {
          cancelled = true;
          results.push(`⛔ Cancelled. ${cleanLinks.length - ji} group(s) not joined.`);
          break;
        }
        const res = await joinGroupWithLink(String(userId), cleanLinks[ji]);
        const line = res.success ? `✅ Joined Group: ${esc(res.groupName || "Group")}` : `❌ Failed: ${esc(res.error || "Unknown")}`;
        results.push(line);
        // Skip overwrite if user is staring at the cancel-confirm dialog —
        // otherwise the Yes/No buttons get wiped and cancel looks broken.
        if (!cancelDialogActiveFor.has(userId)) {
          try {
            await bot.api.editMessageText(joinChatId, joinMsgId,
              `⏳ <b>Joining: ${ji + 1}/${cleanLinks.length}</b>\n\n${results.join("\n")}`,
              { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "join_cancel_request") }
            );
          } catch {}
        }
        if (ji < cleanLinks.length - 1) await new Promise((r) => setTimeout(r, 1500));
      }
      joinCancelRequests.delete(userId);
      cancelDialogActiveFor.delete(userId);
      result += results.join("\n");
      if (cancelled) result += "\n\n⛔ <b>Joining stopped by user.</b>";
      try {
        await bot.api.editMessageText(joinChatId, joinMsgId, result, {
          parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
        });
      } catch {}
    })();
    return;
  }

  if (state.step === "ctc_enter_links") {
    if (!state.ctcData) return;
    const cleanLinks = extractLinksFromText(text);
    if (!cleanLinks.length) { await ctx.reply("❌ No valid WhatsApp links found.", { parse_mode: "HTML" }); return; }
    state.ctcData.groupLinks = cleanLinks;
    state.ctcData.pairs = cleanLinks.map((link) => ({ link, vcfContacts: [] }));
    state.ctcData.currentPairIndex = 0;
    state.step = "ctc_enter_vcf";
    await ctx.reply(
      `✅ <b>${cleanLinks.length} group link(s) saved!</b>\n\n` +
      `📁 <b>Step 2: Send VCF file(s)</b>\n\n` +
      `You can send:\n` +
      `• One VCF for all groups\n` +
      `• Multiple VCFs one by one (one per group in order)\n\n` +
      `Send VCF for <b>Group 1/${cleanLinks.length}</b>:\n<code>${esc(cleanLinks[0])}</code>\n\n` +
      `When ready, tap <b>Start Check</b>:`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("▶️ Start Check", "ctc_start_check").text("❌ Cancel", "main_menu") }
    );
    return;
  }

  // Handle exclude numbers for remove members
  if (state.step === "remove_exclude_numbers") {
    if (!state.removeExcludeData) return;
    const lines = text.split("\n").map(l => l.trim()).filter(l => l.length > 0);
    const excludeNumbers = new Set<string>();
    const excludePrefixes = new Set<string>();
    for (const line of lines) {
      const digits = line.replace(/[^0-9]/g, "");
      if (digits.length === 0) continue;
      // 1-4 digits → treated as a country-code prefix (excludes ALL numbers
      // from that country in the group).
      // 7+ digits → treated as a full phone number (exact match by last 10
      // digits, original behavior).
      // 5-6 digits → ambiguous, ignored.
      if (digits.length >= 1 && digits.length <= 4) {
        excludePrefixes.add(digits);
      } else if (digits.length >= 7) {
        excludeNumbers.add(line.replace(/[^0-9+]/g, ""));
      }
    }

    if (excludeNumbers.size === 0 && excludePrefixes.size === 0) {
      await ctx.reply(
        "❌ Koi valid input nahi mila.\n\n" +
        "• Pura number bhejo with country code (e.g. <code>+919912345678</code>), ya\n" +
        "• Sirf country code bhejo (1-4 digits, e.g. <code>+91</code> ya <code>91</code>)\n\n" +
        "Ya Skip dabao to kuch bhi exclude nahi hoga.",
        {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard().text("⏭️ Skip", "rm_skip_exclude").text("❌ Cancel", "main_menu"),
        }
      );
      return;
    }

    const sections: string[] = [];
    if (excludeNumbers.size > 0) {
      const numList = Array.from(excludeNumbers).map(n => `• ${esc(n)}`).join("\n");
      sections.push(`✅ <b>${excludeNumbers.size} number(s) will be excluded:</b>\n\n${numList}`);
    }
    if (excludePrefixes.size > 0) {
      const prefList = Array.from(excludePrefixes).map(p => `• +${esc(p)} <i>(saare numbers iss country code se)</i>`).join("\n");
      sections.push(`🌐 <b>${excludePrefixes.size} country code(s) will be excluded:</b>\n\n${prefList}`);
    }

    await ctx.reply(
      sections.join("\n\n") +
      `\n\n⚠️ Ye sab numbers groups se NOT remove honge.`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("✅ Confirm & Start", "rm_confirm_with_exclude").text("❌ Cancel", "main_menu") }
    );
    state.removeExcludeData.excludeNumbers = excludeNumbers;
    state.removeExcludeData.excludePrefixes = excludePrefixes;
    state.step = "remove_exclude_confirm";
    return;
  }

  if (state.step === "approval_admin_input") {
    if (!state.approvalData) return;
    const phoneLines = text.split("\n").map(l => l.trim()).filter(l => l.length > 0);
    const phoneNumbers: string[] = [];
    for (const line of phoneLines) {
      const cleaned = line.replace(/[^0-9]/g, "");
      if (cleaned.length >= 7) phoneNumbers.push(cleaned);
    }
    if (phoneNumbers.length === 0) {
      await ctx.reply(
        "❌ No valid phone numbers found. Please send numbers with country code like +919912345678",
        { reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
      );
      return;
    }
    state.approvalData.targetPhones = phoneNumbers;
    await showAdminApprovalChoice(ctx, userId);
    return;
  }

  if (state.step === "make_admin_enter_numbers") {
    if (!state.makeAdminData) return;
    const phoneLines = text.split("\n").map(l => l.trim()).filter(l => l.length > 0);
    const phoneNumbers: string[] = [];
    for (const line of phoneLines) {
      const cleaned = line.replace(/[^0-9+]/g, "");
      if (cleaned.length >= 7) phoneNumbers.push(cleaned);
    }

    if (phoneNumbers.length === 0) {
      await ctx.reply("❌ No valid phone numbers found. Please send numbers with country code like +919912345678",
        { reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
      );
      return;
    }

    const selectedGroups = Array.from(state.makeAdminData.selectedIndices).map(i => state.makeAdminData!.allGroups[i]);
    const chatId = ctx.chat.id;
    userStates.delete(userId);

    const statusMsg = await ctx.reply(
      `⏳ <b>Making ${phoneNumbers.length} number(s) admin in ${selectedGroups.length} group(s)...</b>\n\n⌛ Please wait...`,
      { parse_mode: "HTML" }
    );

    void makeAdminBackground(String(userId), selectedGroups, phoneNumbers, chatId, statusMsg.message_id);
    return;
  }

  if (state.step === "add_members_enter_link") {
    if (!state.addMembersData) return;
    const cleanLinks = extractLinksFromText(text);
    if (!cleanLinks.length) {
      await ctx.reply("❌ No valid WhatsApp group link found.\nExample: <code>https://chat.whatsapp.com/ABC123</code>", { parse_mode: "HTML" });
      return;
    }
    const isMulti = cleanLinks.length > 1;
    const statusMsg = await ctx.reply(`⏳ ${isMulti ? `${cleanLinks.length} groups` : "Group"} info fetch kar raha hun...`, { parse_mode: "HTML" });
    const groups: Array<{ link: string; id: string; name: string }> = [];
    let failedLinks = 0;
    for (const link of cleanLinks) {
      const groupInfo = await getGroupIdFromLink(String(userId), link);
      if (groupInfo) groups.push({ link, id: groupInfo.id, name: groupInfo.subject });
      else failedLinks++;
    }
    try { await ctx.api.deleteMessage(ctx.chat.id, statusMsg.message_id); } catch {}
    if (!groups.length) {
      await ctx.reply(
        "❌ <b>Kisi bhi group ka info nahi mila!</b>\n\nCheck karein:\n• Links sahi hain\n• WhatsApp connected hai\n• Links expired nahi hain",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔄 Try Again", "add_members").text("🏠 Menu", "main_menu") }
      );
      return;
    }
    state.addMembersData.groups = groups;
    state.addMembersData.multiGroup = isMulti;
    state.addMembersData.groupLink = groups[0].link;
    state.addMembersData.groupId = groups[0].id;
    state.addMembersData.groupName = groups[0].name;
    state.step = "add_members_friend_numbers";
    const groupPreview = groups.map(g => `✅ ${esc(g.name)}`).join("\n");
    const failNote = failedLinks > 0 ? `\n⚠️ ${failedLinks} link(s) fetch nahi hui.` : "";
    await ctx.reply(
      `✅ <b>${groups.length} Group(s) found!</b>${failNote}\n\n${groupPreview}\n\n` +
      `👫 <b>Step 2: Friend Numbers</b>\n\n` +
      `Apne friend ke contact numbers bhejo (one per line)\n` +
      `Example:\n<code>919912345678\n919898765432</code>\n\n` +
      (isMulti ? `⚠️ Multiple groups mode: Sirf friend numbers support hoga (sab groups mein add honge).\n\n` : "") +
      `Agar friend add nahi karna to Skip karo.`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "am_skip_friends").text("❌ Cancel", "main_menu") }
    );
    return;
  }

  if (state.step === "add_members_friend_numbers") {
    if (!state.addMembersData) return;
    const lines = text.split("\n").map(l => l.trim()).filter(l => l.length > 0);
    const numbers: string[] = [];
    for (const line of lines) {
      const cleaned = line.replace(/[^0-9]/g, "");
      if (cleaned.length >= 7) numbers.push(cleaned);
    }
    if (numbers.length === 0) {
      await ctx.reply("❌ Koi valid number nahi mila. Number country code ke saath bhejo jaise 919912345678\n\nYa Skip karo.",
        { reply_markup: new InlineKeyboard().text("⏭️ Skip", "am_skip_friends").text("❌ Cancel", "main_menu") }
      );
      return;
    }
    state.addMembersData.friendNumbers = numbers;
    if (state.addMembersData.multiGroup) {
      const d = state.addMembersData;
      d.adminContacts = []; d.navyContacts = []; d.memberContacts = [];
      d.totalToAdd = numbers.length;
      state.step = "add_members_choose_mode";
      const groupList = d.groups.map(g => `• ${esc(g.name)}`).join("\n");
      await ctx.reply(
        `✅ <b>${numbers.length} friend number(s) saved!</b>\n\n` +
        `📋 <b>Groups (${d.groups.length}):</b>\n${groupList}\n\n` +
        `🔢 Total friends to add: <b>${numbers.length}</b> (har group mein)\n\n` +
        `⚙️ Adding mode choose karo:\n\n` +
        `👆 <b>Add 1 by 1</b> — Ek ek karke (safe)\n` +
        `👥 <b>Add Together</b> — Sab ek saath (fast)\n` +
        `🎯 <b>Custom</b> — Apni pace set karo (1-1, 2-2, ya all)`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard()
          .text("👆 Add 1 by 1", "am_mode_one_by_one").text("👥 Together", "am_mode_together").row()
          .text("🎯 Custom", "am_mode_custom").text("❌ Cancel", "main_menu") }
      );
    } else {
      state.step = "add_members_admin_vcf";
      await ctx.reply(
        `✅ <b>${numbers.length} friend number(s) saved!</b>\n\n` +
        `👑 <b>Step 3: Admin VCF File</b>\n\n` +
        `📁 Send Admin VCF file (.vcf)\n\n` +
        `Agar admin ka VCF nahi hai to Skip karo.`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "am_skip_admin").text("❌ Cancel", "main_menu") }
      );
    }
    return;
  }

  if (state.step === "add_members_total_count") {
    if (!state.addMembersData) return;
    const d = state.addMembersData;
    const totalAvailable = d.friendNumbers.length + d.adminContacts.length + d.navyContacts.length + d.memberContacts.length;
    const count = parseInt(text);
    if (isNaN(count) || count < 1) {
      await ctx.reply("❌ Valid number bhejo (minimum 1).");
      return;
    }
    if (count > totalAvailable) {
      await ctx.reply(`❌ Sirf ${totalAvailable} contacts available hain. ${totalAvailable} ya usse kam number bhejo.`);
      return;
    }
    d.totalToAdd = count;
    state.step = "add_members_choose_mode";
    await ctx.reply(
      `🔢 <b>Total ${count} members add honge.</b>\n\n` +
      `⚙️ Adding mode choose karo:\n\n` +
      `👆 <b>Add 1 by 1</b> — Ek ek karke add karega (safe, slow)\n` +
      `👥 <b>Add Together</b> — Sab ek saath add karega (fast)\n` +
      `🎯 <b>Custom</b> — Per category pace set karo (1-1, 2-2, 3-3 ya All)\n`,
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("👆 Add 1 by 1", "am_mode_one_by_one")
          .text("👥 Add Together", "am_mode_together").row()
          .text("🎯 Custom", "am_mode_custom")
          .text("❌ Cancel", "main_menu"),
      }
    );
    return;
  }

  if (state.step === "add_members_set_delay") {
    if (!state.addMembersData) return;
    const seconds = parseInt(text);
    if (isNaN(seconds) || seconds < 1 || seconds > 300) {
      await ctx.reply("❌ Valid seconds bhejo (1-300). Recommended: 15");
      return;
    }
    state.addMembersData.delaySeconds = seconds;
    await showAddMembersReview(ctx, userId);
    return;
  }
});

bot.callbackQuery("rm_confirm_with_exclude", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeExcludeData) return;

  await startRemoveMembersProcess(
    ctx,
    userId,
    state.removeExcludeData.selectedGroups,
    state.removeExcludeData.excludeNumbers,
    state.removeExcludeData.excludePrefixes,
  );
});

// ─── Photo Handler ───────────────────────────────────────────────────────────

bot.on("message:photo", async (ctx) => {
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state) return;

  if (state.step === "group_dp" && state.groupSettings) {
    try {
      const maxDps = state.groupSettings.count;
      if (state.groupSettings.dpBuffers.length >= maxDps) {
        await ctx.reply(`⚠️ <b>Max ${maxDps} DP${maxDps === 1 ? "" : "s"} reached.</b> Tum ${maxDps} group bana rahe ho, isliye max ${maxDps} DP. Done dabake aage badho.`, {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard().text("✅ Done", "group_dp_done").text("❌ Cancel", "main_menu"),
        });
        return;
      }
      const photos = ctx.message.photo;
      const file = await ctx.api.getFile(photos[photos.length - 1].file_id);
      if (!file.file_path) { await ctx.reply("❌ Could not download photo. Try again."); return; }
      const buf = await downloadBuffer(`https://api.telegram.org/file/bot${token}/${file.file_path}`);
      state.groupSettings.dpBuffers.push(buf);
      const count = state.groupSettings.dpBuffers.length;
      await ctx.reply(
        `✅ <b>DP ${count} saved!</b>\n\n` +
        `Aur photos bhej sakte ho (max ${maxDps}), ya <b>✅ Done</b> dabake aage badho.\n` +
        `Total ab tak: <b>${count}/${maxDps}</b>`,
        {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard().text("✅ Done", "group_dp_done").text("❌ Cancel", "main_menu"),
        }
      );
    } catch (err: any) {
      await ctx.reply(`❌ Error: ${esc(err?.message || "Unknown error")}`, { parse_mode: "HTML" });
    }
    return;
  }

  if (state.step === "edit_settings_dp" && state.editSettingsData) {
    try {
      const photos = ctx.message.photo;
      const file = await ctx.api.getFile(photos[photos.length - 1].file_id);
      if (!file.file_path) { await ctx.reply("❌ Could not download photo. Try again."); return; }
      const buf = await downloadBuffer(`https://api.telegram.org/file/bot${token}/${file.file_path}`);
      state.editSettingsData.settings.dpBuffers = [buf];
      state.step = "edit_settings_desc";
      await ctx.reply("✅ <b>DP saved!</b>\n\n📄 <b>Description</b>\n\nDescription bhejo ya skip karo.", {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("⏭️ Skip", "es_desc_skip").text("❌ Cancel", "main_menu"),
      });
    } catch (err: any) {
      await ctx.reply(`❌ Error: ${esc(err?.message || "Unknown error")}`, { parse_mode: "HTML" });
    }
    return;
  }
});

// ─── Document Handler (VCF) ──────────────────────────────────────────────────

bot.on("message:document", async (ctx) => {
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state) return;
  const doc = ctx.message.document;
  if (!(doc.file_name || "").toLowerCase().endsWith(".vcf")) { await ctx.reply("❌ Please send a .vcf file only."); return; }

  if (state.step === "approval_admin_input" && state.approvalData) {
    try {
      const file = await ctx.api.getFile(doc.file_id);
      if (!file.file_path) { await ctx.reply("❌ Could not download file."); return; }
      const content = await downloadText(`https://api.telegram.org/file/bot${token}/${file.file_path}`);
      const rawContacts = parseVCF(content);
      if (!rawContacts.length) { await ctx.reply("❌ No contacts found in VCF file."); return; }
      const phoneNumbers: string[] = [];
      for (const c of rawContacts) {
        const cleaned = (c.phone || "").replace(/[^0-9]/g, "");
        if (cleaned.length >= 7) phoneNumbers.push(cleaned);
      }
      if (phoneNumbers.length === 0) {
        await ctx.reply("❌ No valid phone numbers found in VCF.");
        return;
      }
      state.approvalData.targetPhones = phoneNumbers;
      await showAdminApprovalChoice(ctx, userId);
    } catch (err: any) {
      await ctx.reply(`❌ Error: ${esc(err?.message || "Unknown")}`, { parse_mode: "HTML" });
    }
    return;
  }

  // ── Change Group Name (Auto): collect one VCF per selected group ──
  if (state.step === "cgn_auto_collect_vcf" && state.changeGroupNameData) {
    try {
      const data = state.changeGroupNameData;
      const need = (data.pendingSelectedIds || []).length;
      data.vcfFiles = data.vcfFiles || [];
      if (data.vcfFiles.length >= need) {
        await ctx.reply("✅ All required VCF files already received.");
        return;
      }
      const file = await ctx.api.getFile(doc.file_id);
      if (!file.file_path) { await ctx.reply("❌ Could not download file."); return; }
      const content = await downloadText(`https://api.telegram.org/file/bot${token}/${file.file_path}`);
      const rawContacts = parseVCF(content);
      const phones: string[] = [];
      for (const c of rawContacts) {
        const cleaned = (c.phone || "").replace(/[^0-9]/g, "");
        if (cleaned.length >= 7) phones.push(cleaned);
      }
      if (phones.length === 0) {
        await ctx.reply("❌ No valid phone numbers found in this VCF. Send a different file.");
        return;
      }
      data.vcfFiles.push({ fileName: doc.file_name || "(unnamed.vcf)", phones });
      await cgnAutoAfterVcfUploaded(ctx);
    } catch (err: any) {
      await ctx.reply(`❌ Error: ${esc(err?.message || "Unknown")}`, { parse_mode: "HTML" });
    }
    return;
  }

  if (["add_members_admin_vcf", "add_members_navy_vcf", "add_members_member_vcf"].includes(state.step) && state.addMembersData) {
    try {
      const file = await ctx.api.getFile(doc.file_id);
      if (!file.file_path) { await ctx.reply("❌ Could not download file."); return; }
      const content = await downloadText(`https://api.telegram.org/file/bot${token}/${file.file_path}`);
      const rawContacts = parseVCF(content);
      if (!rawContacts.length) { await ctx.reply("❌ No contacts found in VCF file."); return; }

      if (state.step === "add_members_admin_vcf") {
        state.addMembersData.adminContacts = rawContacts;
        state.step = "add_members_navy_vcf";
        await ctx.reply(
          `✅ <b>${rawContacts.length} admin contacts saved!</b>\n\n` +
          `⚓ <b>Step 4: Navy VCF File</b>\n\n` +
          `📁 Send Navy VCF file (.vcf)\n\n` +
          `Agar navy ka VCF nahi hai to Skip karo.`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "am_skip_navy").text("❌ Cancel", "main_menu") }
        );
      } else if (state.step === "add_members_navy_vcf") {
        state.addMembersData.navyContacts = rawContacts;
        state.step = "add_members_member_vcf";
        await ctx.reply(
          `✅ <b>${rawContacts.length} navy contacts saved!</b>\n\n` +
          `👥 <b>Step 5: Member VCF File</b>\n\n` +
          `📁 Send Member VCF file (.vcf)\n\n` +
          `Agar member ka VCF nahi hai to Skip karo.`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "am_skip_members").text("❌ Cancel", "main_menu") }
        );
      } else if (state.step === "add_members_member_vcf") {
        state.addMembersData.memberContacts = rawContacts;
        const d = state.addMembersData;
        const totalAvailable = d.friendNumbers.length + d.adminContacts.length + d.navyContacts.length + d.memberContacts.length;
        state.step = "add_members_total_count";
        await ctx.reply(
          `✅ <b>${rawContacts.length} member contacts saved!</b>\n\n` +
          `🔢 <b>Step 6: Total Members to Add</b>\n\n` +
          `📊 Available contacts:\n` +
          `👫 Friends: ${d.friendNumbers.length}\n` +
          `👑 Admin: ${d.adminContacts.length}\n` +
          `⚓ Navy: ${d.navyContacts.length}\n` +
          `👥 Members: ${d.memberContacts.length}\n` +
          `━━━━━━━━━━━━━━━━━━\n` +
          `📋 Total available: <b>${totalAvailable}</b>\n\n` +
          `🔢 Kitna members add karna hai total? (Number bhejo)`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
        );
      }
    } catch (err: any) {
      await ctx.reply(`❌ Error: ${esc(err?.message || "Unknown")}`, { parse_mode: "HTML" });
    }
    return;
  }

  if (state.step !== "ctc_enter_vcf" || !state.ctcData) return;

  try {
    const file = await ctx.api.getFile(doc.file_id);
    if (!file.file_path) { await ctx.reply("❌ Could not download file."); return; }
    const content = await downloadText(`https://api.telegram.org/file/bot${token}/${file.file_path}`);
    const rawContacts = parseVCF(content);
    if (!rawContacts.length) { await ctx.reply("❌ No contacts found in VCF file."); return; }

    const vcfFileName = doc.file_name || "unknown.vcf";
    const contacts = rawContacts.map(c => ({ ...c, vcfFileName }));

    const idx = state.ctcData.currentPairIndex;

    if (idx >= state.ctcData.pairs.length) {
      // All pairs filled, just append to last group
      const lastIdx = state.ctcData.pairs.length - 1;
      state.ctcData.pairs[lastIdx].vcfContacts.push(...contacts);
      const total = state.ctcData.pairs[lastIdx].vcfContacts.length;
      await ctx.reply(
        `✅ <b>${contacts.length} contacts added to Group ${lastIdx + 1}</b> (total: ${total})\n\n🚀 Ready to check!`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("▶️ Start Check", "ctc_start_check").text("❌ Cancel", "main_menu") }
      );
      return;
    }

    // Add contacts to current pair
    state.ctcData.pairs[idx].vcfContacts.push(...contacts);
    const total = state.ctcData.pairs[idx].vcfContacts.length;
    state.ctcData.currentPairIndex++;
    const nextIdx = state.ctcData.currentPairIndex;

    if (nextIdx < state.ctcData.pairs.length) {
      await ctx.reply(
        `✅ <b>${contacts.length} contacts added to Group ${idx + 1}</b> (total: ${total})\n\n📁 Send VCF for <b>Group ${nextIdx + 1}/${state.ctcData.pairs.length}</b>:\n<code>${esc(state.ctcData.pairs[nextIdx].link)}</code>\n\n<i>Or tap Start Check if you want to use the same VCF for remaining groups</i>`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("▶️ Start Check", "ctc_start_check").text("❌ Cancel", "main_menu") }
      );
    } else {
      await ctx.reply(
        `✅ <b>${contacts.length} contacts for Group ${idx + 1}</b> (total: ${total})\n\n🎉 All ${state.ctcData.pairs.length} VCF file(s) received!\n\n🚀 Ready to check!`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("▶️ Start Check", "ctc_start_check").text("❌ Cancel", "main_menu") }
      );
    }
  } catch (err: any) {
    await ctx.reply(`❌ Error: ${esc(err?.message || "Unknown")}`, { parse_mode: "HTML" });
  }
});

// ─── Utilities ───────────────────────────────────────────────────────────────

function downloadText(url: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https") ? https : http;
    client.get(url, (res) => { let d = ""; res.on("data", (c) => (d += c)); res.on("end", () => resolve(d)); res.on("error", reject); }).on("error", reject);
  });
}

function downloadBuffer(url: string): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https") ? https : http;
    client.get(url, (res) => {
      const chunks: Buffer[] = [];
      res.on("data", (c) => chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
      res.on("end", () => resolve(Buffer.concat(chunks)));
      res.on("error", reject);
    }).on("error", reject);
  });
}

function splitMessage(msg: string, maxLen: number): string[] {
  const parts: string[] = [];
  let current = "";
  for (const line of msg.split("\n")) {
    if ((current + line + "\n").length > maxLen) { parts.push(current); current = line + "\n"; }
    else current += line + "\n";
  }
  if (current) parts.push(current);
  return parts;
}

export async function startBot() {
  if (!token) {
    console.log("[BOT] TELEGRAM_BOT_TOKEN not set — bot disabled. Set it to enable the Telegram bot.");
    return;
  }

  // Register a global disconnect notifier so users get a Telegram alert in English
  // (with their WhatsApp number) whenever any of their WhatsApp sessions disconnects —
  // including sessions that were silently restored on bot startup.
  setDisconnectNotifier((sessionUserId, reason, phoneNumber) => {
    // Auto-Chat sessions use IDs like `${telegramId}_auto`; map to the actual Telegram user.
    const isAuto = sessionUserId.endsWith("_auto");
    const telegramIdStr = isAuto ? sessionUserId.replace(/_auto$/, "") : sessionUserId;
    const telegramId = Number(telegramIdStr);
    if (!Number.isFinite(telegramId)) return;
    const phoneText = phoneNumber ? phoneNumber : "(unknown)";
    const accountLabel = isAuto ? "Auto Chat WhatsApp" : "WhatsApp";
    const message =
      `⚠️ <b>${accountLabel} Disconnected</b>\n\n` +
      `Your ${accountLabel} number <code>${esc(phoneText)}</code> has been disconnected from the bot.\n\n` +
      `Reason: ${esc(reason || "Unknown")}\n\n` +
      `Please reconnect to continue using the bot.`;
    void bot.api.sendMessage(telegramId, message, {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text(isAuto ? "🤖 Reconnect Auto WA" : "📱 Reconnect WhatsApp", isAuto ? "connect_auto_wa" : "connect_wa")
        .text("🏠 Menu", "main_menu"),
    }).catch((err) => {
      console.error(`[BOT][NOTIFY-DISCONNECT] Failed to notify ${telegramId}:`, err?.message);
    });
  });

  void syncAutoChatSettings().then(() => {
    console.log(`[BOT] Auto Chat settings loaded: global=${autoChatGlobalEnabled} accessList=${autoChatAccessSet.size} users`);
  });

  bot.catch((err) => {
    const e = err.error as any;
    const code = e?.error_code;
    const desc: string = e?.description || e?.message || String(e) || "";
    if (code === 400 && desc.includes("message is not modified")) return;
    console.error(`[BOT] Error in update ${err.ctx?.update?.update_id}: ${desc || err.message}`);
  });

  let retryCount = 0;

  async function launchBot() {
    try {
      await bot.api.deleteWebhook({ drop_pending_updates: true });
      console.log("[BOT] Webhook cleared, starting polling...");
    } catch (err: any) {
      console.error("[BOT] Failed to delete webhook:", err?.message);
    }

    try {
      await bot.start({
        onStart: () => {
          console.log("Telegram bot started successfully!");
          retryCount = 0;
        },
      });
      // bot.start() resolved (graceful stop) — restart polling
      console.log("[BOT] Polling stopped gracefully, restarting in 5s...");
      retryCount = 0;
      setTimeout(() => launchBot(), 5000);
    } catch (err: any) {
      if (err?.error_code === 401) {
        console.error("[BOT] Invalid TELEGRAM_BOT_TOKEN (401 Unauthorized). Bot disabled.");
        return;
      }
      retryCount++;
      const delay = err?.error_code === 409
        ? Math.min(retryCount * 15, 120)   // 409: wait 15s, 30s ... max 2 min
        : Math.min(retryCount * 5, 60);    // other errors: wait 5s, 10s ... max 1 min
      if (err?.error_code === 409) {
        console.log(`[BOT] 409 conflict — another instance running. Retry #${retryCount} in ${delay}s...`);
      } else {
        console.error(`[BOT] Error (retry #${retryCount} in ${delay}s): ${err?.message || err}`);
      }
      setTimeout(() => launchBot(), delay * 1000);
    }
  }

  process.on("SIGTERM", async () => {
    console.log("[BOT] SIGTERM received, stopping bot...");
    try { await bot.stop(); } catch {}
    process.exit(0);
  });

  process.on("SIGINT", async () => {
    console.log("[BOT] SIGINT received, stopping bot...");
    try { await bot.stop(); } catch {}
    process.exit(0);
  });

  // Hydrate per-user language preferences from MongoDB before polling starts,
  // so the very first outgoing message uses the right language.
  try {
    await loadUserLanguages();
  } catch (err: any) {
    console.error("[i18n] loadUserLanguages failed:", err?.message);
  }

  launchBot();
}

export { bot };
