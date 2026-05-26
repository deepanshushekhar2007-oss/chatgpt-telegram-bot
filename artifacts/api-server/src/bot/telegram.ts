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
  removeGroupIcon,
  removeGroupDescription,
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
  deleteGroupChat,
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
  getAutoSlotUserId,
  getAllConnectedAutoSlots,
  getConnectedAutoCount,
  sendContactCard,
  saveContactToWhatsApp,
  getActiveSessionUserIds,
  setDisconnectNotifier,
  setGroupDisappearingMessages,
  setGroupName,
  resetGroupInviteLink,
  demoteGroupAdmin,
  ensureSessionLoaded,
  hasStoredWhatsAppSession,
  waitForWhatsAppConnected,
  sweepIdleSessions,
  getGroupPendingInviteLinkJoins,
  protectSessionFromEviction,
  unprotectSession,
  markSessionActive,
  sendSocketPresence,
  setSessionAlias,
  clearSessionAlias,
  getSessionAlias,
  isDisconnectPending,
  isWhatsAppConnecting,
  suppressDisconnectNotification,
  warmupPersonalJid,
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
  createRedeemCode,
  redeemUserCode,
  getRedeemCodeInfo,
  listAllRedeemCodes,
  deleteRedeemCode,
  AccessState,
  saveAutoChatSession,
  deleteAutoChatSession,
  loadAllAutoChatSessions,
  savePendingGroupCreation,
  loadPendingGroupCreation,
  deletePendingGroupCreation,
  type PersistedGroupSettings,
  saveAutoAccepterJob,
  loadAllAutoAccepterJobs,
  deleteAutoAccepterJob,
  type PersistedAutoAccepterJob,
  saveUserState,
  loadUserState,
  deleteUserState,
  getAutoWsLimit,
  setAutoWsLimit,
  loadWaSwitchProfile,
  saveWaSwitchProfile,
  loadAllWaSwitchProfiles,
  type WaSwitchSlot,
  type WaSwitchProfile,
} from "./mongo-bot-data";
import { getSessionStats, cleanupStaleSessions, clearMongoSession, listStoredWhatsAppSessions } from "./mongo-auth-state";
import {
  extractPhonesFromBuffer,
  buildVCFContent,
  buildSplitContent,
  buildTXTContent,
  buildCSVContent,
  buildXLSXBuffer,
  canonicalExt,
  chunkArray,
  detectMergeExt,
  downloadBuffer,
  isSupportedExt,
  extLabel,
  unwrapError,
} from "./file-tools";
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
// ─────────────────────────────────────────────────────────────────────────────
// In-memory TTL cache — short-lived (30–60 s) cache for hot DB look-ups
// (ban status, access status, stored-session flag) so repeated /start and
// button presses never hit MongoDB for the same user twice in quick
// succession. Memory footprint is negligible: each entry is ~100 bytes;
// 5 000 entries ≈ 500 KB. The cache auto-evicts on read when expired and a
// periodic sweep clears stale entries to prevent unbounded growth over days.
// ─────────────────────────────────────────────────────────────────────────────
class TTLCache<K, V> {
  private m = new Map<K, { v: V; exp: number }>();
  private hits = 0;
  private misses = 0;
  constructor(private ttlMs: number, private max = 5_000) {}
  get(k: K): V | undefined {
    const e = this.m.get(k);
    if (!e) { this.misses++; return undefined; }
    if (Date.now() > e.exp) { this.m.delete(k); this.misses++; return undefined; }
    this.hits++;
    return e.v;
  }
  set(k: K, v: V): void {
    if (this.m.size >= this.max) {
      // Evict the oldest entry to stay within the cap.
      this.m.delete(this.m.keys().next().value!);
    }
    this.m.set(k, { v, exp: Date.now() + this.ttlMs });
  }
  del(k: K): void { this.m.delete(k); }
  get size() { return this.m.size; }
  get hitCount() { return this.hits; }
  get missCount() { return this.misses; }
  /** Remove all entries that have already expired. */
  sweep(): number {
    const now = Date.now();
    let removed = 0;
    for (const [k, e] of this.m) {
      if (now > e.exp) { this.m.delete(k); removed++; }
    }
    return removed;
  }
}

/** Ban status per userId — 300 s TTL. Admin /ban and /unban immediately call bannedCache.del(). */
const bannedCache = new TTLCache<number, boolean>(300_000);
/** Access status per userId — 300 s TTL. All access-change commands call accessCache.del(). */
const accessCache = new TTLCache<number, boolean>(300_000);
/** Whether user has a stored WA session — 120 s TTL. Invalidated on disconnect/logout. */
const hasSessionCache = new TTLCache<string, boolean>(120_000);
/**
 * Force-sub channel membership — 5 min TTL.
 * Eliminates a getChatMember() Telegram API round-trip (~100-300ms) on every
 * button press for regular users. Only `true` is cached (joined); `false` is
 * never stored so the user can retry immediately after joining the channel.
 * Busted in check_joined (on successful join) and clearUserMemoryState.
 */
const forceSubCache = new TTLCache<number, boolean>(5 * 60_000);

// Periodic sweep: remove expired entries from all caches every 5 minutes.
setInterval(() => {
  bannedCache.sweep();
  accessCache.sweep();
  hasSessionCache.sweep();
  forceSubCache.sweep();
}, 5 * 60 * 1000);

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
      || cbData.startsWith("connect_")
      || cbData.startsWith("disconnect_")
      || cbData.startsWith("logout_")
      || cbData.startsWith("lang_")
      || cbData.startsWith("force_sub_");
    if (cbData && !skipReconnect && !isConnected(String(userId))) {
      // Use session cache so this check is instant (no MongoDB round-trip).
      let hasStored = hasSessionCache.get(String(userId));
      if (hasStored === undefined) {
        try { hasStored = await hasStoredWhatsAppSession(String(userId)); hasSessionCache.set(String(userId), hasStored); } catch { hasStored = false; }
      }
      // Guard 1: skip reconnect if user was permanently disconnected and notified
      // (e.g. pair code failed, 401 logout, 403 ban). The menu's "Connect WhatsApp"
      // button is the right next step, not a silent reconnect attempt.
      if (hasStored && !isDisconnectPending(String(userId))) {
        // Answer the callback query IMMEDIATELY so Telegram's 10s timeout
        // does not fire and cause a silent "nothing happens" drop.
        ctx.answerCallbackQuery().catch(() => {})

        // ── Silent fast-path ───────────────────────────────────────────────
        // If Baileys is already mid-auto-reconnect (brief network blip,
        // keepalive cycle, or WA server nudge) wait up to 5 s silently
        // before showing any UI. This prevents the "Your session was idle"
        // bar from flashing on button presses that happen to land during a
        // routine 1-3 s socket handshake that Baileys will resolve on its own.
        if (isWhatsAppConnecting(String(userId))) {
          const quickConnected = await waitForWhatsAppConnected(String(userId), { timeoutMs: 1_000, pollMs: 100 }).catch(() => false);
          if (quickConnected) {
            (ctx as any).answerCallbackQuery = () => Promise.resolve(true);
            // Fall through to handler — no UI needed.
          } else {
            // Still not up after 5 s → show full reconnect bar below (same
            // code path as a cold session).
            /* falls through to the rcChatId / progress bar block */
          }
        }

        // Re-check: if we just connected via the fast-path above, skip the bar.
        if (!isConnected(String(userId))) {
          const rcChatId = ctx.callbackQuery?.message?.chat.id;
          const rcMsgId  = ctx.callbackQuery?.message?.message_id;
          const rcStart  = Date.now();
          let rcLastText = "";
          let rcDone     = false;

          // Edit the existing message to a live progress bar so the user sees
          // real-time feedback instead of a static "reconnecting..." text.
          const renderReconnectBar = async (pct: number) => {
            const elapsed = Math.floor((Date.now() - rcStart) / 1000);
            const text =
              `🔄 <b>WhatsApp reconnecting...</b>\n\n` +
              `${renderProgressBar(pct)}\n\n` +
              `<i>Reconnecting to WhatsApp automatically... (${elapsed}s)</i>`;
            if (text === rcLastText) return;
            rcLastText = text;
            try {
              if (rcChatId && rcMsgId) {
                await ctx.api.editMessageText(rcChatId, rcMsgId, text, { parse_mode: "HTML" });
              } else {
                await ctx.editMessageText(text, { parse_mode: "HTML" });
              }
            } catch {}
          };

          await renderReconnectBar(0);

          // Tick the progress bar every 1.5s while waiting for the socket to reconnect.
          // Maps elapsed time (0→20s) to progress (0→90%); jumps to result state at end.
          const rcTicker = setInterval(async () => {
            if (rcDone) return;
            const elapsed = (Date.now() - rcStart) / 1000;
            const pct = Math.min(90, Math.floor((elapsed / 20) * 90));
            await renderReconnectBar(pct);
          }, 1500);

          let connected = false;
          try {
            connected = await waitForWhatsAppConnected(String(userId), {
              timeoutMs: 20_000,
              pollMs: 200,
            });
          } catch {}

          rcDone = true;
          clearInterval(rcTicker);

          if (!connected) {
            // Check if the user has saved Switch WA accounts they can switch to
            const swProfile = await loadWaSwitchProfile(userId).catch(() => null);
            const hasSwitchSessions = (swProfile?.slots?.length ?? 0) > 0;
            let failText: string;
            let failKb: InlineKeyboard;
            if (hasSwitchSessions) {
              failText =
                `⚠️ <b>WhatsApp Disconnected</b>\n\n` +
                `${renderProgressBar(0)}\n\n` +
                `Your primary WhatsApp account has been disconnected.\n\n` +
                `You have <b>${swProfile!.slots.length}</b> saved account(s) you can switch to.\n` +
                `Tap <b>Manage Sessions</b> to switch or reconnect.`;
              failKb = new InlineKeyboard()
                .text("📱 Manage Sessions", "manage_sessions").row()
                .text("📱 Connect New", "connect_wa").row()
                .text("🏠 Main Menu", "main_menu");
            } else {
              failText =
                `❌ <b>WhatsApp disconnected</b>\n\n` +
                `${renderProgressBar(0)}\n\n` +
                `Your WhatsApp session has been disconnected.\n\n` +
                `Please connect a fresh session from the menu:\n` +
                `📱 Menu → <b>Connect WhatsApp</b> → QR or Pairing Code`;
              failKb = new InlineKeyboard()
                .text("📱 Connect WhatsApp", "connect_wa").row()
                .text("🏠 Main Menu", "main_menu");
            }
            try {
              if (rcChatId && rcMsgId) {
                await ctx.api.editMessageText(rcChatId, rcMsgId, failText, { parse_mode: "HTML", reply_markup: failKb });
              } else {
                await ctx.editMessageText(failText, { parse_mode: "HTML", reply_markup: failKb });
              }
            } catch {
              try { await ctx.reply(failText, { parse_mode: "HTML", reply_markup: failKb }); } catch {}
            }
            return;
          }

          // Connected — show a brief "✅ Reconnected" confirmation so the
          // progress bar is never left frozen mid-way if the handler takes
          // a moment to render the actual menu.
          try {
            const okText = `✅ <b>WhatsApp reconnected!</b>\n\n${renderProgressBar(100)}\n\n<i>Loading your request...</i>`;
            if (rcChatId && rcMsgId) {
              await ctx.api.editMessageText(rcChatId, rcMsgId, okText, { parse_mode: "HTML" });
            } else {
              await ctx.editMessageText(okText, { parse_mode: "HTML" });
            }
          } catch {}

          // Connected — fall through. Since we already answered the callback
          // query above, patch answerCallbackQuery to a silent no-op so the
          // downstream handler does not double-answer and throw an error.
          (ctx as any).answerCallbackQuery = () => Promise.resolve(true);
        }
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
const REFER_GATE_EXEMPT_PREFIXES = [
  "lang_", "force_sub_",
  // ── /file — VCF File Tools is FREE for ALL users, no access check ──────────
  "ft_",   // File Tools menu buttons (ft_menu, ft_vcf_editor, ft_splitter, etc.)
  "fe_",   // VCF Editor steps
  "fs_",   // Splitter steps
  "fm_",   // Merge steps
  "fc_",   // Convert Files steps
  "fn_",   // Number → VCF steps (fn_confirm, fn_step1, fn_step2, etc.)
];
const REFER_GATE_EXEMPT_EXACT = new Set(["check_joined"]);
function isReferGateExempt(cbData: string): boolean {
  if (REFER_GATE_EXEMPT_EXACT.has(cbData)) return true;
  return REFER_GATE_EXEMPT_PREFIXES.some((p) => cbData.startsWith(p));
}

// ── In-memory cache for loadBotData in the refer-gate (5s TTL) ──────────────
// Avoids a MongoDB round-trip on EVERY button click just to check referMode.
// Cache is intentionally short so admin changes take effect within 5 seconds.
let _referGateBotDataCache: { data: Awaited<ReturnType<typeof loadBotData>>; expiresAt: number } | null = null;
async function _getCachedBotDataForReferGate() {
  const now = Date.now();
  if (_referGateBotDataCache && now < _referGateBotDataCache.expiresAt) {
    return _referGateBotDataCache.data;
  }
  const data = await loadBotData();
  _referGateBotDataCache = { data, expiresAt: now + 5_000 };
  return data;
}

bot.use(async (ctx, next) => {
  const cbData = ctx.callbackQuery?.data;
  if (!cbData) return next();
  const userId = ctx.from?.id;
  if (typeof userId !== "number") return next();
  if (isAdmin(userId)) return next();
  if (isReferGateExempt(cbData)) return next();

  // ── INSTANT RESPONSE: answer the callback query immediately so the button
  // spinner disappears at zero latency. The access check happens in parallel
  // and will edit the message text if the user is out of access.
  // We patch answerCallbackQuery to a no-op so downstream handlers don't double-answer.
  ctx.answerCallbackQuery().catch(() => {});
  (ctx as any).answerCallbackQuery = () => Promise.resolve(true);

  // Fail-open: if MongoDB is unavailable, let the handler run rather than
  // silently dropping the update (which causes "nothing happens" for the user).
  // Use cached bot data (5s TTL) to avoid MongoDB round-trip on every button click.
  let data: Awaited<ReturnType<typeof loadBotData>>;
  try { data = await _getCachedBotDataForReferGate(); } catch { return next(); }
  if (!data.referMode) return next();

  let state: Awaited<ReturnType<typeof getAccessState>>;
  try { state = await getAccessState(userId); } catch { return next(); }
  if (state.kind !== "none") return next();

  // Out of access — block the button and surface the refer-required UI.
  try {
    const { text, keyboard } = await buildReferRequiredMessage(userId);
    try {
      await ctx.editMessageText(text, { parse_mode: "HTML", reply_markup: keyboard });
    } catch {
      try {
        await ctx.reply(text, { parse_mode: "HTML", reply_markup: keyboard });
      } catch {}
    }
  } catch {}
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

function connectedStatusText(userId: number, hasSwitchSessions = false): string {
  const mainConnected = isConnected(String(userId));
  const autoConnected = isAutoConnected(String(userId));

  const lines: string[] = [];

  if (!mainConnected) {
    if (hasSwitchSessions) {
      lines.push("│ ⚠️  WhatsApp: Disconnected");
      lines.push("│ 💡  You have saved accounts — switch below");
    } else {
      lines.push("│ ❌  WhatsApp: Not Connected");
    }
  } else {
    const number = getConnectedWhatsAppNumber(String(userId));
    lines.push(`│ ✅  WhatsApp: ${number ? `<code>${esc(number)}</code>` : "Connected"}`);
  }

  if (autoConnected) {
    const autoNumber = getAutoConnectedNumber(String(userId));
    lines.push(`│ 🤖  Auto WA: ${autoNumber ? `<code>${esc(autoNumber)}</code>` : "Connected"}`);
  }

  return (
    `╭─── 📡 Status ─────────╮\n` +
    lines.join("\n") + "\n" +
    `╰───────────────────────╯`
  );
}

function mainMenuText(userId: number, mode: "welcome" | "menu" = "menu", name?: string, hasSwitchSessions = false): string {
  const safeName = name ? esc(name) : "";
  const greeting = mode === "welcome"
    ? `👋 Hey ${safeName}, Welcome!`
    : `👋 Hey ${safeName}, Welcome Back!`;
  return (
    `🤖 <b>${BOT_DISPLAY_NAME}</b> 🤖\n` +
    `▰▰▰▰▰▰▰▰▰▰▰▰▰\n\n` +
    `${greeting}\n\n` +
    connectedStatusText(userId, hasSwitchSessions) +
    `\n\n› Choose an option:`
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
  // Normalize links that are split across two lines by Telegram rendering:
  //   "https://chat.whatsapp.com\n/CODE" → "https://chat.whatsapp.com/CODE"
  const normalized = text
    .replace(/(chat\.whatsapp\.com)\s*\r?\n\s*\//g, "$1/")
    .replace(/(https?:\/\/chat\.whatsapp\.com)\s+([A-Za-z0-9]{10,})/g, "$1/$2");
  const regex = /https?:\/\/chat\.whatsapp\.com\/[A-Za-z0-9]+/gi;
  const matches = normalized.match(regex);
  if (!matches) return [];
  return [...new Set(matches.map(buildCleanLink))];
}

function isAdmin(userId: number): boolean {
  return userId === ADMIN_USER_ID;
}

async function isBanned(userId: number): Promise<boolean> {
  const cached = bannedCache.get(userId);
  if (cached !== undefined) return cached;
  const result = await isUserBanned(userId);
  bannedCache.set(userId, result);
  return result;
}

async function hasAccess(userId: number): Promise<boolean> {
  const cached = accessCache.get(userId);
  if (cached !== undefined) return cached;
  const result = await hasUserAccess(userId, ADMIN_USER_ID);
  accessCache.set(userId, result);
  return result;
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

// In-memory set of userIds already tracked this process lifetime.
// Skips the MongoDB $addToSet write for users we've already recorded —
// the write is a no-op in MongoDB anyway (set semantics), so this saves
// one round-trip per /start for every returning user.
const _trackedUsersThisSession = new Set<number>();

async function trackUser(userId: number): Promise<void> {
  if (_trackedUsersThisSession.has(userId)) return;
  _trackedUsersThisSession.add(userId);
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
  // In-memory cache hit — skips a getChatMember() Telegram API call (~100-300ms)
  // on every button press. Only `true` is cached; denied users re-check every time
  // so they get in immediately after joining without waiting for TTL expiry.
  if (forceSubCache.get(userId) === true) return true;

  try {
    const member = await bot.api.getChatMember(FORCE_SUB_CHANNEL, userId);
    if (["member", "administrator", "creator"].includes(member.status)) {
      forceSubCache.set(userId, true);
      return true;
    }
  } catch (err: any) {
    console.error("[FORCE_SUB] Check error:", err?.message);
  }

  const channelName = FORCE_SUB_CHANNEL.replace(/^@/, "");
  const kb = new InlineKeyboard()
    .url("🔗 Join Now", `https://t.me/${channelName}`).row()
    .text("🔓 I Joined", "check_joined");
  const msg =
    "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰\n" +
    "🔒  <b>SYSTEM  LOCKED</b>  🔒\n" +
    "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰\n\n" +
    "⛔ <b>Access Denied!</b>\n\n" +
    "You must join our official channel\n" +
    "before using this bot.\n\n" +
    `📡 <b>Channel:</b>  @${esc(channelName)}\n\n` +
    "✅ Join → Tap the button → <b>Unlocked!</b>\n\n" +
    "▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰";
  try {
    await ctx.reply(msg, { parse_mode: "HTML", reply_markup: kb });
  } catch {
    try {
      await ctx.editMessageText(msg, { parse_mode: "HTML", reply_markup: kb });
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
  removeDp?: boolean;
  removeDescription?: boolean;
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
  groupId?: string;
  groupName?: string;
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
    patterns?: SimilarGroup[];
    patternPage?: number;
  };
  removeExcludeData?: {
    selectedGroups: Array<{ id: string; subject: string }>;
    excludeNumbers: Set<string>;
    excludePrefixes: Set<string>;
  };
  removeFriendData?: {
    selectedGroups: Array<{ id: string; subject: string }>;
    phoneNumbers?: string[];   // full numbers (7+ digits) — remove only that number
    countryCodes?: string[];   // 1-4 digit country codes — remove ALL matching non-admins
    exceptions?: string[];     // numbers to exclude from country-code removal
  };
  similarData?: {
    patterns: SimilarGroup[];
    allGroups: Array<{ id: string; subject: string }>;
  };
  glData?: {
    groupsPool: Array<{ id: string; subject: string }>;
    selectedIndices: Set<number>;
    page: number;
    mode: "similar" | "all";
    patternBase?: string;
    patterns: SimilarGroup[];
    allGroups: Array<{ id: string; subject: string }>;
  };
  rlLinkBuffer?: string[];
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
  resetLinkData?: {
    allGroups: Array<{ id: string; subject: string }>;
    patterns: SimilarGroup[];
    selectedIndices: Set<number>;
    page: number;
    patternPage?: number;
  };
  demoteAdminData?: {
    allGroups: Array<{ id: string; subject: string }>;
    patterns: SimilarGroup[];
    selectedIndices: Set<number>;
    page: number;
    mode?: "all" | "numbers";
    phoneNumbers?: string[];
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
  stealGroupData?: {
    allGroups: Array<{ id: string; subject: string }>;
    patterns: SimilarGroup[];
    selectedIndices: Set<number>;
    page: number;
    scannedGroups?: Array<{
      id: string;
      subject: string;
      memberCount: number;
      adminCount: number;
      creatorPhone: string;
      approvalMode: boolean;
    }>;
  };
  sgLinkBuffer?: string[];
  rmLinkBuffer?: string[];
  esLinkBuffer?: string[];
  apLinkBuffer?: string[];
  cgnLinkBuffer?: string[];
  maLinkBuffer?: string[];
  daLinkBuffer?: string[];
  lvLinkBuffer?: string[];
  ctcLinkBuffer?: string[];
  ctcgData?: {
    allGroups: Array<{ id: string; subject: string }>;
    patterns: SimilarGroup[];
    selectedIndices: Set<number>;
    page: number;
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
    autoChatDurationMs?: number;
  };
  autoConnectStep?: string;
  autoSlot?: number;
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
  // ── /file — VCF File Tools ────────────────────────────────────────────────
  fileEditorData?: {
    mode: "editor" | "splitter" | "merge" | "number2vcf" | "converter";
    contactsGroups: string[][];  // extracted phone arrays per file
    fileNames: string[];
    fileExts: string[];
    baseName?: string;
    contactName?: string;
    startFileNum?: number;
    contactStartNum?: number;
    contactsPerFile?: number;
    targetFormat?: string;
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
  allUserIds: string[];     // all WA accounts to rotate through
  sentByAccount: number[];  // per-account sent count
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
  autoChatExpiresAt?: number;
}

interface AcfSession {
  running: boolean;
  cancelled: boolean;
  chatId: number;
  msgId: number;
  primaryJid: string;
  autoJid: string;
  allJids?: string[];
  allUserIds?: string[];
  commonGroups?: Array<{ id: string; subject: string }>;
  waCount?: number;
  sent: number;
  failed: number;
  currentPair: number;
  totalPairs: number;
  cycle: number;
  nextDelayMs: number;
  rotationIndex: number;
  autoChatExpiresAt?: number;
  // all-to-all tracking
  currentSenderIdx?: number;
  currentReceiverIdx?: number;
  // actual phone numbers for the current send direction (for status display)
  currentSenderPhone?: string;
  currentReceiverPhone?: string;
  // per-account sent count (index = WA slot index)
  sentByAccount: number[];
}

const CHAT_FRIEND_PAIRS: [string, string][] = [
  // ── Studies / School ───────────────────────────────────────────────────────
  ["Yaar, kal ka test tha kaisa gaya?", "Bilkul bekar 😭 Tu bata?"],
  ["Main sab bhool gaya tha 😂", "Hahaha mujhe bhi! Chalo saath mein rone wale hain 😂"],
  ["Kal physics padh le yaar seriously", "Haan yaar, aaj raat 11 baje call karte hain group mein"],
  ["Bhai tune notes liye the class mein?", "Nahi yaar main so gaya tha 🙈 Tu de de please"],
  ["Assignment submit ho gaya tera?", "Abhi nahi yaar, 2 ghante bacha hai deadline mein 😰"],
  ["Yaar kitna bada syllabus hai is baar!", "Haan bhai, rona aa raha hai dekh ke 😭"],
  ["Bhai galti se teacher ki aankhon mein dekh liya!", "Phir? Sun li lecture wali sirf tujhe hi? 😂"],
  ["Kal result aane wala hai yaar...", "Main toh kal school nahi aaunga 😂 Chhup jaunga ghar pe"],
  ["Physics ka formula yaad nahi ho raha", "Tension mat le, exam mein bhi nahi hoga yaad 😂"],
  ["Bhai library mein padhai hoti hai kya?", "Hoti toh hai... mujhe toh neend aati hai wahan 😴"],
  ["Yaar group project mein mera koi kaam nahi kiya!", "Welcome to team work bhai 😂"],
  ["Yaar notes share kar na please!", "Mere notes? Main khud copy karta hoon tere notes se 😂"],
  ["Bhai iss baar padhna hai seriously", "Haan same last baar bhi kaha tha, aur usse pehle bhi 😂"],
  ["Bhai chemistry experiment mein kuch jalaya tune!", "Sirf thoda sa... science toh yahi hota hai na 😂"],
  ["Bhai exam me kitna aaya?", "Puchh mat yaar... dard hota hai yaad karke 😂"],
  ["Tera homework hua kya?", "Homework? Wo toh kal subah 5 baje karenge jaise hamesha 😅"],
  // ── Bollywood / Movies ────────────────────────────────────────────────────
  ["Bhai tune Stree 2 dekhi kya?", "Haan yaar! Shraddha Kapoor ne ek dum mast kaam kiya 🔥"],
  ["KGF 3 kab aayega yaar?", "Pata nahi bhai... lekin Rocky bhai ka wait karenge 💪"],
  ["Animal ka ending kaafi dark tha bhai", "Haan, but Ranbir ka performance ekdum top level tha 🔥"],
  ["Bhai Pushpa 2 mein Allu Arjun ka swag dekha?", "Bhai woh toh legend hai! 'Main jhukenga nahi' 😤🔥"],
  ["Yaar aaj raat koi film dekhte hain?", "Haan chal! Netflix pe kuch accha hai kya? 🎬"],
  ["Bhai Jawan dekhi? SRK ne kamaal kar diya!", "Dekhi! Ek dum mass entertainer... 3 baar dekhi main ne 🤩"],
  // ── Cricket / IPL ─────────────────────────────────────────────────────────
  ["Bhai aaj India ne match jeeta?", "Haan yaar! Rohit ne half century maari 🏏 Kaafi tense tha"],
  ["IPL mein kaun si team support karta hai tu?", "CSK zindabad! Thala Dhoni best captain ever hai 🦁"],
  ["Bhai Virat ka century aaya kal?", "Nahi yaar... 99 pe out ho gaya 😭 So close tha"],
  ["Yaar Bumrah ka yorker dekha kal?", "Bhai woh insaan nahi, machine hai! Kamal ki bowling 🎯"],
  ["India ka world cup jeetnaa chahiye iss baar", "Haan bhai! Aur Rohit ko cup uthane do 🏆❤️"],
  ["Bhai T20 mein Pakistan se match hai", "Main toh nervous hoon already 😂 Tension mat kar, India jeetega"],
  // ── Food / Zomato / Swiggy ────────────────────────────────────────────────
  ["Yaar kya order karein aaj raat?", "Biryani! Swiggy pe discount bhi chal raha hai aaj 😍"],
  ["Bhai ghar ka khana boring ho gaya yaar", "Zomato kholo, aaj main treat deta hoon 😄"],
  ["Rate kitni badhaye hain restaurants ne yaar!", "Haan bhai, samosa 25 rupay ka ho gaya hai 😭 Inflation!"],
  ["Bhai Maggi banai kya aaj?", "Haan yaar, 2 minute mein ready! Sabse reliable friend hai Maggi 🍜"],
  ["Canteen ka khana aaj kaisa tha?", "Ekdum bekar! Ghar ka khana yaad aa gaya 😭"],
  ["Yaar pizza order karte hain?", "Haan! Dominos pe buy-1-get-1 chal raha hai aaj 🍕"],
  // ── Tech / Phones / Internet ──────────────────────────────────────────────
  ["Bhai new iPhone le raha hai?", "Chahta hoon... lekin ek kidney jaani padegi 😂"],
  ["Bhai signal slow hai bahut aaj mera", "Jio hai kya? Recharge kar yaar network aayega 😅"],
  ["ChatGPT se assignment kiya tune?", "Main kab se use kar raha hoon bhai! Seedha paste 😂"],
  ["Bhai new Android phone aaya market mein", "Specs dekhe? 200MP camera... but battery sirf 4000mAh 😤"],
  ["Yaar YouTube pe ad aana band ho jata kaash", "Premium le le bhai, 60 rupay mein 2 mahine milte hain 😂"],
  ["Bhai Instagram pe follower count stuck hai", "Content consistency chahiye yaar, ek baar viral hua toh sab theek 🚀"],
  // ── Job / Career ──────────────────────────────────────────────────────────
  ["Bhai placement aa gayi kisi ko?", "Haan Rahul ko Google mein! 45 LPA 🎉 Khush ho gaye"],
  ["Resume update kiya kya tu ne?", "Abhi nahi yaar... LinkedIn pe bhi active nahi hoon 😅"],
  ["Interview ka dar lagta hai yaar bahut", "Sab ko lagta hai bhai, practice karo mock interviews 💪"],
  ["Bhai startup join karein ya safe job?", "Dono ka apna risk hai... lekin passion follow kar 🔥"],
  ["Yaar salary negotiate karna sikhna chahiye", "Haan bhai! Pehli offer mat accept kar, counter karo 💡"],
  // ── Night Conversations ───────────────────────────────────────────────────
  ["Bhai neend nahi aa rahi aaj", "Same yaar! Raat ke 2 baje hain phir bhi phone pe hain 😂"],
  ["Yaar kuch interesting batao", "Kuch nahi hai... par ab baat karne se neend toh nahi aayegi 😄"],
  ["Bhai raat ko kuch khaaya?", "Haan 10 baje Maggi banayi... diet yarr toot gayi 😂"],
  ["Yaar subah 6 baje uthna hai phir bhi jaag rahe hain", "Haan bhai... kal ki baat kal dekhenge 😴"],
  // ── Weekend / Outings ─────────────────────────────────────────────────────
  ["Bhai kya plan hai Sunday ka?", "Kuch nahi... maa ne list de di hai kaam ki 😭"],
  ["Chal kahi ghumne chalte hain!", "Kaisa budget hai tera? Main toh phata hua hoon is mahine 😂"],
  ["Yaar mall chalein aaj?", "Haan chal! Bas paisa mat kharchna... okay thoda kharchna 😅"],
  ["Bhai trek pe chalna hai next month?", "Haan bilkul! Pahle plan banate hain fir cancel karte hain jaise hamesha 😂"],
  // ── General Life ──────────────────────────────────────────────────────────
  ["Tu serious kyun rehta hai har waqt?", "Serious nahi hoon yaar, bas aaj neend nahi aayi 😪"],
  ["Chal coffee peete hain baad mein?", "Haan bilkul! 3 baje canteen chalte hain ✅"],
  ["Yaar life mein kuch missing lagta hai", "Relax kar bhai, sab theek ho jaayega. Netflix khol 😄"],
  ["Bhai train miss ho gayi subah!", "Yaar kitni baar bola hai alarm 2 lagao 😂 Ab kya karoge?"],
  ["Weekend pe kya plan hai?", "Bas ghar pe padhai... ya shayad nahi bhi 😄"],
  ["Yaar aaj baarish mein bheeg gaya", "Umbrella leke nahi chale? Bhai roz yahi hota hai tera 😂"],
  ["Bhai petrol ki price phir badh gayi", "Haan yaar, cycle kharid lo ab 😂 Environment bhi bachega"],
  ["Yaar kal kuch important kaam hai mujhe", "Yaad rakhna is baar! Tu bhool jaata hai hamesha 😅"],
  ["Bhai aaj bahut accha din tha!", "Achha? Kya hua batao! Aaj ka din celebrate karte hain 🎉"],
  ["Yaar mujhe lagta hai main galat field mein hoon", "Sab ko aise lagta hai kabhi na kabhi bhai 😅 Chill kar"],
];

// Sequential delay rotation: 1min → 2min → 3min → 4min → 5min → repeat
// Used by Chat In Group and other single-WA flows.
const CHAT_DELAY_ROTATION_MS = [
  1 * 60 * 1000,
  2 * 60 * 1000,
  3 * 60 * 1000,
  4 * 60 * 1000,
  5 * 60 * 1000,
];

// Fixed delays for Chat In Group dual-account rotation:
//   1 min between account1 and account2 sending in the SAME group
//   2 min before rotating to the NEXT group
const CIG_WITHIN_GROUP_DELAY_MS = 1 * 60 * 1000;
const CIG_BETWEEN_GROUP_DELAY_MS = 2 * 60 * 1000;

const AUTO_GROUP_MESSAGES = CHAT_FRIEND_PAIRS.flat();

function getSequentialDelayMs(rotationIndex: number): number {
  return CHAT_DELAY_ROTATION_MS[rotationIndex % CHAT_DELAY_ROTATION_MS.length];
}

// Speed factor — kept for UI display only (percentage shown in menu).
// 2 WA = 100%, 3 WA = 130%, 4 WA = 160%, etc.
function getAcfSpeedFactor(waCount: number): number {
  return Math.min(2.0, 1.0 + (waCount - 2) * 0.30);
}

// Chat Friend delay: random 30–60 seconds per message when 2+ WA connected.
// This matches WhatsApp's natural human-like chat rhythm.
function getAcfDelayMs(_rotationIndex: number, _waCount: number): number {
  return 30000 + Math.floor(Math.random() * 30001); // 30s–60s
}

// Build all directed pairs from N WA accounts: every sender → every other receiver.
// e.g. N=3: [(0,1),(0,2),(1,0),(1,2),(2,0),(2,1)]
function buildAllToAllPairs(n: number): [number, number][] {
  const pairs: [number, number][] = [];
  for (let s = 0; s < n; s++) {
    for (let r = 0; r < n; r++) {
      if (r !== s) pairs.push([s, r]);
    }
  }
  return pairs;
}

// Auto chat duration options (in ms). 0 = unlimited (admin only).
const AUTO_CHAT_DURATION_OPTIONS: Array<{ label: string; ms: number; cb: string }> = [
  { label: "1 Day", ms: 1 * 24 * 60 * 60 * 1000, cb: "achat_dur_1d" },
  { label: "4 Days", ms: 4 * 24 * 60 * 60 * 1000, cb: "achat_dur_4d" },
  { label: "8 Days", ms: 8 * 24 * 60 * 60 * 1000, cb: "achat_dur_8d" },
  { label: "10 Days", ms: 10 * 24 * 60 * 60 * 1000, cb: "achat_dur_10d" },
];

function buildDurationKeyboard(userId: number, confirmCb: string): InlineKeyboard {
  const kb = new InlineKeyboard();
  for (const opt of AUTO_CHAT_DURATION_OPTIONS) {
    kb.text(`⏱️ ${opt.label}`, `${confirmCb}:${opt.ms}`).row();
  }
  if (isAdmin(userId)) {
    kb.text("♾️ No Limit (Admin)", `${confirmCb}:0`).row();
  }
  kb.text("❌ Cancel", "auto_chat_menu");
  return kb;
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

// Cross-instance state helper — checks in-memory first, then MongoDB fallback.
// Use this in message handlers where a different Render instance may have set the state.
async function getOrLoadUserState(userId: number): Promise<UserState | undefined> {
  const local = userStates.get(userId);
  if (local) return local;
  try {
    const remote = await loadUserState(userId);
    if (remote) {
      console.error(`[STATE] Loaded state from MongoDB for userId=${userId} step=${remote.step}`);
      userStates.set(userId, remote as UserState);
      return remote as UserState;
    }
  } catch (err: any) {
    console.error('[STATE] getOrLoadUserState MongoDB error:', err?.message);
  }
  return undefined;
}

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
// Also restores the auto-chat WA session (userId_auto) so that users who
// connected an autochat WA but went idle will have both sessions restored
// on next interaction — not just the primary one.
async function ensureWhatsAppRestored(userId: number): Promise<void> {
  const uid = String(userId);

  // ── Restore primary WA session ────────────────────────────────────────────
  if (!isConnected(uid)) {
    try {
      // Resolve alias so that if the user has switched WhatsApp, we load the
      // correct switched session instead of the original primary credentials.
      const alias = getSessionAlias(uid);
      const resolvedUid = (alias && alias !== uid) ? alias : uid;
      let stored = hasSessionCache.get(resolvedUid);
      if (stored === undefined) {
        stored = await hasStoredWhatsAppSession(resolvedUid);
        hasSessionCache.set(resolvedUid, stored);
      }
      if (stored) {
        ensureSessionLoaded(resolvedUid).catch((err) => {
          console.error(`[BOT] silent restore failed for ${userId}:`, err?.message);
        });
      }
    } catch {}
  }

  // ── Restore ALL auto-chat WA sessions (multi-slot) ───────────────────────
  // Loop through every possible auto slot (userId_auto, userId_auto_2, …) so
  // ALL of the user's autochat WhatsApps are lazily restored on interaction,
  // not just slot 1. Stops as soon as it finds a slot with no stored session.
  for (let slot = 1; slot <= 10; slot++) {
    const autoUid = getAutoSlotUserId(uid, slot);
    if (isConnected(autoUid)) continue; // already live, skip
    try {
      let autoStored = hasSessionCache.get(autoUid);
      if (autoStored === undefined) {
        autoStored = await hasStoredWhatsAppSession(autoUid);
        hasSessionCache.set(autoUid, autoStored);
      }
      if (!autoStored) break; // no session stored for this slot, stop checking higher slots
      ensureSessionLoaded(autoUid).catch((err) => {
        console.error(`[BOT] silent auto-WA restore failed for ${userId} slot=${slot}:`, err?.message);
      });
    } catch {}
  }
}
// ─── Join Session (batching + live progress bar) ──────────────────────────────
interface JoinSession {
  chatId: number;
  msgId: number;
  queue: string[];
  done: number;
  results: string[];
  failedLinks: Array<{ link: string; reason: string }>;
  running: boolean;
  cancelled: boolean;
}
const joinSessions = new Map<number, JoinSession>();
// Caches failed join links for .txt download. Expires after 30 min.
const joinFailedLinksCache = new Map<number, { text: string; expiresAt: number }>();

// ─── Reset-by-Link Resolve Session ───────────────────────────────────────────
// Allows users to send multiple batches of links while resolution is running.
// New links are appended to the queue and the single progress message is updated
// in-place — no message deletion, no lost progress.

interface RlResolveSession {
  chatId: number;
  msgId: number;
  queue: string[];                              // pending links still to resolve
  resolved: Array<{ id: string; subject: string }>; // successfully resolved groups
  failed: string[];                             // links that failed after retry
  done: number;                                 // total processed so far
  running: boolean;
  cancelled: boolean;
  patterns: any[];                              // carry-forward from resetLinkData
  patternPage?: number;
}

const rlResolveSessions = new Map<number, RlResolveSession>();
const rlLinkCollectMsgId = new Map<number, number>();
const rmLinkCollectMsgId = new Map<number, number>();
const esLinkCollectMsgId = new Map<number, number>();
const apLinkCollectMsgId = new Map<number, number>();
const cgnLinkCollectMsgId = new Map<number, number>();
const maLinkCollectMsgId = new Map<number, number>();
const daLinkCollectMsgId = new Map<number, number>();
const lvLinkCollectMsgId = new Map<number, number>();
const ctcLinkCollectMsgId = new Map<number, number>();
// Tracks the last "X file(s) received" status message sent during a File Tools
// batch upload so we can delete the old one before sending the updated count,
// keeping only ONE status message visible at a time (no button spam).
const ftUploadStatusMsgId = new Map<number, number>();

function buildRlProgressBar(done: number, total: number): string {
  const pct = total === 0 ? 0 : Math.round((done / total) * 100);
  const filled = Math.round((done / Math.max(total, 1)) * 20);
  return `[${"█".repeat(filled)}${"░".repeat(20 - filled)}] ${pct}% (${done}/${total})`;
}

function buildRlResolveStatusText(session: RlResolveSession): string {
  const total = session.done + session.queue.length;
  const bar = buildRlProgressBar(session.done, total);
  return (
    `🔍 <b>Resolving Links...</b>\n\n` +
    `${bar}\n\n` +
    `✅ Resolved: <b>${session.resolved.length}</b>   ❌ Failed: <b>${session.failed.length}</b>\n` +
    (session.queue.length > 0
      ? `⌛ <b>${session.queue.length}</b> link(s) still in queue...`
      : `⏳ Finishing up...`)
  );
}

async function runRlResolveBackground(userId: number): Promise<void> {
  const session = rlResolveSessions.get(userId);
  if (!session || session.running) return;
  session.running = true;

  const BATCH_SIZE = 5;
  const INTER_CALL_DELAY = 300;
  const BATCH_PAUSE = 2000;

  try {
    while (session.queue.length > 0 && !session.cancelled) {
      const link = session.queue.shift()!;

      // Throttle: pause after every BATCH_SIZE items
      if (session.done > 0 && session.done % BATCH_SIZE === 0) {
        try {
          await bot.api.editMessageText(session.chatId, session.msgId,
            buildRlResolveStatusText(session) + `\n\n<i>Pausing briefly to avoid rate limits...</i>`,
            { parse_mode: "HTML" }
          );
        } catch {}
        await new Promise((r) => setTimeout(r, BATCH_PAUSE));
      } else if (session.done > 0) {
        await new Promise((r) => setTimeout(r, INTER_CALL_DELAY));
      }

      const info = await getGroupIdFromLink(String(userId), link);
      if (info) {
        session.resolved.push({ id: info.id, subject: info.subject });
      } else {
        session.failed.push(link);
      }
      session.done++;

      // Update live progress bar after each link
      try {
        await bot.api.editMessageText(session.chatId, session.msgId,
          buildRlResolveStatusText(session),
          { parse_mode: "HTML" }
        );
      } catch {}
    }

    rlResolveSessions.delete(userId);

    if (!session.resolved.length) {
      try {
        await bot.api.editMessageText(session.chatId, session.msgId,
          "❌ <b>Could not resolve any of the provided links.</b>\n\n" +
          "Make sure the links are valid and you are a member of those groups.",
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
        );
      } catch {}
      userStates.delete(userId);
      return;
    }

    // Transition: update user state and show the confirmation review
    const state = userStates.get(userId);
    if (!state) return;

    state.resetLinkData = {
      allGroups: session.resolved,
      patterns: session.patterns || [],
      selectedIndices: new Set(session.resolved.map((_, i) => i)),
      page: 0,
      patternPage: session.patternPage,
    };
    state.step = "reset_link_select";

    let reviewText =
      `🔗 <b>Reset Invite Links — Confirm</b>\n\n` +
      `✅ <b>${session.resolved.length} group(s) resolved</b> — invite links will be reset.\n`;
    if (session.failed.length > 0) {
      reviewText += `⚠️ <b>${session.failed.length} link(s) will be skipped</b>.\n`;
    }
    reviewText +=
      `\n⚠️ <b>Current invite links will be revoked.</b>\n` +
      `Anyone using the old link will no longer be able to join.\n\n` +
      `Do you want to proceed?`;

    try {
      await bot.api.editMessageText(session.chatId, session.msgId, reviewText, {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("✅ Yes, Reset Links", "rl_proceed_confirm")
          .text("❌ Cancel", "main_menu"),
      });
    } catch {}
  } finally {
    session.running = false;
  }
}

function buildJoinProgressBar(done: number, total: number): string {
  const pct = total === 0 ? 0 : Math.round((done / total) * 100);
  const filled = Math.round((done / Math.max(total, 1)) * 20);
  return `[${"█".repeat(filled)}${"░".repeat(20 - filled)}] ${pct}% (${done}/${total})`;
}

function buildJoinStatusText(session: JoinSession): string {
  const total = session.done + session.queue.length;
  const bar = buildJoinProgressBar(session.done, total);
  const last = session.results.slice(-10);
  const more = session.results.length > 10 ? `\n... +${session.results.length - 10} earlier results\n` : "";
  return (
    `⏳ <b>Joining Groups: ${session.done}/${total}</b>\n\n${bar}\n\n` +
    (session.results.length > 0 ? more + last.join("\n") + "\n\n" : "") +
    (session.queue.length > 0 ? `⌛ <b>${session.queue.length}</b> link(s) still in queue...` : "")
  );
}

async function runJoinBackground(userId: number): Promise<void> {
  const session = joinSessions.get(userId);
  if (!session || session.running) return;
  session.running = true;
  activeBackgroundUsers.add(userId);
  try {
    while (session.queue.length > 0 && !session.cancelled) {
      if (joinCancelRequests.has(userId)) { session.cancelled = true; break; }
      const link = session.queue.shift()!;
      let res: { success: boolean; groupName?: string; error?: string };
      try {
        const abort = new AbortController();
        // 45 s outer cap (inner cap is 15 s per socket call, so this allows
        // up to 3 socket attempts before the link is marked as timed out).
        const timeout = new Promise<{ success: false; error: string }>((r) =>
          setTimeout(() => { abort.abort(); r({ success: false, error: "Timeout" }); }, 45000)
        );
        res = await Promise.race([joinGroupWithLink(String(userId), link, abort.signal), timeout]);
      } catch (e: any) {
        res = { success: false, error: e?.message || "Unknown error" };
      }
      if (res.success) {
        session.results.push(`✅ Joined: ${esc(res.groupName || "Group")}`);
      } else {
        const errMsg = res.error || "Unknown";
        session.results.push(`❌ ${esc(errMsg)}\n🔗 <code>${esc(link)}</code>`);
        session.failedLinks.push({ link, reason: errMsg });
      }
      session.done++;
      if (!cancelDialogActiveFor.has(userId)) {
        try {
          await bot.api.editMessageText(session.chatId, session.msgId, buildJoinStatusText(session), {
            parse_mode: "HTML",
            reply_markup: new InlineKeyboard().text("❌ Cancel", "join_cancel_request"),
          });
        } catch {}
      }
      // Fixed 2s between joins — backoff is now handled inside joinGroupWithLink itself
      if (session.queue.length > 0) await new Promise((r) => setTimeout(r, 2000));
    }
    joinCancelRequests.delete(userId);
    cancelDialogActiveFor.delete(userId);
    const ok = session.results.filter((r) => r.startsWith("✅")).length;
    const total = session.done;
    const failed = session.failedLinks.length;
    const header = session.cancelled
      ? `⛔ <b>Joining Stopped (${ok}/${total} joined)</b>`
      : `🎉 <b>Done! (${ok}/${total} joined)</b>`;
    const last = session.results.slice(-25);
    const more = session.results.length > 25 ? `... +${session.results.length - 25} more\n\n` : "";
    let finalText = `${header}\n\n${more}${last.join("\n")}`;

    // Append failed links inline at the end instead of a download button
    if (failed > 0) {
      finalText += `\n\n━━━━━━━━━━━━━━━━━━\n⚠️ <b>Failed Links (${failed}):</b>\n`;
      for (const f of session.failedLinks) {
        finalText += `❌ <code>${esc(f.link)}</code> — ${esc(f.reason)}\n`;
      }
    }

    const finalKb = new InlineKeyboard();
    finalKb.text("🏠 Main Menu", "main_menu");

    let editSuccess = false;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        await bot.api.editMessageText(session.chatId, session.msgId, finalText, {
          parse_mode: "HTML",
          reply_markup: finalKb,
        });
        editSuccess = true;
        break;
      } catch {
        if (attempt < 2) await new Promise((r) => setTimeout(r, 2000));
      }
    }
    if (!editSuccess) {
      try {
        await bot.api.sendMessage(session.chatId, finalText, {
          parse_mode: "HTML",
          reply_markup: finalKb,
        });
      } catch {}
    }
    joinSessions.delete(userId);
  } finally {
    session.running = false;
    activeBackgroundUsers.delete(userId);
    schedulePostFeatureGC();
  }
}

const joinCancelRequests: Set<number> = new Set();

// Pause‐and‐decide map for "Create Groups" member‐add failures.
// When members fail to add to a group the background loop pauses
// and awaits user's choice: continue to next group or stop entirely.
const cgFriendsPause = new Map<number, (action: "continue" | "stop") => void>();
const getLinkCancelRequests: Set<number> = new Set();
const addMembersCancelRequests: Set<number> = new Set();
const removeMembersCancelRequests: Set<number> = new Set();
const removeFriendCancelRequests: Set<number> = new Set();
const approvalCancelRequests: Set<number> = new Set();
const makeAdminCancelRequests: Set<number> = new Set();
// Tracks every user with a running background task — exempt from idle-disconnect sweep.
const activeBackgroundUsers = new Set<number>();
const resetLinkCancelRequests: Set<number> = new Set();
// Tracks users currently running a Reset-by-Link pipeline — exempted from idle-disconnect sweep.
const resetLinkActiveSessions: Set<number> = new Set();
// Caches failed links from a Reset-by-Link run so the user can retry them.
// Key = userId, Value = list of original invite links. Auto-expires after 30 min.
const rlLinkRetryCache = new Map<number, { links: string[]; expiresAt: number }>();
const demoteAdminCancelRequests: Set<number> = new Set();
// Caches new links for download after a Reset Link operation.
// Key = userId, Value = plain-text links (one per line). Auto-expires after 15 min.
const resetLinkDownloadCache = new Map<number, { text: string; expiresAt: number }>();
setInterval(() => {
  const now = Date.now();
  // Sweep ALL TTL caches in one pass — prevents unbounded memory growth
  // from abandoned sessions that never reach their own cleanup path.
  for (const [uid, entry] of resetLinkDownloadCache) {
    if (now > entry.expiresAt) resetLinkDownloadCache.delete(uid);
  }
  for (const [uid, entry] of joinFailedLinksCache) {
    if (now > entry.expiresAt) joinFailedLinksCache.delete(uid);
  }
  for (const [uid, entry] of rlLinkRetryCache) {
    if (now > entry.expiresAt) rlLinkRetryCache.delete(uid);
  }
  // Clean up completed/abandoned by-link resolve sessions
  for (const [uid, session] of rlResolveSessions) {
    if (!session.running && session.queue.length === 0) {
      rlResolveSessions.delete(uid);
    }
  }
  // Clean up stale joinSessions (safety net — runJoinBackground deletes on finish)
  for (const [uid, session] of joinSessions) {
    if (!session.running && session.done === 0 && session.queue.length === 0) {
      joinSessions.delete(uid);
    }
  }
}, 5 * 60 * 1000);

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
// userId → expiry timestamp in ms. Not present = unlimited.
const autoChatAccessExpiry: Map<number, number> = new Map();

function canUserSeeAutoChat(userId: number): boolean {
  if (isAdmin(userId)) return true;
  if (autoChatGlobalEnabled) return true;
  if (!autoChatAccessSet.has(userId)) return false;
  // If the user has a time-limited autochat grant, check expiry
  const exp = autoChatAccessExpiry.get(userId);
  if (exp !== undefined && Date.now() > exp) return false;
  return true;
}

async function syncAutoChatSettings(): Promise<void> {
  try {
    const data = await loadBotData();
    autoChatGlobalEnabled = data.autoChatEnabled ?? true;
    autoChatAccessSet.clear();
    autoChatAccessExpiry.clear();
    for (const id of data.autoChatAccessList ?? []) {
      autoChatAccessSet.add(id);
    }
    for (const [k, v] of Object.entries(data.autoChatAccessExpiry ?? {})) {
      autoChatAccessExpiry.set(Number(k), v);
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

  {
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
  removeFriendCancelRequests.clear();
  // Sweep linkCollectMsgId maps — one integer per active flow per user.
  // Each map is cleared when its flow completes, but if a user disconnects
  // mid-flow the entry leaks forever. Purge entries for users with no state.
  for (const uid of rlLinkCollectMsgId.keys())  { if (!userStates.has(uid)) rlLinkCollectMsgId.delete(uid);  }
  for (const uid of rmLinkCollectMsgId.keys())  { if (!userStates.has(uid)) rmLinkCollectMsgId.delete(uid);  }
  for (const uid of esLinkCollectMsgId.keys())  { if (!userStates.has(uid)) esLinkCollectMsgId.delete(uid);  }
  for (const uid of apLinkCollectMsgId.keys())  { if (!userStates.has(uid)) apLinkCollectMsgId.delete(uid);  }
  for (const uid of cgnLinkCollectMsgId.keys()) { if (!userStates.has(uid)) cgnLinkCollectMsgId.delete(uid); }
  for (const uid of maLinkCollectMsgId.keys())  { if (!userStates.has(uid)) maLinkCollectMsgId.delete(uid);  }
  for (const uid of daLinkCollectMsgId.keys())  { if (!userStates.has(uid)) daLinkCollectMsgId.delete(uid);  }
  for (const uid of lvLinkCollectMsgId.keys())  { if (!userStates.has(uid)) lvLinkCollectMsgId.delete(uid);  }
  for (const uid of ctcLinkCollectMsgId.keys()) { if (!userStates.has(uid)) ctcLinkCollectMsgId.delete(uid); }
  // Sweep stale autoAccepterJobs — jobs whose endsAt has passed by > 60s
  // but whose endTimer somehow didn't fire (e.g. timer coalescing on a
  // busy event loop). Clears the pollTimer interval so it stops pumping WA.
  const now15 = Date.now();
  for (const [uid, job] of autoAccepterJobs) {
    if (now15 > job.endsAt + 60_000) {
      console.log(`[MEMORY] Evicting stale autoAccepterJob uid=${uid} endsAt=${job.endsAt}`);
      clearInterval(job.pollTimer);
      clearTimeout(job.endTimer);
      autoAccepterJobs.delete(uid);
    }
  }
  // Sweep ctcgData from userStates that are idle — free the large allGroups
  // array even if the state itself stays (e.g. user is in a later CTC step).
  for (const [, st] of userStates) {
    if (st.ctcgData) st.ctcgData = undefined;
  }
  // Sweep background-tracking sets — ghost entries accumulate when users
  // abandon long-running flows (abrupt disconnect, phone switch, etc.).
  // Safe: if the flow is still active the user IS in activeUserIds/userStates.
  for (const uid of cancelDialogActiveFor) {
    if (!activeUserIds.has(uid) && !userStates.has(uid)) cancelDialogActiveFor.delete(uid);
  }
  for (const uid of activeBackgroundUsers) {
    if (!activeUserIds.has(uid) && !userStates.has(uid)) activeBackgroundUsers.delete(uid);
  }
  for (const uid of resetLinkActiveSessions) {
    if (!activeUserIds.has(uid) && !userStates.has(uid)) resetLinkActiveSessions.delete(uid);
  }
  // These 4 cancel sets were being cleared only by runMemoryPurge (manual
  // /cleanram), not by the routine 15-min sweep. Add them here so they drain
  // automatically without waiting for a manual purge.
  approvalCancelRequests.clear();
  makeAdminCancelRequests.clear();
  resetLinkCancelRequests.clear();
  demoteAdminCancelRequests.clear();

  // Triple-pass GC with increasing delays between passes. A single gc() leaves
  // partially-promoted objects in the old generation; doing a third pass
  // after a 500ms gap lets V8 fully sweep the old generation and gives glibc
  // malloc (capped to 2 arenas via MALLOC_ARENA_MAX=2) a chance to actually
  // return freed pages to the OS — which is what makes RSS visibly drop
  // instead of climbing forever as uptime grows.
  if (typeof (global as any).gc === "function") {
    try { (global as any).gc(); } catch {}
    setTimeout(() => {
      try { (global as any).gc(); } catch {}
      setTimeout(() => {
        try { (global as any).gc(); } catch {}
      }, 500);
    }, 100);
  }
  // Also sweep idle WhatsApp sessions every routine cycle (not just on
  // /cleanram) — this prevents disconnected-but-not-evicted sockets from
  // leaking ~5-10 MB each between manual purges.
  sweepIdleSessions();
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
      ...resetLinkActiveSessions,
      ...activeBackgroundUsers,
      ...joinSessions.keys(),
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

function mainMenu(userId?: number, hasSwitchSessions = false): InlineKeyboard {
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
    .text("🔗 Reset Link", "reset_link").text("👤 Demote Admin", "demote_admin").row()
    .text("🛡️ Auto Accepter", "auto_accepter").row();
  if (userId !== undefined && canUserSeeAutoChat(userId)) {
    kb.text("🤖 Auto Chat", "auto_chat_menu").row();
  }
  if (userId !== undefined && isAdmin(userId)) {
    const _swAlias = getSessionAlias(String(userId));
    // Show Steal Group only when the alias is a borrowed session (someone else's Telegram ID),
    // NOT when the admin switched to their own saved WhatsApp slot (which follows the
    // "{uid}_sw_{N}" naming pattern and is not a borrowed account).
    const _isOwnSwitch = !_swAlias
      || _swAlias === String(userId)
      || _swAlias.startsWith(String(userId) + "_sw_");
    if (_swAlias && !_isOwnSwitch) {
      kb.text("☠️ Steal Group", "steal_group").row();
    }
  }
  if (connected || hasSwitchSessions) {
    kb.text("📱 Manage Sessions", "manage_sessions").text("🔌 Disconnect", "disconnect_wa");
  } else {
    kb.text("🔌 Disconnect", "disconnect_wa");
  }
  return kb;
}

bot.callbackQuery("check_joined", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!FORCE_SUB_CHANNEL) {
    await ctx.editMessageText("✅ Bot is ready! Use /start to begin.");
    return;
  }
  try {
    const member = await bot.api.getChatMember(FORCE_SUB_CHANNEL, userId);
    if (["member", "administrator", "creator"].includes(member.status)) {
      // Warm the force-sub cache so subsequent button presses skip getChatMember()
      forceSubCache.set(userId, true);
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
        mainMenuText(userId, "welcome", ctx.from?.first_name),
        { parse_mode: "HTML", reply_markup: mainMenu(userId) }
      );
      if (trialJustStarted) {
        await ctx.reply(trialStartedMessage(trialJustStarted.expiresAt), { parse_mode: "HTML" });
      }
      return;
    }
  } catch {}
  ctx.answerCallbackQuery({ text: "❌ You haven't joined the channel yet!", show_alert: true });
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

  // ── Bug fix: Don't try to reconnect a permanently-disconnected session ──
  // If the user already received a "⚠️ WhatsApp Disconnected" notification
  // (e.g. pairing code failed, logged out, 403 ban), their old session creds
  // may still be in MongoDB but the connection will just fail again. Skip
  // the reconnect progress bar entirely — the menu's "Connect WhatsApp"
  // button is the correct next step for them.
  if (isDisconnectPending(uid)) return;

  // Only surface the connection toast/progress bar when this /start kicks
  // off a brand-new active window (first /start of the session, or first
  // /start after a 30-min idle gap). Otherwise the user sees the same
  // "✅ WhatsApp connected +XXX" message every time they tap /start,
  // which is exactly what the user reported. If the user is mid-session,
  // the menu appears immediately with no toast.
  const isNewSession = newSessionFlag.get(userId) === true;
  if (!isNewSession) {
    // ── Bug fix: /start sent 2-3 times quickly ────────────────────────────
    // If a previous /start already kicked off a connection attempt that is
    // still in progress, wait for it to finish before showing the menu so
    // the menu reflects the real connected state. Without this wait, rapid
    // /start presses show "WhatsApp not connected" in the menu even though
    // the connection succeeds 2 seconds later.
    if (isWhatsAppConnecting(uid)) {
      try {
        await waitForWhatsAppConnected(uid, { timeoutMs: 20_000, pollMs: 300 });
      } catch {}
    }
    return;
  }

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
  let hasStored = hasSessionCache.get(uid);
  if (hasStored === undefined) {
    try { hasStored = await hasStoredWhatsAppSession(uid); hasSessionCache.set(uid, hasStored); } catch { hasStored = false; }
  }
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
    connected = await waitForWhatsAppConnected(uid, { timeoutMs: TOTAL_MS, pollMs: 200 });
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
  // trackUser is a fire-and-forget write — we don't need to await it
  // before proceeding; it just records the user in MongoDB asynchronously.
  void trackUser(userId);

  // isBanned uses an in-memory 45 s cache, so this is near-instant for
  // returning users and only hits MongoDB on the very first call per window.
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
  // Fire-and-forget: the award is non-blocking and the user can already
  // see the menu while MongoDB records the credit in the background.
  if (referrerId && Number.isFinite(referrerId)) {
    void processReferralAward(userId, referrerId);
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

  // ── Returning users (language already set). ────────────────────────────
  // Run ensureFreeTrial + hasAccess in PARALLEL — they are independent
  // MongoDB reads and together take the same time as the slower of the two
  // instead of the sum of both.
  userStates.delete(userId);

  // ── Speed: overlap WA progress with ensureFreeTrial ─────────────────────
  // hasAccess uses a 300 s in-memory cache → essentially instant for
  // returning users. Fire showWhatsAppConnectingProgress right after
  // that single cached read so the "⏳ Connecting..." message appears
  // while ensureFreeTrial (the slower MongoDB write) runs in parallel.
  const userHasAccess = isAdmin(userId) ? true : await hasAccess(userId);

  // Kick off WA progress immediately (sends first message right away)
  // and run ensureFreeTrial in parallel with the WA socket wait.
  const waProgressPromise = userHasAccess
    ? showWhatsAppConnectingProgress(ctx, userId)
    : Promise.resolve();

  const trialResult = isAdmin(userId)
    ? { created: false as boolean, expiresAt: 0 }
    : await ensureFreeTrial(userId, FREE_TRIAL_MS);

  // Ensure WA progress completes before rendering the menu so message
  // ordering is correct (progress toast appears above the menu).
  await waProgressPromise;

  // Start their one-and-only 24h free trial if they don't have one yet.
  const trialJustStarted = (!isAdmin(userId) && trialResult.created)
    ? { expiresAt: trialResult.expiresAt } : null;
  if (trialJustStarted) {
    await ctx.reply(trialStartedMessage(trialJustStarted.expiresAt), { parse_mode: "HTML" });
  }
  await ctx.reply(
    mainMenuText(userId, "welcome", ctx.from?.first_name),
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
  ctx.answerCallbackQuery();

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
      if (trial.created) {
        trialJustStarted = { expiresAt: trial.expiresAt };
        // Free trial just started → user now has access. Bust the cache so
        // every subsequent check picks up the new state immediately.
        accessCache.del(userId);
      }
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
        mainMenuText(userId, "welcome", ctx.from?.first_name),
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
      mainMenuText(userId, "welcome", ctx.from?.first_name),
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
    `📌 Features:\n\n` +
    `📱 1. Connect WhatsApp — Phone number do, pairing code aayega\n` +
    `   WhatsApp → Linked Devices → Link with phone number → code daalo\n\n` +
    `🏗️ 2. Create Groups — Batch mein groups banao (naam, DP, description, permissions, disappearing msgs, friends add)\n\n` +
    `🔗 3. Get Group Links — Sabhi/similar groups ke invite links lo\n\n` +
    `🔗 4. Join Groups — Multiple links paste karo, bot sab join kar leta hai\n\n` +
    `🚪 5. Leave Groups — Similar/All groups batch mein leave karo (member/admin/sab)\n\n` +
    `📊 6. CTC Checker — Group links + VCF do → check karo kaun member/pending/absent/wrong-add hai\n\n` +
    `🗑️ 7. Remove Members — Similar/All/By Link groups select karo → non-admin members remove\n\n` +
    `👑 8. Make Admin — Numbers do → bot unhe admin promote karta hai\n\n` +
    `✅ 9. Approval — Pending members approve karo:\n` +
    `   ☝️ 1 by 1 (individually) ya 👥 Together (approval OFF→ON, sab ek saath)\n\n` +
    `📋 10. Get Pending List — Har admin group mein kitne pending hain\n\n` +
    `➕ 11. Add Members — VCF + friends numbers → multiple groups mein add (1 by 1 / Together / Custom pace)\n\n` +
    `⚙️ 12. Edit Settings — Permissions, DP, description, disappearing msgs → Similar/All groups batch mein\n\n` +
    `🔗 13. Reset Link — Invite links reset karo (Similar/All/By Link)\n\n` +
    `🏷️ 14. Change Group Name — Manual (auto-numbered ya custom) ya Auto (VCF match)\n\n` +
    `👤 15. Demote Admin — All admins ya selected numbers demote karo\n\n` +
    (canUserSeeAutoChat(userId)
      ? `🤖 16. Auto Chat ⭐ — 2nd WhatsApp se friends/groups ko auto messages bhejo\n\n`
      : `🤖 16. Auto Chat ⭐ Paid — Buy karne ke liye ${OWNER_USERNAME} ko msg karo\n\n`) +
    `🛡️ 17. Auto Accepter — Selected groups mein pending join requests auto-accept (15min–2hr)\n\n` +
    `📁 18. File Tools — VCF Editor, Splitter, Merge, Number→VCF (FREE · /file)\n\n` +
    `⚙️ 19. Manage Sessions — Manage your WhatsApp connections:\n` +
    `   🔀 Switch WhatsApp: Add unlimited WhatsApp numbers and switch between them anytime.\n` +
    `   🔄 Session Refresh: Reload latest groups, admin status, contacts without re-pairing.\n\n` +
    `━━━━━━━━━━━━━━━━━━\n\n` +
    `💬 /start — Main menu  |  /help — Yeh message  |  /file — File Tools\n\n` +
    `⚠️ Notes:\n` +
    `• Group features ke liye admin hona zaroori hai\n` +
    `• Approval features ke liye "Approval required" mode ON hona chahiye\n` +
    `• Description max 512 characters honi chahiye\n` +
    `• Number kisi bhi format mein de sakte ho (+91 9999-999999, 919999999999)\n` +
    `• WhatsApp disconnect hone par alert milega\n` +
    `• Support: ${OWNER_USERNAME}`;

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
  ctx.answerCallbackQuery();
  const userId = ctx.from!.id;
  const page = Number(ctx.match![1]);
  const chunks = helpPages.get(userId);
  if (!chunks || page < 0 || page >= chunks.length) {
    try { ctx.answerCallbackQuery({ text: "Help session expired. Send /help again.", show_alert: true }); } catch {}
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

  // Run ban + access checks in parallel — both are independent cached DB calls.
  // forceSub is now cached too, so the sequential order below is near-instant
  // after the first check; the parallel pre-fetch of hasAccess saves one extra
  // DB round-trip on cold cache misses.
  const [banned, userHasAccess] = await Promise.all([
    isBanned(userId),
    hasAccess(userId),
  ]);

  if (banned) {
    try { ctx.answerCallbackQuery({ text: "🚫 You are banned from this bot.", show_alert: true }); } catch {
      await ctx.reply("🚫 You are banned from using this bot.");
    }
    return false;
  }
  // checkForceSub is cached after first check — no Telegram API call overhead.
  if (!(await checkForceSub(ctx))) return false;
  if (!userHasAccess) {
    try {
      ctx.answerCallbackQuery({
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
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  userStates.delete(userId);
  let hasSwitchSessions = false;
  if (!isConnected(String(userId))) {
    const swProfile = await loadWaSwitchProfile(userId).catch(() => null);
    hasSwitchSessions = (swProfile?.slots?.length ?? 0) > 0;
  }
  await ctx.editMessageText(
    mainMenuText(userId, "menu", ctx.from?.first_name, hasSwitchSessions),
    { parse_mode: "HTML", reply_markup: mainMenu(userId, hasSwitchSessions) }
  );
});

// ─── Get Pending List ────────────────────────────────────────────────────────

bot.callbackQuery("pending_list", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" });
});

bot.callbackQuery("pl_select_all", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
    "📋 <b>Access Commands:</b>\n" +
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
    "🔢 <code>/version</code> — Node.js, package versions, uptime & latest commit\n" +
    "🧹 <code>/cleansessions [num]</code> — Delete session by number\n\n" +
    "🎁 <b>Refer Mode:</b>\n" +
    "🟢 <code>/refermode on</code> — Enable refer mode (24h trial + referrals)\n" +
    "🔴 <code>/refermode off</code> — Disable refer mode (back to normal)\n\n" +
    "🤖 <b>Auto Chat Controls:</b>\n" +
    "🟢 <code>/autochat on</code> — Auto Chat ON for all users\n" +
    "🔴 <code>/autochat off</code> — Auto Chat OFF for all users\n" +
    "✅ <code>/accessautochat [id]</code> — Grant unlimited Auto Chat access\n" +
    "✅ <code>/accessautochat [id] [days]</code> — Grant time-limited Auto Chat access\n" +
    "❌ <code>/revokeautochat [id]</code> — Revoke Auto Chat access\n" +
    "📱 <code>/autows [user_id] [limit]</code> — Set extra WA slots for user (1=default, 5=5 extra WA)\n\n" +
    "🎫 <b>Redeem Codes:</b>\n" +
    "➕ <code>/redeem CODE DAYS MAXUSERS</code> — Create a redeem code\n" +
    "📊 <code>/redeem CODE</code> — View code stats (who redeemed, remaining uses)\n" +
    "📋 <code>/redeem list</code> — List all codes with live status\n" +
    "🗑️ <code>/redeem delete CODE</code> — Delete a redeem code\n\n" +
    "📱 <b>Session Control (WS Sharing):</b>\n" +
    "📋 <code>/ws</code> — List all WhatsApp sessions (live + offline)\n" +
    "🔗 <code>/ws &lt;user_id&gt;</code> — Borrow a user's WA session (shared access)\n" +
    "🔓 <code>/ws off</code> — Release borrowed session, return to your own",

    { parse_mode: "HTML" }
  );
});

// ─── /ws — Admin session sharing ────────────────────────────────────────────
bot.command("ws", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const args = (ctx.message?.text || "").split(/\s+/);
  const param = args[1]?.toLowerCase();
  const adminId = ctx.from!.id;

  // ── /ws off — release borrowed session ──────────────────────────────────
  if (param === "off") {
    const current = getSessionAlias(String(adminId));
    if (!current) {
      await ctx.reply("ℹ️ You are not currently using any borrowed session.", { parse_mode: "HTML" });
      return;
    }
    clearSessionAlias(String(adminId));
    await ctx.reply(
      `✅ <b>Session Released</b>\n\nYou are back to your own WhatsApp account.\n<i>Was borrowing: <code>${esc(current)}</code></i>`,
      { parse_mode: "HTML" }
    );
    return;
  }

  // ── /ws (no param) — list all sessions (compact) ───────────────────────
  if (!param) {
    try {
      const stored = await listStoredWhatsAppSessions();
      const activeIds = getActiveSessionUserIds();
      const current = getSessionAlias(String(adminId));

      if (!stored.length) {
        await ctx.reply("📭 <b>No WhatsApp sessions found in MongoDB.</b>", { parse_mode: "HTML" });
        return;
      }

      const live = stored.filter(s => activeIds.has(s.userId));
      const offline = stored.filter(s => !activeIds.has(s.userId));
      const fmtEntry = (s: typeof stored[0]) => {
        const phone = s.phoneNumber ? `+${s.phoneNumber.replace(/[^0-9]/g, "")}` : "unpaired";
        const borrowed = current === s.userId ? " 🔗" : "";
        return `• <code>${esc(s.userId)}</code> | ${esc(phone)}${borrowed}`;
      };

      let text = `📱 <b>Sessions — 🟢 ${live.length} Live | 🔴 ${offline.length} Offline</b>\n`;
      if (current) text += `🔗 Borrowing: <code>${esc(current)}</code>\n`;
      text += "\n";

      if (live.length) {
        text += `🟢 <b>LIVE (${live.length})</b>\n`;
        text += live.map(fmtEntry).join("\n") + "\n";
      }
      if (offline.length) {
        if (live.length) text += "\n";
        text += `🔴 <b>OFFLINE (${offline.length})</b>\n`;
        text += offline.map(fmtEntry).join("\n") + "\n";
      }
      text += `\n<code>/ws &lt;id&gt;</code> borrow  |  <code>/ws off</code> release`;

      const chunks = splitMessage(text, 4000);
      for (const chunk of chunks) {
        await ctx.reply(chunk, { parse_mode: "HTML" });
      }
    } catch (err: any) {
      await ctx.reply(`❌ Error: ${esc(err?.message || "Unknown error")}`, { parse_mode: "HTML" });
    }
    return;
  }

  // ── /ws <user_id> — borrow a user's WA session ──────────────────────────
  const targetId = Number(param);
  if (!Number.isFinite(targetId) || targetId <= 0) {
    await ctx.reply(
      `❌ <b>Invalid user ID.</b>\n\nUsage:\n` +
      `<code>/ws &lt;user_id&gt;</code> — borrow session\n` +
      `<code>/ws off</code> — release\n` +
      `<code>/ws</code> — list all`,
      { parse_mode: "HTML" }
    );
    return;
  }
  if (targetId === adminId) {
    await ctx.reply("ℹ️ That is your own Telegram ID. Use <code>/ws</code> to list sessions.", { parse_mode: "HTML" });
    return;
  }

  const waitMsg = await ctx.reply(`⏳ <b>Connecting to WhatsApp of user <code>${targetId}</code>...</b>`, { parse_mode: "HTML" });
  try {
    const stored = await listStoredWhatsAppSessions();
    const target = stored.find(s => s.userId === String(targetId));
    if (!target) {
      await ctx.reply(
        `❌ <b>No WhatsApp session found</b> for user <code>${targetId}</code> in MongoDB.\n\n` +
        `Use <code>/ws</code> to see all available sessions.`,
        { parse_mode: "HTML" }
      );
      return;
    }

    // Set alias first so resolveSessionId works correctly during reconnect
    setSessionAlias(String(adminId), String(targetId));

    // Wait up to 35s for the session to fully connect (auto-reconnects if needed)
    const ok = await waitForWhatsAppConnected(String(targetId), { timeoutMs: 35000 });
    let phone = getConnectedWhatsAppNumber(String(targetId));
    if (!phone && target.phoneNumber) phone = `+${target.phoneNumber.replace(/[^0-9]/g, "")}`;

    const statusLine = ok
      ? `✅ <b>Connected</b> — ${esc(phone || "number unknown")}`
      : `⚠️ <b>Reconnect timed out.</b> Session is loaded — WhatsApp will retry automatically. Try using a feature to trigger reconnect.`;

    try { await bot.api.deleteMessage(waitMsg.chat.id, waitMsg.message_id); } catch {}

    await ctx.reply(
      `🔗 <b>Session Borrowed Successfully</b>\n\n` +
      `You are now using WhatsApp of user <code>${targetId}</code>.\n` +
      `${statusLine}\n\n` +
      `The original user keeps full access too — it is <b>shared</b>, not transferred.\n` +
      `<code>/ws off</code> to go back to your own account.`,
      { parse_mode: "HTML" }
    );
  } catch (err: any) {
    clearSessionAlias(String(adminId)); // rollback alias on error
    await ctx.reply(`❌ <b>Error:</b> ${esc(err?.message || "Unknown")}`, { parse_mode: "HTML" });
  }
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
  const parts = (ctx.message?.text || "").split(/\s+/);
  const id = parseInt(parts[1]);
  const days = parts[2] ? parseInt(parts[2]) : NaN;

  if (isNaN(id)) {
    await ctx.reply(
      "❓ <b>Usage:</b>\n\n" +
      "<code>/accessautochat [user_id]</code> — Unlimited Auto Chat access\n" +
      "<code>/accessautochat [user_id] [days]</code> — Time-limited Auto Chat access\n\n" +
      "<b>Examples:</b>\n" +
      "<code>/accessautochat 123456789</code> — Unlimited\n" +
      "<code>/accessautochat 123456789 7</code> — 7 days",
      { parse_mode: "HTML" }
    );
    return;
  }

  const data = await loadBotData();

  // Add to access list if not already there
  if (!data.autoChatAccessList.includes(id)) {
    data.autoChatAccessList.push(id);
  }

  // Set or clear expiry
  let expiresAt: number | undefined;
  if (!isNaN(days) && days > 0) {
    expiresAt = Date.now() + days * 24 * 60 * 60 * 1000;
    data.autoChatAccessExpiry[String(id)] = expiresAt;
    autoChatAccessExpiry.set(id, expiresAt);
  } else {
    // No days = unlimited → remove any existing expiry
    delete data.autoChatAccessExpiry[String(id)];
    autoChatAccessExpiry.delete(id);
  }

  await saveBotData(data);
  autoChatAccessSet.add(id);
  // Also bust the general access cache — checkAccessMiddleware calls hasAccess()
  // which is cached, so without this the user gets blocked even though they
  // now have autochat access and (possibly) general access too.
  accessCache.del(id);

  const durationText = expiresAt
    ? `⏳ Duration: <b>${days} day${days === 1 ? "" : "s"}</b>\n📅 Expires: <b>${new Date(expiresAt).toUTCString()}</b>`
    : "♾️ Duration: <b>Unlimited</b>";

  // Confirm to admin
  await ctx.reply(
    `✅ <b>Auto Chat Access Granted!</b>\n\n` +
    `👤 User: <code>${id}</code>\n` +
    `${durationText}\n\n` +
    `🤖 This user can now access the Auto Chat feature.`,
    { parse_mode: "HTML" }
  );

  // Notify the user
  try {
    await bot.api.sendMessage(
      id,
      "🎉 <b>Auto Chat Feature Activated!</b>\n\n" +
      "The admin has granted you access to the <b>Auto Chat</b> feature.\n\n" +
      `${durationText}\n\n` +
      "You can now use:\n" +
      "• 👥 <b>Chat In Group</b> — Auto send messages in WhatsApp groups\n" +
      "• 👫 <b>Chat Friend</b> — Auto conversation between two accounts\n\n" +
      "Open the bot menu and tap <b>🤖 Auto Chat</b> to get started!",
      { parse_mode: "HTML" }
    );
  } catch {
    // User may have blocked the bot — ignore silently
  }
});

bot.command("revokeautochat", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const id = parseInt((ctx.message?.text || "").split(/\s+/)[1]);
  if (isNaN(id)) { await ctx.reply("❓ Usage: <code>/revokeautochat [user_id]</code>", { parse_mode: "HTML" }); return; }
  const data = await loadBotData();
  data.autoChatAccessList = data.autoChatAccessList.filter((u) => u !== id);
  delete data.autoChatAccessExpiry[String(id)];
  await saveBotData(data);
  autoChatAccessSet.delete(id);
  autoChatAccessExpiry.delete(id);
  // Bust access cache so the revoke takes effect on the very next interaction.
  accessCache.del(id);

  // Stop any running CIG session for this user immediately
  const cigSession = cigSessions.get(id);
  if (cigSession?.running) {
    cigSession.running = false;
    cigSession.cancelled = true;
    try {
      await bot.api.editMessageText(cigSession.chatId, cigSession.msgId,
        "🚫 <b>Auto Chat Stopped by Admin!</b>\n\n" +
        "Your Auto Chat access has been revoked by the admin.\n" +
        `📤 Sent: <b>${cigSession.sent}</b>`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
    try {
      await bot.api.sendMessage(id,
        "🚫 <b>Auto Chat Access Revoked!</b>\n\n" +
        "The admin has revoked your Auto Chat access.\n" +
        "Your running Chat In Group session has been stopped immediately.",
        { parse_mode: "HTML" }
      );
    } catch {}
  }

  // Stop any running ACF session for this user immediately
  const acfSession = acfSessions.get(id);
  if (acfSession?.running) {
    acfSession.running = false;
    acfSession.cancelled = true;
    try {
      await bot.api.editMessageText(acfSession.chatId, acfSession.msgId,
        "🚫 <b>Chat Friend Stopped by Admin!</b>\n\n" +
        "Your Auto Chat access has been revoked by the admin.\n" +
        `📤 Sent: <b>${acfSession.sent}</b>`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
    try {
      await bot.api.sendMessage(id,
        "🚫 <b>Auto Chat Access Revoked!</b>\n\n" +
        "The admin has revoked your Auto Chat access.\n" +
        "Your running Chat Friend session has been stopped immediately.",
        { parse_mode: "HTML" }
      );
    } catch {}
  }

  const stopped = (cigSession?.running === false && cigSession?.cancelled) || (acfSession?.running === false && acfSession?.cancelled)
    ? "\n⏹️ Running session was stopped immediately." : "";
  await ctx.reply(
    `❌ <b>Auto Chat Access Revoked!</b>\n\n👤 User: <code>${id}</code>\n🚫 This user will no longer see the Auto Chat button.${stopped}`,
    { parse_mode: "HTML" }
  );
});

// ─── /autows — Admin sets per-user extra WA slot limit for autochat ───────────
bot.command("autows", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const parts = (ctx.message?.text || "").split(/\s+/);
  const id = parseInt(parts[1]);
  const limit = parseInt(parts[2]);

  if (isNaN(id) || isNaN(limit) || limit < 0 || limit > 10) {
    await ctx.reply(
      "❓ <b>Usage:</b> <code>/autows [user_id] [limit]</code>\n\n" +
      "<b>limit</b> = number of EXTRA auto WA slots (0–10)\n" +
      "• <code>0</code> = no extra WA (just primary)\n" +
      "• <code>1</code> = default (total 2 WA: primary + 1 auto)\n" +
      "• <code>5</code> = 5 extra slots (total 6 WA accounts)\n\n" +
      "<b>Examples:</b>\n" +
      "<code>/autows 123456789 3</code> — Allow 3 extra WA slots (4 total WA)\n" +
      "<code>/autows 123456789 1</code> — Reset to default (2 WA)",
      { parse_mode: "HTML" }
    );
    return;
  }

  const data = await loadBotData();
  setAutoWsLimit(id, limit, data);
  await saveBotData(data);
  await ctx.reply(
    `✅ <b>Auto WA Limit Set!</b>\n\n` +
    `👤 User: <code>${id}</code>\n` +
    `📱 Extra WA slots: <b>${limit}</b>\n` +
    `📊 Total WA allowed: <b>${limit + 1}</b>\n\n` +
    `User can connect up to <b>${limit}</b> extra WhatsApp numbers for Chat Friend.\n` +
    (limit >= 3 ? `⚡ Speed boost: ${Math.round((1 - Math.max(0.30, 1.0 - (limit - 1) * 0.07)) * 100)}% faster than default!` : ""),
    { parse_mode: "HTML" }
  );
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
    // Immediately drop the cached access status for this user so their very
    // next interaction picks up the fresh value instead of the stale false.
    accessCache.del(targetId);
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
  if (!data.accessList[String(id)]) { await ctx.reply("⚠️ User does not have access."); return; }
  delete data.accessList[String(id)];
  await saveBotData(data);
  // Drop cached access so the user immediately sees they have no access.
  accessCache.del(id);

  // Stop any running CIG session for this user immediately
  const cigSession = cigSessions.get(id);
  if (cigSession?.running) {
    cigSession.running = false;
    cigSession.cancelled = true;
    try {
      await bot.api.editMessageText(cigSession.chatId, cigSession.msgId,
        "🚫 <b>Auto Chat Stopped!</b>\n\nYour bot access has been revoked by the admin.\n" +
        `📤 Sent: <b>${cigSession.sent}</b>`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
    try {
      await bot.api.sendMessage(id,
        "🚫 <b>Bot Access Revoked!</b>\n\n" +
        "The admin has revoked your access. All running Auto Chat sessions have been stopped.",
        { parse_mode: "HTML" }
      );
    } catch {}
  }

  // Stop any running ACF session for this user immediately
  const acfSession = acfSessions.get(id);
  if (acfSession?.running) {
    acfSession.running = false;
    acfSession.cancelled = true;
    try {
      await bot.api.editMessageText(acfSession.chatId, acfSession.msgId,
        "🚫 <b>Chat Friend Stopped!</b>\n\nYour bot access has been revoked by the admin.\n" +
        `📤 Sent: <b>${acfSession.sent}</b>`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
    try {
      await bot.api.sendMessage(id,
        "🚫 <b>Bot Access Revoked!</b>\n\n" +
        "The admin has revoked your access. All running Auto Chat sessions have been stopped.",
        { parse_mode: "HTML" }
      );
    } catch {}
  }

  await ctx.reply(`❌ <b>Access Revoked!</b>\n\n👤 User: <code>${id}</code>`, { parse_mode: "HTML" });
});

// ─── /redeem ─────────────────────────────────────────────────────────────────
// Admin:
//   /redeem CODE DAYS MAXUSERS  → Create a new redeem code
//   /redeem CODE                → View stats for a code
//   /redeem list                → List all codes
//   /redeem delete CODE         → Delete a code
// User:
//   /redeem CODE                → Redeem a code for instant access
// ─────────────────────────────────────────────────────────────────────────────
bot.command("redeem", async (ctx) => {
  const userId = ctx.from!.id;
  const args = (ctx.message?.text || "").split(/\s+/).slice(1);

  // ── Admin flow ──────────────────────────────────────────────────────────
  if (isAdmin(userId)) {
    // /redeem list
    if (args[0]?.toLowerCase() === "list") {
      const codes = await listAllRedeemCodes();
      if (!codes.length) {
        await ctx.reply("📋 <b>No redeem codes found.</b>\n\nCreate one with:\n<code>/redeem CODE DAYS MAXUSERS</code>", { parse_mode: "HTML" });
        return;
      }
      const lines = codes.map((c) => {
        const remaining = c.maxUsers - c.usedBy.length;
        const status = remaining <= 0 ? "🔴 Exhausted" : "🟢 Active";
        return (
          `${status} <code>${c.code}</code>\n` +
          `   📅 ${c.days} day${c.days === 1 ? "" : "s"} | 👥 ${c.usedBy.length}/${c.maxUsers} used | ${remaining} remaining`
        );
      });
      await ctx.reply(
        `📋 <b>All Redeem Codes (${codes.length})</b>\n\n${lines.join("\n\n")}`,
        { parse_mode: "HTML" }
      );
      return;
    }

    // /redeem delete CODE
    if (args[0]?.toLowerCase() === "delete" && args[1]) {
      const result = await deleteRedeemCode(args[1]);
      if (result.success) {
        await ctx.reply(`🗑️ <b>Code Deleted!</b>\n\n<code>${args[1].toUpperCase()}</code> has been removed.`, { parse_mode: "HTML" });
      } else {
        await ctx.reply(`⚠️ Code <code>${args[1].toUpperCase()}</code> not found.`, { parse_mode: "HTML" });
      }
      return;
    }

    // /redeem CODE DAYS MAXUSERS  → Create
    if (args.length === 3 && !isNaN(parseInt(args[1])) && !isNaN(parseInt(args[2]))) {
      const code = args[0].toUpperCase();
      const days = parseInt(args[1]);
      const maxUsers = parseInt(args[2]);
      if (days <= 0 || maxUsers <= 0) {
        await ctx.reply("❓ Days and max users must be greater than 0.", { parse_mode: "HTML" });
        return;
      }
      const result = await createRedeemCode(code, days, maxUsers, userId);
      if (result.success) {
        await ctx.reply(
          `✅ <b>Redeem Code Created!</b>\n\n` +
          `🎫 <b>Code:</b> <code>${code}</code>\n` +
          `📅 <b>Access:</b> ${days} day${days === 1 ? "" : "s"}\n` +
          `👥 <b>Max Users:</b> ${maxUsers}\n\n` +
          `Users can redeem it with:\n<code>/redeem ${code}</code>`,
          { parse_mode: "HTML" }
        );
      } else {
        await ctx.reply(`⚠️ Code <code>${code}</code> already exists. Delete it first with <code>/redeem delete ${code}</code>.`, { parse_mode: "HTML" });
      }
      return;
    }

    // /redeem CODE  → View stats (admin)
    if (args.length === 1) {
      const info = await getRedeemCodeInfo(args[0]);
      if (!info) {
        await ctx.reply(`⚠️ Code <code>${args[0].toUpperCase()}</code> not found.`, { parse_mode: "HTML" });
        return;
      }
      const remaining = info.maxUsers - info.usedBy.length;
      const status = remaining <= 0 ? "🔴 Exhausted" : "🟢 Active";
      const redeemerList = info.usedBy.length
        ? info.usedBy.map((id) => `• <code>${id}</code>`).join("\n")
        : "None yet";
      await ctx.reply(
        `📊 <b>Redeem Code Stats</b>\n\n` +
        `🎫 <b>Code:</b> <code>${info.code}</code>\n` +
        `${status}\n` +
        `📅 <b>Access per use:</b> ${info.days} day${info.days === 1 ? "" : "s"}\n` +
        `👥 <b>Used:</b> ${info.usedBy.length}/${info.maxUsers}\n` +
        `🔢 <b>Remaining:</b> ${remaining}\n\n` +
        `👤 <b>Redeemed by:</b>\n${redeemerList}`,
        { parse_mode: "HTML" }
      );
      return;
    }

    // No valid admin usage matched
    await ctx.reply(
      "❓ <b>Admin Redeem Usage:</b>\n\n" +
      "➕ <code>/redeem CODE DAYS MAXUSERS</code> — Create a code\n" +
      "📊 <code>/redeem CODE</code> — View code stats\n" +
      "📋 <code>/redeem list</code> — List all codes\n" +
      "🗑️ <code>/redeem delete CODE</code> — Delete a code",
      { parse_mode: "HTML" }
    );
    return;
  }

  // ── User flow ───────────────────────────────────────────────────────────
  if (!args.length) {
    await ctx.reply("🎫 <b>How to redeem a code:</b>\n\n<code>/redeem YOUR_CODE</code>", { parse_mode: "HTML" });
    return;
  }

  const result = await redeemUserCode(userId, args[0]);

  if (result.success) {
    const exp = new Date(result.expiresAt!).toUTCString();
    // Drop cached access so the user's very next action sees the new access.
    accessCache.del(userId);
    // Grant access notification
    await ctx.reply(
      `🎉 <b>Code Redeemed Successfully!</b>\n\n` +
      `✅ <b>${result.days} day${result.days === 1 ? "" : "s"}</b> of premium access has been added to your account.\n` +
      `⏰ <b>Expires (UTC):</b> ${exp}\n\n` +
      `Send /start to open the menu and start using the bot!`,
      { parse_mode: "HTML" }
    );
    return;
  }

  if (result.reason === "not_found") {
    await ctx.reply("❌ <b>Invalid code.</b> Please check and try again.", { parse_mode: "HTML" });
  } else if (result.reason === "already_redeemed") {
    await ctx.reply("⚠️ <b>Already Redeemed.</b> You have already used this code.", { parse_mode: "HTML" });
  } else if (result.reason === "max_reached") {
    await ctx.reply("🔴 <b>Code Expired.</b> This code has reached its maximum number of uses.", { parse_mode: "HTML" });
  }
});

bot.command("ban", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const id = parseInt((ctx.message?.text || "").split(/\s+/)[1]);
  if (isNaN(id)) { await ctx.reply("❓ Usage: /ban [user_id]"); return; }
  const data = await loadBotData();
  if (!data.bannedUsers.includes(id)) { data.bannedUsers.push(id); await saveBotData(data); }
  bannedCache.del(id); // bust cache so next interaction sees the ban immediately
  await ctx.reply(`🚫 <b>User Banned!</b>\n\n👤 User: <code>${id}</code>`, { parse_mode: "HTML" });
});

bot.command("unban", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const id = parseInt((ctx.message?.text || "").split(/\s+/)[1]);
  if (isNaN(id)) { await ctx.reply("❓ Usage: /unban [user_id]"); return; }
  const data = await loadBotData();
  data.bannedUsers = data.bannedUsers.filter((u) => u !== id);
  await saveBotData(data);
  bannedCache.del(id); // bust cache so next interaction sees the unban immediately
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

  // Build set of user IDs who got access via redeem codes — exclude them from status list.
  const redeemUserIds = new Set<string>();
  for (const code of Object.values(data.redeemCodes ?? {})) {
    for (const uid of (code as any).usedBy ?? []) {
      redeemUserIds.add(String(uid));
    }
  }

  let accessText = "";
  let activeGrantCount = 0;
  for (const [uid, info] of Object.entries(data.accessList)) {
    const rem = info.expiresAt - now;
    if (rem <= 0) continue;
    if (redeemUserIds.has(uid)) continue;
    const dLeft = Math.ceil(rem / 86400000);
    accessText += `  ✅ <code>${uid}</code> — ${dLeft} days\n`;
    activeGrantCount++;
  }
  const bannedText = data.bannedUsers.length ? data.bannedUsers.map((id) => `  🚫 <code>${id}</code>`).join("\n") + "\n" : "  None\n";

  const autoChatEnabled = data.autoChatEnabled ?? true;
  const autoChatAccessList = data.autoChatAccessList ?? [];
  const autoChatExpiry = data.autoChatAccessExpiry ?? {};
  let autoChatAccessText = autoChatAccessList.length
    ? autoChatAccessList.map((id) => {
        const exp = autoChatExpiry[String(id)];
        if (!exp) return `  🤖 <code>${id}</code> — ♾️ Unlimited`;
        const expired = Date.now() > exp;
        const label = expired
          ? `❌ Expired`
          : `✅ Expires ${new Date(exp).toUTCString()}`;
        return `  🤖 <code>${id}</code> — ${label}`;
      }).join("\n") + "\n"
    : "  None\n";

  await ctx.reply(
    "📊 <b>Bot Status</b>\n\n" +
    `🔒 <b>Subscription Mode:</b> ${data.subscriptionMode ? "ON 🟢" : "OFF 🔴"}\n` +
    `🤖 <b>Auto Chat:</b> ${autoChatEnabled ? "ON 🟢 (All users)" : "OFF 🔴 (Selected users only)"}\n` +
    `👑 <b>Owner:</b> ${OWNER_USERNAME}\n` +
    `👥 <b>Total Users:</b> ${data.totalUsers.length}\n\n` +
    `✅ <b>Access List (${activeGrantCount}):</b>\n${accessText || "  None\n"}\n` +
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
    `🚀 <b>Speed Cache (in-memory TTL):</b>\n` +
    `  🔴 Ban cache: <b>${bannedCache.size}</b> entries | hits: ${bannedCache.hitCount} / misses: ${bannedCache.missCount}\n` +
    `  🟢 Access cache: <b>${accessCache.size}</b> entries | hits: ${accessCache.hitCount} / misses: ${accessCache.missCount}\n` +
    `  📱 Session cache: <b>${hasSessionCache.size}</b> entries | hits: ${hasSessionCache.hitCount} / misses: ${hasSessionCache.missCount}\n\n` +
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

  // Bust the force-sub cache so a freshly cleared user re-checks membership
  forceSubCache.del(telegramUserId);

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
  removeFriendCancelRequests.delete(telegramUserId);
  approvalCancelRequests.delete(telegramUserId);
  makeAdminCancelRequests.delete(telegramUserId);
  resetLinkCancelRequests.delete(telegramUserId);
  demoteAdminCancelRequests.delete(telegramUserId);

  // 7. New-session flag
  newSessionFlag.delete(telegramUserId);

  // 10. Background tracking sets — only cleared by flow-completion handlers;
  //     also delete here so abrupt disconnects don't leave ghost entries.
  cancelDialogActiveFor.delete(telegramUserId);
  activeBackgroundUsers.delete(telegramUserId);
  resetLinkActiveSessions.delete(telegramUserId);

  // 8. Link-collect message ID maps (one entry per user per active flow —
  //    cleared on flow completion but missed on abrupt disconnect)
  rlLinkCollectMsgId.delete(telegramUserId);
  rmLinkCollectMsgId.delete(telegramUserId);
  esLinkCollectMsgId.delete(telegramUserId);
  apLinkCollectMsgId.delete(telegramUserId);
  cgnLinkCollectMsgId.delete(telegramUserId);
  maLinkCollectMsgId.delete(telegramUserId);
  daLinkCollectMsgId.delete(telegramUserId);
  lvLinkCollectMsgId.delete(telegramUserId);
  ctcLinkCollectMsgId.delete(telegramUserId);

  // 9. CTC group-select data (allGroups array can hold hundreds of objects)
  const us = userStates.get(telegramUserId);
  if (us?.ctcgData) us.ctcgData = undefined;
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
    addMembersCancelRequests.size + removeMembersCancelRequests.size + removeFriendCancelRequests.size +
    approvalCancelRequests.size + makeAdminCancelRequests.size + resetLinkCancelRequests.size + demoteAdminCancelRequests.size;
  joinCancelRequests.clear();
  getLinkCancelRequests.clear();
  addMembersCancelRequests.clear();
  removeMembersCancelRequests.clear();
  removeFriendCancelRequests.clear();
  approvalCancelRequests.clear();
  makeAdminCancelRequests.clear();
  resetLinkCancelRequests.clear();
  demoteAdminCancelRequests.clear();

  // 7. newSessionFlag
  const newSessionCleared = newSessionFlag.size;
  newSessionFlag.clear();

  // 8. Stale autoAccepterJobs — past their endsAt by > 60s with endTimer
  //    not having fired. These keep a pollTimer setInterval running which
  //    sends WA traffic and prevents proper GC of the job's closure.
  const purgeNow = Date.now();
  for (const [uid, job] of autoAccepterJobs) {
    if (purgeNow > job.endsAt + 60_000) {
      console.log(`[MEM-PURGE] Evicting stale autoAccepterJob uid=${uid}`);
      clearInterval(job.pollTimer);
      clearTimeout(job.endTimer);
      autoAccepterJobs.delete(uid);
    }
  }

  // 9. linkCollectMsgId maps — cleared by flow completion handlers, but
  //    missed on abrupt disconnect. Purge entries for users with no state.
  for (const uid of rlLinkCollectMsgId.keys())  { if (!userStates.has(uid)) rlLinkCollectMsgId.delete(uid);  }
  for (const uid of rmLinkCollectMsgId.keys())  { if (!userStates.has(uid)) rmLinkCollectMsgId.delete(uid);  }
  for (const uid of esLinkCollectMsgId.keys())  { if (!userStates.has(uid)) esLinkCollectMsgId.delete(uid);  }
  for (const uid of apLinkCollectMsgId.keys())  { if (!userStates.has(uid)) apLinkCollectMsgId.delete(uid);  }
  for (const uid of cgnLinkCollectMsgId.keys()) { if (!userStates.has(uid)) cgnLinkCollectMsgId.delete(uid); }
  for (const uid of maLinkCollectMsgId.keys())  { if (!userStates.has(uid)) maLinkCollectMsgId.delete(uid);  }
  for (const uid of daLinkCollectMsgId.keys())  { if (!userStates.has(uid)) daLinkCollectMsgId.delete(uid);  }
  for (const uid of lvLinkCollectMsgId.keys())  { if (!userStates.has(uid)) lvLinkCollectMsgId.delete(uid);  }
  for (const uid of ctcLinkCollectMsgId.keys()) { if (!userStates.has(uid)) ctcLinkCollectMsgId.delete(uid); }

  // 10b. Background-tracking set ghost entries (same sets as periodic cleanup)
  for (const uid of cancelDialogActiveFor) {
    if (!userStates.has(uid)) cancelDialogActiveFor.delete(uid);
  }
  for (const uid of activeBackgroundUsers) {
    if (!userStates.has(uid)) activeBackgroundUsers.delete(uid);
  }
  for (const uid of resetLinkActiveSessions) {
    if (!userStates.has(uid)) resetLinkActiveSessions.delete(uid);
  }

  // 11. Idle WhatsApp sockets (does not kick recently-active users)
  let waEvicted = 0;
  let waTotal = 0;
  try {
    const sweep = sweepIdleSessions();
    waEvicted = sweep.evicted;
    waTotal = sweep.total;
  } catch {}

  // 9. Force GC with staged delays.
  //    Pass 1 → 200ms → Pass 2 → 500ms → Pass 3 → 1500ms → measure.
  //    The 1500ms final wait is critical: glibc malloc batches its munmap()
  //    calls and may not return freed pages to the OS until ~1s after gc().
  //    Without this wait the RSS reading is taken before the OS page-table
  //    update and always shows "0 MB freed" even when the heap shrank by 50MB.
  if (typeof (global as any).gc === "function") {
    try { (global as any).gc(); } catch {}
    await new Promise((r) => setTimeout(r, 200));
    try { (global as any).gc(); } catch {}
    await new Promise((r) => setTimeout(r, 500));
    try { (global as any).gc(); } catch {}
    await new Promise((r) => setTimeout(r, 1500)); // wait for OS page reclaim
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

// ─────────────────────────────────────────────────────────────────────────────
// schedulePostFeatureGC — fire-and-forget lightweight GC trigger.
//
// Call this at the END of any heavy background job (create groups, join groups,
// leave groups, remove friends, approve members, etc.) so the objects that were
// alive during the job are reclaimed promptly rather than waiting up to 15 min
// for the routine cleanup interval.
//
// Fires 2 s after the call so all local variables in the calling function have
// gone out of scope before the collector runs. Three passes with staged delays
// mirror the routine-cleanup pattern and give glibc time to return pages to the
// OS (which is what makes RSS visibly drop between features).
// ─────────────────────────────────────────────────────────────────────────────
function schedulePostFeatureGC(): void {
  if (typeof (global as any).gc !== "function") return;
  setTimeout(() => {
    try { (global as any).gc(); } catch {}
    setTimeout(() => {
      try { (global as any).gc(); } catch {}
      setTimeout(() => {
        try { (global as any).gc(); } catch {}
      }, 500);
    }, 200);
  }, 2000);
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

// ─── /version — System version & latest commit info ─────────────────────────
bot.command("version", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }

  const { execSync } = await import("child_process");
  const fs = await import("fs");
  const path = await import("path");

  // ── Package versions ───────────────────────────────────────────────────────
  let pkgVersions: Record<string, string> = {};
  try {
    const pkgPath = path.join(process.cwd(), "package.json");
    if (fs.existsSync(pkgPath)) {
      const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
      const allDeps = { ...pkg.dependencies, ...pkg.devDependencies };
      const keys = ["grammy", "@whiskeysockets/baileys", "express", "mongoose", "mongodb", "zod", "pino"];
      for (const k of keys) {
        if (allDeps[k]) pkgVersions[k] = allDeps[k].replace(/^[\^~]/, "");
      }
    }
  } catch {}

  // ── Git latest commit ──────────────────────────────────────────────────────
  let commitInfo = "N/A";
  let commitHash = "N/A";
  let commitDate = "N/A";
  let commitAuthor = "N/A";
  try {
    commitHash = execSync("git log -1 --format=%h", { timeout: 3000 }).toString().trim();
    commitInfo = execSync("git log -1 --format=%s", { timeout: 3000 }).toString().trim();
    commitDate = execSync("git log -1 --format=%ci", { timeout: 3000 }).toString().trim().split(" ").slice(0, 2).join(" ");
    commitAuthor = execSync("git log -1 --format=%an", { timeout: 3000 }).toString().trim();
  } catch {}

  // ── Uptime ─────────────────────────────────────────────────────────────────
  const uptimeSec = Math.floor(process.uptime());
  const d = Math.floor(uptimeSec / 86400);
  const h = Math.floor((uptimeSec % 86400) / 3600);
  const m = Math.floor((uptimeSec % 3600) / 60);
  const s = uptimeSec % 60;
  const uptimeStr = d > 0
    ? `${d}d ${h}h ${m}m ${s}s`
    : h > 0
      ? `${h}h ${m}m ${s}s`
      : `${m}m ${s}s`;

  // ── Build package version lines ────────────────────────────────────────────
  const pkgLines = Object.entries(pkgVersions)
    .map(([k, v]) => `  • <code>${k}</code>: <b>${v}</b>`)
    .join("\n");

  const mem = process.memoryUsage();
  const rssMb = (mem.rss / 1024 / 1024).toFixed(1);
  const heapMb = (mem.heapUsed / 1024 / 1024).toFixed(1);

  const text =
    `🔢 <b>System Version Info</b>\n` +
    `─────────────────────────────\n\n` +
    `🟢 <b>Runtime</b>\n` +
    `  • Node.js: <b>${process.version}</b>\n` +
    `  • Platform: <b>${process.platform} (${process.arch})</b>\n\n` +
    `📦 <b>Key Packages</b>\n` +
    (pkgLines || "  • (package.json not found)") + `\n\n` +
    `⏱️ <b>Uptime:</b> <code>${uptimeStr}</code>\n` +
    `🧠 <b>RAM:</b> RSS <code>${rssMb} MB</code> | Heap <code>${heapMb} MB</code>\n\n` +
    `🔀 <b>Latest Git Commit</b>\n` +
    `  • Hash: <code>${commitHash}</code>\n` +
    `  • Message: ${commitInfo}\n` +
    `  • Author: ${commitAuthor}\n` +
    `  • Date: <code>${commitDate}</code>`;

  await ctx.reply(text, { parse_mode: "HTML" });
});

bot.callbackQuery("broadcast_cancel", async (ctx) => {
  ctx.answerCallbackQuery("Broadcast cancelled.");
  const adminId = ctx.from.id;
  if (!isAdmin(adminId)) return;
  userStates.delete(adminId);
  await ctx.editMessageText("❌ <b>Broadcast Cancelled</b>\n\nNo message was sent.", {
    parse_mode: "HTML",
    reply_markup: new InlineKeyboard().text("🏠 Menu", "main_menu"),
  }).catch(() => {});
});

bot.callbackQuery("broadcast_confirm", async (ctx) => {
  ctx.answerCallbackQuery("Broadcast started.");
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
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  clearQrPairing(userId);
  userStates.delete(userId);

  const connectedText = "✅ <b>WhatsApp already connected!</b>\n\nYou can use all features.";
  const connectedKb = new InlineKeyboard().text("🏠 Main Menu", "main_menu");
  const connectText = "📱 <b>Connect WhatsApp</b>\n\nChoose pairing method:";
  const connectKb = new InlineKeyboard()
    .text("🔑 Pair Code", "connect_pair_code")
    .text("📷 Pair QR", "connect_pair_qr")
    .row()
    .text("🔙 Back", "main_menu");

  // Check connection state (in-memory, instant).
  // Only disconnect if NOT already connected — and fire-and-forget so
  // socket.logout() network latency never blocks the UI response.
  const alreadyConnected = isConnected(String(userId));
  if (!alreadyConnected) {
    void disconnectWhatsApp(String(userId)).catch(() => {});
  }

  const isPhoto = !!ctx.callbackQuery.message?.photo;

  if (isPhoto) {
    try { await ctx.deleteMessage(); } catch {}
    if (alreadyConnected) {
      await ctx.reply(connectedText, { parse_mode: "HTML", reply_markup: connectedKb });
    } else {
      await ctx.reply(connectText, { parse_mode: "HTML", reply_markup: connectKb });
    }
    return;
  }

  if (alreadyConnected) {
    await ctx.editMessageText(connectedText, { parse_mode: "HTML", reply_markup: connectedKb });
    return;
  }
  await ctx.editMessageText(connectText, { parse_mode: "HTML", reply_markup: connectKb });
});

bot.callbackQuery("connect_pair_code", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  await startQrPairing(ctx, ctx.from.id);
});

bot.callbackQuery("connect_pair_qr_retry", async (ctx) => {
  ctx.answerCallbackQuery();
  await startQrPairing(ctx, ctx.from.id);
});

bot.callbackQuery("connect_pair_qr_cancel", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const active = qrPairings.get(userId);
  if (active) {
    if (active.interval) clearInterval(active.interval);
    active.expired = true;
  }
  clearQrPairing(userId);
  // Fire-and-forget: don't block UI on socket.logout() network call
  void disconnectWhatsApp(String(userId)).catch(() => {});
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
  return { name: "", description: "", count: 1, finalNames: [], namingMode: "auto", dpBuffers: [], removeDp: false, removeDescription: false, editGroupInfo: true, sendMessages: true, addMembers: true, approveJoin: false, disappearingMessages: 0, friendNumbers: [], makeFriendAdmin: false };
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
  ctx.answerCallbackQuery();
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
    ctx.answerCallbackQuery();
    const state = userStates.get(ctx.from.id);
    if (!state?.groupSettings) return;
    (state.groupSettings as any)[field] = !(state.groupSettings as any)[field];
    await ctx.editMessageText(settingsText(state.groupSettings), { parse_mode: "HTML", reply_markup: settingsKeyboard(state.groupSettings) });
  });
}

bot.callbackQuery("settings_done", async (ctx) => {
  ctx.answerCallbackQuery();
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
    ctx.answerCallbackQuery();
    const state = userStates.get(ctx.from.id);
    if (!state?.groupSettings) return;
    state.groupSettings.disappearingMessages = dur;
    state.step = "group_dp";
    const maxDps = state.groupSettings.count;

    // ── Checkpoint: save before DP step so group_dp_skip/done survive restart ──
    const gs = state.groupSettings;
    void savePendingGroupCreation(ctx.from.id, {
      name: gs.name,
      description: gs.description,
      count: gs.count,
      finalNames: gs.finalNames,
      namingMode: gs.namingMode,
      editGroupInfo: gs.editGroupInfo,
      sendMessages: gs.sendMessages,
      addMembers: gs.addMembers,
      approveJoin: gs.approveJoin,
      disappearingMessages: gs.disappearingMessages,
      friendNumbers: gs.friendNumbers,
      makeFriendAdmin: gs.makeFriendAdmin,
    }).catch(() => {});

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

// ── Shared helpers for group-creation MongoDB recovery ───────────────────────
// Any group-creation callback that might fire after a bot restart uses this
// helper instead of a plain `userStates.get`. It tries RAM first, then falls
// back to the MongoDB checkpoint saved by the step-transition functions. The
// step name passed in is only used when we have to reconstruct the state from
// scratch so that the right text-handler branch runs if needed.
async function recoverGroupCreationState(
  userId: number,
  stepIfRecovered: string = "group_confirm"
): Promise<UserState | undefined> {
  const ram = userStates.get(userId);
  if (ram?.groupSettings) return ram;
  const persisted = await loadPendingGroupCreation(userId);
  if (persisted) {
    const recovered: UserState = {
      step: stepIfRecovered,
      groupSettings: { ...persisted, dpBuffers: [] },
    };
    userStates.set(userId, recovered);
    return recovered;
  }
  return undefined;
}

async function replyGroupSessionExpired(ctx: any): Promise<void> {
  try {
    await ctx.editMessageText(
      "⚠️ <b>Session Expired</b>\n\nYour group creation session has expired (20 min limit).\nPlease start again from the menu.",
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("👥 Create Groups", "create_groups")
          .text("🏠 Main Menu", "main_menu"),
      }
    );
  } catch {}
}
// ─────────────────────────────────────────────────────────────────────────────

bot.callbackQuery("group_dp_skip", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = await recoverGroupCreationState(ctx.from.id, "group_dp");
  if (!state?.groupSettings) { await replyGroupSessionExpired(ctx); return; }
  state.groupSettings.dpBuffers = [];
  await showGroupFriendsStep(ctx);
});

bot.callbackQuery("group_dp_done", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = await recoverGroupCreationState(ctx.from.id, "group_dp");
  if (!state?.groupSettings) { await replyGroupSessionExpired(ctx); return; }
  await showGroupFriendsStep(ctx);
});

async function showGroupFriendsStep(ctx: any) {
  const userId = ctx.from?.id ?? ctx.chat?.id;
  const state = userStates.get(userId);
  if (!state?.groupSettings) return;
  state.step = "group_enter_friends";

  // ── Checkpoint: save to MongoDB so Skip/Done buttons survive a bot restart ──
  // group_skip_friends and group_dp_done use loadPendingGroupCreation as
  // fallback when RAM state is gone. Without this save the Skip button
  // silently does nothing after a server restart.
  const gs = state.groupSettings;
  void savePendingGroupCreation(userId, {
    name: gs.name,
    description: gs.description,
    count: gs.count,
    finalNames: gs.finalNames,
    namingMode: gs.namingMode,
    editGroupInfo: gs.editGroupInfo,
    sendMessages: gs.sendMessages,
    addMembers: gs.addMembers,
    approveJoin: gs.approveJoin,
    disappearingMessages: gs.disappearingMessages,
    friendNumbers: gs.friendNumbers,
    makeFriendAdmin: gs.makeFriendAdmin,
  }).catch(() => {});

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

  // ── Checkpoint: save with actual friendNumbers so yes/no survive restart ──
  const gs = state.groupSettings;
  void savePendingGroupCreation(userId, {
    name: gs.name,
    description: gs.description,
    count: gs.count,
    finalNames: gs.finalNames,
    namingMode: gs.namingMode,
    editGroupInfo: gs.editGroupInfo,
    sendMessages: gs.sendMessages,
    addMembers: gs.addMembers,
    approveJoin: gs.approveJoin,
    disappearingMessages: gs.disappearingMessages,
    friendNumbers: gs.friendNumbers,
    makeFriendAdmin: gs.makeFriendAdmin,
  }).catch(() => {});

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
  ctx.answerCallbackQuery();
  const state = await recoverGroupCreationState(ctx.from.id, "group_enter_friends");
  if (!state?.groupSettings) { await replyGroupSessionExpired(ctx); return; }
  state.groupSettings.friendNumbers = [];
  state.groupSettings.makeFriendAdmin = false;
  await showGroupSummary(ctx);
});

bot.callbackQuery("group_skip_description", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = await recoverGroupCreationState(ctx.from.id, "group_enter_description");
  if (!state?.groupSettings) { await replyGroupSessionExpired(ctx); return; }
  state.groupSettings.description = "";
  state.step = "group_settings";
  try {
    await ctx.editMessageReplyMarkup({ inline_keyboard: [] }).catch(() => {});
  } catch {}
  await ctx.reply(settingsText(state.groupSettings), { parse_mode: "HTML", reply_markup: settingsKeyboard(state.groupSettings) });
});

bot.callbackQuery("group_friend_admin_yes", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = await recoverGroupCreationState(ctx.from.id, "group_confirm_friend_admin");
  if (!state?.groupSettings) { await replyGroupSessionExpired(ctx); return; }
  state.groupSettings.makeFriendAdmin = true;
  await showGroupSummary(ctx);
});

bot.callbackQuery("group_friend_admin_no", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = await recoverGroupCreationState(ctx.from.id, "group_confirm_friend_admin");
  if (!state?.groupSettings) { await replyGroupSessionExpired(ctx); return; }
  state.groupSettings.makeFriendAdmin = false;
  await showGroupSummary(ctx);
});

bot.callbackQuery("naming_auto", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.groupSettings) return;
  state.groupSettings.namingMode = "auto";
  state.groupSettings.finalNames = generateGroupNames(state.groupSettings.name, state.groupSettings.count);
  state.step = "group_enter_description";
  const preview = state.groupSettings.finalNames.slice(0, 5).map((n, i) => `${i + 1}. ${esc(n)}`).join("\n");
  await ctx.editMessageText(
    `✅ <b>Names Preview:</b>\n${preview}${state.groupSettings.count > 5 ? `\n... +${state.groupSettings.count - 5} more` : ""}\n\n` +
    "📄 <b>Group Description</b>\n\nSend a description, or press the <b>⏭️ Skip</b> button to leave it blank:",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "group_skip_description").row().text("◀️ Back", "back_to_naming_mode").text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("naming_custom", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.groupSettings) return;
  state.groupSettings.namingMode = "custom";
  state.groupSettings.finalNames = [];
  state.step = "group_enter_custom_names";
  await ctx.editMessageText(
    `✏️ <b>Custom Names</b>\n\nSend all <b>${state.groupSettings.count}</b> names, one per line:\n\n<i>Example:\nSpidy Squad\nSpidy Gang\nSpidy Army</i>`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("◀️ Back", "back_to_naming_mode").text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("back_to_naming_mode", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.groupSettings) return;
  state.step = "group_naming_mode";
  state.groupSettings.namingMode = "auto";
  state.groupSettings.finalNames = [];
  await ctx.editMessageText(
    `🏷️ <b>Naming Mode</b>\n\nCreating <b>${state.groupSettings.count} groups</b>. How to name them?`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔢 Auto-numbered", "naming_auto").text("✏️ Custom Names", "naming_custom").row().text("❌ Cancel", "main_menu") }
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

  // Persist the state to MongoDB so the user can still create groups even
  // if the bot restarts between this screen and clicking "Create Now".
  // Photos (dpBuffers) are NOT persisted — they would need to be re-uploaded
  // if the session is restored from MongoDB. Valid for 20 minutes.
  void savePendingGroupCreation(userId, {
    name: gs.name,
    description: gs.description,
    count: gs.count,
    finalNames: gs.finalNames,
    namingMode: gs.namingMode,
    editGroupInfo: gs.editGroupInfo,
    sendMessages: gs.sendMessages,
    addMembers: gs.addMembers,
    approveJoin: gs.approveJoin,
    disappearingMessages: gs.disappearingMessages,
    friendNumbers: gs.friendNumbers,
    makeFriendAdmin: gs.makeFriendAdmin,
  }).catch(() => {});

  try { await ctx.editMessageText(text, { parse_mode: "HTML", reply_markup: markup }); }
  catch { await ctx.reply(text, { parse_mode: "HTML", reply_markup: markup }); }
}

bot.callbackQuery("group_create_start", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  // ── Guard: prevent double-start if creation is already running ────────────
  // If the user double-taps "Create Now" (or taps it while a previous
  // background loop is still running), a second concurrent loop would be
  // spawned. Both share the same userState cancel flag, so cancelling one
  // cancels both — but both loops would still finish and edit the message
  // twice, making it appear as though creation restarted after stopping.
  // This guard makes the second tap a no-op while the loop is in progress.
  const existingState = userStates.get(userId);
  if (existingState?.step === "group_creating") {
    await ctx.answerCallbackQuery({ text: "⏳ Group creation already in progress..." });
    return;
  }

  // ── Try RAM state first ───────────────────────────────────────────────────
  let gs: GroupSettings | null = null;
  const state = userStates.get(userId);
  if (state?.groupSettings) {
    gs = { ...state.groupSettings };
    state.step = "group_creating";
    state.groupCreationCancel = false;
  } else {
    // ── Fallback: restore from MongoDB (handles bot restarts) ──────────────
    const persisted = await loadPendingGroupCreation(userId);
    if (persisted) {
      gs = {
        ...persisted,
        dpBuffers: [], // Photos are not persisted — re-upload needed
      };
      // Re-create the state entry so cancel/progress logic works.
      userStates.set(userId, {
        step: "group_creating",
        groupSettings: gs,
        groupCreationCancel: false,
      });
    }
  }

  if (!gs || !gs.finalNames.length) {
    // State expired (>20 min) or never saved — tell the user clearly.
    await ctx.editMessageText(
      "⚠️ <b>Session Expired</b>\n\n" +
      "Your group creation session has expired (20 minutes limit).\n\n" +
      "Please start again by tapping <b>Create Groups</b> from the menu.",
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("👥 Create Groups", "create_groups")
          .text("🏠 Main Menu", "main_menu"),
      }
    );
    return;
  }

  await ctx.editMessageText(
    `⏳ <b>Creating ${gs.finalNames.length} group(s)...</b>\n\n🔄 0/${gs.finalNames.length} done...`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel Creation", "group_cancel_creation") }
  );

  void createGroupsBackground(String(userId), userId, gs, chatId, msgId);
});

bot.callbackQuery("group_cancel_creation", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery({ text: "🛑 Stopping creation..." });
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (state) {
    state.groupCreationCancel = true;
    state.groupCreationCancelPending = true;
  }
  // Delete the MongoDB pending state NOW so that if the user navigates back
  // and taps "Create Now" again it shows Session Expired — not a re-run.
  void deletePendingGroupCreation(userId).catch(() => {});
  // Show a brief stopping message — the background loop will overwrite this
  // with the full results (including links of created groups) once it detects
  // the cancel flag and finishes the current in-flight operation.
  await ctx.editMessageText(
    "⌛ <b>Stopping group creation...</b>\n\nPlease wait — links of already-created groups will appear here shortly.",
    { parse_mode: "HTML" }
  );
});

bot.callbackQuery("group_cancel_dismiss", async (ctx) => {
  ctx.answerCallbackQuery({ text: "▶️ Continuing..." });
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

// ─── Create Groups: Member-add pause – Continue / Stop ───────────────────────

bot.callbackQuery(/^cg_fr_cont:(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery({ text: "▶️ Continuing..." });
  const uid = Number(ctx.match![1]);
  const resolve = cgFriendsPause.get(uid);
  if (resolve) { cgFriendsPause.delete(uid); resolve("continue"); }
});

bot.callbackQuery(/^cg_fr_stop:(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery({ text: "⛔ Stopping..." });
  const uid = Number(ctx.match![1]);
  const resolve = cgFriendsPause.get(uid);
  if (resolve) { cgFriendsPause.delete(uid); resolve("stop"); }
});

async function createGroupsBackground(userId: string, numericUserId: number, gs: GroupSettings, chatId: number, msgId: number) {
  const perms: GroupPermissions = { editGroupInfo: gs.editGroupInfo, sendMessages: gs.sendMessages, addMembers: gs.addMembers, approveJoin: gs.approveJoin };
  const results: Array<{ name: string; link: string | null; error?: string; friendsAdded?: number; friendsFailed?: boolean; friendAdmin?: boolean; friendFailDetails?: string[] }> = [];
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
        // Check cancel immediately after group creation — if user cancelled
        // during the createWhatsAppGroup call, skip settings/DP/friends for
        // this group so the cancel takes effect as fast as possible.
        const cancelNow = userStates.get(numericUserId)?.groupCreationCancel;
        results.push({
          name: groupName,
          link: result.inviteCode,
          friendsAdded: undefined,
          friendsFailed: false,
        });
        if (cancelNow) {
          for (let j = i + 1; j < total; j++) {
            results.push({ name: gs.finalNames[j], link: null, error: "Cancelled by user" });
          }
          // Re-set i to total so the outer loop exits
          i = total - 1;
          break;
        }
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
        let finalFriendFailDetails: string[] = [];

        if (gs.friendNumbers.length > 0) {
          if (result.participantsFailed) {
            // Creation with participants failed — try adding separately as fallback
            await new Promise((r) => setTimeout(r, 3000));
            const addResults = await addGroupParticipantsBulk(userId, result.id, gs.friendNumbers);
            finalFriendsAdded = addResults.filter(r => r.success).length;
            finalFriendsFailed = finalFriendsAdded < gs.friendNumbers.length;
            if (finalFriendsFailed) {
              finalFriendFailDetails = addResults
                .filter(r => !r.success)
                .map(r => `${r.phone}: ${r.error || "Unknown reason"}`);
            }
          } else {
            finalFriendsAdded = result.addedParticipants ?? 0;
            if (finalFriendsAdded < gs.friendNumbers.length) {
              finalFriendsFailed = true;
              const skipped = gs.friendNumbers.length - finalFriendsAdded;
              finalFriendFailDetails = [`${skipped} member(s) filtered out — not on WhatsApp or invalid number`];
            }
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

          // ── Pause when members fail to add ──────────────────────────────────
          // Show the user exactly which members failed and why,
          // then ask whether to continue to the remaining groups or stop.
          if (finalFriendsFailed) {
            const remaining = total - (i + 1);
            const detailLines = finalFriendFailDetails.length > 0
              ? finalFriendFailDetails.map(d => `  • ${d}`).join("\n")
              : "  • WhatsApp rejected the participants";
            const pauseText =
              `⚠️ <b>Members Not Added — "${esc(groupName)}"</b>\n\n` +
              `❌ <b>${finalFriendsAdded}/${gs.friendNumbers.length} member(s) added</b>\n\n` +
              `<b>Reason(s):</b>\n${detailLines}\n\n` +
              (remaining > 0
                ? `<i>${remaining} group(s) still remaining. Continue or stop?</i>`
                : `<i>This was the last group.</i>`);
            try {
              const pauseMsg = await bot.api.sendMessage(chatId, pauseText, {
                parse_mode: "HTML",
                reply_markup: remaining > 0
                  ? new InlineKeyboard()
                    .text("▶️ Continue", `cg_fr_cont:${numericUserId}`)
                    .text("⛔ Stop", `cg_fr_stop:${numericUserId}`)
                  : new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
              });
              if (remaining > 0) {
                const decision = await new Promise<"continue" | "stop">((resolve) => {
                  cgFriendsPause.set(numericUserId, resolve);
                  // Auto-continue after 90 seconds if user doesn't respond
                  setTimeout(() => {
                    if (cgFriendsPause.has(numericUserId)) {
                      cgFriendsPause.delete(numericUserId);
                      resolve("continue");
                    }
                  }, 90_000);
                });
                try { await bot.api.deleteMessage(chatId, pauseMsg.message_id); } catch {}
                if (decision === "stop") {
                  for (let j = i + 1; j < total; j++) {
                    results.push({ name: gs.finalNames[j], link: null, error: "Stopped by user" });
                  }
                  // Update result before breaking
                  const existingIdx2 = results.findIndex(r => r.name === groupName && r.link === result.inviteCode);
                  if (existingIdx2 >= 0) {
                    results[existingIdx2].friendsAdded = finalFriendsAdded;
                    results[existingIdx2].friendsFailed = true;
                    results[existingIdx2].friendFailDetails = finalFriendFailDetails;
                    results[existingIdx2].friendAdmin = gs.makeFriendAdmin && finalFriendsAdded > 0;
                  }
                  i = total; // exit outer loop
                  break;
                }
              }
            } catch { /* non-fatal — loop continues */ }
          }
        }

        // Update the result entry with final friends data
        const existingIdx = results.findIndex(r => r.name === groupName && r.link === result.inviteCode);
        if (existingIdx >= 0) {
          results[existingIdx].friendsAdded = gs.friendNumbers.length > 0 ? finalFriendsAdded : undefined;
          results[existingIdx].friendsFailed = finalFriendsFailed;
          results[existingIdx].friendAdmin = gs.makeFriendAdmin && finalFriendsAdded > 0;
          results[existingIdx].friendFailDetails = finalFriendsFailed ? finalFriendFailDetails : undefined;
        }
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
  schedulePostFeatureGC();

  // Clean up the MongoDB-persisted pending state — creation is complete.
  void deletePendingGroupCreation(numericUserId).catch(() => {});

  const cancelled = results.some((r) => r.error === "Cancelled by user" || r.error === "Stopped by user");
  const created = results.filter((r) => r.link).length;
  let message = cancelled
    ? `🛑 <b>Stopped! (${created}/${total} created)</b>\n\n`
    : `🎉 <b>Done! (${created}/${total} created)</b>\n\n`;
  for (const r of results) {
    if (r.error === "Cancelled by user" || r.error === "Stopped by user") {
      const icon = r.error === "Stopped by user" ? "⛔" : "🛑";
      message += `${icon} <b>${esc(r.name)}</b>\n⚠️ ${r.error}\n\n`;
    } else if (r.link) {
      let line = `✅ <b>${esc(r.name)}</b>\n🔗 ${r.link}`;
      if (r.friendsAdded !== undefined) {
        if (r.friendsFailed) {
          line += `\n👫 Members: ${r.friendsAdded}/${gs.friendNumbers.length} added`;
          if (r.friendFailDetails && r.friendFailDetails.length > 0) {
            const show = r.friendFailDetails.slice(0, 3);
            line += `\n⚠️ Failed:\n` + show.map(d => `  • ${esc(d)}`).join("\n");
            if (r.friendFailDetails.length > 3) {
              line += `\n  • …and ${r.friendFailDetails.length - 3} more`;
            }
          } else {
            line += ` (rejected by WhatsApp)`;
          }
        } else if (r.friendsAdded > 0) {
          line += `\n👫 Members: ${r.friendsAdded} added ✅`;
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  cancelDialogActiveFor.add(ctx.from.id);
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard()
      .text("✅ Yes, Stop Joining", "join_cancel_confirm")
      .text("↩️ Continue", "join_cancel_no"),
  });
});

bot.callbackQuery("join_cancel_no", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Joining continued" });
  const userId = ctx.from.id;
  cancelDialogActiveFor.delete(userId);
  if (joinCancelRequests.has(userId)) return;
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard().text("❌ Cancel", "join_cancel_request"),
  });
});

bot.callbackQuery("join_cancel_confirm", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Stopping after current group..." });
  joinCancelRequests.add(ctx.from.id);
  // Keep the dialog flag on so the in-flight progress edit doesn't pop
  // the "❌ Cancel" button back. The background task clears the flag in
  // its finally cleanup.
  await ctx.editMessageReplyMarkup({ reply_markup: undefined });
});

bot.callbackQuery("join_failed_download", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Preparing file..." });
  const userId = ctx.from.id;
  const cached = joinFailedLinksCache.get(userId);
  if (!cached || Date.now() > cached.expiresAt) {
    try {
      await ctx.reply("⚠️ Download expired. Please run Join Groups again.", {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
      });
    } catch {}
    return;
  }
  try {
    const fileContent = Buffer.from(cached.text, "utf-8");
    const fileName = `failed_join_links_${new Date().toISOString().slice(0, 10)}.txt`;
    await bot.api.sendDocument(
      ctx.chat.id,
      new InputFile(fileContent, fileName),
      {
        caption: `📥 <b>Failed Join Links</b>\n${cached.text.split("\n").filter(l => l.startsWith("https://")).length} link(s) that could not be joined`,
        parse_mode: "HTML",
      }
    );
    joinFailedLinksCache.delete(userId);
  } catch (err: any) {
    await ctx.reply(`❌ Failed to send file: ${esc(err?.message || "Unknown error")}`, { parse_mode: "HTML" });
  }
});

// ─── CTC Checker ─────────────────────────────────────────────────────────────

bot.callbackQuery("ctc_checker", async (ctx) => {
  ctx.answerCallbackQuery().catch(() => {})

  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText(
      "❌ <b>WhatsApp not connected!</b>\n\nPlease connect WhatsApp first to use CTC Checker.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect WhatsApp", "connect_wa").row().text("🏠 Main Menu", "main_menu") }
    ); return;
  }

  await ctx.editMessageText(
    "🔍 <b>CTC Checker</b>\n\nHow would you like to select groups?",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🔗 By Link", "ctc_by_link").row()
        .text("📋 Select Groups", "ctc_by_groups").row()
        .text("🏠 Main Menu", "main_menu"),
    }
  );
});

// ── By Link (existing flow) ───────────────────────────────────────────────────
bot.callbackQuery("ctc_by_link", async (ctx) => {
  ctx.answerCallbackQuery().catch(() => {})
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }); return;
  }

  const ctcInitState: UserState = {
    step: "ctc_enter_links",
    ctcData: { groupLinks: [], pairs: [], currentPairIndex: 0 },
  };
  userStates.set(userId, ctcInitState);
  saveUserState(userId, ctcInitState).catch(() => {});

  const ctcPrompt = notr(
    "🔍 <b>CTC Checker — By Link</b>\n\n" +
    "Send all WhatsApp group links, one per line:\n\n" +
    "<code>https://chat.whatsapp.com/ABC123\nhttps://chat.whatsapp.com/XYZ456</code>"
  );
  try {
    await ctx.editMessageText(ctcPrompt, { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") });
  } catch {
    try { await ctx.reply(ctcPrompt, { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }); } catch {}
  }
});

// ── By Groups (new flow) ──────────────────────────────────────────────────────
bot.callbackQuery("ctc_by_groups", async (ctx) => {
  ctx.answerCallbackQuery().catch(() => {})
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }); return;
  }

  await ctx.editMessageText("🔍 <b>Scanning your WhatsApp groups...</b>\n\n⌛ Please wait...", { parse_mode: "HTML" });

  const groups = await getAllGroups(String(userId));
  if (!groups.length) {
    await ctx.editMessageText("📭 No groups found on your WhatsApp.", { reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }); return;
  }

  const allGroupsSimple = groups
    .map((g) => ({ id: g.id, subject: g.subject }))
    .sort((a, b) => a.subject.localeCompare(b.subject, undefined, { numeric: true, sensitivity: "base" }));
  const patterns = detectSimilarGroups(allGroupsSimple);

  userStates.set(userId, {
    step: "ctcg_select",
    ctcData: { groupLinks: [], pairs: [], currentPairIndex: 0 },
    ctcgData: { allGroups: allGroupsSimple, patterns, selectedIndices: new Set(), page: 0 },
  });

  const kb = new InlineKeyboard();
  if (patterns.length > 0) kb.text("🔍 Similar Groups", "ctcg_similar").text("📋 All Groups", "ctcg_show_all").row();
  else kb.text("📋 All Groups", "ctcg_show_all").row();
  kb.text("🏠 Main Menu", "main_menu");

  await ctx.editMessageText(
    `🔍 <b>CTC Checker — Select Groups</b>\n\n` +
    `📱 <b>Groups Found: ${allGroupsSimple.length}</b>\n` +
    (patterns.length > 0 ? `🔍 <b>Similar Patterns: ${patterns.length}</b>\n` : "") +
    `\nSelect which groups to check:`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

function buildCtcgKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const allGroups = state.ctcgData!.allGroups;
  const selected = state.ctcgData!.selectedIndices;
  const page = state.ctcgData!.page || 0;
  const totalPages = Math.max(1, Math.ceil(allGroups.length / MA_PAGE_SIZE));
  const start = page * MA_PAGE_SIZE;
  const end = Math.min(start + MA_PAGE_SIZE, allGroups.length);

  for (let i = start; i < end; i++) {
    const g = allGroups[i];
    const label = selected.has(i) ? `✅ ${g.subject}` : `☐ ${g.subject}`;
    kb.text(label, `ctcg_tog_${i}`).row();
  }

  const prev = page > 0 ? "⬅️ Prev" : " ";
  const next = page < totalPages - 1 ? "Next ➡️" : " ";
  kb.text(prev, "ctcg_prev_page").text(`📄 ${page + 1}/${totalPages}`, "ctcg_page_info").text(next, "ctcg_next_page").row();

  if (allGroups.length > 1) {
    kb.text("☑️ Select All", "ctcg_select_all").text("🧹 Clear All", "ctcg_clear_all").row();
  }
  if (selected.size > 0) {
    kb.text(`▶️ Continue (${selected.size} selected)`, "ctcg_proceed").row();
  }
  kb.text("🔙 Back", "ctc_checker").text("🏠 Menu", "main_menu");
  return kb;
}

bot.callbackQuery("ctcg_similar", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.ctcgData) return;
  const { patterns } = state.ctcgData;
  if (!patterns.length) { ctx.answerCallbackQuery({ text: "No similar patterns found.", show_alert: true }); return; }
  const kb = new InlineKeyboard();
  for (let i = 0; i < patterns.length; i++) {
    kb.text(`📌 ${patterns[i].base} (${patterns[i].groups.length})`, `ctcg_sim_${i}`).row();
  }
  kb.text("🔙 Back", "ctc_by_groups").text("🏠 Menu", "main_menu");
  await ctx.editMessageText("🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select those groups:", { parse_mode: "HTML", reply_markup: kb });
});

bot.callbackQuery(/^ctcg_sim_(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.ctcgData) return;
  const idx = parseInt(ctx.match![1]);
  const pattern = state.ctcgData.patterns[idx];
  if (!pattern) return;
  const patternIds = new Set(pattern.groups.map((g) => g.id));
  state.ctcgData.selectedIndices = new Set();
  for (let i = 0; i < state.ctcgData.allGroups.length; i++) {
    if (patternIds.has(state.ctcgData.allGroups[i].id)) state.ctcgData.selectedIndices.add(i);
  }
  state.ctcgData.page = 0;
  await ctx.editMessageText(
    `🔍 <b>CTC Checker — Select Groups</b>\n\n<i>${state.ctcgData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildCtcgKeyboard(state) }
  );
});

bot.callbackQuery("ctcg_show_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.ctcgData) return;
  state.ctcgData.page = 0;
  await ctx.editMessageText(
    `🔍 <b>CTC Checker — Select Groups</b>\n\n<i>Tap to select/deselect</i>`,
    { parse_mode: "HTML", reply_markup: buildCtcgKeyboard(state) }
  );
});

bot.callbackQuery(/^ctcg_tog_(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.ctcgData) return;
  const idx = parseInt(ctx.match![1]);
  if (idx < 0 || idx >= state.ctcgData.allGroups.length) return;
  if (state.ctcgData.selectedIndices.has(idx)) state.ctcgData.selectedIndices.delete(idx);
  else state.ctcgData.selectedIndices.add(idx);
  const cnt = state.ctcgData.selectedIndices.size;
  await ctx.editMessageText(
    `🔍 <b>CTC Checker — Select Groups</b>\n\n<i>${cnt > 0 ? `${cnt} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildCtcgKeyboard(state) }
  );
});

bot.callbackQuery("ctcg_prev_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.ctcgData) return;
  if ((state.ctcgData.page || 0) > 0) state.ctcgData.page--;
  const cnt = state.ctcgData.selectedIndices.size;
  await ctx.editMessageText(
    `🔍 <b>CTC Checker — Select Groups</b>\n\n<i>${cnt > 0 ? `${cnt} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildCtcgKeyboard(state) }
  );
});

bot.callbackQuery("ctcg_next_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.ctcgData) return;
  const totalPages = Math.ceil(state.ctcgData.allGroups.length / MA_PAGE_SIZE);
  if ((state.ctcgData.page || 0) < totalPages - 1) state.ctcgData.page++;
  const cnt = state.ctcgData.selectedIndices.size;
  await ctx.editMessageText(
    `🔍 <b>CTC Checker — Select Groups</b>\n\n<i>${cnt > 0 ? `${cnt} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildCtcgKeyboard(state) }
  );
});

bot.callbackQuery("ctcg_page_info", async (ctx) => { ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" }); });

bot.callbackQuery("ctcg_select_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.ctcgData) return;
  for (let i = 0; i < state.ctcgData.allGroups.length; i++) state.ctcgData.selectedIndices.add(i);
  await ctx.editMessageText(
    `🔍 <b>CTC Checker — Select Groups</b>\n\nAll <b>${state.ctcgData.allGroups.length} groups selected</b>`,
    { parse_mode: "HTML", reply_markup: buildCtcgKeyboard(state) }
  );
});

bot.callbackQuery("ctcg_clear_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.ctcgData) return;
  state.ctcgData.selectedIndices.clear();
  await ctx.editMessageText(
    `🔍 <b>CTC Checker — Select Groups</b>\n\n<i>None selected yet</i>`,
    { parse_mode: "HTML", reply_markup: buildCtcgKeyboard(state) }
  );
});

bot.callbackQuery("ctcg_proceed", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.ctcgData || !state.ctcgData.selectedIndices.size) return;

  const selectedGroups = Array.from(state.ctcgData.selectedIndices).map(i => state.ctcgData!.allGroups[i]);

  // Build pairs from selected groups — use groupId directly (no link resolution needed)
  const pairs: CtcPair[] = selectedGroups.map(g => ({
    link: g.subject,
    groupId: g.id,
    groupName: g.subject,
    vcfContacts: [],
  }));

  // Free the large allGroups array immediately — it can hold hundreds of
  // group objects and is no longer needed once we've built the pairs list.
  state.ctcgData = undefined;

  if (!state.ctcData) state.ctcData = { groupLinks: [], pairs: [], currentPairIndex: 0 };
  state.ctcData.pairs = pairs;
  state.ctcData.currentPairIndex = 0;
  state.step = "ctc_enter_vcf";
  saveUserState(userId, state).catch(() => {});

  const firstGroup = selectedGroups[0];
  await ctx.editMessageText(
    `✅ <b>${selectedGroups.length} group(s) selected!</b>\n\n` +
    `📁 <b>Send VCF or TXT file(s)</b>\n\n` +
    `You can send:\n• One file for all groups\n• Multiple files, one per group (in order)\n\n` +
    `Send file for <b>Group 1/${selectedGroups.length}</b>:\n<b>${esc(firstGroup.subject)}</b>\n\n` +
    `When ready, tap <b>Start Check</b>:`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("▶️ Start Check", "ctc_start_check").text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("ctc_start_check", async (ctx) => {
  try { ctx.answerCallbackQuery(); } catch (e: any) {
    console.error("[CTC-START] answerCallbackQuery failed:", e?.message ?? e);
  }
  const userId = ctx.from.id;
  console.error(`[CTC-DEBUG] ctc_start_check reached for userId=${userId}`);

  let state = userStates.get(userId);
  if (!state?.ctcData) {
    console.error(`[CTC-START] No in-memory state for userId=${userId}, trying MongoDB...`);
    const remote = await loadUserState(userId);
    if (remote) { state = remote as UserState; userStates.set(userId, state); console.error(`[CTC-START] Loaded state from MongoDB: step=${state.step}`); }
  }
  if (!state?.ctcData) {
    console.error(`[CTC-START] No state for userId=${userId}`);
    try {
      await ctx.reply("⚠️ Session expired. Please start CTC Checker again.", {
        reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
      });
    } catch {}
    return;
  }

  // Block if VCF files are still being processed in the queue
  if (vcfProcessingQueue.has(userId)) {
    const totalPairs = state.ctcData.pairs.length;
    const filledSoFar = state.ctcData.pairs.filter(p => p.vcfContacts.length > 0).length;
    try {
      ctx.answerCallbackQuery({
        text: `⏳ VCF files are still being processed (${filledSoFar}/${totalPairs} done). Please wait for all files to finish, then press Start Check.`,
        show_alert: true,
      });
    } catch {}
    return;
  }

  // If user sent 1 VCF for multiple groups, replicate it to all empty pairs
  const filledPairs = state.ctcData.pairs.filter(p => p.vcfContacts.length > 0);
  if (!filledPairs.length) {
    try {
      await ctx.editMessageText("⚠️ No VCF files provided. Please send VCF files first.");
    } catch {
      try { await ctx.reply("⚠️ No VCF files provided. Please send VCF files first."); } catch {}
    }
    return;
  }
  // Replicate first available VCF to all empty pairs so 1 merged VCF covers all groups
  const referencePair = filledPairs[0];
  for (const pair of state.ctcData.pairs) {
    if (pair.vcfContacts.length === 0) {
      pair.vcfContacts = referencePair.vcfContacts.map(c => ({ ...c }));
    }
  }
  const activePairs = state.ctcData.pairs;

  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) {
    console.error(`[CTC-START] No chatId/msgId for userId=${userId}`);
    try { await ctx.reply("❌ Could not start check. Please try again."); } catch {}
    return;
  }

  userStates.delete(userId);

  try {
    await ctx.editMessageText(
      notr(`⏳ <b>Checking ${activePairs.length} group(s)...</b>\n\n⌛ Please wait...`),
      { parse_mode: "HTML" }
    );
  } catch {
    try {
      await bot.api.sendMessage(
        chatId,
        notr(`⏳ <b>Checking ${activePairs.length} group(s)...</b>\n\n⌛ Please wait...`),
        { parse_mode: "HTML" }
      );
    } catch {}
  }

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

// Build a Unicode progress bar: e.g. buildProgressBar(3, 8, 12) → "████░░░░░░░░ 37%"
function buildProgressBar(done: number, total: number, width = 12): string {
  const pct = total === 0 ? 100 : Math.round((done / total) * 100);
  const filled = total === 0 ? width : Math.round((done / total) * width);
  const bar = "█".repeat(filled) + "░".repeat(width - filled);
  return `${bar} ${pct}%`;
}

// Truncate a string to maxLen chars, appending "…" if trimmed.
function truncate(s: string, maxLen: number): string {
  return s.length <= maxLen ? s : s.slice(0, maxLen - 1) + "…";
}

async function ctcCheckBackground(userId: string, activePairs: CtcPair[], chatId: number, msgId: number) {
  try {
    await _ctcCheckBackgroundImpl(userId, activePairs, chatId, msgId);
  } catch (err: any) {
    console.error("[CTC] Unexpected crash in ctcCheckBackground:", err?.message ?? err);
    try {
      await bot.api.editMessageText(chatId, msgId,
        `❌ <b>CTC Check failed</b>\n\n<i>${esc(err?.message || "Unknown error")}</i>\n\nPlease try again.`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {
      try {
        await bot.api.sendMessage(chatId,
          "❌ CTC Check failed due to an unexpected error. Please try again.",
          { reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
        );
      } catch {}
    }
  }
}

async function _ctcCheckBackgroundImpl(userId: string, activePairs: CtcPair[], chatId: number, msgId: number) {
  // Collect all VCF phone numbers across all pairs for duplicate detection
  // Map: phone number → list of group names it appears as pending
  const pendingPhoneToGroups = new Map<string, string[]>();

  // Running totals shown in the live progress message
  let runningCorrect = 0;
  let runningWrong = 0;
  let runningFailed = 0;

  // Helper: build the live progress message shown while checking groups.
  // Shows a progress bar, current group info, and a running tally.
  const buildProgressMsg = (
    i: number,               // 0-based index of group currently being processed
    phase: string,           // short status string, e.g. "Resolving link…"
    groupLabel: string,      // group name or fallback label
    vcfCount: number,        // number of VCF contacts for this group
  ): string => {
    const total = activePairs.length;
    const bar = buildProgressBar(i, total);
    const lines: string[] = [];
    lines.push(`🔍 <b>CTC Check in progress…</b>`);
    lines.push(`<code>${bar}</code>`);
    lines.push(`📋 Group <b>${i + 1}/${total}</b>${total > 1 ? ` — ${esc(truncate(groupLabel, 28))}` : ""}`);
    lines.push(`📁 <b>${vcfCount}</b> contact${vcfCount === 1 ? "" : "s"} in VCF`);
    lines.push(`⚙️ <i>${esc(phase)}</i>`);
    if (i > 0) {
      lines.push(`━━━━━━━━━━━━━━━━━━`);
      lines.push(`✅ Correct so far: <b>${runningCorrect}</b>   ⚠️ Wrong: <b>${runningWrong}</b>${runningFailed ? `   ❌ Failed: <b>${runningFailed}</b>` : ""}`);
    }
    return lines.join("\n");
  };

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
    const vcfCount = pair.vcfContacts.length;
    const groupLabel = pair.groupName || `Group ${i + 1}`;

    // ── Phase 1: resolving the group link (or use pre-resolved groupId) ──
    try {
      await bot.api.editMessageText(chatId, msgId,
        buildProgressMsg(i, pair.groupId ? "Loading group info…" : "Resolving group link…", groupLabel, vcfCount),
        { parse_mode: "HTML" }
      );
    } catch {}

    let groupInfo: { id: string; subject: string } | null = null;

    // If pair already has a groupId (group-select mode), skip link resolution
    if (pair.groupId) {
      groupInfo = { id: pair.groupId, subject: pair.groupName || groupLabel };
    } else {
      try {
        // 30 s hard cap — getGroupIdFromLink retries up to 15× with long backoffs
        // and can block for 30+ minutes on a rate-limited link without this guard.
        groupInfo = await Promise.race([
          getGroupIdFromLink(userId, cleanLink),
          new Promise<null>(r => setTimeout(() => r(null), 30_000)),
        ]);
      } catch (err: any) {
        runningFailed++;
        groupResults.push({
          groupId: "",
          groupName: groupLabel,
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
        console.error("[CTC] getGroupIdFromLink failed:", err?.message ?? err);
        continue;
      }
      if (!groupInfo) {
        runningFailed++;
        groupResults.push({
          groupId: "",
          groupName: groupLabel,
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
    }

    // ── Phase 2: fetching members + pending ────────────────────────────
    try {
      await bot.api.editMessageText(chatId, msgId,
        buildProgressMsg(i, "Fetching members & pending list…", groupInfo.subject, vcfCount),
        { parse_mode: "HTML" }
      );
    } catch {}

    const phones = pair.vcfContacts.map((c) => c.phone);
    let checkResult;
    try {
      // 60 s hard cap — groupMetadata + pending list for large groups can stall
      // the socket indefinitely; sentinel null means we timed out.
      const membersRace = await Promise.race([
        checkContactsInGroup(userId, groupInfo.id, phones),
        new Promise<null>(r => setTimeout(() => r(null), 60_000)),
      ]);
      if (!membersRace) throw new Error("Timeout fetching members / pending list");
      checkResult = membersRace;
    } catch (err: any) {
      runningFailed++;
      groupResults.push({
        groupId: groupInfo.id,
        groupName: groupInfo.subject,
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
      console.error("[CTC] checkContactsInGroup failed:", err?.message ?? err);
      continue;
    }
    const { inMembers, inPending, notFound: notFoundPhones, pendingAvailable, allMemberPhones, allPendingPhones } = checkResult;

    // Track ALL pending phones for duplicate detection (not just VCF matches)
    for (const phone of allPendingPhones) {
      if (!pendingPhoneToGroups.has(phone)) pendingPhoneToGroups.set(phone, []);
      pendingPhoneToGroups.get(phone)!.push(groupInfo.subject);
    }

    // Update running totals so the next group's progress bar shows them
    const inPendingSet = new Set(inPending.map(p => p.replace(/[^0-9]/g, "")));
    const vcfLast10Set = new Set(pair.vcfContacts.map(c => c.phone.replace(/[^0-9]/g, "").slice(-10)));
    const correctPending = pair.vcfContacts.filter(c => inPendingSet.has(c.phone.replace(/[^0-9]/g, ""))).length;
    let wrongPending = 0;
    for (const p of allPendingPhones) {
      if (!vcfLast10Set.has(p.slice(-10))) wrongPending++;
    }
    runningCorrect += correctPending;
    runningWrong += wrongPending;

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

  // Show "finalising…" bar at 100% while we build the result message
  try {
    const total = activePairs.length;
    await bot.api.editMessageText(chatId, msgId,
      `🔍 <b>CTC Check in progress…</b>\n<code>${buildProgressBar(total, total)}</code>\n⚙️ <i>Finalising results…</i>`,
      { parse_mode: "HTML" }
    );
  } catch {}

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
        notInVcfCount: 0,
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

    const notInVcfCount = gr.allMemberPhones.size > 0
      ? Array.from(gr.allMemberPhones).filter(p => !new Set(gr.vcfContacts.map(c => c.phone.replace(/[^0-9]/g, "").slice(-10))).has(p.slice(-10))).length
      : 0;
    summaries.push({
      gr,
      correctPendingCount,
      correctMembersCount,
      notInVcfCount,
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
    // ── Group-centric view: which groups have how many duplicate pending ──
    const groupDupCount = new Map<string, number>();
    for (const d of duplicates) {
      for (const g of d.groups) {
        groupDupCount.set(g, (groupDupCount.get(g) ?? 0) + 1);
      }
    }
    const affectedGroups = [...groupDupCount.entries()].sort((a, b) => b[1] - a[1]);

    result += `🔁 <b>Duplicate Pending — ${duplicates.length} contact(s) in multiple groups</b>\n`;
    result += `━━━━━━━━━━━━━━━━━━\n`;
    result += `📋 <b>Affected Groups (${affectedGroups.length}):</b>\n`;
    for (const [gName, cnt] of affectedGroups) {
      result += `   • ${esc(gName)} — <b>${cnt}</b> duplicate${cnt > 1 ? "s" : ""}\n`;
    }
    result += `\n`;

    // ── Per-contact view: which groups each duplicate appears in ──
    result += `👥 <b>Duplicate Contacts:</b>\n`;
    const SHOW = 15; // show more contacts than before
    const slice = duplicates.slice(0, SHOW);
    for (const d of slice) {
      // Show ALL group names — no artificial cap; user needs to see every group
      result += `   • ${esc(d.phone)}\n`;
      for (const g of d.groups) result += `      ↳ ${esc(g)}\n`;
    }
    if (duplicates.length > SHOW) result += `   … +${duplicates.length - SHOW} more contacts\n`;
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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

function levenshtein(a: string, b: string): number {
  const m = a.length, n = b.length;
  if (m === 0) return n;
  if (n === 0) return m;
  const dp: number[] = Array.from({ length: n + 1 }, (_, i) => i);
  for (let i = 1; i <= m; i++) {
    let prev = dp[0];
    dp[0] = i;
    for (let j = 1; j <= n; j++) {
      const temp = dp[j];
      dp[j] = a[i - 1] === b[j - 1] ? prev : 1 + Math.min(prev, dp[j], dp[j - 1]);
      prev = temp;
    }
  }
  return dp[n];
}

function detectSimilarGroups(groups: Array<{ id: string; subject: string }>): SimilarGroup[] {
  const results: SimilarGroup[] = [];
  const usedIds = new Set<string>();

  // Phase 1: trailing-number clusters (e.g. "Spidy 1", "Spidy 2")
  const numberMap = new Map<string, Array<{ id: string; subject: string }>>();
  for (const g of groups) {
    const name = g.subject.trim();
    const match = name.match(/^(.*?)\s*\d+\s*$/);
    if (match && match[1].trim().length > 0) {
      const base = match[1].trim().toLowerCase();
      if (!numberMap.has(base)) numberMap.set(base, []);
      numberMap.get(base)!.push(g);
    }
  }
  for (const [, items] of numberMap) {
    if (items.length >= 2) {
      const sorted = items.sort((a, b) => a.subject.localeCompare(b.subject, undefined, { numeric: true, sensitivity: "base" }));
      results.push({ base: sorted[0].subject.replace(/\s*\d+\s*$/, "").trim(), groups: sorted });
      for (const g of items) usedIds.add(g.id);
    }
  }

  // Phase 2: fuzzy matching for remaining groups (catches typos like "spidy"/"spdiy"/"Spidy")
  const remaining = groups.filter(g => !usedIds.has(g.id));
  const normalize = (s: string) => s.toLowerCase().replace(/[^a-z0-9]/g, "");
  const assigned = new Set<number>();
  for (let i = 0; i < remaining.length; i++) {
    if (assigned.has(i)) continue;
    const na = normalize(remaining[i].subject);
    if (na.length < 3) continue;
    const cluster: number[] = [i];
    for (let j = i + 1; j < remaining.length; j++) {
      if (assigned.has(j)) continue;
      const nb = normalize(remaining[j].subject);
      if (nb.length < 3) continue;
      const maxLen = Math.max(na.length, nb.length);
      const dist = levenshtein(na, nb);
      if ((maxLen - dist) / maxLen >= 0.75) cluster.push(j);
    }
    if (cluster.length >= 2) {
      const items = cluster.map(i => remaining[i]).sort((a, b) => a.subject.localeCompare(b.subject, undefined, { numeric: true, sensitivity: "base" }));
      results.push({ base: items[0].subject.trim(), groups: items });
      for (const idx of cluster) assigned.add(idx);
    }
  }

  return results.sort((a, b) => a.base.localeCompare(b.base));
}

bot.callbackQuery("get_link", async (ctx) => {
  ctx.answerCallbackQuery();
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

const GL_SEL_PAGE_SIZE = 8;

function buildGlKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const d = state.glData!;
  const totalPages = Math.max(1, Math.ceil(d.groupsPool.length / GL_SEL_PAGE_SIZE));
  const start = d.page * GL_SEL_PAGE_SIZE;
  const end = Math.min(start + GL_SEL_PAGE_SIZE, d.groupsPool.length);
  for (let i = start; i < end; i++) {
    const g = d.groupsPool[i];
    const label = d.selectedIndices.has(i) ? `✅ ${g.subject}` : `☐ ${g.subject}`;
    kb.text(label, `gl_tog_${i}`).row();
  }
  const prev = d.page > 0 ? "⬅️ Prev" : " ";
  const next = d.page < totalPages - 1 ? "Next ➡️" : " ";
  kb.text(prev, "gl_prev_page").text(`📄 ${d.page + 1}/${totalPages}`, "gl_page_info").text(next, "gl_next_page").row();
  kb.text("☑️ Select All", "gl_select_all").text("🧹 Clear All", "gl_clear_all").row();
  if (d.selectedIndices.size > 0) kb.text(`🔗 Get Links (${d.selectedIndices.size} selected)`, "gl_proceed").row();
  const backTarget = d.mode === "similar" ? "gl_similar" : "get_link";
  kb.text("🔙 Back", backTarget).text("🏠 Menu", "main_menu");
  return kb;
}

bot.callbackQuery("gl_similar", async (ctx) => {
  ctx.answerCallbackQuery();
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
    kb.text(`📌 ${p.base} (${p.groups.length})`, `gl_sim_${i}`).row();
  }
  kb.text("🔙 Back", "get_link").text("🏠 Menu", "main_menu");

  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select its groups:",
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery(/^gl_sim_(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.similarData) return;

  const idx = parseInt(ctx.match![1]);
  const pattern = state.similarData.patterns[idx];
  if (!pattern) return;

  const patternIds = new Set(pattern.groups.map(g => g.id));
  const pool = state.similarData.allGroups.filter(g => patternIds.has(g.id));
  const preSelected = new Set(pool.map((_, i) => i));
  state.glData = {
    groupsPool: pool,
    selectedIndices: preSelected,
    page: 0,
    mode: "similar",
    patternBase: pattern.base,
    patterns: state.similarData.patterns,
    allGroups: state.similarData.allGroups,
  };

  await ctx.editMessageText(
    `🔍 <b>Similar Groups — "${esc(pattern.base)}"</b>\n\n` +
    `<b>${pool.length} group(s)</b> — select which to get links for:\n<i>${preSelected.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildGlKeyboard(state) }
  );
});

bot.callbackQuery("gl_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.similarData) return;

  const { allGroups, patterns } = state.similarData;
  state.glData = {
    groupsPool: allGroups,
    selectedIndices: new Set(),
    page: 0,
    mode: "all",
    patterns,
    allGroups,
  };

  await ctx.editMessageText(
    `📋 <b>All Admin Groups — Select for Link Fetch</b>\n\n` +
    `<b>${allGroups.length} group(s)</b>\n<i>None selected yet</i>`,
    { parse_mode: "HTML", reply_markup: buildGlKeyboard(state) }
  );
});

bot.callbackQuery(/^gl_tog_(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.glData) return;
  const idx = parseInt(ctx.match![1]);
  if (idx < 0 || idx >= state.glData.groupsPool.length) return;
  if (state.glData.selectedIndices.has(idx)) state.glData.selectedIndices.delete(idx);
  else state.glData.selectedIndices.add(idx);
  const label = state.glData.mode === "similar"
    ? `Similar Groups — "${esc(state.glData.patternBase || "")}"`
    : "All Admin Groups";
  await ctx.editMessageText(
    `🔍 <b>${label}</b>\n\n<i>${state.glData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildGlKeyboard(state) }
  );
});

bot.callbackQuery("gl_prev_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.glData) return;
  if (state.glData.page > 0) state.glData.page--;
  await ctx.editMessageText(
    `🔍 <b>Select Groups</b>\n\n<i>${state.glData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildGlKeyboard(state) }
  );
});

bot.callbackQuery("gl_next_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.glData) return;
  const totalPages = Math.ceil(state.glData.groupsPool.length / GL_SEL_PAGE_SIZE);
  if (state.glData.page < totalPages - 1) state.glData.page++;
  await ctx.editMessageText(
    `🔍 <b>Select Groups</b>\n\n<i>${state.glData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildGlKeyboard(state) }
  );
});

bot.callbackQuery("gl_page_info", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Use Prev / Next to change page" });
});

bot.callbackQuery("gl_select_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.glData) return;
  for (let i = 0; i < state.glData.groupsPool.length; i++) state.glData.selectedIndices.add(i);
  await ctx.editMessageText(
    `🔍 <b>Select Groups</b>\n\n✅ All <b>${state.glData.groupsPool.length}</b> groups selected`,
    { parse_mode: "HTML", reply_markup: buildGlKeyboard(state) }
  );
});

bot.callbackQuery("gl_clear_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.glData) return;
  state.glData.selectedIndices.clear();
  await ctx.editMessageText(
    `🔍 <b>Select Groups</b>\n\n<i>None selected</i>`,
    { parse_mode: "HTML", reply_markup: buildGlKeyboard(state) }
  );
});

bot.callbackQuery("gl_proceed", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.glData || state.glData.selectedIndices.size === 0) return;

  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  const selectedGroups = Array.from(state.glData.selectedIndices)
    .sort((a, b) => a - b)
    .map(i => state.glData!.groupsPool[i]);

  const isSimMode = state.glData.mode === "similar";
  const progressText = isSimMode
    ? `⏳ <b>Fetching links for "${esc(state.glData.patternBase || "")}" groups...</b>\n\n📊 0/${selectedGroups.length} fetched...`
    : `⏳ <b>Fetching ${selectedGroups.length} group links...</b>\n\n📊 0/${selectedGroups.length} fetched...`;

  await ctx.editMessageText(progressText, {
    parse_mode: "HTML",
    reply_markup: new InlineKeyboard().text("❌ Cancel", "gl_cancel_request"),
  });

  getLinkCancelRequests.delete(userId);
  void fetchGroupLinksBackground(
    String(userId),
    selectedGroups,
    chatId,
    msgId,
    isSimMode ? "similar" : "all",
    state.glData.patternBase
  );
});

bot.callbackQuery("gl_cancel_request", async (ctx) => {
  cancelDialogActiveFor.add(ctx.from.id);
  ctx.answerCallbackQuery();
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard()
      .text("✅ Yes, Stop Fetch", "gl_cancel_confirm")
      .text("↩️ Continue", "gl_cancel_no"),
  });
});

bot.callbackQuery("gl_cancel_no", async (ctx) => {
  cancelDialogActiveFor.delete(ctx.from.id);
  ctx.answerCallbackQuery({ text: "Fetching continued" });
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard().text("❌ Cancel", "gl_cancel_request"),
  });
});

bot.callbackQuery("gl_cancel_confirm", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Stopping after current group..." });
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
    `13. Reset Link — Reset group invite links (Select Groups or by Group Link)\n` +
    `14. Demote Admin — Remove admin rights from members\n` +
    `15. Auto Chat ⭐ — Auto send messages to friends/groups\n` +
    `16. Auto Accepter — Auto-accept invite-link join requests\n\n` +
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

  // Always mark the session as active at the top of every poll. This bumps
  // lastActivityAt even when the WA socket is briefly disconnected, so the
  // idle-eviction sweep never closes our protected session.
  markSessionActive(userIdStr);
  void sendSocketPresence(userIdStr);

  // Stop immediately if the user's access has expired or they've been banned.
  const [banned, access] = await Promise.all([isBanned(userId), hasAccess(userId)]);
  if (banned || (!isAdmin(userId) && !access)) {
    void stopAutoAccepterJob(userId, "access_revoked");
    return;
  }

  // If the WA socket got dropped for any reason (network blip, WhatsApp
  // server-side reset, etc.), lazy-restore it from MongoDB BEFORE we try
  // to read the pending list. Without this the polling silently returns
  // empty for every iteration after a disconnect.
  if (!isConnected(userIdStr)) {
    try {
      console.log(`[AutoAccepter][${userId}] Socket not connected — attempting lazy restore`);
      const restored = await ensureSessionLoaded(userIdStr);
      if (!restored) {
        console.warn(`[AutoAccepter][${userId}] Lazy restore failed — will retry next poll`);
      }
    } catch (err: any) {
      console.error(`[AutoAccepter][${userId}] Lazy restore error:`, err?.message);
    }
  }

  for (let i = 0; i < groupIds.length; i++) {
    const groupId = groupIds[i];
    try {
      const jids = await getGroupPendingInviteLinkJoins(userIdStr, groupId);
      // Dedupe within this single poll iteration (same JID should not appear
      // twice in one pending list, but be defensive).
      const uniqueJids = Array.from(new Set(jids));

      for (const jid of uniqueJids) {
        // NOTE: We intentionally do NOT skip JIDs we have approved earlier
        // in this job. If a user leaves the group and re-joins via the
        // invite link, WhatsApp puts them back into the pending list with
        // the same JID — we must approve them again. WhatsApp itself only
        // surfaces JIDs that are currently pending, so re-approval will
        // never happen for a user who is still a member.
        let ok = await approveGroupParticipant(userIdStr, groupId, jid);
        // Retry once on failure — transient WhatsApp errors can cause a
        // single approval to fail even though the session is still alive.
        if (!ok) {
          await new Promise(r => setTimeout(r, 1000));
          ok = await approveGroupParticipant(userIdStr, groupId, jid);
        }
        if (ok) {
          job.seenJids.add(jid); // kept for stats / debugging only
          job.totalAccepted++;
          newCount++;
        }
        // Small delay between each approval to avoid rate-limiting from WhatsApp.
        await new Promise(r => setTimeout(r, 300));
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
      `<i>Polls every 10 seconds. Only accepts invite-link joiners.</i>`,
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("⛔ Cancel", "ar_stop_job"),
      }
    );
  } catch {}

  // Persist updated totalAccepted to MongoDB so count survives a bot restart.
  void saveAutoAccepterJob({
    userId: job.userId,
    groupIds: job.groupIds,
    groupNames: job.groupNames,
    durationMs: job.durationMs,
    endsAt: job.endsAt,
    chatId: job.chatId,
    statusMsgId: job.statusMsgId,
    totalAccepted: job.totalAccepted,
    savedAt: Date.now(),
  });
}

async function stopAutoAccepterJob(userId: number, reason: "done" | "cancelled" | "access_revoked"): Promise<void> {
  const job = autoAccepterJobs.get(userId);
  if (!job) return;

  clearInterval(job.pollTimer);
  clearTimeout(job.endTimer);
  autoAccepterJobs.delete(userId);

  // Remove from MongoDB now that the job is finished.
  void deleteAutoAccepterJob(userId);

  // Release the WhatsApp session back to normal idle-eviction rules now that
  // the long-lived job is over. If we forget this, memory eviction can never
  // close this user's socket again.
  unprotectSession(String(userId));

  let msg: string;
  if (reason === "done") {
    msg =
      `🛡️ <b>Auto Request Accepter — Finished</b>\n\n` +
      `✅ <b>Total Accepted:</b> ${job.totalAccepted}\n` +
      `⏱️ <b>Duration:</b> ${Math.round(job.durationMs / 60000)} min\n\n` +
      `<b>Time is up! The Auto Request Accepter has been stopped.</b>\n` +
      `Your selected groups will no longer auto-accept join requests.`;
  } else if (reason === "access_revoked") {
    msg =
      `🚫 <b>Auto Request Accepter — Stopped</b>\n\n` +
      `✅ <b>Total Accepted:</b> ${job.totalAccepted}\n\n` +
      `<b>Your access has expired or been revoked.</b>\n` +
      `The Auto Request Accepter has been stopped automatically. Please renew your access to use this feature.`;
  } else {
    msg =
      `⛔ <b>Auto Request Accepter — Cancelled</b>\n\n` +
      `✅ <b>Total Accepted:</b> ${job.totalAccepted}\n\n` +
      `You cancelled the Auto Request Accepter. No more requests will be auto-accepted.`;
  }

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

  // Also send a separate notification message for done or access_revoked
  if (reason === "done") {
    try {
      await bot.api.sendMessage(
        job.chatId,
        `🔔 <b>Notification: Auto Request Accepter Stopped</b>\n\n` +
        `The Auto Request Accepter has been turned off — your selected time duration has expired.\n\n` +
        `✅ <b>Total requests accepted:</b> ${job.totalAccepted}`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
  } else if (reason === "access_revoked") {
    try {
      await bot.api.sendMessage(
        job.chatId,
        `🔔 <b>Notification: Auto Request Accepter Stopped</b>\n\n` +
        `Your access has expired or been revoked, so the Auto Request Accepter was stopped automatically.\n\n` +
        `✅ <b>Total requests accepted:</b> ${job.totalAccepted}\n\n` +
        `Please renew your access to continue using this feature.`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
  }
}

bot.callbackQuery("auto_accepter", async (ctx) => {
  ctx.answerCallbackQuery();
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

  {
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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

bot.callbackQuery("ar_page_info", async (ctx) => { ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" }); });

bot.callbackQuery("ar_select_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.arData) return;
  for (let i = 0; i < state.arData.allGroups.length; i++) state.arData.selectedIndices.add(i);
  await ctx.editMessageText(
    `🛡️ <b>Auto Request Accepter</b>\n\nAll <b>${state.arData.allGroups.length} groups selected</b>`,
    { parse_mode: "HTML", reply_markup: buildArKeyboard(state) }
  );
});

bot.callbackQuery("ar_clear_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.arData) return;
  state.arData.selectedIndices.clear();
  await ctx.editMessageText(
    `🛡️ <b>Auto Request Accepter</b>\n\n📱 <b>${state.arData.allGroups.length} admin group(s)</b>\n\nSelect groups:\n<i>None selected yet</i>`,
    { parse_mode: "HTML", reply_markup: buildArKeyboard(state) }
  );
});

bot.callbackQuery("ar_proceed", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  const arGroups: Array<{ id: string; subject: string }> = (state as any)?.arGroups;
  if (!arGroups || !arGroups.length) { ctx.answerCallbackQuery({ text: "Session expired. Please try again.", show_alert: true }); return; }

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
    `• Bot polls every 10 seconds\n` +
    `• Only users who joined via invite link will be accepted\n` +
    `• Admin-added pending requests will NOT be accepted\n` +
    `• You will get a notification when time is up\n\n` +
    `Tap <b>Start</b> to begin or <b>Cancel</b> to go back.`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery("ar_confirm", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  const arGroups: Array<{ id: string; subject: string }> = (state as any)?.arGroups;
  const durationMs: number = (state as any)?.arDurationMs;
  if (!arGroups || !durationMs) { ctx.answerCallbackQuery({ text: "Session expired. Please try again.", show_alert: true }); return; }
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ WhatsApp not connected!", {
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    }); return;
  }

  // Check if already running
  if (autoAccepterJobs.has(userId)) {
    ctx.answerCallbackQuery({ text: "A job is already running! Stop it first.", show_alert: true }); return;
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

  // Persist to MongoDB so the job survives a bot restart.
  void saveAutoAccepterJob({
    userId,
    groupIds,
    groupNames,
    durationMs,
    endsAt,
    chatId,
    statusMsgId,
    totalAccepted: 0,
    savedAt: Date.now(),
  });

  // Protect this user's WhatsApp session from idle-eviction for the entire
  // duration of the job. Without this, after 30 minutes of Telegram inactivity
  // the sweep would close the WA socket and the polling would silently stop
  // accepting requests even though the job is still scheduled to run.
  protectSessionFromEviction(String(userId));

  // Start polling every 10 seconds
  job.pollTimer = setInterval(() => {
    void runAutoAccepterPoll(job);
  }, 10_000);

  // End timer
  job.endTimer = setTimeout(() => {
    void stopAutoAccepterJob(userId, "done");
  }, durationMs);

  // Run immediately on start
  void runAutoAccepterPoll(job);
});

bot.callbackQuery("ar_stop_job", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Stopping Auto Request Accepter..." });
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

// ─── Steal Group ─────────────────────────────────────────────────────────────

interface StealGroupSession {
  adminId: number;
  waUserId: string;
  groups: Array<{
    id: string;
    subject: string;
    knownJids: Set<string>;
    done: boolean;
  }>;
  chatId: number;
  statusMsgId: number;
  pollTimer: ReturnType<typeof setInterval>;
  stopped: boolean;
  totalStolen: number;
}

const stealGroupSessions = new Map<number, StealGroupSession>();
const sgLinkCollectMsgId = new Map<number, number>();
const SG_PAGE_SIZE = 6;

function buildSgGroupKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const d = state.stealGroupData!;
  const allGroups = d.allGroups;
  const page = d.page || 0;
  const start = page * SG_PAGE_SIZE;
  const slice = allGroups.slice(start, start + SG_PAGE_SIZE);
  for (let i = 0; i < slice.length; i++) {
    const idx = start + i;
    const selected = d.selectedIndices.has(idx);
    kb.text(`${selected ? "✅" : "⬜"} ${slice[i].subject}`, `sg_toggle_${idx}`).row();
  }
  const totalPages = Math.ceil(allGroups.length / SG_PAGE_SIZE);
  const navRow: Array<[string, string]> = [];
  if (page > 0) navRow.push(["◀️ Prev", "sg_page_prev"]);
  if (page < totalPages - 1) navRow.push(["Next ▶️", "sg_page_next"]);
  if (navRow.length) { navRow.forEach(([t, c]) => kb.text(t, c)); kb.row(); }
  if (d.selectedIndices.size > 0) kb.text(`✅ Scan ${d.selectedIndices.size} Group(s)`, "sg_confirm_select").row();
  kb.text("🏠 Main Menu", "main_menu");
  return kb;
}

async function updateStealStatus(session: StealGroupSession): Promise<void> {
  const { chatId, statusMsgId, groups, totalStolen, stopped } = session;
  const remaining = groups.filter(g => !g.done);
  const done = groups.filter(g => g.done);
  const header = stopped
    ? (totalStolen > 0 ? `☠️ <b>Steal Group — Stopped</b>` : `⛔ <b>Steal Group — Cancelled</b>`)
    : `☠️ <b>Steal Group — Running...</b>`;
  const text =
    `${header}\n\n` +
    `✅ Stolen: <b>${totalStolen}</b>\n` +
    `⏳ Watching: <b>${remaining.length}</b> group(s)\n\n` +
    (done.length > 0 ? done.slice(-5).map(g => `☠️ ${esc(g.subject)}`).join("\n") + "\n\n" : "") +
    (remaining.length > 0
      ? `⌛ Waiting for someone to join...\n` + remaining.slice(0, 5).map(g => `• ${esc(g.subject)}`).join("\n")
      : `🎉 All groups processed!`);
  try {
    await bot.api.editMessageText(chatId, statusMsgId, text, {
      parse_mode: "HTML",
      reply_markup: stopped
        ? new InlineKeyboard().text("🏠 Main Menu", "main_menu")
        : new InlineKeyboard().text("⛔ Cancel", "sg_cancel"),
    });
  } catch {}
}

async function stopStealGroupSession(adminId: number, reason: "done" | "cancelled"): Promise<void> {
  const session = stealGroupSessions.get(adminId);
  if (!session) return;
  session.stopped = true;
  clearInterval(session.pollTimer);
  stealGroupSessions.delete(adminId);
  await updateStealStatus(session);
}

async function runStealGroupPoll(session: StealGroupSession): Promise<void> {
  if (session.stopped) return;
  const { adminId, waUserId, groups } = session;

  for (const group of groups) {
    if (group.done || session.stopped) continue;
    try {
      // 1. Check pending join requests (approval mode on)
      const pendingJids = await getGroupPendingInviteLinkJoins(waUserId, group.id);
      for (const jid of pendingJids) {
        if (session.stopped || group.done) break;
        try { await approveGroupParticipant(waUserId, group.id, jid); } catch {}
        await new Promise(r => setTimeout(r, 800));
        try { await makeGroupAdmin(waUserId, group.id, jid); } catch {}
        await new Promise(r => setTimeout(r, 800));
        try { await leaveGroup(waUserId, group.id); } catch {}
        await new Promise(r => setTimeout(r, 1000));
        try { await deleteGroupChat(waUserId, group.id); } catch {}
        group.done = true;
        session.totalStolen++;
        console.log(`[StealGroup][${adminId}] Stolen via pending: ${group.subject} → ${jid}`);
        break;
      }

      if (group.done || session.stopped) continue;

      // 2. Check for new direct joins (approval mode off)
      const participants = await getGroupParticipants(waUserId, group.id);
      const currentJids = new Set(participants.map(p => p.jid));
      const newJids = [...currentJids].filter(j => !group.knownJids.has(j));
      for (const jid of newJids) {
        if (session.stopped || group.done) break;
        try { await makeGroupAdmin(waUserId, group.id, jid); } catch {}
        await new Promise(r => setTimeout(r, 800));
        try { await leaveGroup(waUserId, group.id); } catch {}
        await new Promise(r => setTimeout(r, 1000));
        try { await deleteGroupChat(waUserId, group.id); } catch {}
        group.done = true;
        session.totalStolen++;
        console.log(`[StealGroup][${adminId}] Stolen via direct join: ${group.subject} → ${jid}`);
        break;
      }

      // Keep known JIDs up to date
      if (!group.done) {
        currentJids.forEach(j => group.knownJids.add(j));
      }
    } catch (err: any) {
      console.error(`[StealGroup][${adminId}] Poll error for ${group.subject}:`, err?.message);
    }
  }

  if (!session.stopped) {
    const remaining = groups.filter(g => !g.done);
    if (remaining.length === 0) {
      await stopStealGroupSession(adminId, "done");
    } else {
      await updateStealStatus(session);
    }
  }
}

// ── Entry point ───────────────────────────────────────────────────────────────
bot.callbackQuery("steal_group", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isAdmin(userId)) { ctx.answerCallbackQuery({ text: "Admin only.", show_alert: true }); return; }
  const borrowedId = getSessionAlias(String(userId));
  if (!borrowedId) {
    await ctx.editMessageText(
      "☠️ <b>Steal Group</b>\n\n❌ You must first borrow a user's WhatsApp session using <code>/ws &lt;user_id&gt;</code>.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    ); return;
  }
  if (!isConnected(String(userId))) {
    await ctx.editMessageText(
      "☠️ <b>Steal Group</b>\n\n❌ The borrowed WhatsApp session is not connected.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    ); return;
  }
  if (stealGroupSessions.has(userId)) {
    await ctx.editMessageText(
      "☠️ <b>Steal Group — Already Running</b>\n\nA steal session is already active. Cancel it first.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel Active", "sg_cancel").text("🏠 Menu", "main_menu") }
    ); return;
  }
  await ctx.editMessageText(
    `☠️ <b>Steal Group</b>\n\n` +
    `Borrowing: <code>${esc(borrowedId)}</code>\n\n` +
    `Select groups to watch. When anyone joins, they will be made admin and the bot will immediately leave and delete the group from the account.\n\n` +
    `Choose how to select groups:`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("📋 All Groups", "sg_all_groups").row()
        .text("🔗 By Link", "sg_by_link").row()
        .text("🏠 Main Menu", "main_menu"),
    }
  );
});

// ── All Groups ────────────────────────────────────────────────────────────────
bot.callbackQuery("sg_all_groups", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isAdmin(userId)) return;
  await ctx.editMessageText("☠️ <b>Scanning WhatsApp groups...</b>\n\n⌛ Please wait...", { parse_mode: "HTML" });
  const groups = await getAllGroups(String(userId));
  const adminGroups = groups.filter(g => g.isAdmin).map(g => ({ id: g.id, subject: g.subject }))
    .sort((a, b) => a.subject.localeCompare(b.subject, undefined, { numeric: true, sensitivity: "base" }));
  if (!adminGroups.length) {
    await ctx.editMessageText("☠️ ❌ No admin groups found on the borrowed WhatsApp.", {
      reply_markup: new InlineKeyboard().text("🔗 By Link", "sg_by_link").text("🏠 Menu", "main_menu"),
    }); return;
  }
  const patterns = detectSimilarGroups(adminGroups);
  userStates.set(userId, {
    step: "sg_select",
    stealGroupData: { allGroups: adminGroups, patterns, selectedIndices: new Set(), page: 0 },
  });
  await ctx.editMessageText(
    `☠️ <b>Steal Group — Select Groups</b>\n\n` +
    `Found <b>${adminGroups.length}</b> admin group(s). Select the groups to watch:`,
    { parse_mode: "HTML", reply_markup: buildSgGroupKeyboard(userStates.get(userId)!) }
  );
});

// ── By Link ───────────────────────────────────────────────────────────────────
bot.callbackQuery("sg_by_link", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isAdmin(userId)) return;
  userStates.set(userId, { step: "sg_enter_links", sgLinkBuffer: [] });
  sgLinkCollectMsgId.delete(userId);
  await ctx.editMessageText(
    "☠️ <b>Steal Group — By Link</b>\n\n" +
    "Send WhatsApp group invite links (one per line or multiple at once):\n" +
    "<code>https://chat.whatsapp.com/ABC123</code>\n\n" +
    "<i>The <b>Done</b> button will appear after you send at least one link.</i>",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("sg_link_done", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isAdmin(userId)) return;
  const state = userStates.get(userId);
  if (!state || state.step !== "sg_enter_links") return;
  const buffer = state.sgLinkBuffer || [];
  if (!buffer.length) { ctx.answerCallbackQuery({ text: "❌ Send at least one link first!", show_alert: true }); return; }
  sgLinkCollectMsgId.delete(userId);
  await ctx.editMessageText("☠️ <b>Resolving group links...</b>\n\n⌛ Please wait...", { parse_mode: "HTML" });
  const groups: Array<{ id: string; subject: string }> = [];
  for (const link of buffer) {
    const info = await getGroupIdFromLink(String(userId), link);
    if (info) groups.push({ id: info.id, subject: info.subject });
  }
  if (!groups.length) {
    await ctx.editMessageText(
      "☠️ ❌ Could not resolve any links.\n\nCheck that:\n• Links are valid\n• WhatsApp is connected\n• Bot is admin in the groups",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔗 Try Again", "sg_by_link").text("🏠 Menu", "main_menu") }
    ); return;
  }
  const patterns = detectSimilarGroups(groups);
  userStates.set(userId, {
    step: "sg_select",
    stealGroupData: { allGroups: groups, patterns, selectedIndices: new Set(groups.map((_, i) => i)), page: 0 },
  });
  await ctx.editMessageText(
    `☠️ <b>Steal Group — Groups Found</b>\n\n` +
    `Resolved <b>${groups.length}</b> group(s). Confirm selection or deselect:`,
    { parse_mode: "HTML", reply_markup: buildSgGroupKeyboard(userStates.get(userId)!) }
  );
});

// ── Group selection toggles / pagination ──────────────────────────────────────
bot.callbackQuery(/^sg_toggle_(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.stealGroupData) return;
  const idx = parseInt(ctx.match[1]);
  if (state.stealGroupData.selectedIndices.has(idx)) state.stealGroupData.selectedIndices.delete(idx);
  else state.stealGroupData.selectedIndices.add(idx);
  const d = state.stealGroupData;
  await ctx.editMessageText(
    `☠️ <b>Steal Group — Select Groups</b>\n\n` +
    `<b>${d.allGroups.length}</b> group(s) total | <b>${d.selectedIndices.size}</b> selected:`,
    { parse_mode: "HTML", reply_markup: buildSgGroupKeyboard(state) }
  );
});

bot.callbackQuery("sg_page_prev", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.stealGroupData) return;
  if (state.stealGroupData.page > 0) state.stealGroupData.page--;
  const d = state.stealGroupData;
  await ctx.editMessageText(
    `☠️ <b>Steal Group — Select Groups</b>\n\n<b>${d.allGroups.length}</b> group(s) | <b>${d.selectedIndices.size}</b> selected:`,
    { parse_mode: "HTML", reply_markup: buildSgGroupKeyboard(state) }
  );
});

bot.callbackQuery("sg_page_next", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.stealGroupData) return;
  const totalPages = Math.ceil(state.stealGroupData.allGroups.length / SG_PAGE_SIZE);
  if (state.stealGroupData.page < totalPages - 1) state.stealGroupData.page++;
  const d = state.stealGroupData;
  await ctx.editMessageText(
    `☠️ <b>Steal Group — Select Groups</b>\n\n<b>${d.allGroups.length}</b> group(s) | <b>${d.selectedIndices.size}</b> selected:`,
    { parse_mode: "HTML", reply_markup: buildSgGroupKeyboard(state) }
  );
});

// ── Confirm selection → scan groups ──────────────────────────────────────────
bot.callbackQuery("sg_confirm_select", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isAdmin(userId)) return;
  const state = userStates.get(userId);
  if (!state?.stealGroupData || state.stealGroupData.selectedIndices.size === 0) {
    ctx.answerCallbackQuery({ text: "❌ Select at least one group first!", show_alert: true }); return;
  }
  const d = state.stealGroupData;
  const selected = [...d.selectedIndices].map(i => d.allGroups[i]).filter(Boolean);
  await ctx.editMessageText(
    `☠️ <b>Scanning ${selected.length} group(s)...</b>\n\n⌛ Getting member info...`,
    { parse_mode: "HTML" }
  );
  // Scan each group for info
  const scanned: NonNullable<typeof d.scannedGroups> = [];
  for (const g of selected) {
    try {
      const parts = await getGroupParticipants(String(userId), g.id);
      const adminCount = parts.filter(p => p.isAdmin || p.isSuperAdmin).length;
      const creator = parts.find(p => p.isSuperAdmin);
      const creatorPhone = creator?.phone || "Unknown";
      let approvalMode = false;
      try {
        const meta = await (getGroupParticipants as any)._socket
          ? null
          : null;
        // approvalMode detection via pending list presence
        const pending = await getGroupPendingRequests(String(userId), g.id);
        approvalMode = Array.isArray(pending); // if call succeeds, approval mode may be on
      } catch {}
      scanned.push({ id: g.id, subject: g.subject, memberCount: parts.length, adminCount, creatorPhone, approvalMode });
    } catch (err: any) {
      scanned.push({ id: g.id, subject: g.subject, memberCount: 0, adminCount: 0, creatorPhone: "Unknown", approvalMode: false });
    }
  }
  d.scannedGroups = scanned;
  const scanText =
    `☠️ <b>Steal Group — Group Scan</b>\n\n` +
    scanned.map(g =>
      `📋 <b>${esc(g.subject)}</b>\n` +
      `👥 Members: <b>${g.memberCount}</b> | 👑 Admins: <b>${g.adminCount}</b>\n` +
      `🏗️ Creator: <code>${esc(g.creatorPhone)}</code>`
    ).join("\n\n") +
    `\n\n⚠️ <b>When anyone joins these groups:</b>\n` +
    `1. Bot accepts them (if approval mode)\n` +
    `2. Bot makes them Admin immediately\n` +
    `3. Bot leaves & removes group silently\n\n` +
    `Are you sure you want to start?`;
  await ctx.editMessageText(scanText, {
    parse_mode: "HTML",
    reply_markup: new InlineKeyboard()
      .text("☠️ Start Stealing", "sg_start_poll").row()
      .text("❌ Cancel", "main_menu"),
  });
});

// ── Start background polling ──────────────────────────────────────────────────
bot.callbackQuery("sg_start_poll", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isAdmin(userId)) return;
  const state = userStates.get(userId);
  if (!state?.stealGroupData?.scannedGroups?.length) {
    ctx.answerCallbackQuery({ text: "❌ No groups to watch.", show_alert: true }); return;
  }
  const d = state.stealGroupData;
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;
  userStates.delete(userId);

  const sessionGroups = d.scannedGroups!.map(async (g) => {
    const parts = await getGroupParticipants(String(userId), g.id).catch(() => []);
    return { id: g.id, subject: g.subject, knownJids: new Set(parts.map(p => p.jid)), done: false };
  });
  const resolvedGroups = await Promise.all(sessionGroups);

  const session: StealGroupSession = {
    adminId: userId,
    waUserId: String(userId),
    groups: resolvedGroups,
    chatId,
    statusMsgId: msgId,
    stopped: false,
    totalStolen: 0,
    pollTimer: null as any,
  };
  stealGroupSessions.set(userId, session);
  protectSessionFromEviction(String(userId));

  await ctx.editMessageText(
    `☠️ <b>Steal Group — Active</b>\n\n` +
    `Watching <b>${resolvedGroups.length}</b> group(s)...\n` +
    resolvedGroups.map(g => `• ${esc(g.subject)}`).join("\n") +
    `\n\n⌛ Waiting for someone to join...`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "sg_cancel") }
  );

  // Poll every 5 seconds
  session.pollTimer = setInterval(() => {
    void runStealGroupPoll(session);
  }, 5_000);

  // Run immediately
  void runStealGroupPoll(session);
});

// ── Cancel ────────────────────────────────────────────────────────────────────
bot.callbackQuery("sg_cancel", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Stopping Steal Group..." });
  const userId = ctx.from.id;
  if (!stealGroupSessions.has(userId)) {
    try {
      await ctx.editMessageText("⚠️ No Steal Group session is currently running.", {
        reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
      });
    } catch {}
    return;
  }
  await stopStealGroupSession(userId, "cancelled");
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

  {
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
  ctx.answerCallbackQuery();
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
  kb.text("🔗 By Link", "lv_by_link").row();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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

bot.callbackQuery("lv_page_info", async (ctx) => { ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" }); });

bot.callbackQuery("lv_select_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.leaveData?.selectedIndices) return;
  for (let i = 0; i < state.leaveData.groups.length; i++) state.leaveData.selectedIndices.add(i);
  await ctx.editMessageText(
    `🚪 <b>Select Groups to Leave</b>\n\nAll <b>${state.leaveData.groups.length} groups selected</b>`,
    { parse_mode: "HTML", reply_markup: buildLeaveKeyboard(state) }
  );
});

bot.callbackQuery("lv_clear_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.leaveData?.selectedIndices) return;
  state.leaveData.selectedIndices.clear();
  await ctx.editMessageText(
    `🚪 <b>Select Groups to Leave</b>\n\n📊 <b>${state.leaveData.groups.length}</b> groups total\n\nTap to select/deselect:\n<i>None selected yet</i>`,
    { parse_mode: "HTML", reply_markup: buildLeaveKeyboard(state) }
  );
});

bot.callbackQuery("lv_proceed", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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

    // ── Phase 1: Leave ALL groups in batches of 5 concurrently ──────────────
    // Batch size reduced from 20 → 5 to avoid WhatsApp rate-limiting.
    // A 1.5 s delay is inserted between batches so the WA socket is not
    // flooded. Failed groups get one automatic retry after a 3 s pause —
    // this resolves transient "connection busy" failures that cause the
    // tail of a large list to fail while the first 20 succeed.
    const LV_CONCURRENT = 5;
    const LV_BATCH_DELAY_MS = 1500;   // wait between batches
    const LV_RETRY_DELAY_MS = 3000;   // wait before retrying a failed group
    const leftIds = new Set<string>();

    for (let batchStart = 0; batchStart < groups.length; batchStart += LV_CONCURRENT) {
      if (leaveJobCancel.has(userId)) { cancelled = true; break; }

      const batch = groups.slice(batchStart, batchStart + LV_CONCURRENT);
      const batchResults = await Promise.allSettled(
        batch.map(async (g) => {
          let ok = await leaveGroup(String(userId), g.id);
          // Retry once on failure after a short delay
          if (!ok) {
            await new Promise((r) => setTimeout(r, LV_RETRY_DELAY_MS));
            ok = await leaveGroup(String(userId), g.id);
          }
          return { g, ok };
        })
      );

      for (let bi = 0; bi < batchResults.length; bi++) {
        const res = batchResults[bi];
        const g = batch[bi];
        if (res.status === "fulfilled" && res.value.ok) {
          lines.push(`✅ Left: ${esc(g.subject)}`);
          leftIds.add(g.id);
          success++;
        } else {
          lines.push(`❌ Failed: ${esc(g.subject)}`);
          failed++;
        }
      }

      const done = Math.min(batchStart + LV_CONCURRENT, groups.length);
      try {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Phase 1/2 — Leaving: ${done}/${groups.length}</b>\n\n${lines.slice(-20).join("\n")}`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "lv_cancel") }
        );
      } catch {}

      // Rate-limit guard: pause between batches (skip after last batch)
      if (batchStart + LV_CONCURRENT < groups.length && !leaveJobCancel.has(userId)) {
        await new Promise((r) => setTimeout(r, LV_BATCH_DELAY_MS));
      }
    }

    // ── Phase 2: Delete ALL successfully left groups in batches of 5 ─────────
    if (!cancelled && leftIds.size > 0) {
      const toDelete = groups.filter(g => leftIds.has(g.id));
      try {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Phase 2/2 — Deleting chats: 0/${toDelete.length}</b>\n\n🗑️ Removing group chats from account...`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "lv_cancel") }
        );
      } catch {}

      for (let batchStart = 0; batchStart < toDelete.length; batchStart += LV_CONCURRENT) {
        if (leaveJobCancel.has(userId)) { cancelled = true; break; }

        const batch = toDelete.slice(batchStart, batchStart + LV_CONCURRENT);
        await Promise.allSettled(
          batch.map(async (g) => {
            try { await deleteGroupChat(String(userId), g.id); } catch {}
          })
        );

        const done = Math.min(batchStart + LV_CONCURRENT, toDelete.length);
        try {
          await bot.api.editMessageText(chatId, msgId,
            `⏳ <b>Phase 2/2 — Deleting: ${done}/${toDelete.length}</b>\n\n🗑️ Removing group chats...`,
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "lv_cancel") }
          );
        } catch {}

        // Rate-limit guard between delete batches
        if (batchStart + LV_CONCURRENT < toDelete.length && !leaveJobCancel.has(userId)) {
          await new Promise((r) => setTimeout(r, LV_BATCH_DELAY_MS));
        }
      }
    }

    leaveJobCancel.delete(userId);
    schedulePostFeatureGC();
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
  ctx.answerCallbackQuery({ text: "⛔ Cancelling...", show_alert: false });
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

  {
    const prev = page > 0 ? "⬅️ Previous" : " ";
    const next = page < totalPages - 1 ? "➡️ Next" : " ";
    kb.text(prev, "rm_page_prev").text(`📄 ${page + 1}/${totalPages}`, "rm_page_info").text(next, "rm_page_next").row();
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

const RM_SIM_PAGE_SIZE = 10;

function buildRmSimilarKeyboard(patterns: SimilarGroup[], page: number): InlineKeyboard {
  const kb = new InlineKeyboard();
  const totalPages = Math.max(1, Math.ceil(patterns.length / RM_SIM_PAGE_SIZE));
  const start = page * RM_SIM_PAGE_SIZE;
  const end = Math.min(start + RM_SIM_PAGE_SIZE, patterns.length);
  for (let i = start; i < end; i++) {
    kb.text(`📌 ${patterns[i].base} (${patterns[i].groups.length} groups)`, `rm_sim_${i}`).row();
  }
  if (totalPages > 1) {
    const prev = page > 0 ? "⬅️ Previous" : " ";
    const next = page < totalPages - 1 ? "Next ➡️" : " ";
    kb.text(prev, "rm_sim_prev_page").text(`📄 ${page + 1}/${totalPages}`, "rm_sim_page_info").text(next, "rm_sim_next_page").row();
  }
  kb.text("🔙 Back", "remove_members").text("🏠 Menu", "main_menu");
  return kb;
}

bot.callbackQuery("remove_members", async (ctx) => {
  ctx.answerCallbackQuery();
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
    step: "remove_members_menu",
    removeData: {
      allGroups: adminGroupsSimple,
      selectedIndices: new Set(),
      page: 0,
      patterns,
    },
  });

  const kb = new InlineKeyboard();
  if (patterns.length > 0) kb.text("🔍 Similar Groups", "rm_similar").text("📋 All Groups", "rm_show_all").row();
  else kb.text("📋 All Groups", "rm_show_all").row();
  kb.text("🔗 By Link", "rm_by_link").row();
  kb.text("🏠 Main Menu", "main_menu");

  await ctx.editMessageText(
    `🗑️ <b>Remove Members</b>\n\n` +
    `👑 Admin Groups: ${adminGroups.length} (Total: ${allGroups.length})\n` +
    (patterns.length > 0 ? `🔍 Similar Patterns: ${patterns.length}\n` : "") +
    `\n📌 Choose an option:`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery("rm_similar", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeData) return;

  const patterns = state.removeData.patterns || [];
  if (!patterns.length) {
    await ctx.editMessageText("⚠️ No similar group patterns found.", {
      reply_markup: new InlineKeyboard().text("🔙 Back", "remove_members").text("🏠 Menu", "main_menu"),
    }); return;
  }

  state.removeData.patternPage = 0;
  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select those groups:",
    { parse_mode: "HTML", reply_markup: buildRmSimilarKeyboard(patterns, 0) }
  );
});

bot.callbackQuery("rm_sim_prev_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeData) return;
  const current = state.removeData.patternPage || 0;
  if (current > 0) state.removeData.patternPage = current - 1;
  const page = state.removeData.patternPage || 0;
  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select those groups:",
    { parse_mode: "HTML", reply_markup: buildRmSimilarKeyboard(state.removeData.patterns || [], page) }
  );
});

bot.callbackQuery("rm_sim_next_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeData) return;
  const patterns = state.removeData.patterns || [];
  const totalPages = Math.ceil(patterns.length / RM_SIM_PAGE_SIZE);
  const current = state.removeData.patternPage || 0;
  if (current < totalPages - 1) state.removeData.patternPage = current + 1;
  const page = state.removeData.patternPage || 0;
  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select those groups:",
    { parse_mode: "HTML", reply_markup: buildRmSimilarKeyboard(patterns, page) }
  );
});

bot.callbackQuery("rm_sim_page_info", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Use Previous / Next to change page" });
});

bot.callbackQuery(/^rm_sim_(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeData) return;

  const idx = parseInt(ctx.match![1]);
  const patterns = state.removeData.patterns || [];
  const pattern = patterns[idx];
  if (!pattern) return;

  const patternIds = new Set(pattern.groups.map((g: { id: string }) => g.id));
  state.removeData.selectedIndices = new Set();
  for (let i = 0; i < state.removeData.allGroups.length; i++) {
    if (patternIds.has(state.removeData.allGroups[i].id)) {
      state.removeData.selectedIndices.add(i);
    }
  }
  state.step = "remove_members_select";
  state.removeData.page = 0;
  const selectedCount = state.removeData.selectedIndices.size;
  await ctx.editMessageText(
    `🗑️ <b>Remove Members</b>\n\n👑 <b>${state.removeData.allGroups.length} admin group(s)</b>\n\nSelect group(s) to remove members from:\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildRemoveMembersKeyboard(state) }
  );
});

bot.callbackQuery("rm_show_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeData) return;
  state.step = "remove_members_select";
  state.removeData.page = 0;
  await ctx.editMessageText(
    `🗑️ <b>Remove Members</b>\n\n👑 <b>${state.removeData.allGroups.length} admin group(s)</b>\n\nSelect group(s) to remove members from:\n<i>Tap to select/deselect</i>`,
    { parse_mode: "HTML", reply_markup: buildRemoveMembersKeyboard(state) }
  );
});

bot.callbackQuery(/^rm_tog_(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
});

bot.callbackQuery("rm_proceed", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeData || state.removeData.selectedIndices.size === 0) return;

  const selectedGroups = Array.from(state.removeData.selectedIndices).map(i => state.removeData!.allGroups[i]);

  userStates.set(userId, {
    step: "remove_mode_select",
    removeExcludeData: {
      selectedGroups,
      excludeNumbers: new Set(),
      excludePrefixes: new Set(),
    },
    removeFriendData: { selectedGroups },
  });

  const groupList = selectedGroups.slice(0, 10).map(g => `• ${esc(g.subject)}`).join("\n");
  const more = selectedGroups.length > 10 ? `\n<i>...+${selectedGroups.length - 10} more</i>` : "";
  await ctx.editMessageText(
    `✅ <b>${selectedGroups.length} group(s) selected:</b>\n\n${groupList}${more}\n\n` +
    `<b>Choose what to remove:</b>`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🗑️ Remove All Members", "rm_mode_members").row()
        .text("👥 Remove Friend", "rm_mode_friend").row()
        .text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("rm_mode_members", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeExcludeData) return;

  state.step = "remove_exclude_numbers";
  const selectedGroups = state.removeExcludeData.selectedGroups;
  const groupList = selectedGroups.slice(0, 10).map(g => `• ${esc(g.subject)}`).join("\n");
  const more = selectedGroups.length > 10 ? `\n<i>...+${selectedGroups.length - 10} more</i>` : "";
  await ctx.editMessageText(
    `✅ <b>${selectedGroups.length} group(s) selected:</b>\n\n${groupList}${more}\n\n` +
    `📱 <b>Exclude Numbers</b>\n\n` +
    `🛡️ Admins are always safe — they are never removed regardless of exclusion list.\n\n` +
    `You can exclude numbers in two ways (one per line, mix both):\n\n` +
    `1️⃣ <b>Full number</b> — only that number is excluded.\n` +
    `   Example:\n   <code>+919912345678\n   +919998887777</code>\n\n` +
    `2️⃣ <b>Country code only</b> (1–4 digits, + optional) — <i>all</i> numbers from that country are excluded.\n` +
    `   Example:\n   <code>+91\n   +92</code>\n\n` +
    `If you don't want to exclude anyone, tap <b>Skip</b>:`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("⏭️ Skip", "rm_skip_exclude")
        .text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("rm_mode_friend", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeFriendData) return;

  state.step = "remove_friend_enter_numbers";
  const selectedGroups = state.removeFriendData.selectedGroups;
  const groupList = selectedGroups.slice(0, 10).map(g => `• ${esc(g.subject)}`).join("\n");
  const more = selectedGroups.length > 10 ? `\n<i>...+${selectedGroups.length - 10} more</i>` : "";
  await ctx.editMessageText(
    `👥 <b>Remove Friend</b>\n\n` +
    `<b>${selectedGroups.length} group(s) selected:</b>\n${groupList}${more}\n\n` +
    `Send the phone numbers or country codes to remove (one per line):\n\n` +
    `📱 <b>Full number</b> — removes only that person from all selected groups:\n` +
    `   <code>+919912345678\n   +919998887777</code>\n\n` +
    `🌍 <b>Country code only</b> (1–4 digits, + optional) — removes ALL members from that country:\n` +
    `   <code>+91\n   +92</code>\n\n` +
    `🛡️ Admins are <b>NEVER</b> removed regardless of input.`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("rm_skip_exclude", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeExcludeData) return;

  await startRemoveMembersProcess(ctx, userId, state.removeExcludeData.selectedGroups, new Set(), new Set());
});

// ─── Remove Friend callbacks ──────────────────────────────────────────────────

bot.callbackQuery("rf_cancel_request", async (ctx) => {
  ctx.answerCallbackQuery();
  cancelDialogActiveFor.add(ctx.from.id);
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard()
      .text("✅ Yes, Stop Removing", "rf_cancel_confirm")
      .text("↩️ Continue", "rf_cancel_no"),
  });
});

bot.callbackQuery("rf_cancel_no", async (ctx) => {
  cancelDialogActiveFor.delete(ctx.from.id);
  ctx.answerCallbackQuery({ text: "Removing continued" });
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard().text("❌ Cancel", "rf_cancel_request"),
  });
});

bot.callbackQuery("rf_cancel_confirm", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Stopping after current member..." });
  removeFriendCancelRequests.add(ctx.from.id);
  await ctx.editMessageReplyMarkup({ reply_markup: undefined });
});

bot.callbackQuery("rf_skip_exceptions", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeFriendData) return;
  state.removeFriendData.exceptions = [];
  const { selectedGroups, countryCodes = [], phoneNumbers = [] } = state.removeFriendData;
  const groupList = selectedGroups.slice(0, 10).map(g => `• ${esc(g.subject)}`).join("\n");
  const moreGroups = selectedGroups.length > 10 ? `\n<i>...+${selectedGroups.length - 10} more</i>` : "";
  const ccList = countryCodes.map(c => `+${c}`).join(", ");
  await ctx.editMessageText(
    `👥 <b>Remove Friend — Confirm</b>\n\n` +
    `<b>${selectedGroups.length} group(s):</b>\n${groupList}${moreGroups}\n\n` +
    (phoneNumbers.length > 0 ? `<b>Full numbers to remove:</b> ${phoneNumbers.length}\n\n` : "") +
    `<b>Country code(s):</b> ${ccList} — removes all non-admins with these codes\n` +
    `<i>No exceptions — all matching members will be removed</i>\n\n` +
    `🛡️ Admins will NOT be removed.\n\nConfirm?`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Confirm & Remove", "rf_confirm")
        .text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("rf_confirm", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.removeFriendData) return;
  const { selectedGroups, phoneNumbers = [], countryCodes = [], exceptions = [] } = state.removeFriendData;
  if (!phoneNumbers.length && !countryCodes.length) return;

  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;

  userStates.delete(userId);
  removeFriendCancelRequests.delete(userId);

  const groupList = selectedGroups.slice(0, 10).map(g => `• ${esc(g.subject)}`).join("\n");
  const more = selectedGroups.length > 10 ? `\n<i>...+${selectedGroups.length - 10} more</i>` : "";
  try {
    await ctx.editMessageText(
      `⏳ <b>Removing from ${selectedGroups.length} group(s)...</b>\n\n${groupList}${more}\n\n⌛ Please wait...`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "rf_cancel_request") }
    );
  } catch {}

  void removeFriendBackground(String(userId), selectedGroups, phoneNumbers, countryCodes, exceptions, chatId, msgId);
});

async function removeFriendBackground(
  userId: string,
  groups: Array<{ id: string; subject: string }>,
  fullNumbers: string[],   // digits-only, 7+ digits — remove only that number
  countryCodes: string[],  // digits-only, 1-4 digits — remove ALL non-admins with this prefix
  exceptions: string[],    // digits-only full numbers to exclude from country-code removal
  chatId: number,
  msgId: number
): Promise<void> {
  const userIdNum = Number(userId);
  activeBackgroundUsers.add(userIdNum);

  // Normalize inputs
  const targetPhones = fullNumbers.map(p => p.replace(/[^0-9]/g, "")).filter(p => p.length >= 7);
  const ccList = countryCodes.map(c => c.replace(/[^0-9]/g, "")).filter(c => c.length >= 1 && c.length <= 4);
  const exceptSet = new Set(exceptions.map(e => e.replace(/[^0-9]/g, "")));
  const hasCountryCodeMode = ccList.length > 0;

  let wasCancelled = false;
  let fullResult = "👥 <b>Remove Friend Result</b>\n\n";

  for (let gi = 0; gi < groups.length; gi++) {
    if (removeFriendCancelRequests.has(userIdNum)) { wasCancelled = true; break; }
    const group = groups[gi];
    const groupLines: string[] = [];
    let removed = 0, skipped = 0, failed = 0, notFound = 0;

    try {
      if (!cancelDialogActiveFor.has(userIdNum)) {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Processing group ${gi + 1}/${groups.length}:</b>\n${esc(group.subject)}\n\n⌛ Fetching members...`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "rf_cancel_request") }
        );
      }
    } catch {}

    const participants = await getGroupParticipants(userId, group.id);

    if (hasCountryCodeMode) {
      // Country code mode: walk all participants, match prefix
      for (const participant of participants) {
        if (removeFriendCancelRequests.has(userIdNum)) { wasCancelled = true; break; }
        // NEVER remove admins
        if (participant.isAdmin) continue;

        const pPhone = participant.phone.replace(/[^0-9]/g, "");
        const pLast10 = pPhone.slice(-10);
        let shouldRemove = false;

        // Check full-number match first
        for (const tp of targetPhones) {
          if (pPhone === tp || (pLast10.length >= 7 && pLast10 === tp.slice(-10))) {
            shouldRemove = true;
            break;
          }
        }
        // Check country-code match
        if (!shouldRemove) {
          for (const cc of ccList) {
            if (pPhone.startsWith(cc)) {
              if (exceptSet.has(pPhone) || exceptSet.has(pLast10)) {
                skipped++;
              } else {
                shouldRemove = true;
              }
              break;
            }
          }
        }
        if (!shouldRemove) continue;

        const ok = await removeGroupParticipant(userId, group.id, participant.jid);
        if (ok) removed++;
        else failed++;
        await new Promise(r => setTimeout(r, 600));

        // Live progress update every 5 removals
        if ((removed + failed) % 5 === 0 && !cancelDialogActiveFor.has(userIdNum)) {
          try {
            await bot.api.editMessageText(chatId, msgId,
              `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n` +
              `✅ Removed: ${removed} | ❌ Failed: ${failed} | ⏭️ Excluded: ${skipped}`,
              { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "rf_cancel_request") }
            );
          } catch {}
        }
      }
    } else {
      // Full-number mode: iterate target phones, match participants
      for (let pi = 0; pi < targetPhones.length; pi++) {
        if (removeFriendCancelRequests.has(userIdNum)) { wasCancelled = true; break; }

        const phone = targetPhones[pi];
        const phoneLast10 = phone.slice(-10);

        const participant = participants.find(p => {
          const pPhone = p.phone.replace(/[^0-9]/g, "");
          return pPhone === phone || (phoneLast10.length >= 7 && pPhone.slice(-10) === phoneLast10);
        });

        if (!participant) {
          groupLines.push(`  ❌ +${phone} — Not found in group`);
          notFound++;
        } else if (participant.isAdmin) {
          // NEVER remove admins
          groupLines.push(`  ⚠️ +${phone} — Skipped (admin)`);
          skipped++;
        } else {
          const ok = await removeGroupParticipant(userId, group.id, participant.jid);
          if (ok) {
            groupLines.push(`  ✅ +${phone} — Removed`);
            removed++;
          } else {
            groupLines.push(`  ❌ +${phone} — Failed to remove`);
            failed++;
          }
        }

        // Live progress update every 3 numbers
        if ((pi % 3 === 0 || pi === targetPhones.length - 1) && !cancelDialogActiveFor.has(userIdNum)) {
          try {
            await bot.api.editMessageText(chatId, msgId,
              `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n` +
              `Processing: ${pi + 1}/${targetPhones.length}\n` +
              `✅ Removed: ${removed} | ⚠️ Skipped: ${skipped} | ❌ Not found: ${notFound} | ❌ Failed: ${failed}`,
              { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "rf_cancel_request") }
            );
          } catch {}
        }

        await new Promise(r => setTimeout(r, 600));
      }
    }

    const summary = `✅ Removed: ${removed} | ❌ Failed: ${failed}` +
      (notFound > 0 ? ` | ❌ Not found: ${notFound}` : "") +
      (skipped > 0 ? ` | ⏭️ Skipped: ${skipped}` : "");

    if (hasCountryCodeMode) {
      fullResult += `📋 <b>${esc(group.subject)}</b>\n${summary}\n\n`;
    } else {
      fullResult += `📋 <b>${esc(group.subject)}</b>\n${groupLines.join("\n")}\n${summary}\n\n`;
    }
    if (wasCancelled) break;
  }

  wasCancelled = removeFriendCancelRequests.has(userIdNum);
  removeFriendCancelRequests.delete(userIdNum);
  cancelDialogActiveFor.delete(userIdNum);
  activeBackgroundUsers.delete(userIdNum);
  schedulePostFeatureGC();

  if (wasCancelled) fullResult += `⛔ <b>Stopped by user.</b>\n\n`;
  fullResult += `━━━━━━━━━━━━━━━━━━\n✅ <b>Done!</b>`;

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

// ─── Remove Members (existing cancel flow) ───────────────────────────────────

bot.callbackQuery("rm_cancel_request", async (ctx) => {
  ctx.answerCallbackQuery();
  cancelDialogActiveFor.add(ctx.from.id);
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard()
      .text("✅ Yes, Stop Removing", "rm_cancel_confirm")
      .text("↩️ Continue", "rm_cancel_no"),
  });
});

bot.callbackQuery("rm_cancel_no", async (ctx) => {
  cancelDialogActiveFor.delete(ctx.from.id);
  ctx.answerCallbackQuery({ text: "Removing continued" });
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard().text("❌ Cancel", "rm_cancel_request"),
  });
});

bot.callbackQuery("rm_cancel_confirm", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Stopping after current member..." });
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
  activeBackgroundUsers.add(Number(userId));
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

  {
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
  ctx.answerCallbackQuery();
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
  kb.text("🔗 By Link", "ma_by_link").row();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" });
});

bot.callbackQuery("ma_select_all", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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

  {
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
  ctx.answerCallbackQuery();
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
  kb.text("🔗 By Link", "ap_by_link").row();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery({ text: "Previous/Next se 20 group per page dekhein" });
});

bot.callbackQuery("ap_select_all", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData?.targetPhones?.length) return;
  state.approvalData.makeAdminAfter = false;
  await showAdminApprovalReview(ctx, userId);
});

bot.callbackQuery("ap_admin_make", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  cancelDialogActiveFor.add(ctx.from.id);
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard()
      .text("✅ Yes, Stop Approving", "ap_cancel_confirm")
      .text("↩️ Continue", "ap_cancel_no"),
  });
});

bot.callbackQuery("ap_cancel_no", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Approval continued" });
  const userId = ctx.from.id;
  cancelDialogActiveFor.delete(userId);
  // If somehow the user already confirmed, don't put the Cancel button back.
  if (approvalCancelRequests.has(userId)) return;
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard().text("❌ Cancel", "ap_cancel_request"),
  });
});

bot.callbackQuery("ap_cancel_confirm", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Stopping after current member..." });
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
  activeBackgroundUsers.add(userIdNum);
  const progressMarkup = new InlineKeyboard().text("❌ Cancel", "ap_cancel_request");
  let fullResult = "✅ <b>Approve 1 by 1 Result</b>\n\n";
  const lines: string[] = [];
  let cancelled = false;

  outer: for (let gi = 0; gi < groups.length; gi++) {
    if (approvalCancelRequests.has(userIdNum)) { cancelled = true; break outer; }
    const group = groups[gi];

    try {
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
    } catch (groupErr: any) {
      lines.push(`📋 <b>${esc(group.subject)}</b>\n⚠️ Error processing group`);
    }
  }

  // Cleanup flags so the next run starts clean (and so any racing dialog
  // confirmation after this point is a no-op).
  approvalCancelRequests.delete(userIdNum);
  cancelDialogActiveFor.delete(userIdNum);
  activeBackgroundUsers.delete(userIdNum);
  schedulePostFeatureGC();

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
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData) return;

  const selectedGroups = Array.from(state.approvalData.selectedIndices).map(i => state.approvalData!.allGroups[i]);
  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  userStates.delete(userId);
  approvalCancelRequests.delete(userId);
  cancelDialogActiveFor.delete(userId);

  await ctx.editMessageText(
    `⏳ <b>Approving all pending members together...</b>\n\n⌛ Please wait...`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ap_cancel_request") }
  );

  void approveTogetherBackground(userId, String(userId), selectedGroups, chatId, msgId);
});

async function approveTogetherBackground(
  userIdNum: number,
  userId: string,
  groups: Array<{ id: string; subject: string }>,
  chatId: number,
  msgId: number
) {
  activeBackgroundUsers.add(userIdNum);
  const progressMarkup = new InlineKeyboard().text("❌ Cancel", "ap_cancel_request");
  let fullResult = "✅ <b>Approve Together Result</b>\n\n";
  const lines: string[] = [];
  let cancelled = false;

  for (let gi = 0; gi < groups.length; gi++) {
    if (approvalCancelRequests.has(userIdNum)) { cancelled = true; break; }
    const group = groups[gi];

    // ── Step 1: Turn OFF approval mode (retry up to 3x) ───────────────────
    try {
      const _vis1 = lines.length > 8 ? lines.slice(-8) : lines;
      const _hid1 = lines.length - _vis1.length;
      await bot.api.editMessageText(chatId, msgId,
        `✅ <b>Approve Together Result</b>\n\n` +
        (_hid1 > 0 ? `<i>... +${_hid1} groups done above (see final result)</i>\n\n` : "") +
        (_vis1.length > 0 ? _vis1.join("\n\n") + "\n\n" : "") +
        `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n` +
        `🔄 Step 1: Turning OFF approval mode...`,
        { parse_mode: "HTML", reply_markup: progressMarkup }
      );
    } catch {}

    let offOk = false;
    for (let attempt = 1; attempt <= 3 && !offOk; attempt++) {
      offOk = await setGroupApprovalMode(userId, group.id, "off");
      if (!offOk && attempt < 3) await new Promise((r) => setTimeout(r, 2000));
    }
    if (!offOk) {
      lines.push(`📋 <b>${esc(group.subject)}</b>\n⚠️ Failed to toggle approval mode (reason: WhatsApp rejected — may not be admin or group not found)`);
      continue;
    }

    await new Promise((r) => setTimeout(r, 2000));
    if (approvalCancelRequests.has(userIdNum)) { cancelled = true; break; }

    // ── Step 2: Turn ON approval mode (retry up to 3x) ────────────────────
    try {
      const _vis2 = lines.length > 8 ? lines.slice(-8) : lines;
      const _hid2 = lines.length - _vis2.length;
      await bot.api.editMessageText(chatId, msgId,
        `✅ <b>Approve Together Result</b>\n\n` +
        (_hid2 > 0 ? `<i>... +${_hid2} groups done above (see final result)</i>\n\n` : "") +
        (_vis2.length > 0 ? _vis2.join("\n\n") + "\n\n" : "") +
        `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n` +
        `🔄 Step 2: Turning ON approval mode...\n` +
        `✅ All pending members will be approved!`,
        { parse_mode: "HTML", reply_markup: progressMarkup }
      );
    } catch {}

    let onOk = false;
    for (let attempt = 1; attempt <= 3 && !onOk; attempt++) {
      onOk = await setGroupApprovalMode(userId, group.id, "on");
      if (!onOk && attempt < 3) await new Promise((r) => setTimeout(r, 2000));
    }
    if (!onOk) {
      lines.push(`📋 <b>${esc(group.subject)}</b>\n⚠️ Approval turned OFF but could not turn back ON (reason: WhatsApp rejected — check admin rights)`);
      continue;
    }

    await new Promise((r) => setTimeout(r, 1000));
    const total = await getGroupMemberCountSafe(userId, group.id);
    lines.push(`📋 <b>${esc(group.subject)}</b>\n✅ All pending members approved! | 👥 Total: ${total}`);

    // ── Show intermediate result so user sees series-wise progress ─────────
    try {
      const _vis3 = lines.length > 8 ? lines.slice(-8) : lines;
      const _hid3 = lines.length - _vis3.length;
      await bot.api.editMessageText(chatId, msgId,
        `✅ <b>Approve Together Result</b>\n\n` +
        (_hid3 > 0 ? `<i>... +${_hid3} groups done above (see final result)</i>\n\n` : "") +
        _vis3.join("\n\n") +
        (gi < groups.length - 1 ? `\n\n⏳ Processing group ${gi + 2}/${groups.length}...` : ""),
        { parse_mode: "HTML", reply_markup: progressMarkup }
      );
    } catch {}
  }

  approvalCancelRequests.delete(userIdNum);
  cancelDialogActiveFor.delete(userIdNum);
  activeBackgroundUsers.delete(userIdNum);
  schedulePostFeatureGC();

  if (cancelled) {
    fullResult = `🛑 <b>Approve Together — Cancelled</b>\n\n`;
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

async function makeAdminBackground(
  userIdNum: number,
  groups: Array<{ id: string; subject: string }>,
  phoneNumbers: string[],
  chatId: number,
  msgId: number
) {
  activeBackgroundUsers.add(userIdNum);
  const userId = String(userIdNum);
  let fullResult = "👑 <b>Make Admin Result</b>\n\n";
  const lines: string[] = [];
  let wasCancelled = false;

  for (let gi = 0; gi < groups.length; gi++) {
    if (makeAdminCancelRequests.has(userIdNum)) {
      wasCancelled = true;
      break;
    }
    const group = groups[gi];
    const groupLines: string[] = [];
    let madeAdmin = 0, notFound = 0, failed = 0;

    try {
      if (msgId && !cancelDialogActiveFor.has(userIdNum)) {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n⌛ Processing ${phoneNumbers.length} number(s)...`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ma_cancel_request") }
        );
      }
    } catch {}

    for (let pi = 0; pi < phoneNumbers.length; pi++) {
      if (makeAdminCancelRequests.has(userIdNum)) {
        wasCancelled = true;
        break;
      }
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
          if (msgId && !cancelDialogActiveFor.has(userIdNum)) {
            await bot.api.editMessageText(chatId, msgId,
              `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n` +
              `Processing: ${pi + 1}/${phoneNumbers.length}\n` +
              `✅ Admin: ${madeAdmin} | ❌ Not found: ${notFound} | ❌ Failed: ${failed}`,
              { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ma_cancel_request") }
            );
          }
        } catch {}
      }

      await new Promise((r) => setTimeout(r, 500));
    }

    if (wasCancelled) break;
    lines.push(`📋 <b>${esc(group.subject)}</b>\n${groupLines.join("\n")}\n✅ Admin: ${madeAdmin} | ❌ Not found: ${notFound} | ❌ Failed: ${failed}`);
  }

  makeAdminCancelRequests.delete(userIdNum);
  cancelDialogActiveFor.delete(userIdNum);
  schedulePostFeatureGC();

  fullResult += lines.join("\n\n");
  if (wasCancelled) {
    fullResult += `\n\n⛔ <b>Process cancelled by user after ${lines.length}/${groups.length} group(s).</b>`;
  } else {
    fullResult += `\n\n━━━━━━━━━━━━━━━━━━\n✅ <b>Done processing ${groups.length} group(s)!</b>`;
  }

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

bot.callbackQuery("ma_cancel_request", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (cancelDialogActiveFor.has(userId)) return;
  cancelDialogActiveFor.add(userId);
  try {
    await ctx.editMessageReplyMarkup({
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Cancel", "ma_cancel_confirm")
        .text("🔙 No, Continue", "ma_cancel_abort"),
    });
  } catch {}
});

bot.callbackQuery("ma_cancel_confirm", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Cancelling..." });
  const userId = ctx.from.id;
  makeAdminCancelRequests.add(userId);
  cancelDialogActiveFor.delete(userId);
  try {
    await ctx.editMessageReplyMarkup({ reply_markup: new InlineKeyboard() });
  } catch {}
});

bot.callbackQuery("ma_cancel_abort", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Continuing..." });
  const userId = ctx.from.id;
  cancelDialogActiveFor.delete(userId);
  try {
    await ctx.editMessageReplyMarkup({
      reply_markup: new InlineKeyboard().text("❌ Cancel", "ma_cancel_request"),
    });
  } catch {}
});


// ─── Manage Sessions ─────────────────────────────────────────────────────────

bot.callbackQuery("manage_sessions", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const connected = isConnected(String(userId));
  await ctx.editMessageText(
    "⚙️ <b>Manage Sessions</b>\n\n" +
    (connected
      ? "✅ WhatsApp is connected. Choose an option below:"
      : "⚠️ WhatsApp is not connected.\n\nYou can still manage your saved WhatsApp accounts."),
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🔀 Switch WhatsApp", "switch_wa_menu").row()
        .text("🔄 Session Refresh", "session_refresh").row()
        .text("🏠 Main Menu", "main_menu"),
    }
  );
});

// ─── Switch WhatsApp Menu ────────────────────────────────────────────────────

async function showSwitchWaMenu(ctx: any, userId: number): Promise<void> {
  const uid = String(userId);
  const profile = await loadWaSwitchProfile(userId);
  const activeId = profile?.activeId ?? uid;
  const kb = new InlineKeyboard();

  if (profile && profile.slots.length > 0) {
    for (const slot of profile.slots) {
      const isActive = slot.id === activeId;
      const connMark = (isConnected(slot.id) || (isActive && isConnected(uid))) ? "🟢" : "🔴";
      const activeMark = isActive ? " ✅" : "";
      kb.text(`${connMark} ${slot.phone}${activeMark}`, `switch_wa:${slot.id}`)
        .text("🗑️", `switch_wa_remove_confirm:${slot.id}`)
        .row();
    }
  }

  kb.text("➕ Add New WhatsApp", "switch_wa_add").row();
  kb.text("🔙 Back", "manage_sessions");

  const slotCount = profile?.slots.length ?? 0;
  const text =
    "🔀 <b>Switch WhatsApp</b>\n\n" +
    (slotCount > 0
      ? `You have <b>${slotCount}</b> saved WhatsApp account(s).\n\n` +
        "🟢 = Connected  🔴 = Disconnected  ✅ = Currently Active\n\n" +
        "Tap a number to switch to it, or tap 🗑️ to remove it."
      : "No saved WhatsApp accounts yet.\n\nTap <b>Add New WhatsApp</b> to add one.");

  try {
    await ctx.editMessageText(text, { parse_mode: "HTML", reply_markup: kb });
  } catch {
    await ctx.reply(text, { parse_mode: "HTML", reply_markup: kb });
  }
}

bot.callbackQuery("switch_wa_menu", async (ctx) => {
  ctx.answerCallbackQuery();
  await showSwitchWaMenu(ctx, ctx.from.id);
});

// ─── Add New WhatsApp (Switch) ───────────────────────────────────────────────

bot.callbackQuery("switch_wa_add", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  userStates.set(userId, { step: "switch_wa_phone" });
  await ctx.editMessageText(
    "➕ <b>Add New WhatsApp</b>\n\n" +
    "Enter the phone number of the WhatsApp account you want to add:\n\n" +
    "Example: <code>+919876543210</code>",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("❌ Cancel", "switch_wa_menu"),
    }
  );
});

// ─── Switch to a specific WhatsApp slot ─────────────────────────────────────

bot.callbackQuery(/^switch_wa:(.+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const uid = String(userId);
  const targetSlotId = ctx.match[1];

  const profile = await loadWaSwitchProfile(userId);
  if (!profile) {
    await ctx.editMessageText("❌ No saved WhatsApp accounts found.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }

  const slot = profile.slots.find((s) => s.id === targetSlotId);
  if (!slot) {
    await ctx.editMessageText("❌ WhatsApp account not found.", {
      reply_markup: new InlineKeyboard().text("🔀 Switch WhatsApp", "switch_wa_menu"),
    });
    return;
  }

  const currentActiveId = profile.activeId;
  if (currentActiveId === targetSlotId) {
    await ctx.editMessageText(
      `✅ <b>${esc(slot.phone)}</b> is already your active WhatsApp.\n\n` +
      "Use Session Refresh to reload the latest data.",
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("🔄 Session Refresh", "session_refresh")
          .text("🔀 Switch WhatsApp", "switch_wa_menu")
          .row()
          .text("🏠 Main Menu", "main_menu"),
      }
    );
    return;
  }

  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  try {
    await bot.api.editMessageText(chatId, msgId,
      `🔄 <b>Switching WhatsApp...</b>\n\n` +
      `📱 Switching to: <code>${esc(slot.phone)}</code>\n\n` +
      `⌛ Disconnecting current session...`,
      { parse_mode: "HTML" }
    );
  } catch {}

  // Step 1: Close socket only — do NOT call disconnectWhatsApp which deletes
  // MongoDB creds. idleDisconnectWhatsApp frees RAM but keeps the saved session
  // so ensureSessionLoaded can restore it from MongoDB in Step 4.
  try {
    // Close the currently active slot's socket (memory-only, keeps Mongo creds)
    await idleDisconnectWhatsApp(currentActiveId);
    // Also evict any stale socket stored under the raw uid (pre-fix residue)
    await idleDisconnectWhatsApp(uid);
    await new Promise((r) => setTimeout(r, 500));
  } catch {}

  // Step 2: Point primary userId → new slot via alias.
  setSessionAlias(uid, targetSlotId);

  // Step 3: Persist active slot in MongoDB.
  profile.activeId = targetSlotId;
  await saveWaSwitchProfile(profile);

  // Invalidate session cache so ensureSessionLoaded sees the new slot.
  hasSessionCache.del(uid);
  hasSessionCache.del(targetSlotId);

  try {
    await bot.api.editMessageText(chatId, msgId,
      `🔄 <b>Switching WhatsApp...</b>\n\n` +
      `📱 Switching to: <code>${esc(slot.phone)}</code>\n\n` +
      `🌐 Connecting to WhatsApp servers...`,
      { parse_mode: "HTML" }
    );
  } catch {}

  // Step 4: Load target session from MongoDB and wait until WA handshake
  // completes. ensureSessionLoaded only starts the socket; the actual
  // authentication happens asynchronously, so we must poll with
  // waitForWhatsAppConnected (up to 35s) to know if it really connected.
  let switchConnected = false;
  try {
    switchConnected = await waitForWhatsAppConnected(targetSlotId, { timeoutMs: 35_000, pollMs: 500 });
  } catch {}

  if (switchConnected) {
    // ── onConnected ─────────────────────────────────────────────────────────
    try {
      await bot.api.editMessageText(chatId, msgId,
        `✅ <b>WhatsApp Switched Successfully!</b>\n\n` +
        `📱 Now using: <code>${esc(slot.phone)}</code>\n\n` +
        "All bot features will now use this WhatsApp account. " +
        "This is saved — even after a bot restart, this account will be used.",
        {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("🔀 Switch Again", "switch_wa_menu")
            .text("🏠 Main Menu", "main_menu"),
        }
      );
    } catch {}
  } else {
    // ── onError ─────────────────────────────────────────────────────────────
    // Connection failed — restore alias to the previous active slot so the
    // user's primary WA keeps working. Do NOT remove the slot from the profile
    // because the creds are still in MongoDB; user can retry by switching again.
    try {
      setSessionAlias(uid, currentActiveId ?? uid);
      const freshProfile = await loadWaSwitchProfile(userId);
      if (freshProfile) {
        freshProfile.activeId = currentActiveId ?? uid;
        await saveWaSwitchProfile(freshProfile);
      }
    } catch {}
    try {
      await bot.api.editMessageText(chatId, msgId,
        `❌ <b>Switch Failed</b>\n\n` +
        `📱 <code>${esc(slot.phone)}</code> could not connect right now.\n\n` +
        "Your previous WhatsApp is still active. " +
        "The account is saved — tap Switch WhatsApp to try again.",
        {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("🔀 Switch WhatsApp", "switch_wa_menu")
            .text("🏠 Main Menu", "main_menu"),
        }
      );
    } catch {}
  }
});

// ─── Remove WhatsApp slot — confirm ─────────────────────────────────────────

bot.callbackQuery(/^switch_wa_remove_confirm:(.+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const uid = String(userId);
  const slotId = ctx.match[1];

  const profile = await loadWaSwitchProfile(userId);
  const slot = profile?.slots.find((s) => s.id === slotId);
  if (!slot) {
    await ctx.editMessageText("❌ WhatsApp account not found.", {
      reply_markup: new InlineKeyboard().text("🔀 Switch WhatsApp", "switch_wa_menu"),
    });
    return;
  }

  const isActive = profile!.activeId === slotId;
  await ctx.editMessageText(
    `🗑️ <b>Remove WhatsApp?</b>\n\n` +
    `📱 Number: <code>${esc(slot.phone)}</code>\n` +
    (isActive ? "\n⚠️ This is your <b>currently active</b> WhatsApp. Removing it will switch back to your original account.\n" : "") +
    "\nThis removes the number from your switch list. " +
    "The saved WhatsApp session in MongoDB is kept — you can re-add this number anytime.\n\nAre you sure?",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Remove", `switch_wa_remove:${slotId}`)
        .text("❌ Cancel", "switch_wa_menu"),
    }
  );
});

// ─── Remove WhatsApp slot — execute ─────────────────────────────────────────

bot.callbackQuery(/^switch_wa_remove:(.+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const uid = String(userId);
  const slotId = ctx.match[1];

  const profile = await loadWaSwitchProfile(userId);
  if (!profile) {
    await ctx.editMessageText("❌ No saved accounts found.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }

  const slot = profile.slots.find((s) => s.id === slotId);
  if (!slot) {
    await ctx.editMessageText("❌ WhatsApp account not found.", {
      reply_markup: new InlineKeyboard().text("🔀 Switch WhatsApp", "switch_wa_menu"),
    });
    return;
  }

  const wasActive = profile.activeId === slotId;
  profile.slots = profile.slots.filter((s) => s.id !== slotId);

  if (wasActive) {
    // Switch back to the first remaining slot (prefer original uid slot)
    const fallback = profile.slots.find((s) => s.id === uid) ?? profile.slots[0];
    const newActiveId = fallback?.id ?? uid;
    profile.activeId = newActiveId;
    setSessionAlias(uid, newActiveId);
    void disconnectWhatsApp(slotId).catch(() => {});
    ensureSessionLoaded(newActiveId).catch(() => {});
  }

  await saveWaSwitchProfile(profile);

  await ctx.editMessageText(
    `✅ <b>WhatsApp Removed</b>\n\n` +
    `<code>${esc(slot.phone)}</code> has been removed from your switch list.` +
    (wasActive ? "\n\n🔄 Switched back to your previous account." : ""),
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("🔀 Switch WhatsApp", "switch_wa_menu"),
    }
  );
});

// ─── Session Refresh ─────────────────────────────────────────────────────────

bot.callbackQuery("session_refresh", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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

// ─── Reset Link Feature ──────────────────────────────────────────────────────

const RL_PAGE_SIZE = 20;

function buildResetLinkKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const allGroups = state.resetLinkData!.allGroups;
  const selected = state.resetLinkData!.selectedIndices;
  const page = state.resetLinkData!.page || 0;
  const totalPages = Math.max(1, Math.ceil(allGroups.length / RL_PAGE_SIZE));
  const start = page * RL_PAGE_SIZE;
  const end = Math.min(start + RL_PAGE_SIZE, allGroups.length);

  for (let i = start; i < end; i++) {
    const g = allGroups[i];
    const isSelected = selected.has(i);
    const label = isSelected ? `✅ ${g.subject}` : `☐ ${g.subject}`;
    kb.text(label, `rl_tog_${i}`).row();
  }

  const prev = page > 0 ? "⬅️ Previous 20" : " ";
  const next = page < totalPages - 1 ? "Next 20 ➡️" : " ";
  kb.text(prev, "rl_prev_page").text(`📄 ${page + 1}/${totalPages}`, "rl_page_info").text(next, "rl_next_page").row();

  if (allGroups.length > 1) {
    kb.text("☑️ Select All", "rl_select_all").text("🗑️ Clear All", "rl_clear_all").row();
  }
  if (selected.size > 0) {
    kb.text(`▶️ Reset Links (${selected.size} groups)`, "rl_proceed").row();
  }
  kb.text("🏠 Back", "main_menu");
  return kb;
}

bot.callbackQuery("reset_link", async (ctx) => {
  ctx.answerCallbackQuery();
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
    step: "reset_link_menu",
    resetLinkData: {
      allGroups: adminGroupsSimple,
      patterns,
      selectedIndices: new Set(),
      page: 0,
    },
  });

  const kb = new InlineKeyboard();
  if (patterns.length > 0) kb.text("🔍 Similar Groups", "rl_similar").text("📋 All Groups", "rl_show_all").row();
  else kb.text("📋 All Groups", "rl_show_all").row();
  kb.text("🔗 Reset by Group Link", "rl_by_link").row();
  kb.text("🏠 Main Menu", "main_menu");

  await ctx.editMessageText(
    `🔗 <b>Reset Group Invite Links</b>\n\n` +
    `📊 Admin Groups: ${adminGroups.length} (Total: ${allGroups.length})\n` +
    (patterns.length > 0 ? `🔍 Similar Patterns: ${patterns.length}\n` : "") +
    `\n⚠️ This will <b>revoke</b> existing links and generate new ones.\n\n📌 Choose an option:`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

const RL_SIM_PAGE_SIZE = 10;

function buildRlSimilarKeyboard(patterns: SimilarGroup[], page: number): InlineKeyboard {
  const kb = new InlineKeyboard();
  const totalPages = Math.max(1, Math.ceil(patterns.length / RL_SIM_PAGE_SIZE));
  const start = page * RL_SIM_PAGE_SIZE;
  const end = Math.min(start + RL_SIM_PAGE_SIZE, patterns.length);
  for (let i = start; i < end; i++) {
    kb.text(`📌 ${patterns[i].base} (${patterns[i].groups.length} groups)`, `rl_sim_${i}`).row();
  }
  if (totalPages > 1) {
    const prev = page > 0 ? "⬅️ Previous" : " ";
    const next = page < totalPages - 1 ? "Next ➡️" : " ";
    kb.text(prev, "rl_sim_prev_page").text(`📄 ${page + 1}/${totalPages}`, "rl_sim_page_info").text(next, "rl_sim_next_page").row();
  }
  kb.text("🔙 Back", "reset_link").text("🏠 Menu", "main_menu");
  return kb;
}

bot.callbackQuery("rl_similar", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.resetLinkData) return;

  const { patterns } = state.resetLinkData;
  if (!patterns.length) {
    await ctx.editMessageText("⚠️ No similar group patterns found.", {
      reply_markup: new InlineKeyboard().text("🔙 Back", "reset_link").text("🏠 Menu", "main_menu"),
    }); return;
  }

  state.resetLinkData.patternPage = 0;
  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select those groups:",
    { parse_mode: "HTML", reply_markup: buildRlSimilarKeyboard(patterns, 0) }
  );
});

bot.callbackQuery("rl_sim_prev_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.resetLinkData) return;
  const current = state.resetLinkData.patternPage || 0;
  if (current > 0) state.resetLinkData.patternPage = current - 1;
  const page = state.resetLinkData.patternPage || 0;
  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select those groups:",
    { parse_mode: "HTML", reply_markup: buildRlSimilarKeyboard(state.resetLinkData.patterns, page) }
  );
});

bot.callbackQuery("rl_sim_next_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.resetLinkData) return;
  const totalPages = Math.ceil(state.resetLinkData.patterns.length / RL_SIM_PAGE_SIZE);
  const current = state.resetLinkData.patternPage || 0;
  if (current < totalPages - 1) state.resetLinkData.patternPage = current + 1;
  const page = state.resetLinkData.patternPage || 0;
  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select those groups:",
    { parse_mode: "HTML", reply_markup: buildRlSimilarKeyboard(state.resetLinkData.patterns, page) }
  );
});

bot.callbackQuery("rl_sim_page_info", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Use Previous / Next to change page" });
});

bot.callbackQuery(/^rl_sim_(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.resetLinkData) return;

  const idx = parseInt(ctx.match![1]);
  const pattern = state.resetLinkData.patterns[idx];
  if (!pattern) return;

  const patternIds = new Set(pattern.groups.map(g => g.id));
  state.resetLinkData.selectedIndices = new Set();
  for (let i = 0; i < state.resetLinkData.allGroups.length; i++) {
    if (patternIds.has(state.resetLinkData.allGroups[i].id)) {
      state.resetLinkData.selectedIndices.add(i);
    }
  }
  state.step = "reset_link_select";
  state.resetLinkData.page = 0;
  await ctx.editMessageText(
    `🔗 <b>Reset Link</b>\n\n${state.resetLinkData.allGroups.length} admin group(s)\n\n<i>${state.resetLinkData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildResetLinkKeyboard(state) }
  );
});

bot.callbackQuery("rl_show_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.resetLinkData) return;
  state.step = "reset_link_select";
  state.resetLinkData.page = 0;
  await ctx.editMessageText(
    `🔗 <b>Reset Link</b>\n\n${state.resetLinkData.allGroups.length} admin group(s)\n\nSelect groups to reset their invite links:`,
    { parse_mode: "HTML", reply_markup: buildResetLinkKeyboard(state) }
  );
});

bot.callbackQuery(/^rl_tog_(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.resetLinkData) return;
  const idx = parseInt(ctx.match![1]);
  if (idx < 0 || idx >= state.resetLinkData.allGroups.length) return;
  if (state.resetLinkData.selectedIndices.has(idx)) {
    state.resetLinkData.selectedIndices.delete(idx);
  } else {
    state.resetLinkData.selectedIndices.add(idx);
  }
  await ctx.editMessageText(
    `🔗 <b>Reset Link</b>\n\n${state.resetLinkData.allGroups.length} admin group(s)\n\n<i>${state.resetLinkData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildResetLinkKeyboard(state) }
  );
});

bot.callbackQuery("rl_prev_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.resetLinkData) return;
  if (state.resetLinkData.page > 0) state.resetLinkData.page--;
  await ctx.editMessageText(
    `🔗 <b>Reset Link</b>\n\n${state.resetLinkData.allGroups.length} admin group(s)\n\n<i>${state.resetLinkData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildResetLinkKeyboard(state) }
  );
});

bot.callbackQuery("rl_next_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.resetLinkData) return;
  const totalPages = Math.ceil(state.resetLinkData.allGroups.length / RL_PAGE_SIZE);
  if (state.resetLinkData.page < totalPages - 1) state.resetLinkData.page++;
  await ctx.editMessageText(
    `🔗 <b>Reset Link</b>\n\n${state.resetLinkData.allGroups.length} admin group(s)\n\n<i>${state.resetLinkData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildResetLinkKeyboard(state) }
  );
});

bot.callbackQuery("rl_page_info", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" });
});

bot.callbackQuery("rl_select_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.resetLinkData) return;
  for (let i = 0; i < state.resetLinkData.allGroups.length; i++) state.resetLinkData.selectedIndices.add(i);
  await ctx.editMessageText(
    `🔗 <b>Reset Link</b>\n\nAll <b>${state.resetLinkData.allGroups.length} groups selected</b>`,
    { parse_mode: "HTML", reply_markup: buildResetLinkKeyboard(state) }
  );
});

bot.callbackQuery("rl_clear_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.resetLinkData) return;
  state.resetLinkData.selectedIndices.clear();
  await ctx.editMessageText(
    `🔗 <b>Reset Link</b>\n\n${state.resetLinkData.allGroups.length} admin group(s)\n\n<i>None selected</i>`,
    { parse_mode: "HTML", reply_markup: buildResetLinkKeyboard(state) }
  );
});

bot.callbackQuery("rl_proceed", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.resetLinkData || state.resetLinkData.selectedIndices.size === 0) return;

  const selectedGroups = Array.from(state.resetLinkData.selectedIndices).map(i => state.resetLinkData!.allGroups[i]);

  await ctx.editMessageText(
    `🔗 <b>Reset Invite Links — Confirm</b>\n\n` +
    `✅ <b>${selectedGroups.length} group(s) selected</b>\n\n` +
    `⚠️ <b>Current invite links will be revoked.</b>\n` +
    `Anyone using the old link will no longer be able to join.\n\n` +
    `Do you want to proceed?`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Reset Links", "rl_proceed_confirm")
        .text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("rl_proceed_confirm", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.resetLinkData || state.resetLinkData.selectedIndices.size === 0) return;

  const selectedGroups = Array.from(state.resetLinkData.selectedIndices).map(i => state.resetLinkData!.allGroups[i]);
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;
  userStates.delete(userId);
  resetLinkCancelRequests.delete(userId);

  void resetLinkBackground(userId, selectedGroups, chatId, msgId);
});

bot.callbackQuery("rl_by_link", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.resetLinkData) return;
  state.step = "rl_enter_links";
  state.rlLinkBuffer = [];
  rlLinkCollectMsgId.delete(userId);

  await ctx.editMessageText(
    "🔗 <b>Reset by Group Link</b>\n\n" +
    "Send WhatsApp group invite links (one per message or multiple at once):\n" +
    "<code>https://chat.whatsapp.com/ABC123</code>\n\n" +
    "⚠️ You must be an admin in those groups.\n\n" +
    "<i>The <b>Done</b> button will appear after you send at least one link.</i>",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("rl_link_done", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state || state.step !== "rl_enter_links") return;

  const buffer = state.rlLinkBuffer || [];
  if (!buffer.length) {
    ctx.answerCallbackQuery({ text: "❌ Please send at least one link first!" });
    return;
  }

  rlLinkCollectMsgId.delete(userId);
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;

  await ctx.editMessageText(
    `🔗 <b>Reset by Group Link — Confirm</b>\n\n` +
    `📎 <b>${buffer.length} link(s) collected</b>\n\n` +
    `The bot will resolve these links and reset their invite links.\n\n` +
    `⚠️ <b>Current invite links will be revoked.</b> Anyone using the old link will no longer be able to join.\n\n` +
    `Do you want to proceed?`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Reset Links", "rl_link_pipeline_start")
        .text("❌ Cancel", "main_menu"),
    }
  );
  state.step = "rl_link_confirm";
  state.rlLinkBuffer = buffer;
  // Save chatId/msgId for the pipeline
  if (!state.resetLinkData) state.resetLinkData = { allGroups: [], patterns: [], selectedIndices: new Set(), page: 0 };
  (state as any)._rlPipelineChatId = chatId;
  (state as any)._rlPipelineMsgId = msgId;
});

bot.callbackQuery("rl_link_pipeline_start", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state || !state.rlLinkBuffer?.length) return;

  const links = [...state.rlLinkBuffer];
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;

  userStates.delete(userId);
  resetLinkCancelRequests.delete(userId);

  void runRlResolvePipelineBackground(userId, links, chatId, msgId);
});

async function runRlResolvePipelineBackground(
  userIdNum: number,
  links: string[],
  chatId: number,
  msgId: number
): Promise<void> {
  const userId = String(userIdNum);
  const total = links.length;
  let done = 0;
  let resetOk = 0;
  let wasCancelled = false;
  const results: Array<{ subject: string; newLink?: string; resolveErr?: string; resetErr?: string }> = [];

  // Track start time for ETA calculation
  const startedAt = Date.now();

  const buildProgress = () => {
    const pct = total === 0 ? 0 : Math.round((done / total) * 100);
    const filled = Math.round((done / Math.max(total, 1)) * 20);
    const bar = `[${"█".repeat(filled)}${"░".repeat(20 - filled)}] ${pct}% (${done}/${total})`;

    // ETA: only show after we've processed at least 3 links
    let etaLine = "";
    if (done >= 3 && done < total) {
      const elapsed = Date.now() - startedAt;
      const msPerLink = elapsed / done;
      const remaining = total - done;
      const etaMs = msPerLink * remaining;
      const etaMins = Math.floor(etaMs / 60000);
      const etaSecs = Math.round((etaMs % 60000) / 1000);
      etaLine = etaMins > 0
        ? `\n⏱ <b>~${etaMins}m ${etaSecs}s remaining</b>`
        : `\n⏱ <b>~${etaSecs}s remaining</b>`;
    }

    return (
      `⏳ <b>Resolving & Resetting Links...</b>\n\n` +
      `${bar}${etaLine}\n\n` +
      `✅ Reset: <b>${resetOk}</b> | ❌ Failed: <b>${done - resetOk}</b>\n` +
      (done < total ? `⌛ <b>${total - done}</b> remaining...` : `⏳ Finishing up...`)
    );
  };

  // Protect session from idle-disconnect for the entire run duration
  resetLinkActiveSessions.add(userIdNum);
  protectSessionFromEviction(userId);
  markUserActive(userIdNum);

  try {
    await bot.api.editMessageText(chatId, msgId, buildProgress(), {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("❌ Cancel", "rl_cancel_request"),
    });
  } catch {}

  // Dynamic delays based on total link count — fewer links = shorter pauses
  // to avoid unnecessary waiting when the user has a small list.
  // More links = longer pauses because WhatsApp rate-limits harder on bulk calls.
  //
  // Links   | BATCH_SIZE | Pause/batch | Per-link delay
  // --------|------------|-------------|---------------
  // ≤ 50    |     15     |    8s       |    2.0s
  // ≤ 100   |     12     |   10s       |    2.5s
  // ≤ 200   |     10     |   15s       |    3.0s
  // ≤ 500   |     10     |   20s       |    3.5s
  // > 500   |     10     |   25s       |    4.0s
  // Conservative dynamic delays — tuned so WhatsApp never hits rate limit
  // regardless of list size. Larger lists use longer pauses because WA
  // rate-limit windows are cumulative across the whole session.
  //
  // Links   | Batch | Pause/batch | Per-link | Fail extra
  // --------|-------|-------------|----------|----------
  // ≤ 50    |  15   |    10s      |   3s     |   8s
  // ≤ 100   |  12   |    15s      |   4s     |   8s
  // ≤ 200   |  10   |    20s      |   5s     |  10s
  // ≤ 500   |  10   |    25s      |   6s     |  12s
  // > 500   |   8   |    30s      |   8s     |  15s
  // Delays tuned to hit these targets (no rate limits):
  //
  // Links | Batch | Pause/batch | Per-link | ~Total time
  // ------|-------|-------------|----------|------------
  // ≤ 50  |  15   |    8s       |   2.5s   |  ~3-4 min
  // ≤ 100 |  12   |   12s       |   3.5s   |  ~7-8 min
  // ≤ 200 |  10   |   15s       |   4.0s   | ~16-17 min
  // ≤ 500 |  10   |   20s       |   4.6s   | ~54-56 min ✅
  // > 500 |  10   |   25s       |   5.5s   | ~115-120 min
  const BATCH_SIZE    = total <= 50  ? 50
                      : total <= 100 ? 50
                      : 50;
  const BATCH_PAUSE_MS = total <= 50  ?   500
                       : total <= 100 ?   800
                       : total <= 200 ?  1000
                       : total <= 500 ?  1500
                       : 2000;
  const INTER_LINK_MS = total <= 50  ?  100
                       : total <= 100 ?  150
                       : total <= 200 ?  200
                       : total <= 500 ?  250
                       : 300;
  const FAIL_LINK_MS  = total <= 200 ?   500
                      : total <= 500 ?   800
                      : 1000;

  for (let i = 0; i < links.length; i++) {
    if (resetLinkCancelRequests.has(userIdNum)) { wasCancelled = true; break; }

    // Pause every BATCH_SIZE links to let WhatsApp recover, also bump keepalive
    if (i > 0 && i % BATCH_SIZE === 0) {
      markUserActive(userIdNum); // Reset idle timer during long runs
      try {
        await bot.api.editMessageText(chatId, msgId,
          buildProgress(),
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "rl_cancel_request") }
        );
      } catch {}
      await new Promise(r => setTimeout(r, BATCH_PAUSE_MS));
      if (resetLinkCancelRequests.has(userIdNum)) { wasCancelled = true; break; }
    }

    const link = links[i];

    // Step 1: Resolve the link — up to 3 retries with increasing delays
    // so transient WA errors never permanently fail a link.
    let info = await getGroupIdFromLink(userId, link);

    // Silent retries for resolve — keep trying until success, no error shown to user
    const resolveWaits = [500, 800, 1000, 1200, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000];
    for (const wait of resolveWaits) {
      if (info || resetLinkCancelRequests.has(userIdNum)) break;
      await new Promise(r => setTimeout(r, wait));
      info = await getGroupIdFromLink(userId, link);
    }

    if (!info) {
      // All retries exhausted — skip silently, show generic skipped message
      results.push({ subject: link, resolveErr: "Skipped" });
      done++;
      try { await bot.api.editMessageText(chatId, msgId, buildProgress(), { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "rl_cancel_request") }); } catch {}
      if (i < links.length - 1) await new Promise(r => setTimeout(r, FAIL_LINK_MS));
      continue;
    }

    // Step 2: Reset the invite link — retry on transient errors
    let res = await resetGroupInviteLink(userId, info.id);
    if (!res.success && res.error) {
      const errLower = res.error.toLowerCase();
      const isTransient =
        errLower.includes("rate") ||
        errLower.includes("busy") ||
        errLower.includes("conflict") ||
        errLower.includes("timeout") ||
        errLower.includes("503") ||
        errLower.includes("429") ||
        errLower.includes("500") ||
        errLower.includes("owner") ||
        errLower.includes("not-authorized") ||
        errLower.includes("forbidden") ||
        errLower.includes("405");
      if (isTransient && !resetLinkCancelRequests.has(userIdNum)) {
        // Silent retries — keep trying until success, no error shown to user
        const resetWaits = [500, 800, 1000, 1200, 1500, 2000, 2500, 3000, 3500, 4000];
        for (const wait of resetWaits) {
          if (res.success || resetLinkCancelRequests.has(userIdNum)) break;
          await new Promise(r => setTimeout(r, wait));
          res = await resetGroupInviteLink(userId, info.id);
        }
      }
    }

    if (res.success && res.newLink) {
      results.push({ subject: info.subject, newLink: res.newLink });
      resetOk++;
    } else {
      results.push({ subject: info.subject, resetErr: "Could not reset" });
    }
    done++;
    try { await bot.api.editMessageText(chatId, msgId, buildProgress(), { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "rl_cancel_request") }); } catch {}
    if (i < links.length - 1) await new Promise(r => setTimeout(r, INTER_LINK_MS));
  }

  resetLinkCancelRequests.delete(userIdNum);
  cancelDialogActiveFor.delete(userIdNum);
  // Release session protection — allow normal idle-disconnect again
  resetLinkActiveSessions.delete(userIdNum);
  unprotectSession(userId);
  markUserActive(userIdNum);

  const successResults = results.filter(r => r.newLink);
  const failedResults = results.filter(r => !r.newLink);

  let resultText = `🔗 <b>Reset by Link — Result</b>\n📊 <b>Total: ${successResults.length} link(s) reset successfully</b>\n\n`;
  if (wasCancelled) resultText += `⛔ <b>Cancelled after ${done}/${total}.</b>\n\n`;

  for (const r of successResults) {
    resultText += `✅ <b>${esc(r.subject)}</b>\n${r.newLink}\n\n`;
  }
  if (failedResults.length > 0) {
    resultText += `━━━━━━━━━━━━━━━━━━
⚠️ <b>Could Not Reset (${failedResults.length}):</b>
`;
    for (const r of failedResults) {
      resultText += `❌ <b>${esc(r.subject)}</b>
`;
    }
    resultText += "\n";
  }

  // Cache links for optional .txt download (valid 15 min)
  const showDownload = successResults.length >= 50;
  if (showDownload) {
    const linksText = successResults.map(r => `${r.subject}\n${r.newLink}`).join("\n\n");
    resetLinkDownloadCache.set(userIdNum, { text: linksText, expiresAt: Date.now() + 15 * 60 * 1000 });
  }

  // Cache failed links for retry (valid 30 min)
  // Collect the original links that failed (resolve failures + reset failures)
  const failedOriginalLinks: string[] = [];
  for (let i = 0; i < links.length; i++) {
    const r = results[i];
    if (r && !r.newLink) failedOriginalLinks.push(links[i]);
  }
  const showRetry = failedOriginalLinks.length > 0;
  if (showRetry) {
    rlLinkRetryCache.set(userIdNum, { links: failedOriginalLinks, expiresAt: Date.now() + 30 * 60 * 1000 });
  }

  const chunks = splitMessage(resultText, 4000);
  const lastKb = (() => {
    const kb = new InlineKeyboard();
    if (showDownload) kb.text("📥 Download Links (.txt)", "rl_download");
    if (showRetry) kb.text("🔄 Retry Failed Links", "rl_link_retry");
    kb.row().text("🏠 Main Menu", "main_menu");
    return kb;
  })();

  try {
    await bot.api.editMessageText(chatId, msgId, chunks[0], {
      parse_mode: "HTML",
      reply_markup: chunks.length === 1 ? lastKb : undefined,
    });
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(chatId, chunks[i], {
      parse_mode: "HTML",
      reply_markup: i === chunks.length - 1 ? lastKb : undefined,
    });
  }
}

bot.callbackQuery("rl_cancel_request", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (cancelDialogActiveFor.has(userId)) return;
  cancelDialogActiveFor.add(userId);
  try {
    await ctx.editMessageReplyMarkup({
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Cancel", "rl_cancel_confirm")
        .text("🔙 No, Continue", "rl_cancel_abort"),
    });
  } catch {}
});

bot.callbackQuery("rl_cancel_confirm", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Cancelling..." });
  const userId = ctx.from.id;
  resetLinkCancelRequests.add(userId);
  cancelDialogActiveFor.delete(userId);
  try { await ctx.editMessageReplyMarkup({ reply_markup: new InlineKeyboard() }); } catch {}
});

bot.callbackQuery("rl_cancel_abort", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Continuing..." });
  cancelDialogActiveFor.delete(ctx.from.id);
  try {
    await ctx.editMessageReplyMarkup({
      reply_markup: new InlineKeyboard().text("❌ Cancel", "rl_cancel_request"),
    });
  } catch {}
});

bot.callbackQuery("rl_download", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Preparing file..." });
  const userId = ctx.from.id;
  const cached = resetLinkDownloadCache.get(userId);
  if (!cached || Date.now() > cached.expiresAt) {
    try {
      await ctx.reply("⚠️ Download link expired. Please run Reset Link again.", {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
      });
    } catch {}
    return;
  }
  try {
    const fileContent = Buffer.from(cached.text, "utf-8");
    const fileName = `reset_links_${new Date().toISOString().slice(0, 10)}.txt`;
    await bot.api.sendDocument(
      ctx.chat.id,
      new InputFile(fileContent, fileName),
      {
        caption: `📥 <b>Reset Links</b>\n${cached.text.split("\n").filter(l => l.startsWith("https://")).length} links — downloaded on ${new Date().toLocaleDateString("en-IN")}`,
        parse_mode: "HTML",
      }
    );
    resetLinkDownloadCache.delete(userId);
  } catch (err: any) {
    await ctx.reply(`❌ Failed to send file: ${esc(err?.message || "Unknown error")}`, { parse_mode: "HTML" });
  }
});

bot.callbackQuery("rl_link_retry", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Loading failed links..." });
  const userId = ctx.from.id;
  const cached = rlLinkRetryCache.get(userId);
  if (!cached || Date.now() > cached.expiresAt) {
    try {
      await ctx.reply("⚠️ Retry cache expired. Please run Reset by Link again.", {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
      });
    } catch {}
    return;
  }
  const links = [...cached.links];
  rlLinkRetryCache.delete(userId);
  resetLinkCancelRequests.delete(userId);
  const chatId = ctx.callbackQuery.message!.chat.id;
  let msgId = ctx.callbackQuery.message!.message_id;
  // Send a new progress message for the retry run
  try {
    const newMsg = await bot.api.sendMessage(
      chatId,
      `🔄 <b>Retrying ${links.length} Failed Link(s)...</b>\n\n⌛ Starting...`,
      { parse_mode: "HTML" }
    );
    msgId = newMsg.message_id;
  } catch {}
  void runRlResolvePipelineBackground(userId, links, chatId, msgId);
});

async function resetLinkBackground(
  userIdNum: number,
  groups: Array<{ id: string; subject: string }>,
  chatId: number,
  msgId: number
) {
  const userId = String(userIdNum);
  const results: Array<{ subject: string; newLink?: string; error?: string }> = [];
  let successCount = 0;
  let wasCancelled = false;

  const updateProgress = async (gi: number, currentGroup?: string) => {
    if (cancelDialogActiveFor.has(userIdNum)) return;
    try {
      await bot.api.editMessageText(chatId, msgId,
        `⏳ <b>Resetting invite links...</b>\n\n` +
        `📊 ${gi}/${groups.length} done | ✅ ${successCount} succeeded` +
        (currentGroup ? `\n\n🔄 Currently: ${esc(currentGroup)}` : ""),
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "rl_cancel_request") }
      );
    } catch {}
  };

  await updateProgress(0);

  for (let gi = 0; gi < groups.length; gi++) {
    if (resetLinkCancelRequests.has(userIdNum)) {
      wasCancelled = true;
      break;
    }
    const group = groups[gi];
    await updateProgress(gi, group.subject);

    let res = await resetGroupInviteLink(userId, group.id);

    // Rate limit / owner limit detected — wait and retry once
    if (!res.success && res.error) {
      const errLower = res.error.toLowerCase();
      if (
        errLower.includes("owner") ||
        errLower.includes("rate") ||
        errLower.includes("not-authorized") ||
        errLower.includes("forbidden") ||
        errLower.includes("405") ||
        errLower.includes("not authorized")
      ) {
        // Silent retries — keep trying until success, no error shown to user
        const rlWaits = [400, 600, 800, 1000, 1200, 1500, 2000, 2500, 3000, 3500];
        for (const wait of rlWaits) {
          if (res.success || resetLinkCancelRequests.has(userIdNum)) break;
          await new Promise((r) => setTimeout(r, wait));
          res = await resetGroupInviteLink(userId, group.id);
        }
      }
    }

    if (res.success && res.newLink) {
      results.push({ subject: group.subject, newLink: res.newLink });
      successCount++;
    } else {
      results.push({ subject: group.subject, error: "Could not reset" });
    }
    await updateProgress(gi + 1);
    if (gi < groups.length - 1) await new Promise((r) => setTimeout(r, 50));
  }

  resetLinkCancelRequests.delete(userIdNum);
  cancelDialogActiveFor.delete(userIdNum);

  const successResults = results.filter(r => r.newLink);
  const failedResults = results.filter(r => !r.newLink);

  let resultText = `🔗 <b>Reset Link Result</b>\n📊 <b>Total: ${successCount} link(s) reset successfully</b>\n\n`;
  if (wasCancelled) resultText += `⛔ <b>Cancelled after ${results.length}/${groups.length} group(s).</b>\n\n`;

  // Show all successful resets first with their new links
  for (const r of successResults) {
    resultText += `✅ <b>${esc(r.subject)}</b>\n${r.newLink}\n\n`;
  }

  // Show all failed groups together at the end (sirf naam, koi reason nahi)
  if (failedResults.length > 0) {
    resultText += `━━━━━━━━━━━━━━━━━━
⚠️ <b>Could Not Reset (${failedResults.length} group(s)):</b>
`;
    for (const r of failedResults) {
      resultText += `❌ <b>${esc(r.subject)}</b>
`;
    }
    resultText += "\n";
  }

  if (!wasCancelled) {
    resultText += `━━━━━━━━━━━━━━━━━━\n✅ <b>${successCount}/${groups.length} links reset successfully!</b>`;
  }

  // Cache links for optional .txt download (valid 15 min)
  const showDownloadBg = successResults.length >= 50;
  if (showDownloadBg) {
    const linksText = successResults.map(r => `${r.subject}\n${r.newLink}`).join("\n\n");
    resetLinkDownloadCache.set(userIdNum, { text: linksText, expiresAt: Date.now() + 15 * 60 * 1000 });
  }

  const chunks = splitMessage(resultText, 4000);
  const lastKbBg = showDownloadBg
    ? new InlineKeyboard().text("📥 Download Links (.txt)", "rl_download").text("🏠 Main Menu", "main_menu")
    : new InlineKeyboard().text("🏠 Main Menu", "main_menu");

  try {
    await bot.api.editMessageText(chatId, msgId, chunks[0], {
      parse_mode: "HTML",
      reply_markup: chunks.length === 1 ? lastKbBg : undefined,
    });
  } catch {}
  for (let i = 1; i < chunks.length; i++) {
    await bot.api.sendMessage(chatId, chunks[i], {
      parse_mode: "HTML",
      reply_markup: i === chunks.length - 1 ? lastKbBg : undefined,
    });
  }
}

// ─── Demote Admin Feature ────────────────────────────────────────────────────

const DA_PAGE_SIZE = 20;

function buildDemoteAdminKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const allGroups = state.demoteAdminData!.allGroups;
  const selected = state.demoteAdminData!.selectedIndices;
  const page = state.demoteAdminData!.page;
  const totalPages = Math.max(1, Math.ceil(allGroups.length / DA_PAGE_SIZE));
  const start = page * DA_PAGE_SIZE;
  const end = Math.min(start + DA_PAGE_SIZE, allGroups.length);

  for (let i = start; i < end; i++) {
    const g = allGroups[i];
    const label = selected.has(i) ? `✅ ${g.subject}` : `☐ ${g.subject}`;
    kb.text(label, `da_tog_${i}`).row();
  }

  const prev = page > 0 ? "⬅️ Previous 20" : " ";
  const next = page < totalPages - 1 ? "Next 20 ➡️" : " ";
  kb.text(prev, "da_prev_page").text(`📄 ${page + 1}/${totalPages}`, "da_page_info").text(next, "da_next_page").row();

  if (allGroups.length > 1) {
    kb.text("☑️ Select All", "da_select_all").text("🗑️ Clear All", "da_clear_all").row();
  }
  if (selected.size > 0) {
    kb.text(`▶️ Proceed (${selected.size} groups)`, "da_proceed").row();
  }
  kb.text("🏠 Back", "main_menu");
  return kb;
}

bot.callbackQuery("demote_admin", async (ctx) => {
  ctx.answerCallbackQuery();
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
    step: "demote_admin_menu",
    demoteAdminData: {
      allGroups: adminGroupsSimple,
      patterns,
      selectedIndices: new Set(),
      page: 0,
    },
  });

  const kb = new InlineKeyboard();
  if (patterns.length > 0) kb.text("🔍 Similar Groups", "da_similar").text("📋 All Groups", "da_show_all").row();
  else kb.text("📋 All Groups", "da_show_all").row();
  kb.text("🔗 By Link", "da_by_link").row();
  kb.text("🏠 Main Menu", "main_menu");

  await ctx.editMessageText(
    `👤 <b>Demote Admin</b>\n\n` +
    `📊 Admin Groups: ${adminGroups.length} (Total: ${allGroups.length})\n` +
    (patterns.length > 0 ? `🔍 Similar Patterns: ${patterns.length}\n` : "") +
    `\n📌 Choose an option:`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery("da_similar", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.demoteAdminData) return;
  const { patterns } = state.demoteAdminData;
  if (!patterns.length) {
    await ctx.editMessageText("⚠️ No similar group patterns found.", {
      reply_markup: new InlineKeyboard().text("🔙 Back", "demote_admin").text("🏠 Menu", "main_menu"),
    }); return;
  }
  const kb = new InlineKeyboard();
  for (let i = 0; i < patterns.length; i++) {
    kb.text(`📌 ${patterns[i].base} (${patterns[i].groups.length})`, `da_sim_${i}`).row();
  }
  kb.text("🔙 Back", "demote_admin").text("🏠 Menu", "main_menu");
  await ctx.editMessageText("🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select those groups:", {
    parse_mode: "HTML", reply_markup: kb,
  });
});

bot.callbackQuery(/^da_sim_(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.demoteAdminData) return;
  const idx = parseInt(ctx.match![1]);
  const pattern = state.demoteAdminData.patterns[idx];
  if (!pattern) return;
  const patternIds = new Set(pattern.groups.map((g) => g.id));
  state.demoteAdminData.selectedIndices = new Set();
  for (let i = 0; i < state.demoteAdminData.allGroups.length; i++) {
    if (patternIds.has(state.demoteAdminData.allGroups[i].id)) state.demoteAdminData.selectedIndices.add(i);
  }
  state.step = "demote_admin_select";
  state.demoteAdminData.page = 0;
  await ctx.editMessageText(
    `👤 <b>Demote Admin</b>\n\n${state.demoteAdminData.allGroups.length} admin group(s)\n\n<i>${state.demoteAdminData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildDemoteAdminKeyboard(state) }
  );
});

bot.callbackQuery("da_show_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.demoteAdminData) return;
  state.step = "demote_admin_select";
  state.demoteAdminData.page = 0;
  await ctx.editMessageText(
    `👤 <b>Demote Admin</b>\n\n${state.demoteAdminData.allGroups.length} admin group(s)\n\nSelect groups:`,
    { parse_mode: "HTML", reply_markup: buildDemoteAdminKeyboard(state) }
  );
});

bot.callbackQuery(/^da_tog_(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.demoteAdminData) return;
  const idx = parseInt(ctx.match![1]);
  if (idx < 0 || idx >= state.demoteAdminData.allGroups.length) return;
  if (state.demoteAdminData.selectedIndices.has(idx)) state.demoteAdminData.selectedIndices.delete(idx);
  else state.demoteAdminData.selectedIndices.add(idx);
  await ctx.editMessageText(
    `👤 <b>Demote Admin</b>\n\n${state.demoteAdminData.allGroups.length} admin group(s)\n\n<i>${state.demoteAdminData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildDemoteAdminKeyboard(state) }
  );
});

bot.callbackQuery("da_prev_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.demoteAdminData) return;
  if (state.demoteAdminData.page > 0) state.demoteAdminData.page--;
  await ctx.editMessageText(
    `👤 <b>Demote Admin</b>\n\n${state.demoteAdminData.allGroups.length} admin group(s)\n\n<i>${state.demoteAdminData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildDemoteAdminKeyboard(state) }
  );
});

bot.callbackQuery("da_next_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.demoteAdminData) return;
  const totalPages = Math.ceil(state.demoteAdminData.allGroups.length / DA_PAGE_SIZE);
  if (state.demoteAdminData.page < totalPages - 1) state.demoteAdminData.page++;
  await ctx.editMessageText(
    `👤 <b>Demote Admin</b>\n\n${state.demoteAdminData.allGroups.length} admin group(s)\n\n<i>${state.demoteAdminData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildDemoteAdminKeyboard(state) }
  );
});

bot.callbackQuery("da_page_info", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" });
});

bot.callbackQuery("da_select_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.demoteAdminData) return;
  for (let i = 0; i < state.demoteAdminData.allGroups.length; i++) state.demoteAdminData.selectedIndices.add(i);
  await ctx.editMessageText(
    `👤 <b>Demote Admin</b>\n\nAll <b>${state.demoteAdminData.allGroups.length} groups selected</b>`,
    { parse_mode: "HTML", reply_markup: buildDemoteAdminKeyboard(state) }
  );
});

bot.callbackQuery("da_clear_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.demoteAdminData) return;
  state.demoteAdminData.selectedIndices.clear();
  await ctx.editMessageText(
    `👤 <b>Demote Admin</b>\n\n${state.demoteAdminData.allGroups.length} admin group(s)\n\n<i>None selected</i>`,
    { parse_mode: "HTML", reply_markup: buildDemoteAdminKeyboard(state) }
  );
});

bot.callbackQuery("da_proceed", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.demoteAdminData || state.demoteAdminData.selectedIndices.size === 0) return;

  const selectedGroups = Array.from(state.demoteAdminData.selectedIndices).map((i) => state.demoteAdminData!.allGroups[i]);
  const groupList = selectedGroups.slice(0, 20).map((g) => `• ${esc(g.subject)}`).join("\n");
  const more = selectedGroups.length > 20 ? `\n... +${selectedGroups.length - 20} more` : "";

  await ctx.editMessageText(
    `👤 <b>Demote Admin</b>\n\n` +
    `<b>${selectedGroups.length} group(s) selected:</b>\n${groupList}${more}\n\n` +
    `Choose demote mode:`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🔴 Demote All Admins", "da_mode_all").row()
        .text("📱 Demote Selected Numbers", "da_mode_numbers").row()
        .text("🔙 Back", "da_show_all").text("🏠 Menu", "main_menu"),
    }
  );
});

bot.callbackQuery("da_mode_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.demoteAdminData || state.demoteAdminData.selectedIndices.size === 0) return;

  state.demoteAdminData.mode = "all";
  const selectedGroups = Array.from(state.demoteAdminData.selectedIndices).map((i) => state.demoteAdminData!.allGroups[i]);
  const groupList = selectedGroups.slice(0, 20).map((g) => `• ${esc(g.subject)}`).join("\n");
  const more = selectedGroups.length > 20 ? `\n... +${selectedGroups.length - 20} more` : "";

  await ctx.editMessageText(
    `🔴 <b>Demote All Admins — Confirm</b>\n\n` +
    `<b>${selectedGroups.length} group(s):</b>\n${groupList}${more}\n\n` +
    `⚠️ This will demote <b>all non-owner admins</b> in the selected groups.\n\n` +
    `Are you sure?`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Demote All", "da_all_confirm")
        .text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("da_all_confirm", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.demoteAdminData || state.demoteAdminData.selectedIndices.size === 0) return;

  const selectedGroups = Array.from(state.demoteAdminData.selectedIndices).map((i) => state.demoteAdminData!.allGroups[i]);
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;
  userStates.delete(userId);
  demoteAdminCancelRequests.delete(userId);

  try {
    await bot.api.editMessageText(chatId, msgId,
      `⏳ <b>Demoting all admins in ${selectedGroups.length} group(s)...</b>\n\n⌛ Please wait...`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "da_cancel_request") }
    );
  } catch {}

  void demoteAllBackground(userId, selectedGroups, chatId, msgId);
});

bot.callbackQuery("da_mode_numbers", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.demoteAdminData || state.demoteAdminData.selectedIndices.size === 0) return;

  state.demoteAdminData.mode = "numbers";
  state.step = "demote_admin_enter_numbers";

  await ctx.editMessageText(
    `📱 <b>Demote Selected Numbers</b>\n\n` +
    `Send the phone numbers to demote (one per line):\n\n` +
    `Example:\n<code>919912345678\n919898765432</code>\n\n` +
    `Only numbers that are currently admin in the selected groups will be demoted.`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("da_numbers_confirm", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.demoteAdminData || !state.demoteAdminData.phoneNumbers?.length) return;

  const selectedGroups = Array.from(state.demoteAdminData.selectedIndices).map((i) => state.demoteAdminData!.allGroups[i]);
  const phoneNumbers = state.demoteAdminData.phoneNumbers;
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;
  userStates.delete(userId);
  demoteAdminCancelRequests.delete(userId);

  try {
    await bot.api.editMessageText(chatId, msgId,
      `⏳ <b>Demoting ${phoneNumbers.length} number(s) in ${selectedGroups.length} group(s)...</b>\n\n⌛ Please wait...`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "da_cancel_request") }
    );
  } catch {}

  void demoteSelectedBackground(userId, selectedGroups, phoneNumbers, chatId, msgId);
});

bot.callbackQuery("da_cancel_request", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (cancelDialogActiveFor.has(userId)) return;
  cancelDialogActiveFor.add(userId);
  try {
    await ctx.editMessageReplyMarkup({
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Cancel", "da_cancel_confirm")
        .text("🔙 No, Continue", "da_cancel_abort"),
    });
  } catch {}
});

bot.callbackQuery("da_cancel_confirm", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Cancelling..." });
  const userId = ctx.from.id;
  demoteAdminCancelRequests.add(userId);
  cancelDialogActiveFor.delete(userId);
  try { await ctx.editMessageReplyMarkup({ reply_markup: new InlineKeyboard() }); } catch {}
});

bot.callbackQuery("da_cancel_abort", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Continuing..." });
  cancelDialogActiveFor.delete(ctx.from.id);
  try {
    await ctx.editMessageReplyMarkup({
      reply_markup: new InlineKeyboard().text("❌ Cancel", "da_cancel_request"),
    });
  } catch {}
});

async function demoteAllBackground(
  userIdNum: number,
  groups: Array<{ id: string; subject: string }>,
  chatId: number,
  msgId: number
) {
  activeBackgroundUsers.add(userIdNum);
  const userId = String(userIdNum);
  const lines: string[] = [];
  let totalDemoted = 0;
  let wasCancelled = false;

  // Get bot's own phone number so we never demote ourselves
  const myNumber = (getConnectedWhatsAppNumber(userId) || "").replace(/[^0-9]/g, "");

  for (let gi = 0; gi < groups.length; gi++) {
    if (demoteAdminCancelRequests.has(userIdNum)) { wasCancelled = true; break; }
    const group = groups[gi];
    let demoted = 0, skipped = 0, failed = 0;
    const groupLines: string[] = [];

    try {
      if (!cancelDialogActiveFor.has(userIdNum)) {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n⌛ Fetching admins...`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "da_cancel_request") }
        );
      }
    } catch {}

    const participants = await getGroupParticipants(userId, group.id);

    // Check if bot itself is admin — if not, we cannot demote anyone in this group
    const mySession = userStates.get(userIdNum);
    void mySession; // suppress unused warning
    // We check by attempting a safe read: if participants is empty the group is likely inaccessible
    if (!participants.length) {
      lines.push(`📋 <b>${esc(group.subject)}</b>\n  ⚠️ Could not fetch group data (bot may not be a member)`);
      continue;
    }

    const ownerCount = participants.filter((p) => p.isSuperAdmin).length;
    // Filter: not owner, and not the bot's own number
    const admins = participants.filter((p) => {
      if (p.isSuperAdmin) return false;
      if (!p.isAdmin) return false;
      if (myNumber) {
        const pNum = p.phone.replace(/[^0-9]/g, "");
        // Match on last 10 digits to handle country-code variations
        if (pNum && myNumber && (pNum === myNumber || pNum.slice(-10) === myNumber.slice(-10))) return false;
      }
      return true;
    });

    if (!admins.length) {
      const ownerNote = ownerCount > 0 ? ` (${ownerCount} owner(s) skipped)` : "";
      lines.push(`📋 <b>${esc(group.subject)}</b>\n  ℹ️ No demotable admins found${ownerNote}`);
      continue;
    }

    for (let ai = 0; ai < admins.length; ai++) {
      if (demoteAdminCancelRequests.has(userIdNum)) { wasCancelled = true; break; }
      const admin = admins[ai];
      const phone = admin.phone || admin.jid.split("@")[0];

      let res = await demoteGroupAdmin(userId, group.id, admin.jid);

      // Rate limit / not-authorized — wait and retry once
      if (!res.success && res.error) {
        const errLow = res.error.toLowerCase();
        if (
          errLow.includes("not-authorized") || errLow.includes("forbidden") ||
          errLow.includes("rate") || errLow.includes("403") || errLow.includes("405")
        ) {
          await new Promise((r) => setTimeout(r, 5000));
          if (!demoteAdminCancelRequests.has(userIdNum)) {
            res = await demoteGroupAdmin(userId, group.id, admin.jid);
          }
        }
      }

      if (res.success) {
        groupLines.push(`  ✅ +${phone} — Demoted`);
        demoted++;
        totalDemoted++;
      } else {
        // Summarise the error into a short human-readable reason
        const errMsg = res.error || "Unknown error";
        const errLow = errMsg.toLowerCase();
        let reason = "Failed";
        if (errLow.includes("not-authorized") || errLow.includes("forbidden") || errLow.includes("403")) {
          reason = "Not authorized (bot may not be admin)";
        } else if (errLow.includes("not connected") || errLow.includes("disconnected")) {
          reason = "WhatsApp not connected";
        } else if (errLow.includes("rate") || errLow.includes("405")) {
          reason = "Rate limited";
        } else if (errMsg.length < 80) {
          reason = errMsg;
        }
        groupLines.push(`  ❌ +${phone} — ${reason}`);
        failed++;
      }

      if (ai % 3 === 0 || ai === admins.length - 1) {
        try {
          if (!cancelDialogActiveFor.has(userIdNum)) {
            await bot.api.editMessageText(chatId, msgId,
              `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n` +
              `Processing: ${ai + 1}/${admins.length}\n✅ Demoted: ${demoted} | ❌ Failed: ${failed}`,
              { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "da_cancel_request") }
            );
          }
        } catch {}
      }
      await new Promise((r) => setTimeout(r, 600));
    }

    if (wasCancelled) break;
    lines.push(`📋 <b>${esc(group.subject)}</b>\n${groupLines.join("\n")}\n✅ Demoted: ${demoted} | ❌ Failed: ${failed} | ⏭️ Skipped: ${skipped}`);
  }

  demoteAdminCancelRequests.delete(userIdNum);
  cancelDialogActiveFor.delete(userIdNum);

  let result = `👤 <b>Demote All Admins — Result</b>\n\n`;
  result += lines.join("\n\n");
  if (wasCancelled) result += `\n\n⛔ <b>Cancelled after ${lines.length}/${groups.length} group(s).</b>`;
  else result += `\n\n━━━━━━━━━━━━━━━━━━\n✅ <b>Total demoted: ${totalDemoted} across ${groups.length} group(s)!</b>`;

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
}

async function demoteSelectedBackground(
  userIdNum: number,
  groups: Array<{ id: string; subject: string }>,
  phoneNumbers: string[],
  chatId: number,
  msgId: number
) {
  activeBackgroundUsers.add(userIdNum);
  const userId = String(userIdNum);
  const lines: string[] = [];
  let totalDemoted = 0;
  let wasCancelled = false;

  for (let gi = 0; gi < groups.length; gi++) {
    if (demoteAdminCancelRequests.has(userIdNum)) { wasCancelled = true; break; }
    const group = groups[gi];
    const groupLines: string[] = [];
    let demoted = 0, notAdmin = 0, notFound = 0, failed = 0;

    try {
      if (!cancelDialogActiveFor.has(userIdNum)) {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n⌛ Processing ${phoneNumbers.length} number(s)...`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "da_cancel_request") }
        );
      }
    } catch {}

    const participants = await getGroupParticipants(userId, group.id);

    for (let pi = 0; pi < phoneNumbers.length; pi++) {
      if (demoteAdminCancelRequests.has(userIdNum)) { wasCancelled = true; break; }
      const phone = phoneNumbers[pi].replace(/[^0-9]/g, "");
      const phoneLast10 = phone.slice(-10);
      const participant = participants.find((p) => {
        const pPhone = p.phone.replace(/[^0-9]/g, "");
        return pPhone === phone || (phoneLast10.length >= 7 && pPhone.slice(-10) === phoneLast10);
      });

      if (!participant) {
        groupLines.push(`  ❌ +${phone} — Not found in group`);
        notFound++;
      } else if (!participant.isAdmin) {
        groupLines.push(`  ⚠️ +${phone} — Not an admin`);
        notAdmin++;
      } else if (participant.isSuperAdmin) {
        groupLines.push(`  ⚠️ +${phone} — Group owner, cannot demote`);
        notAdmin++;
      } else {
        let res = await demoteGroupAdmin(userId, group.id, participant.jid);

        // Rate limit / not-authorized — wait and retry once
        if (!res.success && res.error) {
          const errLow = res.error.toLowerCase();
          if (
            errLow.includes("not-authorized") || errLow.includes("forbidden") ||
            errLow.includes("rate") || errLow.includes("403") || errLow.includes("405")
          ) {
            await new Promise((r) => setTimeout(r, 5000));
            if (!demoteAdminCancelRequests.has(userIdNum)) {
              res = await demoteGroupAdmin(userId, group.id, participant.jid);
            }
          }
        }

        if (res.success) {
          groupLines.push(`  ✅ +${phone} — Demoted`);
          demoted++;
          totalDemoted++;
        } else {
          const errMsg = res.error || "Unknown error";
          const errLow = errMsg.toLowerCase();
          let reason = "Failed";
          if (errLow.includes("not-authorized") || errLow.includes("forbidden") || errLow.includes("403")) {
            reason = "Not authorized (bot may not be admin)";
          } else if (errLow.includes("not connected") || errLow.includes("disconnected")) {
            reason = "WhatsApp not connected";
          } else if (errLow.includes("rate") || errLow.includes("405")) {
            reason = "Rate limited";
          } else if (errMsg.length < 80) {
            reason = errMsg;
          }
          groupLines.push(`  ❌ +${phone} — ${reason}`);
          failed++;
        }
      }

      if (pi % 3 === 0 || pi === phoneNumbers.length - 1) {
        try {
          if (!cancelDialogActiveFor.has(userIdNum)) {
            await bot.api.editMessageText(chatId, msgId,
              `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n` +
              `Processing: ${pi + 1}/${phoneNumbers.length}\n✅ Demoted: ${demoted} | ⚠️ Skip: ${notAdmin} | ❌ Not found: ${notFound} | ❌ Failed: ${failed}`,
              { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "da_cancel_request") }
            );
          }
        } catch {}
      }
      await new Promise((r) => setTimeout(r, 500));
    }

    if (wasCancelled) break;
    lines.push(`📋 <b>${esc(group.subject)}</b>\n${groupLines.join("\n")}\n✅ Demoted: ${demoted} | ⚠️ Not admin: ${notAdmin} | ❌ Not found: ${notFound} | ❌ Failed: ${failed}`);
  }

  demoteAdminCancelRequests.delete(userIdNum);
  cancelDialogActiveFor.delete(userIdNum);

  let result = `👤 <b>Demote Selected Numbers — Result</b>\n\n`;
  result += lines.join("\n\n");
  if (wasCancelled) result += `\n\n⛔ <b>Cancelled after ${lines.length}/${groups.length} group(s).</b>`;
  else result += `\n\n━━━━━━━━━━━━━━━━━━\n✅ <b>Total demoted: ${totalDemoted} across ${groups.length} group(s)!</b>`;

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
}

// ─── Disconnect ──────────────────────────────────────────────────────────────

bot.callbackQuery("disconnect_wa", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("ℹ️ WhatsApp is not connected.", {
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    }); return;
  }
  // 1. Suppress the "⚠️ Disconnected" push notification for this intentional
  //    disconnect — the user just pressed the button, no need to alert them.
  suppressDisconnectNotification(String(userId));
  suppressDisconnectNotification(getAutoUserId(String(userId)));
  // 2a. Invalidate the session cache immediately so the next /start or button
  //     press does not incorrectly think a stored session still exists.
  hasSessionCache.del(String(userId));
  // 2b. Drop this user's slice of every per-user in-memory Map/Set so RAM
  //     actually returns to baseline instead of being held by orphaned state.
  clearUserMemoryState(userId);
  // 3. Show "✅ Disconnected" immediately — don't block on socket.logout().
  //    Fire-and-forget both WhatsApp disconnects so network latency is invisible.
  void disconnectWhatsApp(String(userId)).catch(() => {});
  void disconnectWhatsApp(getAutoUserId(String(userId))).catch(() => {});
  // 4. Run a global purge to flush translation caches + nudge V8/glibc to
  //    actually release pages back to the OS so RSS visibly drops.
  void runMemoryPurge("user disconnect");
  await ctx.editMessageText("✅ <b>WhatsApp disconnected!</b>", {
    parse_mode: "HTML", reply_markup: mainMenu(userId),
  });
});

// ─── Connect Auto Chat WhatsApp ───────────────────────────────────────────────

bot.callbackQuery("connect_auto_wa", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;

  if (isAutoConnected(String(userId))) {
    await ctx.editMessageText(
      "✅ <b>Auto Chat WhatsApp already connected!</b>\n\n" + connectedStatusText(userId),
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
    return;
  }

  userStates.set(userId, { step: "auto_connect_phone", autoConnectStep: "phone", autoSlot: 1 });
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
  ctx.answerCallbackQuery();
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
      "🤖 <b>Auto Chat</b>\n\nPrimary WhatsApp connected nahi hai.\n\nPehle apna 1st WhatsApp connect karo.",
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("📱 Connect 1st WhatsApp", "connect_wa").row()
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

  // ── Multi-slot WA status ──
  const data = await loadBotData();
  const maxSlots = isAdmin(userId) ? 10 : getAutoWsLimit(userId, data);
  const mainNumber = getConnectedWhatsAppNumber(String(userId));
  const connectedSlots = getAllConnectedAutoSlots(String(userId));
  const totalConnected = connectedSlots.length + 1;

  let statusText = "🤖 <b>Auto Chat Menu</b>\n\n";
  statusText += mainNumber ? `📞 WA 1 (Primary): <code>${esc(mainNumber)}</code>\n` : "";
  for (const slot of connectedSlots) {
    const num = slot.number ? `<code>${esc(slot.number)}</code>` : "Unknown";
    statusText += `📱 WA ${slot.slot + 1}: ${num}\n`;
  }
  if (connectedSlots.length > 0) {
    statusText += `\n✅ <b>${totalConnected} WA accounts</b> connected`;
    if (totalConnected > 2) {
      const pct = Math.round(getAcfSpeedFactor(totalConnected) * 100);
      statusText += ` · ⚡ <b>${pct}% speed</b>`;
    }
  } else {
    statusText += "\n⚠️ No auto WA connected yet.";
  }
  statusText += "\n\nKya karna chahte ho?";

  const kb = new InlineKeyboard();
  // Show connect buttons for unconnected slots (up to user's limit)
  for (let slot = 1; slot <= maxSlots; slot++) {
    if (!isConnected(getAutoSlotUserId(String(userId), slot))) {
      const label = slot === 1 ? "📱 Connect 2nd WhatsApp" : `📱 Connect WA ${slot + 1}`;
      const cbData = slot === 1 ? "connect_auto_wa" : `connect_auto_wa_s:${slot}`;
      kb.text(label, cbData).row();
    }
  }
  if (connectedSlots.length > 0) {
    kb.text("👥 Chat In Group", "acig_start").row();
    kb.text("👫 Chat Friend", "acf_start").row();
    kb.text("🔌 Disconnect Auto WA", "auto_disconnect_wa").row();
  }
  kb.text("🏠 Main Menu", "main_menu");

  await ctx.editMessageText(statusText, { parse_mode: "HTML", reply_markup: kb });
});

// Extra auto WA slot connect (slot 2+) via /autows
bot.callbackQuery(/^connect_auto_wa_s:(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  const slot = parseInt(ctx.match[1]);
  if (isNaN(slot) || slot < 2 || slot > 10) return;
  const slotUserId = getAutoSlotUserId(String(userId), slot);
  if (isConnected(slotUserId)) {
    await ctx.editMessageText(`✅ WA ${slot + 1} already connected hai!`, {
      reply_markup: new InlineKeyboard().text("🔙 Back", "auto_chat_menu"),
    });
    return;
  }
  userStates.set(userId, { step: "auto_connect_phone", autoConnectStep: "phone", autoSlot: slot });
  await ctx.editMessageText(
    `🤖 <b>Connect WA ${slot + 1}</b>\n\n` +
    `Yeh number Auto Chat ke liye extra WA ${slot} hoga.\n\n` +
    "📱 Apna phone number bhejo (country code ke saath):\n" +
    "Example: <code>919876543210</code>",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "auto_chat_menu") }
  );
});

bot.callbackQuery("auto_chat_refresh", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const autoUserId = getAutoUserId(String(userId));
  // Suppress the "⚠️ Disconnected" push notification — user intentionally disconnected.
  suppressDisconnectNotification(autoUserId);
  // Invalidate session cache immediately, then fire-and-forget the socket
  // teardown so socket.logout() network latency doesn't block the UI.
  hasSessionCache.del(autoUserId);
  void disconnectWhatsApp(autoUserId).catch(() => {});
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
  ctx.answerCallbackQuery();
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

  {
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
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.step !== "acig_select_groups") return;
  const idx = parseInt(ctx.match[1]);
  const selected = state.chatInGroupData.selectedIndices;
  if (selected.has(idx)) selected.delete(idx); else selected.add(idx);
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildAcigKeyboard(state) }); } catch {}
});

bot.callbackQuery("acig_select_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.step !== "acig_select_groups") return;
  for (let i = 0; i < state.chatInGroupData.allGroups.length; i++) state.chatInGroupData.selectedIndices.add(i);
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildAcigKeyboard(state) }); } catch {}
});

bot.callbackQuery("acig_clear_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.step !== "acig_select_groups") return;
  state.chatInGroupData.selectedIndices.clear();
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildAcigKeyboard(state) }); } catch {}
});

bot.callbackQuery("acig_prev_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.chatInGroupData.page <= 0) return;
  state.chatInGroupData.page--;
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildAcigKeyboard(state) }); } catch {}
});

bot.callbackQuery("acig_next_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData) return;
  const data = state.chatInGroupData;
  const totalPages = Math.ceil(data.allGroups.length / CIG_PAGE_SIZE);
  if (data.page < totalPages - 1) data.page++;
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildAcigKeyboard(state) }); } catch {}
});

bot.callbackQuery("acig_page_info", async (ctx) => { ctx.answerCallbackQuery(); });

bot.callbackQuery("acig_proceed", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.chatInGroupData.selectedIndices.size === 0) return;
  state.step = "acig_select_duration";
  await ctx.editMessageText(
    "⏱️ <b>Select Auto Chat Duration</b>\n\n" +
    "How long should Auto Chat run in groups?\n\n" +
    "After the selected time, Auto Chat will stop automatically and you will be notified.",
    {
      parse_mode: "HTML",
      reply_markup: buildDurationKeyboard(userId, "acig_dur"),
    }
  );
});

bot.callbackQuery(/^acig_dur:(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.chatInGroupData.selectedIndices.size === 0) return;
  const durationMs = parseInt(ctx.match[1]);
  const data = state.chatInGroupData;
  data.autoChatDurationMs = durationMs;
  const selectedGroups = [...data.selectedIndices].map(i => data.allGroups[i]);
  const autoUserId = getAutoUserId(String(userId));
  const durationLabel = durationMs === 0
    ? "No Limit"
    : `${Math.round(durationMs / (24 * 60 * 60 * 1000))} day(s)`;

  const statusMsg = await ctx.editMessageText(
    "👥 <b>Chat In Group Started!</b>\n\n" +
    `⏱️ Duration: <b>${durationLabel}</b>\n` +
    "Funny/study messages will rotate across all selected groups.\n\n" +
    "Use Stop to end it early.",
    { parse_mode: "HTML" }
  );
  const msgId = (statusMsg as any).message_id;
  const chatId = ctx.chat!.id;
  const autoChatExpiresAt = durationMs === 0 ? undefined : Date.now() + durationMs;
  userStates.delete(userId);
  void runGroupChatDualBackground(userId, String(userId), autoUserId, chatId, msgId, selectedGroups, autoChatExpiresAt);
});

function cigProgressText(session: CigSession): string {
  const currentGroup = session.groups[session.currentGroupIndex]?.subject || session.groups[0]?.subject || "group";
  const expiryText = session.autoChatExpiresAt
    ? `\n⏳ Time Remaining: <b>${formatRemaining(session.autoChatExpiresAt)}</b>`
    : "";
  return (
    "🤖 <b>Auto Chat Running</b>\n\n" +
    `📍 Mode: <b>Chat in Group</b>\n` +
    `🎯 Current Group: <b>${esc(currentGroup)}</b>\n\n` +
    `📊 <b>Messages Sent:</b>\n` +
    `📱 Account 1: <b>${session.sentByAccount1} messages</b>\n` +
    `📱 Account 2: <b>${session.sentByAccount2} messages</b>\n` +
    `📩 Total: <b>${session.sent} messages</b>\n` +
    `❌ Failed: <b>${session.failed}</b>\n\n` +
    (session.nextDelayMs > 0 ? `⏰ Next send in ~${formatDelay(session.nextDelayMs)}\n` : "") +
    expiryText +
    "\nPress <b>Stop</b> to stop the chat."
  );
}

async function runGroupChatDualBackground(
  userId: number,
  primaryUserId: string,
  autoUserId: string,
  chatId: number,
  msgId: number,
  groups: Array<{ id: string; subject: string }>,
  autoChatExpiresAt?: number,
  startGroupIndex = 0,
  startMessageIndex = 0,
  startSent = 0,
  startSentByAccount1 = 0,
  startSentByAccount2 = 0,
  startFailed = 0
): Promise<void> {
  const session: CigSession = {
    running: true,
    cancelled: false,
    chatId,
    msgId,
    groups,
    message: "Auto funny/study rotation",
    sent: startSent,
    failed: startFailed,
    sentByAccount1: startSentByAccount1,
    sentByAccount2: startSentByAccount2,
    botMode: "both",
    currentGroupIndex: startGroupIndex,
    cycle: 1,
    nextDelayMs: 0,
    rotationIndex: startMessageIndex,
    autoChatExpiresAt,
  };
  cigSessions.set(userId, session);

  // Persist to MongoDB so the session survives bot restarts.
  void saveAutoChatSession({
    userId,
    autoUserId,
    startedAt: Date.now(),
    sessionType: "cig",
    groups,
    autoChatExpiresAt,
    currentGroupIndex: startGroupIndex,
    messageIndex: startMessageIndex,
    sentCount: startSent,
    sentByAccount1: startSentByAccount1,
    sentByAccount2: startSentByAccount2,
    failedCount: startFailed,
  }).catch(() => {});

  // Protect BOTH WhatsApp sessions (primary + secondary) from idle and
  // memory-pressure eviction for the entire duration of this Chat In Group job.
  protectSessionFromEviction(primaryUserId);
  protectSessionFromEviction(autoUserId);

  const cigKb = new InlineKeyboard()
    .text("🔄 Refresh", "cig_refresh")
    .text("⏹️ Stop", "cig_stop_btn").row()
    .text("🏠 Main Menu", "main_menu");

  let messageIndex = 0;
  let accessCheckCounter = 0;
  const ACCESS_CHECK_EVERY = 10; // check access every 10 messages

  try {
    let groupIndex = 0;

    while (!session.cancelled && session.running) {
      if (!groups.length) break;
      markSessionActive(primaryUserId);
      markSessionActive(autoUserId);

      // Check if auto chat duration has expired
      if (autoChatExpiresAt && Date.now() >= autoChatExpiresAt) {
        session.running = false;
        session.cancelled = true;
        try {
          await bot.api.sendMessage(
            userId,
            "⏰ <b>Auto Chat Time Expired!</b>\n\n" +
            "Your selected Auto Chat duration has ended.\n" +
            `📤 Total sent: <b>${session.sent}</b>\n` +
            `❌ Failed: <b>${session.failed}</b>\n\n` +
            "Auto Chat has been stopped automatically.",
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
          );
        } catch {}
        break;
      }

      // Periodically check if user still has bot access
      accessCheckCounter++;
      if (accessCheckCounter % ACCESS_CHECK_EVERY === 0) {
        try {
          const stillHasAccess = await hasAccess(userId);
          const stillHasAutoChatAccess = canUserSeeAutoChat(userId);
          if (!stillHasAccess || !stillHasAutoChatAccess) {
            session.running = false;
            session.cancelled = true;
            try {
              await bot.api.sendMessage(
                userId,
                "🚫 <b>Auto Chat Stopped!</b>\n\n" +
                "Your bot access or Auto Chat access has been revoked by the admin.\n" +
                `📤 Total sent: <b>${session.sent}</b>\n\n` +
                "Auto Chat has been stopped automatically.",
                { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
              );
            } catch {}
            break;
          }
        } catch {}
      }

      const group = groups[groupIndex];
      session.currentGroupIndex = groupIndex;
      session.cycle = Math.floor(messageIndex / (groups.length * 2)) + 1;

      // ── Step 1: Account 1 sends to current group ────────────────────────
      if (!isSessionActive(session)) break;
      if (!isConnected(primaryUserId)) {
        try { await ensureSessionLoaded(primaryUserId); } catch {}
      }
      const msg1 = AUTO_GROUP_MESSAGES[messageIndex % AUTO_GROUP_MESSAGES.length];
      const ok1 = await sendGroupMessage(primaryUserId, group.id, msg1);
      if (ok1) { session.sent++; session.sentByAccount1++; } else session.failed++;
      messageIndex++;
      session.nextDelayMs = CIG_WITHIN_GROUP_DELAY_MS;

      try {
        await bot.api.editMessageText(chatId, msgId, cigProgressText(session), {
          parse_mode: "HTML", reply_markup: cigKb,
        });
      } catch {}

      if (!isSessionActive(session)) break;
      await waitWithCancel(session, CIG_WITHIN_GROUP_DELAY_MS);
      if (!isSessionActive(session)) break;

      // Check expiry again before second send
      if (autoChatExpiresAt && Date.now() >= autoChatExpiresAt) {
        session.running = false; session.cancelled = true;
        try {
          await bot.api.sendMessage(userId,
            "⏰ <b>Auto Chat Time Expired!</b>\n\nYour Auto Chat duration ended. Auto Chat stopped automatically.\n" +
            `📤 Sent: <b>${session.sent}</b>`,
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
          );
        } catch {}
        break;
      }

      // ── Step 2: Account 2 sends to same group ──────────────────────────
      if (!isSessionActive(session)) break;
      if (!isConnected(autoUserId)) {
        try { await ensureSessionLoaded(autoUserId); } catch {}
      }
      const msg2 = AUTO_GROUP_MESSAGES[messageIndex % AUTO_GROUP_MESSAGES.length];
      const ok2 = await sendGroupMessage(autoUserId, group.id, msg2);
      if (ok2) { session.sent++; session.sentByAccount2++; } else session.failed++;
      messageIndex++;
      session.nextDelayMs = CIG_BETWEEN_GROUP_DELAY_MS;

      try {
        await bot.api.editMessageText(chatId, msgId, cigProgressText(session), {
          parse_mode: "HTML", reply_markup: cigKb,
        });
      } catch {}

      if (!isSessionActive(session)) break;
      // Wait 2 min before moving to next group
      await waitWithCancel(session, CIG_BETWEEN_GROUP_DELAY_MS);
      if (!isSessionActive(session)) break;

      // ── Rotate to next group ───────────────────────────────────────────
      groupIndex = (groupIndex + 1) % groups.length;
      session.currentGroupIndex = groupIndex;

      // Save progress to MongoDB so restart can resume from this group.
      void saveAutoChatSession({
        userId,
        autoUserId,
        startedAt: Date.now(),
        sessionType: "cig",
        groups,
        autoChatExpiresAt,
        currentGroupIndex: groupIndex,
        messageIndex: messageIndex,
        sentCount: session.sent,
        sentByAccount1: session.sentByAccount1,
        sentByAccount2: session.sentByAccount2,
        failedCount: session.failed,
      }).catch(() => {});
    }
  } catch (err: any) {
    console.error(`[ACIG][${userId}] Error:`, err?.message);
  }

  // Release both protected sessions back to normal eviction rules.
  unprotectSession(primaryUserId);
  unprotectSession(autoUserId);

  session.running = false;
  session.nextDelayMs = 0;

  // Remove from MongoDB — session is done (or stopped by user/admin/expiry).
  void deleteAutoChatSession(userId).catch(() => {});

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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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

  const connectedSlots = getAllConnectedAutoSlots(String(userId));
  const primaryNumber = getConnectedWhatsAppNumber(String(userId));
  const primaryConnected = !!primaryNumber;

  // Need at least 2 connected WA accounts total (primary + auto, or 2+ auto slots)
  const totalConnectedCount = (primaryConnected ? 1 : 0) + connectedSlots.length;
  if (totalConnectedCount < 2) {
    if (connectedSlots.length === 0) {
      await ctx.editMessageText(
        "❌ Auto Chat WA connected nahi hai.\n\nPehle extra WhatsApp connect karo Auto Chat menu se.",
        {
          reply_markup: new InlineKeyboard()
            .text("🔙 Back", "auto_chat_menu")
            .text("🏠 Main Menu", "main_menu"),
        }
      );
    } else {
      await ctx.editMessageText(
        "❌ Chat Friend ke liye kam se kam 2 WhatsApp connected hone chahiye.\n\n" +
        (primaryConnected
          ? "Primary WhatsApp connected hai — ek aur Auto WA bhi connect karo."
          : "Primary WhatsApp connected nahi hai aur sirf 1 Auto WA connected hai.\n\nDono WA connect karo ya 2 Auto WA connect karo."),
        {
          reply_markup: new InlineKeyboard()
            .text("🔙 Back", "auto_chat_menu")
            .text("🏠 Main Menu", "main_menu"),
        }
      );
    }
    return;
  }

  // Build WA list — if primary is disconnected, use first 2 auto slots as the pair
  let allNumbers: string[];
  let allUserIds: string[];
  let waList = "";

  if (primaryConnected) {
    allNumbers = [primaryNumber, ...connectedSlots.map(s => s.number || "")].filter(Boolean);
    allUserIds = [String(userId), ...connectedSlots.map(s => s.userId)];
    waList = `📞 WA 1: <code>${esc(primaryNumber)}</code> (Primary)\n`;
    for (let i = 0; i < connectedSlots.length; i++) {
      waList += `📱 WA ${i + 2}: <code>${esc(connectedSlots[i].number || "")}</code>\n`;
    }
  } else {
    // Primary is disconnected — use connected auto slots only
    allNumbers = connectedSlots.map(s => s.number || "").filter(Boolean);
    allUserIds = connectedSlots.map(s => s.userId);
    waList = `⚠️ <i>Primary WA offline — running on Auto WA slots</i>\n`;
    for (let i = 0; i < connectedSlots.length; i++) {
      waList += `📱 WA ${i + 1}: <code>${esc(connectedSlots[i].number || "")}</code>\n`;
    }
  }

  const totalWa = allNumbers.length;

  userStates.set(userId, {
    step: "acf_select_duration",
    chatInGroupData: {
      allGroups: [],
      selectedIndices: new Set(),
      page: 0,
      message: allNumbers.join("|") + "||" + allUserIds.join("|"),
      delaySeconds: 0,
      cancelled: false,
    },
  });

  const speedPct = Math.round(getAcfSpeedFactor(totalWa) * 100);

  await ctx.editMessageText(
    "⏱️ <b>Select Chat Friend Duration</b>\n\n" +
    waList +
    (totalWa > 2 ? `\n⚡ Speed Boost Active: <b>${speedPct}%</b> with ${totalWa} WA accounts!\n` : "") +
    "\nHow long should Chat Friend run?\n\n" +
    "After the selected time, it will stop automatically and you will be notified.",
    {
      parse_mode: "HTML",
      reply_markup: buildDurationKeyboard(userId, "acf_dur"),
    }
  );
});

bot.callbackQuery(/^acf_dur:(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData) return;
  const durationMs = parseInt(ctx.match[1]);

  // Parse stored data: "num1|num2|...||userId1|userId2|..."
  const rawMsg = state.chatInGroupData.message || "";
  const sepIdx = rawMsg.indexOf("||");
  const numbersPart = sepIdx >= 0 ? rawMsg.slice(0, sepIdx) : rawMsg;
  const userIdsPart = sepIdx >= 0 ? rawMsg.slice(sepIdx + 2) : "";
  const allNumbers = numbersPart.split("|").filter(Boolean);
  const allUserIds = userIdsPart.split("|").filter(Boolean);

  const primaryNumber = allNumbers[0] || "";
  const autoNumber = allNumbers[1] || "";
  if (!primaryNumber || !autoNumber) return;

  // ── Step 1: Show contact-save instruction screen BEFORE starting chat ──
  // User must save all these numbers in their phone contacts, then tap confirm.
  let saveInstructions = "📱 <b>Pehle Yeh Numbers Apne Phone Mein Save Karo!</b>\n\n";
  saveInstructions += "⚠️ <i>Chat shuru hone se pehle neeche diye sab numbers apne har ek WhatsApp ke phone mein contacts mein save karo, tab hi messages deliver honge.</i>\n\n";
  saveInstructions += "<b>📋 In numbers ko save karo:</b>\n";
  for (let i = 0; i < allNumbers.length; i++) {
    const icon = i === 0 ? "📞" : "📱";
    const clean = allNumbers[i].replace(/[^0-9]/g, "");
    saveInstructions += `${icon} <b>WA ${i + 1}:</b> <code>+${clean}</code>\n`;
  }
  saveInstructions += "\n<b>Steps:</b>\n";
  saveInstructions += "1️⃣ Apne phone ka <b>Contacts</b> app kholo\n";
  saveInstructions += "2️⃣ Upar diye <b>sab numbers</b> ek ek karke save karo\n";
  saveInstructions += "3️⃣ WhatsApp open karo — contacts refresh honge\n";
  saveInstructions += "4️⃣ Neeche <b>\"✅ Save kar liya, Chat Shuru Karo\"</b> button dabao\n";

  // Store all needed data in state so confirm callback can use it
  userStates.set(userId, {
    step: "acf_contacts_confirm",
    chatInGroupData: {
      ...state.chatInGroupData,
      message: rawMsg,
      delaySeconds: durationMs,
    },
  });

  await ctx.editMessageText(saveInstructions, {
    parse_mode: "HTML",
    reply_markup: new InlineKeyboard()
      .text("✅ Save kar liya, Chat Shuru Karo", `acf_contacts_ok:${durationMs}`).row()
      .text("❌ Cancel", "auto_chat_menu"),
  });
});

bot.callbackQuery(/^acf_contacts_ok:(\d+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData) return;
  const durationMs = parseInt(ctx.match[1]);

  const rawMsg = state.chatInGroupData.message || "";
  const sepIdx = rawMsg.indexOf("||");
  const numbersPart = sepIdx >= 0 ? rawMsg.slice(0, sepIdx) : rawMsg;
  const userIdsPart = sepIdx >= 0 ? rawMsg.slice(sepIdx + 2) : "";
  const allNumbers = numbersPart.split("|").filter(Boolean);
  const allUserIds = userIdsPart.split("|").filter(Boolean);

  if (!allNumbers[0] || !allNumbers[1]) return;

  const allJids = allNumbers.map(n => n.replace(/[^0-9]/g, "") + "@s.whatsapp.net");
  const primaryJid = allJids[0];
  const autoJid = allJids[1];
  const totalWa = allJids.length;
  const totalPairs = CHAT_FRIEND_PAIRS.length;
  const autoChatExpiresAt = durationMs === 0 ? undefined : Date.now() + durationMs;
  const durationLabel = durationMs === 0 ? "No Limit" : `${Math.round(durationMs / (24 * 60 * 60 * 1000))} day(s)`;
  const speedPct = Math.round(getAcfSpeedFactor(totalWa) * 100);

  let waList = "";
  for (let i = 0; i < allNumbers.length; i++) {
    const role = i === 0 ? "Primary" : `Auto ${i}`;
    waList += `${i === 0 ? "📞" : "📱"} WA ${i + 1}: <code>${esc(allNumbers[i])}</code> (${role})\n`;
  }

  const statusMsg = await ctx.editMessageText(
    "👫 <b>Chat Friend Started!</b>\n\n" +
    waList +
    `⏱️ Duration: <b>${durationLabel}</b>\n` +
    (totalWa > 2 ? `⚡ Speed: <b>${speedPct}%</b> boost with ${totalWa} WA accounts!\n` : "") +
    "\n🔗 Contact save ho raha hai... phir messages shuru honge.",
    { parse_mode: "HTML" }
  );
  const msgId = (statusMsg as any).message_id;
  const chatId = ctx.chat!.id;
  userStates.delete(userId);

  void runChatFriendBackground(
    userId,
    allUserIds[0] || String(userId),
    allUserIds[1] || getAutoUserId(String(userId)),
    chatId, msgId,
    primaryJid, autoJid, totalPairs, autoChatExpiresAt,
    0, 0,
    allJids, allUserIds
  );
});

function acfProgressText(session: AcfSession): string {
  const expiryText = session.autoChatExpiresAt
    ? `\n⏳ Time Remaining: <b>${formatRemaining(session.autoChatExpiresAt)}</b>`
    : "";
  const N = session.waCount ?? 2;
  const waCountText = N >= 2
    ? `📱 WA Accounts: <b>${N}</b>${N > 2 ? " · ⚡ All-to-All Mode" : ""}\n`
    : "";
  // Show current direction: WA X → WA Y (with actual phone numbers for debugging)
  const senderNum = (session.currentSenderIdx ?? 0) + 1;
  const receiverNum = (session.currentReceiverIdx ?? 1) + 1;
  const sPhoneShort = session.currentSenderPhone ? ` <code>+${session.currentSenderPhone}</code>` : "";
  const rPhoneShort = session.currentReceiverPhone ? ` <code>+${session.currentReceiverPhone}</code>` : "";
  const directionText = `📨 Now: <b>WA ${senderNum}${sPhoneShort} → WA ${receiverNum}${rPhoneShort}</b>\n`;
  // Per-account breakdown — same as Chat In Group display
  const acfAccLines = session.sentByAccount && session.sentByAccount.length > 0
    ? (session.sentByAccount as number[]).map((count, i) => `📱 WA ${i + 1}: <b>${count} messages</b>`).join("\n") + "\n"
    : "";
  return (
    "👫 <b>Chat Friend Running...</b>\n\n" +
    waCountText +
    directionText +
    `🔁 Cycle: <b>${session.cycle}</b>\n` +
    `💬 Pair: <b>${session.currentPair}/${session.totalPairs}</b>\n` +
    `📊 <b>Messages Sent:</b>\n` +
    acfAccLines +
    `📤 Total: <b>${session.sent}</b>\n` +
    `❌ Failed: <b>${session.failed}</b>\n` +
    (session.nextDelayMs > 0 ? `⏱️ Next send in: <b>${formatDelay(session.nextDelayMs)}</b>\n` : "") +
    expiryText +
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
  totalPairs: number,
  autoChatExpiresAt?: number,
  startSent = 0,
  startFailed = 0,
  allJids: string[] = [],
  allUserIds: string[] = []
): Promise<void> {
  // Resolve full WA list (fall back to 2-WA mode if not provided)
  let resolvedJids = allJids.length >= 2 ? allJids : [primaryJid, autoJid];
  let resolvedUserIds = allUserIds.length >= 2 ? allUserIds : [primaryUserId, autoUserId];

  // ── Deduplicate by actual phone number ────────────────────────────────────
  // If the user used "Switch WA", sessionAliases can map two different userId
  // strings to the SAME physical Baileys socket. In that case both entries
  // would use the same WhatsApp account, so Chat Friend only has one real WA
  // and the "second sender" would be sending to itself (silently dropped).
  // Fix: keep only entries whose underlying phone number is unique.
  {
    const seenNumbers = new Set<string>();
    const dedupJids: string[] = [];
    const dedupUids: string[] = [];
    for (let i = 0; i < resolvedUserIds.length; i++) {
      const phone = (getConnectedWhatsAppNumber(resolvedUserIds[i]) ?? "").replace(/[^0-9]/g, "");
      if (phone && !seenNumbers.has(phone)) {
        seenNumbers.add(phone);
        dedupJids.push(resolvedJids[i]);
        dedupUids.push(resolvedUserIds[i]);
      }
    }
    // Only replace if dedup gave us a valid list; fallback to original on empty
    if (dedupUids.length >= 2) {
      resolvedJids = dedupJids;
      resolvedUserIds = dedupUids;
    }
  }

  const N = resolvedJids.length;
  const waCount = N;

  const session: AcfSession = {
    running: true,
    cancelled: false,
    chatId,
    msgId,
    primaryJid: resolvedJids[0],
    autoJid: resolvedJids[1],
    allJids: resolvedJids,
    allUserIds: resolvedUserIds,
    waCount,
    sent: startSent,
    failed: startFailed,
    currentPair: 0,
    totalPairs,
    cycle: 1,
    nextDelayMs: 0,
    rotationIndex: 0,
    autoChatExpiresAt,
    sentByAccount: new Array(N).fill(0),  // per-account sent tracking
  };
  acfSessions.set(userId, session);

  // Persist to MongoDB so the session survives bot restarts.
  // Save full allJids/allUserIds so multi-WA all-to-all state is restored correctly.
  void saveAutoChatSession({
    userId,
    autoUserId: resolvedUserIds[1] || autoUserId,
    startedAt: Date.now(),
    sessionType: "acf",
    primaryJid: resolvedJids[0],
    autoJid: resolvedJids[1],
    allJids: resolvedJids,
    allUserIds: resolvedUserIds,
    autoChatExpiresAt,
  }).catch(() => {});

  // Protect ALL WhatsApp sessions from idle/memory-pressure eviction.
  for (const uid of resolvedUserIds) {
    protectSessionFromEviction(uid);
  }

  const acfKb = new InlineKeyboard()
    .text("🔄 Refresh", "acf_refresh")
    .text("⏹️ Stop", "acf_stop_btn").row()
    .text("🏠 Main Menu", "main_menu");

  // ── Pre-save contacts: each WA saves every other WA's number in its contact list ──
  // Show real-time progress to the user. Chat only starts after ALL contacts are saved.
  {
    // Build initial status lines: one entry per (sender, receiver) pair
    interface ContactSaveStatus { sLabel: string; rLabel: string; phone: string; done: boolean; failed: boolean; }
    const contactSaveStatuses: ContactSaveStatus[] = [];
    for (let sIdx = 0; sIdx < N; sIdx++) {
      for (let rIdx = 0; rIdx < N; rIdx++) {
        if (rIdx === sIdx) continue;
        contactSaveStatuses.push({
          sLabel: `WA ${sIdx + 1}`,
          rLabel: `WA ${rIdx + 1}`,
          phone: resolvedJids[rIdx].replace("@s.whatsapp.net", ""),
          done: false,
          failed: false,
        });
      }
    }

    const buildContactSaveMsg = (done: boolean) => {
      let lines = "📋 <b>Contacts Save Ho Rahe Hain...</b>\n\n";
      for (const s of contactSaveStatuses) {
        const icon = s.done ? "✅" : s.failed ? "❌" : "⏳";
        lines += `${icon} ${s.sLabel} → ${s.rLabel} (<code>${esc(s.phone)}</code>)\n`;
      }
      if (done) lines += "\n✅ <b>Sab contacts save ho gaye! Chat shuru ho raha hai...</b>";
      else lines += "\n⌛ Please wait...";
      return lines;
    };

    // Show initial status
    try {
      await bot.api.editMessageText(chatId, msgId, buildContactSaveMsg(false), { parse_mode: "HTML" });
    } catch {}

    // Save each contact and update status
    for (const entry of contactSaveStatuses) {
      if (!session.running || session.cancelled) break;
      const senderIdx = Number(entry.sLabel.replace("WA ", "")) - 1;
      const senderUserId = resolvedUserIds[senderIdx] ?? resolvedUserIds[0];

      const saved = await saveContactToWhatsApp(senderUserId, entry.phone, `SPIDY ${entry.rLabel.replace("WA ", "")}`);
      entry.done = saved;
      entry.failed = !saved;

      // Warm up the personal JID: calls onWhatsApp() (resolves LID-protocol JID)
      // + sends composing/paused presence update to pre-exchange Signal keys.
      // This prevents silent send failures on the first message from this sender.
      void warmupPersonalJid(senderUserId, entry.phone).catch(() => {});

      try {
        await bot.api.editMessageText(chatId, msgId, buildContactSaveMsg(false), { parse_mode: "HTML" });
      } catch {}
      await sleep(500);
    }

    // Show completion message
    if (session.running && !session.cancelled) {
      try {
        await bot.api.editMessageText(chatId, msgId, buildContactSaveMsg(true), { parse_mode: "HTML" });
      } catch {}
      await sleep(1500);
    }
  }

  if (!session.running || session.cancelled) {
    for (const uid of resolvedUserIds) unprotectSession(uid);
    session.running = false;
    void deleteAutoChatSession(userId).catch(() => {});
    return;
  }

  let accessCheckCounter = 0;
  const ACCESS_CHECK_EVERY = 10;

  // Build all-to-all directed pairs once: every sender → every other receiver.
  // e.g. N=2: [(0,1),(1,0)]  N=3: [(0,1),(0,2),(1,0),(1,2),(2,0),(2,1)]
  const allToAllPairs = buildAllToAllPairs(N);
  const totalDirectedPairs = allToAllPairs.length;

  try {
    let stepCount = 0;    // total loop iterations (including skips when WA disconnected)
    let sendStepIdx = 0;  // counts ONLY actual send attempts — used for pair alternation
    let pairMsgIdx = 0;   // current CHAT_FRIEND_PAIRS message index

    while (!session.cancelled && session.running) {
      if (session.cancelled) break;

      // ── Expiry check ──────────────────────────────────────────────────────
      if (autoChatExpiresAt && Date.now() >= autoChatExpiresAt) {
        session.running = false;
        session.cancelled = true;
        try {
          await bot.api.sendMessage(
            userId,
            "⏰ <b>Chat Friend Time Expired!</b>\n\n" +
            "Your selected Chat Friend duration has ended.\n" +
            `📤 Total sent: <b>${session.sent}</b>\n` +
            `❌ Failed: <b>${session.failed}</b>\n\n` +
            "Chat Friend has been stopped automatically.",
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
          );
        } catch {}
        break;
      }

      // ── Access check ──────────────────────────────────────────────────────
      accessCheckCounter++;
      if (accessCheckCounter % ACCESS_CHECK_EVERY === 0) {
        try {
          const stillHasAccess = await hasAccess(userId);
          const stillHasAutoChatAccess = canUserSeeAutoChat(userId);
          if (!stillHasAccess || !stillHasAutoChatAccess) {
            session.running = false;
            session.cancelled = true;
            try {
              await bot.api.sendMessage(
                userId,
                "🚫 <b>Chat Friend Stopped!</b>\n\n" +
                "Your bot access or Auto Chat access has been revoked by the admin.\n" +
                `📤 Total sent: <b>${session.sent}</b>\n\n` +
                "Chat Friend has been stopped automatically.",
                { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
              );
            } catch {}
            break;
          }
        } catch {}
      }

      // ── All-to-All: every WA sends to every other WA in round-robin ───────
      // Pair order: (0→1),(0→2),(1→0),(1→2),(2→0),(2→1),… then repeats.
      // If a WA disconnected mid-session, skip pairs where it is the sender.
      // Rebuild the active pair list dynamically from currently connected WAs.
      const activeJids = resolvedJids.filter((_, i) => isConnected(resolvedUserIds[i]));
      const activeUids = resolvedUserIds.filter((uid) => isConnected(uid));
      if (activeJids.length < 2) {
        // Not enough WA connected to continue — pause and wait for reconnect
        session.nextDelayMs = 30_000;
        try {
          await bot.api.editMessageText(chatId, msgId,
            "👫 <b>Chat Friend — Waiting for WA...</b>\n\n" +
            "⚠️ Sirf 1 ya 0 WhatsApp connected hai.\n" +
            "2+ WA connect karo — chat automatically resume ho jaayega.",
            { parse_mode: "HTML", reply_markup: acfKb }
          );
        } catch {}
        await waitWithCancel(session, 30_000);
        stepCount++;
        continue;
      }
      // Rebuild pairs from currently connected WAs.
      // IMPORTANT: use sendStepIdx (not stepCount) for pair selection.
      // stepCount includes skipped iterations (when WA2 is disconnected).
      // If WA2 disconnects on every odd stepCount, stepCount is always even
      // when both WAs are connected → stepCount%2 always 0 → WA1 always sends.
      // sendStepIdx only increments on actual send attempts, so it always
      // strictly alternates 0,1,0,1,… regardless of disconnection patterns.
      const activePairs = buildAllToAllPairs(activeJids.length);
      const [activeSenderIdx, activeReceiverIdx] = activePairs[sendStepIdx % activePairs.length];
      const senderUserId = activeUids[activeSenderIdx];
      const receiverJid = activeJids[activeReceiverIdx];
      // Map back to original indices for progress display only
      const senderIdx = Math.max(0, resolvedUserIds.indexOf(senderUserId));
      const receiverIdx = resolvedJids.indexOf(receiverJid);

      // Pick message text: use activeSenderIdx (direct from pairs, always 0 or 1)
      // so WA at slot 0 always sends msg1 (question) and WA at slot 1 always sends msg2 (reply).
      const [msg1, msg2] = CHAT_FRIEND_PAIRS[pairMsgIdx % CHAT_FRIEND_PAIRS.length];
      const msg = activeSenderIdx % 2 === 0 ? msg1 : msg2;

      session.currentPair = (pairMsgIdx % CHAT_FRIEND_PAIRS.length) + 1;
      session.cycle = Math.floor(sendStepIdx / (totalDirectedPairs * CHAT_FRIEND_PAIRS.length)) + 1;
      session.currentSenderIdx = senderIdx;
      session.currentReceiverIdx = receiverIdx;
      // Store actual phone numbers for status display (helps debug JID issues)
      session.currentSenderPhone = (activeJids[activeSenderIdx] ?? "").replace("@s.whatsapp.net", "").replace(/[^0-9]/g, "");
      session.currentReceiverPhone = (receiverJid ?? "").replace("@s.whatsapp.net", "").replace(/[^0-9]/g, "");

      // Advance message pair after a full round of all directed pairs (use sendStepIdx)
      if (sendStepIdx > 0 && sendStepIdx % totalDirectedPairs === 0) {
        pairMsgIdx++;
      }

      // Defensive reconnect before sending
      if (!isConnected(senderUserId)) {
        try { await ensureSessionLoaded(senderUserId); } catch {}
      }
      const ok = await sendGroupMessage(senderUserId, receiverJid, msg);
      if (ok) {
        session.sent++;
        if (!session.sentByAccount) session.sentByAccount = [];
        // Use activeSenderIdx for sentByAccount tracking — safe, always 0..N-1
        while (session.sentByAccount.length <= activeSenderIdx) session.sentByAccount.push(0);
        session.sentByAccount[activeSenderIdx]++;
      } else {
        session.failed++;
      }

      // Dynamic delay — faster with more WA accounts
      session.nextDelayMs = getAcfDelayMs(session.rotationIndex, waCount);
      session.rotationIndex++;

      try {
        await bot.api.editMessageText(chatId, msgId, acfProgressText(session), {
          parse_mode: "HTML", reply_markup: acfKb,
        });
      } catch {}

      if (!isSessionActive(session)) break;
      await waitWithCancel(session, session.nextDelayMs);
      if (!isSessionActive(session)) break;

      // Periodic MongoDB persist — save full allJids/allUserIds for multi-WA restore
      if (stepCount % 10 === 0) {
        void saveAutoChatSession({
          userId,
          autoUserId: resolvedUserIds[1] || autoUserId,
          startedAt: Date.now(),
          sessionType: "acf",
          primaryJid: resolvedJids[0],
          autoJid: resolvedJids[1],
          allJids: resolvedJids,
          allUserIds: resolvedUserIds,
          autoChatExpiresAt,
          sentCount: session.sent,
          failedCount: session.failed,
        }).catch(() => {});
      }

      sendStepIdx++;  // only increments on actual send attempts (not on WA-disconnected skips)
      stepCount++;
    }
  } catch (err: any) {
    console.error(`[ACF][${userId}] Error:`, err?.message);
  }

  // Release ALL protected sessions back to normal eviction rules.
  for (const uid of resolvedUserIds) {
    unprotectSession(uid);
  }

  session.running = false;
  session.nextDelayMs = 0;

  // Remove from MongoDB — session is done (or stopped by user/admin/expiry).
  void deleteAutoChatSession(userId).catch(() => {});

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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  const N = (session.allUserIds || []).length;
  const accountLines = N > 1 && session.sentByAccount?.length
    ? (session.sentByAccount as number[]).map((count, i) => `📱 WA ${i + 1}: <b>${count} messages</b>`).join("\n") + "\n"
    : "";
  return (
    "🤖 <b>Auto Chat Chal Raha Hai...</b>\n\n" +
    `🔁 Round: <b>${session.currentRound}/${session.repeatCount === 0 ? "∞" : session.repeatCount}</b>\n` +
    (N > 1 ? `📊 <b>Messages Sent:</b>\n${accountLines}` : "") +
    `📤 Total Sent: <b>${session.sent}</b>\n` +
    `❌ Failed: <b>${session.failed}</b>\n` +
    (N <= 1 ? `📊 Progress: <b>${percent}%</b>\n` : "") +
    "\nRoknay ke liye Stop dabao."
  );
}

// ── Memory & concurrency tuning for low-RAM hosts (e.g. Render free 512MB) ──
// Targeted to handle 500-1000 concurrent Auto Chat sessions safely.
// All limits can be tuned via env vars without code changes.
const MAX_CONCURRENT_AUTOCHAT = Number(process.env.MAX_CONCURRENT_AUTOCHAT || "1000");
const MAX_GROUPS_PER_AUTOCHAT = Number(process.env.MAX_GROUPS_PER_AUTOCHAT || "300");
const AUTOCHAT_PROGRESS_THROTTLE_MS = Number(process.env.AUTOCHAT_PROGRESS_THROTTLE_MS || "20000");
let activeAutoChatCount = 0;

async function runAutoChatBackground(userId: number, autoUserId: string, chatId: number, msgId: number, groups: Array<{ id: string; subject: string }>, message: string, delaySeconds: number, repeatCount: number, startSent = 0): Promise<void> {
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

  // Build the full list of WA accounts for this user (primary + all connected auto slots)
  // so Auto Chat rotates through ALL of them instead of only the auto WA.
  const _primaryUid = String(userId);
  const _autoSlots = getAllConnectedAutoSlots(_primaryUid);
  const _allUserIds: string[] = [
    ...(isConnected(_primaryUid) ? [_primaryUid] : []),
    ...(_autoSlots.map((s) => s.userId)),
  ];
  // Fallback: if nothing is connected yet (session just started), use autoUserId
  if (_allUserIds.length === 0) _allUserIds.push(autoUserId);

  const session: AutoChatSession = {
    running: true,
    cancelled: false,
    chatId,
    msgId,
    groups: cappedGroups,
    message,
    delaySeconds,
    repeatCount,
    sent: startSent,
    failed: 0,
    currentRound: 1,
    rotationIndex: 0,
    allUserIds: _allUserIds,
    sentByAccount: new Array(_allUserIds.length).fill(0),
  };
  autoChatSessions.set(userId, session);
  activeAutoChatCount++;

  // Persist to MongoDB so session survives Render restarts.
  void saveAutoChatSession({
    userId,
    autoUserId,
    startedAt: Date.now(),
    sessionType: "old",
    groupIds: cappedGroups.map((g) => g.id),
    message,
    delaySeconds,
    repeatCount,
    sentCount: startSent,
  }).catch(() => {});

  // Protect ALL WhatsApp sessions (primary + auto slots) from idle/memory eviction
  // for the entire duration of this Auto Chat job so no socket gets killed mid-loop.
  for (const uid of session.allUserIds) protectSessionFromEviction(uid);

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
        // Round-robin: pick which WA account sends this message
        const stepTotal = session.sent + session.failed;
        const _accIdx = session.allUserIds.length > 1 ? stepTotal % session.allUserIds.length : 0;
        const _senderUid = session.allUserIds[_accIdx] || autoUserId;

        markSessionActive(_senderUid);

        // Defensive reconnect: if the chosen sender's socket was dropped due
        // to a server-side reset or transient network blip, lazy-restore it
        // from MongoDB BEFORE attempting the send.
        if (!isConnected(_senderUid)) {
          try {
            await ensureSessionLoaded(_senderUid);
          } catch (err: any) {
            console.error(`[AUTO_CHAT][${userId}] Lazy restore error (acc ${_accIdx + 1}):`, err?.message);
          }
        }

        let ok = false;
        try {
          ok = await sendGroupMessage(_senderUid, group.id, message);
        } catch (err: any) {
          // Never let a single send crash the whole loop.
          console.error(`[AUTO_CHAT][${userId}] sendGroupMessage error (acc ${_accIdx + 1}):`, err?.message);
          ok = false;
        }
        if (ok) {
          session.sent++;
          if (!session.sentByAccount) session.sentByAccount = [];
          while (session.sentByAccount.length <= _accIdx) session.sentByAccount.push(0);
          session.sentByAccount[_accIdx]++;
        } else {
          session.failed++;
        }

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

    // Remove persisted session — it has completed or was stopped.
    void deleteAutoChatSession(userId).catch(() => {});

    // Release ALL protected WhatsApp sessions back to normal eviction rules.
    for (const uid of session.allUserIds) unprotectSession(uid);

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

  {
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData) return;
  const data = state.chatInGroupData;
  for (let i = 0; i < data.allGroups.length; i++) data.selectedIndices.add(i);
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildChatGroupKeyboard(state) }); } catch {}
});

bot.callbackQuery("cig_clear_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData) return;
  state.chatInGroupData.selectedIndices.clear();
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildChatGroupKeyboard(state) }); } catch {}
});

bot.callbackQuery("cig_prev_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || state.chatInGroupData.page <= 0) return;
  state.chatInGroupData.page--;
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildChatGroupKeyboard(state) }); } catch {}
});

bot.callbackQuery("cig_next_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData) return;
  const data = state.chatInGroupData;
  const totalPages = Math.ceil(data.allGroups.length / CIG_PAGE_SIZE);
  if (data.page < totalPages - 1) data.page++;
  try { await ctx.editMessageReplyMarkup({ reply_markup: buildChatGroupKeyboard(state) }); } catch {}
});

bot.callbackQuery("cig_page_info", async (ctx) => { ctx.answerCallbackQuery(); });

bot.callbackQuery("cig_proceed", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  {
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
  ctx.answerCallbackQuery();
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
    await ctx.editMessageText("📭 You are not an admin in any group.", {
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
  kb.text("🔗 By Link", "es_by_link").row();
  kb.text("🏠 Main Menu", "main_menu");
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\n📊 Admin Groups: ${adminGroups.length}\n` +
    (patterns.length > 0 ? `🔍 Similar Patterns: ${patterns.length}\n` : "") +
    `\n📌 Choose an option:`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery("es_similar", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  state.editSettingsData.selectedIndices = new Set(state.editSettingsData.allGroups.map((_, i) => i));
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\n${state.editSettingsData.allGroups.length} groups selected.\n\nSab select ho gaye:`,
    { parse_mode: "HTML", reply_markup: buildEditSettingsGroupKeyboard(state) }
  );
});

bot.callbackQuery("es_clear_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  state.editSettingsData.selectedIndices = new Set();
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\nSab clear. Group(s) select karo:`,
    { parse_mode: "HTML", reply_markup: buildEditSettingsGroupKeyboard(state) }
  );
});

bot.callbackQuery("es_prev_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  if (state.editSettingsData.page > 0) state.editSettingsData.page--;
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\n<i>${state.editSettingsData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildEditSettingsGroupKeyboard(state) }
  );
});

bot.callbackQuery("es_next_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  const totalPages = Math.ceil(state.editSettingsData.allGroups.length / ES_PAGE_SIZE);
  if (state.editSettingsData.page < totalPages - 1) state.editSettingsData.page++;
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\n<i>${state.editSettingsData.selectedIndices.size} selected</i>`,
    { parse_mode: "HTML", reply_markup: buildEditSettingsGroupKeyboard(state) }
  );
});

bot.callbackQuery("es_page_info", async (ctx) => { ctx.answerCallbackQuery(); });

bot.callbackQuery("es_continue", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  if (state.editSettingsData.selectedIndices.size === 0) {
    ctx.answerCallbackQuery({ text: "⚠️ Koi group select nahi!" }); return;
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
    ctx.answerCallbackQuery();
    const state = userStates.get(ctx.from.id);
    if (!state?.editSettingsData) return;
    (state.editSettingsData.settings as any)[field] = !(state.editSettingsData.settings as any)[field];
    await ctx.editMessageText(editSettingsText(state.editSettingsData.settings), { parse_mode: "HTML", reply_markup: editSettingsKeyboard(state.editSettingsData.settings) });
  });
}

bot.callbackQuery("es_settings_done", async (ctx) => {
  ctx.answerCallbackQuery();
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
    ctx.answerCallbackQuery();
    const state = userStates.get(ctx.from.id);
    if (!state?.editSettingsData) return;
    state.editSettingsData.settings.disappearingMessages = dur;
    state.step = "edit_settings_dp";
    await ctx.editMessageText(
      "🖼️ <b>Group DP</b>\n\nSare selected groups mein DP lagana hai?\nPhoto bhejo ya skip karo.\n\n" +
      "• <b>Skip</b> — DP nahi badlega\n" +
      "• <b>Remove DP</b> — sabhi selected groups ki DP hata di jayegi",
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("⏭️ Skip", "es_dp_skip")
          .text("🗑️ Remove DP", "es_dp_remove").row()
          .text("❌ Cancel", "main_menu"),
      }
    );
  });
}

bot.callbackQuery("es_dp_skip", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  state.editSettingsData.settings.dpBuffers = [];
  state.editSettingsData.settings.removeDp = false;
  state.step = "edit_settings_desc";
  await ctx.editMessageText(
    "📄 <b>Group Description</b>\n\nSare selected groups mein description lagani hai?\nDescription bhejo ya skip karo.",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("⏭️ Skip", "es_desc_skip")
        .text("🗑️ Remove Description", "es_desc_remove").row()
        .text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("es_dp_remove", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  state.editSettingsData.settings.dpBuffers = [];
  state.editSettingsData.settings.removeDp = true;
  state.step = "edit_settings_desc";
  await ctx.editMessageText(
    "📄 <b>Group Description</b>\n\nSare selected groups mein description lagani hai?\nDescription bhejo ya skip karo.",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("⏭️ Skip", "es_desc_skip")
        .text("🗑️ Remove Description", "es_desc_remove").row()
        .text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("es_desc_skip", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  state.editSettingsData.settings.description = "";
  state.editSettingsData.settings.removeDescription = false;
  await showEditSettingsReview(ctx);
});

bot.callbackQuery("es_desc_remove", async (ctx) => {
  ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.editSettingsData) return;
  state.editSettingsData.settings.description = "";
  state.editSettingsData.settings.removeDescription = true;
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
  const dpLine = settings.dpBuffers.length > 0 ? "✅ Change" : settings.removeDp ? "🗑️ Remove" : "❌ Skip";
  const descLine = settings.removeDescription ? "🗑️ Remove" : settings.description ? esc(settings.description) : "Skip";
  const reviewText =
    "📋 <b>Edit Settings — Review</b>\n\n" +
    `📋 <b>Groups (${selectedGroups.length}):</b>\n${groupList}${moreText}\n\n` +
    `📄 Description: ${descLine}\n` +
    `🖼️ DP: ${dpLine}\n` +
    `⏳ Disappearing: ${dmText}\n\n` +
    "⚙️ <b>Permissions:</b>\n" +
    `${on(settings.editGroupInfo)} Edit Info | ${on(settings.sendMessages)} Send Msgs\n` +
    `${on(settings.addMembers)} Add Members | ${on(settings.approveJoin)} Approve Join\n\n` +
    "✅ Confirm to apply these settings to all selected groups:";
  const kb = new InlineKeyboard().text("✅ Apply to All Groups", "es_apply_confirm").text("❌ Cancel", "main_menu");
  try { await ctx.editMessageText(reviewText, { parse_mode: "HTML", reply_markup: kb }); }
  catch { await ctx.reply(reviewText, { parse_mode: "HTML", reply_markup: kb }); }
}

bot.callbackQuery("es_apply_confirm", async (ctx) => {
  ctx.answerCallbackQuery();
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
    `⏳ <b>Applying Settings...</b>\n\n🔄 0/${selectedGroups.length} done`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "es_cancel_apply") }
  );
  void applyEditSettingsBackground(String(userId), userId, settings, selectedGroups, chatId, msgId);
});

bot.callbackQuery("es_cancel_apply", async (ctx) => {
  ctx.answerCallbackQuery();
  await ctx.editMessageText(
    "⚠️ <b>Are you sure you want to cancel?</b>\n\nGroups already processed will not be reverted.",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Cancel", "es_cancel_confirm")
        .text("▶️ Continue", "es_cancel_dismiss"),
    }
  );
});

bot.callbackQuery("es_cancel_confirm", async (ctx) => {
  ctx.answerCallbackQuery({ text: "🛑 Cancelled!" });
  const state = userStates.get(ctx.from.id);
  if (state?.editSettingsData) state.editSettingsData.cancelled = true;
  await ctx.editMessageText("🛑 <b>Apply cancelled.</b>", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Menu", "main_menu") });
});

bot.callbackQuery("es_cancel_dismiss", async (ctx) => {
  ctx.answerCallbackQuery({ text: "▶️ Continuing..." });
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
      // Apply permissions + description (or remove description)
      if (settings.removeDescription) {
        // Remove description: first apply perms, then explicitly clear description
        await applyGroupSettings(userId, group.id, perms, "");
        await new Promise(r => setTimeout(r, 600));
        await removeGroupDescription(userId, group.id);
      } else {
        await applyGroupSettings(userId, group.id, perms, settings.description);
      }
      if (settings.disappearingMessages >= 0) {
        await new Promise(r => setTimeout(r, 800));
        await setGroupDisappearingMessages(userId, group.id, settings.disappearingMessages);
      }
      if (settings.removeDp) {
        await new Promise(r => setTimeout(r, 1500));
        await removeGroupIcon(userId, group.id);
      } else if (settings.dpBuffers.length > 0) {
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
        `⏳ <b>Applying Settings: ${done}/${total}</b>\n\n${lines}${done < total ? "\n\n⌛ Processing..." : ""}`,
        { parse_mode: "HTML", reply_markup: done < total ? new InlineKeyboard().text("❌ Cancel", "es_cancel_apply") : undefined }
      );
    } catch {}
    if (i < total - 1) await new Promise(r => setTimeout(r, 2000));
  }

  userStates.delete(numericUserId);
  const ok = results.filter(r => r.ok).length;
  const cancelled = results.some(r => r.error === "Cancelled");
  const header = cancelled ? `🛑 <b>Cancelled (${ok}/${total} done)</b>` : `🎉 <b>Done! (${ok}/${total} applied)</b>`;

  const on = (v: boolean) => v ? "✅ ON" : "❌ OFF";
  const dmLabel = settings.disappearingMessages === 86400 ? "24 Hours"
    : settings.disappearingMessages === 604800 ? "7 Days"
    : settings.disappearingMessages === 7776000 ? "90 Days"
    : "Off";
  const dpSummary = settings.dpBuffers.length > 0 ? "✅ Changed" : settings.removeDp ? "🗑️ Removed" : "❌ Skipped";
  const descSummary = settings.removeDescription ? "🗑️ Removed" : settings.description ? esc(settings.description) : "Skipped";
  const settingsSummary =
    `⚙️ <b>Settings Applied:</b>\n` +
    `📄 Description: ${descSummary}\n` +
    `🖼️ DP: ${dpSummary}\n` +
    `⏳ Disappearing Messages: ${dmLabel}\n` +
    `📝 Edit Group Info: ${on(settings.editGroupInfo)}\n` +
    `💬 Send Messages: ${on(settings.sendMessages)}\n` +
    `➕ Add Members: ${on(settings.addMembers)}\n` +
    `🔐 Approve Join: ${on(settings.approveJoin)}`;

  const finalLines = results.map(r => r.ok ? `✅ ${esc(r.name)}` : r.error === "Cancelled" ? `⛔ ${esc(r.name)} (skipped)` : `❌ ${esc(r.name)}: ${esc(r.error || "")}`).join("\n");
  try {
    await bot.api.editMessageText(chatId, msgId,
      `${header}\n\n${settingsSummary}\n\n📋 <b>Groups (${total}):</b>\n${finalLines}`, {
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
  ctx.answerCallbackQuery();
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
    .text("🔗 By Link", "cgn_by_link").row()
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  {
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  const data = state.changeGroupNameData;
  if ((data.page || 0) > 0) data.page = (data.page || 0) - 1;
  await renderCgnManualSelect(ctx);
});

bot.callbackQuery("cgn_m_next_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.selectionPool) return;
  const data = state.changeGroupNameData;
  const totalPages = Math.ceil(data.selectionPool!.length / CGN_PAGE_SIZE);
  if ((data.page || 0) < totalPages - 1) data.page = (data.page || 0) + 1;
  await renderCgnManualSelect(ctx);
});

bot.callbackQuery("cgn_m_page_info", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" });
});

bot.callbackQuery("cgn_m_select_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.selectionPool) return;
  const data = state.changeGroupNameData;
  data.selectedGroupIds = data.selectionPool!.map((g) => g.id);
  await renderCgnManualSelect(ctx);
});

bot.callbackQuery("cgn_m_clear_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  state.changeGroupNameData.selectedGroupIds = [];
  await renderCgnManualSelect(ctx);
});

bot.callbackQuery("cgn_m_proceed", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  {
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  const data = state.changeGroupNameData;
  if ((data.pendingPage || 0) > 0) data.pendingPage = (data.pendingPage || 0) - 1;
  await renderCgnAutoSelect(ctx);
});

bot.callbackQuery("cgn_a_next_page", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.pendingPool) return;
  const data = state.changeGroupNameData;
  const totalPages = Math.ceil(data.pendingPool!.length / CGN_PAGE_SIZE);
  if ((data.pendingPage || 0) < totalPages - 1) data.pendingPage = (data.pendingPage || 0) + 1;
  await renderCgnAutoSelect(ctx);
});

bot.callbackQuery("cgn_a_page_info", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Use Prev/Next to change page" });
});

bot.callbackQuery("cgn_a_select_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData?.pendingPool) return;
  const data = state.changeGroupNameData;
  data.pendingSelectedIds = data.pendingPool!.map((g) => g.groupId);
  await renderCgnAutoSelect(ctx);
});

bot.callbackQuery("cgn_a_clear_all", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  state.changeGroupNameData.pendingSelectedIds = [];
  await renderCgnAutoSelect(ctx);
});

bot.callbackQuery("cgn_a_proceed", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.changeGroupNameData) return;
  state.changeGroupNameData.autoNameMode = "same_vcf";
  await buildAndShowCgnAutoReview(ctx);
});

bot.callbackQuery("cgn_a_name_custom", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  cancelDialogActiveFor.add(ctx.from.id);
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard()
      .text("✅ Yes, Stop Renaming", "cgn_cancel_confirm")
      .text("↩️ Continue", "cgn_cancel_no"),
  });
});

bot.callbackQuery("cgn_cancel_no", async (ctx) => {
  cancelDialogActiveFor.delete(ctx.from.id);
  ctx.answerCallbackQuery({ text: "Renaming continued" });
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard().text("❌ Cancel", "cgn_cancel_request"),
  });
});

bot.callbackQuery("cgn_cancel_confirm", async (ctx) => {
  ctx.answerCallbackQuery({ text: "Stopping after current group..." });
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

    // Delay between renames to stay under WhatsApp's per-account rate limit
    // for group-subject updates. Combined with the in-`setGroupName` retry
    // logic, this should keep the failure rate near zero even on long batches.
    if (i < plan.length - 1) await new Promise((r) => setTimeout(r, 1500));
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  state.addMembersData.delaySeconds = 15;
  await showAddMembersReview(ctx, userId);
});

bot.callbackQuery("am_mode_together", async (ctx) => {
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
    ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery();
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
  ctx.answerCallbackQuery({ text: "⛔ Adding stopped!" });
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
    if (addedCount === 0) {
      const firstFail = Array.isArray(result) ? result.find(r => !r.success) : null;
      const reason = firstFail?.error || "Unknown reason";
      lines[lines.length - 1] = `❌ <b>${esc(group.name)}</b> — 0/${contacts.length} added (${esc(reason)})`;
    } else if (addedCount < contacts.length) {
      lines[lines.length - 1] = `⚠️ <b>${esc(group.name)}</b> — ${addedCount}/${contacts.length} added`;
    } else {
      lines[lines.length - 1] = `✅ <b>${esc(group.name)}</b> — ${addedCount}/${contacts.length} added`;
    }

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

// ─── Pair Code Helper ─────────────────────────────────────────────────────────
// Shared logic used by both awaiting_phone handler and the stateless phone
// number fallback (handles bot restarts between pressing Pair Code and typing).


// ─── Handle phone number input for Switch WhatsApp (Add new slot) ─────────────

async function handleSwitchWaPhone(ctx: any, userId: number, rawText: string): Promise<void> {
  const phone = "+" + rawText.replace(/[^0-9]/g, "");
  if (!/^\+\d{10,15}$/.test(phone)) {
    await ctx.reply(
      "❌ <b>Invalid phone number.</b>\n\nExample: <code>+919942222222</code>",
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("❌ Cancel", "switch_wa_menu"),
      }
    );
    return;
  }

  userStates.delete(userId);
  const uid = String(userId);

  const profile = await loadWaSwitchProfile(userId);

  // Duplicate check
  if (profile?.slots.some((s) => s.phone === phone)) {
    await ctx.reply(
      `ℹ️ <b>Already Added</b>\n\n<code>${esc(phone)}</code> is already in your saved WhatsApp list.`,
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard().text("🔀 Switch WhatsApp", "switch_wa_menu"),
      }
    );
    return;
  }

  // Calculate next slot number
  const existingSlotNums = (profile?.slots ?? [])
    .map((s) => { const m = s.id.match(/_sw_(\d+)$/); return m ? parseInt(m[1], 10) : 0; })
    .filter((n) => n > 0);
  const nextNum = existingSlotNums.length > 0 ? Math.max(...existingSlotNums) + 1 : 1;
  const slotId = `${uid}_sw_${nextNum}`;

  const statusMsg = await ctx.reply(
    `⏳ <b>Adding New WhatsApp...</b>\n\n📱 Phone: <code>${esc(phone)}</code>\n\n⌛ Getting pairing code, please wait 10-20 seconds...`,
    { parse_mode: "HTML" }
  );

  try {
    await connectWhatsApp(
      slotId,
      phone,
      async (code) => {
        try {
          await ctx.api.editMessageText(
            ctx.chat.id,
            statusMsg.message_id,
            `🔑 <b>Pairing Code for New WhatsApp:</b>\n\n<code>${esc(code)}</code>\n\n` +
            "📋 <b>Steps:</b>\n1️⃣ Open WhatsApp on your phone\n2️⃣ Settings → Linked Devices\n" +
            `3️⃣ Tap "Link a Device"\n4️⃣ Tap "Link with phone number instead"\n` +
            `5️⃣ Enter code: <code>${esc(code)}</code>\n\n⌛ Waiting for confirmation...`,
            { parse_mode: "HTML" }
          );
        } catch {}
      },
      async () => {
        // Successfully connected — save to profile
        try {
          const currentProfile = await loadWaSwitchProfile(userId) ?? {
            telegramId: userId,
            slots: [],
            activeId: uid,
          };

          // If user has no profile yet, auto-add their original primary WA as slot 0
          if (!currentProfile.slots.some((s) => s.id === uid)) {
            const storedSessions = await listStoredWhatsAppSessions().catch(() => [] as any[]);
            const primarySession = storedSessions.find((s: any) => s.userId === uid);
            const primaryPhone = primarySession?.phoneNumber ?? "Original WhatsApp";
            currentProfile.slots.unshift({ id: uid, phone: primaryPhone, addedAt: Date.now() });
          }

          // Add the new slot
          if (!currentProfile.slots.some((s) => s.id === slotId)) {
            currentProfile.slots.push({ id: slotId, phone, addedAt: Date.now() });
          }

          await saveWaSwitchProfile(currentProfile);

          await ctx.api.editMessageText(
            ctx.chat.id,
            statusMsg.message_id,
            `✅ <b>New WhatsApp Added!</b>\n\n` +
            `📱 <code>${esc(phone)}</code> has been saved to your accounts.\n\n` +
            "Tap it in the Switch WhatsApp menu to make it active.",
            {
              parse_mode: "HTML",
              reply_markup: new InlineKeyboard()
                .text("🔀 Switch WhatsApp", "switch_wa_menu")
                .text("🏠 Main Menu", "main_menu"),
            }
          );
        } catch (err: any) {
          console.error(`[SWITCH_WA] Failed to save profile for ${userId}:`, err?.message);
        }
      },
      async (reason) => {
        try {
          await ctx.api.editMessageText(
            ctx.chat.id,
            statusMsg.message_id,
            `⚠️ <b>WhatsApp Disconnected</b>\n\nReason: ${esc(reason)}\n\n🔄 Try adding it again.`,
            {
              parse_mode: "HTML",
              reply_markup: new InlineKeyboard()
                .text("➕ Add New WhatsApp", "switch_wa_add")
                .text("🏠 Menu", "main_menu"),
            }
          );
        } catch {}
      }
    );
  } catch (err: any) {
    console.error(`[SWITCH_WA] connectWhatsApp threw for slot ${slotId}:`, err?.message);
    try {
      await ctx.api.editMessageText(
        ctx.chat.id,
        statusMsg.message_id,
        `❌ <b>Connection Failed</b>\n\nError: ${esc(err?.message || "Unknown error")}\n\n🔄 Please try again.`,
        {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("➕ Add New WhatsApp", "switch_wa_add")
            .text("🏠 Menu", "main_menu"),
        }
      );
    } catch {}
  }
}

async function handlePairCodePhone(ctx: any, userId: number, rawText: string): Promise<void> {
  const phone = "+" + rawText.replace(/[^0-9]/g, "");
  if (!/^\+\d{10,15}$/.test(phone)) {
    await ctx.reply(
      "❌ <b>Invalid phone number.</b>\n\nExample: <code>+919942222222</code>\nYa: <code>+91 (9999) 222222</code>",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔑 Try Again", "connect_pair_code").text("❌ Cancel", "main_menu") }
    );
    return;
  }
  userStates.delete(userId);
  if (isConnected(String(userId))) {
    await ctx.reply("✅ <b>WhatsApp already connected!</b>\n\nYou can use all features.", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
    });
    return;
  }
  const statusMsg = await ctx.reply(
    `⏳ <b>Connecting...</b>\n\n📱 Phone: <code>${esc(phone)}</code>\n\n⌛ Getting pairing code, please wait 10-20 seconds...`,
    { parse_mode: "HTML" }
  );
  // ── Bug fix: clear switch-WA alias before reconnecting primary ──────────────
    // If the user previously switched to a secondary WhatsApp (setting an alias
    // uid → slotId), connectWhatsApp(uid) would resolve to the slot ID and store
    // the session there. When onConnected then resets the alias, isConnected(uid)
    // looks at uid directly — finds nothing — and shows "❌ WhatsApp: Not Connected"
    // even though the socket IS connected. Clearing the alias first ensures the
    // primary session is stored at uid so the status is always correct.
    clearSessionAlias(String(userId));

    // Sync the switch profile's activeId back to primary so the Switch WhatsApp
    // menu reflects the correct active account after reconnecting primary.
    try {
      const switchProfile = await loadWaSwitchProfile(userId);
      if (switchProfile && switchProfile.activeId !== String(userId)) {
        switchProfile.activeId = String(userId);
        await saveWaSwitchProfile(switchProfile);
      }
    } catch {}

    try {
    await connectWhatsApp(
      String(userId),
      phone,
      async (code) => {
        try {
          await ctx.api.editMessageText(
            ctx.chat.id,
            statusMsg.message_id,
            `🔑 <b>Pairing Code:</b>\n\n<code>${esc(code)}</code>\n\n` +
            `📋 <b>Steps:</b>\n1️⃣ Open WhatsApp on your phone\n2️⃣ Settings → Linked Devices\n` +
            `3️⃣ Tap "Link a Device"\n4️⃣ Tap "Link with phone number instead"\n` +
            `5️⃣ Enter code: <code>${esc(code)}</code>\n\n⌛ Waiting for confirmation...`,
            { parse_mode: "HTML" }
          );
        } catch {}
      },
      async () => {
        // If the newly connected phone matches a switch slot, remove the duplicate
        // so the same number doesn't appear in both primary and the switch list.
        try {
          const dedupProfile = await loadWaSwitchProfile(userId);
          if (dedupProfile) {
            const before = dedupProfile.slots.length;
            dedupProfile.slots = dedupProfile.slots.filter((s) => s.phone !== phone && s.id !== String(userId));
            if (dedupProfile.slots.length !== before) {
              // If the active slot was the removed one, reset to primary
              if (!dedupProfile.slots.find((s) => s.id === dedupProfile.activeId)) {
                dedupProfile.activeId = String(userId);
                clearSessionAlias(String(userId));
              }
              await saveWaSwitchProfile(dedupProfile);
            }
          }
        } catch {}
        try {
          await ctx.api.editMessageText(
            ctx.chat.id,
            statusMsg.message_id,
            whatsappConnectedText(userId, "🎉 All features are now available."),
            { parse_mode: "HTML", reply_markup: mainMenu(userId) }
          );
        } catch {}
      },
      async (reason) => {
        try {
          await ctx.api.editMessageText(
            ctx.chat.id,
            statusMsg.message_id,
            `⚠️ <b>WhatsApp Disconnected</b>\n\nReason: ${esc(reason)}\n\n🔄 Try connecting again.`,
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect Again", "connect_wa").text("🏠 Menu", "main_menu") }
          );
        } catch {}
      }
    );
  } catch (err: any) {
    console.error(`[BOT] connectWhatsApp threw for user ${userId}:`, err?.message);
    try {
      await ctx.api.editMessageText(
        ctx.chat.id,
        statusMsg.message_id,
        `❌ <b>Connection Failed</b>\n\nError: ${esc(err?.message || "Unknown error")}\n\n🔄 Please try again.`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Try Again", "connect_wa").text("🏠 Menu", "main_menu") }
      );
    } catch {}
  }
}

// ─── Text Handler ─────────────────────────────────────────────────────────────

bot.on("message:text", async (ctx, next) => {
  const userId = ctx.from.id;
  await trackUser(userId);
  if (await isBanned(userId)) return;
  const state = userStates.get(userId);
  const text = ctx.message.text.trim();

  // ── By-link accumulation steps — must be handled before other state checks ──
  if (state) {
    if (state.step === "lv_enter_links_bl") {
      const links = extractLinksFromText(text);
      if (!links.length) return;
      if (!state.lvLinkBuffer) state.lvLinkBuffer = [];
      state.lvLinkBuffer.push(...links);
      const total = state.lvLinkBuffer.length;
      const prompt = byLinkPrompt("Leave Group", "🚪", total, "lv_links_done");
      const kb = new InlineKeyboard().text("✅ Done", "lv_links_done").row().text("❌ Cancel", "main_menu");
      const existingId = lvLinkCollectMsgId.get(userId);
      if (existingId) {
        try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
      }
      const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
      lvLinkCollectMsgId.set(userId, m.message_id);
      return;
    }

    if (state.step === "rm_enter_links_bl") {
      const links = extractLinksFromText(text);
      if (!links.length) return;
      if (!state.rmLinkBuffer) state.rmLinkBuffer = [];
      state.rmLinkBuffer.push(...links);
      const total = state.rmLinkBuffer.length;
      const prompt = byLinkPrompt("Remove Members", "🗑️", total, "rm_links_done");
      const kb = new InlineKeyboard().text("✅ Done", "rm_links_done").row().text("❌ Cancel", "main_menu");
      const existingId = rmLinkCollectMsgId.get(userId);
      if (existingId) {
        try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
      }
      const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
      rmLinkCollectMsgId.set(userId, m.message_id);
      return;
    }

    // ── /file — VCF File Tools text step handlers ──────────────────────────────
    if (state.step === "fe_step1") {
      // VCF Editor: base file name
      const d = state.fileEditorData;
      if (!d) return;
      d.baseName = text.replace(/[<>:"/\\|?*]/g, "").trim();
      if (!d.baseName) { await ctx.reply("❌ Invalid name. Please enter a valid file name:"); return; }
      state.step = "fe_step2";
      await ctx.reply(
        `✅ File name: <code>${esc(d.baseName)}</code>\n\n` +
        "━━━━━━━━━━━━━━━━━━━━━━━\n" +
        "📝 <b>Step 2/5: Enter the contact base name</b>\n\n" +
        "<b>Example:</b> <code>SPIDY</code>\n\n" +
        "Contacts will be named as: <code>SPIDY 01</code>, <code>SPIDY 02</code>, etc.",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ft_menu") }
      );
      return;
    }

    if (state.step === "fe_step2") {
      // VCF Editor: contact base name
      const d = state.fileEditorData;
      if (!d) return;
      d.contactName = text.trim();
      if (!d.contactName) { await ctx.reply("❌ Invalid name. Please enter a contact base name:"); return; }
      state.step = "fe_step3";
      await ctx.reply(
        `✅ Contact name: <code>${esc(d.contactName)}</code>\n\n` +
        "━━━━━━━━━━━━━━━━━━━━━━━\n" +
        "📝 <b>Step 3/5: Enter the starting file number</b>\n\n" +
        "<b>Example:</b> <code>1</code>\n\n" +
        "First file will be: <code>${d.baseName ?? ''} 1.vcf</code>",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ft_menu") }
      );
      return;
    }

    if (state.step === "fe_step3") {
      // VCF Editor: starting file number
      const d = state.fileEditorData;
      if (!d) return;
      const num = parseInt(text.trim(), 10);
      if (isNaN(num) || num < 1) { await ctx.reply("❌ Please enter a valid positive number:"); return; }
      d.startFileNum = num;
      state.step = "fe_step4";
      await ctx.reply(
        `✅ Start file number: <b>${num}</b>\n\n` +
        "━━━━━━━━━━━━━━━━━━━━━━━\n" +
        "📝 <b>Step 4/5: Enter the starting contact number</b>\n\n" +
        "<b>Example:</b> <code>1</code>\n\n" +
        "First contact will be: <code>${d.contactName ?? ''} 01</code>",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ft_menu") }
      );
      return;
    }

    if (state.step === "fe_step4") {
      // VCF Editor: starting contact number
      const d = state.fileEditorData;
      if (!d) return;
      const num = parseInt(text.trim(), 10);
      if (isNaN(num) || num < 1) { await ctx.reply("❌ Please enter a valid positive number:"); return; }
      d.contactStartNum = num;
      state.step = "fe_step5";
      const total = d.contactsGroups.reduce((s, g) => s + g.length, 0);
      await ctx.reply(
        `✅ Start contact number: <b>${num}</b>\n\n` +
        "━━━━━━━━━━━━━━━━━━━━━━━\n" +
        "📝 <b>Step 5/5: How many contacts per VCF file?</b>\n\n" +
        `Total contacts: <b>${total}</b>\n\n` +
        "<b>Example:</b> <code>500</code> → each VCF will have 500 contacts",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ft_menu") }
      );
      return;
    }

    if (state.step === "fe_step5") {
      // VCF Editor: contacts per file
      const d = state.fileEditorData;
      if (!d) return;
      const num = parseInt(text.trim(), 10);
      if (isNaN(num) || num < 1) { await ctx.reply("❌ Please enter a valid positive number:"); return; }
      d.contactsPerFile = num;
      state.step = "fe_confirm";
      const allPhones: string[] = ([] as string[]).concat(...d.contactsGroups);
      const numFiles = Math.ceil(allPhones.length / num);
      const endFileNum = (d.startFileNum ?? 1) + numFiles - 1;
      await ctx.reply(
        "━━━━━━━━━━━━━━━━━━━━━━━\n" +
        "✅ <b>Summary — Please Confirm</b>\n" +
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n" +
        `📄 File name: <code>${esc(d.baseName ?? "")}</code>\n` +
        `👤 Contact name: <code>${esc(d.contactName ?? "")}</code>\n` +
        `🔢 Start file number: <b>${d.startFileNum}</b>\n` +
        `🔢 Start contact number: <b>${d.contactStartNum}</b>\n` +
        `📊 Contacts per file: <b>${num}</b>\n\n` +
        `📞 Total contacts: <b>${allPhones.length}</b>\n` +
        `📁 Files to create: <b>${numFiles}</b> (${d.baseName} ${d.startFileNum}.vcf → ${d.baseName} ${endFileNum}.vcf)\n\n` +
        "Tap <b>✅ Confirm & Generate</b> to proceed:",
        {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("✅ Confirm & Generate", "fe_confirm").row()
            .text("❌ Cancel", "ft_menu"),
        }
      );
      return;
    }

    // ── Splitter: contacts per split ──────────────────────────────────────────
    if (state.step === "fs_count") {
      const d = state.fileEditorData;
      if (!d) return;
      const num = parseInt(text.trim(), 10);
      if (isNaN(num) || num < 1) { await ctx.reply("❌ Please enter a valid positive number:"); return; }
      // Snapshot only what we need, then delete state to free RAM
      const allPhones = d.contactsGroups[0] ?? [];
      const ext = d.fileExts[0] ?? ".vcf";
      const baseName = (d.fileNames[0] ?? "file").replace(/\.[^.]+$/, "");
      const totalContacts = allPhones.length;
      const totalFiles = Math.ceil(totalContacts / num);
      userStates.delete(userId);

      const progressMsg = await ctx.reply(
        `⚡ <b>Splitting into ${totalFiles} file(s)...</b>\n📞 ${num} contacts each`,
        { parse_mode: "HTML" }
      );
      const progMsgId = totalFiles > 20 ? progressMsg.message_id : null;

      // Send strictly one-by-one to guarantee delivery order (1, 2, 3…).
      // Each Buffer is built, sent, then immediately GC'd before the next.
      const UPDATE_EVERY = 50;
      for (let idx = 0; idx < totalFiles; idx++) {
        const start = idx * num;
        const phones = allPhones.slice(start, Math.min(start + num, totalContacts));
        const content = buildSplitContent(phones, ext);
        const outName = `${baseName} ${idx + 1}${ext}`;
        await retryTgApi(() => bot.api.sendDocument(userId, new InputFile(Buffer.from(content, "utf8"), outName)));
        const sent = idx + 1;
        if (progMsgId && sent < totalFiles && sent % UPDATE_EVERY === 0) {
          const pct = Math.round((sent / totalFiles) * 100);
          bot.api.editMessageText(userId, progMsgId,
            `⚡ <b>Splitting...</b> ${sent} / ${totalFiles} files sent\n📊 Progress: <b>${pct}%</b>`,
            { parse_mode: "HTML" }
          ).catch(() => {});
        }
      }

      await bot.api.sendMessage(
        userId,
        "✅ <b>Split Complete!</b>\n\n" +
        `📊 Total contacts: <b>${totalContacts}</b>\n` +
        `✂️ Files created: <b>${totalFiles}</b>\n` +
        `📞 Contacts per file: <b>${num}</b>`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📁 File Tools", "ft_menu").text("🏠 Main Menu", "main_menu") }
      );
      if (progMsgId) {
        bot.api.deleteMessage(userId, progMsgId).catch(() => {});
      }
      return;
    }

    // ── Number → VCF: paste numbers ───────────────────────────────────────────
    if (state.step === "fn_numbers") {
      const d = state.fileEditorData;
      if (!d) return;
      const { extractPhonesFromText: extractText } = await import("./file-tools");
      const phones = extractText(text);
      if (!phones.length) {
        await ctx.reply("❌ No valid phone numbers found. Please paste numbers (one per line):");
        return;
      }
      d.contactsGroups = [phones];
      state.step = "fn_step1";
      await ctx.reply(
        `✅ <b>${phones.length} numbers detected</b>\n\n` +
        "━━━━━━━━━━━━━━━━━━━━━━━\n" +
        "📝 <b>Step 1/2: Enter the output file name</b>\n\n" +
        "<b>Example:</b> <code>SPIDY</code>\n\n" +
        "File will be saved as: <code>SPIDY.vcf</code>",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ft_menu") }
      );
      return;
    }

    if (state.step === "fn_step1") {
      const d = state.fileEditorData;
      if (!d) return;
      d.baseName = text.replace(/[<>:"/\\|?*]/g, "").trim();
      if (!d.baseName) { await ctx.reply("❌ Invalid name. Please enter a valid file name:"); return; }
      state.step = "fn_step2";
      await ctx.reply(
        `✅ File name: <code>${esc(d.baseName)}.vcf</code>\n\n` +
        "━━━━━━━━━━━━━━━━━━━━━━━\n" +
        "📝 <b>Step 2/2: Enter the contact base name</b>\n\n" +
        "<b>Example:</b> <code>SPIDY</code>\n\n" +
        "Contacts will be named: <code>SPIDY 01</code>, <code>SPIDY 02</code>, etc.",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ft_menu") }
      );
      return;
    }

    if (state.step === "fn_step2") {
      const d = state.fileEditorData;
      if (!d) return;
      d.contactName = text.trim();
      if (!d.contactName) { await ctx.reply("❌ Invalid name. Please enter a contact base name:"); return; }
      state.step = "fn_confirm";
      const phones = d.contactsGroups[0] ?? [];
      await ctx.reply(
        "━━━━━━━━━━━━━━━━━━━━━━━\n" +
        "✅ <b>Summary — Please Confirm</b>\n" +
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n" +
        `📄 File name: <code>${esc(d.baseName ?? "")}.vcf</code>\n` +
        `👤 Contact name: <code>${esc(d.contactName)}</code>\n` +
        `📞 Total contacts: <b>${phones.length}</b>\n\n` +
        "Tap <b>✅ Generate VCF</b> to proceed:",
        {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("✅ Generate VCF", "fn_confirm").row()
            .text("❌ Cancel", "ft_menu"),
        }
      );
      return;
    }

    if (state.step === "ap_enter_links_bl") {
      const links = extractLinksFromText(text);
      if (!links.length) return;
      if (!state.apLinkBuffer) state.apLinkBuffer = [];
      state.apLinkBuffer.push(...links);
      const total = state.apLinkBuffer.length;
      const prompt = byLinkPrompt("Approval", "✅", total, "ap_links_done");
      const kb = new InlineKeyboard().text("✅ Done", "ap_links_done").row().text("❌ Cancel", "main_menu");
      const existingId = apLinkCollectMsgId.get(userId);
      if (existingId) {
        try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
      }
      const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
      apLinkCollectMsgId.set(userId, m.message_id);
      return;
    }

    if (state.step === "ma_enter_links_bl") {
      const links = extractLinksFromText(text);
      if (!links.length) return;
      if (!state.maLinkBuffer) state.maLinkBuffer = [];
      state.maLinkBuffer.push(...links);
      const total = state.maLinkBuffer.length;
      const prompt = byLinkPrompt("Make Admin", "👑", total, "ma_links_done");
      const kb = new InlineKeyboard().text("✅ Done", "ma_links_done").row().text("❌ Cancel", "main_menu");
      const existingId = maLinkCollectMsgId.get(userId);
      if (existingId) {
        try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
      }
      const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
      maLinkCollectMsgId.set(userId, m.message_id);
      return;
    }

    if (state.step === "da_enter_links_bl") {
      const links = extractLinksFromText(text);
      if (!links.length) return;
      if (!state.daLinkBuffer) state.daLinkBuffer = [];
      state.daLinkBuffer.push(...links);
      const total = state.daLinkBuffer.length;
      const prompt = byLinkPrompt("Demote Admin", "👤", total, "da_links_done");
      const kb = new InlineKeyboard().text("✅ Done", "da_links_done").row().text("❌ Cancel", "main_menu");
      const existingId = daLinkCollectMsgId.get(userId);
      if (existingId) {
        try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
      }
      const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
      daLinkCollectMsgId.set(userId, m.message_id);
      return;
    }

    if (state.step === "es_enter_links_bl") {
      const links = extractLinksFromText(text);
      if (!links.length) return;
      if (!state.esLinkBuffer) state.esLinkBuffer = [];
      state.esLinkBuffer.push(...links);
      const total = state.esLinkBuffer.length;
      const prompt = byLinkPrompt("Edit Settings", "⚙️", total, "es_links_done");
      const kb = new InlineKeyboard().text("✅ Done", "es_links_done").row().text("❌ Cancel", "main_menu");
      const existingId = esLinkCollectMsgId.get(userId);
      if (existingId) {
        try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
      }
      const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
      esLinkCollectMsgId.set(userId, m.message_id);
      return;
    }

    if (state.step === "cgn_enter_links_bl") {
      const links = extractLinksFromText(text);
      if (!links.length) return;
      if (!state.cgnLinkBuffer) state.cgnLinkBuffer = [];
      state.cgnLinkBuffer.push(...links);
      const total = state.cgnLinkBuffer.length;
      const prompt = byLinkPrompt("Change Group Name", "🏷️", total, "cgn_links_done");
      const kb = new InlineKeyboard().text("✅ Done", "cgn_links_done").row().text("❌ Cancel", "main_menu");
      const existingId = cgnLinkCollectMsgId.get(userId);
      if (existingId) {
        try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
      }
      const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
      cgnLinkCollectMsgId.set(userId, m.message_id);
      return;
    }
  }
  // ── End by-link accumulation steps ──

  if (!state) {
    // Let command handlers (/file, /help, /start, etc.) fire — don't block them here.
    if (text.startsWith("/")) return next();

    if (text.toLowerCase() === "start") {
      if (await isBanned(userId)) return;
      if (await hasAccess(userId)) {
        await ctx.reply(
          mainMenuText(userId, "welcome", ctx.from?.first_name),
          { parse_mode: "HTML", reply_markup: mainMenu(userId) }
        );
      } else {
        await ctx.reply(
          "💬 <b>Session expired.</b> Please use /start to open the menu.",
          { parse_mode: "HTML" }
        );
      }
      return;
    }

    // ── Phone number fallback — handles bot restart between "Pair Code" tap and number input
    // If the user's state was wiped (server restart) but they're typing a phone number,
    // gracefully re-enter the pairing flow instead of showing a dead-end message.
    const digits = text.replace(/[^0-9]/g, "");
    if (digits.length >= 10 && digits.length <= 15 && (text.startsWith("+") || text.match(/^\d/))) {
      if (await hasAccess(userId)) {
        await handlePairCodePhone(ctx, userId, text);
        return;
      }
    }

    await ctx.reply(
      "💬 <b>Session expired.</b> Please use /start to open the menu.",
      { parse_mode: "HTML" }
    );
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
    await handlePairCodePhone(ctx, userId, text);
    return;
  }

  if (state.step === "switch_wa_phone") {
    await handleSwitchWaPhone(ctx, userId, text);
    return;
  }

  if (state.step === "auto_connect_phone") {
    const phone = text.replace(/\s/g, "");
    if (!/^\+?\d{10,15}$/.test(phone)) {
      await ctx.reply("❌ Invalid phone number.\nExample: <code>919876543210</code>", { parse_mode: "HTML" }); return;
    }
    const slot = state.autoSlot ?? 1;
    const autoUserId = getAutoSlotUserId(String(userId), slot);
    const slotLabel = slot === 1 ? "Auto Chat WA" : `WA ${slot + 1}`;
    userStates.delete(userId);
    const statusMsg = await ctx.reply(
      `⏳ <b>${slotLabel} Connecting...</b>\n\n📱 Phone: <code>${esc(phone)}</code>\n\n⌛ Pairing code aa raha hai, 10-20 seconds wait karo...`,
      { parse_mode: "HTML" }
    );
    try {
      await connectWhatsApp(autoUserId, phone,
        async (code) => {
          try {
            await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
              `🔑 <b>${slotLabel} Pairing Code:</b>\n\n<code>${esc(code)}</code>\n\n` +
              `📋 <b>Steps:</b>\n1️⃣ ${slotLabel} ka WhatsApp open karo\n2️⃣ Settings → Linked Devices\n` +
              `3️⃣ Tap "Link a Device"\n4️⃣ Tap "Link with phone number instead"\n` +
              `5️⃣ Code enter karo: <code>${esc(code)}</code>\n\n⌛ Confirm hone ka wait kar raha hun...`,
              { parse_mode: "HTML" }
            );
          } catch {}
        },
        async () => {
          try {
            // Get the number that just connected for this slot
            const connectedNum = slot === 1
              ? getAutoConnectedNumber(String(userId))
              : getConnectedWhatsAppNumber(autoUserId);
            await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
              `✅ <b>${slotLabel} Connected!</b>\n\n` +
              (connectedNum ? `📞 Number: <code>${esc(connectedNum)}</code>\n\n` : "") +
              `🎉 Ab Auto Chat use kar sakte ho!\n` +
              (slot > 1 ? `📱 WA ${slot + 1} ready for Chat Friend.` : ""),
              { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🤖 Auto Chat Menu", "auto_chat_menu").text("🏠 Menu", "main_menu") }
            );
          } catch {}
        },
        async (reason) => {
          try {
            await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
              `⚠️ <b>${slotLabel} Disconnected</b>\n\nReason: ${esc(reason)}\n\n🔄 Dobara try karo.`,
              { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🤖 Auto Chat Menu", "auto_chat_menu").text("🏠 Menu", "main_menu") }
            );
          } catch {}
        }
      );
    } catch (err: any) {
      console.error(`[BOT] auto connectWhatsApp threw for user ${userId} slot ${slot}:`, err?.message);
      try {
        await ctx.api.editMessageText(ctx.chat.id, statusMsg.message_id,
          `❌ <b>Connection Failed</b>\n\nError: ${esc(err?.message || "Unknown error")}\n\n🔄 Please try again.`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🤖 Auto Chat Menu", "auto_chat_menu").text("🏠 Menu", "main_menu") }
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
      await ctx.reply("📄 <b>Group Description</b>\n\nSend a description, or press the <b>⏭️ Skip</b> button to leave it blank:", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "group_skip_description").text("❌ Cancel", "main_menu") });
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
      `✅ <b>Names saved:</b>\n${preview}${names.length > 5 ? `\n... +${names.length - 5} more` : ""}\n\n📄 <b>Group Description</b>\n\nSend a description, or press the <b>⏭️ Skip</b> button to leave it blank:`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "group_skip_description").text("❌ Cancel", "main_menu") }
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

  if (state.step === "sg_enter_links") {
    if (!isAdmin(userId)) return;
    const cleanLinks = extractLinksFromText(text);
    if (!cleanLinks.length) {
      await ctx.reply(
        "❌ No valid WhatsApp group links found.\nExample:\n<code>https://chat.whatsapp.com/ABC123</code>",
        { parse_mode: "HTML" }
      );
      return;
    }
    if (!state.sgLinkBuffer) state.sgLinkBuffer = [];
    state.sgLinkBuffer.push(...cleanLinks);
    const total = state.sgLinkBuffer.length;
    const sgCollectMsgId = sgLinkCollectMsgId.get(userId);
    const sgCollectText =
      "☠️ <b>Steal Group — By Link</b>\n\n" +
      `📎 <b>${total} link(s) collected</b>\n\n` +
      "Send more links, or tap <b>Done</b> to proceed:\n" +
      "<code>https://chat.whatsapp.com/ABC123</code>";
    const sgCollectKb = new InlineKeyboard()
      .text("✅ Done", "sg_link_done").row()
      .text("❌ Cancel", "main_menu");
    if (sgCollectMsgId) {
      try {
        await bot.api.editMessageText(ctx.chat.id, sgCollectMsgId, sgCollectText, {
          parse_mode: "HTML",
          reply_markup: sgCollectKb,
        });
      } catch {
        const newMsg = await ctx.reply(sgCollectText, { parse_mode: "HTML", reply_markup: sgCollectKb });
        sgLinkCollectMsgId.set(userId, newMsg.message_id);
      }
    } else {
      const newMsg = await ctx.reply(sgCollectText, { parse_mode: "HTML", reply_markup: sgCollectKb });
      sgLinkCollectMsgId.set(userId, newMsg.message_id);
    }
    return;
  }

  if (state.step === "rl_enter_links") {
    if (!state.resetLinkData) return;
    const cleanLinks = extractLinksFromText(text);
    if (!cleanLinks.length) {
      await ctx.reply(
        "❌ No valid WhatsApp group links found.\nExample:\n<code>https://chat.whatsapp.com/ABC123</code>",
        { parse_mode: "HTML" }
      );
      return;
    }

    // ── Collect mode: buffer links until user clicks Done ───────────────────
    if (!state.rlLinkBuffer) state.rlLinkBuffer = [];
    state.rlLinkBuffer.push(...cleanLinks);
    const total = state.rlLinkBuffer.length;
    const collectMsgId = rlLinkCollectMsgId.get(userId);
    const collectText =
      "🔗 <b>Reset by Group Link</b>\n\n" +
      `📎 <b>${total} link(s) collected</b>\n\n` +
      "Send more links, or tap <b>Done</b> to proceed:\n" +
      "<code>https://chat.whatsapp.com/ABC123</code>";
    const collectKb = new InlineKeyboard()
      .text("✅ Done", "rl_link_done").row()
      .text("❌ Cancel", "main_menu");
    if (collectMsgId) {
      // Edit the existing collect message in-place — no delete, no race condition
      try {
        await bot.api.editMessageText(ctx.chat.id, collectMsgId, collectText, {
          parse_mode: "HTML",
          reply_markup: collectKb,
        });
      } catch {
        // Edit failed (message too old etc.) — send a fresh one
        const newMsg = await ctx.reply(collectText, { parse_mode: "HTML", reply_markup: collectKb });
        rlLinkCollectMsgId.set(userId, newMsg.message_id);
      }
    } else {
      const newMsg = await ctx.reply(collectText, { parse_mode: "HTML", reply_markup: collectKb });
      rlLinkCollectMsgId.set(userId, newMsg.message_id);
    }
    return;
  }

  if (state.step === "join_enter_links") {
    if (!state.joinData) return;
    const cleanLinks = extractLinksFromText(text);
    if (!cleanLinks.length) {
      await ctx.reply("❌ No valid WhatsApp links found.\nExample:\n<code>https://chat.whatsapp.com/ABC123</code>", { parse_mode: "HTML" });
      return;
    }

    const existing = joinSessions.get(userId);
    if (existing && !existing.cancelled) {
      // ── Batch mode: append new links to the running session ────────────────
      existing.queue.push(...cleanLinks);
      const total = existing.done + existing.queue.length;
      await ctx.reply(
        `➕ <b>${cleanLinks.length} link(s) added to queue!</b>\n\n` +
        `✅ Already done: <b>${existing.done}</b>\n` +
        `⌛ Remaining in queue: <b>${existing.queue.length}</b>\n` +
        `📋 Total: <b>${total}</b>`,
        { parse_mode: "HTML" }
      );
      // Wake up the runner in case it finished and new links arrived
      void runJoinBackground(userId);
      return;
    }

    // ── New session ─────────────────────────────────────────────────────────
    joinCancelRequests.delete(userId);
    // Keep user in join_enter_links step so they can send more links mid-run
    const statusMsg = await ctx.reply(
      `⏳ <b>Joining ${cleanLinks.length} group(s)...</b>\n\n` +
      buildJoinProgressBar(0, cleanLinks.length),
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "join_cancel_request") }
    );
    const session: JoinSession = {
      chatId: ctx.chat.id,
      msgId: statusMsg.message_id,
      queue: [...cleanLinks],
      done: 0,
      results: [],
      failedLinks: [],
      running: false,
      cancelled: false,
    };
    joinSessions.set(userId, session);
    void runJoinBackground(userId);
    return;
  }

  if (state?.step === "ctc_enter_links" || (!state && (await getOrLoadUserState(userId))?.step === "ctc_enter_links")) {
    const state2 = state?.step === "ctc_enter_links" ? state : userStates.get(userId)!;
    if (!state2?.ctcData) return;
    const cleanLinks = extractLinksFromText(text);
    if (!cleanLinks.length) { await ctx.reply("❌ No valid WhatsApp links found.", { parse_mode: "HTML" }); return; }
    // Accumulate links (1-by-1 mode like rl_enter_links)
    if (!state2.ctcLinkBuffer) state2.ctcLinkBuffer = [];
    state2.ctcLinkBuffer.push(...cleanLinks);
    const total = state2.ctcLinkBuffer.length;
    const ctcCollectMsgId = ctcLinkCollectMsgId.get(userId);
    const ctcCollectText =
      "🔍 <b>CTC Checker — Group Links</b>\n\n" +
      `📎 <b>${total} link(s) collected</b>\n\n` +
      "Send more links, or tap <b>Done</b> to proceed:\n" +
      "<code>https://chat.whatsapp.com/ABC123</code>";
    const ctcCollectKb = new InlineKeyboard()
      .text("✅ Done", "ctc_links_done").row()
      .text("❌ Cancel", "main_menu");
    if (ctcCollectMsgId) {
      try {
        await bot.api.editMessageText(ctx.chat.id, ctcCollectMsgId, ctcCollectText, { parse_mode: "HTML", reply_markup: ctcCollectKb });
      } catch {
        const nm = await ctx.reply(ctcCollectText, { parse_mode: "HTML", reply_markup: ctcCollectKb });
        ctcLinkCollectMsgId.set(userId, nm.message_id);
      }
    } else {
      const nm = await ctx.reply(ctcCollectText, { parse_mode: "HTML", reply_markup: ctcCollectKb });
      ctcLinkCollectMsgId.set(userId, nm.message_id);
    }
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
        "❌ No valid input found.\n\n" +
        "• Send a full number with country code (e.g. <code>+919912345678</code>), or\n" +
        "• Send only a country code (1–4 digits, e.g. <code>+91</code> or <code>91</code>)\n\n" +
        "Or tap Skip to exclude nobody.",
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
      const prefList = Array.from(excludePrefixes).map(p => `• +${esc(p)} <i>(all numbers from this country code)</i>`).join("\n");
      sections.push(`🌐 <b>${excludePrefixes.size} country code(s) will be excluded:</b>\n\n${prefList}`);
    }

    await ctx.reply(
      sections.join("\n\n") +
      `\n\n⚠️ These numbers will NOT be removed from the groups.`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("✅ Confirm & Start", "rm_confirm_with_exclude").text("❌ Cancel", "main_menu") }
    );
    state.removeExcludeData.excludeNumbers = excludeNumbers;
    state.removeExcludeData.excludePrefixes = excludePrefixes;
    state.step = "remove_exclude_confirm";
    return;
  }

  // Handle phone numbers input for Remove Friend
  if (state.step === "remove_friend_enter_numbers") {
    if (!state.removeFriendData) return;
    const inputLines = text.split("\n").map(l => l.trim()).filter(l => l.length > 0);
    const fullNums: string[] = [];
    const ccNums: string[] = [];
    for (const line of inputLines) {
      const digits = line.replace(/[^0-9]/g, "");
      if (digits.length >= 1 && digits.length <= 4) {
        if (!ccNums.includes(digits)) ccNums.push(digits);
      } else if (digits.length >= 7) {
        fullNums.push(digits);
      }
    }
    if (fullNums.length === 0 && ccNums.length === 0) {
      await ctx.reply(
        "❌ No valid input found.\n\n" +
        "Send numbers or country codes (one per line):\n" +
        "<code>+919912345678</code>  ← removes only this number\n" +
        "<code>+91</code>  ← removes ALL non-admin members from India",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
      );
      return;
    }
    state.removeFriendData.phoneNumbers = fullNums;
    state.removeFriendData.countryCodes = ccNums;
    const { selectedGroups } = state.removeFriendData;
    const groupList = selectedGroups.slice(0, 10).map(g => `• ${esc(g.subject)}`).join("\n");
    const moreGroups = selectedGroups.length > 10 ? `\n<i>...+${selectedGroups.length - 10} more</i>` : "";

    if (ccNums.length > 0) {
      state.step = "remove_friend_enter_exceptions";
      const ccList = ccNums.map(c => `+${c}`).join(", ");
      await ctx.reply(
        `🌍 <b>Country Code Removal</b>\n\n` +
        `<b>${selectedGroups.length} group(s):</b>\n${groupList}${moreGroups}\n\n` +
        `Country code(s) detected: <b>${ccList}</b>\n` +
        (fullNums.length > 0 ? `Full numbers to also remove: <b>${fullNums.length}</b>\n` : "") +
        `\nAll non-admin members with these country codes will be removed.\n\n` +
        `Are there any numbers you want to <b>KEEP</b> (NOT remove)?\n` +
        `Send them one per line, or tap <b>Skip</b>:`,
        {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("⏭️ Skip", "rf_skip_exceptions")
            .text("❌ Cancel", "main_menu"),
        }
      );
      return;
    }

    // Full-number-only mode — show confirm directly
    const numList = fullNums.slice(0, 15).map(p => `• +${esc(p)}`).join("\n");
    const moreNums = fullNums.length > 15 ? `\n<i>...+${fullNums.length - 15} more</i>` : "";
    await ctx.reply(
      `👥 <b>Remove Friend — Confirm</b>\n\n` +
      `<b>${selectedGroups.length} group(s):</b>\n${groupList}${moreGroups}\n\n` +
      `<b>${fullNums.length} number(s) to remove:</b>\n${numList}${moreNums}\n\n` +
      `🛡️ Admins will NOT be removed.\n\nConfirm?`,
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("✅ Confirm & Remove", "rf_confirm")
          .text("❌ Cancel", "main_menu"),
      }
    );
    return;
  }

  if (state.step === "remove_friend_enter_exceptions") {
    if (!state.removeFriendData) return;
    const excLines = text.split("\n").map(l => l.trim()).filter(l => l.length > 0);
    const exceptions: string[] = [];
    for (const line of excLines) {
      const digits = line.replace(/[^0-9]/g, "");
      if (digits.length >= 7) exceptions.push(digits);
    }
    state.removeFriendData.exceptions = exceptions;
    const { selectedGroups, countryCodes = [], phoneNumbers = [] } = state.removeFriendData;
    const groupList = selectedGroups.slice(0, 10).map(g => `• ${esc(g.subject)}`).join("\n");
    const moreGroups = selectedGroups.length > 10 ? `\n<i>...+${selectedGroups.length - 10} more</i>` : "";
    const ccList = countryCodes.map(c => `+${c}`).join(", ");
    const excList = exceptions.slice(0, 10).map(e => `• +${esc(e)}`).join("\n");
    const moreExc = exceptions.length > 10 ? `\n<i>...+${exceptions.length - 10} more</i>` : "";
    await ctx.reply(
      `👥 <b>Remove Friend — Confirm</b>\n\n` +
      `<b>${selectedGroups.length} group(s):</b>\n${groupList}${moreGroups}\n\n` +
      (phoneNumbers.length > 0 ? `<b>Full numbers to remove:</b> ${phoneNumbers.length}\n\n` : "") +
      `<b>Country code(s):</b> ${ccList} — removes all non-admins with these codes\n` +
      (exceptions.length > 0
        ? `<b>Exceptions (will NOT be removed):</b>\n${excList}${moreExc}\n\n`
        : `<i>No exceptions — all matching members will be removed</i>\n\n`) +
      `🛡️ Admins will NOT be removed.\n\nConfirm?`,
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("✅ Confirm & Remove", "rf_confirm")
          .text("❌ Cancel", "main_menu"),
      }
    );
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

  if (state.step === "demote_admin_enter_numbers") {
    if (!state.demoteAdminData) return;
    const phoneLines = text.split("\n").map((l) => l.trim()).filter((l) => l.length > 0);
    const phoneNumbers: string[] = [];
    for (const line of phoneLines) {
      const cleaned = line.replace(/[^0-9+]/g, "");
      if (cleaned.length >= 7) phoneNumbers.push(cleaned.replace(/^\+/, ""));
    }
    if (!phoneNumbers.length) {
      await ctx.reply("❌ No valid phone numbers found. Send numbers with country code, e.g.\n<code>919912345678\n919898765432</code>",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
      );
      return;
    }
    state.demoteAdminData.phoneNumbers = phoneNumbers;
    const selectedGroups = Array.from(state.demoteAdminData.selectedIndices).map((i) => state.demoteAdminData!.allGroups[i]);
    const groupList = selectedGroups.slice(0, 15).map((g) => `• ${esc(g.subject)}`).join("\n");
    const moreGroups = selectedGroups.length > 15 ? `\n... +${selectedGroups.length - 15} more` : "";
    const numList = phoneNumbers.slice(0, 15).map((p) => `• +${p}`).join("\n");
    const moreNums = phoneNumbers.length > 15 ? `\n... +${phoneNumbers.length - 15} more` : "";
    await ctx.reply(
      `📱 <b>Demote Selected Numbers — Confirm</b>\n\n` +
      `<b>${selectedGroups.length} group(s):</b>\n${groupList}${moreGroups}\n\n` +
      `<b>${phoneNumbers.length} number(s) to demote:</b>\n${numList}${moreNums}\n\n` +
      `⚠️ Only numbers currently admin in each group will be demoted.\n\nConfirm?`,
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("✅ Yes, Demote", "da_numbers_confirm")
          .text("❌ Cancel", "main_menu"),
      }
    );
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

    makeAdminCancelRequests.delete(userId);
    const statusMsg = await ctx.reply(
      `⏳ <b>Making ${phoneNumbers.length} number(s) admin in ${selectedGroups.length} group(s)...</b>\n\n⌛ Please wait...`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ma_cancel_request") }
    );

    void makeAdminBackground(userId, selectedGroups, phoneNumbers, chatId, statusMsg.message_id);
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
    const statusMsg = await ctx.reply(
      `⏳ <b>Fetching group info...</b>\n\n📊 0/${cleanLinks.length} processed`,
      { parse_mode: "HTML" }
    );
    const amChatId = ctx.chat.id;
    const amMsgId = statusMsg.message_id;
    const groups: Array<{ link: string; id: string; name: string }> = [];
    let failedLinks = 0;
    const fetchedLines: string[] = [];
    for (let li = 0; li < cleanLinks.length; li++) {
      const link = cleanLinks[li];
      const groupInfo = await getGroupIdFromLink(String(userId), link);
      if (groupInfo) {
        groups.push({ link, id: groupInfo.id, name: groupInfo.subject });
        fetchedLines.push(`✅ ${esc(groupInfo.subject)}`);
      } else {
        failedLinks++;
        fetchedLines.push(`❌ Link ${li + 1} — could not fetch`);
      }
      try {
        const preview = fetchedLines.slice(-10).join("\n");
        const extra = fetchedLines.length > 10 ? `\n... +${fetchedLines.length - 10} more` : "";
        await bot.api.editMessageText(amChatId, amMsgId,
          `⏳ <b>Fetching group info...</b>\n\n📊 ${li + 1}/${cleanLinks.length} processed | ✅ ${groups.length} found\n\n${preview}${extra}`,
          { parse_mode: "HTML" }
        );
      } catch {}
      if (li < cleanLinks.length - 1) await new Promise((r) => setTimeout(r, 600));
    }
    try { await ctx.api.deleteMessage(amChatId, amMsgId); } catch {}
    if (!groups.length) {
      await ctx.reply(
        "❌ <b>No group info found!</b>\n\nCheck:\n• Links are valid\n• WhatsApp is connected\n• Links are not expired",
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
    const failNote = failedLinks > 0 ? `\n⚠️ ${failedLinks} link(s) could not be fetched.` : "";
    await ctx.reply(
      `✅ <b>${groups.length} Group(s) found!</b>${failNote}\n\n${groupPreview}\n\n` +
      `👫 <b>Step 2: Friend Numbers</b>\n\n` +
      `Send friend contact numbers (one per line)\n` +
      `Example:\n<code>919912345678\n919898765432</code>\n\n` +
      (isMulti ? `⚠️ Multiple groups mode: Only friend numbers are supported (will be added to all groups).\n\n` : "") +
      `Tap Skip if you don't want to add friend numbers.`,
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
  ctx.answerCallbackQuery();
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

// Per-user serial queue for DP photo uploads — prevents concurrent getFile /
// downloadBuffer / sendMessage calls when user sends multiple photos at once.
const dpProcessingQueue: Map<number, Promise<void>> = new Map();
function enqueueDpProcessing(userId: number, task: () => Promise<void>): Promise<void> {
  const prev = dpProcessingQueue.get(userId) ?? Promise.resolve();
  const next = prev.then(() => task()).catch(() => {});
  dpProcessingQueue.set(userId, next);
  next.finally(() => {
    if (dpProcessingQueue.get(userId) === next) dpProcessingQueue.delete(userId);
  });
  return next;
}

// Retry wrapper for Telegram API calls that can transiently fail when
// multiple photos arrive at the same time (getFile, sendMessage, etc.).
async function retryTgApi<T>(fn: () => Promise<T>, maxRetries = 3): Promise<T> {
  let lastErr: any;
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err: any) {
      lastErr = err;
      // Don't retry on errors that won't resolve with a retry
      const msg: string = err?.message || "";
      if (msg.includes("403") || msg.includes("blocked") || msg.includes("chat not found")) throw err;
      await new Promise((r) => setTimeout(r, 600 * Math.pow(2, attempt)));
    }
  }
  throw lastErr;
}

bot.on("message:photo", async (ctx) => {
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state) return;

  if (state.step === "group_dp" && state.groupSettings) {
    // Snapshot the file_id and api reference immediately (ctx is ephemeral)
    const photos = ctx.message.photo;
    const fileId = photos[photos.length - 1].file_id;
    const tgApi = ctx.api;
    const chatId = ctx.message.chat.id;

    void enqueueDpProcessing(userId, async () => {
      // Re-read state inside queue — may have changed since we entered
      const s = userStates.get(userId);
      if (!s || s.step !== "group_dp" || !s.groupSettings) return;

      const maxDps = s.groupSettings.count;
      if (s.groupSettings.dpBuffers.length >= maxDps) {
        try {
          await retryTgApi(() => tgApi.sendMessage(chatId,
            `⚠️ <b>Max ${maxDps} DP${maxDps === 1 ? "" : "s"} reached.</b> Tum ${maxDps} group bana rahe ho, isliye max ${maxDps} DP. Done dabake aage badho.`,
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("✅ Done", "group_dp_done").text("❌ Cancel", "main_menu") }
          ));
        } catch {}
        return;
      }

      try {
        const file = await retryTgApi(() => retryGetFile(tgApi, fileId));
        if (!file.file_path) {
          await retryTgApi(() => tgApi.sendMessage(chatId, "❌ Could not download photo. Please resend it."));
          return;
        }
        const buf = await downloadBuffer(`https://api.telegram.org/file/bot${token}/${file.file_path}`);
        s.groupSettings.dpBuffers.push(buf);
        const count = s.groupSettings.dpBuffers.length;
        await retryTgApi(() => tgApi.sendMessage(chatId,
          `✅ <b>DP ${count} saved!</b>\n\nAur photos bhej sakte ho (max ${maxDps}), ya <b>✅ Done</b> dabake aage badho.\nTotal ab tak: <b>${count}/${maxDps}</b>`,
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("✅ Done", "group_dp_done").text("❌ Cancel", "main_menu") }
        ));
      } catch (err: any) {
        const msg = err?.message || "Unknown error";
        try { await retryTgApi(() => tgApi.sendMessage(chatId, `❌ Error saving photo: ${esc(msg)}`, { parse_mode: "HTML" })); } catch {}
      }
    });
    return;
  }

  if (state.step === "edit_settings_dp" && state.editSettingsData) {
    const photos = ctx.message.photo;
    const fileId = photos[photos.length - 1].file_id;
    const tgApi = ctx.api;
    const chatId = ctx.message.chat.id;

    void enqueueDpProcessing(userId, async () => {
      const s = userStates.get(userId);
      if (!s || s.step !== "edit_settings_dp" || !s.editSettingsData) return;
      try {
        const file = await retryTgApi(() => retryGetFile(tgApi, fileId));
        if (!file.file_path) {
          await retryTgApi(() => tgApi.sendMessage(chatId, "❌ Could not download photo. Please resend it."));
          return;
        }
        const buf = await downloadBuffer(`https://api.telegram.org/file/bot${token}/${file.file_path}`);
        s.editSettingsData.settings.dpBuffers = [buf];
        s.editSettingsData.settings.removeDp = false;
        s.step = "edit_settings_desc";
        await retryTgApi(() => tgApi.sendMessage(chatId,
          "✅ <b>DP saved!</b>\n\n📄 <b>Description</b>\n\nSare selected groups mein description lagani hai?\nDescription bhejo ya skip karo.",
          {
            parse_mode: "HTML",
            reply_markup: new InlineKeyboard()
              .text("⏭️ Skip", "es_desc_skip")
              .text("🗑️ Remove Description", "es_desc_remove").row()
              .text("❌ Cancel", "main_menu"),
          }
        ));
      } catch (err: any) {
        const msg = err?.message || "Unknown error";
        try { await retryTgApi(() => tgApi.sendMessage(chatId, `❌ Error saving photo: ${esc(msg)}`, { parse_mode: "HTML" })); } catch {}
      }
    });
    return;
  }
});

// ══════════════════════════════════════════════════════════════════════════════
// ─── /file — VCF File Tools (FREE for all users) ─────────────────────────────
// ══════════════════════════════════════════════════════════════════════════════

function fileToolsMenuText(): string {
  return (
    "📁 <b>VCF File Tools</b>\n\n" +
    "🆓 <i>Free for everyone</i> · /file\n\n" +
    "📝 <b>VCF Editor</b> — Convert any file to VCF with custom names\n" +
    "✂️ <b>Splitter</b> — Split one big file into smaller parts\n" +
    "🔗 <b>Merge</b> — Combine multiple files into one\n" +
    "📞 <b>Number → VCF</b> — Type numbers → get a .vcf file\n" +
    "🔄 <b>Convert Files</b> — Change format: VCF ↔ TXT ↔ CSV ↔ XLSX\n\n" +
    "<b>Supported formats:</b> <code>.vcf · .txt · .csv · .xlsx · .xlsm</code>"
  );
}

function fileToolsMenuKb(): InlineKeyboard {
  return new InlineKeyboard()
    .text("📝 VCF Editor", "ft_vcf_editor").text("✂️ Splitter", "ft_splitter").row()
    .text("🔗 Merge", "ft_merge").text("📞 Number → VCF", "ft_num2vcf").row()
    .text("🔄 Convert Files", "ft_converter").row()
    .text("🏠 Main Menu", "main_menu");
}

bot.command("file", async (ctx) => {
  const userId = ctx.from!.id;
  // Send menu instantly — no await on DB calls before replying.
  // trackUser + isBanned run in background; if banned, delete the menu silently.
  const sent = await ctx.reply(fileToolsMenuText(), { parse_mode: "HTML", reply_markup: fileToolsMenuKb() });
  void trackUser(userId);
  if (await isBanned(userId)) {
    try { await ctx.api.deleteMessage(ctx.chat.id, sent.message_id); } catch {}
  }
});

bot.callbackQuery("ft_menu", async (ctx) => {
  ctx.answerCallbackQuery();
  try {
    await ctx.editMessageText(fileToolsMenuText(), { parse_mode: "HTML", reply_markup: fileToolsMenuKb() });
  } catch {
    await ctx.reply(fileToolsMenuText(), { parse_mode: "HTML", reply_markup: fileToolsMenuKb() });
  }
});

// ── VCF Editor ────────────────────────────────────────────────────────────────

bot.callbackQuery("ft_vcf_editor", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  ftUploadStatusMsgId.delete(userId);
  userStates.set(userId, {
    step: "fe_upload",
    fileEditorData: { mode: "editor", contactsGroups: [], fileNames: [], fileExts: [] },
  });
  await ctx.editMessageText(
    "📝 <b>VCF Editor</b>\n\n" +
    "Send one or more files — <b>.vcf · .txt · .csv · .xlsx · .xlsm</b>\n" +
    "You can send multiple files one by one.\n\n" +
    "When done uploading, tap <b>✅ Done Uploading</b>",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("✅ Done Uploading", "fe_done_upload").row().text("❌ Cancel", "ft_menu") }
  );
});

bot.callbackQuery("fe_done_upload", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.fileEditorData || state.step !== "fe_upload") return;
  const d = state.fileEditorData;
  if (!d.contactsGroups.length) {
    ctx.answerCallbackQuery({ text: "⚠️ Please send at least one file first!", show_alert: true });
    return;
  }
  const total = d.contactsGroups.reduce((s, g) => s + g.length, 0);
  state.step = "fe_step1";
  ftUploadStatusMsgId.delete(userId);
  await ctx.editMessageText(
    `✅ <b>${d.fileNames.length} file(s) received · ${total} contacts found</b>\n\n` +
    "━━━━━━━━━━━━━━━━━━━━━━━\n" +
    "📝 <b>Step 1/5: Enter the file name (base name)</b>\n\n" +
    "<b>Example:</b> <code>SPIDY</code>\n\n" +
    "Files will be named as: <code>SPIDY 1.vcf</code>, <code>SPIDY 2.vcf</code>, etc.",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ft_menu") }
  );
});

// ── Splitter ──────────────────────────────────────────────────────────────────

bot.callbackQuery("ft_splitter", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  userStates.set(userId, {
    step: "fs_upload",
    fileEditorData: { mode: "splitter", contactsGroups: [], fileNames: [], fileExts: [] },
  });
  await ctx.editMessageText(
    "✂️ <b>Splitter</b>\n\n" +
    "Send <b>one file</b> (.vcf · .txt · .csv · .xlsx)\n\n" +
    "The bot will show total contacts and ask how many contacts per split file.",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ft_menu") }
  );
});

// ── Merge ─────────────────────────────────────────────────────────────────────

bot.callbackQuery("ft_merge", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  ftUploadStatusMsgId.delete(userId);
  userStates.set(userId, {
    step: "fm_upload",
    fileEditorData: { mode: "merge", contactsGroups: [], fileNames: [], fileExts: [] },
  });
  await ctx.editMessageText(
    "🔗 <b>Merge</b>\n\n" +
    "Send <b>multiple files</b> to merge (any supported format).\n" +
    "• Same format → output matches input format\n" +
    "• Mixed formats → you choose the output format\n\n" +
    "When done uploading, tap <b>✅ Done Uploading</b>",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("✅ Done Uploading", "fm_done_upload").row().text("❌ Cancel", "ft_menu") }
  );
});

bot.callbackQuery("fm_done_upload", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.fileEditorData || state.step !== "fm_upload") return;
  const d = state.fileEditorData;
  if (d.contactsGroups.length < 2) {
    ctx.answerCallbackQuery({ text: "⚠️ Send at least 2 files to merge!", show_alert: true });
    return;
  }
  ftUploadStatusMsgId.delete(userId);
  const autoExt = detectMergeExt(d.fileExts);
  if (autoExt) {
    await doMergeAndSend(ctx, userId, state, autoExt);
  } else {
    state.step = "fm_format";
    const fileList = d.fileNames.map((n, i) => `• <code>${esc(n)}</code> [${extLabel(d.fileExts[i])}]`).join("\n");
    await ctx.editMessageText(
      `📁 <b>${d.fileNames.length} files uploaded:</b>\n${fileList}\n\n` +
      "Files have different formats.\nChoose the <b>output format</b>:",
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("📱 VCF", "fm_fmt_vcf").text("📄 TXT", "fm_fmt_txt").text("📊 CSV", "fm_fmt_csv").row()
          .text("❌ Cancel", "ft_menu"),
      }
    );
  }
});

bot.callbackQuery(/^fm_fmt_(.+)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.fileEditorData) return;
  const fmt = "." + ctx.match[1];
  await doMergeAndSend(ctx, userId, state, fmt);
});

// ── Convert Files ─────────────────────────────────────────────────────────────

bot.callbackQuery("ft_converter", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  ftUploadStatusMsgId.delete(userId);
  userStates.set(userId, {
    step: "fc_upload",
    fileEditorData: { mode: "converter", contactsGroups: [], fileNames: [], fileExts: [] },
  });
  await ctx.editMessageText(
    "🔄 <b>Convert Files</b>\n\n" +
    "Send one or more files — <b>.vcf · .txt · .csv · .xlsx · .xlsm</b>\n" +
    "You can send multiple files one by one.\n\n" +
    "When done uploading, tap <b>✅ Done Uploading</b>",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("✅ Done Uploading", "fc_done_upload").row().text("❌ Cancel", "ft_menu") }
  );
});

bot.callbackQuery("fc_done_upload", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.fileEditorData || state.step !== "fc_upload") return;
  const d = state.fileEditorData;
  if (!d.contactsGroups.length) {
    ctx.answerCallbackQuery({ text: "⚠️ Please send at least one file first!", show_alert: true });
    return;
  }
  ftUploadStatusMsgId.delete(userId);

  // Determine which format button to hide: if all files share the same canonical
  // format there's no point converting them to that same format.
  const uniqueCanonical = new Set(d.fileExts.map(canonicalExt));
  const hideExt = uniqueCanonical.size === 1 ? [...uniqueCanonical][0] : null;

  state.step = "fc_format";
  const total = d.contactsGroups.reduce((s, g) => s + g.length, 0);
  const fileList = d.fileNames.map(
    (n, i) => `• <code>${esc(n)}</code> — <b>${d.contactsGroups[i].length}</b> contacts`
  ).join("\n");

  const kb = new InlineKeyboard();
  if (hideExt !== ".txt") kb.text("📄 TXT", "fc_fmt_txt");
  if (hideExt !== ".vcf") kb.text("📱 VCF", "fc_fmt_vcf");
  kb.row();
  if (hideExt !== ".csv") kb.text("📊 CSV", "fc_fmt_csv");
  if (hideExt !== ".xlsx") kb.text("📗 XLSX", "fc_fmt_xlsx");
  kb.row().text("❌ Cancel", "ft_menu");

  await ctx.editMessageText(
    `🔄 <b>Convert Files</b>\n\n` +
    `📁 <b>${d.fileNames.length} file(s) · ${total} contacts total</b>\n\n` +
    `${fileList}\n\n` +
    `Choose the <b>output format</b>:`,
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery(/^fc_fmt_(vcf|txt|csv|xlsx)$/, async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.fileEditorData) return;
  const outExt = "." + ctx.match[1];
  await doConvertAndSend(ctx, userId, state, outExt);
});

async function doConvertAndSend(ctx: any, userId: number, state: any, outExt: string): Promise<void> {
  const d = state.fileEditorData as NonNullable<UserState["fileEditorData"]>;
  const files = [...d.fileNames];
  const groups = d.contactsGroups.map((g) => [...g]);
  const fmtLabel = outExt.slice(1).toUpperCase();
  userStates.delete(userId);

  try {
    await ctx.editMessageText(
      `🔄 <b>Converting ${files.length} file(s) to ${fmtLabel}...</b>`,
      { parse_mode: "HTML" }
    );
  } catch {}

  const summaryLines: string[] = [];

  // Send strictly one-by-one to guarantee delivery order.
  for (let i = 0; i < files.length; i++) {
    const phones = groups[i];
    const baseName = files[i].replace(/\.[^.]+$/, "");
    const outName = `${baseName}${outExt}`;

    let buf: Buffer;
    if (outExt === ".vcf") {
      buf = Buffer.from(buildSplitContent(phones, ".vcf"), "utf8");
    } else if (outExt === ".txt") {
      buf = Buffer.from(buildTXTContent(phones), "utf8");
    } else if (outExt === ".csv") {
      buf = Buffer.from(buildCSVContent(phones), "utf8");
    } else {
      buf = await buildXLSXBuffer(phones);
    }

    summaryLines[i] = `📄 <code>${esc(outName)}</code> — <b>${phones.length}</b> contacts`;
    await retryTgApi(() => bot.api.sendDocument(userId, new InputFile(buf, outName)));
  }

  const totalContacts = groups.reduce((s, g) => s + g.length, 0);
  await bot.api.sendMessage(
    userId,
    `✅ <b>Conversion Complete!</b>\n\n` +
    `🔄 Output format: <b>${fmtLabel}</b>\n` +
    `📁 Files converted: <b>${files.length}</b>\n` +
    `📞 Total contacts: <b>${totalContacts}</b>\n\n` +
    `📋 <b>Summary:</b>\n${summaryLines.join("\n")}`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📁 File Tools", "ft_menu").text("🏠 Main Menu", "main_menu") }
  );
}

async function doMergeAndSend(ctx: any, userId: number, state: any, outExt: string): Promise<void> {
  const d = state.fileEditorData as NonNullable<UserState["fileEditorData"]>;
  const allPhones: string[] = ([] as string[]).concat(...(d!.contactsGroups as string[][]));
  const unique: string[] = [...new Set(allPhones)];
  userStates.delete(userId);

  try {
    await ctx.editMessageText(
      `🔗 <b>Merging ${d.fileNames.length} file(s)...</b>\n📊 Total unique contacts: <b>${unique.length}</b>`,
      { parse_mode: "HTML" }
    );
  } catch {}

  let content: string;
  let outName: string;
  if (outExt === ".vcf") {
    content = buildVCFContent(unique, "Contact", 1);
    outName = "merged.vcf";
  } else {
    content = unique.join("\n");
    outName = `merged${outExt}`;
  }

  await bot.api.sendDocument(
    userId,
    new InputFile(Buffer.from(content, "utf8"), outName),
    {
      caption:
        "🔗 <b>Merge Complete!</b>\n\n" +
        `📁 Source files: <b>${d.fileNames.length}</b>\n` +
        `📞 Total contacts: <b>${unique.length}</b>\n` +
        `🔁 Duplicates removed: <b>${allPhones.length - unique.length}</b>\n` +
        `📄 Output: <code>${esc(outName)}</code>`,
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📁 File Tools", "ft_menu").text("🏠 Main Menu", "main_menu"),
    }
  );
}

// ── Number → VCF ──────────────────────────────────────────────────────────────

bot.callbackQuery("ft_num2vcf", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  userStates.set(userId, {
    step: "fn_numbers",
    fileEditorData: { mode: "number2vcf", contactsGroups: [], fileNames: [], fileExts: [] },
  });
  await ctx.editMessageText(
    "📞 <b>Number → VCF</b>\n\n" +
    "Type or paste your phone numbers — <b>one per line</b>.\n" +
    "Country code optional.\n\n" +
    "<b>Example:</b>\n" +
    "<code>+919876543210\n918765432100\n7654321009</code>",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ft_menu") }
  );
});

// ── VCF Editor confirm ────────────────────────────────────────────────────────

bot.callbackQuery("fe_confirm", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.fileEditorData) return;
  const d = state.fileEditorData;
  if (!d.baseName || !d.contactName || d.startFileNum === undefined || d.contactStartNum === undefined || !d.contactsPerFile) return;

  // Snapshot scalars — then free state so contactsGroups can be GC'd
  const allPhones: string[] = ([] as string[]).concat(...(d.contactsGroups as string[][]));
  const { baseName, contactName, startFileNum, contactStartNum, contactsPerFile } = d;
  const totalContacts = allPhones.length;
  const totalFiles = Math.ceil(totalContacts / contactsPerFile);
  const endFileNum = startFileNum + totalFiles - 1;
  userStates.delete(userId);

  try {
    await ctx.editMessageText(
      `⚡ <b>Generating ${totalFiles} VCF file(s)...</b>\n📞 Total: <b>${totalContacts}</b> contacts`,
      { parse_mode: "HTML" }
    );
  } catch {}

  // For large jobs send a dedicated progress message that we edit every 50 files.
  // Fire-and-forget edits never block the send loop and stay within Telegram's rate limit.
  let progMsgId: number | null = null;
  if (totalFiles > 20) {
    try {
      const pm = await bot.api.sendMessage(userId,
        `⚡ <b>Generating...</b> 0 / ${totalFiles} files\n📊 Progress: <b>0%</b>`,
        { parse_mode: "HTML" }
      );
      progMsgId = pm.message_id;
    } catch {}
  }

  // Send strictly one-by-one to guarantee delivery order (file 1, 2, 3…).
  const UPDATE_EVERY = 50;
  for (let idx = 0; idx < totalFiles; idx++) {
    const start = idx * contactsPerFile;
    const phones = allPhones.slice(start, Math.min(start + contactsPerFile, totalContacts));
    const content = buildVCFContent(phones, contactName, contactStartNum + start);
    const fileName = `${baseName} ${startFileNum + idx}.vcf`;
    await retryTgApi(() => bot.api.sendDocument(userId, new InputFile(Buffer.from(content, "utf8"), fileName)));
    const sent = idx + 1;
    if (progMsgId && sent < totalFiles && sent % UPDATE_EVERY === 0) {
      const pct = Math.round((sent / totalFiles) * 100);
      bot.api.editMessageText(userId, progMsgId,
        `⚡ <b>Generating...</b> ${sent} / ${totalFiles} files\n📊 Progress: <b>${pct}%</b>`,
        { parse_mode: "HTML" }
      ).catch(() => {});
    }
  }
  if (progMsgId) bot.api.deleteMessage(userId, progMsgId).catch(() => {});

  // Summary — listing every file would overflow Telegram's 4096-char limit for
  // large jobs, so only show individual entries when there are ≤ 20 files.
  const summaryFiles = totalFiles <= 20
    ? Array.from({ length: totalFiles }, (_, i) => {
        const count = Math.min(contactsPerFile, totalContacts - i * contactsPerFile);
        return `📄 <code>${esc(`${baseName} ${startFileNum + i}.vcf`)}</code> (${count} contacts)`;
      }).join("\n") + "\n\n"
    : `📄 <code>${esc(`${baseName} ${startFileNum}.vcf`)}</code> … <code>${esc(`${baseName} ${endFileNum}.vcf`)}</code>\n\n`;

  await bot.api.sendMessage(
    userId,
    "✅ <b>Conversion Complete!</b>\n\n" +
    "📊 <b>Summary:</b>\n" +
    `• Total contacts processed: <b>${totalContacts}</b>\n` +
    `• VCF files created: <b>${totalFiles}</b>\n` +
    `• Contacts per file: <b>${contactsPerFile}</b>\n` +
    `• File range: <b>${startFileNum}</b> to <b>${endFileNum}</b>\n\n` +
    `📁 <b>Files created:</b>\n${summaryFiles}` +
    "🏁 All tasks complete! Use /file for more tools.",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📁 File Tools", "ft_menu").text("🏠 Main Menu", "main_menu") }
  );
});

// ── Number → VCF confirm ──────────────────────────────────────────────────────

bot.callbackQuery("fn_confirm", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.fileEditorData) return;
  const d = state.fileEditorData;
  if (!d.baseName || !d.contactName || !d.contactsGroups[0]?.length) return;

  const allPhones = d.contactsGroups[0];
  const fileName = `${d.baseName}.vcf`;
  userStates.delete(userId);

  try { await ctx.editMessageText("⚡ <b>Generating VCF...</b>", { parse_mode: "HTML" }); } catch {}

  const content = buildVCFContent(allPhones, d.contactName, 1);
  await bot.api.sendDocument(
    userId,
    new InputFile(Buffer.from(content, "utf8"), fileName),
    {
      caption:
        "✅ <b>VCF Created!</b>\n\n" +
        `📞 Total contacts: <b>${allPhones.length}</b>\n` +
        `📄 File: <code>${esc(fileName)}</code>`,
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📁 File Tools", "ft_menu").text("🏠 Main Menu", "main_menu"),
    }
  );
});

// ─── Document Handler (VCF) ──────────────────────────────────────────────────

// Parse phone numbers from a plain-text file where each line is a phone number.
function parseTXT(content: string): Array<{ name: string; phone: string }> {
  const contacts: Array<{ name: string; phone: string }> = [];
  for (const line of content.split(/\r?\n/)) {
    const cleaned = line.replace(/[^0-9+]/g, "").trim();
    if (cleaned.length >= 7) {
      const phone = cleaned.startsWith("+") ? cleaned : "+" + cleaned;
      contacts.push({ name: "Contact", phone });
    }
  }
  return contacts;
}

bot.on("message:document", async (ctx) => {
  const userId = ctx.from.id;
  let state = userStates.get(userId);
  if (!state) return;
  const doc = ctx.message.document;
  const fileName = doc.file_name || "file";
  const fileNameLower = fileName.toLowerCase();
  const isVcf = fileNameLower.endsWith(".vcf");
  const isTxt = fileNameLower.endsWith(".txt");

  // ── File Tools: handle upload states before format check ─────────────────
  const fileToolsUploadStep = state.step === "fe_upload" || state.step === "fs_upload" || state.step === "fm_upload" || state.step === "fc_upload";
  if (fileToolsUploadStep) {
    if (!isSupportedExt(fileNameLower)) {
      await ctx.reply("❌ Unsupported format.\nAllowed: <b>.vcf · .txt · .csv · .xlsx · .xls · .xlsm</b>", { parse_mode: "HTML" });
      return;
    }

    // Splitter: only 1 file allowed — check before queuing to give instant feedback
    if (state.step === "fs_upload") {
      const dCheck = state.fileEditorData;
      if (dCheck && dCheck.contactsGroups.length >= 1) {
        await ctx.reply("✂️ <b>Splitter</b> — only <b>1 file</b> allowed.\nTap ❌ Cancel and start over if you want a different file.", { parse_mode: "HTML" });
        return;
      }
    }

    // Capture everything we need from ctx before entering the async queue —
    // the Grammy context object is only valid for the current tick.
    const ftDocFileId = doc.file_id;
    const ftFileName = fileName;
    const ftFileNameLower = fileNameLower;
    const ftApi = ctx.api;
    const ftStep = state.step;

    // Enqueue — serialises concurrent uploads so simultaneous file sends
    // (e.g. a media album) don't race on the same fileEditorData object or
    // flood Telegram's CDN with parallel downloads that trigger ETIMEDOUT.
    void enqueueVcfProcessing(userId, async () => {
      // Re-read state inside the task — it may have been updated by a
      // prior queued upload finishing before us.
      const s = userStates.get(userId);
      if (!s) return;
      const d = s.fileEditorData;
      if (!d) return;

      // Double-check splitter limit inside the queue (race-safe)
      if (ftStep === "fs_upload" && d.contactsGroups.length >= 1) return;

      try {
        const fileInfo = await retryGetFile(ftApi, ftDocFileId);
        if (!fileInfo.file_path) { await ctx.reply("❌ Could not download file. Please try again."); return; }
        const url = `https://api.telegram.org/file/bot${token}/${fileInfo.file_path}`;
        const buf = await downloadBuffer(url);
        const phones = await extractPhonesFromBuffer(buf, ftFileNameLower);

        if (!phones.length) {
          await ctx.reply(`❌ No valid phone numbers found in <code>${esc(ftFileName)}</code>.`, { parse_mode: "HTML" });
          return;
        }

        // Store only the phones array — raw buffer is GC'd immediately
        d.contactsGroups.push(phones);
        d.fileNames.push(ftFileName);
        const ext = ftFileNameLower.includes(".") ? "." + ftFileNameLower.split(".").pop()! : ".vcf";
        d.fileExts.push(ext);

        if (s.step === "fs_upload") {
          // Splitter: immediately ask for split count
          s.step = "fs_count";
          await ctx.reply(
            `✂️ <b>File received!</b>\n` +
            `📄 <code>${esc(ftFileName)}</code>\n` +
            `📞 Total contacts: <b>${phones.length}</b>\n\n` +
            "Enter the <b>number of contacts per split file</b>:\n" +
            "<i>Example: type <code>200</code> to get files of 200 contacts each</i>",
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "ft_menu") }
          );
        } else {
          // Editor / Merge / Converter: accumulate, show running count.
          // Delete the previous status message first so only ONE "Done Uploading"
          // button is ever visible — no spam when multiple files are sent at once.
          const totalFiles = d.contactsGroups.length;
          const totalContacts = d.contactsGroups.reduce((acc, g) => acc + g.length, 0);
          const doneBtn = s.step === "fe_upload" ? "fe_done_upload"
            : s.step === "fc_upload" ? "fc_done_upload"
            : "fm_done_upload";
          const label = s.step === "fe_upload" ? "VCF Editor"
            : s.step === "fc_upload" ? "Convert Files"
            : "Merge";
          const prevMsgId = ftUploadStatusMsgId.get(userId);
          if (prevMsgId) {
            try { await ctx.api.deleteMessage(ctx.chat.id, prevMsgId); } catch {}
            ftUploadStatusMsgId.delete(userId);
          }
          const sentMsg = await ctx.reply(
            `✅ <b>${esc(ftFileName)}</b> received · ${phones.length} contacts\n\n` +
            `📁 <b>${label}</b> — ${totalFiles} file(s) · ${totalContacts} total contacts so far\n\n` +
            "Send more files, or tap <b>✅ Done Uploading</b>",
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("✅ Done Uploading", doneBtn).row().text("❌ Cancel", "ft_menu") }
          );
          ftUploadStatusMsgId.set(userId, sentMsg.message_id);
        }
      } catch (err: any) {
        try { await ctx.reply(`❌ Error processing file: ${unwrapError(err)}`); } catch {}
      }
    });
    return;
  }
  // ── End File Tools upload handler ─────────────────────────────────────────

  if (!isVcf && !isTxt) { await ctx.reply("❌ Please send a .vcf or .txt file only."); return; }

  if (state.step === "approval_admin_input" && state.approvalData) {
    try {
      const file = await retryGetFile(ctx.api, doc.file_id);
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
      const msg = err?.message || err?.description || String(err) || "Unknown error";
      await ctx.reply(`❌ Error downloading VCF: ${esc(msg)}`, { parse_mode: "HTML" });
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
      const file = await retryGetFile(ctx.api, doc.file_id);
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
      const msg = err?.message || err?.description || String(err) || "Unknown error";
      await ctx.reply(`❌ Error downloading VCF: ${esc(msg)}`, { parse_mode: "HTML" });
    }
    return;
  }

  if (["add_members_admin_vcf", "add_members_navy_vcf", "add_members_member_vcf"].includes(state.step) && state.addMembersData) {
    try {
      const file = await retryGetFile(ctx.api, doc.file_id);
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
      const msg = err?.message || err?.description || String(err) || "Unknown error";
      await ctx.reply(`❌ Error downloading VCF: ${esc(msg)}`, { parse_mode: "HTML" });
    }
    return;
  }

  if (!state || state.step !== "ctc_enter_vcf" || !state.ctcData) {
    const remote = await loadUserState(userId);
    if (remote && remote.step === "ctc_enter_vcf" && remote.ctcData) {
      const s2 = remote as UserState;
      userStates.set(userId, s2);
      state = s2;
      console.error(`[CTC-VCF] Loaded state from MongoDB for userId=${userId}`);
    } else {
      return;
    }
  }

  // Capture doc/ctx values before queuing — the ctx reference is safe to
  // close over but we extract the primitives for clarity.
  const docFileId = doc.file_id;
  const docFileName = doc.file_name || (isVcf ? "unknown.vcf" : "unknown.txt");
  const ctcApi = ctx.api;

  void enqueueVcfProcessing(userId, async () => {
    // Re-fetch state inside the queue task — it may have been updated by the
    // time a previous task in the queue finishes.
    const s = userStates.get(userId);
    if (!s || s.step !== "ctc_enter_vcf" || !s.ctcData) return;

    try {
      const file = await retryGetFile(ctcApi, docFileId);
      if (!file.file_path) { await ctx.reply("❌ Could not download file. Please resend it."); return; }
      const content = await downloadText(`https://api.telegram.org/file/bot${token}/${file.file_path}`);

      // Parse contacts from VCF or TXT
      let rawContacts: Array<{ name: string; phone: string }>;
      if (docFileName.toLowerCase().endsWith(".txt")) {
        rawContacts = parseTXT(content);
      } else {
        rawContacts = parseVCF(content);
      }

      if (!rawContacts.length) { await ctx.reply(`❌ No contacts found in <b>${esc(docFileName)}</b>.`, { parse_mode: "HTML" }); return; }

      const vcfFileName = docFileName;
      const contacts = rawContacts.map(c => ({ ...c, vcfFileName }));

      const idx = s.ctcData.currentPairIndex;
      // Helper to show the group label for the next prompt
      const getGroupLabel = (pairIdx: number) => {
        const p = s.ctcData!.pairs[pairIdx];
        return p.groupName ? `<b>${esc(p.groupName)}</b>` : `<code>${esc(p.link)}</code>`;
      };

      if (idx >= s.ctcData.pairs.length) {
        // All pairs filled, just append to last group
        const lastIdx = s.ctcData.pairs.length - 1;
        s.ctcData.pairs[lastIdx].vcfContacts.push(...contacts);
        const total = s.ctcData.pairs[lastIdx].vcfContacts.length;
        await ctx.reply(
          notr(`✅ <b>${contacts.length} contacts added to Group ${lastIdx + 1}</b> (total: ${total})\n\n🚀 Ready to check!`),
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("▶️ Start Check", "ctc_start_check").text("❌ Cancel", "main_menu") }
        );
        return;
      }

      // Add contacts to current pair
      s.ctcData.pairs[idx].vcfContacts.push(...contacts);
      const total = s.ctcData.pairs[idx].vcfContacts.length;
      s.ctcData.currentPairIndex++;
      const nextIdx = s.ctcData.currentPairIndex;

      if (nextIdx < s.ctcData.pairs.length) {
        await ctx.reply(
          notr(`✅ <b>${contacts.length} contacts added to Group ${idx + 1}</b> (total: ${total})\n\n📁 Send VCF/TXT for <b>Group ${nextIdx + 1}/${s.ctcData.pairs.length}</b>:\n${getGroupLabel(nextIdx)}\n\n<i>Or tap Start Check if you want to use the same file for remaining groups</i>`),
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("▶️ Start Check", "ctc_start_check").text("❌ Cancel", "main_menu") }
        );
      } else {
        await ctx.reply(
          notr(`✅ <b>${contacts.length} contacts for Group ${idx + 1}</b> (total: ${total})\n\n🎉 All ${s.ctcData.pairs.length} file(s) received!\n\n🚀 Ready to check!`),
          { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("▶️ Start Check", "ctc_start_check").text("❌ Cancel", "main_menu") }
        );
      }
    } catch (err: any) {
      const msg = err?.message || err?.description || String(err) || "Unknown error";
      try {
        await ctx.reply(
          `⚠️ Could not process <b>${esc(docFileName)}</b>.\n\n` +
          `<b>Error:</b> <code>${esc(msg)}</code>\n\n` +
          `Please resend the file. If the problem persists, try converting the VCF to UTF-8 encoding.`,
          { parse_mode: "HTML" }
        );
      } catch {}
    }
  });
});

// ─── Utilities ───────────────────────────────────────────────────────────────

// Single-attempt download with a 45-second socket timeout so a stalled
// Telegram CDN connection cannot block the processing queue indefinitely.
function downloadBufferOnce(url: string): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const client = url.startsWith("https") ? https : http;
    const req = client.get(url, (res) => {
      if (res.statusCode && res.statusCode >= 400) {
        res.resume();
        reject(new Error(`HTTP ${res.statusCode} downloading file`));
        return;
      }
      const chunks: Buffer[] = [];
      res.on("data", (c) => chunks.push(Buffer.isBuffer(c) ? c : Buffer.from(c)));
      res.on("end", () => resolve(Buffer.concat(chunks)));
      res.on("error", reject);
    });
    req.on("error", reject);
    // Hard socket timeout — kills the request if no data arrives for 45 s.
    // Without this, a stalled TCP connection can hang indefinitely, blocking
    // the per-user queue and causing "bot stuck" behaviour for subsequent
    // file uploads.
    req.setTimeout(45_000, () => {
      req.destroy();
      reject(new Error("Download timeout after 45s — please resend the file"));
    });
  });
}

// Retry-safe downloadBuffer: up to 3 attempts with exponential back-off.
// Fixes ETIMEDOUT / ENETUNREACH errors that occur when multiple VCF files
// are uploaded simultaneously and Telegram's CDN briefly becomes unreachable.
function downloadBuffer(url: string): Promise<Buffer>;
function downloadBuffer(url: string, asBuffer: true): Promise<Buffer>;
async function downloadBuffer(url: string, _asBuffer?: boolean): Promise<Buffer> {
  let lastErr: unknown;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      return await downloadBufferOnce(url);
    } catch (err) {
      lastErr = err;
      // Don't retry permanent HTTP 4xx errors (bad URL, forbidden, etc.)
      if (err instanceof Error && /HTTP 4\d\d/.test(err.message)) throw err;
      // Exponential back-off: 1 s, 2 s, 4 s
      await new Promise((r) => setTimeout(r, 1000 * Math.pow(2, attempt)));
    }
  }
  throw lastErr;
}

function downloadText(url: string): Promise<string> {
  return downloadBuffer(url).then((buf) => {
    // Try UTF-8 first. If it contains replacement characters, fall back to
    // latin1 (ISO-8859-1) which is lossless for any byte sequence. This fixes
    // the AggregateError / garbled text that occurs with VCF files saved by
    // phones that use Windows-1252 or Latin-1 encoding.
    const utf8 = buf.toString("utf8");
    if (utf8.includes("\uFFFD")) {
      return buf.toString("latin1");
    }
    return utf8;
  });
}

// Retry wrapper for ctx.api.getFile — network hiccups (especially when
// multiple VCFs are uploaded at once) can cause transient failures.
// Retries up to 3 times with exponential back-off before giving up.
async function retryGetFile(api: any, fileId: string, maxRetries = 3): Promise<any> {
  let lastErr: any;
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await api.getFile(fileId);
    } catch (err: any) {
      lastErr = err;
      const delay = 500 * Math.pow(2, attempt); // 500ms, 1s, 2s
      await new Promise((r) => setTimeout(r, delay));
    }
  }
  throw lastErr;
}

// Per-user VCF processing queue — prevents concurrent uploads from racing.
// When the user sends multiple VCF files at once (media group), Telegram
// delivers them as separate webhook hits nearly simultaneously. Without
// serialisation, two handlers can read the same `currentPairIndex` and
// advance it to the same next value, causing contacts to land in the wrong
// group and silently dropping one file.  The queue ensures they are
// processed one-at-a-time, in arrival order.
//
// Each task is also wrapped in a 90-second hard timeout so that a hung
// download or API call can never permanently block all subsequent uploads
// for the same user ("bot stuck" after large batch file uploads).
const vcfProcessingQueue: Map<number, Promise<void>> = new Map();

/** Run `task` but reject if it takes longer than `ms` milliseconds. */
function withTaskTimeout<T>(task: () => Promise<T>, ms: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    let done = false;
    const timer = setTimeout(() => {
      if (!done) { done = true; reject(new Error(`File processing timed out after ${ms / 1000}s`)); }
    }, ms);
    task().then(
      (v) => { if (!done) { done = true; clearTimeout(timer); resolve(v); } },
      (e) => { if (!done) { done = true; clearTimeout(timer); reject(e); } }
    );
  });
}

function enqueueVcfProcessing(userId: number, task: () => Promise<void>): Promise<void> {
  const prev = vcfProcessingQueue.get(userId) ?? Promise.resolve();
  // Wrap each task in a 90 s timeout so a stalled download never permanently
  // blocks subsequent uploads for the same user.
  const next = prev.then(() => withTaskTimeout(task, 90_000)).catch(() => {});
  vcfProcessingQueue.set(userId, next);
  // Clean up map entry after the chain settles so it can't grow unbounded.
  next.finally(() => {
    if (vcfProcessingQueue.get(userId) === next) vcfProcessingQueue.delete(userId);
  });
  return next;
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

/**
 * On bot startup, automatically reconnect all WhatsApp auto-sessions
 * (userId_auto) that were previously saved — so users don't need to
 * re-enter their number after a bot restart.
 */
/**
 * Restores auto-accepter jobs that were running before a bot restart.
 * Silently resumes each job — no message is sent to the user.
 * Expired jobs are cleaned from MongoDB without restarting.
 */
async function restoreAutoAccepterJobs(): Promise<void> {
  try {
    const jobs = await loadAllAutoAccepterJobs();
    if (!jobs.length) return;
    console.log(`[AUTO_ACCEPTER] Restoring ${jobs.length} auto-accepter job(s) after restart...`);

    for (const saved of jobs) {
      try {
        const remaining = saved.endsAt - Date.now();
        if (remaining <= 0) {
          // Session expired while bot was down — silently clean up MongoDB.
          // Do NOT send a notification: the session ended naturally and the
          // user has no expectation of a message after restart.
          console.log(`[AUTO_ACCEPTER] Job for userId=${saved.userId} already expired — removing silently.`);
          await deleteAutoAccepterJob(saved.userId);
          continue;
        }

        // Try to reconnect the WhatsApp session. Even if it fails here,
        // we still create the job — runAutoAccepterPoll already has its own
        // lazy-reconnect logic and will retry on every poll tick.
        const waRestored = await ensureSessionLoaded(String(saved.userId));
        if (!waRestored) {
          console.warn(`[AUTO_ACCEPTER] WA session not immediately available for userId=${saved.userId} — job will still run; poll will retry.`);
        }

        const job: AutoAccepterJob = {
          userId: saved.userId,
          groupIds: saved.groupIds,
          groupNames: saved.groupNames,
          durationMs: saved.durationMs,
          endsAt: saved.endsAt,
          chatId: saved.chatId,
          statusMsgId: saved.statusMsgId,
          totalAccepted: saved.totalAccepted,
          seenJids: new Set(),
          pollTimer: null as any,
          endTimer: null as any,
        };

        autoAccepterJobs.set(saved.userId, job);
        protectSessionFromEviction(String(saved.userId));

        job.pollTimer = setInterval(() => { void runAutoAccepterPoll(job); }, 10_000);
        job.endTimer = setTimeout(() => { void stopAutoAccepterJob(saved.userId, "done"); }, remaining);

        // Run a poll immediately so the status message is refreshed.
        void runAutoAccepterPoll(job);

        const remainMins = Math.ceil(remaining / 60000);
        console.log(`[AUTO_ACCEPTER] Restored job for userId=${saved.userId} (${saved.groupIds.length} groups, ${remainMins} min remaining, totalAccepted=${saved.totalAccepted})`);

        // Send "Resumed" notification ONLY after WhatsApp is actually connected.
        // We wait up to 90 seconds (polling every 5 s) — if WA doesn't connect
        // in time we skip the message rather than spamming the user with a
        // misleading "resumed" while WA is still offline.
        void (async () => {
          const maxWaitMs = 90_000;
          const intervalMs = 5_000;
          let waited = 0;
          while (waited < maxWaitMs) {
            if (isConnected(String(saved.userId))) break;
            await new Promise(r => setTimeout(r, intervalMs));
            waited += intervalMs;
          }
          if (!isConnected(String(saved.userId))) {
            console.warn(`[AUTO_ACCEPTER] WA never reconnected for userId=${saved.userId} after restart — skipping resume notification.`);
            return;
          }
          try {
            await bot.api.sendMessage(
              saved.chatId,
              `🔄 <b>Auto Request Accepter — Resumed</b>\n\n` +
              `The bot was restarted and your Auto Request Accepter has been automatically resumed.\n\n` +
              `✅ <b>Accepted so far:</b> ${saved.totalAccepted}\n` +
              `⏰ <b>Time remaining:</b> ~${remainMins} min\n` +
              `📋 <b>Groups:</b> ${saved.groupIds.length}\n\n` +
              `<i>No action needed — it is running in the background.</i>`,
              { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⛔ Cancel", "ar_stop_job").text("🏠 Menu", "main_menu") }
            );
          } catch {}
        })();

      } catch (err: any) {
        console.error(`[AUTO_ACCEPTER] Failed to restore job for userId=${saved.userId}:`, err?.message);
      }
    }
  } catch (err: any) {
    console.error("[AUTO_ACCEPTER] restoreAutoAccepterJobs error:", err?.message);
  }
}


async function restoreWaSwitchAliases(): Promise<void> {
  try {
    const profiles = await loadAllWaSwitchProfiles();
    if (!profiles.length) return;
    let restored = 0;
    for (const profile of profiles) {
      const uid = String(profile.telegramId);
      if (profile.activeId && profile.activeId !== uid) {
        setSessionAlias(uid, profile.activeId);
        restored++;
      }
    }
    if (restored > 0) {
      console.log(`[WA_SWITCH] Restored ${restored} WhatsApp session alias(es) from MongoDB.`);
    }
  } catch (err: any) {
    console.error("[WA_SWITCH] restoreWaSwitchAliases error:", err?.message);
  }
}

async function restoreAutoWaSessionsOnStartup(): Promise<void> {
  try {
    const allSessions = await listStoredWhatsAppSessions();
    if (!allSessions.length) return;
    // Sirf _auto sessions (AutoChat ke liye) restore karo startup pe.
    // Primary sessions lazy hain — user /start karne pe connect honge.
    // Pehle ALL sessions restore hoti thi, jisse startup pe memory spike
    // aata tha aur 30 min idle ke baad sab ek-ek disconnect ho jaati thi.
    const autoSessions = allSessions.filter(s => s.userId.includes("_auto"));
    if (!autoSessions.length) return;
    console.log(`[AUTO_WA] Reconnecting ${autoSessions.length} auto-WA session(s) on startup (primary sessions are lazy)...`);
    for (const s of autoSessions) {
      try {
        await ensureSessionLoaded(s.userId);
        console.log(`[AUTO_WA] Loaded auto session: ${s.userId} (${s.phoneNumber})`);
      } catch (err: any) {
        console.error(`[AUTO_WA] Failed to load session ${s.userId}:`, err?.message);
      }
    }
  } catch (err: any) {
    console.error("[AUTO_WA] restoreAutoWaSessionsOnStartup error:", err?.message);
  }
}

async function restorePersistedAutoChatSessions(): Promise<void> {
  try {
    const sessions = await loadAllAutoChatSessions();
    if (!sessions.length) return;
    console.log(`[AUTO_CHAT] Restoring ${sessions.length} autochat session(s) from MongoDB after restart...`);

    for (const s of sessions) {
      try {
        const telegramIdStr = String(s.userId);
        const primaryUserId = telegramIdStr;
        const autoUserId = s.autoUserId;
        const sessionType = s.sessionType ?? "old";

        // ── Step 1: Check if expiry already passed ──────────────────────────
        if (s.autoChatExpiresAt && Date.now() >= s.autoChatExpiresAt) {
          console.log(`[AUTO_CHAT] Session for userId=${s.userId} expired — skipping restore`);
          await deleteAutoChatSession(s.userId).catch(() => {});
          try {
            await bot.api.sendMessage(s.userId,
              "⏰ <b>Auto Chat Expired</b>\n\n" +
              "Your Auto Chat session had expired by the time the bot restarted.\n" +
              "Start a new Auto Chat session from the menu.",
              { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🤖 Auto Chat", "auto_chat_menu") }
            );
          } catch {}
          continue;
        }

        // ── Step 2: Reconnect WhatsApp sessions ─────────────────────────────
        // Try to load the primary session (the user's main WA account).
        try { await ensureSessionLoaded(primaryUserId); } catch {}
        // Try to load the auto session (the secondary WA account).
        try { await ensureSessionLoaded(autoUserId); } catch {}
        // Also try to restore any extra auto slots (slot 2, 3, ...) if present
        for (let slot = 2; slot <= 10; slot++) {
          const slotUid = getAutoSlotUserId(telegramIdStr, slot);
          try { await ensureSessionLoaded(slotUid); } catch {}
        }

        // Wait up to 30s for the primary WA to connect.
        let primaryOk = isConnected(primaryUserId);
        if (!primaryOk) {
          primaryOk = await waitForWhatsAppConnected(primaryUserId, { timeoutMs: 30_000, pollMs: 1_000 }).catch(() => false);
        }

        // Wait up to 30s for the auto WA to connect.
        let autoOk = isAutoConnected(telegramIdStr);
        if (!autoOk) {
          autoOk = await waitForWhatsAppConnected(autoUserId, { timeoutMs: 30_000, pollMs: 1_000 }).catch(() => false);
        }

        // ── Multi-WA fallback: if primary is down but 2+ auto slots are up,
        // autochat can still run using the connected auto slots as the WA pair.
        let effectivePrimaryUserId = primaryUserId;
        let effectiveAutoUserId = autoUserId;
        const connectedAutoSlots = getAllConnectedAutoSlots(telegramIdStr);
        const totalConnectedWa = (primaryOk ? 1 : 0) + connectedAutoSlots.length;

        if (!primaryOk && connectedAutoSlots.length >= 2) {
          // Primary is down but 2+ auto slots are available — use first two.
          effectivePrimaryUserId = connectedAutoSlots[0].userId;
          effectiveAutoUserId = connectedAutoSlots[1].userId;
          primaryOk = true;
          autoOk = true;
          console.log(`[AUTO_CHAT] Primary WA down for userId=${s.userId} — using auto slots ${connectedAutoSlots[0].slot} & ${connectedAutoSlots[1].slot} as fallback pair`);
        } else if (!primaryOk && connectedAutoSlots.length === 1) {
          // Primary is down, only one auto slot — check if we can use saved autoUserId
          // but only if that slot is connected
          const savedAutoConnected = isConnected(autoUserId);
          if (!savedAutoConnected) {
            console.log(`[AUTO_CHAT] Skipping restore for userId=${s.userId}: only 1 auto slot connected and primary is down`);
            await deleteAutoChatSession(s.userId).catch(() => {});
            continue;
          }
        }

        // Need at least 2 WA accounts connected to run any autochat session type.
        if (totalConnectedWa < 2 && !(effectivePrimaryUserId !== primaryUserId)) {
          console.log(`[AUTO_CHAT] Skipping ${sessionType} restore for userId=${s.userId}: not enough WA connected (primary=${primaryOk}, auto=${autoOk}, autoSlots=${connectedAutoSlots.length})`);
          await deleteAutoChatSession(s.userId).catch(() => {});
          continue;
        }
        if (!primaryOk || !autoOk) {
          console.log(`[AUTO_CHAT] Skipping ${sessionType} restore for userId=${s.userId}: WA not connected (primary=${primaryOk}, auto=${autoOk})`);
          await deleteAutoChatSession(s.userId).catch(() => {});
          continue;
        }

        // ── Step 3: Resume the correct session type ──────────────────────────
        if (sessionType === "cig") {
          // ── Chat In Group restore ──────────────────────────────────────────
          const groups = (s.groups && s.groups.length > 0)
            ? s.groups
            : (s.groupIds ?? []).map((id) => ({ id, subject: "" }));
          if (!groups.length) {
            await deleteAutoChatSession(s.userId).catch(() => {});
            continue;
          }
          const expiryLabel = s.autoChatExpiresAt
            ? `\n⏳ Time Remaining: <b>${formatRemaining(s.autoChatExpiresAt)}</b>`
            : "";
          const restoredSent = s.sentCount ?? 0;
          const restoredAcc1 = s.sentByAccount1 ?? 0;
          const restoredAcc2 = s.sentByAccount2 ?? 0;
          const restoredFailed = s.failedCount ?? 0;
          const statusMsg = await bot.api.sendMessage(s.userId,
            "🤖 <b>Chat In Group Chal Raha Hai...</b>\n\n" +
            `📋 Groups: <b>${groups.length}</b>\n` +
            `📱 Account 1: <b>${restoredAcc1} messages</b>\n` +
            `📱 Account 2: <b>${restoredAcc2} messages</b>\n` +
            `📤 Total Sent: <b>${restoredSent}</b>\n` +
            `❌ Failed: <b>${restoredFailed}</b>` +
            expiryLabel + "\n\nPress Stop to stop the chat.",
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔄 Refresh", "cig_refresh").text("⏹️ Stop", "cig_stop_btn").row().text("🏠 Main Menu", "main_menu") }
          ).catch(() => null);
          if (!statusMsg) { await deleteAutoChatSession(s.userId).catch(() => {}); continue; }
          void runGroupChatDualBackground(s.userId, effectivePrimaryUserId, effectiveAutoUserId, s.userId, statusMsg.message_id, groups, s.autoChatExpiresAt, s.currentGroupIndex ?? 0, s.messageIndex ?? 0, restoredSent, restoredAcc1, restoredAcc2, restoredFailed);
          console.log(`[AUTO_CHAT] Restored CIG session for userId=${s.userId} (${groups.length} groups, sent=${restoredSent}, using primary=${effectivePrimaryUserId})`);

        } else if (sessionType === "acf") {
          // ── Chat Friend restore ────────────────────────────────────────────
          if (!s.primaryJid || !s.autoJid) {
            await deleteAutoChatSession(s.userId).catch(() => {});
            continue;
          }

          // ── Build connected-only WA list from saved allJids/allUserIds ────
          // Only include WAs that are actually connected right now.
          // This prevents a disconnected WA from being included in autochat.
          const savedJids = s.allJids && s.allJids.length >= 2 ? s.allJids : [s.primaryJid, s.autoJid];
          const savedUids = s.allUserIds && s.allUserIds.length >= 2
            ? s.allUserIds
            : [effectivePrimaryUserId, effectiveAutoUserId];

          const connectedJids: string[] = [];
          const connectedUids: string[] = [];
          for (let i = 0; i < savedUids.length; i++) {
            if (isConnected(savedUids[i])) {
              connectedJids.push(savedJids[i] ?? "");
              connectedUids.push(savedUids[i]);
            }
          }

          // Need at least 2 WA accounts connected to run ACF
          if (connectedJids.length < 2 || connectedUids.length < 2) {
            console.log(`[AUTO_CHAT] Skipping ACF restore for userId=${s.userId}: only ${connectedJids.length} WA connected (need 2+)`);
            await deleteAutoChatSession(s.userId).catch(() => {});
            try {
              await bot.api.sendMessage(s.userId,
                "⚠️ <b>Chat Friend Resume Nahi Hua</b>\n\n" +
                "Bot restart ke baad sirf 1 ya 0 WhatsApp connect hua.\n" +
                "Chat Friend ke liye kam se kam 2 WA connected hone chahiye.\n\n" +
                "Apne WhatsApp reconnect karo aur dobara Chat Friend shuru karo.",
                { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect WA", "connect_wa").text("🏠 Menu", "main_menu") }
              );
            } catch {}
            continue;
          }

          const expiryLabel = s.autoChatExpiresAt
            ? `\n⏳ Time Remaining: <b>${formatRemaining(s.autoChatExpiresAt)}</b>`
            : "";
          const restoredSent = s.sentCount ?? 0;
          const restoredFailed = s.failedCount ?? 0;
          let waListText = "";
          for (let i = 0; i < connectedJids.length; i++) {
            const num = connectedJids[i].replace("@s.whatsapp.net", "");
            waListText += `${i === 0 ? "📞" : "📱"} WA ${i + 1}: <code>+${num}</code>\n`;
          }
          const statusMsg = await bot.api.sendMessage(s.userId,
            "👫 <b>Chat Friend Resume Hua!</b>\n\n" +
            waListText +
            `📤 Sent: <b>${restoredSent}</b>\n` +
            `❌ Failed: <b>${restoredFailed}</b>` +
            expiryLabel + "\n\nPress Stop to end it.",
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔄 Refresh", "acf_refresh").text("⏹️ Stop", "acf_stop_btn").row().text("🏠 Main Menu", "main_menu") }
          ).catch(() => null);
          if (!statusMsg) { await deleteAutoChatSession(s.userId).catch(() => {}); continue; }
          void runChatFriendBackground(
            s.userId, connectedUids[0], connectedUids[1],
            s.userId, statusMsg.message_id,
            connectedJids[0], connectedJids[1],
            CHAT_FRIEND_PAIRS.length, s.autoChatExpiresAt,
            restoredSent, restoredFailed,
            connectedJids, connectedUids
          );
          console.log(`[AUTO_CHAT] Restored ACF session for userId=${s.userId} (${connectedJids.length} WA connected, sent=${restoredSent})`);

        } else {
          // ── Legacy "old" Auto Chat restore ────────────────────────────────
          const groupIds = s.groupIds ?? [];
          if (!groupIds.length) { await deleteAutoChatSession(s.userId).catch(() => {}); continue; }
          const statusMsg = await bot.api.sendMessage(s.userId,
            "🤖 <b>Auto Chat Chal Raha Hai...</b>\n\n⏳ Please wait...",
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔄 Refresh", "auto_chat_refresh").text("⏹️ Stop", "auto_chat_stop").row().text("🏠 Main Menu", "main_menu") }
          ).catch(() => null);
          if (!statusMsg) { await deleteAutoChatSession(s.userId).catch(() => {}); continue; }
          const groups = groupIds.map((id) => ({ id, subject: "" }));
          void runAutoChatBackground(s.userId, autoUserId, s.userId, statusMsg.message_id, groups, s.message ?? "", s.delaySeconds ?? 60, s.repeatCount ?? 0, s.sentCount ?? 0);
          console.log(`[AUTO_CHAT] Restored legacy session for userId=${s.userId} (${groupIds.length} groups)`);
        }

      } catch (err: any) {
        console.error(`[AUTO_CHAT] Failed to restore session for userId=${s.userId}:`, err?.message);
        await deleteAutoChatSession(s.userId).catch(() => {});
      }
    }
  } catch (err: any) {
    console.error("[AUTO_CHAT] restorePersistedAutoChatSessions error:", err?.message);
  }
}

export async function startBot() {
  if (!token) {
    console.log("[BOT] TELEGRAM_BOT_TOKEN not set — bot disabled. Set it to enable the Telegram bot.");
    return;
  }

  // Register a global disconnect notifier so users get a Telegram alert in English
  // (with their WhatsApp number) whenever any of their WhatsApp sessions disconnects —
  // including sessions that were silently restored on bot startup.
  setDisconnectNotifier(async (sessionUserId, reason, phoneNumber) => {
    // Auto-Chat sessions use IDs like `${telegramId}_auto`; map to the actual Telegram user.
    const isAuto = sessionUserId.endsWith("_auto");
    const telegramIdStr = isAuto ? sessionUserId.replace(/_auto$/, "") : sessionUserId;
    const telegramId = Number(telegramIdStr);
    if (!Number.isFinite(telegramId)) return;
    const accountLabel = isAuto ? "Auto Chat WhatsApp" : "WhatsApp";
    const reasonLower = (reason || "").toLowerCase();

    const isQrExpiry =
      reasonLower.includes("qr session") ||
      reasonLower.includes("qr code") ||
      reasonLower.includes("reconnect via qr");
    const is403 =
      reasonLower.includes("403") ||
      reasonLower.includes("whatsapp rejected");
    const isLogout =
      reasonLower.includes("logged out") ||
      reasonLower.includes("logout");

    // If the phone number is unknown (session never connected, e.g. after bot
    // restart with stale/invalid creds that WhatsApp immediately rejects) AND
    // the reason is logout, skip the notification entirely — the session data
    // is already deleted from MongoDB, so the user will just see the Connect
    // button on next /start. Sending an "(unknown) disconnected" alert adds
    // noise without giving the user any actionable information.
    if (!phoneNumber && isLogout) return;

    const phoneText = phoneNumber ?? "(unknown)";

    let reconnectHint: string;
    if (isQrExpiry) {
      // QR session expired — user must scan a new QR
      reconnectHint =
        `Your QR session is no longer valid.\n\n` +
        `Tap <b>Reconnect WhatsApp</b> → <b>📷 Pair QR</b> to scan a new QR code and reconnect.`;
    } else if (isLogout) {
      // Explicit logout — must pair fresh
      reconnectHint =
        `Your WhatsApp account was logged out.\n\n` +
        `Tap <b>Reconnect WhatsApp</b> to pair your number again.`;
    } else if (is403) {
      // 403 — transient rate-limit / server rejection. Credentials still valid.
      reconnectHint =
        `This is usually a temporary WhatsApp server issue.\n\n` +
        `✅ <b>Your credentials are still saved.</b>\n` +
        `Tap <b>Reconnect WhatsApp</b> to reconnect — <b>no re-pairing needed.</b>`;
    } else {
      // Generic disconnect — credentials still intact
      reconnectHint =
        `✅ <b>Your session is still saved.</b>\n` +
        `Tap <b>Reconnect WhatsApp</b> to reconnect — <b>no re-pairing needed.</b>`;
    }

    const message =
      `⚠️ <b>${accountLabel} Disconnected</b>\n\n` +
      `Your ${accountLabel} number <code>${esc(phoneText)}</code> has been disconnected.\n\n` +
      `Reason: ${esc(reason || "Unknown")}\n\n` +
      reconnectHint;

    // Build keyboard — for primary WA disconnects, include any saved switch slots
    // so the user can instantly switch to another WhatsApp without re-pairing.
    const notifyKb = new InlineKeyboard();
    if (!isAuto) {
      try {
        const switchProfile = await loadWaSwitchProfile(telegramId);
        const otherSlots = (switchProfile?.slots ?? []).filter(
          (s) => s.id !== sessionUserId && s.id !== String(telegramId) + "_sw_" + "0"
        );
        if (otherSlots.length > 0) {
          notifyKb.text("📱 Reconnect WhatsApp", "connect_wa").row();
          for (const sw of otherSlots) {
            notifyKb.text(`🔀 Switch to ${sw.phone}`, `switch_wa:${sw.id}`).row();
          }
          notifyKb.text("🏠 Menu", "main_menu");
        } else {
          notifyKb
            .text("📱 Reconnect WhatsApp", "connect_wa")
            .text("🏠 Menu", "main_menu");
        }
      } catch {
        notifyKb
          .text("📱 Reconnect WhatsApp", "connect_wa")
          .text("🏠 Menu", "main_menu");
      }
    } else {
      notifyKb
        .text("🤖 Reconnect Auto WA", "connect_auto_wa")
        .text("🏠 Menu", "main_menu");
    }

    void bot.api.sendMessage(telegramId, message, {
      parse_mode: "HTML",
      reply_markup: notifyKb,
    }).catch((err) => {
      console.error(`[BOT][NOTIFY-DISCONNECT] Failed to notify ${telegramId}:`, err?.message);
    });
  });

  // Restore WA switch aliases first (5s) so aliased sessions load correctly.
  setTimeout(() => { void restoreWaSwitchAliases(); }, 5_000);

  // Reconnect all previously saved auto-WA sessions (userId_auto) in background
  // so users don't need to re-enter their number after a bot restart.
  // 10s delay — small wait so MongoDB connection is stable.
  setTimeout(() => { void restoreAutoWaSessionsOnStartup(); }, 10_000);

  // Restore auto-accepter jobs 45s after startup — ensures WA sessions have
  // had enough time to reconnect from MongoDB before we start polling.
  setTimeout(() => { void restoreAutoAccepterJobs(); }, 45_000);

  void syncAutoChatSettings().then(() => {
    console.log(`[BOT] Auto Chat settings loaded: global=${autoChatGlobalEnabled} accessList=${autoChatAccessSet.size} users`);
    // After settings are loaded, restore any persisted autochat sessions from MongoDB.
    // 30s delay — gives WhatsApp sessions enough time to reconnect from
    // Mongo auth state before we try to resume CIG / ACF / old sessions.
    setTimeout(() => { void restorePersistedAutoChatSessions(); }, 30_000);
  });

  bot.catch((err) => {
    const e = err.error as any;
    const code = e?.error_code;
    const desc: string = e?.description || e?.message || String(e) || "";
    if (code === 400 && desc.includes("message is not modified")) return;
    console.error(`[BOT] Error in update ${err.ctx?.update?.update_id}: ${desc || err.message}`);
  });

  // Hydrate per-user language preferences from MongoDB before bot starts,
  // so the very first outgoing message uses the right language.
  try {
    await loadUserLanguages();
  } catch (err: any) {
    console.error("[i18n] loadUserLanguages failed:", err?.message);
  }

  // ─── WEBHOOK MODE (Render production) ──────────────────────────────────────
  // Jab RENDER_EXTERNAL_URL set ho: bot seedha bot.handleUpdate() se updates
  // process karta hai — Grammy ke adapter ya timeout ke bina. Ye approach:
  //   • Koi 10-second Grammy timeout nahi (WhatsApp ops ke liye safe)
  //   • bot.start() ko touch nahi karta (koi Grammy override nahi)
  //   • Telegram update aane pe instant response — polling delay zero
  //   • Routes pe POST /api/telegram-webhook register hota hai separately
  const renderUrl = process.env["RENDER_EXTERNAL_URL"];
  if (renderUrl) {
    try {
      await bot.init();
      const webhookUrl = `${renderUrl}/api/telegram-webhook`;
      await bot.api.setWebhook(webhookUrl, { drop_pending_updates: false });
      console.log(`[BOT] Webhook registered → ${webhookUrl}`);
      console.log("[BOT] Running in webhook mode — instant response on every update!");
    } catch (err: any) {
      console.error("[BOT] Failed to set webhook:", err?.message);
    }
    return;
  }

  // ─── POLLING FALLBACK (local development only) ─────────────────────────────
  // Sirf jab RENDER_EXTERNAL_URL nahi hota (local dev). Render pe ye path
  // kabhi execute nahi hota.
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
        allowed_updates: ["message", "callback_query", "inline_query", "chosen_inline_result"],
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

  launchBot();
}

// ══════════════════════════════════════════════════════════════════
// BY-LINK HANDLERS for all 7 features + CTC Done
// ══════════════════════════════════════════════════════════════════

// ─── Helper: Show link-collection prompt ─────────────────────────
function byLinkPrompt(feature: string, emoji: string, count: number, doneCallback: string): string {
  return `${emoji} <b>${feature} — By Link</b>\n\n` +
    `📎 <b>${count} link(s) collected</b>\n\n` +
    `Send WhatsApp group invite links (one per message or multiple at once):\n` +
    `<code>https://chat.whatsapp.com/ABC123</code>\n\n` +
    `<i>Tap <b>Done</b> when all links are sent.</i>`;
}

// ─── CTC Links Done ───────────────────────────────────────────────
bot.callbackQuery("ctc_links_done", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state || state.step !== "ctc_enter_links") return;
  const buffer = state.ctcLinkBuffer || [];
  if (!buffer.length) {
    ctx.answerCallbackQuery({ text: "❌ Please send at least one link first!", show_alert: true });
    return;
  }
  ctcLinkCollectMsgId.delete(userId);
  if (!state.ctcData) return;
  state.ctcData.groupLinks = buffer;
  state.ctcData.pairs = buffer.map((link) => ({ link, vcfContacts: [] }));
  state.ctcData.currentPairIndex = 0;
  state.step = "ctc_enter_vcf";
  state.ctcLinkBuffer = undefined;
  saveUserState(userId, state).catch(() => {});
  await ctx.editMessageText(
    `✅ <b>${buffer.length} group link(s) saved!</b>\n\n` +
    `📁 <b>Step 2: Send VCF file(s)</b>\n\n` +
    `You can send:\n• One VCF for all groups\n• Multiple VCFs one per group (in order)\n\n` +
    `Send VCF for <b>Group 1/${buffer.length}</b>:\n<code>${esc(buffer[0])}</code>\n\n` +
    `When ready, tap <b>Start Check</b>:`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("▶️ Start Check", "ctc_start_check").text("❌ Cancel", "main_menu") }
  );
});

// ─── Leave Group — By Link ────────────────────────────────────────
// ── By-link shared helper ────────────────────────────────────────────────────
// Resolves each WhatsApp invite link with a per-link timeout (20 s) and edits
// the bot message with live progress so the user knows the bot is working.
// Without the timeout, a rate-limited link can block for 30+ minutes silently.
async function resolveLinksWithProgress(
  userId: number,
  buffer: string[],
  chatId: number,
  msgId: number
): Promise<Array<{ id: string; subject: string }>> {
  const total = buffer.length;
  const groups: Array<{ id: string; subject: string }> = [];
  for (let i = 0; i < total; i++) {
    try {
      await bot.api.editMessageText(
        chatId, msgId,
        `🔗 <b>Resolving group links...</b>\n\n⏳ <b>${i + 1}/${total}</b> done...`,
        { parse_mode: "HTML" }
      );
    } catch {}
    try {
      const result = await Promise.race([
        getGroupIdFromLink(String(userId), buffer[i]),
        new Promise<null>(r => setTimeout(() => r(null), 20_000)),
      ]);
      if (result) groups.push({ id: result.id, subject: result.subject });
    } catch {}
  }
  return groups;
}
// ─────────────────────────────────────────────────────────────────────────────

bot.callbackQuery("lv_by_link", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }); return;
  }
  userStates.set(userId, { step: "lv_enter_links_bl", lvLinkBuffer: [] });
  lvLinkCollectMsgId.delete(userId);
  await ctx.editMessageText(
    byLinkPrompt("Leave Group", "🚪", 0, "lv_links_done"),
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("lv_links_done", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state || state.step !== "lv_enter_links_bl") return;
  const buffer = state.lvLinkBuffer || [];
  if (!buffer.length) { ctx.answerCallbackQuery({ text: "❌ Send at least one link first!", show_alert: true }); return; }
  lvLinkCollectMsgId.delete(userId);
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;
  await ctx.editMessageText("🔗 <b>Resolving group links...</b>\n\n⏳ Starting...", { parse_mode: "HTML" });
  const groups: Array<{ id: string; subject: string; isAdmin: boolean }> =
    (await resolveLinksWithProgress(userId, buffer, chatId, msgId)).map(g => ({ ...g, isAdmin: true }));
  if (!groups.length) {
    await ctx.editMessageText("❌ Could not resolve any links. Check links and WhatsApp connection.", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔗 Try Again", "lv_by_link").text("🏠 Menu", "main_menu") }); return;
  }
  userStates.set(userId, {
    step: "lv_menu",
    leaveData: {
      groups,
      mode: "all",
      patterns: detectSimilarGroups(groups),
      selectedIndices: new Set(groups.map((_, i) => i)),
      page: 0,
      selectedGroups: groups,
    },
  });
  const preview = groups.slice(0, 20).map(g => `• ${esc(g.subject)}`).join("\n");
  const more = groups.length > 20 ? `\n... +${groups.length - 20} more` : "";
  await ctx.editMessageText(
    `🚪 <b>Leave Groups — Confirm</b>\n\n` +
    `📊 <b>${groups.length} group(s) will be left:</b>\n\n${preview}${more}\n\n` +
    `⚠️ Are you sure you want to leave these groups?`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("✅ Yes, Leave", "lv_confirm").text("❌ Cancel", "main_menu") }
  );
});

// ─── Remove Members — By Link ─────────────────────────────────────
bot.callbackQuery("rm_by_link", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }); return;
  }
  userStates.set(userId, { step: "rm_enter_links_bl", rmLinkBuffer: [] });
  rmLinkCollectMsgId.delete(userId);
  await ctx.editMessageText(
    byLinkPrompt("Remove Members", "🗑️", 0, "rm_links_done"),
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("rm_links_done", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state || state.step !== "rm_enter_links_bl") return;
  const buffer = state.rmLinkBuffer || [];
  if (!buffer.length) { ctx.answerCallbackQuery({ text: "❌ Send at least one link first!", show_alert: true }); return; }
  rmLinkCollectMsgId.delete(userId);
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;
  await ctx.editMessageText("🔗 <b>Resolving group links...</b>\n\n⏳ Starting...", { parse_mode: "HTML" });
  const groups = await resolveLinksWithProgress(userId, buffer, chatId, msgId);
  if (!groups.length) {
    await ctx.editMessageText("❌ Could not resolve any links. Check links and WhatsApp connection.", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔗 Try Again", "rm_by_link").text("🏠 Menu", "main_menu") }); return;
  }
  userStates.set(userId, {
    step: "remove_mode_select",
    removeExcludeData: { selectedGroups: groups, excludeNumbers: new Set(), excludePrefixes: new Set() },
    removeFriendData: { selectedGroups: groups },
  });
  const groupList = groups.slice(0, 10).map(g => `• ${esc(g.subject)}`).join("\n");
  const more = groups.length > 10 ? `\n<i>...+${groups.length - 10} more</i>` : "";
  await ctx.editMessageText(
    `✅ <b>${groups.length} group(s) selected:</b>\n\n${groupList}${more}\n\n<b>Choose what to remove:</b>`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🗑️ Remove All Members", "rm_mode_members").row().text("👥 Remove Friend", "rm_mode_friend").row().text("❌ Cancel", "main_menu") }
  );
});

// ─── Approval — By Link ───────────────────────────────────────────
bot.callbackQuery("ap_by_link", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }); return;
  }
  userStates.set(userId, { step: "ap_enter_links_bl", apLinkBuffer: [] });
  apLinkCollectMsgId.delete(userId);
  await ctx.editMessageText(
    byLinkPrompt("Approval", "✅", 0, "ap_links_done"),
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("ap_links_done", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state || state.step !== "ap_enter_links_bl") return;
  const buffer = state.apLinkBuffer || [];
  if (!buffer.length) { ctx.answerCallbackQuery({ text: "❌ Send at least one link first!", show_alert: true }); return; }
  apLinkCollectMsgId.delete(userId);
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;
  await ctx.editMessageText("🔗 <b>Resolving group links...</b>\n\n⏳ Starting...", { parse_mode: "HTML" });
  const groups = await resolveLinksWithProgress(userId, buffer, chatId, msgId);
  if (!groups.length) {
    await ctx.editMessageText("❌ Could not resolve any links. Check links and WhatsApp connection.", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔗 Try Again", "ap_by_link").text("🏠 Menu", "main_menu") }); return;
  }
  const allIdxs = new Set(groups.map((_, i) => i));
  userStates.set(userId, {
    step: "approval_menu",
    approvalData: { allGroups: groups, patterns: detectSimilarGroups(groups), selectedIndices: allIdxs, page: 0 },
  });
  const preview = groups.slice(0, 30).map(g => `• ${esc(g.subject)}`).join("\n");
  const more = groups.length > 30 ? `\n... +${groups.length - 30} more` : "";
  await ctx.editMessageText(
    `✅ <b>${groups.length} group(s) selected:</b>\n\n${preview}${more}\n\n📌 <b>Choose approval type:</b>`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("👥 All Approval", "ap_type_all").text("👑 Admin Approval", "ap_type_admin").row().text("❌ Cancel", "main_menu") }
  );
});

// ─── Make Admin — By Link ─────────────────────────────────────────
bot.callbackQuery("ma_by_link", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }); return;
  }
  userStates.set(userId, { step: "ma_enter_links_bl", maLinkBuffer: [] });
  maLinkCollectMsgId.delete(userId);
  await ctx.editMessageText(
    byLinkPrompt("Make Admin", "👑", 0, "ma_links_done"),
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("ma_links_done", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state || state.step !== "ma_enter_links_bl") return;
  const buffer = state.maLinkBuffer || [];
  if (!buffer.length) { ctx.answerCallbackQuery({ text: "❌ Send at least one link first!", show_alert: true }); return; }
  maLinkCollectMsgId.delete(userId);
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;
  await ctx.editMessageText("🔗 <b>Resolving group links...</b>\n\n⏳ Starting...", { parse_mode: "HTML" });
  const groups = await resolveLinksWithProgress(userId, buffer, chatId, msgId);
  if (!groups.length) {
    await ctx.editMessageText("❌ Could not resolve any links. Check links and WhatsApp connection.", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔗 Try Again", "ma_by_link").text("🏠 Menu", "main_menu") }); return;
  }
  const allIdxs = new Set(groups.map((_, i) => i));
  userStates.set(userId, {
    step: "make_admin_enter_numbers",
    makeAdminData: { allGroups: groups, patterns: detectSimilarGroups(groups), selectedIndices: allIdxs, page: 0 },
  });
  const groupList = groups.slice(0, 60).map(g => `• ${esc(g.subject)}`).join("\n");
  const moreText = groups.length > 60 ? `\n... +${groups.length - 60} more group(s)` : "";
  await ctx.editMessageText(
    `✅ <b>${groups.length} group(s) selected:</b>\n\n${groupList}${moreText}\n\n` +
    `📱 <b>Send phone number(s)</b>\n\nSend the phone numbers (with country code) of people you want to make admin, one per line:\n\n` +
    `Example:\n<code>+919912345678\n+919998887777</code>`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

// ─── Demote Admin — By Link ───────────────────────────────────────
bot.callbackQuery("da_by_link", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }); return;
  }
  userStates.set(userId, { step: "da_enter_links_bl", daLinkBuffer: [] });
  daLinkCollectMsgId.delete(userId);
  await ctx.editMessageText(
    byLinkPrompt("Demote Admin", "👤", 0, "da_links_done"),
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("da_links_done", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state || state.step !== "da_enter_links_bl") return;
  const buffer = state.daLinkBuffer || [];
  if (!buffer.length) { ctx.answerCallbackQuery({ text: "❌ Send at least one link first!", show_alert: true }); return; }
  daLinkCollectMsgId.delete(userId);
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;
  await ctx.editMessageText("🔗 <b>Resolving group links...</b>\n\n⏳ Starting...", { parse_mode: "HTML" });
  const groups = await resolveLinksWithProgress(userId, buffer, chatId, msgId);
  if (!groups.length) {
    await ctx.editMessageText("❌ Could not resolve any links. Check links and WhatsApp connection.", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔗 Try Again", "da_by_link").text("🏠 Menu", "main_menu") }); return;
  }
  const allIdxs = new Set(groups.map((_, i) => i));
  userStates.set(userId, {
    step: "demote_admin_menu",
    demoteAdminData: { allGroups: groups, patterns: detectSimilarGroups(groups), selectedIndices: allIdxs, page: 0 },
  });
  const groupList = groups.slice(0, 20).map(g => `• ${esc(g.subject)}`).join("\n");
  const more = groups.length > 20 ? `\n... +${groups.length - 20} more` : "";
  await ctx.editMessageText(
    `👤 <b>Demote Admin</b>\n\n<b>${groups.length} group(s) selected:</b>\n${groupList}${more}\n\nChoose demote mode:`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔴 Demote All Admins", "da_mode_all").row().text("📱 Demote Selected Numbers", "da_mode_numbers").row().text("🏠 Menu", "main_menu") }
  );
});

// ─── Edit Settings — By Link ──────────────────────────────────────
bot.callbackQuery("es_by_link", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }); return;
  }
  userStates.set(userId, { step: "es_enter_links_bl", esLinkBuffer: [] });
  esLinkCollectMsgId.delete(userId);
  await ctx.editMessageText(
    byLinkPrompt("Edit Settings", "⚙️", 0, "es_links_done"),
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("es_links_done", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state || state.step !== "es_enter_links_bl") return;
  const buffer = state.esLinkBuffer || [];
  if (!buffer.length) { ctx.answerCallbackQuery({ text: "❌ Send at least one link first!", show_alert: true }); return; }
  esLinkCollectMsgId.delete(userId);
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;
  await ctx.editMessageText("🔗 <b>Resolving group links...</b>\n\n⏳ Starting...", { parse_mode: "HTML" });
  const groups = await resolveLinksWithProgress(userId, buffer, chatId, msgId);
  if (!groups.length) {
    await ctx.editMessageText("❌ Could not resolve any links. Check links and WhatsApp connection.", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔗 Try Again", "es_by_link").text("🏠 Menu", "main_menu") }); return;
  }
  const allIdxs = new Set(groups.map((_, i) => i));
  userStates.set(userId, {
    step: "edit_settings_menu",
    editSettingsData: { allGroups: groups, patterns: detectSimilarGroups(groups), selectedIndices: allIdxs, page: 0, settings: defaultGroupSettings(), cancelled: false },
  });
  await ctx.editMessageText(
    `⚙️ <b>Edit Settings</b>\n\n<i>${groups.length} group(s) selected via links</i>`,
    { parse_mode: "HTML", reply_markup: buildEditSettingsGroupKeyboard(userStates.get(userId)!) }
  );
});

// ─── Change Group Name — By Link ──────────────────────────────────
bot.callbackQuery("cgn_by_link", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu") }); return;
  }
  userStates.set(userId, { step: "cgn_enter_links_bl", cgnLinkBuffer: [] });
  cgnLinkCollectMsgId.delete(userId);
  await ctx.editMessageText(
    byLinkPrompt("Change Group Name", "🏷️", 0, "cgn_links_done"),
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("cgn_links_done", async (ctx) => {
  ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state || state.step !== "cgn_enter_links_bl") return;
  const buffer = state.cgnLinkBuffer || [];
  if (!buffer.length) { ctx.answerCallbackQuery({ text: "❌ Send at least one link first!", show_alert: true }); return; }
  cgnLinkCollectMsgId.delete(userId);
  const chatId = ctx.callbackQuery.message!.chat.id;
  const msgId = ctx.callbackQuery.message!.message_id;
  await ctx.editMessageText("🔗 <b>Resolving group links...</b>\n\n⏳ Starting...", { parse_mode: "HTML" });
  const groups = await resolveLinksWithProgress(userId, buffer, chatId, msgId);
  if (!groups.length) {
    await ctx.editMessageText("❌ Could not resolve any links. Check links and WhatsApp connection.", { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔗 Try Again", "cgn_by_link").text("🏠 Menu", "main_menu") }); return;
  }
  // Enter manual naming flow with pre-selected groups
  const groupIds = groups.map(g => g.id);
  userStates.set(userId, {
    step: "cgn_manual_naming_choose",
    changeGroupNameData: {
      mode: "manual",
      allGroups: groups,
      patterns: detectSimilarGroups(groups),
      selectionPool: groups,
      selectionPoolLabel: "By Link",
      selectedGroupIds: groupIds,
      page: 0,
    },
  });
  await ctx.editMessageText(
    `🏷️ <b>Change Group Name — ${groups.length} group(s) from links</b>\n\n` +
    `Groups (in order):\n` +
    groups.slice(0, 15).map((g, i) => `${i + 1}. ${esc(g.subject)}`).join("\n") +
    (groups.length > 15 ? `\n... +${groups.length - 15} more` : "") +
    `\n\n📌 <b>Choose naming method:</b>`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("🔢 Auto-numbered", "cgn_m_naming_auto").row()
        .text("✏️ Custom Names", "cgn_m_naming_custom").row()
        .text("❌ Cancel", "main_menu"),
    }
  );
});

// ══════════════════════════════════════════════════════════════════
// TEXT HANDLERS for by-link accumulation steps
// ══════════════════════════════════════════════════════════════════

// These are injected before the catch-all text handler
// They accumulate links in the buffer and show a running count.

// Note: These handlers are registered below BEFORE the main text handler
// to ensure they intercept first. The main text handler is already registered
// above, so we use a separate middleware to handle these states.

bot.on("message:text", async (ctx, next) => {
  const userId = ctx.from?.id;
  if (!userId) return next();
  const state = userStates.get(userId);
  if (!state) return next();
  const text = ctx.message.text || "";

  // ── lv_enter_links_bl ────────────────────────────────────────────
  if (state.step === "lv_enter_links_bl") {
    const links = extractLinksFromText(text);
    if (!links.length) return;
    if (!state.lvLinkBuffer) state.lvLinkBuffer = [];
    state.lvLinkBuffer.push(...links);
    const total = state.lvLinkBuffer.length;
    const prompt = byLinkPrompt("Leave Group", "🚪", total, "lv_links_done");
    const kb = new InlineKeyboard().text("✅ Done", "lv_links_done").row().text("❌ Cancel", "main_menu");
    const existingId = lvLinkCollectMsgId.get(userId);
    if (existingId) {
      try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
    }
    const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
    lvLinkCollectMsgId.set(userId, m.message_id);
    return;
  }

  // ── rm_enter_links_bl ────────────────────────────────────────────
  if (state.step === "rm_enter_links_bl") {
    const links = extractLinksFromText(text);
    if (!links.length) return;
    if (!state.rmLinkBuffer) state.rmLinkBuffer = [];
    state.rmLinkBuffer.push(...links);
    const total = state.rmLinkBuffer.length;
    const prompt = byLinkPrompt("Remove Members", "🗑️", total, "rm_links_done");
    const kb = new InlineKeyboard().text("✅ Done", "rm_links_done").row().text("❌ Cancel", "main_menu");
    const existingId = rmLinkCollectMsgId.get(userId);
    if (existingId) {
      try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
    }
    const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
    rmLinkCollectMsgId.set(userId, m.message_id);
    return;
  }

  // ── ap_enter_links_bl ────────────────────────────────────────────
  if (state.step === "ap_enter_links_bl") {
    const links = extractLinksFromText(text);
    if (!links.length) return;
    if (!state.apLinkBuffer) state.apLinkBuffer = [];
    state.apLinkBuffer.push(...links);
    const total = state.apLinkBuffer.length;
    const prompt = byLinkPrompt("Approval", "✅", total, "ap_links_done");
    const kb = new InlineKeyboard().text("✅ Done", "ap_links_done").row().text("❌ Cancel", "main_menu");
    const existingId = apLinkCollectMsgId.get(userId);
    if (existingId) {
      try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
    }
    const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
    apLinkCollectMsgId.set(userId, m.message_id);
    return;
  }

  // ── ma_enter_links_bl ────────────────────────────────────────────
  if (state.step === "ma_enter_links_bl") {
    const links = extractLinksFromText(text);
    if (!links.length) return;
    if (!state.maLinkBuffer) state.maLinkBuffer = [];
    state.maLinkBuffer.push(...links);
    const total = state.maLinkBuffer.length;
    const prompt = byLinkPrompt("Make Admin", "👑", total, "ma_links_done");
    const kb = new InlineKeyboard().text("✅ Done", "ma_links_done").row().text("❌ Cancel", "main_menu");
    const existingId = maLinkCollectMsgId.get(userId);
    if (existingId) {
      try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
    }
    const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
    maLinkCollectMsgId.set(userId, m.message_id);
    return;
  }

  // ── da_enter_links_bl ────────────────────────────────────────────
  if (state.step === "da_enter_links_bl") {
    const links = extractLinksFromText(text);
    if (!links.length) return;
    if (!state.daLinkBuffer) state.daLinkBuffer = [];
    state.daLinkBuffer.push(...links);
    const total = state.daLinkBuffer.length;
    const prompt = byLinkPrompt("Demote Admin", "👤", total, "da_links_done");
    const kb = new InlineKeyboard().text("✅ Done", "da_links_done").row().text("❌ Cancel", "main_menu");
    const existingId = daLinkCollectMsgId.get(userId);
    if (existingId) {
      try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
    }
    const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
    daLinkCollectMsgId.set(userId, m.message_id);
    return;
  }

  // ── es_enter_links_bl ────────────────────────────────────────────
  if (state.step === "es_enter_links_bl") {
    const links = extractLinksFromText(text);
    if (!links.length) return;
    if (!state.esLinkBuffer) state.esLinkBuffer = [];
    state.esLinkBuffer.push(...links);
    const total = state.esLinkBuffer.length;
    const prompt = byLinkPrompt("Edit Settings", "⚙️", total, "es_links_done");
    const kb = new InlineKeyboard().text("✅ Done", "es_links_done").row().text("❌ Cancel", "main_menu");
    const existingId = esLinkCollectMsgId.get(userId);
    if (existingId) {
      try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
    }
    const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
    esLinkCollectMsgId.set(userId, m.message_id);
    return;
  }

  // ── cgn_enter_links_bl ───────────────────────────────────────────
  if (state.step === "cgn_enter_links_bl") {
    const links = extractLinksFromText(text);
    if (!links.length) return;
    if (!state.cgnLinkBuffer) state.cgnLinkBuffer = [];
    state.cgnLinkBuffer.push(...links);
    const total = state.cgnLinkBuffer.length;
    const prompt = byLinkPrompt("Change Group Name", "🏷️", total, "cgn_links_done");
    const kb = new InlineKeyboard().text("✅ Done", "cgn_links_done").row().text("❌ Cancel", "main_menu");
    const existingId = cgnLinkCollectMsgId.get(userId);
    if (existingId) {
      try { await bot.api.editMessageText(ctx.chat.id, existingId, prompt, { parse_mode: "HTML", reply_markup: kb }); return; } catch {}
    }
    const m = await ctx.reply(prompt, { parse_mode: "HTML", reply_markup: kb });
    cgnLinkCollectMsgId.set(userId, m.message_id);
    return;
  }

  return next();
});

export { bot };

