import { Bot, InlineKeyboard, InputFile } from "grammy";
import {
  connectWhatsApp,
  connectWhatsAppQr,
  isConnected,
  disconnectWhatsApp,
  createWhatsAppGroup,
  applyGroupSettings,
  setGroupIcon,
  joinGroupWithLink,
  getGroupPendingRequests,
  getGroupPendingRequestsJids,
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
} from "./whatsapp";
import { parseVCF, normalizePhone } from "./vcf-parser";
import QRCode from "qrcode";
import https from "https";
import http from "http";
import {
  loadBotData,
  saveBotData,
  trackUser as trackUserMongo,
  isUserBanned,
  hasUserAccess,
} from "./mongo-bot-data";

const token = process.env["TELEGRAM_BOT_TOKEN"] || "";

const ADMIN_USER_ID = Number(process.env["ADMIN_USER_ID"] || "0");
const FORCE_SUB_CHANNEL = process.env["FORCE_SUB_CHANNEL"] || "";
const OWNER_USERNAME = "@SPIDYWS";
const BOT_DISPLAY_NAME = "ᴡꜱ ᴀᴜᴛᴏᴍᴀᴛɪᴏɴ";

const bot = new Bot(token || "placeholder");

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
  dpFileId: string | null;
  dpBuffer: Buffer | null;
  editGroupInfo: boolean;
  sendMessages: boolean;
  addMembers: boolean;
  approveJoin: boolean;
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
  ctcData?: {
    groupLinks: string[];
    pairs: CtcPair[];
    currentPairIndex: number;
  };
  joinData?: { links: string[] };
  leaveData?: {
    groups: Array<{ id: string; subject: string; isAdmin: boolean }>;
    mode: "member" | "admin" | "all";
  };
  removeData?: {
    allGroups: Array<{ id: string; subject: string }>;
    selectedIndices: Set<number>;
    page: number;
  };
  removeExcludeData?: {
    selectedGroups: Array<{ id: string; subject: string }>;
    excludeNumbers: Set<string>;
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
  };
  addMembersData?: {
    groupLink: string;
    groupId: string;
    groupName: string;
    friendNumbers: string[];
    adminContacts: Array<{ name: string; phone: string }>;
    navyContacts: Array<{ name: string; phone: string }>;
    memberContacts: Array<{ name: string; phone: string }>;
    totalToAdd: number;
    mode: "one_by_one" | "together" | "";
    delaySeconds: number;
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
  botMode: "single" | "both";
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
];

const autoChatSessions: Map<number, AutoChatSession> = new Map();
const cigSessions: Map<number, CigSession> = new Map();
const acfSessions: Map<number, AcfSession> = new Map();
const userStates: Map<number, UserState> = new Map();
const joinCancelRequests: Set<number> = new Set();
const getLinkCancelRequests: Set<number> = new Set();
const addMembersCancelRequests: Set<number> = new Set();

const MA_PAGE_SIZE = 20;
const PL_PAGE_SIZE = 20;
const AP_PAGE_SIZE = 20;

function pendingSumExpression(items: Array<{ pendingCount: number }>): string {
  const counts = items.map((g) => g.pendingCount).filter((count) => count > 0);
  const total = counts.reduce((sum, count) => sum + count, 0);
  return counts.length ? `${counts.join("+")} = ${total}` : "0";
}

function pendingCopyText(title: string, items: Array<{ groupName: string; pendingCount: number }>): string {
  let text = `📋 <b>${esc(title)}</b>\n\n<pre>`;
  for (const g of items) {
    text += `${esc(g.groupName)} ✅ ${g.pendingCount}\n`;
  }
  text += `\nTotal sum = ${pendingSumExpression(items)}`;
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
    .text("🤖 Auto Chat", "auto_chat_menu").row()
    .text("🔌 Disconnect", "disconnect_wa");
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
      await ctx.editMessageText(
        mainMenuText(userId, "welcome"),
        { parse_mode: "HTML", reply_markup: mainMenu(userId) }
      );
      return;
    }
  } catch {}
  await ctx.answerCallbackQuery({ text: "❌ You haven't joined the channel yet!", show_alert: true });
});

bot.command("start", async (ctx) => {
  const userId = ctx.from!.id;
  await trackUser(userId);
  if (await isBanned(userId)) {
    await ctx.reply("🚫 You are banned from using this bot.");
    return;
  }
  if (!(await checkForceSub(ctx))) return;
  if (!(await hasAccess(userId))) {
    await ctx.reply(
      `🔒 <b>Subscription Required!</b>\n\n` +
      `This bot requires a subscription to use.\n\n` +
      `👤 Contact owner: <b>${OWNER_USERNAME}</b>\n` +
      `📩 Ask admin for access.`,
      { parse_mode: "HTML" }
    );
    return;
  }
  userStates.delete(userId);
  await ctx.reply(
    mainMenuText(userId, "welcome"),
    { parse_mode: "HTML", reply_markup: mainMenu(userId) }
  );
});

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
    `• Phone number do → 8-digit pairing code milega\n` +
    `• WhatsApp → Linked Devices mein code daalo\n` +
    `• Ek baar connect hone ke baad sab features use karo\n\n` +

    `🏗️ 2. Create Groups\n` +
    `• Ek saath kaafi saare WhatsApp groups banao\n` +
    `• Custom ya auto-numbered names (e.g. Group 1, Group 2...)\n` +
    `• Group description aur DP (icon) set kar sakte ho\n` +
    `• Permissions: kaun message, kaun add kar sakta hai\n` +
    `• Approval mode ON/OFF kar sakte ho\n` +
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
    `• Group link do → Friend numbers do → Admin/Navy/Member VCF do\n` +
    `• Total kitna add karna hai batao\n` +
    `• Add 1 by 1 (safe, with delay) ya Add Together (fast, ek baar mein)\n` +
    `• Live progress dikhta hai\n` +
    `• Invite/Cancel errors automatic skip hote hain\n` +
    `• Beech mein cancel kar sakte ho\n\n` +

    `━━━━━━━━━━━━━━━━━━\n\n` +
    `💬 Commands:\n` +
    `/start — Bot start karo & main menu dekho\n` +
    `/help  — Yeh help message dekho\n\n` +

    `━━━━━━━━━━━━━━━━━━\n\n` +
    `⚠️ Important Notes:\n` +
    `• CTC Pending ke liye aap group admin hone chahiye\n` +
    `• Group mein "Approval required" mode ON hona chahiye\n` +
    `• 1 by 1 Approval ke liye bhi admin hona zaroori hai`;

  const helpText =
    `👤 <b>Owner:</b> ${OWNER_USERNAME}\n\n` +
    `<pre>${codeBlock}</pre>\n\n` +
    `👤 <b>Owner:</b> ${OWNER_USERNAME}`;

  await ctx.reply(helpText, {
    parse_mode: "HTML",
    reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
  });
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

  // Sort by pending count descending
  pendingOnly.sort((a, b) => b.pendingCount - a.pendingCount);

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
    "📊 <code>/status</code> — View bot statistics",
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
    const exp = new Date(data.accessList[String(targetId)].expiresAt).toUTCString();
    await ctx.reply(`✅ <b>Access Granted!</b>\n\n👤 User: <code>${targetId}</code>\n📅 Days: ${days}\n⏰ Expires: ${exp}`, { parse_mode: "HTML" });
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
  await ctx.reply(
    "📊 <b>Bot Status</b>\n\n" +
    `🔒 <b>Subscription Mode:</b> ${data.subscriptionMode ? "ON 🟢" : "OFF 🔴"}\n` +
    `👑 <b>Owner:</b> ${OWNER_USERNAME}\n` +
    `👥 <b>Total Users:</b> ${data.totalUsers.length}\n\n` +
    `✅ <b>Access List (${Object.keys(data.accessList).length}):</b>\n${accessText || "  None\n"}\n` +
    `🚫 <b>Banned (${data.bannedUsers.length}):</b>\n${bannedText}`,
    { parse_mode: "HTML" }
  );
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
  return { name: "", description: "", count: 1, finalNames: [], namingMode: "auto", dpFileId: null, dpBuffer: null, editGroupInfo: true, sendMessages: true, addMembers: true, approveJoin: false };
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
  state.step = "group_dp";
  await ctx.editMessageText(
    "🖼️ <b>Group Profile Photo</b>\n\nSend a photo to use as group DP.\nOr skip to create groups without a photo.",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "group_dp_skip").text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("group_dp_skip", async (ctx) => {
  await ctx.answerCallbackQuery();
  const state = userStates.get(ctx.from.id);
  if (!state?.groupSettings) return;
  state.groupSettings.dpFileId = null; state.groupSettings.dpBuffer = null;
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
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.groupSettings) return;
  const gs = state.groupSettings;
  const namesList = gs.finalNames.map((n, i) => `${i + 1}. ${esc(n)}`).join("\n");
  state.step = "group_confirm";
  const text =
    "📋 <b>Group Creation Summary</b>\n\n" +
    `📝 <b>Names (${gs.finalNames.length}):</b>\n${namesList}\n\n` +
    `📄 <b>Description:</b> ${gs.description ? esc(gs.description) : "None"}\n` +
    `🖼️ <b>Group DP:</b> ${gs.dpBuffer ? "✅ Yes" : "❌ None"}\n\n` +
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
  }
  await ctx.editMessageText(
    "🛑 <b>Group creation cancelled.</b>\n\nGroups already created will remain.",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
  );
});

bot.callbackQuery("group_cancel_dismiss", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "▶️ Continuing..." });
});

async function createGroupsBackground(userId: string, numericUserId: number, gs: GroupSettings, chatId: number, msgId: number) {
  const perms: GroupPermissions = { editGroupInfo: gs.editGroupInfo, sendMessages: gs.sendMessages, addMembers: gs.addMembers, approveJoin: gs.approveJoin };
  const results: Array<{ name: string; link: string | null; error?: string }> = [];
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
      const result = await createWhatsAppGroup(userId, groupName);
      if (result) {
        await new Promise((r) => setTimeout(r, 1500));
        await applyGroupSettings(userId, result.id, perms, gs.description);
        if (gs.dpBuffer) { await new Promise((r) => setTimeout(r, 2000)); await setGroupIcon(userId, result.id, gs.dpBuffer); }
        results.push({ name: groupName, link: result.inviteCode });
      } else {
        results.push({ name: groupName, link: null, error: "Failed to create" });
      }
    } catch (err: any) {
      results.push({ name: groupName, link: null, error: err?.message || "Unknown error" });
    }

    const done = i + 1;
    const lines = results.map((r) => r.link ? `✅ ${esc(r.name)}` : `❌ ${esc(r.name)}`).join("\n");
    try {
      await bot.api.editMessageText(chatId, msgId,
        `⏳ <b>Creating Groups: ${done}/${total}</b>\n\n${lines}${done < total ? "\n\n⌛ Processing..." : ""}`,
        { parse_mode: "HTML", reply_markup: done < total ? new InlineKeyboard().text("❌ Cancel Creation", "group_cancel_creation") : undefined }
      );
    } catch {}

    if (i < total - 1) await new Promise((r) => setTimeout(r, 2000));
  }

  userStates.delete(numericUserId);

  const cancelled = results.some((r) => r.error === "Cancelled by user");
  const created = results.filter((r) => r.link).length;
  let message = cancelled
    ? `🛑 <b>Cancelled! (${created}/${total} created before cancel)</b>\n\n`
    : `🎉 <b>Done! (${created}/${total} created)</b>\n\n`;
  for (const r of results) {
    if (r.error === "Cancelled by user") {
      message += `🛑 <b>${esc(r.name)}</b>\n⚠️ Cancelled\n\n`;
    } else {
      message += r.link ? `✅ <b>${esc(r.name)}</b>\n🔗 ${r.link}\n\n` : `❌ <b>${esc(r.name)}</b>\n⚠️ ${esc(r.error || "")}\n\n`;
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
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard()
      .text("✅ Yes, Stop Joining", "join_cancel_confirm")
      .text("↩️ Continue", "join_cancel_no"),
  });
});

bot.callbackQuery("join_cancel_no", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Joining continued" });
  const userId = ctx.from.id;
  if (joinCancelRequests.has(userId)) return;
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard().text("❌ Cancel", "join_cancel_request"),
  });
});

bot.callbackQuery("join_cancel_confirm", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "Stopping after current group..." });
  joinCancelRequests.add(ctx.from.id);
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

async function ctcCheckBackground(userId: string, activePairs: CtcPair[], chatId: number, msgId: number) {
  let result = "📊 <b>CTC Check Results</b>\n\n";

  // Collect all VCF phone numbers across all pairs for duplicate detection
  // Map: phone number → list of group names it appears as pending
  const pendingPhoneToGroups = new Map<string, string[]>();

  // First pass: collect results per group
  const groupResults: Array<{
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
        `⏳ <b>Checking group ${i + 1}/${activePairs.length}...</b>\n\n⌛ Please wait...`,
        { parse_mode: "HTML" }
      );
    } catch {}

    const groupInfo = await getGroupIdFromLink(userId, cleanLink);
    if (!groupInfo) {
      groupResults.push({
        groupName: `Group ${i + 1} (could not access)`,
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
      if (!pendingPhoneToGroups.has(phone)) {
        pendingPhoneToGroups.set(phone, []);
      }
      pendingPhoneToGroups.get(phone)!.push(groupInfo.subject);
    }

    groupResults.push({
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

  // Build all VCF phone numbers across ALL pairs for wrong-adding detection
  // Wrong adding: a number is a group MEMBER (not pending) but NOT in the VCF for that group
  for (let i = 0; i < activePairs.length; i++) {
    const pair = activePairs[i];
    const gr = groupResults[i];
    if (!gr || gr.couldNotAccess) {
      result += `❌ <b>Group ${i + 1}</b>: Could not access\n${esc(gr?.link || pair.link)}\n\n`;
      continue;
    }

    const inMembersSet = new Set(gr.inMembers.map(p => p.replace(/[^0-9]/g, "")));
    const inPendingSet = new Set(gr.inPending.map(p => p.replace(/[^0-9]/g, "")));

    const inMembersContacts = gr.vcfContacts.filter((c) => inMembersSet.has(c.phone.replace(/[^0-9]/g, "")));
    const inPendingContacts = gr.vcfContacts.filter((c) => inPendingSet.has(c.phone.replace(/[^0-9]/g, "")));
    const notFoundContacts = gr.vcfContacts.filter((c) => !inMembersSet.has(c.phone.replace(/[^0-9]/g, "")) && !inPendingSet.has(c.phone.replace(/[^0-9]/g, "")));

    // Build VCF phone set (last-10-digit) for robust matching
    const vcfLast10Set = new Set(gr.vcfContacts.map(c => c.phone.replace(/[^0-9]/g, "").slice(-10)));

    // Wrong Adding: pending contacts that are NOT in the VCF
    // (someone who requested to join but is NOT in your contact list)
    const wrongAdding: string[] = [];
    for (const pendingPhone of gr.allPendingPhones) {
      const pLast10 = pendingPhone.slice(-10);
      if (pLast10.length >= 7 && !vcfLast10Set.has(pLast10)) {
        wrongAdding.push("+" + pendingPhone);
      }
    }

    // Members Not in VCF: actual group members who are NOT in the VCF
    const membersNotInVcf: string[] = [];
    for (const memberPhone of gr.allMemberPhones) {
      const mLast10 = memberPhone.slice(-10);
      if (mLast10.length >= 7 && !vcfLast10Set.has(mLast10)) {
        membersNotInVcf.push("+" + memberPhone);
      }
    }

    // Group header
    result += `📋 <b>${esc(gr.groupName)}</b>\n`;
    result += `🔗 ${esc(gr.link)}\n`;
    if (!gr.pendingAvailable) {
      result += `⚠️ <i>Pending detection unavailable (need to be admin + "Approval required" ON)</i>\n`;
    }
    result += "\n";

    // ⚠️ Wrong Adding: pending contacts NOT in VCF
    if (wrongAdding.length > 0) {
      result += `⚠️ <b>Wrong Pending Request (${wrongAdding.length}):</b>\n`;
      result += `<i>These numbers are in pending but NOT in your VCF</i>\n`;
      for (const phone of wrongAdding) result += `  ⚠️ ${esc(phone)}\n`;
      result += "\n";
    } else {
      result += `⚠️ <b>Wrong Pending Request: 0</b>\n\n`;
    }

    // ✅ Correct Pending: pending contacts that ARE in VCF — show per-VCF summary
    if (inPendingContacts.length > 0) {
      // Group by VCF file name
      const byVcf = new Map<string, number>();
      for (const c of inPendingContacts) {
        byVcf.set(c.vcfFileName, (byVcf.get(c.vcfFileName) || 0) + 1);
      }
      result += `✅ <b>Correct Pending (${inPendingContacts.length}):</b>\n`;
      for (const [vcf, count] of byVcf.entries()) {
        result += `  📄 ${esc(vcf)} — ${count} contact${count > 1 ? "s" : ""} pending\n`;
      }
      result += "\n";
    }

    result += "━━━━━━━━━━━━━━━━━━\n\n";
  }

  // Duplicate pending detection: contacts in pending of multiple groups
  const duplicates: Array<{ phone: string; groups: string[] }> = [];
  for (const [phone, groups] of pendingPhoneToGroups.entries()) {
    if (groups.length > 1) {
      duplicates.push({ phone: "+" + phone, groups });
    }
  }

  if (duplicates.length > 0) {
    result += `🔁 <b>Duplicate Pending Contacts (${duplicates.length}):</b>\n`;
    result += `<i>Same contact pending in multiple groups</i>\n\n`;
    for (const d of duplicates) {
      result += `  🔁 ${esc(d.phone)}\n`;
      for (const g of d.groups) result += `    📌 ${esc(g)}\n`;
    }
    result += "\n━━━━━━━━━━━━━━━━━━\n\n";
  }

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
    .map(([, items]) => ({ base: items[0].subject.replace(/\s*\d+\s*$/, "").trim(), groups: items }));
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
  const allGroupsSimple = adminGroups.map((g) => ({ id: g.id, subject: g.subject }));
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
  await ctx.answerCallbackQuery();
  await ctx.editMessageReplyMarkup({
    reply_markup: new InlineKeyboard()
      .text("✅ Yes, Stop Fetch", "gl_cancel_confirm")
      .text("↩️ Continue", "gl_cancel_no"),
  });
});

bot.callbackQuery("gl_cancel_no", async (ctx) => {
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
const GL_BATCH_DELAY_MS = 600;

async function fetchGroupLinksBackground(
  userId: string,
  groups: Array<{ id: string; subject: string }>,
  chatId: number,
  msgId: number,
  mode: "all" | "similar",
  patternBase?: string
) {
  const results: Array<{ subject: string; link: string | null }> = new Array(groups.length).fill(null).map((_, i) => ({ subject: groups[i].subject, link: null }));
  let fetchedCount = 0;
  let successCount = 0;

  const updateProgress = async () => {
    try {
      const label = mode === "similar" ? `Fetching links for "${esc(patternBase!)}" groups` : "Fetching all group links";
      await bot.api.editMessageText(chatId, msgId,
        `⏳ <b>${label}...</b>\n\n📊 ${fetchedCount}/${groups.length} fetched | ✅ ${successCount} links found`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "gl_cancel_request") }
      );
    } catch {}
  };

  for (let i = 0; i < groups.length; i += GL_BATCH_SIZE) {
    if (getLinkCancelRequests.has(Number(userId))) break;
    const batch = groups.slice(i, i + GL_BATCH_SIZE);
    const batchResults = await Promise.allSettled(
      batch.map((g) => getGroupInviteLink(userId, g.id, 3))
    );

    for (let j = 0; j < batch.length; j++) {
      const res = batchResults[j];
      const link = res.status === "fulfilled" ? res.value : null;
      results[i + j].link = link;
      fetchedCount++;
      if (link) successCount++;
    }

    await updateProgress();
    if (i + GL_BATCH_SIZE < groups.length) {
      await new Promise((r) => setTimeout(r, GL_BATCH_DELAY_MS));
    }
  }

  const wasCancelled = getLinkCancelRequests.has(Number(userId));
  getLinkCancelRequests.delete(Number(userId));

  let result: string;
  if (mode === "similar") {
    result = `🔗 <b>"${esc(patternBase!)}" Pattern</b>\n`;
    result += `📊 <b>Total: ${groups.length} groups | ✅ ${successCount} links fetched</b>\n\n`;
  } else {
    result = `📋 <b>All Group Links</b>\n📊 <b>Total: ${groups.length} groups | ✅ ${successCount} links fetched</b>\n\n`;
  }

  if (wasCancelled) result += "⛔ <b>Fetch stopped by user.</b>\n\n";

  const successResults = results.filter((r) => r.link);
  const failedResults = results.filter((r) => !r.link);

  for (const r of successResults) {
    result += `📌 ${esc(r.subject)}\n${r.link}\n\n`;
  }

  if (failedResults.length) {
    result += "⚠️ <b>Links Not Fetched</b>\n";
    for (const r of failedResults) result += `• ${esc(r.subject)}\n`;
  }

  const kb = mode === "similar"
    ? new InlineKeyboard().text("🔙 Back", "gl_similar").text("🏠 Menu", "main_menu")
    : new InlineKeyboard().text("🏠 Main Menu", "main_menu");

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

// ─── Leave Group ─────────────────────────────────────────────────────────────

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
  await ctx.editMessageText(
    "🚪 <b>Leave Groups</b>\n\nChoose which groups to leave:",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("👤 Leave Member Groups", "leave_member").text("👑 Leave Admin Groups", "leave_admin").row()
        .text("🗑️ Leave All Groups", "leave_all").row()
        .text("🏠 Main Menu", "main_menu"),
    }
  );
});

async function showLeaveConfirmation(ctx: any, userId: number, mode: "member" | "admin" | "all") {
  await ctx.editMessageText("🔍 <b>Scanning groups...</b>", { parse_mode: "HTML" });
  const allGroups = await getAllGroups(String(userId));
  const filtered = mode === "member" ? allGroups.filter((g) => !g.isAdmin)
    : mode === "admin" ? allGroups.filter((g) => g.isAdmin)
    : allGroups;

  if (!filtered.length) {
    const label = mode === "member" ? "member" : mode === "admin" ? "admin" : "any";
    await ctx.editMessageText(`📭 No ${label} groups found.`, {
      reply_markup: new InlineKeyboard().text("🔙 Back", "leave_group").text("🏠 Menu", "main_menu"),
    }); return;
  }

  const modeLabel = mode === "member" ? "👤 Member" : mode === "admin" ? "👑 Admin" : "🗑️ All";
  let text = `🚪 <b>Leave ${modeLabel} Groups</b>\n\n`;
  text += `📊 <b>Groups to leave: ${filtered.length}</b>\n\n`;
  for (const g of filtered) text += `• ${esc(g.subject)} ${g.isAdmin ? "👑" : "👤"}\n`;
  text += "\n⚠️ <b>Are you sure you want to leave these groups?</b>";

  userStates.set(userId, {
    step: "leave_confirm",
    leaveData: { groups: filtered.map((g) => ({ id: g.id, subject: g.subject, isAdmin: g.isAdmin })), mode },
  });

  const chunks = splitMessage(text, 4000);
  for (let i = 0; i < chunks.length; i++) {
    const isLast = i === chunks.length - 1;
    const kb = isLast ? new InlineKeyboard().text("✅ Yes, Leave All", "leave_confirm_yes").text("❌ Cancel", "leave_group") : undefined;
    if (i === 0) await ctx.editMessageText(chunks[i], { parse_mode: "HTML", reply_markup: kb });
    else await ctx.reply(chunks[i], { parse_mode: "HTML", reply_markup: kb });
  }
}

bot.callbackQuery("leave_member", async (ctx) => { await ctx.answerCallbackQuery(); await showLeaveConfirmation(ctx, ctx.from.id, "member"); });
bot.callbackQuery("leave_admin", async (ctx) => { await ctx.answerCallbackQuery(); await showLeaveConfirmation(ctx, ctx.from.id, "admin"); });
bot.callbackQuery("leave_all", async (ctx) => { await ctx.answerCallbackQuery(); await showLeaveConfirmation(ctx, ctx.from.id, "all"); });

bot.callbackQuery("leave_confirm_yes", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.leaveData) return;
  const { groups } = state.leaveData;

  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  userStates.delete(userId);
  await ctx.editMessageText(`⏳ <b>Leaving ${groups.length} group(s)...</b>\n\n🔄 0/${groups.length} done...`, { parse_mode: "HTML" });

  void (async () => {
    let result = "🚪 <b>Leave Groups Result</b>\n\n";
    const lines: string[] = [];
    let success = 0, failed = 0;
    for (let li = 0; li < groups.length; li++) {
      const g = groups[li];
      const ok = await leaveGroup(String(userId), g.id);
      if (ok) { lines.push(`✅ Left: ${esc(g.subject)}`); success++; }
      else { lines.push(`❌ Failed: ${esc(g.subject)}`); failed++; }
      try {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Leaving: ${li + 1}/${groups.length}</b>\n\n${lines.join("\n")}`,
          { parse_mode: "HTML" }
        );
      } catch {}
      if (li < groups.length - 1) await new Promise((r) => setTimeout(r, 1000));
    }
    result += lines.join("\n") + `\n\n📊 <b>Done! ✅ ${success} left | ❌ ${failed} failed</b>`;
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
    },
  });

  const groupList = selectedGroups.map(g => `• ${esc(g.subject)}`).join("\n");
  await ctx.editMessageText(
    `✅ <b>${selectedGroups.length} group(s) selected:</b>\n\n${groupList}\n\n` +
    `📱 <b>Exclude Numbers</b>\n\n` +
    `If you do NOT want to remove certain numbers, send them now (one per line, with country code).\n\n` +
    `Example:\n<code>+919912345678\n+919998887777</code>\n\n` +
    `If you don't want to exclude any numbers, tap <b>Skip</b>:`,
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

  await startRemoveMembersProcess(ctx, userId, state.removeExcludeData.selectedGroups, new Set());
});

async function startRemoveMembersProcess(
  ctx: any,
  userId: number,
  selectedGroups: Array<{ id: string; subject: string }>,
  excludeNumbers: Set<string>
) {
  const chatId = ctx.callbackQuery?.message?.chat.id || ctx.chat?.id;
  const msgId = ctx.callbackQuery?.message?.message_id;

  userStates.delete(userId);

  const excludeList = Array.from(excludeNumbers).map(n => n.replace(/[^0-9]/g, ""));

  const groupList = selectedGroups.map(g => `• ${esc(g.subject)}`).join("\n");
  const excludeText = excludeList.length > 0
    ? `\n🚫 <b>Excluding ${excludeList.length} number(s)</b>`
    : "";

  const statusText = `⏳ <b>Removing members from ${selectedGroups.length} group(s)...</b>\n\n${groupList}${excludeText}\n\n⌛ Please wait...`;

  try {
    if (msgId) {
      await ctx.editMessageText(statusText, { parse_mode: "HTML" });
    } else {
      await ctx.reply(statusText, { parse_mode: "HTML" });
    }
  } catch {}

  void removeAllGroupMembersBackground(String(userId), selectedGroups, excludeList, chatId, msgId);
}

async function removeAllGroupMembersBackground(
  userId: string,
  groups: Array<{ id: string; subject: string }>,
  excludeNumbers: string[],
  chatId: number,
  msgId: number | undefined
) {
  let fullResult = "🗑️ <b>Remove Members Result</b>\n\n";
  const excludeSet = new Set(excludeNumbers);

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
      return true;
    });

    if (!nonAdmins.length) {
      fullResult += `📋 <b>${esc(group.subject)}</b>\n`;
      fullResult += `✅ No members to remove (all are admins or excluded)\n\n`;
      continue;
    }

    let removed = 0, failed = 0;
    for (let pi = 0; pi < nonAdmins.length; pi++) {
      const p = nonAdmins[pi];
      const ok = await removeGroupParticipant(userId, group.id, p.jid);
      if (ok) removed++;
      else failed++;

      // Update progress every 5 removals
      if (pi % 5 === 0 || pi === nonAdmins.length - 1) {
        try {
          if (msgId) {
            await bot.api.editMessageText(chatId, msgId,
              `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n🗑️ Removing: ${pi + 1}/${nonAdmins.length}\n✅ Removed: ${removed} | ❌ Failed: ${failed}`,
              { parse_mode: "HTML" }
            );
          }
        } catch {}
      }

      await new Promise((r) => setTimeout(r, 500));
    }

    fullResult += `📋 <b>${esc(group.subject)}</b>\n`;
    fullResult += `🗑️ Removed: ${removed} | ❌ Failed: ${failed}\n\n`;
  }

  fullResult += `━━━━━━━━━━━━━━━━━━\n✅ <b>Done processing ${groups.length} group(s)!</b>`;

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
  const groupList = selectedGroups.map(g => `• ${esc(g.subject)}`).join("\n");

  await ctx.editMessageText(
    `✅ <b>${selectedGroups.length} group(s) selected:</b>\n\n${groupList}\n\n` +
    `📌 <b>Choose approval method:</b>\n\n` +
    `• <b>Approve 1 by 1</b> — Approve each pending member one at a time\n` +
    `• <b>Approve Together</b> — Turn off approval setting, then turn it back on to approve all at once`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("☝️ Approve 1 by 1", "ap_one_by_one")
        .text("👥 Approve Together", "ap_together")
        .row()
        .text("❌ Cancel", "main_menu"),
    }
  );
});

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
  await ctx.editMessageText(`⏳ <b>Approving pending members 1 by 1...</b>\n\n⌛ Please wait...`, { parse_mode: "HTML" });

  void approveOneByOneBackground(String(userId), selectedGroups, chatId, msgId);
});

async function approveOneByOneBackground(
  userId: string,
  groups: Array<{ id: string; subject: string }>,
  chatId: number,
  msgId: number
) {
  let fullResult = "✅ <b>Approve 1 by 1 Result</b>\n\n";
  const lines: string[] = [];

  for (let gi = 0; gi < groups.length; gi++) {
    const group = groups[gi];

    try {
      if (msgId) {
        await bot.api.editMessageText(chatId, msgId,
          `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n⌛ Fetching pending members...`,
          { parse_mode: "HTML" }
        );
      }
    } catch {}

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
      const jid = pendingJids[pi];
      const ok = await approveGroupParticipant(userId, group.id, jid);
      if (ok) approved++;
      else failed++;

      if (pi % 3 === 0 || pi === pendingJids.length - 1) {
        try {
          if (msgId) {
            await bot.api.editMessageText(chatId, msgId,
              `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n` +
              `✅ Approving: ${pi + 1}/${pendingJids.length}\n` +
              `Approved: ${approved} | Failed: ${failed}`,
              { parse_mode: "HTML" }
            );
          }
        } catch {}
      }

      // 1s delay between approvals to avoid WhatsApp rate limiting
      await new Promise((r) => setTimeout(r, 1000));
    }

    lines.push(`📋 <b>${esc(group.subject)}</b>\n✅ Approved: ${approved} | ❌ Failed: ${failed}`);
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

    lines.push(`📋 <b>${esc(group.subject)}</b>\n✅ All pending members approved!`);
    await new Promise((r) => setTimeout(r, 1000));
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
  await disconnectWhatsApp(String(userId));
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

  if (!isAutoConnected(String(userId))) {
    await ctx.editMessageText(
      "🤖 <b>Auto Chat</b>\n\n" +
      "Dusra WhatsApp abhi connect nahi hai.\n\n" +
      "Auto Chat ke liye ek aur WhatsApp number connect karo:",
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
    "⚠️ <b>Auto Chat Band Karo?</b>\n\nKya aap auto chat band karna chahte ho?",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Haan, Band Karo", "auto_chat_stop_confirm")
        .text("❌ Wapas Jao", "auto_chat_refresh"),
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
  await ctx.editMessageText("⏹️ <b>Auto Chat band kar diya gaya!</b>", {
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
    "⚠️ <b>Auto Chat WA Disconnect karo?</b>",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Haan", "auto_disconnect_confirm")
        .text("❌ Cancel", "main_menu"),
    }
  );
});

bot.callbackQuery("auto_disconnect_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const autoUserId = getAutoUserId(String(userId));
  await disconnectWhatsApp(autoUserId);
  const session = autoChatSessions.get(userId);
  if (session) { session.cancelled = true; session.running = false; }
  autoChatSessions.delete(userId);
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

  kb.text("☑️ Sab Select", "acig_select_all").text("🧹 Clear", "acig_clear_all").row();
  if (selected.size > 0) {
    kb.text(`✅ Aage Badho (${selected.size} groups)`, "acig_proceed").row();
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
  state.step = "acig_enter_message";
  const count = state.chatInGroupData.selectedIndices.size;
  await ctx.editMessageText(
    `✅ <b>${count} groups select kiye!</b>\n\n` +
    "📝 Ab wo message bhejo jo in groups me dono WhatsApp se bhejnha hai:",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("acig_confirm_start", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.chatInGroupData || !state.chatInGroupData.message) return;

  const data = state.chatInGroupData;
  const selectedGroups = [...data.selectedIndices].map(i => data.allGroups[i]);
  const autoUserId = getAutoUserId(String(userId));

  const statusMsg = await ctx.editMessageText(
    "👥 <b>Chat In Group Shuru Ho Gaya!</b>\n\n⏳ Dono WhatsApp se background me messages ja rahe hain...",
    { parse_mode: "HTML" }
  );
  const msgId = (statusMsg as any).message_id;
  const chatId = ctx.chat!.id;
  userStates.delete(userId);
  void runGroupChatDualBackground(userId, String(userId), autoUserId, chatId, msgId, selectedGroups, data.message, data.delaySeconds);
});

function cigProgressText(session: CigSession): string {
  const total = session.groups.length * (session.botMode === "both" ? 2 : 1);
  const processed = session.sent + session.failed;
  const percent = total > 0 ? Math.floor((processed / total) * 100) : 0;
  return (
    "👥 <b>Chat In Group Chal Raha Hai...</b>\n\n" +
    `📤 Sent: <b>${session.sent}</b>\n` +
    `❌ Failed: <b>${session.failed}</b>\n` +
    `📊 Progress: <b>${percent}%</b> (${processed}/${total})\n` +
    (session.botMode === "both" ? "🤖 Dono WA se bhej raha hai\n" : "") +
    "\nRoknay ke liye Stop dabao."
  );
}

async function runGroupChatDualBackground(
  userId: number,
  primaryUserId: string,
  autoUserId: string,
  chatId: number,
  msgId: number,
  groups: Array<{ id: string; subject: string }>,
  message: string,
  delaySeconds: number
): Promise<void> {
  const session: CigSession = {
    running: true,
    cancelled: false,
    chatId,
    msgId,
    groups,
    message,
    sent: 0,
    failed: 0,
    botMode: "both",
  };
  cigSessions.set(userId, session);

  try {
    for (const group of groups) {
      if (session.cancelled) break;

      const ok1 = await sendGroupMessage(primaryUserId, group.id, message);
      if (ok1) session.sent++; else session.failed++;

      try {
        await bot.api.editMessageText(chatId, msgId, cigProgressText(session), {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("🔄 Refresh", "cig_refresh")
            .text("⏹️ Stop", "cig_stop_btn").row()
            .text("🏠 Main Menu", "main_menu"),
        });
      } catch {}

      if (session.cancelled) break;
      if (delaySeconds > 0) await new Promise(r => setTimeout(r, delaySeconds * 1000));

      const ok2 = await sendGroupMessage(autoUserId, group.id, message);
      if (ok2) session.sent++; else session.failed++;

      try {
        await bot.api.editMessageText(chatId, msgId, cigProgressText(session), {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("🔄 Refresh", "cig_refresh")
            .text("⏹️ Stop", "cig_stop_btn").row()
            .text("🏠 Main Menu", "main_menu"),
        });
      } catch {}

      if (!session.cancelled && delaySeconds > 0) await new Promise(r => setTimeout(r, delaySeconds * 1000));
    }
  } catch (err: any) {
    console.error(`[ACIG][${userId}] Error:`, err?.message);
  }

  session.running = false;
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
    "⚠️ <b>Chat In Group Band Karo?</b>\n\nKya aap messages bhejnha band karna chahte ho?",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Haan, Band Karo", "cig_stop_confirm")
        .text("❌ Wapas Jao", "cig_refresh"),
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
  await ctx.editMessageText("⏹️ <b>Chat In Group band kar diya gaya!</b>", {
    parse_mode: "HTML",
    reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
  });
});

// ─── Chat Friend Feature ────────────────────────────────────────────────────────

bot.callbackQuery("acf_start", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
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
    "👫 <b>Chat Friend Shuru Ho Gaya!</b>\n\n" +
    `📞 Primary: <code>${esc(primaryNumber)}</code>\n` +
    `🤖 Auto: <code>${esc(autoNumber)}</code>\n\n` +
    `⏳ ${totalPairs} conversation pairs background me chal rahi hain...`,
    { parse_mode: "HTML" }
  );
  const msgId = (statusMsg as any).message_id;
  const chatId = ctx.chat!.id;

  void runChatFriendBackground(userId, String(userId), getAutoUserId(String(userId)), chatId, msgId, primaryJid, autoJid, totalPairs);
});

function acfProgressText(session: AcfSession): string {
  const percent = session.totalPairs > 0 ? Math.floor((session.currentPair / session.totalPairs) * 100) : 0;
  return (
    "👫 <b>Chat Friend Chal Raha Hai...</b>\n\n" +
    `💬 Pair: <b>${session.currentPair}/${session.totalPairs}</b>\n` +
    `📤 Sent: <b>${session.sent}</b>\n` +
    `❌ Failed: <b>${session.failed}</b>\n` +
    `📊 Progress: <b>${percent}%</b>\n\n` +
    "Roknay ke liye Stop dabao."
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
  };
  acfSessions.set(userId, session);

  try {
    for (let i = 0; i < CHAT_FRIEND_PAIRS.length; i++) {
      if (session.cancelled) break;
      session.currentPair = i + 1;

      const [msg1, msg2] = CHAT_FRIEND_PAIRS[i];

      const ok1 = await sendGroupMessage(primaryUserId, autoJid, msg1);
      if (ok1) session.sent++; else session.failed++;

      try {
        await bot.api.editMessageText(chatId, msgId, acfProgressText(session), {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("🔄 Refresh", "acf_refresh")
            .text("⏹️ Stop", "acf_stop_btn").row()
            .text("🏠 Main Menu", "main_menu"),
        });
      } catch {}

      if (session.cancelled) break;
      await new Promise(r => setTimeout(r, 5000));

      const ok2 = await sendGroupMessage(autoUserId, primaryJid, msg2);
      if (ok2) session.sent++; else session.failed++;

      try {
        await bot.api.editMessageText(chatId, msgId, acfProgressText(session), {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("🔄 Refresh", "acf_refresh")
            .text("⏹️ Stop", "acf_stop_btn").row()
            .text("🏠 Main Menu", "main_menu"),
        });
      } catch {}

      if (!session.cancelled) await new Promise(r => setTimeout(r, 5000));
    }
  } catch (err: any) {
    console.error(`[ACF][${userId}] Error:`, err?.message);
  }

  session.running = false;
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
    "⚠️ <b>Chat Friend Band Karo?</b>",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Haan, Band Karo", "acf_stop_confirm")
        .text("❌ Wapas Jao", "acf_refresh"),
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
  await ctx.editMessageText("⏹️ <b>Chat Friend band kar diya gaya!</b>", {
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

async function runAutoChatBackground(userId: number, autoUserId: string, chatId: number, msgId: number, groups: Array<{ id: string; subject: string }>, message: string, delaySeconds: number, repeatCount: number): Promise<void> {
  const session: AutoChatSession = {
    running: true,
    cancelled: false,
    chatId,
    msgId,
    groups,
    message,
    delaySeconds,
    repeatCount,
    sent: 0,
    failed: 0,
    currentRound: 1,
  };
  autoChatSessions.set(userId, session);

  const maxRounds = repeatCount === 0 ? Infinity : repeatCount;

  try {
    for (let round = 1; round <= maxRounds; round++) {
      if (session.cancelled) break;
      session.currentRound = round;

      for (const group of groups) {
        if (session.cancelled) break;
        const ok = await sendGroupMessage(autoUserId, group.id, message);
        if (ok) session.sent++; else session.failed++;

        try {
          await bot.api.editMessageText(chatId, msgId, autoChatProgressText(session), {
            parse_mode: "HTML",
            reply_markup: new InlineKeyboard()
              .text("🔄 Refresh", "auto_chat_refresh")
              .text("⏹️ Stop", "auto_chat_stop").row()
              .text("🏠 Main Menu", "main_menu"),
          });
        } catch {}

        if (!session.cancelled && delaySeconds > 0) {
          await new Promise(r => setTimeout(r, delaySeconds * 1000));
        }
      }

      if (!session.cancelled && round < maxRounds && delaySeconds > 0) {
        await new Promise(r => setTimeout(r, delaySeconds * 1000));
      }
    }
  } catch (err: any) {
    console.error(`[AUTO_CHAT][${userId}] Error:`, err?.message);
  }

  session.running = false;
  if (!session.cancelled) {
    try {
      await bot.api.editMessageText(chatId, msgId,
        `✅ <b>Auto Chat Complete!</b>\n\n📤 Sent: ${session.sent}\n❌ Failed: ${session.failed}`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
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

  kb.text("☑️ Sab Select", "cig_select_all").text("🧹 Clear", "cig_clear_all").row();
  if (selected.size > 0) {
    kb.text(`✅ Aage Badho (${selected.size} groups)`, "cig_proceed").row();
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
  };
  cigSessions.set(userId, session);

  for (let i = 0; i < groups.length; i++) {
    if (session.cancelled) break;
    const group = groups[i];
    const ok = await sendGroupMessage(waUserId, group.id, message);
    if (ok) session.sent++; else session.failed++;

    try {
      await bot.api.editMessageText(chatId, msgId,
        `📤 <b>Messages bhej raha hun...</b>\n\n` +
        `✅ Sent: ${session.sent}\n❌ Failed: ${session.failed}\n` +
        `📊 Progress: ${i + 1}/${groups.length}\n\n` +
        `⏳ Last: ${esc(group.subject)}`,
        {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard()
            .text("🔄 Refresh", "cig_refresh")
            .text("⏹️ Stop", "cig_stop_btn").row()
            .text("🏠 Main Menu", "main_menu"),
        }
      );
    } catch {}

    if (!session.cancelled && i < groups.length - 1 && delaySeconds > 0) {
      await new Promise(r => setTimeout(r, delaySeconds * 1000));
    }
  }

  session.running = false;
  if (!session.cancelled) {
    try {
      await bot.api.editMessageText(chatId, msgId,
        `✅ <b>Done!</b>\n\n📤 Sent: ${session.sent}\n❌ Failed: ${session.failed}\n📊 Total: ${groups.length} groups`,
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
      );
    } catch {}
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
      friendNumbers: [], adminContacts: [], navyContacts: [], memberContacts: [],
      totalToAdd: 0, mode: "", delaySeconds: 15, cancelled: false,
    },
  });
  await ctx.editMessageText(
    "➕ <b>Add Members to Group</b>\n\n" +
    "🔗 <b>Step 1:</b> Send the WhatsApp group link where you want to add members.\n\n" +
    "Example: <code>https://chat.whatsapp.com/ABC123xyz</code>",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

bot.callbackQuery("am_skip_friends", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  state.addMembersData.friendNumbers = [];
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
  await ctx.editMessageText(
    "🔢 <b>Step 6: Total Members to Add</b>\n\n" +
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

async function showAddMembersReview(ctx: any, userId: number) {
  const state = userStates.get(userId);
  if (!state?.addMembersData) return;
  const d = state.addMembersData;
  state.step = "add_members_confirm";
  const modeText = d.mode === "one_by_one" ? `1 by 1 (${d.delaySeconds}s delay)` : "All Together";
  const reviewText =
    "📋 <b>Add Members — Final Review</b>\n\n" +
    `🔗 Group: <b>${esc(d.groupName)}</b>\n` +
    `📋 Group ID: <code>${esc(d.groupId)}</code>\n\n` +
    `👫 Friends: ${d.friendNumbers.length}\n` +
    `👑 Admin VCF: ${d.adminContacts.length}\n` +
    `⚓ Navy VCF: ${d.navyContacts.length}\n` +
    `👥 Member VCF: ${d.memberContacts.length}\n` +
    `━━━━━━━━━━━━━━━━━━\n` +
    `🔢 Total to add: <b>${d.totalToAdd}</b>\n` +
    `⚙️ Mode: <b>${modeText}</b>\n\n` +
    `⚠️ Confirm karke Start karo:`;
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
  } else {
    await startAddMembersTogether(ctx, userId, chatId);
  }
});

bot.callbackQuery("am_cancel_adding", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "⛔ Adding stopped!" });
  addMembersCancelRequests.add(ctx.from.id);
});

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
        if (
          errMsg.includes("Already") ||
          errMsg.includes("Not on WhatsApp") ||
          errMsg.includes("Recently left")
        ) {
          skipped++;
          results.push(`⏭️ +${contact.phone} — ${errMsg}`);
        } else {
          added++;
          results.push(`✅ +${contact.phone} (${contact.category}) — Pending`);
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
          if (
            errMsg.includes("Already") ||
            errMsg.includes("Not on WhatsApp") ||
            errMsg.includes("Recently left")
          ) {
            skipped++;
            resultLines.push(`⏭️ +${r.phone} — ${errMsg} (${cat})`);
          } else {
            added++;
            resultLines.push(`✅ +${r.phone} (${cat}) — Pending`);
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
        await ctx.reply(
          `🔒 <b>Subscription Required!</b>\n\n👤 Contact owner: <b>${OWNER_USERNAME}</b>`,
          { parse_mode: "HTML" }
        );
        return;
      }
      await ctx.reply(
        mainMenuText(userId, "welcome"),
        { parse_mode: "HTML", reply_markup: mainMenu(userId) }
      );
      return;
    }
    await ctx.reply("💬 Use /start to begin.", { reply_markup: mainMenu(userId) });
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
          .text("✅ Shuru Karo!", "acig_confirm_start")
          .text("❌ Cancel", "main_menu"),
      }
    );
    return;
  }

  if (state.step === "awaiting_phone") {
    const phone = text.replace(/\s/g, "");
    if (!/^\+?\d{10,15}$/.test(phone)) {
      await ctx.reply("❌ Invalid phone number.\nExample: <code>+919942222222</code>", { parse_mode: "HTML" }); return;
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
          .text("✅ Haan, Bhejo!", "cig_start_confirm")
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
          .text("✅ Shuru Karo!", "auto_chat_confirm_start")
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
        try {
          await bot.api.editMessageText(joinChatId, joinMsgId,
            `⏳ <b>Joining: ${ji + 1}/${cleanLinks.length}</b>\n\n${results.join("\n")}`,
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "join_cancel_request") }
          );
        } catch {}
        if (ji < cleanLinks.length - 1) await new Promise((r) => setTimeout(r, 1500));
      }
      joinCancelRequests.delete(userId);
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
    for (const line of lines) {
      const cleaned = line.replace(/[^0-9+]/g, "");
      if (cleaned.length >= 7) excludeNumbers.add(cleaned);
    }

    if (excludeNumbers.size === 0) {
      await ctx.reply("❌ No valid numbers found. Please send numbers with country code like +919912345678\n\nOr tap Skip to not exclude anyone.",
        { reply_markup: new InlineKeyboard().text("⏭️ Skip", "rm_skip_exclude").text("❌ Cancel", "main_menu") }
      );
      return;
    }

    const excludeList = Array.from(excludeNumbers).map(n => `• ${esc(n)}`).join("\n");
    await ctx.reply(
      `✅ <b>${excludeNumbers.size} number(s) will be excluded:</b>\n\n${excludeList}\n\n⚠️ These numbers will NOT be removed from the groups.`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("✅ Confirm & Start", "rm_confirm_with_exclude").text("❌ Cancel", "main_menu") }
    );
    state.removeExcludeData.excludeNumbers = excludeNumbers;
    state.step = "remove_exclude_confirm";
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
    const link = cleanLinks[0];
    const statusMsg = await ctx.reply("⏳ Group info fetch kar raha hun...", { parse_mode: "HTML" });
    const groupInfo = await getGroupIdFromLink(String(userId), link);
    if (!groupInfo) {
      try { await ctx.api.deleteMessage(ctx.chat.id, statusMsg.message_id); } catch {}
      await ctx.reply(
        "❌ <b>Group info nahi mil paya!</b>\n\nCheck karein:\n• Link sahi hai\n• WhatsApp connected hai\n• Link expired nahi hai",
        { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🔄 Try Again", "add_members").text("🏠 Menu", "main_menu") }
      );
      return;
    }
    state.addMembersData.groupLink = link;
    state.addMembersData.groupId = groupInfo.id;
    state.addMembersData.groupName = groupInfo.subject;
    state.step = "add_members_friend_numbers";
    try { await ctx.api.deleteMessage(ctx.chat.id, statusMsg.message_id); } catch {}
    await ctx.reply(
      `✅ <b>Group found!</b>\n\n` +
      `📋 Name: <b>${esc(groupInfo.subject)}</b>\n` +
      `🔗 ID: <code>${esc(groupInfo.id)}</code>\n\n` +
      `👫 <b>Step 2: Friend Numbers</b>\n\n` +
      `Apne friend ke contact numbers bhejo (one per line)\n` +
      `Example:\n<code>919912345678\n919898765432</code>\n\n` +
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
    state.step = "add_members_admin_vcf";
    await ctx.reply(
      `✅ <b>${numbers.length} friend number(s) saved!</b>\n\n` +
      `👑 <b>Step 3: Admin VCF File</b>\n\n` +
      `📁 Send Admin VCF file (.vcf)\n\n` +
      `Agar admin ka VCF nahi hai to Skip karo.`,
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("⏭️ Skip", "am_skip_admin").text("❌ Cancel", "main_menu") }
    );
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
      `👥 <b>Add Together</b> — Sab ek saath add karega (fast)\n`,
      {
        parse_mode: "HTML",
        reply_markup: new InlineKeyboard()
          .text("👆 Add 1 by 1", "am_mode_one_by_one")
          .text("👥 Add Together", "am_mode_together").row()
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

  await startRemoveMembersProcess(ctx, userId, state.removeExcludeData.selectedGroups, state.removeExcludeData.excludeNumbers);
});

// ─── Photo Handler ───────────────────────────────────────────────────────────

bot.on("message:photo", async (ctx) => {
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state || state.step !== "group_dp" || !state.groupSettings) return;
  try {
    const photos = ctx.message.photo;
    const file = await ctx.api.getFile(photos[photos.length - 1].file_id);
    if (!file.file_path) { await ctx.reply("❌ Could not download photo. Try again."); return; }
    state.groupSettings.dpBuffer = await downloadBuffer(`https://api.telegram.org/file/bot${token}/${file.file_path}`);
    state.groupSettings.dpFileId = photos[photos.length - 1].file_id;
    await ctx.reply("✅ <b>Group DP saved!</b>", { parse_mode: "HTML" });
    await showGroupSummary(ctx);
  } catch (err: any) {
    await ctx.reply(`❌ Error: ${esc(err?.message || "Unknown error")}`, { parse_mode: "HTML" });
  }
});

// ─── Document Handler (VCF) ──────────────────────────────────────────────────

bot.on("message:document", async (ctx) => {
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state) return;
  const doc = ctx.message.document;
  if (!(doc.file_name || "").toLowerCase().endsWith(".vcf")) { await ctx.reply("❌ Please send a .vcf file only."); return; }

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

export function startBot() {
  if (!token) {
    console.log("[BOT] TELEGRAM_BOT_TOKEN not set — bot disabled. Set it to enable the Telegram bot.");
    return;
  }

  bot.catch((err) => {
    const e = err.error as any;
    const code = e?.error_code;
    const desc: string = e?.description || e?.message || String(e) || "";
    if (code === 400 && desc.includes("message is not modified")) return;
    console.error(`[BOT] Error in update ${err.ctx?.update?.update_id}: ${desc || err.message}`);
  });

  let retryCount = 0;
  const MAX_RETRIES = 5;

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
    } catch (err: any) {
      if (err?.error_code === 409) {
        retryCount++;
        if (retryCount > MAX_RETRIES) {
          console.error(`[BOT] 409 conflict — max retries (${MAX_RETRIES}) exceeded. Bot disabled.`);
          return;
        }
        const delay = Math.min(retryCount * 15, 60);
        console.log(`[BOT] 409 conflict — another instance running. Retry ${retryCount}/${MAX_RETRIES} in ${delay}s...`);
        setTimeout(() => launchBot(), delay * 1000);
        return;
      }
      if (err?.error_code === 401) {
        console.error("[BOT] Invalid TELEGRAM_BOT_TOKEN (401 Unauthorized). Bot disabled. Please set a valid token in environment variables.");
        return;
      }
      console.error("[BOT] Fatal error:", err?.message || err);
      console.error("[BOT] Bot disabled due to error. Server will continue running.");
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

export { bot };
