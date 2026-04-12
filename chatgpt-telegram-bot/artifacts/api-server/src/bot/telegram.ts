import { Bot, InlineKeyboard } from "grammy";
import {
  connectWhatsApp,
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
  requestGroupUnban,
} from "./whatsapp";
import { parseVCF, normalizePhone } from "./vcf-parser";
import https from "https";
import http from "http";
import fs from "fs";
import path from "path";

const token = process.env["TELEGRAM_BOT_TOKEN"] || "";

const ADMIN_USER_ID = Number(process.env["ADMIN_USER_ID"] || "0");
const FORCE_SUB_CHANNEL = process.env["FORCE_SUB_CHANNEL"] || "";
const OWNER_USERNAME = "@SPIDYWS";

const bot = new Bot(token || "placeholder");

function esc(text: string): string {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
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

const DATA_DIR = process.env["BOT_DATA_PATH"] || path.join(process.cwd(), "bot_data");

interface BotData {
  subscriptionMode: boolean;
  accessList: Record<string, { expiresAt: number; grantedBy: number }>;
  bannedUsers: number[];
  totalUsers: number[];
}

function ensureDataDir(): void {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
}

function loadBotData(): BotData {
  ensureDataDir();
  const filePath = path.join(DATA_DIR, "bot_data.json");
  if (fs.existsSync(filePath)) {
    try { return JSON.parse(fs.readFileSync(filePath, "utf-8")); } catch {}
  }
  return { subscriptionMode: false, accessList: {}, bannedUsers: [], totalUsers: [] };
}

function saveBotData(data: BotData): void {
  ensureDataDir();
  fs.writeFileSync(path.join(DATA_DIR, "bot_data.json"), JSON.stringify(data, null, 2));
}

function trackUser(userId: number): void {
  const data = loadBotData();
  if (!data.totalUsers.includes(userId)) {
    data.totalUsers.push(userId);
    saveBotData(data);
  }
}

function isAdmin(userId: number): boolean {
  return userId === ADMIN_USER_ID;
}

function isBanned(userId: number): boolean {
  return loadBotData().bannedUsers.includes(userId);
}

function hasAccess(userId: number): boolean {
  if (isAdmin(userId)) return true;
  const data = loadBotData();
  if (!data.subscriptionMode) return true;
  const access = data.accessList[String(userId)];
  if (!access) return false;
  if (Date.now() > access.expiresAt) {
    delete data.accessList[String(userId)];
    saveBotData(data);
    return false;
  }
  return true;
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
  };
  makeAdminData?: {
    allGroups: Array<{ id: string; subject: string }>;
    patterns: SimilarGroup[];
    selectedIndices: Set<number>;
    page: number;
  };
  approvalData?: {
    allGroups: Array<{ id: string; subject: string }>;
    patterns: SimilarGroup[];
    selectedIndices: Set<number>;
    page: number;
  };
  unbanData?: {
    bannedGroups: Array<{ id: string; subject: string }>;
    patterns: SimilarGroup[];
    selectedIndices: Set<number>;
    page: number;
    cancelFlag?: boolean;
  };
  joinCancelFlag?: boolean;
}

const userStates: Map<number, UserState> = new Map();

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
    .text("📋 Get Pending List", "pending_list").text("🔓 Unban Groups", "unban_groups").row()
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
      const connected = isConnected(String(userId));
      await ctx.editMessageText(
        "🤖 <b>WhatsApp Bot Manager</b>\n\n" +
          "✅ Joined! Choose an option below:",
        { parse_mode: "HTML", reply_markup: mainMenu(userId) }
      );
      return;
    }
  } catch {}
  await ctx.answerCallbackQuery({ text: "❌ You haven't joined the channel yet!", show_alert: true });
});

bot.command("start", async (ctx) => {
  const userId = ctx.from!.id;
  trackUser(userId);
  if (isBanned(userId)) {
    await ctx.reply("🚫 You are banned from using this bot.");
    return;
  }
  if (!(await checkForceSub(ctx))) return;
  if (!hasAccess(userId)) {
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
  const connected = isConnected(String(userId));
  await ctx.reply(
    "🤖 <b>WhatsApp Bot Manager</b>\n\n" +
      "👋 Welcome! Choose an option below:\n\n" +
      (connected ? "✅ WhatsApp Connected\n" : "📱 Connect your WhatsApp first\n"),
    { parse_mode: "HTML", reply_markup: mainMenu(userId) }
  );
});

bot.command("help", async (ctx) => {
  const userId = ctx.from!.id;
  trackUser(userId);
  if (isBanned(userId)) return;

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
  if (isBanned(userId)) {
    try { await ctx.answerCallbackQuery({ text: "🚫 You are banned from this bot.", show_alert: true }); } catch {
      await ctx.reply("🚫 You are banned from using this bot.");
    }
    return false;
  }
  if (!(await checkForceSub(ctx))) return false;
  if (!hasAccess(userId)) {
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
    "🤖 <b>WhatsApp Bot Manager</b>\n\nChoose an option below:",
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

  if (!list.length) {
    await ctx.editMessageText(
      "📋 <b>Pending List</b>\n\nNo admin groups found.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
    return;
  }

  // Sort by pending count descending
  list.sort((a, b) => b.pendingCount - a.pendingCount);

  // Detect similar patterns from admin group names
  const groupsForPattern = list.map((g) => ({ id: g.groupId, subject: g.groupName }));
  const patterns = detectSimilarGroups(groupsForPattern);

  userStates.set(userId, {
    step: "pending_list_menu",
    pendingListData: { patterns, allPending: list },
  });

  const kb = new InlineKeyboard();
  if (patterns.length > 0) kb.text("🔍 Similar Groups", "pl_similar").text("📋 All Groups", "pl_all").row();
  else kb.text("📋 All Groups", "pl_all").row();
  kb.text("🏠 Main Menu", "main_menu");

  await ctx.editMessageText(
    `📋 <b>Pending List</b>\n\n` +
    `📊 Admin Groups: ${list.length}\n` +
    `⏳ Total Pending: ${list.reduce((s, g) => s + g.pendingCount, 0)}\n` +
    (patterns.length > 0 ? `🔍 Similar Patterns: ${patterns.length}\n` : "") +
    `\n📌 Choose an option:`,
    { parse_mode: "HTML", reply_markup: kb }
  );
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

  let text = `📌 <b>"${esc(pattern.base)}" — ${pattern.groups.length} groups</b>\n\n`;
  for (const g of pattern.groups) {
    const found = state.pendingListData.allPending.find((ap) => ap.groupId === g.id);
    const count = found?.pendingCount ?? 0;
    text += `${esc(g.subject)} ✅ ${count}\n`;
  }

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
  let text = `📋 <b>All Admin Groups — Pending List</b>\n📊 Total: ${allPending.length} groups\n\n`;
  for (const g of allPending) {
    text += `${esc(g.groupName)} ✅ ${g.pendingCount}\n`;
  }

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
    "📊 <code>/status</code> — View bot statistics",
    { parse_mode: "HTML" }
  );
});

bot.command("access", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const args = (ctx.message?.text || "").split(/\s+/).slice(1);
  if (!args.length) { await ctx.reply("❓ Usage:\n/access on\n/access off\n/access [user_id] [days]"); return; }

  if (args[0] === "on") {
    const data = loadBotData(); data.subscriptionMode = true; saveBotData(data);
    await ctx.reply(`🔒 <b>Subscription Mode: ON</b>\n\nOnly users with access can use the bot.\n👤 Owner: <b>${OWNER_USERNAME}</b>`, { parse_mode: "HTML" });
    return;
  }
  if (args[0] === "off") {
    const data = loadBotData(); data.subscriptionMode = false; saveBotData(data);
    await ctx.reply("🔓 <b>Subscription Mode: OFF</b>\n\nAll users can use the bot for free.", { parse_mode: "HTML" });
    return;
  }
  if (args.length >= 2) {
    const targetId = parseInt(args[0]), days = parseInt(args[1]);
    if (isNaN(targetId) || isNaN(days) || days <= 0) { await ctx.reply("❓ Example: /access 123456789 30"); return; }
    const data = loadBotData();
    data.accessList[String(targetId)] = { expiresAt: Date.now() + days * 86400000, grantedBy: ctx.from!.id };
    saveBotData(data);
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
  const data = loadBotData();
  if (data.accessList[String(id)]) { delete data.accessList[String(id)]; saveBotData(data); await ctx.reply(`❌ <b>Access Revoked!</b>\n\n👤 User: <code>${id}</code>`, { parse_mode: "HTML" }); }
  else await ctx.reply("⚠️ User does not have access.");
});

bot.command("ban", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const id = parseInt((ctx.message?.text || "").split(/\s+/)[1]);
  if (isNaN(id)) { await ctx.reply("❓ Usage: /ban [user_id]"); return; }
  const data = loadBotData();
  if (!data.bannedUsers.includes(id)) { data.bannedUsers.push(id); saveBotData(data); }
  await ctx.reply(`🚫 <b>User Banned!</b>\n\n👤 User: <code>${id}</code>`, { parse_mode: "HTML" });
});

bot.command("unban", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const id = parseInt((ctx.message?.text || "").split(/\s+/)[1]);
  if (isNaN(id)) { await ctx.reply("❓ Usage: /unban [user_id]"); return; }
  const data = loadBotData();
  data.bannedUsers = data.bannedUsers.filter((u) => u !== id);
  saveBotData(data);
  await ctx.reply(`✅ <b>User Unbanned!</b>\n\n👤 User: <code>${id}</code>`, { parse_mode: "HTML" });
});

bot.command("status", async (ctx) => {
  if (!isAdmin(ctx.from!.id)) { await ctx.reply("🚫 You are not an admin."); return; }
  const data = loadBotData();
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

// ─── Connect WhatsApp ────────────────────────────────────────────────────────

bot.callbackQuery("connect_wa", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (isConnected(String(userId))) {
    await ctx.editMessageText(
      "✅ <b>WhatsApp already connected!</b>\n\nYou can use all features.",
      { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    );
    return;
  }
  userStates.set(userId, { step: "awaiting_phone" });
  await ctx.editMessageText(
    "📱 <b>Connect WhatsApp</b>\n\nEnter your phone number with country code:\n\nExample: <code>+919942222222</code>",
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
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
    { parse_mode: "HTML" }
  );

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
    { parse_mode: "HTML" }
  );

  void fetchGroupLinksBackground(String(userId), allGroups, chatId, msgId, "all");
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
        { parse_mode: "HTML" }
      );
    } catch {}
  };

  for (let i = 0; i < groups.length; i += GL_BATCH_SIZE) {
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

  let result: string;
  if (mode === "similar") {
    result = `🔗 <b>"${esc(patternBase!)}" Pattern</b>\n`;
    result += `📊 <b>Total: ${groups.length} groups | ✅ ${successCount} links fetched</b>\n\n`;
  } else {
    result = `📋 <b>All Group Links</b>\n📊 <b>Total: ${groups.length} groups | ✅ ${successCount} links fetched</b>\n\n`;
  }

  for (const r of results) {
    result += r.link ? `📌 ${esc(r.subject)}\n${r.link}\n\n` : `📌 ${esc(r.subject)}\n⚠️ Could not get link\n\n`;
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

const RM_PAGE_SIZE = 8;

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

const MA_PAGE_SIZE = 8;

function buildMakeAdminKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const allGroups = state.makeAdminData!.allGroups;
  const selected = state.makeAdminData!.selectedIndices;
  const page = state.makeAdminData!.page || 0;
  const totalPages = Math.ceil(allGroups.length / MA_PAGE_SIZE);
  const start = page * MA_PAGE_SIZE;
  const end = Math.min(start + MA_PAGE_SIZE, allGroups.length);

  for (let i = start; i < end; i++) {
    const g = allGroups[i];
    const isSelected = selected.has(i);
    const label = isSelected ? `✅ ${g.subject}` : `☐ ${g.subject}`;
    kb.text(label, `ma_tog_${i}`).row();
  }

  if (totalPages > 1) {
    if (page > 0) kb.text("⬅️ Previous", "ma_page_prev");
    kb.text(`📄 ${page + 1}/${totalPages}`, "ma_page_info");
    if (page < totalPages - 1) kb.text("➡️ Next", "ma_page_next");
    kb.row();
  }

  if (allGroups.length > 1) {
    kb.text("☑️ Select All", "ma_select_all").row();
  }

  if (selected.size > 0) {
    kb.text(`▶️ Continue (${selected.size} selected)`, "ma_proceed").row();
  }

  kb.text("🏠 Back", "main_menu");
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

bot.callbackQuery("ma_page_prev", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.makeAdminData) return;
  if (state.makeAdminData.page > 0) state.makeAdminData.page--;
  const selectedCount = state.makeAdminData.selectedIndices.size;
  await ctx.editMessageText(
    `👑 <b>Make Admin</b>\n\n👑 <b>${state.makeAdminData.allGroups.length} admin group(s)</b>\n\nSelect group(s):\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildMakeAdminKeyboard(state) }
  );
});

bot.callbackQuery("ma_page_next", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.makeAdminData) return;
  const totalPages = Math.ceil(state.makeAdminData.allGroups.length / MA_PAGE_SIZE);
  if (state.makeAdminData.page < totalPages - 1) state.makeAdminData.page++;
  const selectedCount = state.makeAdminData.selectedIndices.size;
  await ctx.editMessageText(
    `👑 <b>Make Admin</b>\n\n👑 <b>${state.makeAdminData.allGroups.length} admin group(s)</b>\n\nSelect group(s):\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildMakeAdminKeyboard(state) }
  );
});

bot.callbackQuery("ma_page_info", async (ctx) => { await ctx.answerCallbackQuery(); });

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

bot.callbackQuery("ma_proceed", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.makeAdminData || state.makeAdminData.selectedIndices.size === 0) return;

  state.step = "make_admin_enter_numbers";
  const selectedGroups = Array.from(state.makeAdminData.selectedIndices).map(i => state.makeAdminData!.allGroups[i]);
  const groupList = selectedGroups.map(g => `• ${esc(g.subject)}`).join("\n");

  await ctx.editMessageText(
    `✅ <b>${selectedGroups.length} group(s) selected:</b>\n\n${groupList}\n\n` +
    `📱 <b>Send phone number(s)</b>\n\n` +
    `Send the phone numbers (with country code) of people you want to make admin, one per line:\n\n` +
    `Example:\n<code>+919912345678\n+919998887777</code>`,
    { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("❌ Cancel", "main_menu") }
  );
});

// ─── Approval ────────────────────────────────────────────────────────────────

const AP_PAGE_SIZE = 8;

function buildApprovalKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const allGroups = state.approvalData!.allGroups;
  const selected = state.approvalData!.selectedIndices;
  const page = state.approvalData!.page || 0;
  const totalPages = Math.ceil(allGroups.length / AP_PAGE_SIZE);
  const start = page * AP_PAGE_SIZE;
  const end = Math.min(start + AP_PAGE_SIZE, allGroups.length);

  for (let i = start; i < end; i++) {
    const g = allGroups[i];
    const isSelected = selected.has(i);
    const label = isSelected ? `✅ ${g.subject}` : `☐ ${g.subject}`;
    kb.text(label, `ap_tog_${i}`).row();
  }

  if (totalPages > 1) {
    if (page > 0) kb.text("⬅️ Previous", "ap_page_prev");
    kb.text(`📄 ${page + 1}/${totalPages}`, "ap_page_info");
    if (page < totalPages - 1) kb.text("➡️ Next", "ap_page_next");
    kb.row();
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

bot.callbackQuery("ap_page_prev", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData) return;
  if (state.approvalData.page > 0) state.approvalData.page--;
  const selectedCount = state.approvalData.selectedIndices.size;
  await ctx.editMessageText(
    `✅ <b>Approval</b>\n\n👑 <b>${state.approvalData.allGroups.length} admin group(s)</b>\n\nSelect group(s):\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildApprovalKeyboard(state) }
  );
});

bot.callbackQuery("ap_page_next", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.approvalData) return;
  const totalPages = Math.ceil(state.approvalData.allGroups.length / AP_PAGE_SIZE);
  if (state.approvalData.page < totalPages - 1) state.approvalData.page++;
  const selectedCount = state.approvalData.selectedIndices.size;
  await ctx.editMessageText(
    `✅ <b>Approval</b>\n\n👑 <b>${state.approvalData.allGroups.length} admin group(s)</b>\n\nSelect group(s):\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildApprovalKeyboard(state) }
  );
});

bot.callbackQuery("ap_page_info", async (ctx) => { await ctx.answerCallbackQuery(); });

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

// ─── Unban Groups ────────────────────────────────────────────────────────────

function buildUnbanPrompt(groupName: string, groupId: string): string {
  return `Hello WhatsApp Support Team,

My WhatsApp group "${groupName}" has been banned/restricted, and I believe this may have happened by mistake.

Group name: ${groupName}
Group ID: ${groupId}

This group is very important to me as it includes my family, school, and other personal communication. Losing access has affected my daily communication and important connections.

I always try to follow WhatsApp's policies. If anything was done unintentionally that violated the rules, I sincerely apologize for it.

I kindly request you to please review and restore my group "${groupName}" as soon as possible.

This group means a lot to me, and I would be very grateful for your help.

Thank you for your support.`;
}

const UNBAN_PAGE_SIZE = 8;

function buildUnbanGroupsKeyboard(state: UserState): InlineKeyboard {
  const kb = new InlineKeyboard();
  const bannedGroups = state.unbanData!.bannedGroups;
  const selected = state.unbanData!.selectedIndices;
  const page = state.unbanData!.page || 0;
  const totalPages = Math.ceil(bannedGroups.length / UNBAN_PAGE_SIZE);
  const start = page * UNBAN_PAGE_SIZE;
  const end = Math.min(start + UNBAN_PAGE_SIZE, bannedGroups.length);

  for (let i = start; i < end; i++) {
    const g = bannedGroups[i];
    const isSelected = selected.has(i);
    const label = isSelected ? `✅ ${g.subject}` : `☐ ${g.subject}`;
    kb.text(label, `ub_tog_${i}`).row();
  }

  if (totalPages > 1) {
    if (page > 0) kb.text("⬅️ Prev", "ub_page_prev");
    kb.text(`📄 ${page + 1}/${totalPages}`, "ub_page_info");
    if (page < totalPages - 1) kb.text("➡️ Next", "ub_page_next");
    kb.row();
  }

  if (bannedGroups.length > 1) {
    if (state.unbanData!.patterns.length > 0) {
      kb.text("🔍 Similar Groups", "ub_similar").row();
    }
    kb.text("☑️ Select All", "ub_select_all").row();
  }

  if (selected.size > 0) {
    kb.text(`▶️ Continue (${selected.size} selected)`, "ub_proceed").row();
  }

  kb.text("🏠 Back", "main_menu");
  return kb;
}

bot.callbackQuery("unban_groups", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  if (!(await checkAccessMiddleware(ctx))) return;
  if (!isConnected(String(userId))) {
    await ctx.editMessageText("❌ <b>WhatsApp not connected!</b>", {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("📱 Connect", "connect_wa").text("🏠 Menu", "main_menu"),
    }); return;
  }

  await ctx.editMessageText("🔍 <b>Scanning your admin groups for banned ones...</b>\n\n⌛ Please wait...", { parse_mode: "HTML" });

  const allGroups = await getAllGroups(String(userId));
  const adminGroups = allGroups.filter((g) => g.isAdmin);

  // Detect banned groups: groups where user is admin but has 0 or very few participants
  // and the group appears to be restricted (can't send messages, etc.)
  // We detect them by checking groupMetadata — banned groups show specific characteristics
  const session = (await import("./whatsapp")).getSession(String(userId));
  const bannedGroups: Array<{ id: string; subject: string }> = [];

  if (session?.socket && session.connected) {
    for (const g of adminGroups) {
      try {
        const meta = await session.socket.groupMetadata(g.id);
        // A group is considered banned if it has restrict set without announcement mode
        // OR if we can detect the "This group is no longer available" state
        // Baileys marks banned groups with announce mode + specific restriction fields
        const isBanned = (meta as any).isCommunityAnnounce === false &&
          ((meta as any).restrict === true || (meta as any).noFrequentlyForwarded === true);

        // Also check via groupFetchAllParticipating data
        const isLikelyBanned = adminGroups.find(ag => ag.id === g.id && ag.participantCount <= 1);

        if (isBanned || isLikelyBanned) {
          bannedGroups.push({ id: g.id, subject: g.subject });
        }
      } catch (err: any) {
        const msg = (err?.message || "").toLowerCase();
        // If we get forbidden/not-authorized errors when trying to access the group,
        // it's likely banned
        if (msg.includes("not-authorized") || msg.includes("forbidden") || msg.includes("not-participant")) {
          bannedGroups.push({ id: g.id, subject: g.subject });
        }
      }
    }
  }

  // If no banned groups detected through metadata, show all admin groups
  // so user can manually select which ones are banned
  const groupsToShow = bannedGroups.length > 0 ? bannedGroups : adminGroups.map(g => ({ id: g.id, subject: g.subject }));
  const isFallback = bannedGroups.length === 0;

  if (!groupsToShow.length) {
    await ctx.editMessageText(
      "📭 No admin groups found on your WhatsApp.",
      { reply_markup: new InlineKeyboard().text("🏠 Main Menu", "main_menu") }
    ); return;
  }

  const patterns = detectSimilarGroups(groupsToShow);

  userStates.set(userId, {
    step: "unban_select",
    unbanData: {
      bannedGroups: groupsToShow,
      patterns,
      selectedIndices: new Set(),
      page: 0,
    },
  });

  const state = userStates.get(userId)!;
  await ctx.editMessageText(
    `🔓 <b>Unban Groups</b>\n\n` +
    (isFallback
      ? `⚠️ <b>Could not auto-detect banned groups.</b>\nShowing all ${groupsToShow.length} admin group(s).\nManually select the banned ones:\n`
      : `🚫 <b>${groupsToShow.length} possibly banned group(s) found!</b>\n`) +
    `\nSelect the group(s) you want to send an unban request for:\n<i>Tap to select/deselect</i>`,
    { parse_mode: "HTML", reply_markup: buildUnbanGroupsKeyboard(state) }
  );
});

bot.callbackQuery(/^ub_tog_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.unbanData) return;

  const idx = parseInt(ctx.match![1]);
  if (idx < 0 || idx >= state.unbanData.bannedGroups.length) return;

  if (state.unbanData.selectedIndices.has(idx)) {
    state.unbanData.selectedIndices.delete(idx);
  } else {
    state.unbanData.selectedIndices.add(idx);
  }

  const selectedCount = state.unbanData.selectedIndices.size;
  await ctx.editMessageText(
    `🔓 <b>Unban Groups</b>\n\n📊 <b>${state.unbanData.bannedGroups.length} group(s)</b>\n\nSelect group(s) to send unban request:\n<i>${selectedCount > 0 ? `${selectedCount} selected` : "None selected yet"}</i>`,
    { parse_mode: "HTML", reply_markup: buildUnbanGroupsKeyboard(state) }
  );
});

bot.callbackQuery("ub_page_prev", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.unbanData) return;
  if (state.unbanData.page > 0) state.unbanData.page--;
  await ctx.editMessageText(
    `🔓 <b>Unban Groups</b>\n\n📊 <b>${state.unbanData.bannedGroups.length} group(s)</b>\n\nSelect group(s) to send unban request:`,
    { parse_mode: "HTML", reply_markup: buildUnbanGroupsKeyboard(state) }
  );
});

bot.callbackQuery("ub_page_next", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.unbanData) return;
  const totalPages = Math.ceil(state.unbanData.bannedGroups.length / UNBAN_PAGE_SIZE);
  if (state.unbanData.page < totalPages - 1) state.unbanData.page++;
  await ctx.editMessageText(
    `🔓 <b>Unban Groups</b>\n\n📊 <b>${state.unbanData.bannedGroups.length} group(s)</b>\n\nSelect group(s) to send unban request:`,
    { parse_mode: "HTML", reply_markup: buildUnbanGroupsKeyboard(state) }
  );
});

bot.callbackQuery("ub_page_info", async (ctx) => { await ctx.answerCallbackQuery(); });

bot.callbackQuery("ub_select_all", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.unbanData) return;

  for (let i = 0; i < state.unbanData.bannedGroups.length; i++) {
    state.unbanData.selectedIndices.add(i);
  }

  await ctx.editMessageText(
    `🔓 <b>Unban Groups</b>\n\nAll <b>${state.unbanData.bannedGroups.length} groups selected</b>`,
    { parse_mode: "HTML", reply_markup: buildUnbanGroupsKeyboard(state) }
  );
});

bot.callbackQuery("ub_similar", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.unbanData) return;

  const { patterns } = state.unbanData;
  if (!patterns.length) {
    await ctx.editMessageText("⚠️ No similar group patterns found.", {
      reply_markup: new InlineKeyboard().text("🔙 Back", "unban_groups").text("🏠 Menu", "main_menu"),
    }); return;
  }

  const kb = new InlineKeyboard();
  for (let i = 0; i < patterns.length; i++) {
    const p = patterns[i];
    kb.text(`📌 ${p.base} (${p.groups.length} groups)`, `ub_sim_${i}`).row();
  }
  kb.text("🔙 Back", "unban_groups").text("🏠 Menu", "main_menu");

  await ctx.editMessageText(
    "🔍 <b>Similar Group Patterns</b>\n\nTap a pattern to select all those groups:",
    { parse_mode: "HTML", reply_markup: kb }
  );
});

bot.callbackQuery(/^ub_sim_(\d+)$/, async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.unbanData) return;

  const idx = parseInt(ctx.match![1]);
  const pattern = state.unbanData.patterns[idx];
  if (!pattern) return;

  const patternIds = new Set(pattern.groups.map(g => g.id));
  for (let i = 0; i < state.unbanData.bannedGroups.length; i++) {
    if (patternIds.has(state.unbanData.bannedGroups[i].id)) {
      state.unbanData.selectedIndices.add(i);
    }
  }

  state.step = "unban_select";
  await ctx.editMessageText(
    `🔓 <b>Unban Groups</b>\n\n📊 <b>${state.unbanData.bannedGroups.length} group(s)</b>\n\n<b>${state.unbanData.selectedIndices.size} selected</b>`,
    { parse_mode: "HTML", reply_markup: buildUnbanGroupsKeyboard(state) }
  );
});

bot.callbackQuery("ub_proceed", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (!state?.unbanData || state.unbanData.selectedIndices.size === 0) return;

  const selectedGroups = Array.from(state.unbanData.selectedIndices).map(i => state.unbanData!.bannedGroups[i]);
  const groupList = selectedGroups.map(g => `• ${esc(g.subject)}`).join("\n");
  const chatId = ctx.callbackQuery.message?.chat.id;
  const msgId = ctx.callbackQuery.message?.message_id;
  if (!chatId || !msgId) return;

  state.unbanData.cancelFlag = false;

  await ctx.editMessageText(
    `🔓 <b>Unban Process Started</b>\n\n` +
    `📋 <b>Groups (${selectedGroups.length}):</b>\n${groupList}\n\n` +
    `⏳ <b>0/${selectedGroups.length} done</b>\n\n` +
    `<i>Bot is sending unban request to WhatsApp Support for each group...</i>`,
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("🛑 Cancel", "ub_cancel"),
    }
  );

  void unbanGroupsBackground(String(userId), selectedGroups, chatId, msgId, userId);
});

bot.callbackQuery("ub_cancel", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "⚠️ Are you sure?" });
  const userId = ctx.from.id;
  await ctx.editMessageText(
    "⚠️ <b>Cancel Unban Process?</b>\n\nAre you sure you want to stop the unban requests?",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Cancel", "ub_cancel_confirm")
        .text("▶️ Continue", "ub_cancel_dismiss"),
    }
  );
});

bot.callbackQuery("ub_cancel_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (state?.unbanData) {
    state.unbanData.cancelFlag = true;
  }
  await ctx.editMessageText(
    "🛑 <b>Cancelling unban process...</b>\n\nWaiting for current request to complete.",
    { parse_mode: "HTML" }
  );
});

bot.callbackQuery("ub_cancel_dismiss", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "▶️ Continuing..." });
});

async function unbanGroupsBackground(
  userId: string,
  groups: Array<{ id: string; subject: string }>,
  chatId: number,
  msgId: number,
  telegramUserId: number
) {
  const lines: string[] = [];
  let doneCount = 0;

  for (let gi = 0; gi < groups.length; gi++) {
    const currentState = userStates.get(telegramUserId);
    if (currentState?.unbanData?.cancelFlag) {
      lines.push(`\n🛑 <b>Process cancelled after ${gi} group(s)</b>`);
      break;
    }

    const group = groups[gi];

    try {
      await bot.api.editMessageText(chatId, msgId,
        `🔓 <b>Unban Process</b>\n\n` +
        `⏳ <b>Group ${gi + 1}/${groups.length}: ${esc(group.subject)}</b>\n\n` +
        `🔄 Contacting WhatsApp Support...\n\n` +
        `${lines.join("\n")}`,
        {
          parse_mode: "HTML",
          reply_markup: new InlineKeyboard().text("🛑 Cancel", "ub_cancel"),
        }
      );
    } catch {}

    let statusMsg = "";

    try {
      const prompt = buildUnbanPrompt(group.subject, group.id);
      const result = await requestGroupUnban(userId, group.id, prompt);
      if (result.reviewSent && result.supportMessageSent) {
        statusMsg = `✅ Review request + WhatsApp Support message sent\n📩 Support chat: <code>${esc(result.supportJid || "WhatsApp Support")}</code>`;
      } else if (result.supportMessageSent) {
        statusMsg = `✅ WhatsApp Support chat started and message sent\n📩 Support chat: <code>${esc(result.supportJid || "WhatsApp Support")}</code>`;
      } else if (result.reviewSent) {
        statusMsg = "✅ Review request sent through WhatsApp";
      } else {
        statusMsg = `❌ Failed: ${esc(result.error || "Unknown")}`;
      }
    } catch (err: any) {
      statusMsg = `❌ Error: ${esc(err?.message || "Unknown")}`;
    }

    lines.push(`📋 <b>${esc(group.subject)}</b>\n${statusMsg}`);
    doneCount++;

    try {
      await bot.api.editMessageText(chatId, msgId,
        `🔓 <b>Unban Process</b>\n\n` +
        `⏳ <b>${doneCount}/${groups.length} done</b>\n\n` +
        `${lines.join("\n\n")}`,
        {
          parse_mode: "HTML",
          reply_markup: doneCount < groups.length
            ? new InlineKeyboard().text("🛑 Cancel", "ub_cancel")
            : new InlineKeyboard().text("🏠 Main Menu", "main_menu"),
        }
      );
    } catch {}

    // Wait between requests to avoid rate limiting
    if (gi < groups.length - 1) {
      await new Promise((r) => setTimeout(r, 3000));
    }
  }

  const finalText =
    `🔓 <b>Unban Process Complete</b>\n\n` +
    `📊 <b>Processed: ${doneCount}/${groups.length} groups</b>\n\n` +
    `${lines.join("\n\n")}`;

  const chunks = splitMessage(finalText, 4000);
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

  userStates.delete(telegramUserId);
}

// ─── Join Cancel ─────────────────────────────────────────────────────────────

bot.callbackQuery("join_cancel", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "⚠️ Are you sure you want to cancel joining?", show_alert: false });
  const userId = ctx.from.id;
  await ctx.editMessageText(
    "⚠️ <b>Cancel Join Process?</b>\n\nAre you sure you want to stop joining groups?",
    {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard()
        .text("✅ Yes, Cancel", "join_cancel_confirm")
        .text("▶️ Continue", "join_cancel_dismiss"),
    }
  );
});

bot.callbackQuery("join_cancel_confirm", async (ctx) => {
  await ctx.answerCallbackQuery();
  const userId = ctx.from.id;
  const state = userStates.get(userId);
  if (state) {
    state.joinCancelFlag = true;
  }
  await ctx.editMessageText(
    "🛑 <b>Cancelling...</b>\n\nWaiting for current join to finish, then stopping.",
    { parse_mode: "HTML" }
  );
});

bot.callbackQuery("join_cancel_dismiss", async (ctx) => {
  await ctx.answerCallbackQuery({ text: "▶️ Continuing..." });
});

// ─── Text Handler ─────────────────────────────────────────────────────────────

bot.on("message:text", async (ctx) => {
  const userId = ctx.from.id;
  trackUser(userId);
  if (isBanned(userId)) return;
  const state = userStates.get(userId);
  const text = ctx.message.text.trim();

  if (!state) { await ctx.reply("💬 Use /start to begin.", { reply_markup: mainMenu(userId) }); return; }

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
            "✅ <b>WhatsApp Connected!</b>\n\n🎉 All features are now available.",
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
    const joinState: UserState = { step: "join_running", joinCancelFlag: false };
    userStates.set(userId, joinState);
    const statusMsg = await ctx.reply(`⏳ <b>Joining ${cleanLinks.length} group(s)...</b>\n\n🔄 0/${cleanLinks.length} done...`, {
      parse_mode: "HTML",
      reply_markup: new InlineKeyboard().text("🛑 Cancel", "join_cancel"),
    });
    const joinChatId = ctx.chat.id;
    const joinMsgId = statusMsg.message_id;
    void (async () => {
      let result = "🔗 <b>Join Groups Result</b>\n\n";
      const results: string[] = [];
      for (let ji = 0; ji < cleanLinks.length; ji++) {
        const currentState = userStates.get(userId);
        if (currentState?.joinCancelFlag) {
          results.push(`\n🛑 <b>Process cancelled by user after ${ji} group(s)</b>`);
          break;
        }
        const res = await joinGroupWithLink(String(userId), cleanLinks[ji]);
        const line = res.success ? `✅ Joined: ${esc(res.groupName || "Group")}` : `❌ Failed: ${esc(res.error || "Unknown")}`;
        results.push(line);
        try {
          await bot.api.editMessageText(joinChatId, joinMsgId,
            `⏳ <b>Joining: ${ji + 1}/${cleanLinks.length}</b>\n\n${results.join("\n")}`,
            { parse_mode: "HTML", reply_markup: new InlineKeyboard().text("🛑 Cancel", "join_cancel") }
          );
        } catch {}
        if (ji < cleanLinks.length - 1) await new Promise((r) => setTimeout(r, 1500));
      }
      result += results.join("\n");
      userStates.delete(userId);
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
  if (!state || state.step !== "ctc_enter_vcf" || !state.ctcData) return;
  const doc = ctx.message.document;
  if (!(doc.file_name || "").toLowerCase().endsWith(".vcf")) { await ctx.reply("❌ Please send a .vcf file only."); return; }

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
          console.error(`[BOT] 409 conflict — max retries (${MAX_RETRIES}) exceeded. Exiting...`);
          process.exit(1);
        }
        const delay = Math.min(retryCount * 15, 60);
        console.log(`[BOT] 409 conflict — another instance running. Retry ${retryCount}/${MAX_RETRIES} in ${delay}s...`);
        setTimeout(() => launchBot(), delay * 1000);
        return;
      }
      console.error("[BOT] Fatal error:", err?.message || err);
      process.exit(1);
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
