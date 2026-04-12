import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  Browsers,
} from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import pino from "pino";
import path from "path";
import fs from "fs";

const logger = pino({ level: "silent" });

let socketGenCounter = 0;

interface WhatsAppSession {
  socket: ReturnType<typeof makeWASocket> | null;
  connected: boolean;
  pairingCode: string | null;
  connecting: boolean;
  phoneNumber: string;
  codeRequested: boolean;
  wasConnected: boolean;
  retryCount: number;
  socketGenId: number;
  connectLock: boolean;
}

const sessions: Map<string, WhatsAppSession> = new Map();

function getSessionPath(userId: string): string {
  const base = process.env["WA_SESSION_PATH"] || path.join(process.cwd(), "wa_sessions");
  return path.join(base, userId);
}

function clearSessionFiles(userId: string): void {
  const sessionPath = getSessionPath(userId);
  if (fs.existsSync(sessionPath)) {
    fs.rmSync(sessionPath, { recursive: true, force: true });
    console.log(`[WA][${userId}] Cleared session files`);
  }
}

function ensureSessionDir(userId: string): string {
  const sessionPath = getSessionPath(userId);
  if (!fs.existsSync(sessionPath)) {
    fs.mkdirSync(sessionPath, { recursive: true });
  }
  return sessionPath;
}

function closeSocketSafe(sock: ReturnType<typeof makeWASocket> | null): void {
  if (!sock) return;
  try { sock.end(undefined); } catch {}
}

async function createSocket(
  userId: string,
  phoneNumber: string,
  onCode: (code: string) => void,
  onConnected: () => void,
  onDisconnected: (reason: string) => void,
  session: WhatsAppSession
): Promise<void> {
  if (session.socket) {
    closeSocketSafe(session.socket);
    session.socket = null;
  }

  const myGenId = ++socketGenCounter;
  session.socketGenId = myGenId;
  session.codeRequested = false;

  const sessionPath = ensureSessionDir(userId);
  let { state, saveCreds } = await useMultiFileAuthState(sessionPath);

  // Stale registered session without ever connecting → clear and restart fresh
  if (state.creds.registered && session.retryCount >= 3 && !session.wasConnected) {
    console.log(`[WA][${userId}] gen=${myGenId} Stale registered session after ${session.retryCount} fails — clearing`);
    clearSessionFiles(userId);
    session.retryCount = 0;
    const fresh = await useMultiFileAuthState(ensureSessionDir(userId));
    state = fresh.state;
    saveCreds = fresh.saveCreds;
  }

  const { version } = await fetchLatestBaileysVersion();
  console.log(`[WA][${userId}] Creating socket gen=${myGenId} version=${version.join(".")} registered=${state.creds.registered}`);

  const sock = makeWASocket({
    version,
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, logger),
    },
    printQRInTerminal: false,
    logger,
    // Browsers.ubuntu("Chrome") = ["Ubuntu", "Chrome", "20.0.04"]
    // This is required for phone number pairing to work correctly
    browser: Browsers.ubuntu("Chrome"),
    connectTimeoutMs: 60000,
    defaultQueryTimeoutMs: 60000,
    keepAliveIntervalMs: 20000,
    generateHighQualityLinkPreview: false,
    syncFullHistory: false,
    markOnlineOnConnect: false,
    retryRequestDelayMs: 250,
  });

  session.socket = sock;
  sock.ev.on("creds.update", saveCreds);

  // KEY FIX: Request pairing code IMMEDIATELY after socket creation
  // DO NOT wait for QR event — that's the wrong pattern for phone linking.
  // The pairing code must be requested before WhatsApp generates a QR.
  if (!state.creds.registered) {
    const cleaned = phoneNumber.replace(/[^0-9]/g, "");
    console.log(`[WA][${userId}] gen=${myGenId} Not registered — will request pairing code for ${cleaned} in 1.5s`);

    setTimeout(async () => {
      if (session.socketGenId !== myGenId) {
        console.log(`[WA][${userId}] Pairing code skipped — socket gen changed (was ${myGenId})`);
        return;
      }
      if (!sessions.has(userId)) {
        console.log(`[WA][${userId}] Session gone — skipping pairing code`);
        return;
      }
      if (session.codeRequested) {
        console.log(`[WA][${userId}] Code already requested gen=${myGenId}`);
        return;
      }
      session.codeRequested = true;
      try {
        console.log(`[WA][${userId}] Requesting pairing code for ${cleaned} gen=${myGenId}...`);
        const code = await sock.requestPairingCode(cleaned);
        if (!code) {
          console.error(`[WA][${userId}] requestPairingCode returned empty gen=${myGenId}`);
          session.codeRequested = false;
          return;
        }
        if (session.socketGenId !== myGenId) {
          console.log(`[WA][${userId}] Got code but socket gen changed — discarding gen=${myGenId}`);
          return;
        }
        const formatted = code.match(/.{1,4}/g)?.join("-") ?? code;
        session.pairingCode = formatted;
        session.connecting = false;
        console.log(`[WA][${userId}] Got pairing code: ${formatted} gen=${myGenId}`);
        onCode(formatted);
      } catch (err: any) {
        console.error(`[WA][${userId}] requestPairingCode failed gen=${myGenId}: ${err?.message}`);
        session.codeRequested = false;
        // If code request fails, retry after a delay
        setTimeout(async () => {
          if (session.socketGenId !== myGenId || !sessions.has(userId)) return;
          session.codeRequested = false;
          try {
            const code2 = await sock.requestPairingCode(cleaned);
            if (!code2 || session.socketGenId !== myGenId) return;
            const formatted2 = code2.match(/.{1,4}/g)?.join("-") ?? code2;
            session.pairingCode = formatted2;
            session.codeRequested = true;
            console.log(`[WA][${userId}] Got pairing code (retry): ${formatted2} gen=${myGenId}`);
            onCode(formatted2);
          } catch (err2: any) {
            console.error(`[WA][${userId}] requestPairingCode retry also failed gen=${myGenId}: ${err2?.message}`);
          }
        }, 3000);
      }
    }, 1500);
  }

  sock.ev.on("connection.update", async (update) => {
    // Stale socket check
    if (session.socketGenId !== myGenId) {
      console.log(`[WA][${userId}] Ignoring stale socket gen=${myGenId} (current=${session.socketGenId})`);
      closeSocketSafe(sock);
      return;
    }

    const currentSession = sessions.get(userId);
    if (!currentSession || currentSession !== session) {
      console.log(`[WA][${userId}] Session replaced, ignoring gen=${myGenId}`);
      closeSocketSafe(sock);
      return;
    }

    const { connection, lastDisconnect } = update;
    console.log(`[WA][${userId}] Update gen=${myGenId}: connection=${connection}`);

    if (connection === "open") {
      console.log(`[WA][${userId}] Connected! gen=${myGenId}`);
      session.connected = true;
      session.wasConnected = true;
      session.connecting = false;
      session.retryCount = 0;
      onConnected();
    }

    if (connection === "close") {
      session.connected = false;
      session.connecting = false;

      const boom = lastDisconnect?.error as Boom | undefined;
      const statusCode = boom?.output?.statusCode;
      const reason = boom?.message || "Unknown";

      console.log(`[WA][${userId}] Closed gen=${myGenId}. code=${statusCode} reason=${reason} wasConnected=${session.wasConnected} retries=${session.retryCount}`);

      // Real logout — user manually unlinked from WA settings
      if (statusCode === DisconnectReason.loggedOut) {
        console.log(`[WA][${userId}] Logout detected gen=${myGenId}`);
        clearSessionFiles(userId);
        sessions.delete(userId);
        onDisconnected("WhatsApp ne logout kar diya. Dobara connect karein.");
        return;
      }

      // 401 = stale credentials, clear and retry
      if (statusCode === 401 && !session.wasConnected) {
        console.log(`[WA][${userId}] 401 invalid creds gen=${myGenId} — clearing and retrying`);
        closeSocketSafe(sock);
        session.socket = null;
        clearSessionFiles(userId);
        session.codeRequested = false;
        session.retryCount++;

        if (session.retryCount > 3) {
          sessions.delete(userId);
          onDisconnected("Connection baar baar fail ho raha hai. Dobara try karein.");
          return;
        }

        setTimeout(async () => {
          if (session.socketGenId !== myGenId) return;
          if (sessions.has(userId)) {
            try {
              await createSocket(userId, phoneNumber, onCode, onConnected, onDisconnected, session);
            } catch (e: any) {
              sessions.delete(userId);
              onDisconnected(`Retry failed: ${e?.message}`);
            }
          }
        }, 3000);
        return;
      }

      // 515 = "Stream Errored (restart required)"
      // This is NORMAL after user successfully enters pairing code.
      // WhatsApp saves credentials first, then sends 515.
      // DO NOT clear session files — registered=true must persist for reconnect.
      if (statusCode === 515 || reason.includes("Stream Errored")) {
        console.log(`[WA][${userId}] 515 stream restart gen=${myGenId} — normal after pairing, reconnecting (session preserved)`);
        closeSocketSafe(sock);
        session.socket = null;
        session.codeRequested = false;
        session.retryCount++;

        if (session.retryCount > 8) {
          sessions.delete(userId);
          onDisconnected("Bahut zyada reconnect attempts. Dobara try karein.");
          return;
        }

        const delay = session.wasConnected ? 3000 : 2000;
        setTimeout(async () => {
          if (session.socketGenId !== myGenId) return;
          if (sessions.has(userId)) {
            try {
              await createSocket(userId, phoneNumber, onCode, onConnected, onDisconnected, session);
            } catch (e: any) {
              sessions.delete(userId);
              onDisconnected(`Reconnect failed: ${e?.message}`);
            }
          }
        }, delay);
        return;
      }

      // All other close reasons — reconnect after delay
      console.log(`[WA][${userId}] Will reconnect in 5s gen=${myGenId}...`);
      session.codeRequested = false;
      session.retryCount++;
      setTimeout(async () => {
        if (session.socketGenId !== myGenId) return;
        if (sessions.has(userId)) {
          try {
            await createSocket(userId, phoneNumber, onCode, onConnected, onDisconnected, session);
          } catch (e: any) {
            sessions.delete(userId);
            onDisconnected(`Reconnect failed: ${e?.message}`);
          }
        }
      }, 5000);
    }
  });
}

export async function connectWhatsApp(
  userId: string,
  phoneNumber: string,
  onCode: (code: string) => void,
  onConnected: () => void,
  onDisconnected: (reason: string) => void
): Promise<void> {
  const existing = sessions.get(userId);

  if (existing?.connectLock) {
    console.log(`[WA][${userId}] Connection already in progress, ignoring duplicate`);
    return;
  }

  // Close and remove old session
  if (existing?.socket) {
    closeSocketSafe(existing.socket);
    existing.socket = null;
  }
  sessions.delete(userId);
  clearSessionFiles(userId);

  const session: WhatsAppSession = {
    socket: null,
    connected: false,
    pairingCode: null,
    connecting: true,
    phoneNumber,
    codeRequested: false,
    wasConnected: false,
    retryCount: 0,
    socketGenId: 0,
    connectLock: true,
  };
  sessions.set(userId, session);

  try {
    await createSocket(userId, phoneNumber, onCode, onConnected, onDisconnected, session);
  } finally {
    session.connectLock = false;
  }
}

export function getSession(userId: string): WhatsAppSession | undefined {
  return sessions.get(userId);
}

export interface GroupUnbanRequestResult {
  success: boolean;
  reviewSent: boolean;
  supportMessageSent: boolean;
  supportJid?: string;
  error?: string;
}

function getSupportJidFromEnv(): string | null {
  const raw = process.env["WA_SUPPORT_JID"]?.trim();
  if (!raw) return null;
  if (raw.includes("@")) return raw;
  const digits = raw.replace(/[^0-9]/g, "");
  return digits ? `${digits}@s.whatsapp.net` : null;
}

async function resolveWhatsAppSupportJid(sock: any): Promise<string> {
  const configured = getSupportJidFromEnv();
  if (configured) return configured;

  const supportPhone = (process.env["WA_SUPPORT_PHONE"] || "16282000080").replace(/[^0-9]/g, "");
  try {
    const matches = await sock.onWhatsApp(supportPhone);
    const existing = matches?.find((item: any) => item?.exists && item?.jid);
    if (existing?.jid) return existing.jid;
  } catch {}

  return `${supportPhone}@s.whatsapp.net`;
}

async function sendSupportChatMessage(sock: any, supportJid: string, text: string): Promise<void> {
  try { await sock.presenceSubscribe(supportJid); } catch {}
  try { await sock.sendPresenceUpdate("composing", supportJid); } catch {}
  await new Promise((resolve) => setTimeout(resolve, 700));
  try { await sock.sendPresenceUpdate("paused", supportJid); } catch {}
  await sock.sendMessage(supportJid, { text });
}

export async function requestGroupUnban(
  userId: string,
  groupId: string,
  message: string
): Promise<GroupUnbanRequestResult> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) {
    return {
      success: false,
      reviewSent: false,
      supportMessageSent: false,
      error: "WhatsApp disconnected",
    };
  }

  const sock: any = session.socket;
  let reviewSent = false;
  let supportMessageSent = false;
  let supportJid: string | undefined;
  const errors: string[] = [];

  try {
    if (typeof sock.groupRequestReview === "function") {
      await sock.groupRequestReview(groupId);
      reviewSent = true;
    }
  } catch (err: any) {
    errors.push(`review: ${err?.message || "failed"}`);
  }

  try {
    supportJid = await resolveWhatsAppSupportJid(sock);
    await sendSupportChatMessage(sock, supportJid, message);
    supportMessageSent = true;
  } catch (err: any) {
    errors.push(`support message: ${err?.message || "failed"}`);
  }

  return {
    success: reviewSent || supportMessageSent,
    reviewSent,
    supportMessageSent,
    supportJid,
    error: errors.length ? errors.join("; ") : undefined,
  };
}

export function isConnected(userId: string): boolean {
  const s = sessions.get(userId);
  return s?.connected === true && s?.socket !== null;
}

export async function disconnectWhatsApp(userId: string): Promise<void> {
  const session = sessions.get(userId);
  if (session) {
    session.socketGenId = -1;
    if (session.socket) {
      try { await session.socket.logout(); } catch {}
      closeSocketSafe(session.socket);
    }
  }
  sessions.delete(userId);
  clearSessionFiles(userId);
}

export interface GroupResult {
  id: string;
  inviteCode: string;
}

export async function createWhatsAppGroup(
  userId: string,
  groupName: string,
  participants: string[] = []
): Promise<GroupResult | null> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return null;

  try {
    const group = await session.socket.groupCreate(groupName, participants);
    const inviteCode = await session.socket.groupInviteCode(group.id);
    return {
      id: group.id,
      inviteCode: `https://chat.whatsapp.com/${inviteCode}`,
    };
  } catch (err: any) {
    console.error(`[WA][${userId}] Group creation error:`, err?.message);
    return null;
  }
}

export interface GroupPermissions {
  editGroupInfo: boolean;
  sendMessages: boolean;
  addMembers: boolean;
  approveJoin: boolean;
}

export async function applyGroupSettings(
  userId: string,
  groupId: string,
  perms: GroupPermissions,
  description: string
): Promise<void> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return;
  const sock = session.socket;

  try {
    if (description) await sock.groupUpdateDescription(groupId, description);
  } catch (e: any) { console.error(`[WA][${userId}] desc error:`, e?.message); }

  try {
    await sock.groupSettingUpdate(groupId, perms.editGroupInfo ? "unlocked" : "locked");
  } catch (e: any) { console.error(`[WA][${userId}] editGroupInfo error:`, e?.message); }

  try {
    await sock.groupSettingUpdate(groupId, perms.sendMessages ? "not_announcement" : "announcement");
  } catch (e: any) { console.error(`[WA][${userId}] sendMsg error:`, e?.message); }

  try {
    await (sock as any).groupMemberAddMode(groupId, perms.addMembers ? "all_member_add" : "admin_add");
  } catch (e: any) { console.error(`[WA][${userId}] addMembers error:`, e?.message); }

  try {
    await (sock as any).groupJoinApprovalMode(groupId, perms.approveJoin ? "on" : "off");
  } catch (e: any) { console.error(`[WA][${userId}] approveJoin error:`, e?.message); }
}

export async function setGroupIcon(
  userId: string,
  groupId: string,
  imageBuffer: Buffer
): Promise<boolean> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return false;
  try {
    await session.socket.updateProfilePicture(groupId, imageBuffer);
    return true;
  } catch (err: any) {
    console.error(`[WA][${userId}] Group icon error:`, err?.message);
    return false;
  }
}

function extractInviteCode(link: string): string {
  const withoutQuery = link.split("?")[0];
  return withoutQuery
    .replace("https://chat.whatsapp.com/", "")
    .replace("http://chat.whatsapp.com/", "")
    .replace(/\/$/, "")
    .trim();
}

export async function joinGroupWithLink(
  userId: string,
  link: string
): Promise<{ success: boolean; groupName?: string; error?: string }> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) {
    return { success: false, error: "WhatsApp not connected" };
  }
  try {
    const code = extractInviteCode(link);
    const result = await session.socket.groupAcceptInvite(code);
    return { success: true, groupName: result ?? "Group" };
  } catch (err: any) {
    console.error(`[WA][${userId}] Join group error:`, err?.message);
    return { success: false, error: err?.message || "Failed to join" };
  }
}

function extractNumber(jid: string): string {
  return jid.replace(/:\d+@/, "@").replace(/@.*$/, "").replace(/[^0-9]/g, "");
}

export async function getGroupPendingRequests(
  userId: string,
  groupId: string
): Promise<string[]> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return [];
  try {
    const requests = await session.socket.groupRequestParticipantsList(groupId);
    return requests.map((r: any) => extractNumber(r.jid || "")).filter(Boolean);
  } catch (err: any) {
    console.error(`[WA][${userId}] Pending requests error:`, err?.message);
    return [];
  }
}

// Returns raw normalized JIDs from pending list (preserves @lid or @s.whatsapp.net format)
// Use this for actual approval calls — do NOT reconstruct JIDs from phone numbers
export async function getGroupPendingRequestsJids(
  userId: string,
  groupId: string
): Promise<string[]> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return [];
  try {
    const requests = await session.socket.groupRequestParticipantsList(groupId);
    console.log(`[WA][${userId}] getGroupPendingRequestsJids raw count: ${requests.length}`);
    if (requests.length > 0) {
      console.log(`[WA][${userId}] first pending raw:`, JSON.stringify(requests[0]));
    }
    return requests
      .map((r: any) => {
        const jid: string = r.jid || "";
        // Normalize device suffix (e.g. 91xxx:2@s.whatsapp.net → 91xxx@s.whatsapp.net)
        return jid.replace(/:\d+@/, "@");
      })
      .filter(Boolean);
  } catch (err: any) {
    console.error(`[WA][${userId}] getGroupPendingRequestsJids error:`, err?.message);
    return [];
  }
}

// Extract the numeric phone from a WhatsApp JID
// "919898989898@s.whatsapp.net" → "919898989898"
// "919898989898:2@s.whatsapp.net" → "919898989898"
function extractPhoneFromJid(jid: string): string {
  if (!jid) return "";
  const stripped = jid.replace(/:\d+@/, "@");
  const [part] = stripped.split("@");
  return /^\d+$/.test(part) ? part : "";
}

// Match a cleaned phone number against a set — tries exact match first,
// then last-10-digit suffix match to handle country code variations
function phoneMatchesSet(phone: string, set: Set<string>): boolean {
  if (set.has(phone)) return true;
  if (phone.length >= 10) {
    const last10 = phone.slice(-10);
    for (const p of set) {
      if (p.endsWith(last10)) return true;
    }
  }
  return false;
}

export async function checkContactsInGroup(
  userId: string,
  groupId: string,
  phoneNumbers: string[]
): Promise<{ inMembers: string[]; inPending: string[]; notFound: string[]; pendingAvailable: boolean; allMemberPhones: Set<string>; allPendingPhones: Set<string> }> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) {
    return { inMembers: [], inPending: [], notFound: phoneNumbers, pendingAvailable: false, allMemberPhones: new Set(), allPendingPhones: new Set() };
  }

  try {
    const [metaResult, pendingResult] = await Promise.allSettled([
      session.socket.groupMetadata(groupId),
      session.socket.groupRequestParticipantsList(groupId),
    ]);

    // Build phone set from member JIDs
    // IMPORTANT: When group uses LID addressing mode, p.id is a LID (not a phone JID).
    // In that case, p.phoneNumber contains the real phone in JID format.
    const memberPhones = new Set<string>();
    if (metaResult.status === "fulfilled") {
      for (const p of metaResult.value.participants) {
        const pId = p.id || "";
        if (pId.endsWith("@lid")) {
          // LID mode — use phoneNumber field for the real phone JID
          const ph = extractPhoneFromJid((p as any).phoneNumber || "");
          if (ph) memberPhones.add(ph);
        } else {
          // Normal phone JID
          const ph = extractPhoneFromJid(pId);
          if (ph) memberPhones.add(ph);
        }
      }
      console.log(`[WA][${userId}] CTC members extracted: ${memberPhones.size} (addressingMode: ${(metaResult.value as any).addressingMode || "pn"})`);
    } else {
      console.error(`[WA][${userId}] groupMetadata failed:`, (metaResult as PromiseRejectedResult).reason?.message);
    }

    // Build phone set from pending request attrs
    // Each item is the raw XML attrs: { jid, add_request_code, time, ... }
    // In LID groups the jid attr may itself be a LID — also check phone_number attr
    const pendingPhones = new Set<string>();
    let pendingAvailable = false;
    if (pendingResult.status === "fulfilled" && Array.isArray(pendingResult.value)) {
      pendingAvailable = true;
      const rawList = pendingResult.value as any[];
      console.log(`[WA][${userId}] CTC raw pending count: ${rawList.length}`);
      if (rawList.length > 0) {
        console.log(`[WA][${userId}] CTC first pending item keys:`, Object.keys(rawList[0]).join(", "));
        console.log(`[WA][${userId}] CTC first pending item:`, JSON.stringify(rawList[0]));
      }
      for (const r of rawList) {
        // Try all possible attribute names that could hold the phone/JID
        for (const key of ["jid", "phone_number", "pn", "participant"]) {
          const val = (r as any)[key] || "";
          if (!val) continue;
          const pjid = val.endsWith("@lid") ? "" : val; // skip pure LID values
          const ph = extractPhoneFromJid(pjid);
          if (ph) { pendingPhones.add(ph); break; }
        }
      }
      console.log(`[WA][${userId}] CTC pending phones extracted: ${pendingPhones.size}`);
    } else if (pendingResult.status === "rejected") {
      const err = (pendingResult as PromiseRejectedResult).reason;
      console.warn(`[WA][${userId}] groupRequestParticipantsList failed (need admin + approval mode):`, err?.message);
    }

    console.log(`[WA][${userId}] CTC check — members: ${memberPhones.size}, pending: ${pendingPhones.size}, contacts: ${phoneNumbers.length}`);

    const inMembers: string[] = [];
    const inPending: string[] = [];
    const notFound: string[] = [];

    for (const phone of phoneNumbers) {
      const cleaned = phone.replace(/[^0-9]/g, "");
      if (phoneMatchesSet(cleaned, memberPhones)) {
        inMembers.push(phone);
      } else if (phoneMatchesSet(cleaned, pendingPhones)) {
        inPending.push(phone);
      } else {
        notFound.push(phone);
      }
    }

    return { inMembers, inPending, notFound, pendingAvailable, allMemberPhones: memberPhones, allPendingPhones: pendingPhones };
  } catch (err: any) {
    console.error(`[WA][${userId}] checkContactsInGroup error:`, err?.message);
    return { inMembers: [], inPending: [], notFound: phoneNumbers, pendingAvailable: false, allMemberPhones: new Set(), allPendingPhones: new Set() };
  }
}

export async function getGroupIdFromLink(
  userId: string,
  inviteLink: string
): Promise<{ id: string; subject: string } | null> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return null;
  try {
    const code = extractInviteCode(inviteLink);
    const metadata = await session.socket.groupGetInviteInfo(code);
    return { id: metadata.id, subject: metadata.subject };
  } catch (err: any) {
    console.error(`[WA][${userId}] Group info error:`, err?.message);
    return null;
  }
}

export interface GroupInfo {
  id: string;
  subject: string;
  isAdmin: boolean;
  isMember: boolean;
  participantCount: number;
}

function normalizeJid(jid: string): string {
  return jid.replace(/:\d+@/, "@").split("@")[0].replace(/[^0-9]/g, "");
}

export async function getAllGroups(userId: string): Promise<GroupInfo[]> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return [];
  try {
    const groups = await session.socket.groupFetchAllParticipating();
    const sock = session.socket;

    const rawMyJid = sock.user?.id || "";
    const myLid = (sock.user as any)?.lid || "";

    const myJidNormalized = rawMyJid ? rawMyJid.replace(/:\d+@/, "@") : "";
    const myLidNormalized = myLid ? myLid.replace(/:\d+@/, "@") : "";
    const myNumber = normalizeJid(rawMyJid) || session.phoneNumber.replace(/[^0-9]/g, "");

    console.log(`[WA][${userId}] getAllGroups: myJid=${myJidNormalized} myLid=${myLidNormalized} myNumber=${myNumber} total=${Object.keys(groups).length}`);

    const result: GroupInfo[] = [];

    for (const [id, meta] of Object.entries(groups)) {
      const participants: any[] = (meta as any).participants || [];
      let isAdmin = false;

      for (const p of participants) {
        const pJid = (p.id || "").replace(/:\d+@/, "@");
        const pLid = ((p as any).lid || "").replace(/:\d+@/, "@");
        const pNumber = normalizeJid(p.id || "");

        const matchByJid = myJidNormalized && pJid && pJid === myJidNormalized;
        const matchByLid = myLidNormalized && pLid && pLid === myLidNormalized;
        const matchByLidCross1 = myLidNormalized && pJid && pJid === myLidNormalized;
        const matchByLidCross2 = myJidNormalized && pLid && pLid === myJidNormalized;
        const matchByNumber = myNumber.length >= 7 && pNumber.length >= 7 && (
          pNumber === myNumber || pNumber.endsWith(myNumber) || myNumber.endsWith(pNumber)
        );

        const isMe = matchByJid || matchByLid || matchByLidCross1 || matchByLidCross2 || matchByNumber;
        if (isMe) {
          isAdmin = p.admin === "admin" || p.admin === "superadmin";
          break;
        }
      }

      result.push({
        id,
        subject: (meta as any).subject || "Unknown",
        isAdmin,
        isMember: true,
        participantCount: participants.length,
      });
    }

    return result;
  } catch (err: any) {
    console.error(`[WA][${userId}] Get all groups error:`, err?.message);
    return [];
  }
}

const FATAL_INVITE_ERRORS = ["not-authorized", "forbidden", "item-not-found", "not-participant", "403", "401"];

function isFatalInviteError(err: any): boolean {
  const msg = (err?.message ?? err?.data ?? String(err)).toLowerCase();
  return FATAL_INVITE_ERRORS.some((e) => msg.includes(e));
}

export async function getGroupInviteLink(userId: string, groupId: string, maxRetries: number = 3): Promise<string | null> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const code = await session.socket.groupInviteCode(groupId);
      if (code) return `https://chat.whatsapp.com/${code}`;
    } catch (err: any) {
      console.error(`[WA][${userId}] Get invite link error for ${groupId} (attempt ${attempt}/${maxRetries}):`, err?.message ?? err?.data);
      if (isFatalInviteError(err)) return null;
    }

    try {
      const meta = await session.socket!.groupMetadata(groupId);
      if (meta && (meta as any).inviteCode) {
        return `https://chat.whatsapp.com/${(meta as any).inviteCode}`;
      }
    } catch (err2: any) {
      console.error(`[WA][${userId}] groupMetadata fallback error for ${groupId} (attempt ${attempt}/${maxRetries}):`, err2?.message ?? err2?.data);
      if (isFatalInviteError(err2)) return null;
    }

    if (attempt < maxRetries) {
      const delay = 1500;
      console.log(`[WA][${userId}] Retrying getGroupInviteLink for ${groupId} in ${delay}ms...`);
      await new Promise((r) => setTimeout(r, delay));
      if (!session.socket || !session.connected) return null;
    }
  }

  return null;
}

export async function leaveGroup(userId: string, groupId: string): Promise<boolean> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return false;
  try {
    await session.socket.groupLeave(groupId);
    return true;
  } catch (err: any) {
    console.error(`[WA][${userId}] Leave group error:`, err?.message);
    return false;
  }
}

export interface ParticipantInfo {
  jid: string;
  phone: string; // real phone number — works even in LID mode
  isAdmin: boolean;
  isSuperAdmin: boolean;
}

export async function getGroupParticipants(userId: string, groupId: string): Promise<ParticipantInfo[]> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return [];
  try {
    const meta = await session.socket.groupMetadata(groupId);
    return meta.participants.map((p: any) => {
      const jidRaw: string = p.id || "";
      let phone: string;
      if (jidRaw.endsWith("@lid")) {
        // LID mode: p.phoneNumber holds the real phone JID e.g. "919234038011@s.whatsapp.net"
        phone = extractPhoneFromJid((p as any).phoneNumber || "") || extractPhoneFromJid(jidRaw);
      } else {
        phone = extractPhoneFromJid(jidRaw) || "";
      }
      return {
        jid: jidRaw,
        phone,
        isAdmin: p.admin === "admin" || p.admin === "superadmin",
        isSuperAdmin: p.admin === "superadmin",
      };
    });
  } catch (err: any) {
    console.error(`[WA][${userId}] Get participants error:`, err?.message);
    return [];
  }
}

export async function getGroupPendingList(
  userId: string
): Promise<Array<{ groupId: string; groupName: string; pendingCount: number }>> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return [];
  try {
    const groups = await session.socket.groupFetchAllParticipating();
    const rawMyJid = session.socket.user?.id || "";
    const myLid = (session.socket.user as any)?.lid || "";
    const myJidNormalized = rawMyJid ? rawMyJid.replace(/:\d+@/, "@") : "";
    const myLidNormalized = myLid ? myLid.replace(/:\d+@/, "@") : "";
    const myNumber = normalizeJid(rawMyJid) || session.phoneNumber.replace(/[^0-9]/g, "");

    console.log(`[WA][${userId}] getGroupPendingList: myJid=${myJidNormalized} myLid=${myLidNormalized} myNumber=${myNumber} total=${Object.keys(groups).length}`);

    const adminGroups: Array<{ id: string; subject: string }> = [];
    for (const [id, meta] of Object.entries(groups)) {
      const participants: any[] = (meta as any).participants || [];
      let isAdmin = false;
      for (const p of participants) {
        const pJid = (p.id || "").replace(/:\d+@/, "@");
        const pLid = ((p as any).lid || "").replace(/:\d+@/, "@");
        const pNumber = normalizeJid(p.id || "");

        const matchByJid    = myJidNormalized && pJid && pJid === myJidNormalized;
        const matchByLid    = myLidNormalized && pLid && pLid === myLidNormalized;
        const matchByLidCross1 = myLidNormalized && pJid && pJid === myLidNormalized;
        const matchByLidCross2 = myJidNormalized && pLid && pLid === myJidNormalized;
        const matchByNumber = myNumber.length >= 7 && pNumber.length >= 7 &&
          (pNumber === myNumber || pNumber.endsWith(myNumber) || myNumber.endsWith(pNumber));

        const isMe = matchByJid || matchByLid || matchByLidCross1 || matchByLidCross2 || matchByNumber;
        if (isMe) {
          isAdmin = p.admin === "admin" || p.admin === "superadmin";
          break;
        }
      }
      if (isAdmin) adminGroups.push({ id, subject: (meta as any).subject || id });
    }
    console.log(`[WA][${userId}] getGroupPendingList: adminGroups=${adminGroups.length}`);

    const results = await Promise.allSettled(
      adminGroups.map(async (g) => {
        try {
          const pending = await session.socket!.groupRequestParticipantsList(g.id);
          return { groupId: g.id, groupName: g.subject, pendingCount: Array.isArray(pending) ? pending.length : 0 };
        } catch {
          return { groupId: g.id, groupName: g.subject, pendingCount: 0 };
        }
      })
    );

    return results
      .filter((r) => r.status === "fulfilled")
      .map((r) => (r as PromiseFulfilledResult<any>).value);
  } catch (err: any) {
    console.error(`[WA][${userId}] getGroupPendingList error:`, err?.message);
    return [];
  }
}

export async function removeGroupParticipant(
  userId: string,
  groupId: string,
  participantJid: string
): Promise<boolean> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return false;
  try {
    await session.socket.groupParticipantsUpdate(groupId, [participantJid], "remove");
    return true;
  } catch (err: any) {
    console.error(`[WA][${userId}] Remove participant error:`, err?.message);
    return false;
  }
}

export async function makeGroupAdmin(
  userId: string,
  groupId: string,
  participantJid: string
): Promise<boolean> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return false;
  try {
    await session.socket.groupParticipantsUpdate(groupId, [participantJid], "promote");
    return true;
  } catch (err: any) {
    console.error(`[WA][${userId}] Make admin error:`, err?.message);
    return false;
  }
}

export async function approveGroupParticipant(
  userId: string,
  groupId: string,
  participantJid: string
): Promise<boolean> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return false;
  try {
    await session.socket.groupRequestParticipantsUpdate(groupId, [participantJid], "approve");
    return true;
  } catch (err: any) {
    console.error(`[WA][${userId}] Approve participant error:`, err?.message);
    return false;
  }
}

export async function setGroupApprovalMode(
  userId: string,
  groupId: string,
  mode: "on" | "off"
): Promise<boolean> {
  const session = sessions.get(userId);
  if (!session?.socket || !session.connected) return false;
  try {
    await (session.socket as any).groupJoinApprovalMode(groupId, mode);
    return true;
  } catch (err: any) {
    console.error(`[WA][${userId}] Set approval mode error:`, err?.message);
    return false;
  }
}

export async function findParticipantByPhone(
  userId: string,
  groupId: string,
  phoneNumber: string
): Promise<string | null> {
  const participants = await getGroupParticipants(userId, groupId);
  const cleaned = phoneNumber.replace(/[^0-9]/g, "");
  const last10 = cleaned.slice(-10);
  for (const p of participants) {
    const pCleaned = p.phone.replace(/[^0-9]/g, "");
    const pLast10 = pCleaned.slice(-10);
    if (pCleaned === cleaned || (last10.length >= 7 && pLast10 === last10)) {
      return p.jid;
    }
  }
  return null;
}
