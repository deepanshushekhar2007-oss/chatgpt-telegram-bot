import { getCollection } from "./mongodb";

export interface RedeemCode {
  code: string;
  days: number;
  maxUsers: number;
  usedBy: number[];
  createdAt: number;
  createdBy: number;
}

interface BotData {
  subscriptionMode: boolean;
  accessList: Record<string, { expiresAt: number; grantedBy: number }>;
  bannedUsers: number[];
  totalUsers: number[];
  autoChatEnabled: boolean;
  autoChatAccessList: number[];
  autoChatAccessExpiry: Record<string, number>;
  referMode: boolean;
  freeTrials: Record<string, { startedAt: number; expiresAt: number; warned?: boolean }>;
  referredBy: Record<string, number>;
  referralAccess: Record<string, { expiresAt: number; totalReferred: number }>;
  redeemCodes: Record<string, RedeemCode>;
  // userId (string) → number of EXTRA WS slots beyond the default 2 (primary + auto).
  // 0 = max 2 WS total, 1 = max 3 WS, ..., 8 = max 10 WS (hard cap).
  extraWsSlots: Record<string, number>;
}

const DEFAULT_DATA: BotData = {
  subscriptionMode: false,
  accessList: {},
  bannedUsers: [],
  totalUsers: [],
  autoChatEnabled: true,
  autoChatAccessList: [],
  autoChatAccessExpiry: {},
  referMode: false,
  freeTrials: {},
  referredBy: {},
  referralAccess: {},
  redeemCodes: {},
  extraWsSlots: {},
};

export async function loadBotData(): Promise<BotData> {
  try {
    const col = await getCollection("bot_data");
    const doc = await col.findOne({ _id: "main" as any });
    if (doc) {
      return {
        subscriptionMode: doc.subscriptionMode ?? false,
        accessList: doc.accessList ?? {},
        bannedUsers: doc.bannedUsers ?? [],
        totalUsers: doc.totalUsers ?? [],
        autoChatEnabled: doc.autoChatEnabled ?? true,
        autoChatAccessList: doc.autoChatAccessList ?? [],
        autoChatAccessExpiry: doc.autoChatAccessExpiry ?? {},
        referMode: doc.referMode ?? false,
        freeTrials: doc.freeTrials ?? {},
        referredBy: doc.referredBy ?? {},
        referralAccess: doc.referralAccess ?? {},
        redeemCodes: doc.redeemCodes ?? {},
        extraWsSlots: doc.extraWsSlots ?? {},
      };
    }
  } catch (err: any) {
    console.error("[MongoDB] loadBotData error:", err?.message);
  }
  return { ...DEFAULT_DATA };
}

export async function saveBotData(data: BotData): Promise<void> {
  try {
    const col = await getCollection("bot_data");
    await col.updateOne(
      { _id: "main" as any },
      {
        $set: {
          subscriptionMode: data.subscriptionMode,
          accessList: data.accessList,
          bannedUsers: data.bannedUsers,
          totalUsers: data.totalUsers,
          autoChatEnabled: data.autoChatEnabled,
          autoChatAccessList: data.autoChatAccessList,
          autoChatAccessExpiry: data.autoChatAccessExpiry,
          referMode: data.referMode,
          freeTrials: data.freeTrials,
          referredBy: data.referredBy,
          referralAccess: data.referralAccess,
          redeemCodes: data.redeemCodes,
          extraWsSlots: data.extraWsSlots,
          updatedAt: new Date(),
        },
      },
      { upsert: true }
    );
  } catch (err: any) {
    console.error("[MongoDB] saveBotData error:", err?.message);
  }
}

export async function trackUser(userId: number): Promise<void> {
  try {
    const col = await getCollection("bot_data");
    await col.updateOne(
      { _id: "main" as any },
      {
        $addToSet: { totalUsers: userId },
        $setOnInsert: {
          subscriptionMode: false,
          accessList: {},
          bannedUsers: [],
          autoChatEnabled: true,
          autoChatAccessList: [],
          referMode: false,
          freeTrials: {},
          referredBy: {},
          referralAccess: {},
          redeemCodes: {},
          extraWsSlots: {},
        },
      },
      { upsert: true }
    );
  } catch (err: any) {
    console.error("[MongoDB] trackUser error:", err?.message);
  }
}

export async function isUserBanned(userId: number): Promise<boolean> {
  const data = await loadBotData();
  return data.bannedUsers.includes(userId);
}

export async function hasUserAccess(userId: number, adminUserId: number): Promise<boolean> {
  if (userId === adminUserId) return true;
  const state = await getUserAccessState(userId, adminUserId);
  return state.kind !== "none";
}

export type AccessKind =
  | "admin"
  | "subscription_open"
  | "admin_grant"
  | "trial"
  | "referral"
  | "redeem"
  | "none";

export interface AccessState {
  kind: AccessKind;
  expiresAt?: number;
}

export async function getUserAccessState(
  userId: number,
  adminUserId: number
): Promise<AccessState> {
  if (userId === adminUserId) return { kind: "admin" };
  const data = await loadBotData();
  const now = Date.now();

  const granted = data.accessList[String(userId)];
  if (granted && granted.expiresAt <= now) {
    delete data.accessList[String(userId)];
    await saveBotData(data);
  }

  type Window = { kind: AccessState["kind"]; expiresAt: number };
  const windows: Window[] = [];
  const grant = data.accessList[String(userId)];
  if (grant && grant.expiresAt > now) {
    windows.push({ kind: "admin_grant", expiresAt: grant.expiresAt });
  }
  if (data.referMode) {
    const trial = data.freeTrials[String(userId)];
    if (trial && trial.expiresAt > now) {
      windows.push({ kind: "trial", expiresAt: trial.expiresAt });
    }
  }
  const ref = data.referralAccess[String(userId)];
  if (ref && ref.expiresAt > now) {
    windows.push({ kind: "referral", expiresAt: ref.expiresAt });
  }

  if (windows.length > 0) {
    windows.sort((a, b) => b.expiresAt - a.expiresAt);
    return { kind: windows[0].kind, expiresAt: windows[0].expiresAt };
  }

  if (data.referMode) return { kind: "none" };
  if (!data.subscriptionMode) return { kind: "subscription_open" };
  return { kind: "none" };
}

export async function ensureFreeTrial(
  userId: number,
  durationMs: number
): Promise<{ created: boolean; expiresAt: number }> {
  const data = await loadBotData();
  const existing = data.freeTrials[String(userId)];
  if (existing) return { created: false, expiresAt: existing.expiresAt };

  const granted = data.accessList[String(userId)];
  if (granted && granted.expiresAt > Date.now()) {
    return { created: false, expiresAt: granted.expiresAt };
  }
  const ref = data.referralAccess[String(userId)];
  if (ref && ref.expiresAt > Date.now()) {
    return { created: false, expiresAt: ref.expiresAt };
  }

  const now = Date.now();
  const expiresAt = now + durationMs;
  data.freeTrials[String(userId)] = { startedAt: now, expiresAt };
  await saveBotData(data);
  return { created: true, expiresAt };
}

export async function recordReferral(
  refereeId: number,
  referrerId: number,
  accessMs: number,
  adminUserId: number
): Promise<{
  success: boolean;
  reason?:
    | "self_referral"
    | "already_referred"
    | "referrer_banned"
    | "admin_grant_active"
    | "trial_active";
  referrerExpiresAt?: number;
  totalReferred?: number;
}> {
  if (refereeId === referrerId) return { success: false, reason: "self_referral" };
  const data = await loadBotData();

  if (data.bannedUsers.includes(referrerId)) {
    return { success: false, reason: "referrer_banned" };
  }
  if (data.referredBy[String(refereeId)]) {
    return { success: false, reason: "already_referred" };
  }

  const granted = data.accessList[String(refereeId)];
  if (granted && granted.expiresAt > Date.now()) {
    return { success: false, reason: "admin_grant_active" };
  }
  const trial = data.freeTrials[String(refereeId)];
  if (trial && trial.expiresAt > Date.now()) {
    return { success: false, reason: "trial_active" };
  }

  data.referredBy[String(refereeId)] = referrerId;

  if (referrerId !== adminUserId) {
    const now = Date.now();
    const existingRef = data.referralAccess[String(referrerId)];
    const referrerTrial = data.freeTrials[String(referrerId)];
    const referrerGrant = data.accessList[String(referrerId)];
    const candidates = [now];
    if (existingRef && existingRef.expiresAt > now) candidates.push(existingRef.expiresAt);
    if (referrerTrial && referrerTrial.expiresAt > now) candidates.push(referrerTrial.expiresAt);
    if (referrerGrant && referrerGrant.expiresAt > now) candidates.push(referrerGrant.expiresAt);
    const baseFrom = Math.max(...candidates);
    const newExpires = baseFrom + accessMs;
    const totalReferred = (existingRef?.totalReferred ?? 0) + 1;
    data.referralAccess[String(referrerId)] = { expiresAt: newExpires, totalReferred };
    await saveBotData(data);
    return { success: true, referrerExpiresAt: newExpires, totalReferred };
  }
  await saveBotData(data);
  return { success: true };
}

export async function setReferMode(enabled: boolean): Promise<void> {
  const data = await loadBotData();
  data.referMode = enabled;
  await saveBotData(data);
}

export async function findAndMarkTrialsToWarn(
  warnBeforeMs: number
): Promise<Array<{ userId: number; expiresAt: number }>> {
  const data = await loadBotData();
  if (!data.referMode) return [];
  const now = Date.now();
  const toWarn: Array<{ userId: number; expiresAt: number }> = [];
  let changed = false;
  for (const [uidStr, trial] of Object.entries(data.freeTrials)) {
    if (trial.warned) continue;
    const remaining = trial.expiresAt - now;
    if (remaining > 0 && remaining <= warnBeforeMs) {
      const uid = Number(uidStr);
      if (!Number.isFinite(uid)) continue;
      toWarn.push({ userId: uid, expiresAt: trial.expiresAt });
      data.freeTrials[uidStr] = { ...trial, warned: true };
      changed = true;
    }
  }
  if (changed) await saveBotData(data);
  return toWarn;
}

export async function getReferralStats(userId: number): Promise<{
  expiresAt: number;
  totalReferred: number;
}> {
  const data = await loadBotData();
  const ref = data.referralAccess[String(userId)];
  return {
    expiresAt: ref?.expiresAt ?? 0,
    totalReferred: ref?.totalReferred ?? 0,
  };
}

// ── Redeem Code Functions ────────────────────────────────────────────────────

export async function createRedeemCode(
  code: string,
  days: number,
  maxUsers: number,
  adminId: number
): Promise<{ success: boolean; reason?: "already_exists" }> {
  const data = await loadBotData();
  const upperCode = code.toUpperCase();
  if (data.redeemCodes[upperCode]) {
    return { success: false, reason: "already_exists" };
  }
  data.redeemCodes[upperCode] = {
    code: upperCode,
    days,
    maxUsers,
    usedBy: [],
    createdAt: Date.now(),
    createdBy: adminId,
  };
  await saveBotData(data);
  return { success: true };
}

export async function redeemUserCode(
  userId: number,
  code: string
): Promise<{
  success: boolean;
  reason?: "not_found" | "already_redeemed" | "max_reached";
  days?: number;
  expiresAt?: number;
}> {
  const data = await loadBotData();
  const upperCode = code.toUpperCase();
  const entry = data.redeemCodes[upperCode];

  if (!entry) return { success: false, reason: "not_found" };
  if (entry.usedBy.includes(userId)) return { success: false, reason: "already_redeemed" };
  if (entry.usedBy.length >= entry.maxUsers) return { success: false, reason: "max_reached" };

  const now = Date.now();
  const existing = data.accessList[String(userId)];
  const baseFrom = existing && existing.expiresAt > now ? existing.expiresAt : now;
  const expiresAt = baseFrom + entry.days * 86400000;

  data.accessList[String(userId)] = { expiresAt, grantedBy: entry.createdBy };
  entry.usedBy.push(userId);
  data.redeemCodes[upperCode] = entry;
  await saveBotData(data);

  return { success: true, days: entry.days, expiresAt };
}

export async function getRedeemCodeInfo(
  code: string
): Promise<RedeemCode | null> {
  const data = await loadBotData();
  return data.redeemCodes[code.toUpperCase()] ?? null;
}

export async function listAllRedeemCodes(): Promise<RedeemCode[]> {
  const data = await loadBotData();
  return Object.values(data.redeemCodes);
}

export async function deleteRedeemCode(
  code: string
): Promise<{ success: boolean }> {
  const data = await loadBotData();
  const upperCode = code.toUpperCase();
  if (!data.redeemCodes[upperCode]) return { success: false };
  delete data.redeemCodes[upperCode];
  await saveBotData(data);
  return { success: true };
}

// ── Extra WS Slots ─────────────────────────────────────────────────────────
// Admin grants users the ability to connect extra WhatsApp accounts (3rd, 4th...10th).
// Each /add2ws call increments the user's extra slots by 1 (max 8 extra = 10 total).

export const MAX_EXTRA_WS_SLOTS = 8; // max 10 total WS accounts (2 default + 8 extra)

export async function getExtraWsSlots(userId: number): Promise<number> {
  const data = await loadBotData();
  return data.extraWsSlots[String(userId)] ?? 0;
}

export async function addOneWsSlot(userId: number): Promise<{ newTotal: number; alreadyMax: boolean }> {
  const data = await loadBotData();
  const current = data.extraWsSlots[String(userId)] ?? 0;
  if (current >= MAX_EXTRA_WS_SLOTS) {
    return { newTotal: 2 + current, alreadyMax: true };
  }
  const next = current + 1;
  data.extraWsSlots[String(userId)] = next;
  await saveBotData(data);
  return { newTotal: 2 + next, alreadyMax: false };
}

export async function loadAllExtraWsSlots(): Promise<Record<string, number>> {
  const data = await loadBotData();
  return data.extraWsSlots ?? {};
}

// ─── Autochat Session Persistence ──────────────────────────────────────────

export interface PersistedAutoChatSession {
  userId: number;
  autoUserId: string;
  startedAt: number;
  sessionType: "old" | "cig" | "acf";

  // ── old / cig shared ──────────────────────────────────────────────────────
  groupIds?: string[];
  message?: string;
  delaySeconds?: number;
  repeatCount?: number;

  // ── cig-specific ──────────────────────────────────────────────────────────
  groups?: Array<{ id: string; subject: string }>;
  autoChatExpiresAt?: number;
  currentGroupIndex?: number;
  messageIndex?: number;

  // ── acf-specific ──────────────────────────────────────────────────────────
  primaryJid?: string;
  autoJid?: string;

  // ── multi-WS extension ────────────────────────────────────────────────────
  // When >2 WS accounts are connected, store all WS session IDs.
  // For CIG: wsUserIds list + per-group WS membership.
  // For ACF: wsUserIds list + wsJids list (all accounts' JIDs).
  wsUserIds?: string[];
  wsJids?: string[];
  wsGroupMembership?: number[][]; // parallel to groups[]: indices into wsUserIds for each group

  // ── progress persistence ─────────────────────────────────────────────────
  sentCount?: number; // total messages sent so far (restore after restart)
}

export async function saveAutoChatSession(session: PersistedAutoChatSession): Promise<void> {
  try {
    const col = await getCollection("autochat_sessions");
    await col.replaceOne({ userId: session.userId }, session, { upsert: true });
  } catch {}
}

export async function deleteAutoChatSession(userId: number): Promise<void> {
  try {
    const col = await getCollection("autochat_sessions");
    await col.deleteOne({ userId });
  } catch {}
}

export async function loadAllAutoChatSessions(): Promise<PersistedAutoChatSession[]> {
  try {
    const col = await getCollection("autochat_sessions");
    const docs = await col.find({}).toArray();
    return docs.map((d) => ({
      userId: d["userId"] as number,
      autoUserId: d["autoUserId"] as string,
      startedAt: d["startedAt"] as number,
      sessionType: (d["sessionType"] as "old" | "cig" | "acf") ?? "old",
      groupIds: (d["groupIds"] || []) as string[],
      message: d["message"] as string | undefined,
      delaySeconds: d["delaySeconds"] as number | undefined,
      repeatCount: d["repeatCount"] as number | undefined,
      groups: d["groups"] as Array<{ id: string; subject: string }> | undefined,
      autoChatExpiresAt: d["autoChatExpiresAt"] as number | undefined,
      currentGroupIndex: d["currentGroupIndex"] as number | undefined,
      messageIndex: d["messageIndex"] as number | undefined,
      primaryJid: d["primaryJid"] as string | undefined,
      autoJid: d["autoJid"] as string | undefined,
      wsUserIds: d["wsUserIds"] as string[] | undefined,
      wsJids: d["wsJids"] as string[] | undefined,
      wsGroupMembership: d["wsGroupMembership"] as number[][] | undefined,
      sentCount: d["sentCount"] as number | undefined,
    }));
  } catch {
    return [];
  }
}

// ─── Pending Group Creation Persistence ─────────────────────────────────────

export interface PersistedGroupSettings {
  name: string;
  description: string;
  count: number;
  finalNames: string[];
  namingMode: "auto" | "custom";
  editGroupInfo: boolean;
  sendMessages: boolean;
  addMembers: boolean;
  approveJoin: boolean;
  disappearingMessages: number;
  friendNumbers: string[];
  makeFriendAdmin: boolean;
}

export const PENDING_GROUP_TTL_MS = 20 * 60 * 1000;

export async function savePendingGroupCreation(
  userId: number,
  gs: PersistedGroupSettings
): Promise<void> {
  try {
    const col = await getCollection("pending_group_creation");
    await col.replaceOne(
      { userId },
      {
        userId,
        savedAt: Date.now(),
        expiresAt: Date.now() + PENDING_GROUP_TTL_MS,
        groupSettings: gs,
      },
      { upsert: true }
    );
  } catch {}
}

export async function loadPendingGroupCreation(
  userId: number
): Promise<PersistedGroupSettings | null> {
  try {
    const col = await getCollection("pending_group_creation");
    const doc = await col.findOne({ userId });
    if (!doc) return null;
    if (doc["expiresAt"] && Date.now() > (doc["expiresAt"] as number)) {
      await col.deleteOne({ userId }).catch(() => {});
      return null;
    }
    return doc["groupSettings"] as PersistedGroupSettings;
  } catch {
    return null;
  }
}

export async function deletePendingGroupCreation(userId: number): Promise<void> {
  try {
    const col = await getCollection("pending_group_creation");
    await col.deleteOne({ userId });
  } catch {}
}
