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
  // userId (string) → expiry timestamp in ms. Absent = unlimited access.
  autoChatAccessExpiry: Record<string, number>;
  // ── Referral Mode ────────────────────────────────────────────────────────
  // When referMode is ON:
  //   - Every new user automatically gets a 24-hour free trial (after they
  //     join the force-sub channel) which unlocks every feature except
  //     Auto Chat (Auto Chat continues to obey its own admin-controlled
  //     toggle / access list, exactly like before).
  //   - When the trial expires, the user can no longer use any button.
  //     They are shown their personal referral link and can earn 1 day of
  //     access for every new person who starts the bot through that link.
  //   - Admin can still grant access manually with /access [id] [days].
  //     Users with admin-granted access do NOT need to refer anyone.
  // When referMode is OFF, the bot behaves exactly like before — only the
  // existing /access subscription-mode logic applies.
  referMode: boolean;
  // userId -> trial window. `warned` flips to true the first time we send
  // the "30 min left" reminder so the same user is never pinged twice for
  // the same trial.
  freeTrials: Record<string, { startedAt: number; expiresAt: number; warned?: boolean }>;
  // refereeUserId -> referrerUserId (a given user can only be referred once,
  // ever — this prevents the same user from being recycled to farm rewards).
  referredBy: Record<string, number>;
  // referrerUserId -> accumulated referral access window + lifetime count
  referralAccess: Record<string, { expiresAt: number; totalReferred: number }>;
  // ── Redeem Codes ─────────────────────────────────────────────────────────
  // Admin creates codes with /redeem CODE DAYS MAXUSERS.
  // Users redeem with /redeem CODE to get instant access.
  redeemCodes: Record<string, RedeemCode>;
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

// Legacy access check kept for backward compatibility — used by callers that
// only need a yes/no answer and don't care about referral mode details.
// In referral mode this returns true if the user has admin grant, an active
// trial, or active referral-earned access.
export async function hasUserAccess(userId: number, adminUserId: number): Promise<boolean> {
  if (userId === adminUserId) return true;
  const state = await getUserAccessState(userId, adminUserId);
  return state.kind !== "none";
}

// ── Access state helpers ────────────────────────────────────────────────────

export type AccessKind =
  | "admin"
  | "subscription_open"   // refermode OFF and subscriptionMode OFF → free for all
  | "admin_grant"         // entry in accessList still valid
  | "trial"               // 24h free trial active (refermode only)
  | "referral"            // referral-earned days active (refermode only)
  | "redeem"              // access granted via redeem code
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

  // Clean up expired admin grants so the DB doesn't grow forever.
  const granted = data.accessList[String(userId)];
  if (granted && granted.expiresAt <= now) {
    delete data.accessList[String(userId)];
    await saveBotData(data);
  }

  // Build the list of every active access window the user has earned.
  // We always pick the one that expires LATEST, so:
  //   - trial 24h + 1 referral (24h) => "Referral access, 47h left"
  //   - admin grant 7d + 3 referrals => "Referral access, 10d left"
  //   - admin grant 30d alone        => "Premium access, 30d left"
  // The user never loses time because of which "kind" was checked first.
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
  // Referral access is honoured regardless of refermode — once a user has
  // earned referral days, they keep them even if admin flips refermode off.
  const ref = data.referralAccess[String(userId)];
  if (ref && ref.expiresAt > now) {
    windows.push({ kind: "referral", expiresAt: ref.expiresAt });
  }

  if (windows.length > 0) {
    windows.sort((a, b) => b.expiresAt - a.expiresAt);
    return { kind: windows[0].kind, expiresAt: windows[0].expiresAt };
  }

  // No active windows. Refermode ON => locked out. Refermode OFF =>
  // fall back to the original subscription-mode behaviour.
  if (data.referMode) return { kind: "none" };
  if (!data.subscriptionMode) return { kind: "subscription_open" };
  return { kind: "none" };
}

// Idempotently start a 24h trial for the given user. Returns whether a new
// trial was created (callers use this to decide whether to send the welcome
// trial notification). Will not start a trial for users that already have an
// active trial, an active referral window, or admin-granted access.
export async function ensureFreeTrial(
  userId: number,
  durationMs: number
): Promise<{ created: boolean; expiresAt: number }> {
  const data = await loadBotData();
  const existing = data.freeTrials[String(userId)];
  if (existing) return { created: false, expiresAt: existing.expiresAt };

  // Don't start a fresh trial if the user already has access through some
  // other route — it would be misleading to tell them "your trial just
  // started" when they already paid / were given access by admin.
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

// Record a referral. Each user can only be referred ONCE, ever — this is
// enforced via the referredBy map so abusers can't rejoin to farm credits.
// On success the referrer's access window is extended by `accessMs` (days
// stack, they don't reset). Returns the new access state for the referrer
// so the caller can notify them.
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

  // Don't reward referring a user who already has admin-granted access — they
  // didn't actually need a referral, so it would be a free farm.
  const granted = data.accessList[String(refereeId)];
  if (granted && granted.expiresAt > Date.now()) {
    return { success: false, reason: "admin_grant_active" };
  }
  // Don't reward when the referee has an active trial — trials are the
  // initial onboarding window, not something to be farmed by re-sharing
  // the link to existing trial users.
  const trial = data.freeTrials[String(refereeId)];
  if (trial && trial.expiresAt > Date.now()) {
    return { success: false, reason: "trial_active" };
  }

  data.referredBy[String(refereeId)] = referrerId;

  // Referrer access stacks. The new day is added on TOP of every access
  // window the referrer currently has — their own trial, any earlier
  // referral days, and admin-granted access — so 1 referral always
  // means a real +24h, never silently overlapping an existing window.
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
  // Admin doesn't need access tracking, but still record the referee link.
  await saveBotData(data);
  return { success: true };
}

export async function setReferMode(enabled: boolean): Promise<void> {
  const data = await loadBotData();
  data.referMode = enabled;
  await saveBotData(data);
}

// Find every user whose free trial expires inside (now, now+warnBeforeMs]
// and that hasn't been warned yet. Returns the userIds AND atomically marks
// them as warned so concurrent ticks won't double-send. Caller is
// responsible for actually sending the Telegram message — if delivery
// fails (user blocked the bot, etc.) we still consider the warning
// "delivered" because there is nothing useful to retry to.
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
    // Only warn while the trial is still active AND we are inside the
    // warning window. Trials that already expired silently are skipped —
    // the next button press surfaces the refer-required UI anyway.
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

  // Grant access — stacks on top of existing access like /access command
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

// ─── Autochat Session Persistence ──────────────────────────────────────────

export interface PersistedAutoChatSession {
  userId: number;
  autoUserId: string;
  groupIds: string[];
  message: string;
  delaySeconds: number;
  repeatCount: number;
  startedAt: number;
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
      groupIds: (d["groupIds"] || []) as string[],
      message: d["message"] as string,
      delaySeconds: d["delaySeconds"] as number,
      repeatCount: d["repeatCount"] as number,
      startedAt: d["startedAt"] as number,
    }));
  } catch {
    return [];
  }
}
