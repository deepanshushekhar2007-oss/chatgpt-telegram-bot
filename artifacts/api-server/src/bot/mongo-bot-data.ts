import { getCollection } from "./mongodb";

interface BotData {
  subscriptionMode: boolean;
  accessList: Record<string, { expiresAt: number; grantedBy: number }>;
  bannedUsers: number[];
  totalUsers: number[];
  autoChatEnabled: boolean;
  autoChatAccessList: number[];
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
}

const DEFAULT_DATA: BotData = {
  subscriptionMode: false,
  accessList: {},
  bannedUsers: [],
  totalUsers: [],
  autoChatEnabled: true,
  autoChatAccessList: [],
  referMode: false,
  freeTrials: {},
  referredBy: {},
  referralAccess: {},
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
        referMode: doc.referMode ?? false,
        freeTrials: doc.freeTrials ?? {},
        referredBy: doc.referredBy ?? {},
        referralAccess: doc.referralAccess ?? {},
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
          referMode: data.referMode,
          freeTrials: data.freeTrials,
          referredBy: data.referredBy,
          referralAccess: data.referralAccess,
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

  // Admin-granted access always wins (works regardless of refermode).
  const granted = data.accessList[String(userId)];
  if (granted) {
    if (granted.expiresAt > Date.now()) {
      return { kind: "admin_grant", expiresAt: granted.expiresAt };
    }
    // Expired — clean up
    delete data.accessList[String(userId)];
    await saveBotData(data);
  }

  if (data.referMode) {
    const trial = data.freeTrials[String(userId)];
    if (trial && trial.expiresAt > Date.now()) {
      return { kind: "trial", expiresAt: trial.expiresAt };
    }
    const ref = data.referralAccess[String(userId)];
    if (ref && ref.expiresAt > Date.now()) {
      return { kind: "referral", expiresAt: ref.expiresAt };
    }
    return { kind: "none" };
  }

  // Refermode OFF → fall back to the original subscription-mode behaviour.
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

  // Referrer access stacks — if they already have time left, extend it;
  // otherwise start fresh from now.
  if (referrerId !== adminUserId) {
    const now = Date.now();
    const existingRef = data.referralAccess[String(referrerId)];
    const baseFrom = existingRef && existingRef.expiresAt > now ? existingRef.expiresAt : now;
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
