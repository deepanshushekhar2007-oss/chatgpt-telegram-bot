import { getCollection } from "./mongodb";

interface BotData {
  subscriptionMode: boolean;
  accessList: Record<string, { expiresAt: number; grantedBy: number }>;
  bannedUsers: number[];
  totalUsers: number[];
  autoChatEnabled: boolean;
  autoChatAccessList: number[];
}

const DEFAULT_DATA: BotData = {
  subscriptionMode: false,
  accessList: {},
  bannedUsers: [],
  totalUsers: [],
  autoChatEnabled: true,
  autoChatAccessList: [],
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
      { $addToSet: { totalUsers: userId }, $setOnInsert: { subscriptionMode: false, accessList: {}, bannedUsers: [], autoChatEnabled: true, autoChatAccessList: [] } },
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
  const data = await loadBotData();
  if (!data.subscriptionMode) return true;
  const access = data.accessList[String(userId)];
  if (!access) return false;
  if (Date.now() > access.expiresAt) {
    delete data.accessList[String(userId)];
    await saveBotData(data);
    return false;
  }
  return true;
}
