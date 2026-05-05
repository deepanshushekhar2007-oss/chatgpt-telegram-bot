import makeWASocket, {
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  Browsers,
} from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import pino from "pino";
import { useMongoDBAuthState, clearMongoSession, listStoredWhatsAppSessions, savePairingMode } from "./mongo-auth-state";

const logger = pino({ level: "silent" });

// Memory optimization: cache Baileys version so it is fetched only ONCE
// instead of on every socket creation and reconnect
let _cachedVersion: number[] | null = null;
async function getCachedBaileysVersion(): Promise<number[]> {
  if (!_cachedVersion) {
    const result = await fetchLatestBaileysVersion();
    _cachedVersion = result.version;
  }
  return _cachedVersion;
}

let socketGenCounter = 0;

interface WhatsAppSession {
  socket: ReturnType<typeof makeWASocket> | null;
  connected: boolean;
  pairingCode: string | null;
  qrCode: string | null;
  qrExpiresAt: number | null;
  pairingMode: "code" | "qr";
  connecting: boolean;
  phoneNumber: string;
  codeRequested: boolean;
  wasConnected: boolean;
  retryCount: number;
  socketGenId: number;
  connectLock: boolean;
  // ms timestamp of the last time this session was actively used (any
  // user-initiated WA call or incoming message). Drives idle eviction so
  // we can keep memory bounded on small hosts (Render free tier = 512MB).
  lastActivityAt: number;
}

const sessions: Map<string, WhatsAppSession> = new Map();

// ── Per-session timer tracker ─────────────────────────────────────────────
// Every reconnect / pairing-code retry uses setTimeout. The callback closes
// over the (large) Baileys socket and auth-state objects. If we don't cancel
// these timers when a session is disconnected/evicted, the closure keeps
// the socket & creds + signal keys alive for the full delay (up to 2 min
// for 403 backoff). On a 512MB host that single retained closure can pin
// 5–10 MB per disconnected user — so RAM never drops after a logout.
const sessionTimers: Map<string, Set<NodeJS.Timeout>> = new Map();

function trackSessionTimer(userId: string, fn: () => void | Promise<void>, ms: number): NodeJS.Timeout {
  const t = setTimeout(async () => {
    sessionTimers.get(userId)?.delete(t);
    try { await fn(); } catch (err: any) {
      console.error(`[WA][TIMER][${userId}] error:`, err?.message);
    }
  }, ms);
  let bucket = sessionTimers.get(userId);
  if (!bucket) { bucket = new Set(); sessionTimers.set(userId, bucket); }
  bucket.add(t);
  return t;
}

function clearAllSessionTimers(userId: string): number {
  const bucket = sessionTimers.get(userId);
  if (!bucket) return 0;
  let cleared = 0;
  for (const t of bucket) { clearTimeout(t); cleared++; }
  bucket.clear();
  sessionTimers.delete(userId);
  return cleared;
}

// Try to actually return freed pages to the OS. V8's gc() reclaims heap
// objects, but on Linux glibc malloc holds onto freed regions in its
// arena (RSS stays high even though the JS heap shrunk). A best-effort
// double-gc + small wait gives V8 multiple passes and lets the OS reclaim.
async function forceMemoryReclaim(): Promise<void> {
  if (typeof (global as any).gc !== "function") return;
  try {
    (global as any).gc();
    await new Promise((r) => setTimeout(r, 50));
    (global as any).gc();
  } catch {}
}

// ── Memory-aware idle session eviction ─────────────────────────────────────
// Cap raised to 500 per request. NOTE: 500 simultaneous Baileys sockets
// will NOT actually fit in 512MB — each socket holds ~5-10MB (WebSocket +
// Signal-protocol keys + auth state + buffers), so the realistic ceiling
// on a 512MB Render box is ~25-30 live sockets before OOM. The
// MEMORY_PRESSURE_RSS_MB guard below will start evicting LRU sockets
// once RSS climbs past the threshold, so even with the cap at 500 the
// process will stay within memory budget — extra sessions just get
// evicted faster. Set WA_MAX_LIVE_SESSIONS to a smaller number via env
// if you upgrade to a paid Render plan and want a hard cap that matches
// your real RAM budget.
const IDLE_EVICTION_MS = Number(process.env["WA_IDLE_EVICT_MS"] || 30 * 60 * 1000); // 30 min
const MAX_LIVE_SESSIONS = Number(process.env["WA_MAX_LIVE_SESSIONS"] || 500);
const MEMORY_PRESSURE_RSS_MB = Number(process.env["WA_MEMORY_PRESSURE_MB"] || 380);

export function markSessionActive(userId: string): void {
  const s = sessions.get(userId);
  if (s) s.lastActivityAt = Date.now();
}

// Sessions that are currently running a long-lived background job (e.g. the
// Auto Request Accepter). While a userId is in this set, sweepIdleSessions
// will NEVER evict their WhatsApp socket — neither via the 30-minute idle
// rule nor via memory-pressure LRU. Background jobs that protect a session
// MUST also unprotect it when they finish so memory eviction can resume.
const protectedUserIds: Set<string> = new Set();

export function protectSessionFromEviction(userId: string): void {
  protectedUserIds.add(userId);
  // Also bump activity so any in-flight sweep this minute won't grab it.
  const s = sessions.get(userId);
  if (s) s.lastActivityAt = Date.now();
  console.log(`[WA][PROTECT] User ${userId} session protected from idle eviction (${protectedUserIds.size} total)`);
}

export function unprotectSession(userId: string): void {
  if (protectedUserIds.delete(userId)) {
    console.log(`[WA][PROTECT] User ${userId} session unprotected (${protectedUserIds.size} remaining)`);
  }
}

export function isSessionProtected(userId: string): boolean {
  return protectedUserIds.has(userId);
}

// Internal: same as `sessions.get(userId)` but ALSO bumps the session's
// last-activity timestamp. EVERY exported WhatsApp helper that uses the
// socket goes through this so that real socket usage (fetching a link,
// adding members, leaving a group, sending a message, etc.) counts as
// activity. Without this the 30-minute idle eviction sweep evicts the
// session even while the user is actively using the bot — because the
// bumps were only happening on session restore / isConnected() checks,
// not on the actual socket calls.
function useSession(userId: string): WhatsAppSession | undefined {
  const s = sessions.get(userId);
  if (s) s.lastActivityAt = Date.now();
  return s;
}

// Sessions that have been bumped within this window are considered
// "actively in use" and are protected from memory-pressure LRU eviction.
// This prevents the situation where heavy concurrent usage briefly
// pushes RSS over the threshold and the sweep accidentally kills the
// user who is RIGHT NOW pressing buttons. The 30-minute idle eviction
// (pass 1 of the sweep) is unaffected — it only fires when the session
// has been idle past IDLE_EVICTION_MS, so an actively-used session
// already gets skipped there.
const ACTIVE_SESSION_PROTECTION_MS = 60 * 1000;

function evictSessionSocket(userId: string, session: WhatsAppSession): void {
  // Cancel ANY pending reconnect/pairing timers FIRST — otherwise their
  // closures keep the soon-to-be-orphaned socket + auth state alive in RAM
  // until they fire (up to 2 min on 403 backoff).
  const cleared = clearAllSessionTimers(userId);
  // Invalidate the gen so anything still in-flight ignores this socket.
  session.socketGenId = -1;
  // Close the underlying socket but keep the session record so isConnected()
  // reports false, the user sees "WhatsApp not connected", and lazy restore
  // can pick it back up from MongoDB on next interaction.
  closeSocketSafe(session.socket);
  session.socket = null;
  session.connected = false;
  session.connecting = false;
  // Remove the session entry entirely so the next interaction triggers a
  // full lazy restore (which loads creds from MongoDB and reopens the socket).
  sessions.delete(userId);
  console.log(`[WA][EVICT][${userId}] Idle socket closed to free memory (cleared ${cleared} timers)`);
  // Best-effort: ask V8 to reclaim now so RSS actually drops on disconnect.
  void forceMemoryReclaim();
}

export function sweepIdleSessions(): { evicted: number; total: number } {
  const now = Date.now();
  let evicted = 0;

  // 1. Always evict sessions that have been idle past the threshold,
  //    EXCEPT sessions that are explicitly protected (e.g. running an
  //    Auto Request Accepter job). Protected sessions stay alive even
  //    while the user is not interacting with the bot, so background
  //    polling can keep working for the entire job duration.
  for (const [userId, session] of [...sessions.entries()]) {
    if (session.connectLock) continue;
    if (protectedUserIds.has(userId)) continue;
    if (now - session.lastActivityAt > IDLE_EVICTION_MS) {
      evictSessionSocket(userId, session);
      evicted++;
    }
  }

  // 2. If we're still over the live-session cap or under memory pressure,
  //    evict least-recently-used sessions until we're back in budget.
  //    IMPORTANT: sessions that were active within the last
  //    ACTIVE_SESSION_PROTECTION_MS window are excluded from this LRU
  //    eviction — they belong to users who are right now pressing
  //    buttons, and killing them would cause the exact "WhatsApp
  //    disconnected mid-use" bug we're trying to prevent. Idle
  //    sessions still get cleaned up; only ACTIVE ones are protected.
  //    Sessions in `protectedUserIds` are also always excluded.
  const rssMb = process.memoryUsage().rss / 1024 / 1024;
  const overCap = sessions.size > MAX_LIVE_SESSIONS;
  const memoryHigh = rssMb > MEMORY_PRESSURE_RSS_MB;
  if (overCap || memoryHigh) {
    const protectionCutoff = now - ACTIVE_SESSION_PROTECTION_MS;
    const candidates = [...sessions.entries()]
      .filter(
        ([userId, s]) =>
          !s.connectLock &&
          !protectedUserIds.has(userId) &&
          s.lastActivityAt < protectionCutoff
      )
      .sort((a, b) => a[1].lastActivityAt - b[1].lastActivityAt);
    while (
      candidates.length > 0 &&
      (sessions.size > MAX_LIVE_SESSIONS ||
        process.memoryUsage().rss / 1024 / 1024 > MEMORY_PRESSURE_RSS_MB)
    ) {
      const [userId, session] = candidates.shift()!;
      evictSessionSocket(userId, session);
      evicted++;
    }
  }

  return { evicted, total: sessions.size };
}

// Lazily reload a session from MongoDB on the next user interaction. Returns
// true once the socket is open and connected, false if no stored creds exist
// or the connection failed.
export async function ensureSessionLoaded(userId: string): Promise<boolean> {
  const existing = sessions.get(userId);
  if (existing?.connected && existing.socket) {
    existing.lastActivityAt = Date.now();
    return true;
  }
  if (existing?.connectLock) {
    // Another caller is already restoring — just wait briefly.
    for (let i = 0; i < 50 && existing.connectLock; i++) {
      await new Promise((r) => setTimeout(r, 100));
    }
    return existing.connected && existing.socket !== null;
  }

  // No live session — check MongoDB for stored creds and lazy-restore.
  const stored = (await listStoredWhatsAppSessions()).find((s) => s.userId === userId);
  if (!stored) return false;

  const session: WhatsAppSession = {
    socket: null,
    connected: false,
    pairingCode: null,
    qrCode: null,
    qrExpiresAt: null,
    pairingMode: stored.pairingMode || "code",
    connecting: true,
    phoneNumber: stored.phoneNumber,
    codeRequested: false,
    wasConnected: true,
    retryCount: 0,
    socketGenId: 0,
    connectLock: true,
    lastActivityAt: Date.now(),
  };
  sessions.set(userId, session);

  try {
    // Make room before opening a new socket if we're already at the cap.
    if (sessions.size > MAX_LIVE_SESSIONS) sweepIdleSessions();
    await createSocket(
      stored.userId,
      stored.phoneNumber,
      stored.pairingMode || "code",
      () => {},
      () => {},
      () => console.log(`[WA][LAZY][${stored.userId}] Session restored on demand`),
      (reason) => {
        console.log(`[WA][LAZY][${stored.userId}] Restore disconnected: ${reason}`);
        notifyDisconnect(stored.userId, reason);
      },
      session
    );
    return session.connected && session.socket !== null;
  } catch (err: any) {
    console.error(`[WA][LAZY][${stored.userId}] Failed:`, err?.message);
    sessions.delete(stored.userId);
    return false;
  } finally {
    session.connectLock = false;
  }
}

async function clearSessionData(userId: string): Promise<void> {
  try {
    await clearMongoSession(userId);
    console.log(`[WA][${userId}] Cleared session from MongoDB`);
  } catch (err: any) {
    console.error(`[WA][${userId}] clearSessionData error:`, err?.message);
  }
}

function closeSocketSafe(sock: ReturnType<typeof makeWASocket> | null): void {
  if (!sock) return;
  // 1) End the underlying websocket
  try { sock.end(undefined); } catch {}
  // 2) Drop all event listeners so closures (creds.update, connection.update,
  //    saveCreds reference, etc.) become unreachable and GC-able. Without this,
  //    every reconnect leaks one full set of listeners and their captured
  //    state — over hours this is a major source of OOM on small hosts.
  try { (sock.ev as any)?.removeAllListeners?.(); } catch {}
  try { (sock as any).ws?.removeAllListeners?.(); } catch {}
}

async function createSocket(
  userId: string,
  phoneNumber: string,
  pairingMode: "code" | "qr",
  onCode: (code: string) => void,
  onQr: (qr: string, expiresAt: number) => void,
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

  let { state, saveCreds } = await useMongoDBAuthState(userId);

  if (state.creds.registered && session.retryCount >= 3 && !session.wasConnected) {
    console.log(`[WA][${userId}] gen=${myGenId} Stale registered session after ${session.retryCount} fails — clearing`);
    await clearSessionData(userId);
    session.retryCount = 0;
    const fresh = await useMongoDBAuthState(userId);
    state = fresh.state;
    saveCreds = fresh.saveCreds;
  }

  const version = await getCachedBaileysVersion();
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
    keepAliveIntervalMs: 30000,
    generateHighQualityLinkPreview: false,
    syncFullHistory: false,
    markOnlineOnConnect: false,
    retryRequestDelayMs: 250,
    getMessage: async () => undefined, // No in-memory message buffering
  });

  session.socket = sock;
  sock.ev.on("creds.update", saveCreds);

  if (!state.creds.registered && pairingMode === "code") {
    const cleaned = phoneNumber.replace(/[^0-9]/g, "");

    // Guard: QR sessions have no phone number. If creds are unregistered and
    // there is no phone number, attempting a pairing code request will always
    // fail (WhatsApp requires a number). Bail out early with a clear message
    // so the user knows to reconnect via QR instead of seeing a cryptic error.
    if (!cleaned) {
      console.log(`[WA][${userId}] QR session with unregistered creds — skipping pairing code, asking user to reconnect via QR`);
      closeSocketSafe(sock);
      session.socket = null;
      clearAllSessionTimers(userId);
      sessions.delete(userId);
      void clearSessionData(userId);
      onDisconnected("Your QR session has expired or is no longer valid. Please reconnect via QR code.");
      void forceMemoryReclaim();
      return;
    }

    let codeRequestScheduled = false;

    const requestCode = async () => {
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
        trackSessionTimer(userId, async () => {
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
            closeSocketSafe(sock);
            session.socket = null;
            clearAllSessionTimers(userId);
            sessions.delete(userId);
            onDisconnected("Pairing code nahi mil raha. Thodi der baad dobara try karein.");
          }
        }, 5000);
      }
    };

    // Strategy: wait for "connecting" state then hold 3s for WebSocket handshake to settle.
    // Fallback: request after 10s regardless, for slow/cold-start servers (e.g. Render).
    // Requesting code too early (< 2s after "connecting") causes WhatsApp to issue a
    // "fake" code that appears valid but cannot be entered on the phone.
    const fallbackTimer = setTimeout(async () => {
      if (!codeRequestScheduled) {
        codeRequestScheduled = true;
        console.log(`[WA][${userId}] gen=${myGenId} Fallback: requesting pairing code after 10s`);
        await requestCode();
      }
    }, 10000);

    sock.ev.on("connection.update", async (update) => {
      if (codeRequestScheduled || session.codeRequested) return;
      const { connection } = update;
      // Only trigger on "connecting" — this means the WebSocket is being established.
      // Do NOT trigger on "open" (that means already registered / re-auth).
      // Wait 3s after "connecting" so the noise handshake with WA servers completes.
      if (connection === "connecting") {
        codeRequestScheduled = true;
        clearTimeout(fallbackTimer);
        console.log(`[WA][${userId}] gen=${myGenId} Socket connecting — requesting pairing code in 3s`);
        setTimeout(async () => {
          await requestCode();
        }, 3000);
      }
    });
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

    const { connection, lastDisconnect, qr } = update;
    // Only log when there's a real connection state change. The frequent
    // `connection=undefined` updates (qr/isNewLogin/etc) are noise that
    // floods stdout and was hurting RAM/IO on small hosts.
    if (connection) {
      console.log(`[WA][${userId}] Update gen=${myGenId}: connection=${connection}`);
    }

    if (pairingMode === "qr" && qr && !session.connected) {
      const expiresAt = Date.now() + 60000;
      session.qrCode = qr;
      session.qrExpiresAt = expiresAt;
      session.connecting = false;
      console.log(`[WA][${userId}] QR generated gen=${myGenId}`);
      onQr(qr, expiresAt);
    }

    if (connection === "open") {
      console.log(`[WA][${userId}] Connected! gen=${myGenId}`);
      session.connected = true;
      session.wasConnected = true;
      session.connecting = false;
      session.qrCode = null;
      session.qrExpiresAt = null;
      session.retryCount = 0;
      savePairingMode(userId, pairingMode).catch(() => {});
      onConnected();
    }

    if (connection === "close") {
      session.connected = false;
      session.connecting = false;

      const boom = lastDisconnect?.error as Boom | undefined;
      const statusCode = boom?.output?.statusCode;
      const reason = boom?.message || "Unknown";

      console.log(`[WA][${userId}] Closed gen=${myGenId}. code=${statusCode} reason=${reason} wasConnected=${session.wasConnected} retries=${session.retryCount} pairingMode=${pairingMode}`);

      // When reconnecting an already-established session, never re-show QR and never
      // request a new pairing code. Baileys uses saved creds directly.
      // For QR sessions (phoneNumber empty): keep pairingMode="qr" so Baileys
      // doesn't try to request a pairing code with an empty phone number.
      // For code sessions: use "code" so Baileys handles the 515 stream restart.
      const hasPhone = phoneNumber.replace(/[^0-9]/g, "").length > 0;
      const reconnectMode: "code" | "qr" = (session.wasConnected && hasPhone) ? "code" : pairingMode;
      const reconnectOnQr = session.wasConnected ? () => {} : onQr;

      // ── QR session already-connected disconnect: limited retries ────────────
      // Phone/code sessions can silently reconnect using saved credentials.
      // QR sessions have NO phone number stored — but Baileys CAN still
      // restore the session using saved creds (creds.json) WITHOUT showing
      // a new QR code. This handles 30-minute keepalive drops, network blips,
      // and WhatsApp server-side evictions. We allow up to MAX_QR_RETRIES
      // attempts; if all fail the user gets one notification to re-scan.
      //
      // Exception: statusCode 515 ("Stream Errored") is the normal Baileys
      // post-scan restart — handled below.
      const isQrSession = !hasPhone;
      const MAX_QR_RETRIES = 5;
      if (isQrSession && session.wasConnected && statusCode !== 515 && !reason.includes("Stream Errored")) {
        if (session.retryCount >= MAX_QR_RETRIES) {
          console.log(`[WA][${userId}] QR session failed to reconnect after ${session.retryCount} tries — stopping. User must re-scan.`);
          closeSocketSafe(sock);
          session.socket = null;
          clearAllSessionTimers(userId);
          sessions.delete(userId);
          onDisconnected("Reconnect via QR code");
          void forceMemoryReclaim();
          return;
        }
        session.retryCount++;
        session.codeRequested = false;
        const delay = Math.min(5000 * session.retryCount, 30000); // 5s, 10s, 15s, 20s, 25s
        console.log(`[WA][${userId}] QR session disconnected (code=${statusCode}) — retry ${session.retryCount}/${MAX_QR_RETRIES} in ${delay / 1000}s`);
        closeSocketSafe(sock);
        session.socket = null;
        trackSessionTimer(userId, async () => {
          if (session.socketGenId !== myGenId) return;
          const currentSession = sessions.get(userId);
          if (!currentSession || currentSession !== session) return;
          try {
            await createSocket(userId, phoneNumber, "qr", onCode, reconnectOnQr, onConnected, onDisconnected, session);
          } catch (e: any) {
            clearAllSessionTimers(userId);
            sessions.delete(userId);
            onDisconnected(`Reconnect failed: ${e?.message}`);
          }
        }, delay);
        return;
      }

      // 401 / loggedOut — two very different scenarios share the same status code:
      //   A) Session that WAS connected gets 401 → often transient after QR scan or
      //      after a 515 stream restart.  Retry silently a few times before accepting
      //      it as a real logout (phone/code sessions only — QR sessions stopped above).
      //   B) Session that NEVER connected gets 401 → stale / invalid creds.  Clear
      //      immediately and let the user reconnect.
      if (statusCode === DisconnectReason.loggedOut) {
        closeSocketSafe(sock);
        session.socket = null;
        session.codeRequested = false;

        if (session.wasConnected && hasPhone && session.retryCount < 3) {
          // Transient 401 after successful connection (phone/code sessions only)
          session.retryCount++;
          const delay = 4000 * session.retryCount;
          console.log(`[WA][${userId}] 401 after live session gen=${myGenId} — retry ${session.retryCount}/3 in ${delay / 1000}s (mode=${reconnectMode})`);
          trackSessionTimer(userId, async () => {
            if (session.socketGenId !== myGenId) return;
            const currentSession = sessions.get(userId);
            if (!currentSession || currentSession !== session) return;
            try {
              await createSocket(userId, phoneNumber, reconnectMode, onCode, reconnectOnQr, onConnected, onDisconnected, session);
            } catch (e: any) {
              clearAllSessionTimers(userId);
              sessions.delete(userId);
              onDisconnected(`Reconnect failed: ${e?.message}`);
            }
          }, delay);
          return;
        }

        // Real logout confirmed (never connected, or retried enough times)
        console.log(`[WA][${userId}] Logout confirmed gen=${myGenId} (wasConnected=${session.wasConnected} retries=${session.retryCount})`);
        void clearSessionData(userId);
        clearAllSessionTimers(userId);
        sessions.delete(userId);
        onDisconnected("Logged out by WhatsApp. Please reconnect from the menu.");
        void forceMemoryReclaim();
        return;
      }

      // 515 = "Stream Errored (restart required)"
      // NORMAL after user successfully scans QR or enters pairing code.
      // WhatsApp saves credentials first, then sends 515.
      // DO NOT clear session files — creds must persist for reconnect.
      // For QR sessions: allow up to 3 retries (the post-scan 515 + 1-2 bounces).
      // For code sessions: allow up to 8 retries.
      if (statusCode === 515 || reason.includes("Stream Errored")) {
        console.log(`[WA][${userId}] 515 stream restart gen=${myGenId} — reconnecting in ${reconnectMode} mode (session preserved)`);
        closeSocketSafe(sock);
        session.socket = null;
        session.codeRequested = false;
        session.retryCount++;

        const max515Retries = isQrSession ? 3 : 8;
        if (session.retryCount > max515Retries) {
          clearAllSessionTimers(userId);
          sessions.delete(userId);
          void clearSessionData(userId);
          onDisconnected("Too many reconnect attempts. Please reconnect from the menu.");
          void forceMemoryReclaim();
          return;
        }

        const delay = session.wasConnected ? 3000 : 2000;
        trackSessionTimer(userId, async () => {
          if (session.socketGenId !== myGenId) return;
          const currentSession = sessions.get(userId);
          if (!currentSession || currentSession !== session) return;
          try {
            await createSocket(userId, phoneNumber, reconnectMode, onCode, reconnectOnQr, onConnected, onDisconnected, session);
          } catch (e: any) {
            clearAllSessionTimers(userId);
            sessions.delete(userId);
            onDisconnected(`Reconnect failed: ${e?.message}`);
          }
        }, delay);
        return;
      }

      // 403 = WhatsApp permanently rejected this session (banned, session invalidated, or device unlinked remotely)
      // Retrying endlessly on 403 causes memory leaks (500+ sockets accumulate). Cap retries tightly.
      // QR sessions get only 2 retries since they can't silently reconnect anyway.
      if (statusCode === 403) {
        session.retryCount++;
        const MAX_403_RETRIES = session.wasConnected ? (hasPhone ? 10 : 2) : 3;
        if (session.retryCount > MAX_403_RETRIES) {
          console.log(`[WA][${userId}] 403 repeated ${session.retryCount}x — WhatsApp permanently rejected. Stopping reconnect.`);
          closeSocketSafe(sock);
          session.socket = null;
          clearAllSessionTimers(userId);
          sessions.delete(userId);
          void clearSessionData(userId);
          onDisconnected("WhatsApp rejected this connection (403). Please reconnect from the menu.");
          void forceMemoryReclaim();
          return;
        }
        // Exponential backoff for 403: 10s, 20s, 40s... up to 2 min
        const delay = Math.min(10000 * Math.pow(2, session.retryCount - 1), 120000);
        console.log(`[WA][${userId}] 403 retry ${session.retryCount}/${MAX_403_RETRIES} in ${delay / 1000}s gen=${myGenId} mode=${reconnectMode}...`);
        session.codeRequested = false;
        trackSessionTimer(userId, async () => {
          if (session.socketGenId !== myGenId) return;
          const currentSession = sessions.get(userId);
          if (!currentSession || currentSession !== session) return;
          try {
            await createSocket(userId, phoneNumber, reconnectMode, onCode, reconnectOnQr, onConnected, onDisconnected, session);
          } catch (e: any) {
            clearAllSessionTimers(userId);
            sessions.delete(userId);
            onDisconnected(`Reconnect failed: ${e?.message}`);
          }
        }, delay);
        return;
      }

      // All other close reasons — reconnect after delay with a safety cap
      // QR sessions that reach here have wasConnected=false (still in QR scan phase).
      // Phone/code sessions: up to 50 retries with exponential backoff.
      const MAX_RETRIES = 50;
      session.codeRequested = false;
      session.retryCount++;
      if (session.retryCount > MAX_RETRIES) {
        console.log(`[WA][${userId}] Too many retries (${session.retryCount}) — stopping reconnect to prevent memory leak.`);
        closeSocketSafe(sock);
        session.socket = null;
        clearAllSessionTimers(userId);
        sessions.delete(userId);
        void clearSessionData(userId);
        onDisconnected("Connection keeps failing. Please reconnect from the menu.");
        void forceMemoryReclaim();
        return;
      }
      // Exponential backoff: 5s → 10s → 20s → max 2 min
      const reconnectDelay = Math.min(5000 * Math.pow(1.5, Math.floor(session.retryCount / 5)), 120000);
      console.log(`[WA][${userId}] Will reconnect in ${reconnectDelay / 1000}s gen=${myGenId} mode=${reconnectMode}...`);
      trackSessionTimer(userId, async () => {
        if (session.socketGenId !== myGenId) return;
        const currentSession = sessions.get(userId);
        if (!currentSession || currentSession !== session) return;
        try {
          await createSocket(userId, phoneNumber, reconnectMode, onCode, reconnectOnQr, onConnected, onDisconnected, session);
        } catch (e: any) {
          clearAllSessionTimers(userId);
          sessions.delete(userId);
          onDisconnected(`Reconnect failed: ${e?.message}`);
        }
      }, reconnectDelay);
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
  // Cancel any leftover timers from a previous failed attempt so they don't
  // fire after we've already started a fresh socket below.
  clearAllSessionTimers(userId);
  sessions.delete(userId);

  const session: WhatsAppSession = {
    socket: null,
    connected: false,
    pairingCode: null,
    qrCode: null,
    qrExpiresAt: null,
    pairingMode: "code",
    connecting: true,
    phoneNumber,
    codeRequested: false,
    wasConnected: false,
    retryCount: 0,
    socketGenId: 0,
    connectLock: true,
    lastActivityAt: Date.now(),
  };
  sessions.set(userId, session);

  try {
    // Make room before opening a new socket if we're already at the cap.
    if (sessions.size > MAX_LIVE_SESSIONS) sweepIdleSessions();
    await createSocket(userId, phoneNumber, "code", onCode, () => {}, onConnected, onDisconnected, session);
  } finally {
    session.connectLock = false;
  }
}

export async function connectWhatsAppQr(
  userId: string,
  onQr: (qr: string, expiresAt: number) => void,
  onConnected: () => void,
  onDisconnected: (reason: string) => void
): Promise<void> {
  const existing = sessions.get(userId);

  if (existing?.connectLock) {
    console.log(`[WA][${userId}] QR connection already in progress, ignoring duplicate`);
    return;
  }

  if (existing?.socket) {
    closeSocketSafe(existing.socket);
    existing.socket = null;
  }
  // Cancel any leftover timers from a previous failed attempt so they don't
  // step on the fresh QR socket we're about to create below.
  clearAllSessionTimers(userId);
  sessions.delete(userId);

  const session: WhatsAppSession = {
    socket: null,
    connected: false,
    pairingCode: null,
    qrCode: null,
    qrExpiresAt: null,
    pairingMode: "qr",
    connecting: true,
    phoneNumber: "",
    codeRequested: false,
    wasConnected: false,
    retryCount: 0,
    socketGenId: 0,
    connectLock: true,
    lastActivityAt: Date.now(),
  };
  sessions.set(userId, session);

  try {
    if (sessions.size > MAX_LIVE_SESSIONS) sweepIdleSessions();
    await createSocket(userId, "", "qr", () => {}, onQr, onConnected, onDisconnected, session);
  } finally {
    session.connectLock = false;
  }
}

export function getSession(userId: string): WhatsAppSession | undefined {
  const s = sessions.get(userId);
  if (s) s.lastActivityAt = Date.now();
  return s;
}

export function isConnected(userId: string): boolean {
  const s = sessions.get(userId);
  if (s) s.lastActivityAt = Date.now();
  return s?.connected === true && s?.socket !== null;
}

export async function sendSocketPresence(userId: string): Promise<void> {
  const s = sessions.get(userId);
  if (!s?.socket || !s.connected) return;
  try {
    await s.socket.sendPresenceUpdate("available");
  } catch {}
}

export function getConnectedWhatsAppNumber(userId: string): string | null {
  const s = sessions.get(userId);
  if (!s?.connected || !s.socket) return null;

  const savedDigits = s.phoneNumber.replace(/[^0-9]/g, "");
  if (savedDigits.length >= 7) return `+${savedDigits}`;

  const socketUserId = s.socket.user?.id || "";
  const socketDigits = socketUserId.split(":")[0].split("@")[0].replace(/[^0-9]/g, "");
  return socketDigits.length >= 7 ? `+${socketDigits}` : null;
}

// Optional callback that gets invoked whenever a session disconnects.
// Used to push English-language Telegram alerts to the user with their phone number.
let disconnectNotifier: ((userId: string, reason: string, phoneNumber: string | null) => void) | null = null;

export function setDisconnectNotifier(fn: (userId: string, reason: string, phoneNumber: string | null) => void): void {
  disconnectNotifier = fn;
}

// Cooldown: send at most ONE disconnect notification per user per 5 minutes.
// Without this, every reconnect retry (which can happen every ~60s) fires
// a fresh Telegram message, flooding the user's chat.
const lastDisconnectNotifyAt = new Map<string, number>();
const DISCONNECT_NOTIFY_COOLDOWN_MS = 5 * 60 * 1000; // 5 minutes

export function notifyDisconnect(userId: string, reason: string): void {
  try {
    const now = Date.now();
    const last = lastDisconnectNotifyAt.get(userId) ?? 0;
    if (now - last < DISCONNECT_NOTIFY_COOLDOWN_MS) return; // already notified recently — skip
    lastDisconnectNotifyAt.set(userId, now);
    const phone = getConnectedWhatsAppNumber(userId) ?? sessions.get(userId)?.phoneNumber ?? null;
    disconnectNotifier?.(userId, reason, phone);
  } catch {}
}

export async function restoreWhatsAppSessions(): Promise<void> {
  const storedSessions = await listStoredWhatsAppSessions();
  console.log(`[WA][RESTORE] Found ${storedSessions.length} saved WhatsApp session(s)`);

  // On small hosts (Render free tier = 512MB) we can't keep more than ~6
  // Baileys sockets in memory at once. Restore at most that many up front;
  // the rest will be lazy-restored on demand when the user interacts with
  // the bot (see ensureSessionLoaded). All session creds stay in MongoDB —
  // lazy restore reuses them, no re-pairing needed.
  const restoreLimit = Math.min(storedSessions.length, MAX_LIVE_SESSIONS);
  if (storedSessions.length > restoreLimit) {
    console.log(
      `[WA][RESTORE] Restoring first ${restoreLimit} of ${storedSessions.length} sessions on startup; ` +
      `rest will lazy-restore on demand to keep memory under ${MEMORY_PRESSURE_RSS_MB}MB.`
    );
  }

  for (let _i = 0; _i < restoreLimit; _i++) {
    const stored = storedSessions[_i];
    if (sessions.has(stored.userId)) continue;
    if (_i > 0) {
      // Memory optimization: stagger restores so sockets don't all open at
      // the same instant — 5s gives the previous one time to settle.
      await new Promise((r) => setTimeout(r, 5000));
    }

    const storedMode = stored.pairingMode || "code";
    const session: WhatsAppSession = {
      socket: null,
      connected: false,
      pairingCode: null,
      qrCode: null,
      qrExpiresAt: null,
      pairingMode: storedMode,
      connecting: true,
      phoneNumber: stored.phoneNumber,
      codeRequested: false,
      wasConnected: true,
      retryCount: 0,
      socketGenId: 0,
      connectLock: true,
      lastActivityAt: Date.now(),
    };

    sessions.set(stored.userId, session);
    try {
      await createSocket(
        stored.userId,
        stored.phoneNumber,
        storedMode,
        () => {},
        () => {},
        () => console.log(`[WA][RESTORE][${stored.userId}] Session restored`),
        (reason) => {
          console.log(`[WA][RESTORE][${stored.userId}] Restore disconnected: ${reason}`);
          notifyDisconnect(stored.userId, reason);
        },
        session
      );
    } catch (err: any) {
      console.error(`[WA][RESTORE][${stored.userId}] Failed:`, err?.message);
      sessions.delete(stored.userId);
    } finally {
      session.connectLock = false;
    }
  }
}

// Reconnect the existing WhatsApp socket WITHOUT clearing saved credentials.
// Used by the "Session Refresh" feature to force baileys to re-sync the latest
// groups/admin state from WhatsApp servers (helps when the bot was made admin
// in a new group but doesn't see it yet).
export async function refreshWhatsAppSession(
  userId: string,
  onConnected: () => void,
  onError: (reason: string) => void
): Promise<void> {
  const existing = sessions.get(userId);
  if (!existing) {
    onError("No active session found. Please connect WhatsApp first.");
    return;
  }
  if (existing.connectLock) {
    onError("Another connect/refresh is already in progress. Please wait.");
    return;
  }

  const { state } = await useMongoDBAuthState(userId);
  if (!state.creds.registered && !state.creds.me?.id) {
    onError("Saved credentials are missing — please use Connect WhatsApp instead.");
    return;
  }

  // Close current socket but keep the in-memory session shell so other callers
  // see "connecting" state during the refresh.
  if (existing.socket) {
    closeSocketSafe(existing.socket);
    existing.socket = null;
  }
  // Cancel any pending reconnect/pairing timers from the previous socket so
  // they don't fire and step on the fresh socket we're about to create.
  clearAllSessionTimers(userId);
  existing.connected = false;
  existing.connecting = true;
  existing.connectLock = true;
  existing.retryCount = 0;
  existing.wasConnected = true;

  const phoneNumber = existing.phoneNumber || "";

  try {
    await createSocket(
      userId,
      phoneNumber,
      "code",
      () => {}, // no pairing code expected for already-registered session
      () => {}, // no QR
      () => {
        try { onConnected(); } catch {}
      },
      (reason) => {
        try { onError(reason); } catch {}
      },
      existing,
    );
  } catch (err: any) {
    onError(err?.message || "Refresh failed");
  } finally {
    existing.connectLock = false;
  }
}

export async function disconnectWhatsApp(userId: string): Promise<void> {
  const session = useSession(userId);
  // Cancel pending reconnect/pairing timers BEFORE doing anything else so
  // the closures (which retain the socket + auth state) become unreachable
  // and GC-able as soon as we drop the session entry.
  const clearedTimers = clearAllSessionTimers(userId);
  if (session) {
    session.socketGenId = -1;
    if (session.socket) {
      try { await session.socket.logout(); } catch {}
      closeSocketSafe(session.socket);
      session.socket = null;
    }
    session.connected = false;
    session.connecting = false;
  }
  sessions.delete(userId);
  // Fire-and-forget Mongo cleanup — don't block the user's UI on it.
  void clearSessionData(userId);
  console.log(`[WA][DISCONNECT][${userId}] Session removed (cleared ${clearedTimers} timers)`);
  // Force V8 to actually reclaim the socket + creds + signal-key cache so
  // RSS drops right after disconnect, not 30+ minutes later when the next
  // sweep runs. Without this the user observes "WhatsApp disconnected but
  // RAM is still high" — which is exactly the bug being fixed.
  void forceMemoryReclaim();
}

// Memory-only disconnect for the idle timer.
// Closes the live Baileys socket so RAM is freed, but does NOT call
// socket.logout() (which would unlink the device on WhatsApp servers)
// and does NOT clear MongoDB creds. The next user interaction goes
// through ensureSessionLoaded(), which silently restores the socket
// from the saved Mongo creds — no re-pairing needed.
export async function idleDisconnectWhatsApp(userId: string): Promise<void> {
  const session = useSession(userId);
  if (!session) return;
  session.socketGenId = -1;
  evictSessionSocket(userId, session);
}

export interface GroupResult {
  id: string;
  inviteCode: string;
  addedParticipants?: number;
  participantsFailed?: boolean;
}

export async function createWhatsAppGroup(
  userId: string,
  groupName: string,
  participants: string[] = []
): Promise<GroupResult | null> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return null;

  // Clean numbers (digits only) and build raw JIDs.
  const cleanedNumbers = participants
    .map(p => p.includes("@") ? p.split("@")[0] : p.replace(/[^0-9]/g, ""))
    .filter(n => n.length >= 10);
  const rawJids = cleanedNumbers.map(n => `${n}@s.whatsapp.net`);

  // ── Validate numbers via onWhatsApp BEFORE creating ──
  // Why: groupCreate(name, [bad_jid, ...]) throws an error and rejects the
  // whole creation. With 2–3 friend numbers, even one invalid/non-WA number
  // would cascade into 3 failed retries, triggering WhatsApp rate-limits and
  // also breaking the fallback empty-group attempt. Validating first means we
  // only pass JIDs that we know exist on WhatsApp, dramatically reducing the
  // chance of failure on small batches.
  let validJids: string[] = rawJids;
  if (cleanedNumbers.length > 0) {
    try {
      const checkResults = await (session.socket as any).onWhatsApp(...cleanedNumbers);
      if (Array.isArray(checkResults)) {
        const verifiedJids = checkResults
          .filter((r: any) => r && r.exists && r.jid)
          .map((r: any) => String(r.jid));
        if (verifiedJids.length > 0) {
          validJids = verifiedJids;
        } else {
          // None of the supplied numbers exist on WhatsApp — create empty.
          validJids = [];
        }
        console.log(`[WA][${userId}] onWhatsApp check: ${verifiedJids.length}/${cleanedNumbers.length} valid`);
      }
    } catch (err: any) {
      console.error(`[WA][${userId}] onWhatsApp check failed, using raw jids:`, err?.message);
      // Fall back to using raw jids — better to try than abort.
    }
  }

  async function tryCreate(participantList: string[], maxAttempts = 1): Promise<{ id: string } | null> {
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        const group = await session.socket!.groupCreate(groupName, participantList);
        return { id: group.id };
      } catch (err: any) {
        console.error(`[WA][${userId}] groupCreate attempt ${attempt}/${maxAttempts} (with ${participantList.length} participants) failed:`, err?.message);
        if (attempt < maxAttempts) {
          await new Promise(r => setTimeout(r, attempt * 2500));
        }
      }
    }
    return null;
  }

  let groupId: string | null = null;
  let participantsFailed = false;
  let addedAtCreation = 0;

  // Step 1: Try ONCE with the validated participants (no retry — failure here
  // usually means a bad participant, retrying with the same list won't help
  // and just burns rate-limit budget).
  if (validJids.length > 0) {
    const res = await tryCreate(validJids, 1);
    if (res) {
      groupId = res.id;
      addedAtCreation = validJids.length;
    }
  }

  // Step 2: If we don't have a group yet (either no valid jids, or creation
  // with participants failed), create an empty group with up to 3 attempts.
  if (!groupId) {
    if (validJids.length > 0 || rawJids.length > 0) {
      // Brief cool-off after a failed create attempt to avoid rate-limit.
      await new Promise(r => setTimeout(r, 2500));
    }
    const res2 = await tryCreate([], 3);
    if (res2) {
      groupId = res2.id;
      if (rawJids.length > 0) participantsFailed = true;
    }
  }

  if (!groupId) return null;

  try {
    const inviteCode = await session.socket.groupInviteCode(groupId);
    return {
      id: groupId,
      inviteCode: `https://chat.whatsapp.com/${inviteCode}`,
      addedParticipants: addedAtCreation,
      participantsFailed,
    };
  } catch (err: any) {
    console.error(`[WA][${userId}] groupInviteCode error:`, err?.message);
    return {
      id: groupId,
      inviteCode: "",
      addedParticipants: addedAtCreation,
      participantsFailed,
    };
  }
}

export interface GroupPermissions {
  editGroupInfo: boolean;
  sendMessages: boolean;
  addMembers: boolean;
  approveJoin: boolean;
}

// Rename a single group. Returns ok:true on success or ok:false with a
// human-readable error string. Used by the "Change Group Name" feature.
export async function setGroupName(
  userId: string,
  groupId: string,
  newName: string
): Promise<{ ok: boolean; error?: string }> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) {
    return { ok: false, error: "WhatsApp not connected" };
  }
  const trimmed = (newName || "").trim();
  if (!trimmed) return { ok: false, error: "Empty name" };
  // WhatsApp's hard limit on group subject length is 25 characters in the
  // official client. The server actually accepts a bit more, but to keep
  // results predictable we cap here and surface a clean error instead of
  // silently failing later.
  if (trimmed.length > 100) return { ok: false, error: "Name too long (max 100 chars)" };

  // WhatsApp throttles group-subject updates per-account. When we batch-rename
  // many groups back to back, the server returns "rate-overlimit" for any
  // request that exceeded the throttle window. Instead of giving up on the
  // first failure, transparently retry a few times with exponential backoff —
  // the throttle window is short (a few seconds), so a brief pause almost
  // always succeeds. This is invisible to the caller; only persistent
  // failures bubble up.
  const RETRY_DELAYS_MS = [4000, 10000, 25000]; // 4s, 10s, 25s
  let lastErr = "Unknown error";
  for (let attempt = 0; attempt <= RETRY_DELAYS_MS.length; attempt++) {
    try {
      await session.socket.groupUpdateSubject(groupId, trimmed);
      return { ok: true };
    } catch (e: any) {
      lastErr = e?.message || e?.data || "Unknown error";
      const isRateLimit =
        typeof lastErr === "string" &&
        (lastErr.includes("rate-overlimit") ||
          lastErr.includes("rate-limit") ||
          lastErr.includes("overlimit"));
      // Only retry rate-limit errors. Other errors (not-authorized, bad-request,
      // forbidden, etc.) are permanent and should fail fast so the user sees
      // the real reason.
      if (!isRateLimit || attempt === RETRY_DELAYS_MS.length) {
        return { ok: false, error: lastErr };
      }
      const waitMs = RETRY_DELAYS_MS[attempt];
      console.log(`[WA][${userId}] setGroupName rate-limit on ${groupId}, retrying in ${waitMs}ms (attempt ${attempt + 1}/${RETRY_DELAYS_MS.length})`);
      await new Promise((r) => setTimeout(r, waitMs));
      // Re-check session is still alive before retrying.
      if (!session.connected) {
        return { ok: false, error: "WhatsApp disconnected during retry" };
      }
    }
  }
  return { ok: false, error: lastErr };
}

export async function applyGroupSettings(
  userId: string,
  groupId: string,
  perms: GroupPermissions,
  description: string
): Promise<void> {
  const session = useSession(userId);
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

export async function setGroupDisappearingMessages(
  userId: string,
  groupId: string,
  duration: number
): Promise<boolean> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return false;
  try {
    await session.socket.groupToggleEphemeral(groupId, duration);
    return true;
  } catch (err: any) {
    console.error(`[WA][${userId}] setGroupDisappearingMessages error:`, err?.message);
    return false;
  }
}

export async function setGroupIcon(
  userId: string,
  groupId: string,
  imageBuffer: Buffer
): Promise<boolean> {
  const session = useSession(userId);
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
  const session = useSession(userId);
  if (!session?.socket || !session.connected) {
    return { success: false, error: "WhatsApp not connected" };
  }
  const MAX_CONFLICT_RETRIES = 3;
  for (let attempt = 1; attempt <= MAX_CONFLICT_RETRIES; attempt++) {
    try {
      const code = extractInviteCode(link);
      const result = await session.socket.groupAcceptInvite(code);
      let groupName = "Group";
      if (typeof result === "string" && result) {
        try {
          const metadata = await session.socket.groupMetadata(result);
          groupName = metadata?.subject || result;
        } catch {
          groupName = result;
        }
      }
      return { success: true, groupName };
    } catch (err: any) {
      const msg = (err?.message || "").toLowerCase();
      console.error(`[WA][${userId}] Join group error (attempt ${attempt}):`, err?.message);
      if (msg.includes("conflict") && attempt < MAX_CONFLICT_RETRIES) {
        await new Promise((r) => setTimeout(r, 3000 * attempt));
        if (!session.socket || !session.connected) {
          return { success: false, error: "WhatsApp not connected" };
        }
        continue;
      }
      if (msg.includes("conflict")) {
        return { success: false, error: "Server busy — please wait a moment and try again" };
      }
      return { success: false, error: err?.message || "Failed to join" };
    }
  }
  return { success: false, error: "Failed to join after retries" };
}

export async function resetGroupInviteLink(
  userId: string,
  groupId: string
): Promise<{ success: boolean; newLink?: string; error?: string }> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) {
    return { success: false, error: "WhatsApp not connected" };
  }
  try {
    await session.socket.groupRevokeInvite(groupId);
    await new Promise((r) => setTimeout(r, 1200));
    if (!session.socket || !session.connected) {
      return { success: false, error: "Disconnected during reset" };
    }
    const newCode = await session.socket.groupInviteCode(groupId);
    if (!newCode) return { success: false, error: "Could not fetch new link" };
    return { success: true, newLink: `https://chat.whatsapp.com/${newCode}` };
  } catch (err: any) {
    console.error(`[WA][${userId}] Reset invite link error:`, err?.message);
    return { success: false, error: err?.message || "Failed to reset link" };
  }
}

function extractNumber(jid: string): string {
  return jid.replace(/:\d+@/, "@").replace(/@.*$/, "").replace(/[^0-9]/g, "");
}

export async function getGroupPendingRequests(
  userId: string,
  groupId: string
): Promise<string[]> {
  const session = useSession(userId);
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
  const session = useSession(userId);
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

// Returns JIDs of pending participants who should be auto-approved by the
// Auto Request Accepter — i.e. anyone who joined via the GROUP INVITE LINK.
//
// WhatsApp's pending list (groupRequestParticipantsList) is a flat list of
// XML attrs from the `<membership_approval_request>` node. In Baileys
// 7.0.0-rc.9 the relevant attribute is `request_method`, typed as:
//
//   'invite_link' | 'linked_group_join' | 'non_admin_add' | undefined
//
// Behaviour we want:
//   • request_method === "invite_link"      → ACCEPT (user clicked the link)
//   • request_method === "non_admin_add"    → REJECT (an existing member
//                                              tried to add this user — admin
//                                              must review manually)
//   • request_method === "linked_group_join"→ REJECT (community / parent
//                                              group join, not the link)
//   • request_method missing / undefined    → ACCEPT (the WhatsApp server
//                                              does not always send this
//                                              attribute; the overwhelmingly
//                                              common case for a missing
//                                              method is an invite-link join,
//                                              so we default to accept rather
//                                              than silently dropping valid
//                                              joiners — which was the
//                                              "only one user gets accepted"
//                                              bug)
export async function getGroupPendingInviteLinkJoins(
  userId: string,
  groupId: string
): Promise<string[]> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return [];
  try {
    const requests = await session.socket.groupRequestParticipantsList(groupId);

    if (requests.length > 0) {
      console.log(
        `[WA][${userId}] getGroupPendingInviteLinkJoins raw count: ${requests.length}, sample:`,
        JSON.stringify(requests[0])
      );
    }

    const inviteLinkRequests = requests.filter((r: any) => {
      const rawMethod =
        r.request_method ??
        r.requestMethod ??
        r.method ??
        (r.add_request && (r.add_request.method || r.add_request.request_method));
      const method = rawMethod ? String(rawMethod).toLowerCase() : "";

      // REJECT only the methods we explicitly know are NOT invite-link joins.
      if (
        method === "non_admin_add" ||
        method === "nonadminadd" ||
        method === "added_by_member" ||
        method === "linked_group_join" ||
        method === "linkedgroupjoin"
      ) {
        return false;
      }

      // Everything else (invite_link, missing/undefined method, or a value
      // we have not seen before) is treated as an invite-link join and
      // approved. Missing method is common and should NOT block approval.
      return true;
    });

    console.log(
      `[WA][${userId}] getGroupPendingInviteLinkJoins filtered: ${inviteLinkRequests.length}/${requests.length} accepted as invite-link joins`
    );

    return inviteLinkRequests
      .map((r: any) => {
        const jid: string = r.jid || "";
        return jid.replace(/:\d+@/, "@");
      })
      .filter(Boolean);
  } catch (err: any) {
    console.error(`[WA][${userId}] getGroupPendingInviteLinkJoins error:`, err?.message);
    return [];
  }
}

// Returns pending requests with both raw JID (for approval calls) and resolved phone
// (for matching against user-supplied numbers). Handles both @s.whatsapp.net and @lid JIDs.
export async function getGroupPendingRequestsDetailed(
  userId: string,
  groupId: string
): Promise<Array<{ jid: string; phone: string }>> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return [];
  try {
    const requests = await session.socket.groupRequestParticipantsList(groupId);
    if (requests.length > 0) {
      console.log(`[WA][${userId}] getGroupPendingRequestsDetailed first raw:`, JSON.stringify(requests[0]));
    }
    return requests
      .map((r: any) => {
        const rawJid: string = r.jid || "";
        const jid = rawJid.replace(/:\d+@/, "@");
        let phone = "";
        // For @lid jids the local part is NOT a phone number — try the phoneNumber field instead.
        if (jid.endsWith("@lid")) {
          const phoneJid: string = r.phoneNumber || r.phone_number || r.lidPhoneNumber || "";
          phone = extractPhoneFromJid(phoneJid);
        } else {
          phone = extractPhoneFromJid(jid);
        }
        return { jid, phone };
      })
      .filter((x: any) => !!x.jid);
  } catch (err: any) {
    console.error(`[WA][${userId}] getGroupPendingRequestsDetailed error:`, err?.message);
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
  const session = useSession(userId);
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
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return null;
  const code = extractInviteCode(inviteLink);
  let lastErr: any = null;
  for (let attempt = 0; attempt < 3; attempt++) {
    if (attempt > 0) {
      // Back off between retries: 3s, 6s
      await new Promise((r) => setTimeout(r, 3000 * attempt));
    }
    try {
      const metadata = await session.socket.groupGetInviteInfo(code);
      return { id: metadata.id, subject: metadata.subject };
    } catch (err: any) {
      lastErr = err;
      console.error(`[WA][${userId}] Group info error (attempt ${attempt + 1}/3):`, err?.message);
    }
  }
  return null;
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
  const session = useSession(userId);
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

export async function getGroupInviteLink(userId: string, groupId: string, maxRetries: number = 5): Promise<string | null> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return null;

  // Exponential backoff: 1.5s, 3s, 5s, 8s, 12s. Total worst-case ~30s for 5
  // attempts. This is much more forgiving than the old fixed 1.5s × 3 (which
  // gave up after ~5s and silently dropped 2-3 groups out of every 10 when WA
  // throttled or stream-replied late).
  const backoffSchedule = [1500, 3000, 5000, 8000, 12000];

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const code = await session.socket.groupInviteCode(groupId);
      if (code) return `https://chat.whatsapp.com/${code}`;
    } catch (err: any) {
      console.error(`[WA][${userId}] Get invite link error for ${groupId} (attempt ${attempt}/${maxRetries}):`, err?.message ?? err?.data);
      if (isFatalInviteError(err)) return null;
    }

    // Metadata fallback only on later attempts — it's expensive (full fetch)
    // and on the first try we want to be fast. By attempt 3 we want every
    // possible recovery path active.
    if (attempt >= 3) {
      try {
        const meta = await session.socket!.groupMetadata(groupId);
        if (meta && (meta as any).inviteCode) {
          return `https://chat.whatsapp.com/${(meta as any).inviteCode}`;
        }
      } catch (err2: any) {
        console.error(`[WA][${userId}] groupMetadata fallback error for ${groupId} (attempt ${attempt}/${maxRetries}):`, err2?.message ?? err2?.data);
        if (isFatalInviteError(err2)) return null;
      }
    }

    if (attempt < maxRetries) {
      const delay = backoffSchedule[Math.min(attempt - 1, backoffSchedule.length - 1)];
      console.log(`[WA][${userId}] Retrying getGroupInviteLink for ${groupId} in ${delay}ms (attempt ${attempt + 1}/${maxRetries})...`);
      await new Promise((r) => setTimeout(r, delay));
      if (!session.socket || !session.connected) return null;
    }
  }

  return null;
}

export async function leaveGroup(userId: string, groupId: string): Promise<boolean> {
  const session = useSession(userId);
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
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return [];
  try {
    const meta = await session.socket.groupMetadata(groupId);
    return meta.participants.map((p: any) => {
      const jidRaw: string = p.id || "";
      let phone: string;
      // Operation JID: for @lid participants WhatsApp requires the phone-based
      // JID (e.g. 919234038011@s.whatsapp.net) for participant update calls
      // (demote/promote/remove). Using the raw @lid JID causes silent failures.
      let operationJid: string;
      if (jidRaw.endsWith("@lid")) {
        // LID mode: p.phoneNumber holds the real phone JID e.g. "919234038011@s.whatsapp.net"
        const phoneJid: string = (p as any).phoneNumber || "";
        phone = extractPhoneFromJid(phoneJid) || extractPhoneFromJid(jidRaw);
        operationJid = phoneJid || jidRaw;
      } else {
        phone = extractPhoneFromJid(jidRaw) || "";
        operationJid = jidRaw;
      }
      return {
        jid: operationJid,
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
  const session = useSession(userId);
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
  const session = useSession(userId);
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
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return false;
  try {
    await session.socket.groupParticipantsUpdate(groupId, [participantJid], "promote");
    return true;
  } catch (err: any) {
    console.error(`[WA][${userId}] Make admin error:`, err?.message);
    return false;
  }
}

export async function demoteGroupAdmin(
  userId: string,
  groupId: string,
  participantJid: string
): Promise<{ success: boolean; error?: string }> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return { success: false, error: "Not connected" };
  try {
    await session.socket.groupParticipantsUpdate(groupId, [participantJid], "demote");
    return { success: true };
  } catch (err: any) {
    const msg: string = err?.message || String(err) || "Unknown error";
    console.error(`[WA][${userId}] Demote admin error:`, msg);
    return { success: false, error: msg };
  }
}

export async function approveGroupParticipant(
  userId: string,
  groupId: string,
  participantJid: string
): Promise<boolean> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return false;
  try {
    await session.socket.groupRequestParticipantsUpdate(groupId, [participantJid], "approve");
    return true;
  } catch (err: any) {
    console.error(`[WA][${userId}] Approve participant error:`, err?.message);
    return false;
  }
}

// Reject a single pending join request. Used by the CTC "Fix Wrong Pending"
// flow to cancel join requests that are NOT in the user's VCF.
export async function rejectGroupParticipant(
  userId: string,
  groupId: string,
  participantJid: string
): Promise<boolean> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return false;
  try {
    await session.socket.groupRequestParticipantsUpdate(groupId, [participantJid], "reject");
    return true;
  } catch (err: any) {
    console.error(`[WA][${userId}] Reject participant error:`, err?.message);
    return false;
  }
}

// Reject many pending join requests in one call (more efficient than looping
// rejectGroupParticipant). Returns the number of JIDs WhatsApp accepted as
// rejected.
export async function rejectGroupParticipantsBulk(
  userId: string,
  groupId: string,
  participantJids: string[]
): Promise<number> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return 0;
  if (!participantJids.length) return 0;
  try {
    const result = await session.socket.groupRequestParticipantsUpdate(
      groupId,
      participantJids,
      "reject"
    );
    if (Array.isArray(result)) {
      // Baileys returns an array of { jid, status } items — count successes.
      let ok = 0;
      for (const r of result as any[]) {
        const s = String(r?.status || "").toLowerCase();
        if (s === "200" || s === "success" || s === "") ok++;
      }
      return ok || participantJids.length;
    }
    return participantJids.length;
  } catch (err: any) {
    console.error(`[WA][${userId}] Bulk reject error:`, err?.message);
    return 0;
  }
}

export async function setGroupApprovalMode(
  userId: string,
  groupId: string,
  mode: "on" | "off"
): Promise<boolean> {
  const session = useSession(userId);
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

export async function addGroupParticipant(
  userId: string,
  groupId: string,
  phoneNumber: string
): Promise<{ success: boolean; error?: string }> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) {
    return { success: false, error: "WhatsApp not connected" };
  }
  try {
    const cleaned = phoneNumber.replace(/[^0-9]/g, "");
    const jid = `${cleaned}@s.whatsapp.net`;
    const result = await session.socket.groupParticipantsUpdate(groupId, [jid], "add");
    const status = Array.isArray(result) && result.length > 0 ? (result[0] as any) : null;
    if (status) {
      const statusCode = status.status || status.content?.attrs?.type || "";
      const statusStr = String(statusCode).toLowerCase();
      if (statusStr === "200" || statusStr === "success") {
        return { success: true };
      }
      if (statusStr === "409" || statusStr.includes("conflict") || statusStr.includes("exist")) {
        return { success: false, error: "Already in group" };
      }
      if (statusStr === "403" || statusStr.includes("invite") || statusStr.includes("not-authorized")) {
        return { success: false, error: "Invite required / Cannot add" };
      }
      if (statusStr === "408" || statusStr.includes("recently")) {
        return { success: false, error: "Recently left, cannot add now" };
      }
      if (statusStr === "404" || statusStr.includes("not-exist") || statusStr.includes("not on whatsapp")) {
        return { success: false, error: "Not on WhatsApp" };
      }
      return { success: false, error: `Status: ${statusStr}` };
    }
    return { success: true };
  } catch (err: any) {
    const msg = err?.message || String(err);
    console.error(`[WA][${userId}] Add participant error:`, msg);
    if (msg.includes("invite") || msg.includes("not-authorized") || msg.includes("403")) {
      return { success: false, error: "Invite required / Cannot add" };
    }
    return { success: false, error: msg };
  }
}

export async function addGroupParticipantsBulk(
  userId: string,
  groupId: string,
  phoneNumbers: string[]
): Promise<Array<{ phone: string; success: boolean; error?: string }>> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) {
    return phoneNumbers.map(p => ({ phone: p, success: false, error: "WhatsApp not connected" }));
  }

  const cleanedNumbers = phoneNumbers.map(p => p.replace(/[^0-9]/g, "")).filter(p => p.length >= 10);
  if (cleanedNumbers.length === 0) {
    return phoneNumbers.map(p => ({ phone: p, success: false, error: "Invalid number" }));
  }
  const jids = cleanedNumbers.map(p => `${p}@s.whatsapp.net`);

  // ── Pre-validate via onWhatsApp to filter out unreachable numbers ──
  // Same reason as createWhatsAppGroup: a single bad jid in the bulk call can
  // cause the whole groupParticipantsUpdate to throw, marking ALL numbers as
  // failed — even the valid ones.
  let toAdd = cleanedNumbers.map((n, i) => ({ phone: n, jid: jids[i] }));
  const skipped: Array<{ phone: string; reason: string }> = [];
  try {
    const checkResults = await (session.socket as any).onWhatsApp(...cleanedNumbers);
    if (Array.isArray(checkResults)) {
      const validJidSet = new Set(
        checkResults.filter((r: any) => r && r.exists && r.jid).map((r: any) => String(r.jid))
      );
      const next: typeof toAdd = [];
      for (const item of toAdd) {
        if (validJidSet.has(item.jid)) {
          next.push(item);
        } else {
          skipped.push({ phone: item.phone, reason: "Not on WhatsApp" });
        }
      }
      if (next.length > 0) toAdd = next;
      console.log(`[WA][${userId}] addBulk onWhatsApp: ${next.length}/${cleanedNumbers.length} valid, ${skipped.length} skipped`);
    }
  } catch (err: any) {
    console.error(`[WA][${userId}] addBulk onWhatsApp check failed, proceeding:`, err?.message);
  }

  function classify(statusStr: string): { success: boolean; error?: string } {
    const s = statusStr.toLowerCase();
    if (s === "200" || s === "success") return { success: true };
    if (s === "403" || s.includes("invite") || s.includes("not-authorized")) {
      return { success: false, error: "Invite required" };
    }
    if (s === "409" || s.includes("exist") || s.includes("conflict")) {
      return { success: false, error: "Already in group" };
    }
    if (s === "404" || s.includes("not-exist") || s.includes("not on whatsapp")) {
      return { success: false, error: "Not on WhatsApp" };
    }
    return { success: false, error: `Status: ${s || "unknown"}` };
  }

  // ── Try bulk update first ──
  let bulkResult: any = null;
  let bulkThrew = false;
  try {
    bulkResult = await session.socket.groupParticipantsUpdate(groupId, toAdd.map(i => i.jid), "add");
  } catch (err: any) {
    console.error(`[WA][${userId}] groupParticipantsUpdate bulk threw:`, err?.message);
    bulkThrew = true;
  }

  const finalResults: Array<{ phone: string; success: boolean; error?: string }> = [];

  if (!bulkThrew) {
    for (let i = 0; i < toAdd.length; i++) {
      const status = Array.isArray(bulkResult) && bulkResult[i] ? (bulkResult[i] as any) : null;
      if (status) {
        const statusCode = status.status || status.content?.attrs?.type || "";
        finalResults.push({ phone: toAdd[i].phone, ...classify(String(statusCode)) });
      } else {
        // No status entry -> assume success
        finalResults.push({ phone: toAdd[i].phone, success: true });
      }
    }
  } else {
    // ── Fallback: bulk threw, so try one-by-one to isolate the bad jids ──
    // This rescues the valid numbers when one bad number poisoned the bulk.
    for (const item of toAdd) {
      try {
        const r = await session.socket.groupParticipantsUpdate(groupId, [item.jid], "add");
        const status = Array.isArray(r) && r.length > 0 ? (r[0] as any) : null;
        if (status) {
          const statusCode = status.status || status.content?.attrs?.type || "";
          finalResults.push({ phone: item.phone, ...classify(String(statusCode)) });
        } else {
          finalResults.push({ phone: item.phone, success: true });
        }
      } catch (err: any) {
        const msg = err?.message || "Failed";
        console.error(`[WA][${userId}] single add failed for ${item.phone}:`, msg);
        finalResults.push({ phone: item.phone, success: false, error: msg });
      }
      await new Promise(r => setTimeout(r, 1200));
    }
  }

  // Append skipped numbers (filtered out by onWhatsApp check)
  for (const sk of skipped) {
    finalResults.push({ phone: sk.phone, success: false, error: sk.reason });
  }
  return finalResults;
}

export async function isUserInGroup(
  userId: string,
  groupId: string
): Promise<boolean> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return false;
  try {
    const groups = await session.socket.groupFetchAllParticipating();
    return groupId in groups;
  } catch {
    return false;
  }
}

export async function sendGroupMessage(
  userId: string,
  groupId: string,
  text: string
): Promise<boolean> {
  const session = useSession(userId);
  if (!session?.socket || !session.connected) return false;
  try {
    await session.socket.sendMessage(groupId, { text });
    return true;
  } catch (err: any) {
    console.error(`[WA][${userId}] sendGroupMessage error:`, err?.message);
    return false;
  }
}

export function getAutoUserId(userId: string): string {
  return `${userId}_auto`;
}

export function isAutoConnected(userId: string): boolean {
  return isConnected(getAutoUserId(userId));
}

export function getAutoConnectedNumber(userId: string): string | null {
  return getConnectedWhatsAppNumber(getAutoUserId(userId));
}

// Returns all currently active session user IDs (used by cleanup to avoid deleting live sessions)
export function getActiveSessionUserIds(): Set<string> {
  return new Set(sessions.keys());
}

// Check whether a user has WhatsApp credentials saved in MongoDB. Used by
// /start to decide whether to show the "connecting WhatsApp" progress bar.
// Returns false on any DB error so we never block the menu on transient issues.
export async function hasStoredWhatsAppSession(userId: string): Promise<boolean> {
  try {
    const all = await listStoredWhatsAppSessions();
    return all.some((s) => s.userId === userId);
  } catch {
    return false;
  }
}

// Returns true once the user's WhatsApp socket is fully connected, polling at
// `pollMs` intervals up to `timeoutMs`. Used to drive the live progress bar.
// If the session isn't already loading, this triggers ensureSessionLoaded().
export async function waitForWhatsAppConnected(
  userId: string,
  opts: { timeoutMs?: number; pollMs?: number } = {}
): Promise<boolean> {
  const timeoutMs = opts.timeoutMs ?? 30000;
  const pollMs = opts.pollMs ?? 500;
  if (isConnected(userId)) return true;
  // Kick off lazy restore in background — safe to call even if already loading.
  const restorePromise = ensureSessionLoaded(userId).catch(() => false);
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (isConnected(userId)) return true;
    await new Promise((r) => setTimeout(r, pollMs));
  }
  // Final check after restorePromise settles in case it just finished.
  await restorePromise;
  return isConnected(userId);
}
