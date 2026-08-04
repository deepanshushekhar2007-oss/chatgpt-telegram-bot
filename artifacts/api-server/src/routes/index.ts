import { Router, type IRouter } from "express";
import healthRouter from "./health";
import { bot } from "../bot/telegram";

const router: IRouter = Router();

router.use(healthRouter);

// Telegram webhook endpoint — Telegram updates directly POST karta hai yahan.
// bot.handleUpdate() directly use kiya hai Grammy adapter ke bina:
//   • Koi Grammy 10-second timeout nahi — WhatsApp ops ke liye safe
//   • bot.start() touch nahi hota — koi Grammy override nahi
//   • Hamesha 200 return karo Telegram ko — retry loops avoid karne ke liye
//   • Sirf Render production pe active hota hai (RENDER_EXTERNAL_URL set ho tab)
router.post("/telegram-webhook", async (req, res) => {
  // Pehle 200 bhejo Telegram ko — bot processing async mein karega.
  // Isse Telegram timeout nahi hoga chahe processing time zyada lage.
  res.sendStatus(200);
  try {
    // Safety net: Render restart pe Telegram pehle se set webhook URL pe
    // turant buffered updates bhej deta hai — lekin startBot() async hai
    // aur bot.init() complete hone mein 3-10 sec lag sakte hain. Agar us
    // window mein update aaya to bot.handleUpdate() "Bot not initialized!"
    // throw karta tha aur update silently drop ho jata tha (WhatsApp
    // reconnect kabhi trigger nahi hota). Grammy 1.42 ka init() idempotent
    // hai — concurrent calls safely handle hoti hain via internal mePromise.
    if (!bot.isInited()) {
      await bot.init();
    }
    await bot.handleUpdate(req.body);
  } catch (err: any) {
    // BotError = Grammy ke andar middleware ne throw kiya — bot.errorHandler
    // ko call karo taaki bot.catch() registered handler invoke ho.
    // Non-BotError = Grammy ke bahar ki problem (e.g. "Bot not initialized"
    // agar init race condition kisi aur wajah se reh jaaye).
    if (err?.ctx !== undefined) {
      try { await (bot as any).errorHandler(err); } catch {}
    } else {
      console.error("[WEBHOOK] Error handling update:", err?.message);
    }
  }
});

export default router;
