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
    await bot.handleUpdate(req.body);
  } catch (err: any) {
    console.error("[WEBHOOK] Error handling update:", err?.message);
  }
});

export default router;
