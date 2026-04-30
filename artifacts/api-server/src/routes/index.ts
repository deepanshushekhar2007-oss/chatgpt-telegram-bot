import { Router, type IRouter } from "express";
import healthRouter from "./health";
import { botWebhookHandler } from "../bot/telegram";

const router: IRouter = Router();

router.use(healthRouter);

// Telegram webhook endpoint — Telegram updates directly POST karta hai yahan.
// Ye sirf webhook mode mein use hota hai (jab RENDER_EXTERNAL_URL set ho).
router.post("/telegram-webhook", (req, res, next) => {
  botWebhookHandler(req, res, next).catch(next);
});

export default router;
