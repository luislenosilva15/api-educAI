import { Router, Request, Response } from "express";
import { getDatabases, getCollections, getCardData } from "../metabase";

const router = Router();

router.get("/card/:id/data", async (req: Request, res: Response) => {
  try {
    const cardId = Number(req.params.id);
    const data = await getCardData(cardId);
    res.json({ success: true, data });
  } catch (error: any) {
    const detail = error.response?.data ?? error.message;
    console.error("[/card/:id/data]", detail);
    res.status(500).json({ success: false, message: error.message, detail });
  }
});

export default router;
