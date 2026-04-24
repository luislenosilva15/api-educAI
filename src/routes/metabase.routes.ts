import { Router, Request, Response } from "express";
import { getDatabases, getCollections, getCardData, runNativeQuery } from "../metabase";

const router = Router();

const DATABASE_ID = 2; // AgendaBase

const buildPaymentsQuery = (schoolId: number) => `
SELECT
    s.id,
    TO_CHAR(DATE_TRUNC('month', p.paid_at)::date, 'MM/YYYY') AS "Mes",
    CASE
        WHEN p.method_payment = 1 AND p.installments = 1 AND p.receiving_method IS NULL THEN 'Cartão de crédito - à vista'
        WHEN p.method_payment = 1 AND p.installments > 1 AND p.receiving_method IS NULL THEN 'Cartão de crédito - parcelado'
        WHEN p.method_payment = 1 AND p.installments = 1 AND p.receiving_method = 1  THEN 'PIX no crédito - à vista'
        WHEN p.method_payment = 1 AND p.installments > 1 AND p.receiving_method = 1  THEN 'PIX no crédito - parcelado'
        WHEN p.method_payment = 3 THEN 'Boleto'
        WHEN p.method_payment = 4 THEN 'Pix'
    END AS "Metodo_Pagamento",
    CASE
        WHEN o.orderable_type = 'RecurrentBill'                              THEN 'Cobrança Recorrente'
        WHEN o.orderable_type = 'SchoolProduct'  AND sp.plan_kind = 1        THEN 'Cobrança Única'
        WHEN o.orderable_type = 'SchoolProduct'  AND sp.plan_kind = 2        THEN 'Matrícula'
        WHEN o.orderable_type = 'SchoolProduct'  AND sp.plan_kind = 3        THEN 'Shop'
        WHEN o.orderable_type = 'PaymentsDomain::Bill' AND b.kind = 1        THEN 'Remessa e Retorno'
        WHEN o.orderable_type = 'PaymentsDomain::Bill' AND b.kind = 2        THEN 'Ficha Financeira'
    END AS "Produto",
    CASE
        WHEN o.orderable_type = 'RecurrentBill' THEN 'Recorrente'
        ELSE 'Spot'
    END AS "Recorrente_ou_Spot",
    COUNT(DISTINCT p.id)                                              AS "Qtd_Transacoes",
    SUM(p.amount_cents)                                               AS "Valor_Transacao",
    SUM(p.net_amount_cents + p.installment_fee_revenue_cents)         AS "Receita_Bruta"
FROM payments p
INNER JOIN orders          o  ON o.id  = p.order_id
INNER JOIN schools         s  ON s.id  = p.school_id
LEFT  JOIN school_products sp ON sp.id = o.orderable_id AND o.orderable_type = 'SchoolProduct'
LEFT  JOIN bills            b ON b.id  = o.orderable_id AND o.orderable_type = 'PaymentsDomain::Bill'
LEFT  JOIN recurrent_bills rb ON rb.id = o.orderable_id AND o.orderable_type = 'RecurrentBill'
WHERE 1=1
  AND s.id             = ${schoolId}
  AND s.plan          IN (2,3,4,6,7)
  AND p.receiver      != 2
  AND p.status         = 3
  AND p.method_payment IN (1,3,4)
  AND p.paid_at       >= DATE '2025-01-01'
GROUP BY
    s.id,
    DATE_TRUNC('month', p.paid_at)::date,
    p.method_payment,
    p.receiving_method,
    p.installments,
    o.orderable_type,
    sp.plan_kind,
    b.kind
ORDER BY
    DATE_TRUNC('month', p.paid_at)::date,
    "Produto",
    "Metodo_Pagamento"
`;

router.get("/payments/:schoolId", async (req: Request, res: Response) => {
  try {
    const schoolId = Number(req.params.schoolId);
    const data = await runNativeQuery(DATABASE_ID, buildPaymentsQuery(schoolId));
    res.json({ success: true, data });
  } catch (error: any) {
    const detail = error.response?.data ?? error.message;
    console.error("[/payments/:schoolId]", detail);
    res.status(500).json({ success: false, message: error.message, detail });
  }
});

router.get("/databases", async (_req: Request, res: Response) => {
  try {
    const data = await getDatabases();
    res.json({ success: true, data });
  } catch (error: any) {
    const detail = error.response?.data ?? error.message;
    console.error("[/databases]", detail);
    res.status(500).json({ success: false, message: error.message, detail });
  }
});

router.get("/collections", async (_req: Request, res: Response) => {
  try {
    const data = await getCollections();
    res.json({ success: true, data });
  } catch (error: any) {
    const detail = error.response?.data ?? error.message;
    console.error("[/collections]", detail);
    res.status(500).json({ success: false, message: error.message, detail });
  }
});

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
