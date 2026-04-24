import { Router, Request, Response } from "express";
import {
  getDatabases,
  getCollections,
  getCardData,
  runNativeQuery,
} from "../metabase";

const router = Router();

const DATABASE_ID = 2; // AgendaBase

const buildRecurrentCardsQuery = (schoolId: number) => `
SELECT
    rp.title                 AS "Plano",
    COUNT(DISTINCT rc.id)    AS "Qtd_Cartoes_Habilitados"
FROM recurrent_plans rp
INNER JOIN edupay_subscribes es ON es.recurrent_plan_id = rp.id
    AND es.active     = true
    AND es.deleted_at IS NULL
INNER JOIN responsible_cards rc ON rc.id = es.responsible_card_id
    AND rc.status     = true
    AND rc.deleted_at IS NULL
WHERE rp.school_id = ${schoolId}
  AND rp.status    = 1
GROUP BY rp.title
ORDER BY rp.title
`;

const buildPaymentsQuery = (schoolId: number) => `
WITH labeled AS (
  SELECT
    TO_CHAR(DATE_TRUNC('month', p.paid_at)::date, 'MM/YYYY') AS "Mes",
    CASE
        WHEN p.method_payment = 1 AND p.installments = 1 AND p.receiving_method IS NULL THEN 'Cartão de crédito - à vista'
        WHEN p.method_payment = 1 AND p.installments > 1 AND p.receiving_method IS NULL THEN 'Cartão de crédito - parcelado'
        WHEN p.method_payment = 1 AND p.receiving_method = 1                           THEN 'Pix'
        WHEN p.method_payment = 3                                                       THEN 'Boleto'
        WHEN p.method_payment = 4                                                       THEN 'Pix'
    END AS "Metodo_Pagamento",
    CASE
        WHEN o.orderable_type = 'RecurrentBill'                             THEN 'Cobrança Recorrente'
        WHEN o.orderable_type = 'SchoolProduct' AND sp.plan_kind = 1        THEN 'Cobrança Única'
        WHEN o.orderable_type = 'SchoolProduct' AND sp.plan_kind = 2        THEN 'Matrícula'
        WHEN o.orderable_type = 'SchoolProduct' AND sp.plan_kind = 3        THEN 'Shop'
        WHEN o.orderable_type = 'PaymentsDomain::Bill' AND b.kind = 1       THEN 'Remessa e Retorno'
        WHEN o.orderable_type = 'PaymentsDomain::Bill' AND b.kind = 2       THEN 'Ficha Financeira'
    END AS "Produto",
    CASE
        WHEN o.orderable_type = 'RecurrentBill' THEN 'Recorrente'
        ELSE 'Spot'
    END AS "Recorrente_ou_Spot",
    p.id                                                    AS payment_id,
    p.amount_cents,
    p.net_amount_cents + p.installment_fee_revenue_cents    AS receita,
    COALESCE(rb.due_date, b.due_date)                       AS due_date,
    p.paid_at::date                                         AS paid_date
  FROM payments p
  INNER JOIN orders          o  ON o.id  = p.order_id
  INNER JOIN schools         s  ON s.id  = p.school_id
  LEFT  JOIN school_products sp ON sp.id = o.orderable_id AND o.orderable_type = 'SchoolProduct'
  LEFT  JOIN bills            b ON b.id  = o.orderable_id AND o.orderable_type = 'PaymentsDomain::Bill'
  LEFT  JOIN recurrent_bills rb ON rb.id = o.orderable_id AND o.orderable_type = 'RecurrentBill'
  WHERE s.id             = ${schoolId}
    AND s.plan          IN (2,3,4,6,7)
    AND p.receiver      != 2
    AND p.status         = 3
    AND p.method_payment IN (1,3,4)
    AND p.paid_at       >= DATE '2025-01-01'
)
SELECT
    "Mes",
    "Metodo_Pagamento",
    "Produto",
    "Recorrente_ou_Spot",
    COUNT(DISTINCT payment_id)                        AS "Qtd_Transacoes",
    SUM(amount_cents)                                 AS "Valor_Transacao",
    SUM(receita)                                      AS "Receita_Bruta",
    ROUND(AVG(paid_date - due_date))                  AS "Media_Dias_Apos_Vencimento"
FROM labeled
GROUP BY
    "Mes",
    "Metodo_Pagamento",
    "Produto",
    "Recorrente_ou_Spot"
ORDER BY
    "Mes",
    "Produto",
    "Metodo_Pagamento"
`;

const buildOverdueRecurrentQuery = (schoolId: number) => `
SELECT
    rp_profile.name                                 AS "Responsavel",
    resp.email                                      AS "Email",
    STRING_AGG(DISTINCT sp.name, ', ')              AS "Alunos",
    rplan.title                                     AS "Plano",
    TO_CHAR(rb.due_date, 'DD/MM/YYYY')              AS "Vencimento",
    rb.price_cents                                  AS "Valor_Cents",
    (CURRENT_DATE - rb.due_date)                    AS "Dias_Atraso"
FROM recurrent_bills rb
INNER JOIN recurrent_plans rplan ON rplan.id = rb.recurrent_plan_id
    AND rplan.school_id = ${schoolId}
    AND rplan.status    = 1
INNER JOIN edupay_subscribes es ON es.recurrent_plan_id = rplan.id
    AND es.active        = true
    AND es.deleted_at   IS NULL
    AND es.userable_type = 'ResponsibleProfile'
INNER JOIN responsible_profiles rp_profile ON rp_profile.id = es.userable_id
    AND rp_profile.deleted_at IS NULL
LEFT  JOIN responsibles resp ON resp.responsible_profile_id = rp_profile.id
    AND resp.deleted_at IS NULL
LEFT  JOIN responsible_profiles_student_profiles rpsp ON rpsp.responsible_profile_id = rp_profile.id
LEFT  JOIN student_profiles sp ON sp.id = rpsp.student_profile_id
    AND sp.school_id   = ${schoolId}
    AND sp.deleted_at IS NULL
WHERE rb.due_date   < CURRENT_DATE
  AND rb.school_id  = ${schoolId}
  AND NOT EXISTS (
      SELECT 1
      FROM orders o
      INNER JOIN payments p ON p.order_id = o.id AND p.status = 3
      WHERE o.orderable_type = 'RecurrentBill'
        AND o.orderable_id   = rb.id
  )
GROUP BY
    rp_profile.name,
    resp.email,
    rplan.title,
    rb.due_date,
    rb.price_cents
ORDER BY rp_profile.name, rb.due_date
`;

router.get("/payments/:schoolId", async (req: Request, res: Response) => {
  try {
    const schoolId = Number(req.params.schoolId);
    const [payments, recurrentCards, overdueRecurrent] = await Promise.all([
      runNativeQuery(DATABASE_ID, buildPaymentsQuery(schoolId)),
      runNativeQuery(DATABASE_ID, buildRecurrentCardsQuery(schoolId)),
      runNativeQuery(DATABASE_ID, buildOverdueRecurrentQuery(schoolId)),
    ]);
    res.json({
      success: true,
      data: { payments, recurrent_cards: recurrentCards, overdue_recurrent: overdueRecurrent },
    });
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
