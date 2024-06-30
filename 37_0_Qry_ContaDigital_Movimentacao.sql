
---===============================================================================---
--- Movimentação Conta Digital - Transações Fraudulentas
---===============================================================================---

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_ContaDigital_Fraude` 


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_ContaDigital_Fraude` AS 

with

base_1 as (
        select
        distinct
            cl.document as Cpf,
            cl.uuid as customer_id,
            cl.created_at as dt_abertura,
            FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura,
            BlMass.Lote as Dt_LoteMassivo,
            BlMass.Motivo as MotivoBloq_Massivo,
            tranlim.Flag_Risco_Limit_Vol,
            tranlim.Flag_Risco_Limit_Val,
            ev.observation as motivo,
            ev.user_name,
            ev.event_date,
            FORMAT_DATE("%Y%m",ev.event_date)as Safra_Bloqueio,
            case
            when ev.observation = 'Fraude confirmada' then 'Fraude confirmada'
            when ev.observation = 'Suspeita de fraude' then 'Suspeita de fraude'
            when ev.observation = 'Bloqueio de cadastro' then 'Bloqueio de cadastro'
            when ev.observation is null then 'Sem Bloqueio'
            when ev.observation = '' then 'Sem Bloqueio'
            else 'Outros' end as MotivoBloqueio,
            case
            when ev.observation In ('Fraude confirmada','Suspeita de fraude')  then 'Bloqueio Fraude'
            when ev.observation = 'Bloqueio de cadastro' then 'Bloqueio Preventivo'
            when ev.observation is null then 'Sem Bloqueio'
            when ev.observation = '' then 'Sem Bloqueio'
            else 'Outros' end as Flag_Bloqueio,
            RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
        from  `eai-datalake-data-sandbox.core.customers`   cl                                           
        left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id 

        --------------------------- Bloqueio Massivo ---------------------------------

        left join (
          with
          base_bloqueio_Massivo as (
          SELECT 
          CustomerID
          ,Lote
          ,Motivo
          ,RANK() OVER (PARTITION BY CustomerID ORDER BY Lote desc) AS Rank_Bloqueio
          FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2` 
          order by 1,4
          )
          select
          *
          from base_bloqueio_Massivo where Rank_Bloqueio = 1
          ) BlMass on BlMass.CustomerID = cl.uuid 

        --------------------------- Abusadores limite 300 - 180 dias ---------------------------------

        left join (
               select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_tran_limt_cliente`
          ) tranlim on tranlim.customer_id = cl.uuid 
        where 
        --date(ev.event_date) between '2022-01-01' and '2023-12-31'
        date(ev.event_date) >= current_date - 60
        and  
        ev.observation in ('Fraude confirmada', 'Suspeita de fraude')
        order by 1

),base as (
select * from base_1 
where Rank_Ult_Status = 1 and MotivoBloq_Massivo is null and Flag_Risco_Limit_Vol is null

), base_tranx_Contadigital as (

SELECT
     cash_transaction.created_at as created_at,
    --DATE(cash_transaction.created_at) as created_at,
    cash_transaction.flow,
    CASE WHEN (pix.type in ('CASH_IN', 'CASH_IN_REFUND') or ted.type = 'CASH_IN' or p2p.type = 'CASH_IN' or qpo.type = 'CASH_IN' or flow = 'BILLET') THEN 'CASH-IN' 
         WHEN (pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') or  ted.type = 'CASH_OUT' or p2p.type = 'CASH_OUT') THEN 'CASH-OUT' 
         END AS type,
    CASE WHEN flow = 'PIX' THEN
    CASE WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN pix_in_payee.payee_id
         WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payer.payer_id END
         WHEN flow = 'TED' THEN
    CASE WHEN ted.type = 'CASH_IN' THEN ted_in_payee.payee_id
         WHEN ted.type = 'CASH_OUT' THEN ted_payer.payer_id END
         WHEN flow = 'BILLET' THEN billet.payee_id 
         WHEN p2p.type = 'CASH_OUT' THEN p2p_payer.payer_id
         WHEN p2p.type = 'CASH_OUT' THEN p2p_payee.payee_id
         WHEN qpo.type = 'CASH_IN' THEN qpo.payee_document
         ELSE flow 
    END AS customer_id,
    CASE 
        WHEN cash_transaction.flow = 'PIX' AND PIX.TYPE IN ('CASH_IN', 'CASH_IN_REFUND') THEN 'PIX-IN'
        WHEN cash_transaction.flow = 'PIX' AND PIX.TYPE IN ('CASH_OUT', 'CASH_OUT_REFUND') THEN 'PIX-OUT'
        WHEN cash_transaction.flow = 'TED' AND TED.TYPE IN ('CASH_IN') THEN 'TED-IN'
        WHEN cash_transaction.flow = 'TED' AND TED.TYPE IN ('CASH_OUT') THEN 'TED-OUT'
        WHEN cash_transaction.flow = 'BILLET' AND BILLET.TYPE IN ('CASH_IN') THEN 'BOLETO'
        WHEN cash_transaction.flow = 'P2P' AND P2P.TYPE IN ('CASH_OUT') THEN 'P2P-OUT'
        WHEN cash_transaction.flow = 'P2P' AND P2P.TYPE IN ('CASH_IN') THEN 'P2P-IN'
        WHEN cash_transaction.flow = 'QRCODE_PIX_OUT' AND QPO.TYPE IN ('CASH_IN') THEN 'QRCODE_PIX_OUT'
        ELSE 'JUDICIAL_DEBT'
    END AS flowoperation,
    COUNT(DISTINCT cash_transaction.id) as qtdtransacoes,
    ROUND(SUM(cash_transaction.amount)/100,2) as valor
from  `eai-datalake-data-sandbox.cashback.cash_transaction` as cash_transaction
LEFT join `eai-datalake-data-sandbox.cashback.pix`  pix on cash_transaction.id = pix.cash_transaction_id 
LEFT join `eai-datalake-data-sandbox.cashback.pix_payer` pix_payer on pix_payer.pix_id = pix.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_in_payee` pix_in_payee on pix_in_payee.pix_id = pix.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted` ted on cash_transaction.id = ted.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_payer` ted_payer on ted_payer.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_in_payee`ted_in_payee on ted_in_payee.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p` p2p on cash_transaction.id = p2p.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payer` p2p_payer on p2p_payer.p2p_id = p2p.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payee` p2p_payee on p2p_payee.p2p_id = p2p.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.billet` billet on cash_transaction.id = billet.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.qrcode_pix_out` qpo on cash_transaction.id = qpo.cash_transaction_id
WHERE 
--DATE(cash_transaction.created_at) >= '2023-01-01' --AND DATE(cash_transaction.created_at) <= CURRENT_DATE
--AND 
(pix.status = 'APPROVED' or ted.status = 'APPROVED' or p2p.status = 'APPROVED' or billet.status = 'APPROVED' or qpo.status = 'PAID')
GROUP BY 1,2,3,4,5
ORDER BY 1
)
     select
     FORMAT_DATE("%Y%m",a.created_at)as Safra_Tranx
     ,bfd.Safra_Abertura
     ,bfd.Flag_Bloqueio
     ,bfd.Safra_Bloqueio
     ,bfd.motivo
     ,bfd.MotivoBloq_Massivo
     ,a.flow
     ,a.type
     ,a.flowoperation
     ,Case 
               When valor <100 Then '01-0 a 99'
               When valor >=100 and valor <300 Then '02-101 a 299'
               When valor =300 Then '03- 300'
               When valor > 300 and valor <=500 Then '04-301 a 500'
               When valor > 500 and valor <=1000 Then '05-501 a 1.000'
               When valor > 1000 and valor <=1500 Then '06-1001 a 1500'
               When valor > 1500 and valor <=2000 Then '07-1501 a 2000'
               When valor > 2000 and valor <=3000 Then '08-2001 a 3000'
               When valor > 3000 and valor <=5000 Then '09-3001 a 5000'
               When valor > 5000 and valor <=10000 Then '10-5001 a 10000'
               When valor > 1000 and valor <=15000 Then '11-10001 a 15000'
               When valor > 15000 and valor <=20000 Then '12-15001 a 20000'
               When valor > 20000                  Then '13-20001>'
      End as Intervalo_Valor
     ,case 
          when EXTRACT(HOUR FROM a.created_at) in (0,1,2,3,4,5,6) then '01 Madrugada'
          when EXTRACT(HOUR FROM a.created_at) in (7,8,9,10,11,12) then '02 Manhã'
          when EXTRACT(HOUR FROM a.created_at) in (13,14,15,16,17,18) then '03 Tarde'
          when EXTRACT(HOUR FROM a.created_at) in (19,20,21,22,23) then '04 Noite'
     else 'NA' end as Periodo_Tranx
     ,Sum(a.qtdtransacoes) as qtdtransacoes
     ,Sum(a.valor) as valor
     
  
     from base_tranx_Contadigital a 
     join base bfd on bfd.customer_id = a.customer_id
     where type = 'CASH-IN'
     group by 1,2,3,4,5,6,7,8,9,10,11


;
--and customer_id = 'CUS-a74462db-0510-4c85-9dee-27ca841901e8'

---===============================================================================---
--- Movimentação Conta Digital
---===============================================================================---



-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_ContaDigital_Movimentacao` 


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_ContaDigital_Movimentacao` AS 

with

base as (
        select
        distinct
            cl.document as Cpf,
            cl.uuid as customer_id,
            cl.created_at as dt_abertura,
            FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura,
            ev.observation as motivo,
            ev.user_name,
            ev.event_date,
            FORMAT_DATE("%Y%m",ev.event_date)as Safra_Bloqueio,
            case
            when ev.observation = 'Fraude confirmada' then 'Fraude confirmada'
            when ev.observation = 'Suspeita de fraude' then 'Suspeita de fraude'
            when ev.observation = 'Bloqueio de cadastro' then 'Bloqueio de cadastro'
            when ev.observation is null then 'Sem Bloqueio'
            when ev.observation = '' then 'Sem Bloqueio'
            else 'Outros' end as MotivoBloqueio,
            case
            when ev.observation In ('Fraude confirmada','Suspeita de fraude')  then 'Bloqueio Fraude'
            when ev.observation = 'Bloqueio de cadastro' then 'Bloqueio Preventivo'
            when ev.observation is null then 'Sem Bloqueio'
            when ev.observation = '' then 'Sem Bloqueio'
            else 'Outros' end as Flag_Bloqueio,
            RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
        from  `eai-datalake-data-sandbox.core.customers`   cl                                           
        left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id 
        where 
        --date(ev.event_date) between '2022-01-01' and '2023-12-31'
        --date(ev.event_date) >= current_date - 180
        --and  
        ev.observation not in ('Fraude confirmada', 'Suspeita de fraude')


), base_tranx_Contadigital as (

SELECT
    cash_transaction.created_at as created_at,
    cash_transaction.flow,
    CASE WHEN (pix.type in ('CASH_IN', 'CASH_IN_REFUND') or ted.type = 'CASH_IN' or p2p.type = 'CASH_IN' or qpo.type = 'CASH_IN' or flow = 'BILLET') THEN 'CASH-IN' 
         WHEN (pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') or  ted.type = 'CASH_OUT' or p2p.type = 'CASH_OUT') THEN 'CASH-OUT' 
         END AS type,
    CASE WHEN flow = 'PIX' THEN
    CASE WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN pix_in_payee.payee_id
         WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payer.payer_id END
         WHEN flow = 'TED' THEN
    CASE WHEN ted.type = 'CASH_IN' THEN ted_in_payee.payee_id
         WHEN ted.type = 'CASH_OUT' THEN ted_payer.payer_id END
         WHEN flow = 'BILLET' THEN billet.payee_id 
         WHEN p2p.type = 'CASH_OUT' THEN p2p_payer.payer_id
         WHEN p2p.type = 'CASH_OUT' THEN p2p_payee.payee_id
         WHEN qpo.type = 'CASH_IN' THEN qpo.payee_document
         ELSE flow 
    END AS customer_id,
    CASE 
        WHEN cash_transaction.flow = 'PIX' AND PIX.TYPE IN ('CASH_IN', 'CASH_IN_REFUND') THEN 'PIX-IN'
        WHEN cash_transaction.flow = 'PIX' AND PIX.TYPE IN ('CASH_OUT', 'CASH_OUT_REFUND') THEN 'PIX-OUT'
        WHEN cash_transaction.flow = 'TED' AND TED.TYPE IN ('CASH_IN') THEN 'TED-IN'
        WHEN cash_transaction.flow = 'TED' AND TED.TYPE IN ('CASH_OUT') THEN 'TED-OUT'
        WHEN cash_transaction.flow = 'BILLET' AND BILLET.TYPE IN ('CASH_IN') THEN 'BOLETO'
        WHEN cash_transaction.flow = 'P2P' AND P2P.TYPE IN ('CASH_OUT') THEN 'P2P-OUT'
        WHEN cash_transaction.flow = 'P2P' AND P2P.TYPE IN ('CASH_IN') THEN 'P2P-IN'
        WHEN cash_transaction.flow = 'QRCODE_PIX_OUT' AND QPO.TYPE IN ('CASH_IN') THEN 'QRCODE_PIX_OUT'
        ELSE 'JUDICIAL_DEBT'
    END AS flowoperation,
    COUNT(DISTINCT cash_transaction.id) as qtdtransacoes,
    ROUND(SUM(cash_transaction.amount)/100,2) as valor
from  `eai-datalake-data-sandbox.cashback.cash_transaction` as cash_transaction
LEFT join `eai-datalake-data-sandbox.cashback.pix`  pix on cash_transaction.id = pix.cash_transaction_id 
LEFT join `eai-datalake-data-sandbox.cashback.pix_payer` pix_payer on pix_payer.pix_id = pix.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_in_payee` pix_in_payee on pix_in_payee.pix_id = pix.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted` ted on cash_transaction.id = ted.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_payer` ted_payer on ted_payer.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_in_payee`ted_in_payee on ted_in_payee.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p` p2p on cash_transaction.id = p2p.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payer` p2p_payer on p2p_payer.p2p_id = p2p.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payee` p2p_payee on p2p_payee.p2p_id = p2p.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.billet` billet on cash_transaction.id = billet.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.qrcode_pix_out` qpo on cash_transaction.id = qpo.cash_transaction_id
WHERE 
DATE(cash_transaction.created_at) >= current_date - 180
--DATE(cash_transaction.created_at) >= '2023-01-01' --AND DATE(cash_transaction.created_at) <= CURRENT_DATE
AND 
(pix.status = 'APPROVED' or ted.status = 'APPROVED' or p2p.status = 'APPROVED' or billet.status = 'APPROVED' or qpo.status = 'PAID')
GROUP BY 1,2,3,4,5
ORDER BY 1
)
     select
     FORMAT_DATE("%Y%m",a.created_at)as Safra_Tranx
     ,bfd.Safra_Abertura
     ,bfd.Flag_Bloqueio
     ,bfd.Safra_Bloqueio
     ,bfd.motivo
     ,a.flow
     ,a.type
     ,a.flowoperation
     ,Case 
               When valor <100 Then '01-0 a 99'
               When valor >=100 and valor <300 Then '02-101 a 299'
               When valor =300 Then '03- 300'
               When valor > 300 and valor <=500 Then '04-301 a 500'
               When valor > 500 and valor <=1000 Then '05-501 a 1.000'
               When valor > 1000 and valor <=1500 Then '06-1001 a 1500'
               When valor > 1500 and valor <=2000 Then '07-1501 a 2000'
               When valor > 2000 and valor <=3000 Then '08-2001 a 3000'
               When valor > 3000 and valor <=5000 Then '09-3001 a 5000'
               When valor > 5000 and valor <=10000 Then '10-5001 a 10000'
               When valor > 1000 and valor <=15000 Then '11-10001 a 15000'
               When valor > 15000 and valor <=20000 Then '12-15001 a 20000'
               When valor > 20000                  Then '13-20001>'
      End as Intervalo_Valor
      ,case 
          when EXTRACT(HOUR FROM a.created_at) in (0,1,2,3,4,5,6) then '01 Madrugada'
          when EXTRACT(HOUR FROM a.created_at) in (7,8,9,10,11,12) then '02 Manhã'
          when EXTRACT(HOUR FROM a.created_at) in (13,14,15,16,17,18) then '03 Tarde'
          when EXTRACT(HOUR FROM a.created_at) in (19,20,21,22,23) then '04 Noite'
          else 'NA' end as Periodo_Tranx
     ,Sum(a.qtdtransacoes) as qtdtransacoes
     ,Sum(a.valor) as valor
     
  
     from base_tranx_Contadigital a 
     join base bfd on bfd.customer_id = a.customer_id
     where type = 'CASH-IN'
     group by 1,2,3,4,5,6,7,8,9,10






