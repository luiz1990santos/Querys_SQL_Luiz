--==========================================================================================================
-- Base Movimentações - Contas Digital Cash IN e Cash Out
--==========================================================================================================

-- select sum(valor) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Movimentacao_CashIn_Out` where CPF_CNPJ_Destino = '02994637537'

-- select count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Movimentacao_CashIn_Out`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Movimentacao_CashIn_Out` AS 

with

base_Cash_In_Out  as (

SELECT
cash_transaction.created_at as created_at,
cash_transaction.flow,
CASE WHEN (pix.type in ('CASH_IN', 'CASH_IN_REFUND') or ted.type = 'CASH_IN' or p2p.type = 'CASH_IN' or qpo.type = 'CASH_IN' or flow = 'BILLET') THEN 'CASH-IN' 
WHEN (pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') or  ted.type = 'CASH_OUT' or p2p.type = 'CASH_OUT') THEN 'CASH-OUT' 
END AS type,

--------------ORIGEM---------------------------------------------------------------------------------	

CASE WHEN flow = 'PIX' THEN
CASE 
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') and cl_pix_in_payer.uuid = pix_in_payer.document THEN cl_pix_in_payer.document
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN pix_in_payer.document
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') and cl_pix_payer.uuid = pix_payer.payer_id THEN cl_pix_payer.document 
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payer.payer_id
END
WHEN flow = 'TED' THEN
CASE 
WHEN ted.type in ('CASH_IN') and cl_ted_in_payer.uuid = ted_in_payer.document THEN cl_ted_in_payer.document
WHEN ted.type in ('CASH_IN') THEN ted_in_payer.document
WHEN ted.type in ('CASH_OUT') and cl_ted_payer.uuid = ted_payer.payer_id THEN cl_ted_payer.document 
WHEN ted.type in ('CASH_OUT') THEN ted_payer.payer_id
END
WHEN flow = 'P2P' THEN
CASE 
WHEN p2p.type = 'CASH_OUT' THEN cl_p2p_payer.document
END
WHEN flow = 'BILLET' THEN billet.line_code
WHEN flow = 'QRCODE_PIX_OUT' THEN qrcode_informations.document
ELSE 'VERIFICAR'  END AS CPF_CNPJ_Origem,

--------------ORIGEM - NOME ---------------------------------------------------------------------------------	

CASE WHEN flow = 'PIX' THEN
CASE 
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') and cl_pix_in_payer.uuid = pix_in_payer.document THEN cl_pix_in_payer.full_name
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN pix_in_payer.name
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') and cl_pix_payer.uuid = pix_payer.payer_id THEN cl_pix_payer.full_name
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payer.name
END
WHEN flow = 'TED' THEN
CASE 
WHEN ted.type in ('CASH_IN') and cl_ted_in_payer.uuid = ted_in_payer.document THEN cl_ted_in_payer.full_name
WHEN ted.type in ('CASH_IN') THEN ted_in_payer.name
WHEN ted.type in ('CASH_OUT') and cl_ted_payer.uuid = ted_payer.payer_id THEN cl_ted_payer.full_name
WHEN ted.type in ('CASH_OUT') THEN ted_payer.name
END
WHEN flow = 'P2P' THEN
CASE 
WHEN p2p.type = 'CASH_OUT' THEN cl_p2p_payer.full_name
END
WHEN flow = 'BILLET' THEN 'ABASTECE-AI'
WHEN flow = 'QRCODE_PIX_OUT' THEN qrcode_informations.name
ELSE 'VERIFICAR' END AS Origem_Nome,

CASE WHEN flow = 'PIX' THEN
CASE 
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN pix_in_payer.bank_name 
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN 'ABASTECE-AI'
END
WHEN flow = 'TED' THEN
CASE WHEN ted.type in ('CASH_IN', 'CASH_IN_REFUND') THEN ted_in_payer.bank_name 
WHEN ted.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN 'ABASTECE-AI'
END
WHEN flow = 'P2P' THEN
CASE 
WHEN p2p.type = 'CASH_OUT' THEN 'ABASTECE-AI'
--WHEN p2p.type = 'CASH_OUT' THEN 'ABASTECE-AI'
END
WHEN flow = 'BILLET' THEN 'ABASTECE-AI'
WHEN flow = 'QRCODE_PIX_OUT' THEN qrcode_informations.bank_name

ELSE 'VERIFICAR' END AS Origem_Banco,

--------------DESTINO - NOME ---------------------------------------------------------------------------------	

CASE WHEN flow = 'PIX' THEN
CASE 
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') and cl_pix_in_payee.uuid = pix_in_payee.payee_id THEN cl_pix_in_payee.document 
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payee.document
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') and cl_pix_in_payee.uuid = pix_in_payee.payee_id THEN cl_pix_in_payee.document 
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN pix_in_payee.payee_id 
END
WHEN flow = 'TED' THEN
CASE 
WHEN ted.type in ('CASH_OUT', 'CASH_OUT_REFUND') and cl_ted_in_payee.uuid = ted_in_payee.payee_id THEN cl_ted_in_payee.document 
WHEN ted.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN ted_payee.document
WHEN ted.type in ('CASH_IN', 'CASH_IN_REFUND') and cl_ted_in_payee.uuid = ted_in_payee.payee_id THEN cl_ted_in_payee.document 
WHEN ted.type in ('CASH_IN', 'CASH_IN_REFUND') THEN ted_in_payee.payee_id 
END
WHEN flow = 'P2P' THEN
CASE 
--WHEN p2p.type = 'CASH_OUT' THEN cl_p2p_payer.document
WHEN p2p.type = 'CASH_OUT' THEN cl_p2p_payee.document
END
WHEN flow = 'BILLET' THEN cl_billet.document
WHEN flow = 'QRCODE_PIX_OUT' THEN qpo.payee_document
ELSE 'VERIFICAR' 
END AS CPF_CNPJ_Destino,

CASE WHEN flow = 'PIX' THEN
CASE 
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payee.name
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN cl_pix_in_payee.full_name 
END
WHEN flow = 'TED' THEN
CASE 
WHEN ted.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN ted_payee.name
WHEN ted.type in ('CASH_IN', 'CASH_IN_REFUND') THEN cl_ted_in_payee.full_name 
END
WHEN flow = 'P2P' THEN
CASE 
--WHEN p2p.type = 'CASH_OUT' THEN cl_p2p_payer.full_name
WHEN p2p.type = 'CASH_OUT' THEN cl_p2p_payee.full_name
END
WHEN flow = 'BILLET' THEN cl_billet.full_name
WHEN flow = 'QRCODE_PIX_OUT' THEN cl_qpo.fantasy_name
ELSE 'VERIFICAR'
END AS Destino_Nome,

CASE WHEN flow = 'PIX' THEN
CASE 
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payee.bank_name
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN 'ABASETECE-AI'
END
WHEN flow = 'TED' THEN
CASE 
WHEN ted.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN ted_payee.bank_name
WHEN ted.type in ('CASH_IN', 'CASH_IN_REFUND') THEN 'ABASETECE-AI'
END
WHEN flow = 'P2P' THEN
CASE 
--WHEN p2p.type = 'CASH_OUT' THEN 'ABASTECE-AI'
WHEN p2p.type = 'CASH_OUT' THEN 'ABASTECE-AI'
END
WHEN flow = 'BILLET' THEN 'ABASETECE-AI'
WHEN flow = 'QRCODE_PIX_OUT' THEN 'NaoInformado'
ELSE 'VERIFICAR'
END AS Destino_Banco,

-----------OPERÇÃO----------------------------------------------------------------------------------------------

    CASE 
        WHEN cash_transaction.flow = 'PIX' AND pix.type IN ('CASH_IN', 'CASH_IN_REFUND') THEN 'PIX-IN'
        WHEN cash_transaction.flow = 'PIX' AND pix.type IN ('CASH_OUT', 'CASH_OUT_REFUND') THEN 'PIX-OUT'
        WHEN cash_transaction.flow = 'TED' AND ted.type IN ('CASH_IN') THEN 'TED-IN'
        WHEN cash_transaction.flow = 'TED' AND ted.type IN ('CASH_OUT') THEN 'TED-OUT'
        WHEN cash_transaction.flow = 'P2P' AND p2p.type IN ('CASH_OUT') THEN 'P2P-OUT'
        WHEN cash_transaction.flow = 'P2P' AND P2P.type IN ('CASH_IN') THEN 'P2P-IN'
        WHEN cash_transaction.flow = 'BILLET' AND billet.type IN ('CASH_IN') THEN 'BOLETO'
        WHEN cash_transaction.flow = 'QRCODE_PIX_OUT' AND QPO.type IN ('CASH_IN') THEN 'QRCODE_PIX_OUT'
        ELSE 'JUDICIAL_DEBT'
    END AS OPERACAO,
    pix.end_to_end_id as codigo_transacao,
    COUNT(DISTINCT cash_transaction.id) as qtdtransacoes,
    SUM(cash_transaction.amount)/100 as valor
    --ROUND(SUM(cash_transaction.amount)/100,2) as valor
FROM  `eai-datalake-data-sandbox.cashback.cash_transaction` as cash_transaction

LEFT join `eai-datalake-data-sandbox.cashback.pix`  pix on cash_transaction.id = pix.cash_transaction_id 
LEFT join `eai-datalake-data-sandbox.cashback.pix_payer` pix_payer on pix_payer.pix_id = pix.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_in_payee` pix_in_payee on pix_in_payee.pix_id = pix.id 
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_payee` pix_payee   ON pix.id = pix_payee.pix_id --- RECEBEDOR
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_in_payer` pix_in_payer ON pix_in_payer.pix_id = pix.id --- EMISSOR

LEFT JOIN `eai-datalake-data-sandbox.cashback.ted` ted on cash_transaction.id = ted.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_in_payee` ted_in_payee on ted_in_payee.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_in_payer` ted_in_payer on ted_in_payer.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_payee`  ted_payee on ted_payee.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_payer` ted_payer on ted_payer.ted_id = ted.id

LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p` p2p on cash_transaction.id = p2p.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payer` p2p_payer on p2p_payer.p2p_id = p2p.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payee` p2p_payee on p2p_payee.p2p_id = p2p.id

LEFT JOIN `eai-datalake-data-sandbox.cashback.billet` billet on cash_transaction.id = billet.cash_transaction_id

LEFT JOIN `eai-datalake-data-sandbox.cashback.qrcode_pix_out` qpo on cash_transaction.id = qpo.cash_transaction_id
LEFT JOIN  `eai-datalake-data-sandbox.cashback.qrcode_pix_out_payer_informations` qrcode_informations on qrcode_informations.qrcode_pix_out_id= qpo.id
LEFT JOIN  `eai-datalake-data-sandbox.cashback.qrcode_pix_out_refund` qrcode_refund on qrcode_refund.origin_qrcode_pix_id = qrcode_informations.qrcode_pix_out_id

LEFT JOIN `eai-datalake-data-sandbox.core.customers`    cl_pix_in_payer on cl_pix_in_payer.uuid = pix_in_payer.document
LEFT JOIN `eai-datalake-data-sandbox.core.customers`    cl_pix_payer on cl_pix_payer.uuid = pix_payer.payer_id
LEFT JOIN `eai-datalake-data-sandbox.core.customers`    cl_pix_in_payee on cl_pix_in_payee.uuid = pix_in_payee.payee_id

LEFT JOIN `eai-datalake-data-sandbox.core.customers` cl_ted_in_payee on cl_ted_in_payee.uuid = ted_in_payee.payee_id
LEFT JOIN `eai-datalake-data-sandbox.core.customers` cl_ted_payer on cl_ted_payer.uuid = ted_payer.payer_id
LEFT JOIN `eai-datalake-data-sandbox.core.customers` cl_ted_in_payer on cl_ted_in_payer.uuid = ted_in_payee.payee_id

LEFT JOIN `eai-datalake-data-sandbox.core.customers` cl_p2p_payer on cl_p2p_payer.uuid = p2p_payer.payer_id
LEFT JOIN `eai-datalake-data-sandbox.core.customers` cl_p2p_payee on cl_p2p_payee.uuid = p2p_payee.payee_id

LEFT JOIN `eai-datalake-data-sandbox.core.customers` cl_billet on cl_billet.uuid = billet.payee_id
LEFT JOIN `eai-datalake-data-sandbox.backoffice.store` cl_qpo on cl_qpo.document = qpo.payee_document 


WHERE 
date(cash_transaction.created_at) >= current_date - 90
--and cash_transaction.flow = 'QRCODE_PIX_OUT'
AND 
(pix.status = 'APPROVED' or ted.status = 'APPROVED' or p2p.status = 'APPROVED' or billet.status = 'APPROVED' or qpo.status = 'PAID')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
ORDER BY 1


), base_Consolidada as (
select 
a.created_at as Dt_Transacao
,cl_CPF_CNPJ_Origem.status as StatusConta
,flow
,type
,cl_CPF_CNPJ_Origem.created_at as Dt_Abert_Origem
,CPF_CNPJ_Origem
,Origem_Nome
,Origem_Banco
,cl_CPF_CNPJ_Destino.created_at as Dt_Abert_Destino
,CPF_CNPJ_Destino
,Destino_Nome
,Destino_Banco
,codigo_transacao
,case
when CPF_CNPJ_Origem = CPF_CNPJ_Destino then 'MesmaTitularidade'
else 'OutraTitularidade' end as Flag_Titularidade
,Case 
When valor <100 Then '1-0 a 99'
When valor >=100 and valor <=300 Then '2-101 a 300'
When valor > 300 and valor <=500 Then '3-301 a 500'
When valor > 500 and valor <=1000 Then '4-501 a 1.000'
When valor > 1000 and valor <=1500 Then '5-1001 a 1500'
When valor > 1500 and valor <=2000 Then '6-1501 a 2000'
When valor > 2000 and valor <=3000 Then '7-2001 a 3000'
When valor > 3000 and valor <=5000 Then '8-3001 a 5000'
When valor > 5000                  Then '9-5001>'
End as Intervalo_Valor
,case 
when EXTRACT(HOUR FROM a.created_at) in (0,1,2,3,4,5,6) then '01 Madrugada'
when EXTRACT(HOUR FROM a.created_at) in (7,8,9,10,11,12) then '02 Manhã'
when EXTRACT(HOUR FROM a.created_at) in (13,14,15,16,17,18) then '03 Tarde'
when EXTRACT(HOUR FROM a.created_at) in (19,20,21,22,23) then '04 Noite'
else 'NA' end as Periodo_Tranx
,OPERACAO
,qtdtransacoes
,valor
from base_Cash_In_Out a
left join `eai-datalake-data-sandbox.core.customers`  cl_CPF_CNPJ_Origem on cl_CPF_CNPJ_Origem.document = a.CPF_CNPJ_Origem
left join `eai-datalake-data-sandbox.core.customers`  cl_CPF_CNPJ_Destino on cl_CPF_CNPJ_Destino.document = a.CPF_CNPJ_Destino

)
select
a.Dt_Transacao
,case   
    when a.StatusConta is null then 'Não é Cliente KMV'
    else a.StatusConta 
end as StatusConta 
,a.flow
,a.type
,CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Origem), DAY) <=1 THEN '00_<1DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Origem), DAY) <=5 THEN '01_<5DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Origem), DAY) <=30 THEN '02_<30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Origem), DAY) <=60 THEN '03_<60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Origem), DAY) <=90 THEN '04_<90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Origem), DAY) <=120 THEN '05_<120DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Origem), DAY) <=160 THEN '06_<160DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Origem), DAY) <=190 THEN '07_<190DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Origem), DAY) <=220 THEN '08_<220DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Origem), DAY) <=260 THEN '09_<260DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Origem), DAY) <=290 THEN '10_<290DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Origem), DAY) <=365 THEN '11_1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Origem), DAY) >=365 THEN '12_+1ANO'
    else '13_NaoTemConta'
END AS Temp_Conta_Origem
,cl_ori.uuid as CustomerID_Origem
,a.CPF_CNPJ_Origem
,CASE 
    WHEN LENGTH(a.CPF_CNPJ_Origem) = 11 THEN 'CPF'
    WHEN LENGTH(a.CPF_CNPJ_Origem) = 14 THEN 'CNPJ'
    ELSE 'Verificar'
END AS Flag_tipo_documento_Origem
,a.Origem_Nome
,a.Origem_Banco

,CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Destino), DAY) <=1 THEN '00_<1DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Destino), DAY) <=5 THEN '01_<5DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Destino), DAY) <=30 THEN '02_<30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Destino), DAY) <=60 THEN '03_<60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Destino), DAY) <=90 THEN '04_<90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Destino), DAY) <=120 THEN '05_<120DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Destino), DAY) <=160 THEN '06_<160DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Destino), DAY) <=190 THEN '07_<190DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Destino), DAY) <=220 THEN '08_<220DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Destino), DAY) <=260 THEN '09_<260DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Destino), DAY) <=290 THEN '10_<290DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Destino), DAY) <=365 THEN '11_1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abert_Destino), DAY) >=365 THEN '12_+1ANO'
    else '13_NaoTemConta'
END AS Temp_Conta_Destino
,a.CPF_CNPJ_Destino
,CASE 
    WHEN LENGTH(a.CPF_CNPJ_Destino) = 11 THEN 'CPF'
    WHEN LENGTH(a.CPF_CNPJ_Destino) = 14 THEN 'CNPJ'
    ELSE 'Verificar'
END AS Flag_tipo_documento_Destino
,cl_des.uuid as CustomerID_Destino
,a.Destino_Nome
,a.Destino_Banco
,a.Flag_Titularidade
,a.Intervalo_Valor
,a.Periodo_Tranx
,a.OPERACAO
,a.qtdtransacoes
,a.valor
,codigo_transacao
,case
    when codigo_transacao like 'D%' then 'Devolução'
    else null
end as Flag_codigo
from base_Consolidada a 
left join `eai-datalake-data-sandbox.core.customers` as cl_ori
on a.CPF_CNPJ_Origem = cl_ori.document
left join `eai-datalake-data-sandbox.core.customers` as cl_des
on a.CPF_CNPJ_Destino = cl_des.document


--where 
--CPF_CNPJ_Origem = '02028427205'
--CPF_CNPJ_Destino = '29748825000170'
--where type = 'CASH-OUT'
--CPF_Origem = 'CUS-9cfe3f4c-c230-45b7-ab84-816b0637e102'
--select * from base_Cash_In_Out where CPF_CNPJ_Destino = 'CUS-9cfe3f4c-c230-45b7-ab84-816b0637e102'


    /* select
     FORMAT_DATE("%Y%m",a.created_at)as Safra_Tranx
     ,a.customer_id
     ,a.flow
     ,a.type
     ,a.flowoperation
     ,Case 
               When valor <100 Then '1-0 a 99'
               When valor >=100 and valor <=300 Then '2-101 a 300'
               When valor > 300 and valor <=500 Then '3-301 a 500'
               When valor > 500 and valor <=1000 Then '4-501 a 1.000'
               When valor > 1000 and valor <=1500 Then '5-1001 a 1500'
               When valor > 1500 and valor <=2000 Then '6-1501 a 2000'
               When valor > 2000 and valor <=3000 Then '7-2001 a 3000'
               When valor > 3000 and valor <=5000 Then '8-3001 a 5000'
               When valor > 5000                  Then '9-5001>'
      End as Intervalo_Valor
     ,case 
          when EXTRACT(HOUR FROM a.created_at) in (0,1,2,3,4,5,6) then '01 Madrugada'
          when EXTRACT(HOUR FROM a.created_at) in (7,8,9,10,11,12) then '02 Manhã'
          when EXTRACT(HOUR FROM a.created_at) in (13,14,15,16,17,18) then '03 Tarde'
          when EXTRACT(HOUR FROM a.created_at) in (19,20,21,22,23) then '04 Noite'
     else 'NA' end as Periodo_Tranx
     ,Sum(a.qtdtransacoes) as qtdtransacoes
     ,Sum(a.valor) as valor
     
  
     from base_Cash_In_Out a 
     where
     a.customer_id = 'CUS-439529de-bb5c-4774-a0f1-065c138da3d8'
     --type = 'CASH-IN'
     group by 1,2,3,4,5,6,7*/