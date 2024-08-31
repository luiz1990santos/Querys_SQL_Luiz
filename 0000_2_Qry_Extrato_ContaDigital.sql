--Extrato ContaDigital
/*
select max(created_at),min(created_at) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_ContaDigital` 
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_ContaDigital` 

*/
--CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_ContaDigital` AS 

INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_ContaDigital` 

with

base_ContaDigital as (
SELECT
cash_transaction.created_at as created_at,
cash_transaction.flow,
code,
CASE WHEN (pix.type in ('CASH_IN', 'CASH_IN_REFUND') or ted.type = 'CASH_IN' or p2p.type = 'CASH_IN' or qpo.type = 'CASH_IN' or flow = 'BILLET') THEN 'CASH-IN' 
WHEN (pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') or  ted.type = 'CASH_OUT' or p2p.type = 'CASH_OUT') THEN 'CASH-OUT' 
END AS type,

Case 
when (pix.status = 'APPROVED' or ted.status = 'APPROVED' or p2p.status = 'APPROVED' or billet.status = 'APPROVED' or qpo.status = 'PAID') then 'Aprovado'
when (pix.status != 'APPROVED' or ted.status != 'APPROVED' or p2p.status != 'APPROVED' or billet.status != 'APPROVED' or qpo.status != 'PAID') then 'Negado'
else 'NA' end as Flag_Status,

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
        WHEN cash_transaction.flow = 'PIX' AND pix.type IN ('CASH_IN') THEN 'PIX-IN'
        WHEN cash_transaction.flow = 'PIX' AND pix.type IN ('CASH_IN_REFUND') THEN 'PIX-IN_CASH_IN_REFUND'
        WHEN cash_transaction.flow = 'PIX' AND pix.type IN ('CASH_OUT') THEN 'PIX-OUT'
        WHEN cash_transaction.flow = 'PIX' AND pix.type IN ('CASH_OUT_REFUND') THEN 'PIX-OUT_CASH_OUT_REFUND'
        WHEN cash_transaction.flow = 'TED' AND ted.type IN ('CASH_IN') THEN 'TED-IN'
        WHEN cash_transaction.flow = 'TED' AND ted.type IN ('CASH_OUT') THEN 'TED-OUT'
        WHEN cash_transaction.flow = 'P2P' AND p2p.type IN ('CASH_OUT') THEN 'P2P-OUT'
        WHEN cash_transaction.flow = 'P2P' AND P2P.type IN ('CASH_IN') THEN 'P2P-IN'
        WHEN cash_transaction.flow = 'BILLET' AND billet.type IN ('CASH_IN') THEN 'BOLETO'
        WHEN cash_transaction.flow = 'QRCODE_PIX_OUT' AND QPO.type IN ('CASH_IN') THEN 'QRCODE_PIX_OUT'
        ELSE 'JUDICIAL_DEBT'
    END AS OPERACAO,
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
cash_transaction.created_at > (select max(created_at) as Dt_Transacao from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_ContaDigital` ) and DATE(cash_transaction.created_at) <= current_date - 1
--cash_transaction.created_at  >= (select max(created_at) as created_at  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_ContaDigital` )
--date(cash_transaction.created_at) <= '2024-07-30' --current_date - 2
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
ORDER BY 1
), Base_Consolidada_ContaDigital as (
select
a.*
,case when CPF_CNPJ_Origem = CPF_CNPJ_Destino then 'Mesma_Titularidade' else 'Outra_Titularidade' end as Flag_Titularidade
,case when cl_Orig.CPF_Cliente = a.CPF_CNPJ_Origem then cl_Orig.Flag_Perfil else 'NaoCliente' end as Flag_Perfil_Origem
,case when cl_dest.CPF_Cliente = a.CPF_CNPJ_Destino then cl_dest.Flag_Perfil else 'NaoCliente' end as Flag_Perfil_Destino

from base_ContaDigital a
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` cl_dest on cl_dest.CPF_Cliente = a.CPF_CNPJ_Destino
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` cl_Orig on cl_Orig.CPF_Cliente = a.CPF_CNPJ_Origem
--where CPF_CNPJ_Destino = '08280580670'
)
select
*
from Base_Consolidada_ContaDigital

