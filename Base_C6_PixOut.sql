with

Base_Pixuot as (

SELECT 
distinct
pix_payer.payer_id as  CustomerID
,cl.document as CPF_Origem
,pix_payer.name as ClienteOrigem
,pix.end_to_end_id
,pix.status
,pix.key_value
,pix.key_type
,pix.type
,pix.scheduled_date
,Cash_Transaction.flow
,(Cash_Transaction.amount)/100 as Vl_Tranx
,Cash_Transaction.description
,Cash_Transaction.created_at AS DT_TRANX
,FORMAT_DATE("%Y%m",Cash_Transaction.created_at)as Safra_Tranx
,pix_payee.name AS Favorecido
,pix_payee.document AS CPF_Favorecido
,case
when	char_length(pix_payee.document) >=14 then 'CNPJ'
when	char_length(pix_payee.document) <=11 then 'CPF'
else 'N/A' end as Flag_Favorecida
,case when pix_payee.document = cl.document then 'MesmaTitularidade' else 'OutraTitularidade' end as  Flag_Titularidade
,pix_payee.bank_number 
,pix_payee.agency_number as Agencia_Favorecido
,pix_payee.account_number||"-"||pix_payee.account_check_number as Conta_Favorecido
,pix_payee.bank_name as Banco_Favorecido
,pix_payee.bank_ispb as Cod_Banco_ISPB

FROM `eai-datalake-data-sandbox.cashback.pix_payer`               pix_payer
LEFT JOIN `eai-datalake-data-sandbox.core.customers`              cl                ON pix_payer.payer_id = cl.uuid
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix`                pix               ON pix_payer.pix_id = pix.id 
LEFT JOIN `eai-datalake-data-sandbox.cashback.cash_transaction`   Cash_Transaction  ON pix.cash_transaction_id = Cash_Transaction.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_payee`          pix_payee         ON pix.id = pix_payee.pix_id
WHERE 
date (Cash_Transaction.created_at)  >= '2023-10-27'
AND 
pix.`type` = 'CASH_OUT'
AND pix.`status` = 'APPROVED'
--AND cl.status not in ('BLOCK','BLOCKED','UNBLOCK')
--AND pix_payee.document = '70155110616'
--AND pix_payer.payer_id in ('CUS-2e414b71-f8ac-47f9-a842-0aa3826e302b')
) select * from Base_Pixuot
where CPF_Origem in ()
