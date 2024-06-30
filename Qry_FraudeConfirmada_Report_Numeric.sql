/*

 SELECT
    distinct
    *
  FROM   `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_1` 
*/

-- Casos_Fraude_202101_202207

DECLARE INICIO_MES DATE;
DECLARE FIM_MES DATE;

SET INICIO_MES = '2023-10-01';
SET FIM_MES = '2023-10-01';


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_1` AS 


--base_dados_consulta as (

            with
            base as (
            select distinct
                   cl.document as Cpf,
                   cl.uuid,
                   cl.created_at as dt_abertura,
                   ev.observation as motivo,
                   ev.user_name,
                   ev.event_date,
                   RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
            from  `eai-datalake-data-sandbox.core.customers`   cl                                           
            left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id 
            where date(ev.event_date) between '2024-01-01' and '2024-01-31'
            )
             select * from base 
             where Rank_Ult_Status = 1
             and  motivo = 'Fraude confirmada'
;

--),base_Pix_OUT as (
-- SELECT * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_2`
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_2` AS 

SELECT 
  distinct
  '34656383000172' as CNPJ_ES
  ,FORMAT_DATETIME("%Y%m%d",Cash_Transaction.created_at) as Data_do_Evento
  ,case
    when pix_payee.document in ('46392130000703','00394460005887') then '16'
  else '18' end as Tipo_de_fraude_golpe
  ,'03' as Avaliacao_da_Conta
  ,'02' as Conhecimento_da_Fraude_Golpe
  ,'N' as CNPJ_da_Entidade_Comunicante
  ,case
    when Cash_Transaction.flow = 'P2P' then '01'
    when Cash_Transaction.flow = 'TED' then '02'
    when Cash_Transaction.flow = 'PIX' then '03'
  else '11' end as Instrumento_Utilizado
  ,'01' as Tipo_de_Cliente_conta_evento
  ,cast(bd.Cpf as string) as CPF_CNPJ_Cliente_conta_evento
  ,FORMAT_DATETIME("%Y%m%d",cl.created_at) as Inicio_do_Relacionamento
  ,cc1.bank_account_agency as Agencia
  ,cc1.bank_account_number||cc1.bank_account_check_number as Conta
  ,'N' as Policia_Comunicada
  ,'05' as Status_da_conta
  ,Cash_Transaction.amount/100 as Valor_do_Evento
  ,0 as Valor_Bloqueado
  ,0 as Valor_Recuperado
  ,0 as Valor_Perda_Operacional
  ,'N' as IF_Recebedora_Comunicada
  ,codban.CNPJ as CNPJ_IF_Recebedora
  ,CASE
      WHEN CHAR_LENGTH(pix_payee.document) >=14 THEN '02'
      WHEN CHAR_LENGTH(pix_payee.document) <=11 THEN '01'
    ELSE'N/A'  END  AS Tipo_de_Cliente_conta_credito
  --,cast(pix_payee.document as numeric) as CPF_CNPJ_Cliente_conta_credito
  ,pix_payee.document as CPF_CNPJ_Cliente_conta_credito
  ,pix_payee.agency_number as Agencia_Fav
  ,pix_payee.account_number||"-"||pix_payee.account_check_number as Conta_Fav
  ,key_type as Tipo_de_chave_PIX
  ,pix.key_value as Chave_PIX
  ,'N' as Relato_de_Fraude_PIX
  ,pix_payee.bank_name as Banco_Favorecido
  ,pix.type || '_PIX' as Tipo_Transacao
  ,FORMAT_DATE("%Y%m",Cash_Transaction.created_at)as Safra_Tranx
  ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
  ,FORMAT_DATE("%Y%m",bd.event_date)as Safra_Bloqueio

FROM `eai-datalake-data-sandbox.cashback.pix_payer`                                 pix_payer 
LEFT JOIN `eai-datalake-data-sandbox.core.customers`                                cl                ON pix_payer.payer_id = cl.uuid
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix`                                  pix               ON pix_payer.pix_id = pix.id 
LEFT JOIN `eai-datalake-data-sandbox.cashback.cash_transaction`                     Cash_Transaction  ON pix.cash_transaction_id = Cash_Transaction.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_payee`                            pix_payee         ON pix.id = pix_payee.pix_id
LEFT JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.TB_COD_BANCO_ISPB`  codban            ON codban.ISPB  = cast(pix_payee.bank_ispb as numeric)
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_1`          bd                ON cast(bd.Cpf as string) = cl.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`                   cc1          ON cc1.bank_token = cast(bd.Cpf as string)

where 
pix.status IN ('APPROVED')
--and date (pix_payer.created_at)  between '2022-07-01' and '2022-10-30'
and Cash_Transaction.amount > 0
and pix.type = 'CASH_OUT'
--order by 9 

;

--), base_Pix_IN as (
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_3`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_3` AS 

SELECT 
distinct

  '34656383000172' as CNPJ_ES
  ,FORMAT_DATETIME("%Y%m%d",Cash_Transaction.created_at) as Data_do_Evento
  ,case
    when pix_payer.document in ('46392130000703','00394460005887') then '16'
  else '18' end as Tipo_de_fraude_golpe
  ,'03' as Avaliacao_da_Conta
  ,'02' as Conhecimento_da_Fraude_Golpe
  ,'N' as CNPJ_da_Entidade_Comunicante
    ,case
    when Cash_Transaction.flow = 'P2P' then '01'
    when Cash_Transaction.flow = 'TED' then '02'
    when Cash_Transaction.flow = 'PIX' then '03'
  else '11' end as Instrumento_Utilizado
  ,'01' as Tipo_de_Cliente_conta_evento
  ,cast(cc1.bank_token as String) as CPF_CNPJ_Cliente_conta_evento
  ,FORMAT_DATETIME("%Y%m%d",cl_fav.created_at) as Inicio_do_Relacionamento
  ,cc1.bank_account_agency as Agencia
  ,cc1.bank_account_number||cc1.bank_account_check_number as Conta
  ,'N' as Policia_Comunicada
  ,'05' as Status_da_conta
  ,Cash_Transaction.amount/100 as Valor_do_Evento
  ,0 as Valor_Bloqueado
  ,0 as Valor_Recuperado
  ,0 as Valor_Perda_Operacional
  ,'N' as IF_Recebedora_Comunicada
  ,'34656383000172' as CNPJ_IF_Recebedora
  ,CASE
    WHEN CHAR_LENGTH(pix_payer.document) >=14 THEN '02'
    WHEN CHAR_LENGTH(pix_payer.document) <=11 THEN '01'
  ELSE'N/A'  END  AS Tipo_de_Cliente_conta_credito
  --,cast(pix_payer.document as numeric) as CPF_CNPJ_Cliente_conta_credito
  ,pix_payer.document as CPF_CNPJ_Cliente_conta_credito
  ,pix_payer.agency_number as Agencia_Fav
  ,pix_payer.account_number||"-"||pix_payer.account_check_number as Conta_Fav
  ,key_type as Tipo_de_chave_PIX
  ,pix.key_value as Chave_PIX
  ,'N' as Relato_de_Fraude_PIX
  ,pix_payer.bank_name as Banco_Favorecido
  ,pix.type || '_PIX' as Tipo_Transacao
  ,FORMAT_DATE("%Y%m",Cash_Transaction.created_at)as Safra_Tranx
  ,FORMAT_DATE("%Y%m",cl_fav.created_at)as Safra_Abertura
  ,FORMAT_DATE("%Y%m",bd.event_date)as Safra_Bloqueio

FROM `eai-datalake-data-sandbox.cashback.pix_in_payer`                              pix_payer 
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix`                                  pix               ON pix_payer.pix_id = pix.id 
LEFT JOIN `eai-datalake-data-sandbox.cashback.cash_transaction`                     Cash_Transaction  ON pix.cash_transaction_id = Cash_Transaction.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_in_payee`                         pix_payee         ON pix.id = pix_payee.pix_id
LEFT JOIN `eai-datalake-data-sandbox.core.customers`                                cl_fav            ON pix_payee.payee_id = cl_fav.uuid
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_1`          bd                ON cast(bd.Cpf as string) = cl_fav.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc1               ON cc1.bank_token = cast(bd.Cpf as string)

where 
pix.status IN ('APPROVED')
--and date (pix_payer.created_at)   between '2022-07-01' and '2022-10-30'
and Cash_Transaction.amount >0
and pix.type = 'CASH_IN'

;
--), base_p2p as (
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_4`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_4` AS 
SELECT  
distinct
 '34656383000172' as CNPJ_ES
  ,FORMAT_DATETIME("%Y%m%d",Cash_Transaction.created_at) as Data_do_Evento
  ,case
    when cc.bank_token in ('46392130000703','00394460005887') then '16'
  else '18' end as Tipo_de_fraude_golpe
  ,'03' as Avaliacao_da_Conta
  ,'02' as Conhecimento_da_Fraude_Golpe
  ,'N' as CNPJ_da_Entidade_Comunicante
  ,case
    when Cash_Transaction.flow = 'P2P' then '01'
    when Cash_Transaction.flow = 'TED' then '02'
    when Cash_Transaction.flow = 'PIX' then '03'
  else '11' end as Instrumento_Utilizado
  ,'01' as Tipo_de_Cliente_conta_evento
  ,cc1.bank_token as CPF_CNPJ_Cliente_conta_evento
  ,FORMAT_DATETIME("%Y%m%d",cl.created_at) as Inicio_do_Relacionamento
  ,cc1.bank_account_agency as Agencia
  ,cc1.bank_account_number||cc1.bank_account_check_number as Conta
  ,'N' as Policia_Comunicada
  ,'05' as Status_da_conta
  ,Cash_Transaction.amount/100 as Valor_do_Evento
  ,0 as Valor_Bloqueado
  ,0 as Valor_Recuperado
  ,0 as Valor_Perda_Operacional
  ,'N' as IF_Recebedora_Comunicada
  ,'34656383000172' as CNPJ_IF_Recebedora
  ,CASE
    WHEN CHAR_LENGTH(cc.bank_token ) >=14 THEN '02'
    WHEN CHAR_LENGTH(cc.bank_token ) <=11 THEN '01'
  ELSE'N/A'  END  AS Tipo_de_Cliente_conta_credito
  --,cast(cc.bank_token as numeric) as CPF_CNPJ_Cliente_conta_credito
  ,cc.bank_token as CPF_CNPJ_Cliente_conta_credito
  ,cc.bank_account_agency as Agencia_Fav
  ,cc.bank_account_number||cc.bank_account_check_number as Conta_Fav
  ,'0' as Tipo_de_chave_PIX
  ,'0' as Chave_PIX
  ,'N' as Relato_de_Fraude_PIX
  ,'ABASTECEAI' as Banco_Favorecido
  ,p2p.type || '_P2P' as Tipo_Transacao
  ,FORMAT_DATE("%Y%m",Cash_Transaction.created_at)as Safra_Tranx
  ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
  ,FORMAT_DATE("%Y%m",bd.event_date)as Safra_Bloqueio


FROM `eai-datalake-data-sandbox.cashback.p2p_payer`                                 p2p_payer
LEFT JOIN `eai-datalake-data-sandbox.core.customers`                                cl                ON p2p_payer.payer_id = cl.uuid
JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`                   cc1               ON cc1.bank_token = cl.document
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p`                                  p2p               ON p2p_payer.p2p_id = p2p.id 
LEFT JOIN `eai-datalake-data-sandbox.cashback.cash_transaction`                     Cash_Transaction  ON p2p.cash_transaction_id = Cash_Transaction.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payee`                            p2p_payee         ON p2p.id = p2p_payee.p2p_id
LEFT JOIN `eai-datalake-data-sandbox.core.customers`                                cl_fav            ON p2p_payee.payee_id = cl_fav.uuid
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_1`          bd                ON cast(bd.Cpf as string) = cl.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc                ON cc.bank_token = cl_fav.document
where 
p2p.status IN ('APPROVED')
--and date (p2p_payer.created_at)   between '2022-07-01' and '2022-10-30'
and Cash_Transaction.amount >0
;

--), base_ted_out as (

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_5` AS 

SELECT 
distinct 
'34656383000172' as CNPJ_ES
  ,FORMAT_DATETIME("%Y%m%d",Cash_Transaction.created_at) as Data_do_Evento
  ,case
    when ted_payee.document in ('46392130000703','00394460005887') then '16'
  else '18' end as Tipo_de_fraude_golpe
  ,'03' as Avaliacao_da_Conta
  ,'02' as Conhecimento_da_Fraude_Golpe
  ,'N' as CNPJ_da_Entidade_Comunicante
  ,case
    when Cash_Transaction.flow = 'P2P' then '01'
    when Cash_Transaction.flow = 'TED' then '02'
    when Cash_Transaction.flow = 'PIX' then '03'
  else '11' end as Instrumento_Utilizado
  ,'01' as Tipo_de_Cliente_conta_evento
  ,cl.document as CPF_CNPJ_Cliente_conta_evento
  ,FORMAT_DATETIME("%Y%m%d",cl.created_at) as Inicio_do_Relacionamento
  ,cc1.bank_account_agency as Agencia
  ,cc1.bank_account_number||cc1.bank_account_check_number as Conta
  ,'N' as Policia_Comunicada
  ,'05' as Status_da_conta
  ,Cash_Transaction.amount/100 as Valor_do_Evento
  ,0 as Valor_Bloqueado
  ,0 as Valor_Recuperado
  ,0 as Valor_Perda_Operacional
  ,'N' as IF_Recebedora_Comunicada
  ,'0' as CNPJ_IF_Recebedora
  ,CASE
    WHEN CHAR_LENGTH(ted_payee.document ) >=14 THEN '02'
    WHEN CHAR_LENGTH(ted_payee.document  ) <=11 THEN '01'
  ELSE'N/A'  END  AS Tipo_de_Cliente_conta_credito
  --,cast(ted_payee.document as numeric) as CPF_CNPJ_Cliente_conta_credito
  ,ted_payee.document as CPF_CNPJ_Cliente_conta_credito
  ,ted_payee.agency_number as Agencia_Fav
  ,ted_payee.account_number||"-"||ted_payee.account_check_number as Conta_Fav
  ,'0' as Tipo_de_chave_PIX
  ,'0' as Chave_PIX
  ,'N' as Relato_de_Fraude_PIX
  ,ted_payee.bank_name as Banco_Favorecido
  ,ted.type || '_TED' as Tipo_Transacao
  ,FORMAT_DATE("%Y%m",Cash_Transaction.created_at)as Safra_Tranx
  ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
  ,FORMAT_DATE("%Y%m",bd.event_date)as Safra_Bloqueio

FROM `eai-datalake-data-sandbox.cashback.ted_payer`                                 ted_payer
LEFT JOIN `eai-datalake-data-sandbox.core.customers`                                cl                ON ted_payer.payer_id = cl.uuid
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted`                                  ted               ON ted_payer.ted_id = ted.id 
LEFT JOIN `eai-datalake-data-sandbox.cashback.cash_transaction`                     Cash_Transaction  ON ted.cash_transaction_id = Cash_Transaction.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_payee`                            ted_payee         ON ted.id = ted_payee.ted_id
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_1`          bd                ON cast(bd.Cpf as string) = cl.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc1               ON cc1.bank_token = cl.document

where 
ted.status IN ('APPROVED')
--and date (ted_payer.created_at)   between '2022-07-01' and '2022-10-30'
and Cash_Transaction.amount >0

;
--),base_ted_in as (
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_6`
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_6` AS 

SELECT 
distinct 
'34656383000172' as CNPJ_ES
  ,FORMAT_DATETIME("%Y%m%d",Cash_Transaction.created_at) as Data_do_Evento
  ,case
    when ted_payer.document in ('46392130000703','00394460005887') then '16'
  else '18' end as Tipo_de_fraude_golpe
  ,'03' as Avaliacao_da_Conta
  ,'02' as Conhecimento_da_Fraude_Golpe
  ,'N' as CNPJ_da_Entidade_Comunicante
  ,case
    when Cash_Transaction.flow = 'P2P' then '01'
    when Cash_Transaction.flow = 'TED' then '02'
    when Cash_Transaction.flow = 'PIX' then '03'
  else '11' end as Instrumento_Utilizado
  ,'01' as Tipo_de_Cliente_conta_evento
  ,cl_fav.document as CPF_CNPJ_Cliente_conta_evento
  ,FORMAT_DATETIME("%Y%m%d",cl_fav.created_at) as Inicio_do_Relacionamento
  ,cc1.bank_account_agency as Agencia
  ,cc1.bank_account_number||cc1.bank_account_check_number as Conta
  ,'N' as Policia_Comunicada
  ,'05' as Status_da_conta
  ,Cash_Transaction.amount/100 as Valor_do_Evento
  ,0 as Valor_Bloqueado
  ,0 as Valor_Recuperado
  ,0 as Valor_Perda_Operacional
  ,'N' as IF_Recebedora_Comunicada
  ,'34656383000172'  as CNPJ_IF_Recebedora
  ,CASE
    WHEN CHARACTER_LENGTH(cast(cast(ted_payer.document as numeric) as string)) >=12 THEN '02'
    WHEN CHARACTER_LENGTH(cast(cast(ted_payer.document as numeric) as string)) <=11 THEN '01'
  ELSE'N/A'  END  AS Tipo_de_Cliente_conta_credito
  --,cast(ted_payer.document as numeric) as CPF_CNPJ_Cliente_conta_credito
  ,ted_payer.document as CPF_CNPJ_Cliente_conta_credito
  ,ted_payer.agency_number as Agencia_Fav
  ,ted_payer.account_number||"-"||ted_payer.account_check_number as Conta_Fav
  ,'0' as Tipo_de_chave_PIX
  ,'0' as Chave_PIX
  ,'N' as Relato_de_Fraude_PIX
  ,ted_payer.bank_name as Banco_Favorecido
  ,ted.type || '_TED' as Tipo_Transacao
  ,FORMAT_DATE("%Y%m",Cash_Transaction.created_at)as Safra_Tranx
  ,FORMAT_DATE("%Y%m",cl_fav.created_at)as Safra_Abertura
  ,FORMAT_DATE("%Y%m",bd.event_date)as Safra_Bloqueio

FROM `eai-datalake-data-sandbox.cashback.ted_in_payer`                              ted_payer 
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted`                                  ted               ON ted_payer.ted_id = ted.id 
LEFT JOIN `eai-datalake-data-sandbox.cashback.cash_transaction`                     Cash_Transaction  ON ted.cash_transaction_id = Cash_Transaction.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_in_payee`                         ted_payee         ON ted.id = ted_payee.ted_id
LEFT JOIN `eai-datalake-data-sandbox.core.customers`                                cl_fav            ON ted_payee.payee_id = cl_fav.uuid
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_1`          bd                ON cast(bd.Cpf as string) = cl_fav.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc1               ON cc1.bank_token = cast(bd.Cpf as string)

where 
ted.status IN ('APPROVED')
--and date (ted_payer.created_at)   between '2022-07-01' and '2022-10-30'
and Cash_Transaction.amount >0

;

--),base_pagamentos_cartao as (
-- SELECT distinct * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_7` where CPF_CNPJ_Cliente_conta_evento = '00362163057'

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_7` AS 

SELECT 
distinct
'34656383000172' as CNPJ_ES
  --,gateway_id
  ,FORMAT_DATETIME("%Y%m%d",pt.created_at) as Data_do_Evento
  ,'18'as Tipo_de_fraude_golpe
  ,'03' as Avaliacao_da_Conta
  ,'02' as Conhecimento_da_Fraude_Golpe
  ,'N' as CNPJ_da_Entidade_Comunicante
  ,case
    when pt.payment_method = 'CREDIT_CARD' then '09'
    when pt.payment_method = 'BALANCE' then '11'
  else '11' end as Instrumento_Utilizado
  ,'01' as Tipo_de_Cliente_conta_evento
  ,cast(cl.document as string) as CPF_CNPJ_Cliente_conta_evento
  ,FORMAT_DATETIME("%Y%m%d",cl.created_at) as Inicio_do_Relacionamento
  ,cc1.bank_account_agency as Agencia
  ,cc1.bank_account_number||cc1.bank_account_check_number as Conta
  ,'N' as Policia_Comunicada
  ,'05' as Status_da_conta
  ,pt.transaction_value/1 as Valor_do_Evento
  ,0 as Valor_Bloqueado
  ,0 as Valor_Recuperado
  ,0 as Valor_Perda_Operacional
  ,'N' as IF_Recebedora_Comunicada
  ,'0' as CNPJ_IF_Recebedora
  ,'0' as Tipo_de_Cliente_conta_credito
  --,0 as CPF_CNPJ_Cliente_conta_credito
  ,'0'  as CPF_CNPJ_Cliente_conta_credito
  ,'0' as Agencia_Fav
  ,'0' as Conta_Fav
  ,'0' as Tipo_de_chave_PIX
  ,'0' as Chave_PIX
  ,'N' as Relato_de_Fraude_PIX
  ,ban.Emissor_do_Banco as Banco_Favorecido
  ,pt.payment_method as Tipo_Transacao
  ,FORMAT_DATE("%Y%m",pt.created_at)as Safra_Tranx
  ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
  ,FORMAT_DATE("%Y%m",bd.event_date)as Safra_Bloqueio
  

FROM `eai-datalake-data-sandbox.payment.payment_transaction`                                          pt
join `eai-datalake-data-sandbox.payment.payment`                                                      b     on b.id = pt.payment_id
join ( 
            with
            base as (
            select distinct
                   cl.document,
                   cl.uuid,
                   cl.created_at,
                   ev.observation,
                   ev.user_name,
                   ev.event_date,
                   RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
            from  `eai-datalake-data-sandbox.core.customers`   cl                                           
            left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id 
            )
             select * from base where Rank_Ult_Status = 1
             ) cl  on b.customer_id = cl.uuid
join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_1`                               bd   on cast(bd.Cpf as string) = cl.document
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias`          ub   on ub.customer_id = b.customer_id
join `eai-datalake-data-sandbox.core.orders`                                                            ord   on ord.uuid = b.order_id
join `eai-datalake-data-sandbox.payment.payment_instrument`                              payment_instrument   on pt.payment_instrument_id = payment_instrument.id
join `eai-datalake-data-sandbox.payment.customer_card`                                                 card   on payment_instrument.uuid = card.uuid
join (select distinct * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos` )     ban   on cast(ban.BIN as string) = card.bin
left join `eai-datalake-data-sandbox.payment.payment_customer_account`                                  cc1   on cc1.bank_token = cast(bd.Cpf as string)

--left join (select distinct * from `eai-datalake-data-sandbox.payment.customer_card` where document = '00362163057')       card  on card.document = cast(bd.Cpf as string)
--left join `eai-datalake-data-sandbox.payment.payment_customer_account`                                cc1   on cc1.bank_token = cast(bd.Cpf as string)
--left join (select distinct * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos` )  ban on cast(ban.BIN as string) = card.bin

WHERE 
pt.payment_method in ('CREDIT_CARD')
--and date (pt.created_at)   between '2022-07-01' and '2022-10-30'
and pt.status in ('AUTHORIZED','COMPLETE','COMPLETED','SETTLEMENT')
and pt.transaction_value > 0
--and cl.document = '22768693804'
--and pt.gateway_id = '07vfvncg'
order by 9
;

--), base_pagamentos_Saldo as (

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_8` AS 

SELECT 
distinct
'34656383000172' as CNPJ_ES
  ,FORMAT_DATETIME("%Y%m%d",pt.created_at) as Data_do_Evento
  ,'18'as Tipo_de_fraude_golpe
  ,'03' as Avaliacao_da_Conta
  ,'02' as Conhecimento_da_Fraude_Golpe
  ,'N' as CNPJ_da_Entidade_Comunicante
  ,case
    when pt.payment_method = 'CREDIT_CARD' then '09'
    when pt.payment_method = 'BALANCE' then '11'
  else '11' end as Instrumento_Utilizado
  ,'01' as Tipo_de_Cliente_conta_evento
  ,cl.document as CPF_CNPJ_Cliente_conta_evento
  ,FORMAT_DATETIME("%Y%m%d",cl.created_at) as Inicio_do_Relacionamento
  ,cc1.bank_account_agency as Agencia
  ,cc1.bank_account_number||cc1.bank_account_check_number as Conta
  ,'N' as Policia_Comunicada
  ,'05' as Status_da_conta
  ,pt.transaction_value/1 as Valor_do_Evento
  ,0 as Valor_Bloqueado
  ,0 as Valor_Recuperado
  ,0 as Valor_Perda_Operacional
  ,'N' as IF_Recebedora_Comunicada
  ,'0' as CNPJ_IF_Recebedora
  ,'0' as Tipo_de_Cliente_conta_credito
  --,0  as CPF_CNPJ_Cliente_conta_credito
  ,'0'  as CPF_CNPJ_Cliente_conta_credito
  ,'0' as Agencia_Fav
  ,'0' as Conta_Fav
  ,'0' as Tipo_de_chave_PIX
  ,'0' as Chave_PIX
  ,'N' as Relato_de_Fraude_PIX
  ,'ABASTECEAI' as Banco_Favorecido
  ,pt.payment_method as Tipo_Transacao
  ,FORMAT_DATE("%Y%m",pt.created_at)as Safra_Tranx
  ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
  ,FORMAT_DATE("%Y%m",bd.event_date)as Safra_Bloqueio
  

FROM `eai-datalake-data-sandbox.payment.payment_transaction`                                          pt
join `eai-datalake-data-sandbox.payment.payment`                                                      b   on b.id = pt.payment_id
join ( 
            with
            base as (
            select distinct
                   cl.document,
                   cl.uuid,
                   cl.created_at,
                   ev.observation,
                   ev.user_name,
                   ev.event_date,
                   RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
            from  `eai-datalake-data-sandbox.core.customers`   cl                                           
            left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id 
            )
             select * from base where Rank_Ult_Status = 1
             ) cl  on b.customer_id = cl.uuid
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias`       ub    on ub.customer_id = b.customer_id
left join `eai-datalake-data-sandbox.core.orders`                                                     ord   on ord.uuid = b.order_id
join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_1`                            bd    on cast(bd.Cpf as string) = cl.document
left join `eai-datalake-data-sandbox.payment.payment_customer_account`                                cc1   on cc1.bank_token = cast(bd.Cpf as string)

WHERE 
pt.payment_method in ('BALANCE')
--and date (pt.created_at)   between '2022-07-01' and '2022-10-30'
and pt.status in ('AUTHORIZED','COMPLETE','COMPLETED','SETTLEMENT')
and pt.transaction_value >0


;


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_final_1` AS 

select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_2`
union all
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_3`
union all
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_4`
union all
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_5`
union all
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_6`
union all
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_7`
union all
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_8`

;
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_final`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_final` AS 

with

base_final as (
              select
              distinct
              a.CNPJ_ES
              ,a.Data_do_Evento
              ,a.Tipo_de_fraude_golpe
              ,a.Avaliacao_da_Conta
              ,a.Conhecimento_da_Fraude_Golpe
              ,a.CNPJ_da_Entidade_Comunicante
              ,a.Instrumento_Utilizado
              ,a.Tipo_de_Cliente_conta_evento
              ,a.CPF_CNPJ_Cliente_conta_evento
              ,a.Inicio_do_Relacionamento
              ,a.Agencia
              ,a.Conta
              ,a.Policia_Comunicada
              ,a.Status_da_conta
              ,a.Valor_do_Evento
              ,a.Valor_Bloqueado
              ,a.Valor_Recuperado
              ,a.Valor_Perda_Operacional
              ,a.IF_Recebedora_Comunicada
              ,a.CNPJ_IF_Recebedora
              ,a.Tipo_de_Cliente_conta_credito
              ,a.CPF_CNPJ_Cliente_conta_credito
              ,a.Agencia_Fav
              ,a.Conta_Fav
              ,a.Tipo_de_chave_PIX
              ,a.Chave_PIX
              ,a.Relato_de_Fraude_PIX
              ,a.Banco_Favorecido
              ,a.Tipo_Transacao
              ,a.Safra_Tranx
              ,a.Safra_Abertura
              ,a.Safra_Bloqueio
              ,RANK() OVER (PARTITION BY a.CPF_CNPJ_Cliente_conta_evento ORDER BY sum(a.Valor_do_Evento) desc) AS Rank_Tranx

              from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_final_1` a
              group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32
)

select
              distinct
              a.CNPJ_ES
              ,a.Data_do_Evento
              ,a.Tipo_de_fraude_golpe
              ,a.Avaliacao_da_Conta
              ,a.Conhecimento_da_Fraude_Golpe
              ,a.CNPJ_da_Entidade_Comunicante
              ,a.Instrumento_Utilizado
              ,a.Tipo_de_Cliente_conta_evento
              ,a.CPF_CNPJ_Cliente_conta_evento
              ,a.Inicio_do_Relacionamento
              ,a.Agencia
              ,a.Conta
              ,a.Policia_Comunicada
              ,a.Status_da_conta
              ,a.Valor_do_Evento
              --,sl.SALDO as Valor_Bloqueado
              ,case when a.Rank_Tranx = 1 then  sl.SALDO else 0 end as Valor_Bloqueado
              ,a.Valor_Recuperado
              ,a.Valor_Perda_Operacional
              ,a.IF_Recebedora_Comunicada
              ,a.CNPJ_IF_Recebedora
              ,a.Tipo_de_Cliente_conta_credito
              ,a.CPF_CNPJ_Cliente_conta_credito
              ,a.Agencia_Fav
              ,a.Conta_Fav
              ,a.Tipo_de_chave_PIX
              ,a.Chave_PIX
              ,a.Relato_de_Fraude_PIX
              ,a.Banco_Favorecido
              ,a.Tipo_Transacao
              ,a.Safra_Tranx
              ,a.Safra_Abertura
              ,a.Safra_Bloqueio

              from base_final a
              left join (with
                          base as (
                          SELECT 
                          T.numerodocumento as  DOCUMENTO,                     -- CPF / CNPJ    
                          round(sum(T.valor),2) as SALDO                   -- SOMA DOS VALORES QUE COMPÕEM O SALDO
                          FROM `eai-datalake-data-sandbox.orbitall.tb_topaz` T
                          where    status = 'MOVIMENTAÇÃO EXECUTADA'       -- STATUS NECESSARIO DE TRANSACOES CONCLUIDAS    
                          and T.data <= CURRENT_DATE()-1          -- FIXAR COM A DATA COM A QUAL SE PRETENDE ANALISAR O SALDO    
                          and length(T.numerodocumento) <= 11     -- REMOVER CASO QUEIRA TRAZER TAMBÉM CNPJ

                          group by 1
                          ) select * from base a
                                      join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_BV_final_1`  bd on bd.CPF_CNPJ_Cliente_conta_evento = a.DOCUMENTO
                        )sl on sl.DOCUMENTO = a.CPF_CNPJ_Cliente_conta_evento
                --where a.CPF_CNPJ_Cliente_conta_evento ='07054400879'
              order by 9