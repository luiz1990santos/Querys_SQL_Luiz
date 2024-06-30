------------------------------------------------------------------------------------------------
-- RELATÓRIO BACEN - CONTAS ENCERRADAS --                                                      | 
------------------------------------------------------------------------------------------------
-- VARIÁVEIS DE INFORMAÇÕES DO SEMESTRE VIGENTE                                                |
------------------------------------------------------------------------------------------------


 -- PROCESSO MANUAL:

  DECLARE ano_vigente STRING;
  DECLARE data_criacao_arquivo STRING;
  DECLARE inicio_semestre DATE;
  DECLARE fim_semestre DATE;

  SET ano_vigente = FORMAT_DATE('%Y', CURRENT_DATE());
  SET data_criacao_arquivo = REPLACE(FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()), '-', '');



  
  SET inicio_semestre = DATE(CONCAT(ano_vigente,'-04-01'));
  SET fim_semestre = DATE(CONCAT(ano_vigente,'-04-30'));
  

  /*
  SET inicio_semestre = DATE(CONCAT(ano_vigente,'-07-01'));
  SET fim_semestre = DATE(CONCAT(ano_vigente,'-12-31'));
  */


-----------------------------------------------------------------------------------------------------------


/*

DECLARE data_atual DATE;
DECLARE inicio_semestre DATE;
DECLARE fim_semestre DATE;
DECLARE ano_vigente STRING;
DECLARE data_criacao_arquivo STRING;

SET data_atual = CURRENT_DATE();
SET ano_vigente = FORMAT_DATE('%Y', CURRENT_DATE());
 
IF data_atual BETWEEN DATE(CONCAT(ano_vigente,'-01-01')) AND DATE(CONCAT(ano_vigente,'-06-30')) THEN 
  SET inicio_semestre = DATE(CONCAT(ano_vigente,'-01-01'));
  SET fim_semestre = DATE(CONCAT(ano_vigente,'-06-30'));

ELSEIF data_atual BETWEEN DATE(CONCAT(ano_vigente,'-07-01')) AND DATE(CONCAT(ano_vigente,'-12-31')) THEN 
  SET inicio_semestre = DATE(CONCAT(ano_vigente,'-07-01'));
  SET fim_semestre = DATE(CONCAT(ano_vigente,'-12-31'));
 

END IF;

*/

------------------------------------------------------------------------------------------------

-------------------------------------------------------------------
-- ETAPA 1 - TABELA DE TODOS OS CLIENTES E SEU ÚLTIMO STATUS      |
-------------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas` AS 

--base_dados_consulta as (

with 

base as (
    select distinct
        CASE
            WHEN CHAR_LENGTH(cl.document) >= 14 THEN 'PJ'
            WHEN CHAR_LENGTH(cl.document) <= 11 THEN 'PF'
            ELSE 'N/A'
        END AS `Tipo_PF_ou_PJ`,
        cl.full_name as Nome_Completo_Titular,
        cl.document as CPF,
        cl.status,
        format_date('%Y-%m-%d', cl.created_at) as Data_Abertura_Conta,
        ev.event_date,
        ev.observation as Motivo_Encerramento_Conta,      
        RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
    from  `eai-datalake-data-sandbox.core.customers` cl                                          
    left join `eai-datalake-data-sandbox.core.customer_event` ev
    on ev.customer_id = cl.id
    where cl.status = 'BLOCKED'
    and date(ev.created_at) between '2024-05-01' and '2024-05-31' 
    and ev.observation in ('Conta encerrada','Fraude confirmada')
)
select bd.* from base bd
  --select Motivo_Encerramento_Conta, count(*) from base  
  where Rank_Ult_Status = 1
 

;
-----------------------------------------------------
-- ETAPA 2 - TABELA TRANSAÇÕES PIX OUT APROVADAS    |
-----------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas2`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas2` AS 

SELECT 
  distinct
  '34656383000172' as CNPJ_ES
  ,DATETIME(Cash_Transaction.created_at) as Data_do_Evento
  --,FORMAT_DATETIME("%Y%m%d",Cash_Transaction.created_at) as Data_do_Evento
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
  ,cast(pix_payee.document as string) as CPF_CNPJ_Cliente_conta_credito
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
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas`  bd                ON cast(bd.Cpf as string) = cl.document 
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`                   cc1          ON cc1.bank_token = cast(bd.Cpf as string)

where 
pix.status IN ('APPROVED')
-- and date (pix_payer.created_at) between inicio_trimestre and fim_trimestre
and Cash_Transaction.amount > 0
and pix.type = 'CASH_OUT'
--order by 9 
;

-----------------------------------------------------
-- ETAPA 3 - TABELA TRANSAÇÕES PIX IN APROVADAS     |
-----------------------------------------------------
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas3`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas3` AS 

SELECT 
distinct

  '34656383000172' as CNPJ_ES
  ,DATETIME(Cash_Transaction.created_at) as Data_do_Evento
  --,FORMAT_DATETIME("%Y%m%d",Cash_Transaction.created_at) as Data_do_Evento
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
  ,cast(pix_payer.document as string) as CPF_CNPJ_Cliente_conta_credito
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
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas`          bd                ON cast(bd.Cpf as string) = cl_fav.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc1               ON cc1.bank_token = cast(bd.Cpf as string)

where 
pix.status IN ('APPROVED')
--and date (pix_payer.created_at) between inicio_trimestre and fim_trimestre
and Cash_Transaction.amount > 0
and pix.type = 'CASH_IN'
;

-----------------------------------------------------
-- ETAPA 4 - TABELA TRANSAÇÕES P2P APROVADAS        |
-----------------------------------------------------
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas4`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas4` AS 

SELECT  
distinct
 '34656383000172' as CNPJ_ES
 ,DATETIME(Cash_Transaction.created_at) as Data_do_Evento
  --,FORMAT_DATETIME("%Y%m%d",Cash_Transaction.created_at) as Data_do_Evento
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
  ,cast(cc.bank_token as string) as CPF_CNPJ_Cliente_conta_credito
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
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas`          bd                ON cast(bd.Cpf as string) = cl.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc                ON cc.bank_token = cl_fav.document
where 
p2p.status IN ('APPROVED')
--and date (p2p_payer.created_at) between inicio_trimestre and fim_trimestre
and Cash_Transaction.amount > 0
;

-----------------------------------------------------
-- ETAPA 5 - TABELA TRANSAÇÕES TED OUT APROVADAS    |
-----------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas5`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas5` AS 

SELECT 
distinct 
'34656383000172' as CNPJ_ES
 ,DATETIME(Cash_Transaction.created_at) as Data_do_Evento
  --,FORMAT_DATETIME("%Y%m%d",Cash_Transaction.created_at) as Data_do_Evento
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
  ,cast(ted_payee.document as string) as CPF_CNPJ_Cliente_conta_credito
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
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas`          bd                ON cast(bd.Cpf as string) = cl.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc1               ON cc1.bank_token = cl.document

where 
ted.status IN ('APPROVED')
--and date (ted_payer.created_at) between inicio_trimestre and fim_trimestre
and Cash_Transaction.amount > 0
;

-----------------------------------------------------
-- ETAPA 6 - TABELA TRANSAÇÕES TED IN APROVADAS     |
-----------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas6`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas6` AS 

SELECT 
distinct 
'34656383000172' as CNPJ_ES
 ,DATETIME(Cash_Transaction.created_at) as Data_do_Evento
  --,FORMAT_DATETIME("%Y%m%d",Cash_Transaction.created_at) as Data_do_Evento
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
  ,cast(ted_payer.document as String) as CPF_CNPJ_Cliente_conta_credito
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
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas`          bd                ON cast(bd.Cpf as string) = cl_fav.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc1               ON cc1.bank_token = cast(bd.Cpf as string)

where 
ted.status IN ('APPROVED')
--and date (ted_payer.created_at) between inicio_trimestre and fim_trimestre
and Cash_Transaction.amount > 0
;

-----------------------------------------------------
-- ETAPA 7 - CONSULTA TRANSAÇÕES CARTÕES APROVADAS  |
-----------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas7`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas7` AS 

SELECT 
distinct
'34656383000172' as CNPJ_ES
  --,gateway_id
   ,DATETIME(pt.created_at) as Data_do_Evento
  --,FORMAT_DATETIME("%Y%m%d",pt.created_at) as Data_do_Evento
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
  ,'0' as CPF_CNPJ_Cliente_conta_credito
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
join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas`                   bd   on cast(bd.Cpf as string) = cl.document
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
pt.payment_method in ('CREDIT_CARD','DEBIT_CARD','DIGITAL_WALLET')
--and date (pt.created_at) between inicio_trimestre and fim_trimestre
and pt.status in ('AUTHORIZED','COMPLETE','COMPLETED','SETTLEMENT')
and pt.transaction_value > 0
--and cl.document = '22768693804'
--and pt.gateway_id = '07vfvncg'
order by 9
;

---------------------------------------------------------------
-- ETAPA 8 - TABELA TRANSAÇÕES DE SALDO EM CONTA APROVADAS    |
---------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas8`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas8` AS 

SELECT 
distinct
'34656383000172' as CNPJ_ES
,DATETIME(pt.created_at) as Data_do_Evento
  --,FORMAT_DATETIME("%Y%m%d",pt.created_at) as Data_do_Evento
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
  ,'0' as CPF_CNPJ_Cliente_conta_credito
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
join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas`                bd    on cast(bd.Cpf as string) = cl.document
left join `eai-datalake-data-sandbox.payment.payment_customer_account`                                cc1   on cc1.bank_token = cast(bd.Cpf as string)

WHERE 
pt.payment_method in ('BALANCE')
--and date (pt.created_at) between inicio_trimestre and fim_trimestre
and pt.status in ('AUTHORIZED','COMPLETE','COMPLETED','SETTLEMENT')
and pt.transaction_value >0
;
---------------------------------------------------------------
-- ETAPA 9 - TABELA TRANSAÇÕES VIA BOLETO APROVADAS           |
---------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas9`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas9` AS 

SELECT  
distinct
 '34656383000172' as CNPJ_ES
 ,DATETIME(Cash_Transaction.created_at) as Data_do_Evento
  --,FORMAT_DATETIME("%Y%m%d",Cash_Transaction.created_at) as Data_do_Evento
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
    when Cash_Transaction.flow = 'BILLET'then '04'
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
  ,cast(cc.bank_token as String) as CPF_CNPJ_Cliente_conta_credito
  ,cc.bank_account_agency as Agencia_Fav
  ,cc.bank_account_number||cc.bank_account_check_number as Conta_Fav
  ,'0' as Tipo_de_chave_PIX
  ,'0' as Chave_PIX
  ,'N' as Relato_de_Fraude_PIX
  ,'ABASTECEAI' as Banco_Favorecido
  ,billet.type || '_BILLET' as Tipo_Transacao
  ,FORMAT_DATE("%Y%m",Cash_Transaction.created_at)as Safra_Tranx
  ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
  ,FORMAT_DATE("%Y%m",bd.event_date)as Safra_Bloqueio

FROM `eai-datalake-data-sandbox.cashback.p2p_payer`                                 p2p_payer
LEFT JOIN `eai-datalake-data-sandbox.core.customers`                                cl                ON p2p_payer.payer_id = cl.uuid
JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`                   cc1               ON cc1.bank_token = cl.document
LEFT JOIN `eai-datalake-data-sandbox.cashback.billet`                               billet            ON p2p_payer.p2p_id = billet.id 
LEFT JOIN `eai-datalake-data-sandbox.cashback.cash_transaction`                     Cash_Transaction  ON billet.cash_transaction_id = Cash_Transaction.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payee`                            p2p_payee         ON billet.id = p2p_payee.p2p_id
LEFT JOIN `eai-datalake-data-sandbox.core.customers`                                cl_fav            ON p2p_payee.payee_id = cl_fav.uuid
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas`          bd                ON cast(bd.Cpf as string) = cl.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc                ON cc.bank_token = cl_fav.document
where 
billet.status IN ('APPROVED')
--and date (p2p_payee.created_at) between inicio_trimestre and fim_trimestre
and Cash_Transaction.amount > 0
;


/*
select count(*), Tipo_de_Cliente_conta_evento from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_9` 
group by Tipo_de_Cliente_conta_evento
*/
-------------------------------------------------------------------
-- ETAPA 10 - CONSOLIDAÇÃO E UNIÃO DE TODAS AS ETAPAS ANTERIORES  |
-------------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas_final_1` 

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas_final_1` AS 

  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas2`
  union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas3`
  union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas4`  
  union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas5`
  union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas6`
  union all
  --select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas7`
  --union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas8`
  union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas9`
;

---------------------------------------------------------------
-- ETAPA 10 - TABELA BACEN FINAL COM TODAS AS INFORMAÇÕES     |
---------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas_final` 


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas_final` AS 


with
base_final as (

select 
distinct
 a.Conta
,a.CPF_CNPJ_Cliente_conta_evento
,a.Data_do_Evento
,a.Tipo_de_chave_PIX
,a.Chave_PIX
,a.Banco_Favorecido
,a.CPF_CNPJ_Cliente_conta_credito
,CASE
WHEN cast(CPF_CNPJ_Cliente_conta_evento as string) = cast(CPF_CNPJ_Cliente_conta_credito as string) THEN 'Mesma titularidade'
when cast(CPF_CNPJ_Cliente_conta_credito as string) = '0' or cast(CPF_CNPJ_Cliente_conta_credito as string) is null then 'NA'
ELSE 'Outra titularidade'  
END as Flag_Titularidade
,a.Tipo_Transacao
,a.Valor_do_Evento
,RANK() OVER (PARTITION BY a.CPF_CNPJ_Cliente_conta_evento ORDER BY a.Data_do_Evento desc) AS Rank_Tranx

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas_final_1`  a
order by 1,9


)
select
*
from base_final 
where Rank_Tranx = 1
and CPF_CNPJ_Cliente_conta_evento is not null

;

---------------------------------------------
-- TABELA BACEN - Arquivo Contas Encerradas |
--------------------------------------------- 

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas_Report` order by Data_Encerramento_Conta

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas_Report` AS 

with 

base_final as (
    select distinct
    *
    from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas`

),base_saldo as(
  SELECT
    T.numerodocumento as  DOCUMENTO                     -- CPF / CNPJ    
    --,T.data as Dt_Saldo
    ,round(sum(T.valor),2) as Saldo                   -- SOMA DOS VALORES QUE COMPÕEM O SALDO
    FROM `eai-datalake-data-sandbox.orbitall.tb_topaz` T
    join (select * from base_final) cl on cl.CPF = T.numerodocumento
    where T.status = 'MOVIMENTAÇÃO EXECUTADA'       -- STATUS NECESSARIO DE TRANSACOES CONCLUIDAS    
    and T.data <= CURRENT_DATE()-1          -- FIXAR COM A DATA COM A QUAL SE PRETENDE ANALISAR O SALDO    
    and length(T.numerodocumento) <= 11     -- REMOVER CASO QUEIRA TRAZER TAMBÉM CNPJ
    group by 1

    ), base_saldo_final as (
      select
       DOCUMENTO,
       Saldo
      from base_saldo
       --where Saldo <=0
    
    ), base_final1 as (
    select
    distinct
    Conta
    ,bd.Tipo_PF_ou_PJ
    ,bd.Nome_Completo_Titular
    ,bd.CPF
    ,bd.status
    ,tran.Data_do_Evento
    ,format_date('%Y-%m-%d', date(bd.Data_Abertura_Conta)) as Data_Abertura_Conta
    ,format_date('%Y-%m-%d', date(bd.event_date)) as Data_Encerramento_Conta
    ,bd.Motivo_Encerramento_Conta
    ,tran.Tipo_Transacao
    ,tran.Valor_do_Evento
    ,sld.Saldo

    from base_final bd
    left join base_saldo_final sld on sld.DOCUMENTO = bd.CPF
    left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas_final` tran on tran.CPF_CNPJ_Cliente_conta_evento = bd.CPF
  
    )
    select
      ROW_NUMBER() OVER () AS Item
      ,bd.Conta as Numero_da_Conta
      ,bd.Tipo_PF_ou_PJ
      ,bd.Nome_Completo_Titular
      ,concat(
      substring(bd.CPF, 1, 3), '.', 
      substring(bd.CPF, 4, 3), '.', 
      substring(bd.CPF, 7, 3), '-', 
      substring(bd.CPF, 10, 2)
    ) AS CPF
    --,bd.Data_Abertura_Conta
    ,FORMAT_DATE('%d/%m/%Y', PARSE_DATE('%Y-%m-%d', bd.Data_Abertura_Conta)) AS Data_Abertura_Conta
    ,'Última movimentação no conta' as Ultima_Movimentacao_conta
    ,date(Data_do_evento) as Data_Ultima_movimentacao 
    , case 
       when date(Data_do_evento) > date(bd.Data_Encerramento_Conta) then 'Transação feita depois da data de encerramento informada.'
       else 'Transação feita dentro da data de encerramento.'
      end as Flag_validacao_data
    ,COALESCE(bd.Valor_do_Evento, 0.0) AS Valor_BRL
    ,bd.Data_Encerramento_Conta as Data_Encerramento_Sem_Formatacao
    ,FORMAT_DATE('%d/%m/%Y', PARSE_DATE('%Y-%m-%d', bd.Data_Encerramento_Conta)) AS Data_Encerramento_Conta
    ,COALESCE(bd.Saldo, 0.0) AS Saldo
    ,bd.Motivo_Encerramento_Conta
    from base_final1 bd
;

/*

  select Flag_validacao_data, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas_Report`
  group by Flag_validacao_data

*/
   
/*
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas_Report`
  where Flag_validacao_data = 'Transação feita depois da data de encerramento informada.'
*/


/*

  DECLARE ano_vigente STRING;
  DECLARE data_criacao_arquivo STRING;
  DECLARE inicio_semestre DATE;
  DECLARE fim_semestre DATE;

  SET ano_vigente = FORMAT_DATE('%Y', CURRENT_DATE());
  SET data_criacao_arquivo = REPLACE(FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()), '-', '');


  /*
  SET inicio_semestre = DATE(CONCAT(ano_vigente,'-01-01'));
  SET fim_semestre = DATE(CONCAT(ano_vigente,'-06-30'));
  */

  /*  
  SET inicio_semestre = DATE(CONCAT(ano_vigente,'-07-01'));
  SET fim_semestre = DATE(CONCAT(ano_vigente,'-12-31'));
  */

/*

select 
  ROW_NUMBER() OVER () AS Item,
  r.Numero_da_Conta,
  r.Tipo_PF_ou_PJ,
  r.Nome_Completo_Titular,
  r.CPF,
  r.Data_Abertura_Conta,
  r.Data_Ultima_movimentacao,
  r.Flag_validacao_data,
  r.Valor_BRL,
  r.Data_Encerramento_Conta,
  r.Saldo,
  r.Motivo_Encerramento_Conta
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas_Report` as r
 where date(Data_Encerramento_Sem_Formatacao) between inicio_semestre and fim_semestre

*/


 -- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas_Report` where Motivo_Encerramento_Conta 

-- select * from `eai-datalake-data-sandbox.payment.customer_account`  limit 100


with base_thalita as (
select
    base.Item, base.CustomerID, p.customer_id, base.Numero_da_Conta, base.Tipo_PF_ou_PJ, base.Nome_Completo_Titular, base.CPF, base.Data_Abertura_Conta, base.Ultima_Movimentacao_conta, base.Data_Ultima_movimentacao, base.Flag_validacao_data, base.Valor_BRL, base.Data_Encerramento_Conta, base.Saldo, base.Motivo_Encerramento_Conta,
          CASE 
            WHEN pt.transaction_value = 300 and pt.payment_method IN ("CASH") AND pt.status IN ("AUTHORIZED", "SETTLEMENT", "COMPLETED") THEN 1
            else 0  
          END AS Tranx_Limite_300,
      FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_contas_encerradas_Report` as base 
      LEFT JOIN`eai-datalake-data-sandbox.payment.payment` as p
      ON base.CustomerID = p.customer_id
      LEFT JOIN `eai-datalake-data-sandbox.payment.payment_instrument` as pi 
      ON p.id = pi.id
      LEFT JOIN `eai-datalake-data-sandbox.payment.payment_transaction` as pt 
      ON pt.payment_id = p.id
      LEFT JOIN `eai-datalake-data-sandbox.core.orders` as ord 
      ON p.order_id = ord.uuid
      --WHERE
         -- AND pt.transaction_value > 0
         --pt.payment_method IN ("CASH")
         --AND pt.status IN ("AUTHORIZED", "SETTLEMENT", "COMPLETED")
), base_3 as (
select 
    base_cl.Item, base_cl.CustomerID, base_cl.Numero_da_Conta, base_cl.Tipo_PF_ou_PJ, base_cl.Nome_Completo_Titular, base_cl.CPF, base_cl.Data_Abertura_Conta, base_cl.Ultima_Movimentacao_conta, base_cl.Data_Ultima_movimentacao, base_cl.Flag_validacao_data, base_cl.Valor_BRL, base_cl.Data_Encerramento_Conta, base_cl.Saldo, base_cl.Motivo_Encerramento_Conta,  sum(Tranx_Limite_300) as Tranx_Limite_300
from base as base_cl
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
), base_4 as ( select   
                      *, 
                     ev.user_name as UsuarioStatus, 
                     RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual

from base_3 as base_cl
              left join `eai-datalake-data-sandbox.core.customers` cl
              on cl.uuid = base_cl.CustomerID
              left join (select * from `eai-datalake-data-sandbox.core.customer_event` 
                     where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT'))Ev on ev.customer_id = cl.id
              ) 
select * from base_4 
where Rank_Ult_Atual = 1
;
