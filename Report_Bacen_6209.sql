------------------------------------------------------------------------------------------------
-- RELATÓRIO BACEN - 6209 --                                                                   | 
------------------------------------------------------------------------------------------------
-- VARIÁVEIS DE INFORMAÇÕES DO TRIMESTRE VIGENTE                                               |
------------------------------------------------------------------------------------------------




 -- PROCESSO MANUAL:

  DECLARE ano_vigente STRING;
  DECLARE data_criacao_arquivo STRING;
  DECLARE inicio_trimestre DATE;
  DECLARE fim_trimestre DATE;
  DECLARE trimestre_vigente STRING;
  DECLARE database STRING;

  #Alterar o ano de vigência
  SET ano_vigente = '2023';
  SET data_criacao_arquivo = REPLACE(FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()), '-', '');



  #Alterar a data de inicio, fim, número do trimestre vigente e o mês da database
  SET inicio_trimestre = DATE(CONCAT(ano_vigente,'-10-01'));
  SET fim_trimestre = DATE(CONCAT(ano_vigente,'-12-31'));
  SET trimestre_vigente = '4';
  SET database = CONCAT(ano_vigente,'12');


/*
 PROCESSO MANUAL:

  DECLARE ano_vigente STRING;
  DECLARE data_criacao_arquivo STRING;
  DECLARE inicio_trimestre DATE;
  DECLARE fim_trimestre DATE;
  DECLARE trimestre_vigente STRING;
  DECLARE database STRING;

  SET ano_vigente = 2023;
  SET data_criacao_arquivo = REPLACE(FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()), '-', '');
*/


  /*
  SET inicio_trimestre = DATE(CONCAT(ano_vigente,'-01-01'));
  SET fim_trimestre = DATE(CONCAT(ano_vigente,'-03-31'));
  SET trimestre_vigente = '1';
  SET database = CONCAT(ano_vigente,'03');
  */

  /*
  SET inicio_trimestre = DATE(CONCAT(ano_vigente,'-04-01'));
  SET fim_trimestre = DATE(CONCAT(ano_vigente,'-06-30'));
  SET trimestre_vigente = '2';
  SET database = CONCAT(ano_vigente,'06');
  */

  /*
  SET inicio_trimestre = DATE(CONCAT(ano_vigente,'-07-01'));
  SET fim_trimestre = DATE(CONCAT(ano_vigente,'-09-30'));
  SET trimestre_vigente = '3';
  SET database = CONCAT(ano_vigente,'09');
  */

  /*
  SET inicio_trimestre = DATE(CONCAT(ano_vigente,'-10-01'));
  SET fim_trimestre = DATE(CONCAT(ano_vigente,'-12-31'));
  SET trimestre_vigente = '4';
  SET database = CONCAT(ano_vigente,'12');
  */
-----------------------------------------------------------------------------------------------------------

/*

DECLARE data_atual DATE;
DECLARE ano_anterior int64;
DECLARE inicio_trimestre DATE;
DECLARE fim_trimestre DATE;
DECLARE ano_vigente STRING;
DECLARE trimestre_vigente STRING;
DECLARE data_criacao_arquivo STRING;
DECLARE database STRING;


SET data_atual = CURRENT_DATE();
SET ano_anterior = EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)); 
SET ano_vigente = FORMAT_DATE('%Y', CURRENT_DATE());
SET data_criacao_arquivo = REPLACE(FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()), '-', '');
 
IF data_atual BETWEEN DATE(CONCAT(ano_vigente,'-01-01')) AND DATE(CONCAT(ano_vigente,'-03-31')) THEN 
  SET inicio_trimestre = DATE(CONCAT(ano_anterior,'-10-01'));
  SET fim_trimestre = DATE(CONCAT(ano_anterior,'-12-31'));
  SET trimestre_vigente = '4';
  SET database = CONCAT(ano_anterior,'12');
ELSEIF data_atual BETWEEN DATE(CONCAT(ano_vigente,'-04-01')) AND DATE(CONCAT(ano_vigente,'-06-30')) THEN 
  SET inicio_trimestre = DATE(CONCAT(ano_vigente,'-01-01'));
  SET fim_trimestre = DATE(CONCAT(ano_vigente,'-03-31'));
  SET trimestre_vigente = '1';
  SET database = CONCAT(ano_vigente,'03');
ELSEIF data_atual BETWEEN DATE(CONCAT(ano_vigente,'-07-01')) AND DATE(CONCAT(ano_vigente,'-09-30')) THEN 
  SET inicio_trimestre = DATE(CONCAT(ano_vigente,'-04-01'));
  SET fim_trimestre = DATE(CONCAT(ano_vigente,'-06-30'));
  SET trimestre_vigente = '2';
  SET database = CONCAT(ano_vigente,'06');
ELSEIF data_atual BETWEEN DATE(CONCAT(ano_vigente,'-10-01')) AND DATE(CONCAT(ano_vigente,'-12-31')) THEN 
  SET inicio_trimestre = DATE(CONCAT(ano_vigente,'-07-01'));
  SET fim_trimestre = DATE(CONCAT(ano_vigente,'-09-30'));
  SET trimestre_vigente = '3';
  SET database = CONCAT(ano_vigente,'09');
END IF;

*/
---select data_criacao_arquivo, ano_vigente,database, inicio_trimestre, fim_trimestre;
------------------------------------------------------------------------------------------------

-------------------------------------------------------------------
-- ETAPA 1 - TABELA DE TODOS OS CLIENTES E SEU ÚLTIMO STATUS      |
-------------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_1` AS 

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
            --where date(ev.event_date) between inicio_trimestre and fim_trimestre
            )
             select * from base 
             where Rank_Ult_Status = 1
             --and  motivo = 'Fraude confirmada'
             --AND date(base.dt_abertura) between inicio_trimestre and fim_trimestre
;

-----------------------------------------------------
-- ETAPA 2 - TABELA TRANSAÇÕES PIX OUT APROVADAS    |
-----------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_2` AS 

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
  ,cast(pix_payee.document as numeric) as CPF_CNPJ_Cliente_conta_credito
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
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_1`         bd                ON cast(bd.Cpf as string) = cl.document 
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`                   cc1          ON cc1.bank_token = cast(bd.Cpf as string)

where 
pix.status IN ('APPROVED')
and date (pix_payer.created_at) between inicio_trimestre and fim_trimestre
and Cash_Transaction.amount > 0
and pix.type = 'CASH_OUT'
--order by 9 
;

-----------------------------------------------------
-- ETAPA 3 - TABELA TRANSAÇÕES PIX IN APROVADAS     |
-----------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_3` AS 

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
  ,cast(pix_payer.document as numeric) as CPF_CNPJ_Cliente_conta_credito
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
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_1`          bd                ON cast(bd.Cpf as string) = cl_fav.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc1               ON cc1.bank_token = cast(bd.Cpf as string)

where 
pix.status IN ('APPROVED')
and date (pix_payer.created_at) between inicio_trimestre and fim_trimestre
and Cash_Transaction.amount > 0
and pix.type = 'CASH_IN'
;

-----------------------------------------------------
-- ETAPA 4 - TABELA TRANSAÇÕES P2P APROVADAS        |
-----------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_4` AS 
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
  ,cast(cc.bank_token as numeric) as CPF_CNPJ_Cliente_conta_credito
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
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_1`          bd                ON cast(bd.Cpf as string) = cl.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc                ON cc.bank_token = cl_fav.document
where 
p2p.status IN ('APPROVED')
and date (p2p_payer.created_at) between inicio_trimestre and fim_trimestre
and Cash_Transaction.amount > 0
;

-----------------------------------------------------
-- ETAPA 5 - TABELA TRANSAÇÕES TED OUT APROVADAS    |
-----------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_5` AS 

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
  ,cast(ted_payee.document as numeric) as CPF_CNPJ_Cliente_conta_credito
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
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_1`          bd                ON cast(bd.Cpf as string) = cl.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc1               ON cc1.bank_token = cl.document

where 
ted.status IN ('APPROVED')
and date (ted_payer.created_at) between inicio_trimestre and fim_trimestre
and Cash_Transaction.amount > 0
;

-----------------------------------------------------
-- ETAPA 6 - TABELA TRANSAÇÕES TED IN APROVADAS     |
-----------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_6` AS 

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
  ,cast(ted_payer.document as numeric) as CPF_CNPJ_Cliente_conta_credito
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
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_1`          bd                ON cast(bd.Cpf as string) = cl_fav.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc1               ON cc1.bank_token = cast(bd.Cpf as string)

where 
ted.status IN ('APPROVED')
and date (ted_payer.created_at) between inicio_trimestre and fim_trimestre
and Cash_Transaction.amount > 0
;

-----------------------------------------------------
-- ETAPA 7 - CONSULTA TRANSAÇÕES CARTÕES APROVADAS  |
-----------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_7` AS 

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
  ,0 as CPF_CNPJ_Cliente_conta_credito
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
join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_1`                               bd   on cast(bd.Cpf as string) = cl.document
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
and date (pt.created_at) between inicio_trimestre and fim_trimestre
and pt.status in ('AUTHORIZED','COMPLETE','COMPLETED','SETTLEMENT')
and pt.transaction_value > 0
--and cl.document = '22768693804'
--and pt.gateway_id = '07vfvncg'
order by 9
;

---------------------------------------------------------------
-- ETAPA 8 - TABELA TRANSAÇÕES DE SALDO EM CONTA APROVADAS    |
---------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_8` AS 

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
  ,0  as CPF_CNPJ_Cliente_conta_credito
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
join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_1`                            bd    on cast(bd.Cpf as string) = cl.document
left join `eai-datalake-data-sandbox.payment.payment_customer_account`                                cc1   on cc1.bank_token = cast(bd.Cpf as string)

WHERE 
pt.payment_method in ('BALANCE')
and date (pt.created_at) between inicio_trimestre and fim_trimestre
and pt.status in ('AUTHORIZED','COMPLETE','COMPLETED','SETTLEMENT')
and pt.transaction_value >0
;
---------------------------------------------------------------
-- ETAPA 9 - TABELA TRANSAÇÕES VIA BOLETO APROVADAS           |
---------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_9` AS 
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
  ,cast(cc.bank_token as numeric) as CPF_CNPJ_Cliente_conta_credito
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
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_1`          bd                ON cast(bd.Cpf as string) = cl.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account`              cc                ON cc.bank_token = cl_fav.document
where 
billet.status IN ('APPROVED')
and date (p2p_payee.created_at) between inicio_trimestre and fim_trimestre
and Cash_Transaction.amount > 0
;


/*
select count(*), Tipo_de_Cliente_conta_evento from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_9` 
group by Tipo_de_Cliente_conta_evento
*/
-------------------------------------------------------------------
-- ETAPA 10 - CONSOLIDAÇÃO E UNIÃO DE TODAS AS ETAPAS ANTERIORES  |
-------------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_final_1` AS 

  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_2`
  union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_3`
  union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_4`  
  union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_5`
  union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_6`
  union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_7`
  union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_8`
  union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_9`
;

---------------------------------------------------------------
-- ETAPA 10 - TABELA BACEN FINAL COM TODAS AS INFORMAÇÕES     |
---------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_final` AS 
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
              ,CASE
                WHEN cast(CPF_CNPJ_Cliente_conta_credito as NUMERIC) = cast(CPF_CNPJ_Cliente_conta_evento as NUMERIC) THEN 'Mesma titularidade'
                ELSE 'Outra titularidade'  
              END as Flag_Titularidade
              ,CASE
                WHEN Tipo_Transacao in ('CASH_OUT_TED','CASH_IN_TED') THEN 'PRODUTO: 03 - Ordem de Transferência de Crédito'
                WHEn Tipo_Transacao in ('CASH_OUT_PIX','CASH_IN_PIX') THEN 'PRODUTO: 09 - PIX'
                WHEN Tipo_Transacao in ('CASH_IN_BILLET') THEN 'OPERAÇÃO: 02 - Boleto de pagamentos intrabancários'
                WHEN Tipo_Transacao in ('CASH_OUT_P2P') THEN 'OPERAÇÃO: 03 - Transferências de clientes (book transfer)'
                ELSE 'Outros'  
              END as Flag_Tipo_Transacao
              from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_final_1` a
              group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32

;

----------------------------------------------------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------------------------
  

---select data_criacao_arquivo, ano_vigente,database, inicio_trimestre, fim_trimestre;
------------------------------------------------------------------------------------------------




--------------------------------------------
-- TABELA BACEN - Arquivo CONGLOME.TXT     |
-------------------------------------------- 
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_CONGLOME` AS 
  SELECT 
    'CONGLOME' AS Nome_do_arquivo,
    data_criacao_arquivo AS Dt,
    '09515813' AS Instituicao,
    '00000000' AS Quantidade_de_registros
;    

--------------------------------------------
-- TABELA BACEN - Arquivo USUREMOT.TXT     |
-------------------------------------------- 
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_USUREMOT` AS 
  SELECT 
    'USUREMOT' AS Nome_do_arquivo,
    data_criacao_arquivo AS Dt,
    '09515813' AS Instituicao,
    '00000001' AS Quantidade_de_registros,
    trimestre_vigente AS Trimestre,
    ano_vigente AS Ano,
    '000000000' AS Internet_Banking_PF, 
    '000000000' AS Internet_Banking_PJ, 
    '000000000' AS Home_Banking, 
    '000000000' AS Office_Banking, 
    LPAD(CAST(COUNT(DISTINCT CPF_CNPJ_Cliente_conta_credito) AS STRING), 9, '0') AS Mobile_Banking_PF,
    '000000000' AS Mobile_Banking_PJ
  FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_final`
;

--------------------------------------------
-- TABELA BACEN - Arquivo ESTATCRT.TXT     |
-------------------------------------------- 
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_ESTATCRT` AS 
  SELECT 
    'ESTATCRT' AS Nome_do_arquivo,
    data_criacao_arquivo AS Dt,
    '09515813' AS Instituicao,
    '00000000' AS Quantidade_de_registros
;  

--------------------------------------------
-- TABELA BACEN - Arquivo ESTATATM.TXT     |
-------------------------------------------- 
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_ESTATATM` AS 
  SELECT 
    'ESTATATM' AS Nome_do_arquivo,
    data_criacao_arquivo AS Dt,
    '09515813' AS Instituicao,
    '00000000' AS Quantidade_de_registros
;  

--------------------------------------------
-- TABELA BACEN - Arquivo TRANSOPA.TXT     |
-------------------------------------------- 
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_TRANSOPA` AS 
  SELECT 
    'TRANSOPA' AS Nome_do_arquivo,
    data_criacao_arquivo AS Dt,
    '09515813' AS Instituicao,
    '00000002' AS Quantidade_de_registros,
    trimestre_vigente AS Trimestre,
    ano_vigente AS Ano,
    '01' AS Canal_de_Acesso,
    '03' AS Produto,
    '04' AS Acesso_ao_ATM,
    LPAD(CAST(COUNT(CPF_CNPJ_Cliente_conta_credito) AS STRING), 12, '0') AS Quantidade,
    CASE 
        WHEN SUM(Valor_do_Evento) IS NULL THEN LPAD('0', 15, '0')
        ELSE LPAD(CAST(SUM(Valor_do_Evento * 100) AS STRING), 15, '0')
    END AS Valor
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_final`
  where Flag_Tipo_Transacao in ('PRODUTO: 03 - Ordem de Transferência de Crédito','OPERAÇÃO: 03 - Transferências de clientes (book transfer)')  
  UNION ALL       
  SELECT 
    'TRANSOPA' AS Nome_do_arquivo,
    data_criacao_arquivo AS Dt,
    '09515813' AS Instituicao,
    '00000002' AS Quantidade_de_registros,
    trimestre_vigente AS Trimestre,
    ano_vigente AS Ano,
    '01' AS Canal_de_Acesso,
    '09' AS Produto,
    '04' AS Acesso_ao_ATM,
    LPAD(CAST(COUNT(CPF_CNPJ_Cliente_conta_credito) AS STRING), 12, '0') AS Quantidade,
    CASE 
        WHEN SUM(Valor_do_Evento) IS NULL THEN LPAD('0', 15, '0')
        ELSE LPAD(CAST(SUM(Valor_do_Evento * 100) AS STRING), 15, '0')
    END AS Valor
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_final`
  where Flag_Tipo_Transacao = 'PRODUTO: 09 - PIX'    
;


--------------------------------------------
-- TABELA BACEN - Arquivo OPEINTRA.TXT     | 
-------------------------------------------- 
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_OPEINTRA` AS 
    SELECT 
    'OPEINTRA' AS Nome_do_arquivo,
    data_criacao_arquivo AS Dt,
    '09515813' AS Instituicao,
    '00000002' AS Quantidade_de_registros,
    ano_vigente AS Ano,
    trimestre_vigente AS Trimestre, 
    '03' as Operacao,
    LPAD(CAST(COUNT(CPF_CNPJ_Cliente_conta_credito) AS STRING), 12, '0') AS Quantidade,
    CASE 
        WHEN SUM(Valor_do_Evento) IS NULL THEN LPAD('0', 15, '0')
        ELSE LPAD(CAST(SUM(Valor_do_Evento * 100) AS STRING), 15, '0')
    END AS Valor
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_final`
  where Flag_Tipo_Transacao = 'OPERAÇÃO: 03 - Transferências de clientes (book transfer)'
  UNION ALL
  SELECT 
    'OPEINTRA' AS Nome_do_arquivo,
    data_criacao_arquivo AS Dt,
    '09515813' AS Instituicao,
    '00000002' AS Quantidade_de_registros,
    ano_vigente AS Ano,
    trimestre_vigente AS Trimestre, 
    '02' as Operacao,
    LPAD(CAST(COUNT(CPF_CNPJ_Cliente_conta_credito) AS STRING), 12, '0') AS Quantidade,
    CASE 
        WHEN SUM(Valor_do_Evento) IS NULL THEN LPAD('0', 15, '0')
        ELSE LPAD(CAST(SUM(Valor_do_Evento * 100) AS STRING), 15, '0')
    END AS Valor
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_final`
  where Flag_Tipo_Transacao = 'OPERAÇÃO: 02 - Boleto de pagamentos intrabancários'
;


--------------------------------------------
-- TABELA BACEN - Arquivo CONTATOS.TXT     |
-------------------------------------------- 
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_CONTATOS` AS 
  SELECT 
    'CONTATOS' AS Nome_do_arquivo,
    data_criacao_arquivo AS Dt,
    '09515813' AS Instituicao,
    '00000004' AS Quantidade_de_registros,
    ano_vigente AS Ano,
    trimestre_vigente AS Trimestre, 
    'D' AS Tipo_de_contato,
    'Valter Nakashima                                  ' as Nome,
    'Diretor de SPB                                    ' as Cargo,
    '11994725554                                       ' as Numero_de_telefone,
    'valter.nakashima@e-ai.com.br                      ' as E_mail
  UNION ALL
  SELECT 
    'CONTATOS' AS Nome_do_arquivo,
    data_criacao_arquivo AS Dt,
    '09515813' AS Instituicao,
    '00000004' AS Quantidade_de_registros,
    ano_vigente AS Ano,
    trimestre_vigente AS Trimestre, 
    'T' AS Tipo_de_contato,
    'Gislaine Juliana da Paz Nogueira                  ' as Nome,
    'Gerente de Prevenção à Fraudes                    ' as Cargo,
    '11991296929                                       ' as Numero_de_telefone,
    'gislaine.nogueira@e-ai.com.br                     ' as E_mail
  UNION ALL
  SELECT 
    'CONTATOS' AS Nome_do_arquivo,
    data_criacao_arquivo AS Dt,
    '09515813' AS Instituicao,
    '00000004' AS Quantidade_de_registros,
    ano_vigente AS Ano,
    trimestre_vigente AS Trimestre, 
    'T' AS Tipo_de_contato,
    'Jair Cardoso Vieira                               ' as Nome,
    'Especialista em Prevenção à Fraudes               ' as Cargo,
    '11994356819                                       ' as Numero_de_telefone,
    'jair.vieira@e-ai.com.br                           ' as E_mail
  UNION ALL
  SELECT 
    'CONTATOS' AS Nome_do_arquivo,
    data_criacao_arquivo AS Dt,
    '09515813' AS Instituicao,
    '00000004' AS Quantidade_de_registros,
    ano_vigente AS Ano,
    trimestre_vigente AS Trimestre, 
    'T' AS Tipo_de_contato,
    'Luiz dos Santos                                   ' as Nome,
    'Estagiário                                        ' as Cargo,
    '11990266688                                       ' as Numero_de_telefone,
    'luiz.ssantos@e-ai.com.br                          ' as E_mail
;


--------------------------------------------
-- TABELA BACEN - Arquivo DATABASE.TXT     | 
-------------------------------------------- 
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_DATABASE` AS 
  SELECT 
    'DATABASE' AS Nome_do_arquivo,
    data_criacao_arquivo AS Dt,
    '09515813' AS Instituicao,
    database AS Database
;   

--select Flag_Titularidade, count(*) as Quantidade from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_final` group by 1;

--select Flag_Tipo_Transacao, count(*) as Quantidade from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_final` group by 1;


/*
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_CONGLOME`;
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_USUREMOT`;
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_ESTATCRT`;
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_ESTATATM`;
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_TRANSOPA`;
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_OPEINTRA`;
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_CONTATOS`;
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bacen_DATABASE`;
select Flag_Titularidade, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Report_Bacen_final` 
where Tipo_Transacao like '%P2P%'
group by 1


*/


