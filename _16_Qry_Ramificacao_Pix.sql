--> TABELA RAMIFICAÇÃO DE SAIDAS DE PIX

  -- SELECT  * FROM  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Ramificar_pix`

CREATE OR REPLACE TABLE  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Ramificar_pix` AS

WITH
  base_AbasteceAi AS (
  SELECT
    DISTINCT 
    pix_payer.payer_id AS CustomerID,
    cl.document AS CPF_Origem,
    pix_payer.name AS ClienteOrigem,
    cl.status AS Status_Conta,
    pix.end_to_end_id,
    pix.status,
    pix.key_value,
    pix.key_type,
    pix.type,
    pix.scheduled_date,
    Cash_Transaction.flow,
    Cash_Transaction.amount/100 AS amount,
    Cash_Transaction.description,
    Cash_Transaction.created_at AS DT_TRANX,
    pix_payee.name AS Nome_Favorecido,
    pix_payee.document AS CPF_Favorecido,
    CASE
      WHEN pix_payee.document = cl.document THEN 'MesmaTitularidade' ELSE 'OutraTitularidade' END AS Flag_Titularidade,
    pix_payee.bank_number,
    pix_payee.agency_number AS Agencia_Favorecido,
    pix_payee.account_number||"-"||pix_payee.account_check_number AS Conta_Favorecido,
    pix_payee.bank_name AS Banco_Favorecido,
    pix_payee.bank_ispb AS Cod_Banco_ISPB,
    CASE
      WHEN CHAR_LENGTH(pix_payee.document) >=14 THEN 'CNPJ'
      WHEN CHAR_LENGTH(pix_payee.document) <=11 THEN 'CPF'
    ELSE'N/A'  END  AS Flag_Favorecida,
    CASE
      WHEN cbk.Customer_Email = cl.email THEN 'CBK'  ELSE  'NAOCBK' END  AS Flag_CBK,
    cbk.qtd_CBK AS Qtd_Contestcao
  FROM    `eai-datalake-data-sandbox.cashback.pix_payer` pix_payer
  LEFT JOIN    `eai-datalake-data-sandbox.core.customers` cl ON    pix_payer.payer_id = cl.uuid
  LEFT JOIN    `eai-datalake-data-sandbox.cashback.pix` pix ON pix_payer.pix_id = pix.id
  LEFT JOIN    `eai-datalake-data-sandbox.cashback.cash_transaction` Cash_Transaction  ON    pix.cash_transaction_id = Cash_Transaction.id
  LEFT JOIN    `eai-datalake-data-sandbox.cashback.pix_payee` pix_payee  ON    pix.id = pix_payee.pix_id
  LEFT JOIN (
    SELECT
      DISTINCT FORMAT_DATE("%Y%m",Transaction_Date)AS Safra_tranx,
      cbk.Order_ID,
      cbk.Customer_Email,
      COUNT(* ) AS qtd_CBK,
      COUNT(DISTINCT Customer_Email) AS qtd_cliente
    FROM  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` cbk
    WHERE
      cbk.Reason = 'Fraud'
      AND cbk.Status = 'Open'
      AND cbk.Kind = 'Chargeback'
    GROUP BY 1, 2, 3) cbk  ON cbk.Customer_Email = cl.email
    WHERE
    date (Cash_Transaction.created_at) >= (CURRENT_DATE() - 60)
    AND pix.`type` = 'CASH_OUT'
    AND pix.`status` = 'APPROVED'
    AND pix_payee.bank_name LIKE '%ABASTECE%' 
    --AND pix_payer.payer_id IN ('CUS-360a66d2-8b01-4be0-bbbd-83c2cdd485a6') 
    --AND cl.document = '26137550672' --CLIENTE 
    --AND pix_payee.document = '26137550672' --FAVORECIDO 
  ),  base_NaoAbasteceAi AS (
    SELECT
      DISTINCT pix_payer.payer_id AS CustomerID,
      cl.document AS CPF_Origem,
      pix_payer.name AS ClienteOrigem,
      cl.status AS Status_NaoAbasteceAi,
      pix.end_to_end_id,
      pix.status,
      pix.key_value,
      pix.key_type,
      pix.type,
      pix.scheduled_date,
      Cash_Transaction.flow,
      Cash_Transaction.amount/100 AS amount,
      Cash_Transaction.description,
      Cash_Transaction.created_at AS DT_TRANX,
      pix_payee.name AS Nome_Favorecido,
      pix_payee.document AS CPF_Favorecido,
      CASE WHEN pix_payee.document = cl.document THEN 'MesmaTitularidade'  ELSE  'OutraTitularidade' END  AS Flag_Titularidade,
      pix_payee.bank_number,
      pix_payee.agency_number AS Agencia_Favorecido,
      pix_payee.account_number||"-"||pix_payee.account_check_number AS Conta_Favorecido,
      pix_payee.bank_name AS Banco_Favorecido,
      pix_payee.bank_ispb AS Cod_Banco_ISPB,
      CASE
        WHEN CHAR_LENGTH(pix_payee.document) >=14 THEN 'CNPJ'
        WHEN CHAR_LENGTH(pix_payee.document) <=11 THEN 'CPF'
      ELSE    'N/A'  END    AS Flag_Favorecida,
      CASE
        WHEN cbk.Customer_Email = cl.email THEN 'CBK'    ELSE    'NAOCBK'  END  AS Flag_CBK_NAOEAI,
      cbk.qtd_CBK AS Qtd_ContestcaoNaoEAI
    FROM    `eai-datalake-data-sandbox.cashback.pix_payer` pix_payer
    LEFT JOIN    `eai-datalake-data-sandbox.core.customers` cl  ON    pix_payer.payer_id = cl.uuid
    LEFT JOIN    `eai-datalake-data-sandbox.cashback.pix` pix  ON    pix_payer.pix_id = pix.id
    LEFT JOIN    `eai-datalake-data-sandbox.cashback.cash_transaction` Cash_Transaction  ON    pix.cash_transaction_id = Cash_Transaction.id
    LEFT JOIN    `eai-datalake-data-sandbox.cashback.pix_payee` pix_payee  ON    pix.id = pix_payee.pix_id
    LEFT JOIN (
      SELECT
        DISTINCT FORMAT_DATE("%Y%m",Transaction_Date)AS Safra_tranx,
        cbk.Order_ID,
        cbk.Customer_Email,
        COUNT(* ) AS qtd_CBK,
        COUNT(DISTINCT Customer_Email) AS qtd_cliente
      FROM
        `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` cbk
      WHERE
        cbk.Reason = 'Fraud'
        AND cbk.Status = 'Open'
        AND cbk.Kind = 'Chargeback'
      GROUP BY
        1, 2, 3) cbk  ON    cbk.Customer_Email = cl.email
    WHERE
      date (Cash_Transaction.created_at) >= (CURRENT_DATE() - 60)
      AND pix.`type` = 'CASH_OUT'
      AND pix.`status` = 'APPROVED'
      AND pix_payee.bank_name NOT LIKE '%ABASTECE%' 
      --AND pix_payer.payer_id IN ('CUS-360a66d2-8b01-4be0-bbbd-83c2cdd485a6') 
      --AND pix_payee.document = '27072762272' 
    )
  SELECT
    a.Status_Conta,
    a.CustomerID,
    a.CPF_Origem,
    a.ClienteOrigem,
    b.Flag_Favorecida,
    b.Nome_Favorecido,
    b.CPF_Favorecido,
    b.Banco_Favorecido,
    Flag_CBK,
    Qtd_Contestcao,
    Flag_CBK_NAOEAI,
    Qtd_ContestcaoNaoEAI,
    COUNT(DISTINCT b.CPF_Favorecido) AS Qtd_Favorecido
  FROM  base_AbasteceAi a
  LEFT JOIN  base_NaoAbasteceAi b ON  a.CPF_Origem = b.CPF_Origem
GROUP BY  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12



