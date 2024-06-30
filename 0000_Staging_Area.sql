/*

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Chargeback_PayPal_Staging_Area_TESTE` 
AS
SELECT *
FROM EXTERNAL_QUERY('eai-data-sandbox', '''
SELECT *
FROM `eai-data-sandbox.eai-prevencao-fraudes.Chargeback_PayPal_Staging_Area*.csv`
''');

*/

----------------------------------------
-- STAGING AREA DOS INPUTS MANUAIS     |
----------------------------------------

/*
------------------------------------------------
                                               |
--  Tb_AllowMe_Staging_Area                    |
                                               |
--  Tb_Zaig_Staging_Area                       |
                                               |
--  Tb_Zaig_PJ_Staging_Area                    |
                                               |
--  Tb_Unico_SMS_Staging_Area                  |
                                               |
--  Tb_Unico_BIO_Staging_Area                  |
                                               |
--  Tb_Aereas_PayPal_Staging_Area              |
                                               |
--  Tb_Chargeback_PayPal_Staging_Area          |
                                               |
------------------------------------------------
*/


/*
------------------------------------------------------------------
-- MAIOR E MENOR DATA DOS FORNECEDORES EMPILLHADOS               |
------------------------------------------------------------------
with bases_fornecedores as (
select '01 - Allow Me' as Fornecedor, min(date(created_at)) as Primeiro_Registro, max(date(created_at)) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe`
union all 
select '02 - Zaig' as Fornecedor, min(date(data_cadastro)) as Primeiro_Registro, max(date(data_cadastro)) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig`
union all
select '03 - Zaig PJ' as Fornecedor, min(date(data_cadastro)) as Primeiro_Registro, max(date(data_cadastro)) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_PJ`
union all
select '04 - Unico SMS' as Fornecedor, min(date(DATA_ENVIO_SMS)) as Primeiro_Registro, max(date(DATA_ENVIO_SMS)) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone_Hist`
union all
select '05 - Unico BIO' as Fornecedor, min(date(created_at)) as Primeiro_Registro, max(date(created_at)) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_3`
union all
select '06 - PayPal Aéreas' as Fornecedor, min(date(Created_Datetime)) as Primeiro_Registro, max(date(Created_Datetime)) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2`
union all
select '07 - PayPal Dispute' as Fornecedor,min(Effective_Date) as Primeiro_Registro, max(Effective_Date) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico`
) select * from bases_fornecedores order by 1;
*/




/* 
--------------------------------------------------------------------------
                                                                         |
                                                                         |
  CRIAÇÃO DA Dw_AllowMe COM AS CAMPOS                                    |
  NECESSÁRIOS DA Tb_AllowMe_Staging_Area                                 |
                                                                         |
                                                                         |
  DELIMITADOR DE CAMPOS: VIRGULA                                         |   
                                                                         |
--------------------------------------------------------------------------
*/     
----------------------------------------------------------------------------------------------------------------------
-- DEVIDO AOS DADOS NÃO ESTAREM BEM ESTRUTURADOS, OS CAMPOS PRECISAM SER DEFINIDOS MANUALMENTE COMO STRING           |
-- NA PRÓXIMA ETAPA OS CAMPOS SÃO CORRIGIDOS                                                                         |
----------------------------------------------------------------------------------------------------------------------

/*
transaction_id:STRING
,similar_transaction_id:STRING
,created_at:STRING
,integration:STRING
,user_id:STRING
,ip_address:STRING
,browser:STRING
,browser_version:STRING
,browser_canvasFP:STRING
,browser_language:STRING
,browser_webglFP:STRING
,browser_audioFP:STRING
,browser_cookie:STRING
,os_audioStackInfo:STRING
,os_graphicBoard:STRING
,os_platform:STRING
,os_numberOfCPUCores:STRING
,os_memory:STRING
,app_id:STRING
,network_operator:STRING
,mobile_model_name:STRING
,mobile_manufacturer_name:STRING
,mobile_os_name:STRING
,mobile_os_version:STRING
,contextual_id:STRING
,new_device:STRING
,similarity_percentage:STRING
,authorized:STRING
,authorized_at:STRING
,fraud:STRING
,rules_matched:STRING
,rules_details:STRING
,metadata:STRING
,score_classification:STRING
,score:STRING
*/

-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_AllowMe_Staging_Area                                     |
-----------------------------------------------------------------------------------
-- select min(created_at) as Primeiro_Registro, max(created_at) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_AllowMe_Staging_Area`;


-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Dw_AllowMe                                                  |
-----------------------------------------------------------------------------------
-- select min(created_at) min, max(created_at) max FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe`;



--CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe` AS 

INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe`

        select 
        distinct
         a.transaction_id
        ,a.similar_transaction_id
        --,a.created_at
        ,cast( a.created_at as DATETIME) as created_at_dtAllowme
        ,TIMESTAMP(cast( a.created_at as DATETIME) ,'America/Sao_Paulo') as created_at
        --,DATETIME(a.created_at,'America/Sao_Paulo')
        --,DATETIME(TIMESTAMP(DATETIME(a.created_at,'America/Sao_Paulo')))as created_at
        ,a.integration
        ,a.user_id
        ,a.ip_address
        ,a.browser
        ,a.browser_version
        ,a.browser_canvasFP
        ,a.browser_language
        ,a.browser_webglFP
        ,a.browser_audioFP
        ,a.browser_cookie
        ,a.os_audioStackInfo
        ,a.os_graphicBoard
        ,a.os_platform
        ,a.os_numberOfCPUCores
        ,a.os_memory
        ,a.app_id
        ,a.network_operator
        ,a.mobile_model_name
        ,a.mobile_manufacturer_name
        ,a.mobile_os_name
        ,a.mobile_os_version
        ,a.contextual_id
        ,a.new_device
        ,a.similarity_percentage
        ,a.authorized
        ,a.authorized_at
        ,a.fraud
        ,a.rules_matched
        ,a.rules_details
        ,a.metadata
        ,a.score_classification
        ,cast(a.score as Numeric) as score
        FROM  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_AllowMe_Staging_Area` a
        --where a.integration != 'integration'
        where TIMESTAMP(a.created_at) >= (select max(TIMESTAMP(created_at_dtAllowme)) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe` )

;

 

/* 
--------------------------------------------------------------------------
                                                                         |
                                                                         |
  CRIAÇÃO DA Tb_bd_zaig COM AS CAMPOS                                    |
  NECESSÁRIOS DA Tb_Zaig_Staging_Area                                    |
                                                                         |
                                                                         |
  DELIMITADOR DE CAMPOS: PONTO E VIRGULA                                 |   
                                                                         |
--------------------------------------------------------------------------
*/ 

-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_Zaig_Staging_Area                                        |
-----------------------------------------------------------------------------------
-- select min(data_cadastro) as Primeiro_Registro, max(data_cadastro) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Zaig_Staging_Area`;


-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_bd_zaig                                                  |
-----------------------------------------------------------------------------------
-- select min(data_cadastro) min, max(data_cadastro) max FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig`;


-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Dw_Zaig                                                  |
-----------------------------------------------------------------------------------
-- select min(data_cadastro) min, max(data_cadastro) max FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig`;


--CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig` AS 
--select distinct *from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig`
INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig` 

    select 
    distinct
        esteira
        ,natural_person_id
        ,cpf
        ,nome
        ,email
        ,ddd
        ,numero
        ,nome_da_mae
        ,rua
        ,numero_9
        ,bairro
        ,cidade
        ,estado
        ,cep
        ,pais
        ,data_cadastro 
        ,decisao
        ,razao
        ,indicators
        ,session_id
        ,modelo_do_dispositivo
        ,plataforma
        ,ip
        ,pais_do_ip
        ,ip_tor
        ,cast(gps_latitude as STRING) as gps_latitude
        ,cast(gps_longitude as STRING) as gps_longitude
        ,cast(device_scan_date as STRING) as data_device_scan
        ,cast(tree_score as INTEGER) as tree_score
        ,makrosystem_score 
        --,cast(makrosystem_score as STRING) as makrosystem_score

    FROM  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Zaig_Staging_Area` 
    where TIMESTAMP(data_cadastro) >= (select max(TIMESTAMP(data_cadastro)) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig` )


;

--CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` AS 
--select distinct *from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig`
INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig`

        select 
        distinct
        esteira
        ,natural_person_id
        ,cpf
        ,nome
        ,email
        ,ddd
        ,numero
        ,nome_da_mae
        ,rua
        ,numero_9
        ,bairro
        ,cidade
        ,estado
        ,cep
        ,pais
        ,data_cadastro as DT_REGISTO_ZAIG_HRAMERICANO
        ,TIMESTAMP(DATETIME(data_cadastro,'America/Sao_Paulo')) as data_cadastro
        ,decisao
        ,razao
        ,indicators
        ,session_id
        ,modelo_do_dispositivo
        ,plataforma
        ,ip
        ,pais_do_ip
        ,ip_tor
        ,gps_latitude
        ,gps_longitude
        ,data_device_scan 
        ,tree_score
        ,cast(makrosystem_score as INTEGER) as score_makrosystem
        ,REPLACE(REPLACE(cpf,'.', ''),'-', '') as Cpf_Cliente
        FROM  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig` a
        where TIMESTAMP(data_cadastro) >= (select max(TIMESTAMP(data_cadastro)) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` )

;

/*
select date(data_cadastro), count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` group by 1 order by 1
;
*/

/* 
--------------------------------------------------------------------------
                                                                         |
                                                                         |
  CRIAÇÃO DA Tb_bd_zaig_PJ COM AS CAMPOS                                 |
  NECESSÁRIOS DA Tb_Zaig_PJ_Staging_Area                                 |
                                                                         |
                                                                         |
  DELIMITADOR DE CAMPOS: PONTO E VIRGULA                                 |   
                                                                         |
--------------------------------------------------------------------------
*/ 

-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_Zaig_PJ_Staging_Area                                     |
-----------------------------------------------------------------------------------
-- select min(data_cadastro) as Primeiro_Registro, max(data_cadastro) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Zaig_PJ_Staging_Area`;


-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_bd_zaig_PJ                                               |
-----------------------------------------------------------------------------------
-- select min(data_cadastro) min, max(data_cadastro) max FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_PJ`;


-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Dw_Zaig_PJ                                                  |
-----------------------------------------------------------------------------------
-- select min(data_cadastro) min, max(data_cadastro) max FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_PJ`;


--CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_PJ` AS 
INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_PJ`
  select distinct
      esteira, 
      legal_person_id, 
      cnpj, 
      razao_social, 
      nome_fantasia, 
      ddd, 
      numero, 
      rua, 
      numero_endereco, 
      bairro, 
      cidade, 
      estado, 
      cep, 
      pais, 
      data_cadastro, 
      decisao, 
      razao, 
      indicadores, 
      tree_score
  FROM  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Zaig_PJ_Staging_Area` 
  where TIMESTAMP(data_cadastro) >= (select max(TIMESTAMP(data_cadastro)) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_PJ`)
;


---------------------------------------------------------------------------------------------------------------------------------------------------------------------

-------------------------------------------
-- TABELA HISTORICO - Dw_Zaig_PJ          |
-------------------------------------------

-- CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_PJ` AS 
INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_PJ`
    select distinct
        esteira, 
        legal_person_id, 
        cnpj, 
        razao_social, 
        nome_fantasia, 
        ddd, 
        numero, 
        rua, 
        numero_endereco, 
        bairro, cidade, 
        estado, 
        cep, 
        pais, 
        data_cadastro as DT_REGISTO_ZAIG_HRAMERICANO,
        TIMESTAMP(DATETIME(data_cadastro,'America/Sao_Paulo')) as data_cadastro,
        decisao, 
        razao, 
        indicadores, 
        tree_score
        FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_PJ` a
        where TIMESTAMP(data_cadastro) >= (select max(TIMESTAMP(data_cadastro)) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_PJ`)
;




/* 
--------------------------------------------------------------------------
                                                                         |
                                                                         |
  CRIAÇÃO DA Tabela_Unico_SMS_BIO_Telefone_Hist COM AS CAMPOS            |
  NECESSÁRIOS DA Tb_Unico_SMS_Staging_Area                               |
                                                                         |
                                                                         |
  DELIMITADOR DE CAMPOS: PONTO E VIRGULA                                 |   
                                                                         |
--------------------------------------------------------------------------
*/ 

-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_Unico_SMS_Staging_Area                               |
-----------------------------------------------------------------------------------
-- select count(*) volume, min(DATA_ENVIO_SMS) as Primeiro_Registro, max(DATA_ENVIO_SMS) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_SMS_Staging_Area`;


-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tabela_Unico_SMS_BIO_Telefone_Hist                               |
-----------------------------------------------------------------------------------
-- select count(*) volume, min(DATA_ENVIO_SMS) as Primeiro_Registro, max(DATA_ENVIO_SMS) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone_Hist`;


--CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone_Hist`  AS
INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone_Hist` 

SELECT
distinct
  a.DATA_ENVIO_SMS,
  cast(replace(replace(a.CPF,".",""),"-","") as int64) as CPF,
  a.NOME,
  a.E_MAIL,
  a.TELEFONE,
  a.PEDIDO,
  a.TEMPLATE,
  a.ENVIO_SMS,
  a.ETAPA_SMS,
  a.LIVENESS,
  a.TIPIFICA____O as TIPIFICACAO,
  a.FACEMATCH,
  a.OCRCODE,
  a.RESULTADO_AN__LISE as RESULTADO_ANALISE,
  a.DURA____O as DURACAO,
  a.SCORE,
  CURRENT_DATE AS DataInclusaoBase
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_SMS_Staging_Area` a
where date(a.DATA_ENVIO_SMS) > (select max(date(DATA_ENVIO_SMS)) FROM  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone_Hist` )
order by 1 desc
;




/* 
--------------------------------------------------------------------------
                                                                         |
                                                                         |
  CRIAÇÃO DA Tb_BIO_Cadastrada_2, Tb_BIO_Cadastrada_3 COM AS CAMPOS      |
  NECESSÁRIOS DA Tb_Unico_BIO_Staging_Area                               |
                                                                         |
                                                                         |
  DELIMITADOR DE CAMPOS: PONTO E VIRGULA                                 |   
                                                                         |
--------------------------------------------------------------------------
*/ 

-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_Unico_BIO_Staging_Area                                   |
-----------------------------------------------------------------------------------
-- select count(*) volume, min(DATA) as Primeiro_Registro, max(DATA) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_BIO_Staging_Area`;


-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_BIO_Cadastrada_2                                         |
-----------------------------------------------------------------------------------
-- select count(*) volume, min(created_at) as Primeiro_Registro, max(created_at) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_2`;


-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_BIO_Cadastrada_3                                         |
-----------------------------------------------------------------------------------
-- select count(*) volume, min(created_at) as Primeiro_Registro, max(created_at) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_3`;

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_2` AS 

with
base as (
SELECT 
Distinct
REPLACE(REPLACE(CPF,'.', ''),'-', '') as CPF
,FORMAT_DATE("%Y%m",DATA)as Safra_Ev
,DATA as created_at
,LIVENESS as Status_Conta_EV
,RESULTADO as Resultado
,'ResetSenha' as Origem
,RANK() OVER (PARTITION BY CPF ORDER BY DATA desc) AS Rank_Ult_Atual


FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_BIO_Staging_Area` 
where DATA is not null
order by 1 

)
select * from base where Rank_Ult_Atual = 1

;



CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_3` AS 

with
base_UltimoScore as (
select
CPF
,max(DATA_ENVIO_SMS) as DATA_ENVIO_SMS
,max(SCORE) as SCORE
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_SMS_Staging_Area` 
group by 1
)
select 
Distinct
cast(ultSocre.CPF as STRING) as CPF
,FORMAT_DATE("%Y%m",ultSocre.DATA_ENVIO_SMS)as Safra_Ev
,ultSocre.DATA_ENVIO_SMS as created_at
,Bio.LIVENESS as Status_Conta_EV
--,ultSocre.SCORE
,case 
  when ultSocre.SCORE < 0 then 'Negado'
  when ultSocre.SCORE = 0 then 'Neutro'
  when ultSocre.SCORE > 0 then 'Aprovado'
  else 'SemRetorno' end as Resultado
,Bio.TEMPLATE as Origem
,RANK() OVER (PARTITION BY ultSocre.CPF ORDER BY ultSocre.DATA_ENVIO_SMS desc) AS Rank_Ult_Atual
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_SMS_Staging_Area` Bio
join base_UltimoScore ultSocre on ultSocre.CPF = Bio.CPF and ultSocre.SCORE = Bio.SCORE
--where Bio.CPF = 10069686440
order by 1,2 desc

;



/* 
--------------------------------------------------------------------------
                                                                         |
                                                                         |
  CRIAÇÃO DA Tb_Transacional_Aereas_PayPal_V2 COM AS CAMPOS              |
  NECESSÁRIOS DA Tb_Aereas_PayPal_Staging_Area                           |
                                                                         |
                                                                         |
  DELIMITADOR DE CAMPOS: VIRGULA                                         |   
                                                                         |
--------------------------------------------------------------------------
*/ 

----------------------------------------------------------------------------------------------------------------------
-- DEVIDO AOS DADOS NÃO ESTAREM BEM ESTRUTURADOS, OS CAMPOS PRECISAM SER DEFINIDOS MANUALMENTE COMO STRING           |
-- NA PRÓXIMA ETAPA OS CAMPOS SÃO CORRIGIDOS                                                                         |
----------------------------------------------------------------------------------------------------------------------


/*

select date(Created_Datetime),Risk_Decision, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2` 
group by 1,2
order by 1 desc
;

*/

/*


create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2` as 
with base_aereas as (
select 
 distinct 
    Transaction_ID, 
    Subscription_ID, 
    Transaction_Type, 
    Transaction_Status, 
    Escrow_Status, 
    PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', Created_Datetime) AS Created_Datetime,
    CAST(Created_Timezone AS INT64) AS Created_Timezone, 
    PARSE_DATE('%m/%d/%Y', Settlement_Date) AS Settlement_Date,
    PARSE_DATE('%m/%d/%Y', Disbursement_Date) AS Disbursement_Date, 
    Merchant_Account, 
    Currency_ISO_Code,
    CAST(Amount_Authorized AS FLOAT64) AS Amount_Authorized,
    CAST(Amount_Submitted_For_Settlement AS FLOAT64) AS Amount_Submitted_For_Settlement,
    Service_Fee, 
    Tax_Amount, 
    Tax_Exempt, 
    Purchase_Order_Number, 
    Order_ID, 
    Descriptor_Name, 
    Descriptor_Phone, 
    Descriptor_URL, 
    Refunded_Transaction_ID,
    Payment_Instrument_Type,
    Card_Type, 
    Cardholder_Name,
    CAST(First_Six_of_Credit_Card AS INT64) AS First_Six_of_Credit_Card,
    CAST(Last_Four_of_Credit_Card AS INT64) AS Last_Four_of_Credit_Card,
    Credit_Card_Number,
    Expiration_Date,
    Credit_Card_Customer_Location,
    Customer_ID,
    Payment_Method_Token, 
    Credit_Card_Unique_Identifier, 
    Customer_First_Name, 
    Customer_Last_Name, 
    Customer_Company,
    Customer_Email, 
    Customer_Phone, 
    Customer_Fax, 
    Customer_Website, 
    Billing_Address_ID, 
    Billing_First_Name, 
    Billing_Last_Name, 
    Billing_Company, 
    Billing_Street_Address,
    Billing_Extended_Address, 
    Billing_City__Locality_, 
    Billing_State_Province__Region_, 
    Billing_Postal_Code, 
    Billing_Country, 
    Shipping_Address_ID, 
    Shipping_First_Name, 
    Shipping_Last_Name, 
    Shipping_Company, 
    Shipping_Street_Address, 
    Shipping_Extended_Address, 
    Shipping_City__Locality_, 
    Shipping_State_Province__Region_, 
    Shipping_Postal_Code, 
    Shipping_Country, 
    User,
    IP_Address,
    Creating_Using_Token,
    Transaction_Source, Authorization_Code, 
    CAST(Processor_Response_Code AS INT64) AS Processor_Response_Code,
    Processor_Response_Text, 
    Gateway_Rejection_Reason, 
    Postal_Code_Response_Code, 
    Street_Address_Response_Code, 
    AVS_Response_Text, 
    CVV_Response_Code, 
    CVV_Response_Text, 
    CAST(Settlement_Amount AS FLOAT64) AS Settlement_Amount,
    Settlement_Currency_ISO_Code, 
    CAST(Settlement_Currency_Exchange_Rate AS FLOAT64) AS Settlement_Currency_Exchange_Rate,
    Settlement_Base_Currency_Exchange_Rate, 
    Settlement_Batch_ID, 
    Fraud_Detected, 
    Disputed_Date,
    Authorized_Transaction_ID, 
    Bairro_do_Endere__o AS Bairro_do_Endereco, 
    CEP, 
    CPF,
    Complemento_do_Endere__o AS Complemento_do_Endereco, 
    Endere__o AS Endereco, 
    Estado_do_Endere__o AS Estado_do_Endereco, 
    N__mero_do_Endere__o AS Numero_do_Endereco, 
    Pa__s_do_Endere__o AS Pais_do_Endereco, 
    Country_of_Issuance, 
    Issuing_Bank, 
    Durbin_Regulated, 
    Commercial,
    Prepaid, 
    Payroll, 
    Healthcare,
    Affluent_Category,
    Debit,
    Product_ID, 
    _3DS__Authentication_ID,
    _3DS___Status, 
    _3DS___PARes_Status, 
    _3DS___ECI_Flag, 
    _3DS___CAVV, 
    _3DS___Signature_Verification, 
    _3DS___Version, 
    _3DS___XID, 
    _3DS___DS_Transaction_ID, 
    _3DS___Challenge_Requested, 
    _3DS___Exemption_Requested, 
    _3DS___Merchant_Requested_Exemption_Type, 
    _3DS___Rule_Summary,
    _3DS___SCA_Exemption_Type, 
    _3DS___Merchant_Requested_SCA_exemption, 
    PayPal_Payer_Email, 
    PayPal_Payment_ID, 
    PayPal_Authorization_ID, 
    PayPal_Debug_ID, 
    PayPal_Capture_ID, 
    PayPal_Refund_ID, 
    PayPal_Custom_Field, 
    PayPal_Payer_ID, 
    PayPal_Payer_First_Name, 
    PayPal_Payer_Last_Name, 
    PayPal_Seller_Protection_Status, 
    PayPal_Transaction_Fee_Amount, 
    PayPal_Transaction_Fee_Currency_ISO_Code, 
    PayPal_Refund_From_Transaction_Fee_Amount, 
    PayPal_Refund_From_Transaction_Fee_Currency_ISO_Code, 
    PayPal_Payee_Email, 
    PayPal_Payee_ID, 
    Apple_Pay_Card_Last_Four, 
    Apple_Pay_Card_Expiration_Month, 
    Apple_Pay_Card_Expiration_Year, 
    Apple_Pay_Cardholder_Name, 
    Android_Pay_Source_Card_Last_Four, 
    Android_Pay_Source_Card_Type, 
    Source_Card_Last_Four, 
    Risk_ID, 
    Risk_Decision, 
    Device_Data_Captured, 
    Decision_Reasons, 
    Fraud_Protection_Chargeback_Protections, 
    Acquirer_Reference_Number, 
    Venmo_Username, 
    Venmo_Profile_ID, 
    ACH_Reason_Code 
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Aereas_PayPal_Staging_Area`
 ) select * from base_aereas 
;


*/



-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_Aereas_PayPal_Staging_Area                               |
-----------------------------------------------------------------------------------
-- select min(Created_Datetime) as Primeiro_Registro, max(Created_Datetime) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Aereas_PayPal_Staging_Area`;
-- OBS: DATAS INVERTIDAS NESSA ETAPA!!!


-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_Transacional_Aereas_PayPal_V2                            |
-----------------------------------------------------------------------------------
-- select min(Created_Datetime) as Primeiro_Registro, max(Created_Datetime) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2`;



-- SELECT * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2` LIMIT 10; 


INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2`
WITH base_aereas AS (
    SELECT DISTINCT
        Transaction_ID, 
        Subscription_ID, 
        Transaction_Type, 
        Transaction_Status, 
        Escrow_Status,
        PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', Created_Datetime) AS Created_Datetime,
        CAST(Created_Timezone AS INT64) AS Created_Timezone, 
        PARSE_DATE('%m/%d/%Y', Settlement_Date) AS Settlement_Date,
        PARSE_DATE('%m/%d/%Y', Disbursement_Date) AS Disbursement_Date, 
        Merchant_Account, 
        Currency_ISO_Code,
        CAST(Amount_Authorized AS FLOAT64) AS Amount_Authorized,
        CAST(Amount_Submitted_For_Settlement AS FLOAT64) AS Amount_Submitted_For_Settlement,
        Service_Fee, 
        Tax_Amount, 
        Tax_Exempt, 
        Purchase_Order_Number, 
        Order_ID, 
        Descriptor_Name, 
        Descriptor_Phone, 
        Descriptor_URL, 
        Refunded_Transaction_ID, 
        Payment_Instrument_Type, 
        Card_Type, 
        Cardholder_Name,
        CAST(First_Six_of_Credit_Card AS INT64) AS First_Six_of_Credit_Card,
        CAST(Last_Four_of_Credit_Card AS INT64) AS Last_Four_of_Credit_Card,
        Credit_Card_Number, 
        Expiration_Date, 
        Credit_Card_Customer_Location, 
        Customer_ID, 
        Payment_Method_Token, 
        Credit_Card_Unique_Identifier, 
        Customer_First_Name, 
        Customer_Last_Name, 
        Customer_Company, 
        Customer_Email, 
        Customer_Phone, 
        Customer_Fax, 
        Customer_Website, 
        Billing_Address_ID, 
        Billing_First_Name, 
        Billing_Last_Name, 
        Billing_Company, 
        Billing_Street_Address, 
        Billing_Extended_Address, 
        Billing_City__Locality_, 
        Billing_State_Province__Region_, 
        Billing_Postal_Code, 
        Billing_Country, 
        Shipping_Address_ID, 
        Shipping_First_Name, 
        Shipping_Last_Name, 
        Shipping_Company, 
        Shipping_Street_Address, 
        Shipping_Extended_Address, 
        Shipping_City__Locality_, 
        Shipping_State_Province__Region_, 
        Shipping_Postal_Code, 
        Shipping_Country, 
        User, 
        IP_Address, 
        Creating_Using_Token, 
        Transaction_Source, 
        Authorization_Code,
        CAST(Processor_Response_Code AS INT64) AS Processor_Response_Code,
        Processor_Response_Text, 
        Gateway_Rejection_Reason, 
        Postal_Code_Response_Code, 
        Street_Address_Response_Code, 
        AVS_Response_Text, 
        CVV_Response_Code, 
        CVV_Response_Text,
        CAST(Settlement_Amount AS FLOAT64) AS Settlement_Amount,
        Settlement_Currency_ISO_Code,
        CAST(Settlement_Currency_Exchange_Rate AS FLOAT64) AS Settlement_Currency_Exchange_Rate,
        Settlement_Base_Currency_Exchange_Rate, 
        Settlement_Batch_ID, 
        Fraud_Detected, 
        Disputed_Date, 
        Authorized_Transaction_ID,
        Bairro_do_Endere__o AS Bairro_do_Endereco, 
        CEP, 
        CPF,
        Complemento_do_Endere__o AS Complemento_do_Endereco, 
        Endere__o AS Endereco, 
        Estado_do_Endere__o AS Estado_do_Endereco, 
        N__mero_do_Endere__o AS Numero_do_Endereco, 
        Pa__s_do_Endere__o AS Pais_do_Endereco, 
        Country_of_Issuance, 
        Issuing_Bank, 
        Durbin_Regulated, 
        Commercial, 
        Prepaid, 
        Payroll, 
        Healthcare, 
        Affluent_Category, 
        Debit, 
        Product_ID,
        _3DS__Authentication_ID, 
        _3DS___Status, 
        _3DS___PARes_Status, 
        _3DS___ECI_Flag, 
        _3DS___CAVV, 
        _3DS___Signature_Verification, 
        _3DS___Version, 
        _3DS___XID, 
        _3DS___DS_Transaction_ID, 
        _3DS___Challenge_Requested, 
        _3DS___Exemption_Requested, 
        _3DS___Merchant_Requested_Exemption_Type, 
        _3DS___Rule_Summary, 
        _3DS___SCA_Exemption_Type, 
        _3DS___Merchant_Requested_SCA_exemption, 
        PayPal_Payer_Email, 
        PayPal_Payment_ID, 
        PayPal_Authorization_ID, 
        PayPal_Debug_ID, 
        PayPal_Capture_ID, 
        PayPal_Refund_ID, 
        PayPal_Custom_Field, 
        PayPal_Payer_ID, 
        PayPal_Payer_First_Name, 
        PayPal_Payer_Last_Name, 
        PayPal_Seller_Protection_Status, 
        PayPal_Transaction_Fee_Amount, 
        PayPal_Transaction_Fee_Currency_ISO_Code, 
        PayPal_Refund_From_Transaction_Fee_Amount, 
        PayPal_Refund_From_Transaction_Fee_Currency_ISO_Code, 
        PayPal_Payee_Email, 
        PayPal_Payee_ID, 
        Apple_Pay_Card_Last_Four, 
        Apple_Pay_Card_Expiration_Month, 
        Apple_Pay_Card_Expiration_Year, 
        Apple_Pay_Cardholder_Name, 
        Android_Pay_Source_Card_Last_Four, 
        Android_Pay_Source_Card_Type, 
        Source_Card_Last_Four, 
        Risk_ID, 
        Risk_Decision, 
        Device_Data_Captured, 
        Decision_Reasons, 
        Fraud_Protection_Chargeback_Protections, 
        Acquirer_Reference_Number, 
        Venmo_Username, 
        Venmo_Profile_ID, 
        ACH_Reason_Code  
    FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Aereas_PayPal_Staging_Area`  
)
SELECT * FROM base_aereas 
WHERE Created_Datetime >= (SELECT MAX(Created_Datetime) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2`);



/* 
--------------------------------------------------------------------------
                                                                         |
                                                                         |
  CRIAÇÃO DA Base_Consolidada_PayPal_DataLake_V2 COM AS CAMPOS           |
  NECESSÁRIOS DA Tb_Chargeback_PayPal_Staging_Area                       |
                                                                         |
  DELIMITADOR DE CAMPOS: VIRGULA                                         |
                                                                         |
--------------------------------------------------------------------------
*/ 

-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_Chargeback_PayPal_Staging_Area                           |
-----------------------------------------------------------------------------------
--select min(Effective_Date) as Primeiro_Registro, max(Effective_Date) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Chargeback_PayPal_Staging_Area`;



-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Base_Consolidada_PayPal_DataLake_V2                         |
-----------------------------------------------------------------------------------
-- select min(Effective_Date) as Primeiro_Registro, max(Effective_Date) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_V2`;


-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Base_Consolidada_PayPal_DataLake_cbk_historico                         |
-----------------------------------------------------------------------------------
-- select min(Effective_Date) as Primeiro_Registro, max(Effective_Date) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico`;



CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_V2` AS 
select 
    distinct
    Dispute_ID,
    Original_Dispute_ID,
    Received_Date,
    Effective_Date,
    Last_Updated,
    Transaction_Date,
    Amount_Disputed,
    Amount_Won,
    Transaction_Amount,
    Currency_ISO_Code,
    Kind,
    Reason,
    Status,
    Transaction_ID,
    Merchant_Account,
    Order_ID,
    Credit_Card_Number,
    Card_Type,
    Customer_Name,
    Customer_Email,
    Refunded,
    Reply_Before_Date,
    Payment_Method_Token,
    Unprotected_Reason as Chargeback_Protection,
    SUBSTR(Credit_Card_Number, 1, 6) AS BIN
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Chargeback_PayPal_Staging_Area`  
;


INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico`
-- create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` as

SELECT
 distinct
 *
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_V2`
where Effective_Date >= (select max(Effective_Date) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` )

;

-- select Unprotected_Reason, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Chargeback_PayPal_Staging_Area`  group by 1;


-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_V2`