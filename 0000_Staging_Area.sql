/*
Tb_AllowMe_Staging_Area_New
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
--  Tb_AllowMe_Staging_Area_New                | 
                                               |
--  Tb_Zaig_Staging_Area                       |
                                               |
--  Tb_Zaig_PJ_Staging_Area                    |
                                               |
--  Tb_Unico_SMS_Staging_Area                  |
                                               |
--  Tb_Unico_Analistas_Staging_Area            |
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
,os_platform:STRING
,app_id:STRING
,mobile_model_name:STRING
,mobile_manufacturer_name:STRING
,mobile_os_name:STRING
,mobile_os_version:STRING
,device_location_latitude:STRING
,device_location_longitude:STRING
,contextual_id:STRING
,new_device:STRING
,rules_matched:STRING
,rules_details:STRING
,metadata:STRING
,score_classification:STRING
,score:STRING
,device_blocklisted:STRING
,device_network_effect_blocklisted:STRING
,risk_level:STRING
,ip_location_latitude:STRING
,ip_location_longitude:STRING
*/


-----------------------------------------------------------------------------------
-- DEPARA CLASSIFICAÇÃO ALLOWME                                                   |
-----------------------------------------------------------------------------------
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_DePara_Classificacao_AllowMe`;

-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_AllowMe_Staging_Area_New                                 |
-----------------------------------------------------------------------------------
-- select min(created_at) as Primeiro_Registro, max(created_at) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_AllowMe_Staging_Area_New`;


-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Dw_AllowMe_V2                                               |
-----------------------------------------------------------------------------------
-- select min(created_at) min, max(created_at) max FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe_V2`;



CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe_V2` AS 
--INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe_V2`
    with base_allowme as (
        select 
            transaction_id, 
            similar_transaction_id, 
            created_at,
            SUBSTR(created_at, 1, 19) AS created_at_limpo,
            --TIMESTAMP(cast(created_at as DATETIME) ,'America/Sao_Paulo') as created_at,
            integration, 
            user_id, 
            ip_address, 
            os_platform, 
            app_id, 
            mobile_model_name, 
            mobile_manufacturer_name, 
            mobile_os_name, 
            mobile_os_version, 
            device_location_latitude, 
            device_location_longitude, 
            contextual_id, 
            new_device, 
            rules_matched, 
            rules_details, 
            `metadata`, 
            score_classification, 
            cast(score as Numeric) as score, 
            device_blocklisted, 
            device_network_effect_blocklisted, 
            cast(risk_level as Numeric) as risk_level, 
            ip_location_latitude, 
            ip_location_longitude
            FROM  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_AllowMe_Staging_Area_New` 
            
    ), base_limpeza_datas as ( 
            SELECT
                transaction_id, 
                similar_transaction_id, 
                CASE
                    -- Caso o formato seja 'YYYY-MM-DD HH:MM:SS'
                    WHEN REGEXP_CONTAINS(created_at_limpo, r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$') THEN PARSE_DATETIME('%Y-%m-%d %H:%M:%S', created_at_limpo) 
                    -- Caso o formato seja 'DD/MM/YYYY HH:MM'
                    WHEN REGEXP_CONTAINS(created_at, r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$') THEN PARSE_DATETIME('%d/%m/%Y %H:%M', created_at)
                    -- Caso o formato não corresponda a nenhum dos padrões
                    ELSE NULL
                END AS dt_allowme,
                integration, 
                user_id, 
                LPAD(CAST(user_id AS STRING), 11, '0') AS cpf_completo,
                ip_address, 
                os_platform, 
                app_id, 
                mobile_model_name, 
                mobile_manufacturer_name, 
                mobile_os_name, 
                mobile_os_version, 
                device_location_latitude, 
                device_location_longitude, 
                contextual_id, 
                new_device, 
                rules_matched, 
                rules_details, 
                `metadata`, 
                score_classification, 
                score, 
                device_blocklisted, 
                device_network_effect_blocklisted, 
                risk_level, 
                depara.Classificacao,
                CASE 
                    WHEN  /*
                        rules_matched = '9' or 
                        rules_matched like '9,%' or 
                        */
                        rules_matched like '%12%' or 
                        /*
                        rules_matched like '%16%' or 
                        rules_matched like '%17%' or
                        */ 
                        rules_matched like '%28%' or 
                        rules_matched like '%34%' or 
                        rules_matched like '39%' or 
                        /*
                        rules_matched like '%48%' or 
                        */
                        rules_matched like '%51%' 
                        /*
                        rules_matched like '%52%' or 
                        rules_matched like '%62%' 
                        */
                     THEN 'Negado'
                     ELSE 'Aprovado'
                end as Flag_Regras,
                CASE    
                    WHEN device_blocklisted = 'true' THEN 'Sim'
                    WHEN device_blocklisted = 'false' THEN 'Não'
                    else 'Não definido'
                end as Flag_device_lista_bloqueados,
                CASE    
                    WHEN device_network_effect_blocklisted = 'true' THEN 'Sim'
                    WHEN device_network_effect_blocklisted = 'false' THEN 'Não'
                    else 'Não definido'
                end as Flag_device_lista_bloqueados_por_efeito_rede,
                CASE    
                    WHEN new_device = 'true' THEN 'NOVO'
                    WHEN new_device = 'false' THEN 'CONHECIDO' 
                    ELSE 'Sem definição'
                END as Flag_Dispositivo,
                ip_location_latitude, 
                ip_location_longitude
        FROM base_allowme as allowme
        left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_DePara_Classificacao_AllowMe` as depara
        on allowme.risk_level = depara.Codigo
    ) 
     select 
        distinct
            transaction_id, 
            similar_transaction_id, 
            dt_allowme,
            TIMESTAMP(cast(dt_allowme as DATETIME) ,'America/Sao_Paulo') as created_at,
            integration, 
            user_id, 
            cpf_completo,
            ip_address, 
            os_platform, 
            app_id, 
            mobile_model_name, 
            mobile_manufacturer_name, 
            mobile_os_name, 
            mobile_os_version, 
            device_location_latitude, 
            device_location_longitude, 
            contextual_id, 
            new_device, 
            rules_matched, 
            rules_details, 
            `metadata`, 
            score_classification, 
            score, 
            device_blocklisted, 
            device_network_effect_blocklisted, 
            risk_level, 
            case
                when Classificacao is null then 'Não classificado'
                else Classificacao 
            end as classificacao,
            Flag_Regras,
            CONCAT(
                (SELECT STRING_AGG(
                    CONCAT(
                        JSON_VALUE(variable, '$.name')
                    ), 
                    ' | '
                ) FROM UNNEST(JSON_EXTRACT_ARRAY(rules_details, '$')) AS variable)
            ) AS indicators,
            Flag_device_lista_bloqueados,
            Flag_device_lista_bloqueados_por_efeito_rede,
            Flag_Dispositivo,
            ip_location_latitude, 
            ip_location_longitude
    FROM  base_limpeza_datas
    --where TIMESTAMP(a.created_at) >= (select max(TIMESTAMP(created_at_dtAllowme)) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe_V2` )
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

/*

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


*/

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
where date(a.DATA_ENVIO_SMS) >= (select max(date(DATA_ENVIO_SMS)) FROM  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone_Hist` )
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
  CRIAÇÃO DA Tb_Unico_Analistas                                          |
  NECESSÁRIOS DA Tb_Unico_Analistas_Staging_Area                         |
                                                                         |
                                                                         |
  DELIMITADOR DE CAMPOS: PONTO E VIRGULA                                 |   
                                                                         |
--------------------------------------------------------------------------
*/ 

-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_Unico_Analistas_Staging_Area                                   |
-----------------------------------------------------------------------------------
-- select min(DATA_A____O) as Primeiro_Registro, max(DATA_A____O) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_Analistas_Staging_Area`;


-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_Unico_Analistas_Staging_Area                                         |
-----------------------------------------------------------------------------------
-- select count(*) volume, min(created_at) as Primeiro_Registro, max(created_at) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_Analistas_Staging_Area`;


-----------------------------------------------------------------------------------
-- MENOR E MAIOR DATA Tb_Unico_Analistas_Staging_Area                                         |
-----------------------------------------------------------------------------------
-- select count(*) volume, min(created_at) as Primeiro_Registro, max(created_at) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_Analistas_Staging_Area`;

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_Analistas` AS 

with
base as (
    select 
    distinct 
        REPLACE(REPLACE(CNPJ_CPF,'.', ''),'-', '') as CPF, 
        --NOME, 
        SCORE_SUSPEITO, 
        --SCORE, 
        --PARCEIRO, 
        --REGIONAL, 
        --FILIAL, 
        A____O as ACAO, 
        RESPONS__VEL as RESPONSAVEL, 
        DATA_A____O as DATA_ACAO,
        RANK() OVER (PARTITION BY CNPJ_CPF ORDER BY DATA_A____O desc) AS Rank_Ult_Atual


FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_Analistas_Staging_Area` 
--where DATA is not null
order by 1 

), chamado_acao as (
select 
    *,
    case
        when DT_CRIACAO >= DATA_ACAO then 'Chamado na data ou após ação'
        else 'Chamado antigo'
    end as flag_acao

from base as analista
join `eai-datalake-data-sandbox.siebel.chamados` as chamado
on analista.CPF = chamado.CPF
where Rank_Ult_Atual = 1 
) select * from chamado_acao where RESPONSAVEL = 'ANEZIA.BRITO' 



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



------------------------------------------------------------------
-- MAIOR E MENOR DATA DOS FORNECEDORES EMPILLHADOS               |
------------------------------------------------------------------
with bases_fornecedores as (
select '01 - Allow Me' as Fornecedor, count(*) as Volume, min(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', timestamp(dt_allowme))) as Primeiro_Registro, max(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',timestamp(dt_allowme))) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe_V2`
union all 
select '02 - Zaig' as Fornecedor, count(*) as Volume, min(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',timestamp(data_cadastro))) as Primeiro_Registro, max(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',timestamp(data_cadastro))) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Consolidado`
union all
select '03 - Zaig PJ' as Fornecedor, count(*) as Volume, min(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',timestamp(data_cadastro))) as Primeiro_Registro, max(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',timestamp(data_cadastro))) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_PJ`
union all
select '04 - Unico SMS' as Fornecedor, count(*) as Volume, min(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',timestamp(DATA_ENVIO_SMS))) as Primeiro_Registro, max(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',timestamp(DATA_ENVIO_SMS))) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone_Hist`
union all
select '05 - Unico BIO' as Fornecedor, count(*) as Volume, min(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',timestamp(created_at))) as Primeiro_Registro, max(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',timestamp(created_at))) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_3`
union all
select '06 - PayPal Aéreas' as Fornecedor, count(*) as Volume, min(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',timestamp(Created_Datetime))) as Primeiro_Registro, max(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',timestamp(Created_Datetime))) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2`
union all
select '07 - PayPal Dispute' as Fornecedor, count(*) as Volume, min(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',timestamp(Effective_Date))) as Primeiro_Registro, max(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',timestamp(Effective_Date))) as Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico`
) select * from bases_fornecedores order by 1;



------------------------------------------------------------
-- importar bloqueios realizados massivamene               |
-- Tb_Clientes_Bloqueio_Massivo                            |
------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo` order by 3 desc



--------------------------------------
-- Menor e Maior data de Bloqueio    | 
-------------------------------------- 

/*
 
WITH cte AS (
    SELECT 
        Lote,
        Motivo,
        ROW_NUMBER() OVER (ORDER BY Lote ASC) as Primeiro,
        ROW_NUMBER() OVER (ORDER BY Lote DESC) as Ultimo
    FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2`
)
SELECT 
    (SELECT Lote FROM cte WHERE Primeiro = 1) as Primeira_Dt_Bloqueios,
    (SELECT Lote FROM cte WHERE Ultimo = 1) as Ultima_Dt_Bloqueios,
    (SELECT Motivo FROM cte WHERE Primeiro = 1) as Primeiro_Motivo,
    (SELECT Motivo FROM cte WHERE Ultimo = 1) as Ultimo_Motivo
*/

/*


create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2` as 
  with 
  base_bloqueios_massivo as (
  select *, LPAD(CAST(CPF AS STRING), 11, '0') AS cpf_completo from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo`
)select distinct * from base_bloqueios_massivo where cpf_completo <> '00000000000' order by Lote

;

*/

/*
 
 select 
    min(Lote) as Primeira_Dt_Bloqueios, 
    max(Lote) as Ultima_Dt_Bloqueios,
    count(*) as Quantidade_Bloqueios
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2`

*/


/*
with
base_bloqueioMassivo as (
SELECT
 distinct
 CustomerID
,CPF
,Lote
,Motivo
,RANK() OVER (PARTITION BY CPF ORDER BY Lote desc) AS Rank_Bloqueio

FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo` 
order by 2,3 desc
) select * from base_bloqueioMassivo where Rank_Bloqueio = 1
*/



