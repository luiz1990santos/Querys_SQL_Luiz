--==========================================================================================================
-- Base Monitoramento AllowMe
--==========================================================================================================

--------------------------
--  Tb_bd_AllowMe        |
--------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_Crivo_AllowMe` where CPF_Cliente = '00207200092'

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_Crivo_AllowMe` where UF_DDD is not null

-- select min(created_at_dtAllowme) as Data_Primeiro_Registro, max(created_at_dtAllowme) as Data_Ultimo_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_Crivo_AllowMe`

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_AllowMe_Ult_Decisao` limit 10


-- BASE DE DADOS HISTORICO ALLOWME

-- select * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe`
-- select integration,count(*) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe` group by 1
-- select * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_AllowMe`
-- select min(TIMESTAMP(created_at)) min, max(TIMESTAMP(created_at)) max, count(*) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_AllowMe`
-- select min(TIMESTAMP(created_at)) min, max(TIMESTAMP(created_at)) max FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe`
-- select date(created_at) data, count(*) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_AllowMe` group by 1 order by 1 desc
-- select * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_AllowMe` order by 3 
-- select integration, count(*) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_AllowMe` group by 1 order by 1 
-- select * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_bd_allowme_teste`


 --CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe` AS 
 --CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_AllowMe` AS 
INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe`
--CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_AllowMe` AS 


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
        where TIMESTAMP(a.created_at) > (select max(TIMESTAMP(created_at_dtAllowme)) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe` )

;


----------------------------------------------------------------------------------------------------------------------------------------------------

/*
--------------------------Cadastro----------------------------------------------------------

 CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_AllowMe_Cadastro` AS 

with
--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe` where   user_id = 29605878801 order by 3
base_login as (

select 
distinct
transaction_id
,created_at_dtAllowme
,integration
,user_id
,ip_address
,browser_cookie
,os_numberOfCPUCores
,os_memory
,app_id
,mobile_model_name
,mobile_manufacturer_name
,mobile_os_name
,mobile_os_version
,similarity_percentage
,authorized
,authorized_at
,fraud
,rules_matched
,rules_details
,metadata
,score_classification
,score

FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe`
where date(created_at_dtAllowme) >=  current_date - 365
and integration = 'cadastro' -- 'cadastro' 'login'
and fraud <> 'FALSO'

), Convert_Json as (
select
a.*
,REPLACE(JSON_EXTRACT(rules_details, '$.name'),'"', '') as name
,REPLACE(JSON_EXTRACT(rules_details, '$.email'),'"', '') as email
,REPLACE(JSON_EXTRACT(rules_details, '$.phone'),'"', '') as phone
,REPLACE(JSON_EXTRACT(rules_details, '$.source'),'"', '') as source
,REPLACE(JSON_EXTRACT(rules_details, '$.onboardingid'),'"', '') as onboardingid
,substr(fraud, 1,2) as Cod_Regra_1Regra
,RANK() OVER (PARTITION BY user_id,date(created_at_dtAllowme) ORDER BY EXTRACT(time FROM created_at_dtAllowme) desc) AS Rank_Ult_DecAllowMe
from base_login a
)
select 
*
from Convert_Json
where Rank_Ult_DecAllowMe = 1
;
*/


---------------------------------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_AllowMe_Ult_Decisao` AS 
with

Base_AllowMe as (

select
distinct
--transaction_id
user_id as CPF_Cliente
,cl.status as StatusConta
,cl.created_at as Dt_Abertura
,cl.trusted 
,case when cl.document = allowme.user_id then 'EAI' else 'NaoCliente' end as Flag_Cliente
,RANK() OVER (PARTITION BY user_id ORDER BY created_at_dtAllowme desc) AS Rank_Ult_DecAllowMe
,created_at_dtAllowme
,FORMAT_DATETIME("%Y%m",created_at_dtAllowme) as Safra_Cadastro
,integration
,ip_address
,browser_cookie
--,os_numberOfCPUCores
--,os_memory
,app_id
,mobile_model_name
,mobile_manufacturer_name
,mobile_os_name
,mobile_os_version
,similarity_percentage
,authorized
,authorized_at
,fraud
,case when fraud = 'false' then 'Aprovado' else 'Negado' end as Flag_Decisao
,rules_matched
,rules_details
,metadata
,score_classification
,score
,REPLACE(JSON_EXTRACT(rules_details, '$.name[0]'),'"', '') as nome
,REPLACE(JSON_EXTRACT(metadata, '$.addresses[0].street'),'"', '') AS rua
,REPLACE(JSON_EXTRACT(metadata, '$.addresses[0].number'),'"', '') AS numero
,REPLACE(JSON_EXTRACT(metadata, '$.addresses[0].neighborhood'),'"', '') AS bairro
,REPLACE(JSON_EXTRACT(metadata, '$.addresses[0].city'),'"', '') AS cidade
,REPLACE(JSON_EXTRACT(metadata, '$.addresses[0].zipcode'),'"', '') AS CEP
,UPPER(REPLACE(JSON_EXTRACT(metadata, '$.addresses[0].state'),'"', '')) as UF
,REPLACE(JSON_EXTRACT(metadata, '$.email'),'"', '') as email
,substr(REPLACE(JSON_EXTRACT(metadata, '$.phones[0]'),'"', ''), 3,2) as DDD
,substr(REPLACE(JSON_EXTRACT(metadata, '$.phones[0]'),'"', ''), 5) as telefone
,substr(REPLACE(JSON_EXTRACT(metadata, '$.phones[0]'),'"', ''), 3) as DDD_telefone
,UPPER(REPLACE(JSON_EXTRACT(metadata, '$.addresses[0].state'),'"', '')) || substr(REPLACE(JSON_EXTRACT(metadata, '$.phones[0]'),'"', ''), 3,2) as UF_DDD



FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe` allowme
left join `eai-datalake-data-sandbox.core.customers` cl on cl.document = allowme.user_id
where 
date(created_at_dtAllowme) >=  current_date - 60
--and integration = 'cadastro'
order by 1,2)
select * from Base_AllowMe where Rank_Ult_DecAllowme = 1
--and UF is not null
-- and CPF_Cliente = '00000004006'
-- limit 100
;


-------------------------------------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_Crivo_AllowMe` AS 

with

Base_AllowMe as (

select
distinct
*
,CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=1 THEN '00_<1DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=5 THEN '01_<5DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=30 THEN '02_<30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=60 THEN '03_<60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=90 THEN '04_<90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=120 THEN '05_<120DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=160 THEN '06_<160DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=190 THEN '07_<190DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=220 THEN '08_<220DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=260 THEN '09_<260DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=290 THEN '10_<290DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=365 THEN '11_1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) >=365 THEN '12_+1ANO'
    else '13_NaoTemConta'
END AS Temp_Conta

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_AllowMe_Ult_Decisao`
),Base_AllowMe2 as (
SELECT
distinct
case when rules_matched like '%16%' then 'BateramRegraDevice' else 'Outros' end as FlagRegra
,case when email like '%teste%' or email like '%Teste%' or email like '%TESTE%' then 'Risco' else 'NA' end Flag_Valida_email
,case when nome like '%teste%' or nome like '%Teste%' or nome like '%TESTE%' then 'Risco' else 'NA' end Flag_Valida_name
,bd.*

from (Select * from Base_AllowMe where  Rank_Ult_DecAllowMe = 1) bd

order by 2 desc
),Base_AllowMe2_DevicePorCpf as (
select
browser_cookie
,count(distinct CPF_Cliente) as Qtd_CPF
,count(distinct DDD_Telefone) as Qtd_Telefone
from Base_AllowMe2
group by 1
order by 2 desc
)
select
distinct
bd.*
,Dv.Qtd_CPF
,Dv.Qtd_Telefone
,case when dv.browser_cookie = bd.browser_cookie then '>3DevicePorCPF' else 'NA' end as Flag_Device
from Base_AllowMe2 bd
left join (select * from Base_AllowMe2_DevicePorCpf where Qtd_CPF > 3) Dv on dv.browser_cookie = bd.browser_cookie
where FlagRegra = 'BateramRegraDevice'
;
