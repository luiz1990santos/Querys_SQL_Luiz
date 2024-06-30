-- BASE DE DADOS HISTORICO ALLOWME

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



