with

Base_AllowMe as (

select
distinct
--transaction_id
user_id as CPF_Cliente
,cl.status as StatusConta
,cl.created_at as Dt_Abetura
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
,REPLACE(JSON_EXTRACT(metadata, '$.name'),'"', '') as name
,REPLACE(JSON_EXTRACT(metadata, '$.email'),'"', '') as email
,substr(REPLACE(JSON_EXTRACT(metadata, '$.phone'),'"', ''), 1,2) as DDD
,REPLACE(JSON_EXTRACT(metadata, '$.phone'),'"', '') as phone
,REPLACE(JSON_EXTRACT(metadata, '$.source'),'"', '') as source
,REPLACE(JSON_EXTRACT(metadata, '$.onboardingid'),'"', '') as onboardingid


FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe` allowme
left join `eai-datalake-data-sandbox.core.customers` cl on cl.document = allowme.user_id
where 
date(created_at_dtAllowme) between '2024-04-01' and '2024-05-31'
--and integration = 'cadastro'
order by 1,2
), Base_AllowMe2 as (
SELECT
distinct
case when rules_matched like '%48%' then 'Alteração de fingerprint por endereço IP ignorando a variável usuário' else 'Outros' end as FlagRegra
,bd.*

from (Select * from Base_AllowMe where  Rank_Ult_DecAllowMe = 1) bd

order by 2 desc
) select * from Base_AllowMe2
where FlagRegra = 'Alteração de fingerprint por endereço IP ignorando a variável usuário'
order by created_at_dtAllowme 

/*),Base_AllowMe2_DevicePorCpf as (
select
browser_cookie
,count(distinct CPF_Cliente) as Qtd_CPF
from Base_AllowMe2
group by 1
order by 2 desc
)
select
distinct
bd.*
,case when dv.browser_cookie = bd.browser_cookie then '>3DevicePorCPF' else 'NA' end as Flag_Device
from Base_AllowMe2 bd
left join (select * from Base_AllowMe2_DevicePorCpf where Qtd_CPF > 3) Dv on dv.browser_cookie = bd.browser_cookie
where FlagRegra = '48'
*/
