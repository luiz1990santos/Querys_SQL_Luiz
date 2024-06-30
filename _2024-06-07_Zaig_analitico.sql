-- Contas Light 72.817
with base_light as (
  select distinct 
        *, 
        RANK() OVER (PARTITION BY CPF, date(data_cadastro), esteira ORDER BY EXTRACT(time FROM data_cadastro) desc) AS Rank_Ult_Decisao 
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig`
),
Base_Ult_Decisao_Light as (
  select
    *
  from base_light
  where Rank_Ult_Decisao = 1 
    and esteira = 'Abastece Aí - Light' 
    and decisao = 'automatically_approved' 
    and date(data_cadastro) between '2024-04-01' and '2024-04-30'
),
base_full as (
  select distinct 
        *, 
        RANK() OVER (PARTITION BY CPF, date(data_cadastro), esteira ORDER BY EXTRACT(time FROM data_cadastro) desc) AS Rank_Ult_Decisao 
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig`
),
Base_Ult_Decisao_Full as (
  select
    *
  from base_full
  where Rank_Ult_Decisao = 1 
    and esteira = 'Abastece Aí' 
    --and decisao = 'automatically_approved' 
    and date(data_cadastro) between '2024-04-01' and '2024-04-30'
)--, base_light_nao_seguram_full as (
select 
  l.*
from Base_Ult_Decisao_Light l
left join Base_Ult_Decisao_Full f
on l.CPF = f.CPF
where f.CPF is null
/*
)select * from base_light_nao_seguram_full as a 
left join `eai-datalake-data-sandbox.core.customers` as b
a.CPF 
where document = '9905477799'
*/
