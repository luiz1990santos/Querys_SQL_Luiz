with
    tri2_2023 as (

    select distinct '2º Trimestre de 2023' as Trimestre, count(transaction_id) as Volume, count(distinct user_id) as CPFs_Distintos  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe`
    where integration = 'login'
    and date(created_at) between '2023-04-01' and '2023-06-30'

), tri3_2023 as (

    select distinct '3º Trimestre de 2023' as Trimestre, count(transaction_id) as Volume, count(distinct user_id) as CPFs_Distintos from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe`
    where integration = 'login'
    and date(created_at) between '2023-07-01' and '2023-09-30'

), tri4_2023 as (

    select distinct '4º Trimestre de 2023' as Trimestre, count(transaction_id) as Volume, count(distinct user_id) as CPFs_Distintos from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe`
    where integration = 'login'
    and date(created_at) between '2023-10-01' and '2023-12-31'

), tri1_2024 as (

    select distinct '1º Trimestre de 2024' as Trimestre, count(transaction_id) as Volume, count(distinct user_id) as CPFs_Distintos from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe`
    where integration = 'login'
    and date(created_at) between '2024-01-01' and '2024-03-31'

  ) select * from tri2_2023
    union all
    select * from tri3_2023
    union all
    select * from tri4_2023
    union all
    select * from tri1_2024
;




/*
select date(created_at), count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe`
group by 1
order by 1
*/
