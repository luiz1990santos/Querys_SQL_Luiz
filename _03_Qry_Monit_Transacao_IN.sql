--======================================================================================
--> MONITORAMENTO CLIENTES TRANSACIONANDO IN - 60 DIAS
--======================================================================================


-- select status, count(*) from `eai-datalake-data-sandbox.elephant.transaction` WHERE date(created_transaction_at) >= current_date - 5 group by 1
-- select * from `eai-datalake-data-sandbox.core.customers` WHERE date(created_at) >= current_date - 5



CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacao_In` AS


with

Base_Dados as (
SELECT
DISTINCT
    FORMAT_DATETIME('%Y%m',a.created_transaction_at) as Safra
    ,a.customer_id
    ,cl.document as cpf
    --,date(cl.created_at) as dt_conta
    --,current_date as mis_date
    ,DATE_DIFF(date(current_date), date(cl.created_at), DAY) as Temp_Conta

    ,case 
    when ub.customer_id = a.customer_id then 'ClienteUber' 
    when vip.customer_id = a.customer_id then 'Vip'
    else 'ClienteUrbano' end as Flag_Cliente

    ,cl.status as Status_Conta
    ,a.flow
    ,Count (a.uuid) AS Quantidade
    ,Sum(a.amount)/100 as Vl_Tranx

FROM `eai-datalake-data-sandbox.elephant.transaction` a
join `eai-datalake-data-sandbox.core.customers`       cl on cl.uuid = a.customer_id
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias` ub on ub.customer_id = a.customer_id
left join (select distinct CPF, c.uuid as customer_id
					from (select distinct * from `eai-datalake-data-sandbox.loyalty.tblParticipantes` 
					where
						Vip is not null 
						and Inativo = false)		vip
					join (select * from`eai-datalake-data-sandbox.core.customers`  ) c on vip.CPF = c.document) vip on vip.customer_id = a.customer_id

WHERE 
  date(a.created_transaction_at) >= current_date - 60
  AND a.status IN ('APPROVED')
  AND a.type = 'CASH_IN' 
  AND a.flow IN ('BILLET', 'PIX','TED','P2P') 
  GROUP BY 1,2,3,4,5,6,7
  --HAVING Quantidade >= 4
  ORDER BY 2 desc 
)
select * from Base_Dados 
where
Status_Conta = 'ACTIVE'
--and Vl_Tranx >= 1500
