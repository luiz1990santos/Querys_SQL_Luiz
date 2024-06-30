--======================================================================================
--> TABELA DE CLIENTES UBER COM TRANSAÇÃO NOS ULTIMOS 180 DIAS
--======================================================================================
--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias` AS 
SELECT 
distinct
date(idcl.created_at) as Dt_Abertura
,idcl.uuid as customer_id
,idcl.document as cpf
,idcl.full_name as Nome
,idcl.status as Status_Conta

from `eai-datalake-data-sandbox.core.customers`  idcl
join `eai-datalake-data-sandbox.payment.payment`  trn on trn.customer_id = idcl.uuid
join `eai-datalake-data-sandbox.core.orders` orders on trn.order_id = orders.uuid
join `eai-datalake-data-sandbox.core.order_benefit`  Order_Benefit ON orders.`id` = Order_Benefit.`order_id`
where  date(orders.created_at) >= current_date - 180
and (Order_Benefit.origin_type = 'EAI:UBER'  or upper(Order_Benefit.description) LIKE '%UBER%')
and order_status = 'CONFIRMED'
--and idcl.uuid = 'CUS-022bb1da-0131-4057-a5f7-c1a3fa4ac24c'