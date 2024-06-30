--======================================================================================
--> ******* MONITORAMENTO CHARGEBACK - VERSÃO 2 **********/
--======================================================================================
-- select _PARAMETRO_REGRA, count(*) from analytics_prevencao_fraude.tb_chargeback_enriquecido_v2 group by 1
-- select max(Dt_Tranx), min(Dt_Tranx) from analytics_prevencao_fraude.tb_chargeback_enriquecido_v2
-- select max(Effective_Date), min(Effective_Date) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake`
-- select min(Effective_Date), max(Effective_Date) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico`

/*
select 
max(Effective_Date)
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake` cbk
where 
  cbk.Reason = 'Fraud'
  and cbk.Status = 'Open'
  and cbk.Kind = 'Chargeback'
  and Dispute_ID = 'v7vpjm77wrcsjkfs'
*/

---importar dado d-1 para inserir na tabela historico
-- Base_Consolidada_PayPal_DataLake

-- select max(Effective_Date), min(Effective_Date) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico`

--CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` as




CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_chargeback_enriquecido_v2` AS 

with

tb_temp_orders_cbk as (

  select
  distinct
         cbk.Dispute_ID
        --,cbk.Original_Dispute_ID
        ,cbk.Received_Date
        ,cbk.Effective_Date
        --,cbk.Last_Updated
        ,cbk.Transaction_Date
        --,cbk.Amount_Disputed
        ,cbk.Amount_Won
        ,cbk.Transaction_Amount
        ,cbk.Currency_ISO_Code
        ,cbk.Kind
        ,cbk.Reason
        ,cbk.Status
        --,cbk.Case_Number
        ,cbk.Transaction_ID
        ,cbk.Merchant_Account
        ,ord.`latitude` AS latitude
        ,ord.`longitude` AS longitude
        ,cbk.Order_ID
        ,cbk.Credit_Card_Number
        ,cbk.Card_Type
        ,ft.Card_Brand
        ,ft.First_6_of_CC	as Bin
        ,ft.Issuing_Bank as Banco_Emissor
        ,ord.customer_id as IDCliente
        ,cl.trusted
        ,cl.created_at as Dt_Conta
        ,cl.status as status_Conta
        ,cl.uuid as Customer_id 
        ,cl.document as CPF
        ,cbk.Customer_Name
        ,cbk.Customer_Email
        --,cbk.Refunded
        --,cbk.Fraud_Detected
        --,cbk.Reply_Before_Date
        --,cbk.Disputed_Date
        ,cbk.Payment_Method_Token
        ,ord.store_id as Posto
        --,cbk.Chargeback_Protection
        ,case 
        when cbk.Transaction_Amount >0 and cbk.Amount_Won = 0  then 'PayPal_Perdeu'
        else 'PayPal_Ganho' end as Flag_StatusCBK

  FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico`    cbk
  JOIN (SELECT DISTINCT * FROM  `eai-datalake-data-sandbox.paypal.transaction_level_fee_report`)  ft      on cbk.Transaction_ID = ft.Transaction_ID
  JOIN `eai-datalake-data-sandbox.core.orders`                                                    ord     on cbk.Order_ID = ord.uuid
  JOIN `eai-datalake-data-sandbox.core.customers`                                                 cl      on ord.customer_id = cl.id
  where 
  date(cbk.Effective_Date) >= current_date - 120
  and cbk.Reason = 'Fraud'
  and cbk.Status = 'Open'
  and cbk.Kind = 'Chargeback'
),tb_temp_cbk_ultimo_pedido as (

    SELECT 
    distinct
        o.customer_id as IDCliente,
        MAX(CASE WHEN o.order_status = 'CONFIRMED' THEN o.created_at ELSE null END) as ultimoPedidoAprovado,
        MAX(CASE WHEN o.order_status <> 'CONFIRMED' THEN o.created_at ELSE null END) as ultimoPedidoReprovado
    FROM  tb_temp_orders_cbk   cbk
    LEFT JOIN `eai-datalake-data-sandbox.core.orders`     o       ON cbk.Order_ID = o.uuid
    GROUP BY o.customer_id
),Cliente_recorrente as (
  select
  distinct
   cbk.Customer_id
   ,count(cbk.Dispute_ID) as Qtd_disputa
  
  FROM tb_temp_orders_cbk    cbk
  group by 1
  order by 2 desc

),Posto_recorrente as (
  select
  distinct
   cbk.Posto
   ,count(cbk.Dispute_ID) as Qtd_disputa
  
  FROM tb_temp_orders_cbk    cbk
  group by 1
  order by 2 desc

),base_chargeback as (

select
distinct

cbk.Dispute_ID
,cbk.Currency_ISO_Code
,cbk.Kind
,cbk.Reason
,cbk.Status
,cbk.Transaction_ID
,case
when cbk.Merchant_Account = 'ipirangaBRL' then 'Abastecimento'
when cbk.Merchant_Account = 'recargabrl' then 'Recarga'
when cbk.Merchant_Account = 'ubereaibrl' then 'V_Uber' else cbk.Merchant_Account end as Flag_Canal
,cbk.Order_ID
,cbk.Credit_Card_Number
,cbk.Card_Brand
,cbk.Bin
,cbk.Banco_Emissor
,cbk.IDCliente
,cbk.Customer_id 
,cl_rec.Qtd_disputa as Cliente_Recorrencia
,cbk.CPF
,cbk.Customer_Name
,cbk.Customer_Email
,cbk.Payment_Method_Token
,cbk.Posto
,cbk.latitude
,cbk.longitude
,post.Qtd_disputa as Posto_Recorrencia
,cbk.Effective_Date as Dt_Contestacao
,Pay_T.transaction_value as Vl_Tranx
,Flag_StatusCBK
,date(Pay_T.created_at) as Dt_Tranx
,case
when ub.customer_id = pay.customer_id then 'ClienteUber' 
else 'ClienteUrbano' end as Flag_Cliente

,FORMAT_DATETIME("%Y-%m-%dT%T-03:00",cbk.Dt_Conta) AS data_abertura_conta
,IF(cbk.trusted=1,True,False) as trusted
,cbk.status_Conta
,DATE_DIFF(cbk.Received_Date,date(Pay_T.created_at), DAY) as diasTransacaoCBK
,DATE_DIFF(cbk.Received_Date,date(Pay_T.created_at),  MONTH) as mesesTransacaoCBK
,DATE_DIFF( date(Pay_T.created_at),date(cbk.Dt_Conta), DAY) as diasContaCBK
,IF(date(cbk.Dt_Conta)=date(Pay_T.created_at),True,False) as CBKAbertura
,FORMAT_DATETIME("%Y-%m-%dT%T-03:00",up.ultimoPedidoAprovado) as ultimoPedidoAprovado
,FORMAT_DATETIME("%Y-%m-%dT%T-03:00",up.ultimoPedidoReprovado) as ultimoPedidoReprovado
,IF(DATE_DIFF(date(Pay_T.created_at),date(cbk.Dt_Conta), DAY)<=5,True,False) as ContaComAte5Dias

FROM tb_temp_orders_cbk                                                                         cbk
JOIN `eai-datalake-data-sandbox.payment.payment`                                                pay     ON pay.order_id = cbk.Order_ID
JOIN `eai-datalake-data-sandbox.payment.payment_transaction`                                    Pay_T   ON pay.id = Pay_T.payment_id AND Pay_T.payment_method = 'CREDIT_CARD'
LEFT JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias` ub      ON ub.customer_id = pay.customer_id
LEFT JOIN Cliente_recorrente                                                                    cl_rec  ON cbk.Customer_id = cl_rec.Customer_id
LEFT JOIN Posto_recorrente                                                                      post    ON cbk.Posto =post.Posto
LEFT JOIN tb_temp_cbk_ultimo_pedido                                                             up      ON cbk.IDCliente = up.IDCliente
),base_tranx as (
      SELECT        
       date (pt.created_at)as Dt_tranx
      ,FORMAT_DATETIME("%Y%m",pt.created_at) as Safra_Tranx
      ,pt.payment_method as Pagamento
      ,b.customer_id
      ,gateway_id
      ,cbk.Transaction_ID
      ,case
      when gateway_id = cbk.Transaction_ID then 1
      else 0 end as Flag_Tranx_Cont
      ,RANK() OVER (PARTITION BY b.customer_id ORDER BY gateway_id) AS Rank_Tranx

FROM `eai-datalake-data-sandbox.payment.payment_transaction`                                          pt
join `eai-datalake-data-sandbox.payment.payment`                                                      b   on b.id = pt.payment_id
join tb_temp_orders_cbk                                                                               cbk on b.customer_id = cbk.Customer_id 
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias`       ub  on ub.customer_id = b.customer_id
left join `eai-datalake-data-sandbox.core.orders`                                                     ord on ord.uuid = b.order_id

WHERE 
 pt.payment_method in ('CREDIT_CARD') --'CASH',
AND pt.status in ('AUTHORIZED', 'COMPLETED')
),
Contestacao_por_Postos as (
select
Customer_id,
count(distinct Posto) as Qtd_Posto
FROM base_chargeback 
group by 1
), base_qtd_mes as ( 
  select customer_id,count(distinct FORMAT_DATETIME("%Y%m",Transaction_Date)) as qtd_Mes from tb_temp_orders_cbk group by 1
), base_qtd_tranx as ( 
  select customer_id, count(gateway_id) as qtd_tran from base_tranx group by 1
), base_qtd_Cont as ( 
  select customer_id, count(distinct Transaction_ID) as qtd_Cbk from tb_temp_orders_cbk group by 1
)
select
distinct
a.*
,case  
when  a.status_Conta = 'ACTIVE' and b.Qtd_Posto >2 then '08. Conta com +2 contestacao em postos distintos'
when cast(a.ContaComAte5Dias as string) = 'true'  and a.status_Conta = 'ACTIVE'  and a.Cliente_Recorrencia >2  and a.Posto_Recorrencia >=10 and a.Posto_Recorrencia <19 then '01. Conta recente <5Dias com mais de 2 Contestacao e posto 10-19 Recorrencia'
when cast(a.ContaComAte5Dias as string) = 'true'  and a.status_Conta = 'ACTIVE'  and a.Cliente_Recorrencia >3 then '02. Conta recente <5Dias com mais de 3 Contestacao'
when cast(a.ContaComAte5Dias as string) = 'true'  and a.status_Conta = 'ACTIVE'  and a.Posto_Recorrencia >=20  then '03. Conta recente <5Dias com contestacao em Posto +20 Recorrente'

when cast(a.ContaComAte5Dias as string) = 'false' and a.status_Conta = 'ACTIVE'  and a.Cliente_Recorrencia >=10 then '04. Conta >5Dias com mais de 10 Contestacao'
when cast(a.ContaComAte5Dias as string) = 'false' and a.status_Conta = 'ACTIVE'  and a.Cliente_Recorrencia >=2 and a.Posto_Recorrencia >=20 then '05. Conta >5Dias com mais de 2 Contestacao em Posto +20 Recorrente'
when cast(a.ContaComAte5Dias as string) = 'false' and a.status_Conta = 'ACTIVE'  and a.Cliente_Recorrencia >1 then '06. Conta >5Dias com mais de 1 Contestacao'
else 'ND' end as _PARAMETRO_REGRA
,case 
when Rank_Tranx	= 1 	and qtd_tran = 1 	and qtd_Cbk = 1     then 'Fraude Cliente 1trans /1MesTransacionando/ 1contestacao'
when   qtd_tran >= 5 	and qtd_Cbk <= 5 	and qtd_Mes =1 			then 'Fraude Cliente +5trans /1MesTransacionando/ -4contestacao'
when   qtd_tran >= 5 	and qtd_Cbk <= 5 	and qtd_Mes >3 		  then 'Cliente +5trans /+3MesTransacionando/ -5contestacao'
when   qtd_tran >= 5 	and qtd_Cbk <= 5 	and qtd_Mes >2 		  then 'Cliente +5trans /+2MesTransacionando/ -5contestacao'
when   qtd_tran >= 5 	and qtd_Cbk <= 5 	and qtd_Mes >1 		  then 'Cliente +5trans /+1MesTransacionando/ -5contestacao'
when   qtd_tran >= 1 	and qtd_Cbk > 15 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ +15contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 15 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 15contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 14 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 14contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 13 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 13contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 12 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 12contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 11 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 11contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 10 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 10contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 9 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 9contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 8 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 8contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 7 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 7contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 6 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 6contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 5 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 5contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 4 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 4contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 3 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 3contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 2 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 2contestacao'
when   qtd_tran >= 1 	and qtd_Cbk = 1 	and qtd_Mes >=1 		then 'Cliente +1trans /+1MesTransacionando/ 1contestacao'
when   qtd_tran >= 5 	and qtd_Cbk >= 4 	and qtd_Mes =3      then 'Cliente +5trans /3MesTransacionando/ +4contestacao'
when   qtd_tran >= 5 	and qtd_Cbk >= 4 	and qtd_Mes =2      then 'Cliente +5trans /2MesTransacionando/ +4contestacao'
when   qtd_tran >= 5 	and qtd_Cbk >= 4 	and qtd_Mes =1      then 'Cliente +5trans /1MesTransacionando/ +4contestacao'
when   qtd_tran >= 5 	and qtd_Cbk >= 4 	and qtd_Mes >1      then 'Cliente +5trans /+1MesTransacionando/ +4contestacao'
when   qtd_tran >= 200 	and qtd_Cbk >= 3 	                  then 'Cliente +200trans / +3contestacao'
when   qtd_tran >= 100 	and qtd_Cbk >= 4 	 	                then 'Cliente +100trans / +4contestacao'
when qtd_tran	< qtd_tran  then 'Verificar'
else 'NA' end as Flag_Risco

FROM base_chargeback              a
left join Contestacao_por_Postos  b     on a.Customer_id = b.Customer_id
 join base_tranx                  tran  on a.customer_id = tran.customer_id and tran.gateway_id = a.Transaction_ID
 join base_qtd_tranx              qtran on qtran.customer_id = a.customer_id
 join base_qtd_Cont               qcbk  on qcbk.customer_id = a.customer_id
 join base_qtd_mes                qtMes on qtMes.customer_id = a.customer_id

;