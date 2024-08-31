--Extrato Pagamento
/*
select max(Dt_Transacao),min(Dt_Transacao) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_Pagamento_Order` 
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_Pagamento_Order` 
*/
--CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_Pagamento_Order` AS 

INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_Pagamento_Order` 

with

base_transacional as (

SELECT
DISTINCT

p.order_id
,o.order_status as StatusTransOrder
,o.sales_channel
,'PAGAMENTO' as type
,order_code as code
,p.customer_id
,CPF_Cliente
,o.store_id
,o.created_at as Dt_Transacao
,case 
when o.code like '%REC%' then 'Recarga'
when o.code like '%LIV%' then 'Livelo'
when o.code like '%AZU%' then 'TudoAzul'
when o.code like '%SMI%'  then 'Smiles'
when o.code like '%ROC%'  then 'RockInRio'
when o.sales_channel = 'APP_ULTRAGAZ' then 'UltraGaz'
when o.sales_channel = 'DRYWASHBRL' then 'DryWash'
when o.code like '%FUT%' then 'Futebol'
when o.sales_channel = 'ECOMMERCE'  then 'Shopping'
when o.sales_channel in ('APP','APP_AMPM','APP_JET_OIL','PDV_QRCODE') then 'Abastecimento'
else o.sales_channel end as Flag_Merchant_Account_Tranx
,case
When te.error_message is not null and te.error_message != 'Unavailable' then 'CanceladoBancoEmissor'
When o.order_status in ('CANCELED_BY_CUSTOMER','CANCELED_BY_STORE','CANCELED_BY_BACKOFFICE','EXPIRED','PENDING','REVERSED') 
and te.error_message is null then 'CanceladoKMV'
When o.order_status in ('CANCELED_BY_GATEWAY','CANCELED_BY_PRE_AUTHORIZATION_TIMEOUT','PRE_AUTHORIZED_ERROR') 
and te.error_message is null or te.error_message in ('Unavailable') then 'CanceladoPayPal'
When o.order_status in ('CONFIRMATION_WAITING','CONFIRMED','PRE_AUTHORIZATION_WAITING','PRE_AUTHORIZED_BY_GATEWAY') 
and te.error_message is null then 'Aprovado'
else 'NA' end as Flag_StatusTransacao
,case
When o.order_status in ('CANCELED_BY_CUSTOMER','CANCELED_BY_STORE','CANCELED_BY_BACKOFFICE','EXPIRED','PENDING','REVERSED') 
     and te.error_message is null then 'CanceladoKMV'
When o.order_status in ('CANCELED_BY_GATEWAY','CANCELED_BY_PRE_AUTHORIZATION_TIMEOUT','PRE_AUTHORIZED_ERROR') 
     and te.error_message is null or te.error_message in ('Unavailable') then 'CanceladoPayPal'
When o.order_status in ('CONFIRMATION_WAITING','CONFIRMED','PRE_AUTHORIZATION_WAITING','PRE_AUTHORIZED_BY_GATEWAY') 
     and te.error_message is null then 'Aprovado'
else te.error_message end as Flag_StatusBancoEmissor

,COUNT(DISTINCT pt.payment_id) AS Volume
,SUM(pt.transaction_value) AS Valor

--- Quatidade de transação

,MAX(IF(pt.payment_method = 'CREDIT_CARD','CC','')) AS Vol_CartaoCredito
,MAX(IF(pt.payment_method = 'DEBIT_CARD','CD','')) AS Vol_CartaoDebito
,MAX(IF(pt.payment_method = 'CASH','D','')) AS Vol_Dinheiro
,MAX(IF(pt.payment_method = 'BALANCE','S','')) AS Vol_Saldo
,MAX(IF(pt.payment_method = 'COUPON','C','')) AS Vol_Cupom
,MAX(IF(pt.payment_method = 'DIGITAL_WALLET','CDP','')) AS Vol_CarteiraDigital_PayPal
,MAX(IF(pt.payment_method = 'GOOGLE_PAY','GP','')) AS Vol_Google_Pay
,MAX(IF(pt.payment_method = 'APPLE_PAY','AP','')) AS Vol_Apple_Pay

--- Valor transação

,MAX(IF(pt.payment_method = 'CREDIT_CARD',pt.transaction_value,0)) AS Val_CartaoCredito
,MAX(IF(pt.payment_method = 'DEBIT_CARD',pt.transaction_value,0)) AS Val_CartaoDebito
,MAX(IF(pt.payment_method = 'CASH',pt.transaction_value,0)) AS Val_Dinheiro
,MAX(IF(pt.payment_method = 'BALANCE',pt.transaction_value,0)) AS Val_Saldo
,MAX(IF(pt.payment_method = 'COUPON',pt.transaction_value,0)) AS Val_Cupom
,MAX(IF(pt.payment_method = 'DIGITAL_WALLET',pt.transaction_value,0)) AS Val_CarteiraDigital_PayPal
,MAX(IF(pt.payment_method = 'GOOGLE_PAY',pt.transaction_value,0)) AS Val_Google_Pay
,MAX(IF(pt.payment_method = 'APPLE_PAY',pt.transaction_value,0)) AS Val_Apple_Pay


FROM `eai-datalake-data-sandbox.payment.payment` p
join `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
join `eai-datalake-data-sandbox.core.orders` o on p.order_id = o.uuid
left join `eai-datalake-data-sandbox.payment.payment_transaction_error` te on p.order_id = te.order_id
left join (
        select
        distinct
            CustomerID
            ,CPF_Cliente
            ,Flag_TempodeConta
            ,Flag_Trusted
            ,Flag_Ativo
            ,Flag_Perfil
            ,count(*) as Qtd_Cliente
        from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`
        group by 1,2,3,4,5,6
) cl on cl.CustomerID = p.customer_id

WHERE 
o.created_at > (select max(Dt_Transacao) as Dt_Transacao from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_Pagamento_Order` ) and DATE(o.created_at) <= current_date - 1
--o.created_at  >= (select max(Dt_Transacao)as Dt_Transacao from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_Pagamento_Order` )
--date(o.created_at)  <= '2024-08-05' --current_date - 10
--FORMAT_DATE("%Y",o.created_at) <= '2020'
--DATE(p.created_at) >= '2024-01-01' and DATE(p.created_at) <= current_date - 1
--and pt.payment_method IN ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET",'GOOGLE_PAY')
--and p.order_id = 'ORD-a6943ed6-286c-4a36-912b-887185d74c75'
--and 
--CPF_Cliente = '31001127846'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
order by 7 desc
), base_transacional_1 as (
SELECT
order_id
,StatusTransOrder
,sales_channel
,type
,code
,Flag_Merchant_Account_Tranx
,customer_id
,CPF_Cliente
,store_id
,Dt_Transacao
,Flag_StatusTransacao
,Flag_StatusBancoEmissor
,Volume
,Valor
,Vol_CartaoCredito||Vol_CartaoDebito||Vol_Dinheiro||Vol_Saldo||Vol_Cupom||Vol_CarteiraDigital_PayPal||Vol_Google_Pay||Vol_Apple_Pay as Cod_Transacao
,Val_CartaoCredito
,Val_CartaoDebito
,Val_Dinheiro
,Val_Saldo
,Val_Cupom
,Val_CarteiraDigital_PayPal
,Val_Google_Pay
,Val_Apple_Pay

from base_transacional
)
SELECT
distinct
tran.*
,pgto.FormaPagamento

from base_transacional_1 tran
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Meio_Pagamento` pgto on pgto.Cod_Transacao = tran.Cod_Transacao
