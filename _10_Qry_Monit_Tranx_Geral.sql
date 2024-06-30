--======================================================================================
--> MONITORAMENTO TRANSAÇÕES EM GERAL - ULTIMOS 60 DIAS
--======================================================================================

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_monit_Transacao_Hr` 

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_monit_Transacao_Hr` AS 

with

base_pagamentos as (

    select
    distinct
      tranx_d.created_transaction_at
      ,payment_transaction.gateway_id
      ,FORMAT_DATE("%Y%m",tranx_d.created_transaction_at)as Safra_Tranx
      ,date(tranx_d.created_transaction_at) as Dt_Tranx
      ,FORMAT_DATE("%d",tranx_d.created_transaction_at)as Dia
      ,EXTRACT(HOUR FROM tranx_d.created_transaction_at)as Hr_Tranx
      ,case
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 0 then 'M0'
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 1 then 'M-1'
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 2 then 'M-2'
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 3 then 'M-3'
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 4 then 'M-4'
      else 'Outros' end as Flag_Filtro_Mes
      ,CASE
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(tranx_d.created_transaction_at), DAY) <=0  THEN '01_<D0'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(tranx_d.created_transaction_at), DAY) <=1  THEN '02_<D-1'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(tranx_d.created_transaction_at), DAY) <=2  THEN '03_<D-2'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(tranx_d.created_transaction_at), DAY) <=3  THEN '04_<D-3'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(tranx_d.created_transaction_at), DAY) >3   THEN '05_<OutrosDias'   
      END AS Flag_Filtro_Dia 
      ,CASE
        WHEN cl.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
        WHEN cl.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
        WHEN cl.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
        WHEN cl.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
        WHEN cl.state IN ('SC','PR','RS') THEN 'SUL'
      ELSE 'SUL' END AS RegiaoCliente
      ,CASE
        WHEN post.UF IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
        WHEN post.UF IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
        WHEN post.UF IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
        WHEN post.UF IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
        WHEN post.UF IN ('SC','PR','RS') THEN 'SUL'
      ELSE 'SUL' END AS RegiaoPosto
      ,ord.uuid as Order_ID
      ,cl.customer_id
      ,cl.Flag_Trusted
      ,cl.StatusConta
      ,tranx_d.operation
      ,tranx_d.status
      ,tranx_d.type
      ,tranx_d.amount/100 as Vl_Tranx
      ,payment_transaction.transaction_value
      ,Case 
        When tranx_d.amount/100 >=0	    and tranx_d.amount/100<=100 	Then '01 0-100'
        When tranx_d.amount/100 >= 101 	and tranx_d.amount/100 <=250 	Then '02 101-250'
        When tranx_d.amount/100 >= 251 	and tranx_d.amount/100 <300 	Then '03 251-299'
        When tranx_d.amount/100 = 300 	 	                                  Then '04 300'
        When tranx_d.amount/100 >= 301 	and tranx_d.amount/100 <=350 	Then '05 301-350'
        When tranx_d.amount/100 >= 351 	and tranx_d.amount/100 <=450 	Then '06 351-450'
        When tranx_d.amount/100 >= 451 	and tranx_d.amount/100 <=550 	Then '07 451-550'
        When tranx_d.amount/100 >= 551 	and tranx_d.amount/100 <=650 	Then '08 551-650'
        When tranx_d.amount/100 >= 651 	and tranx_d.amount/100 <=750 	Then '09 651-750'
        When tranx_d.amount/100 >= 751 	and tranx_d.amount/100 <=850 	Then '10 751-850'
        When tranx_d.amount/100 >= 851 	and tranx_d.amount/100 <=900 	Then '11 851-900'
        When tranx_d.amount/100 >= 901 	and tranx_d.amount/100 <=1000 	Then '12 901-1000'
      Else '13 Outros' End as Intervalo_Valor_Comportamento
      ,case
      when payment_transaction.transaction_value is null then (tranx_d.amount/100)
      else payment_transaction.transaction_value end as Flag_Valor
      ,ord.cashback_percentage
      ,ord.cashback_value/100 as cashback_value
      ,payment_transaction.status	as StatusTranx
      ,ord.code
      ,ord.sales_channel
      ,payment_transaction.payment_method	as Tipo_Pagto_App
      ,tranx_d.flow
      ,ord.order_status
      ,case
        when ord.uuid = payment.order_id then payment_transaction.payment_method
        when ord.uuid is null then 'OUTROS'
        when ord.uuid = payment.order_id and tranx_d.status in ('DENIED','CANCELLED') then 'TIPO_N/A'      
        else 'VERIFICAR' end as Tipo_Pgto

      ,case 
        when tranx_d.status in ('APPROVED','FINISHED') then 'Aprovado'
        when tranx_d.status in ('DENIED','CANCELLED') then 'Negado'
        when tranx_d.status in ('PROCESSING','SCHEDULED','PENDING') then 'Processando'
      end as Flag_Status_Transacao
      ,Case
        when type = 'ORDER'	 and flow ='APP' then 'Abastecimento'
        when type = 'ORDER' 	 and flow ='SERVICE' then 'Recarga'
        when type = 'ORDER' 	 and flow ='APP_AMPM' then 'Ampm'
        when type = 'ORDER' 	 and flow ='ECOMMERCE' then 'Futebol'
        when type = 'ORDER'   and flow ='PDV_QRCODE' then 'Outros' --'Dry'
        when type = 'ORDER'   and flow ='APP_JET_OIL' then 'Outros' --'JetOil'
        when type = 'ORDER'   and flow ='APP_TUDOAZUL' then 'TudoAzul'
        when type = 'ORDER'   and flow ='APP_ULTRAGAZ' then 'Outros' --'Ultragaz'
        when type = 'CASH_IN' and flow ='TIP' then 'GorjetaVIP'
        when type = 'CASH_IN' and flow ='VOUCHER' then 'Cupom'
        when type = 'CASH_OUT'and flow ='P2P' then 'P2P_Out'
        when type = 'CASH_IN' and flow ='P2P' then 'P2P_In'
        when type = 'CASH_IN' and flow ='PIX' then 'PIX_In'
        when type = 'CASH_OUT'and flow ='PIX' then 'PIX_Out'
        when type = 'CASH_OUT'and flow ='TED' then 'TED_Out'
        when type = 'CASH_IN' and flow ='BILLET' then 'Boleto_In'
        when type = 'CASH_IN' and flow ='CONCESSION' then 'Pagto_Interno_In'
      Else 'Verificar' end as Flag_Canal
      ,Case
        When type in ('ORDER','CASH_OUT') then 'Saida'
      else 'Entrada' end as Flag_Operacao

    ,CASE
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=0    THEN '01_00-Hoje'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=1    THEN '02_01-Ontem'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=10   THEN '03_00-10DIAS'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=30   THEN '04_11-30DIAS'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=60   THEN '05_31-60DIAS'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=90   THEN '06_61-90DIAS'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=180  THEN '07_91-180DIAS'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=364  THEN '08_180-1ANO'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) >=365  THEN '09_+1ANO'
    Else '10_HojeNaoAtualizada' END AS Flag_TempConta
    ,RANK() OVER (PARTITION BY cl.customer_id ORDER BY tranx_d.created_transaction_at) AS Rank_1Tranx

      ,case
        when dppaypal.string_field_0 <> substring(ord.code,1,STRPOS(ord.code,'-')) then tranx_d.type
        when dppaypal.string_field_0 = substring(ord.code,1,STRPOS(ord.code,'-')) then dppaypal.string_field_2
        Else tranx_d.type end as Flag_Merchant_Account
    ,case 
      when PrimPaypal.order_id = ord.uuid then 'PrimTran' 
    else 'OutrasTran' end as Flag_Tranx_Cliente


      ,substring(ord.code,1,STRPOS(ord.code,'-')) as order_code

    FROM `eai-datalake-data-sandbox.elephant.transaction`                                     tranx_d
    Left Join (Select cl.uuid as customer_id,cl.created_at,en.state, case when cl.trusted = 1 then 'Trusted' else 'NoTrusted' end as Flag_Trusted, cl.status as StatusConta 
              from `eai-datalake-data-sandbox.core.customers`  cl
              left join `eai-datalake-data-sandbox.core.address` en on en.id = cl.address_id
              ) cl  on cl.customer_id = tranx_d.customer_id

    left join (select * from `eai-datalake-data-sandbox.core.orders` ) ord  on ord.uuid = tranx_d.own_id
    left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.TB_DE_PARA_PAYPAL_PEDIDO`  dppaypal   on dppaypal.string_field_0 = substring(ord.code,1,STRPOS(ord.code,'-')) 
    left join `eai-datalake-data-sandbox.payment.payment`                                      payment    on tranx_d.own_id = payment.order_id
    left join (select * from `eai-datalake-data-sandbox.payment.payment_transaction`
                where status not in ('REVERSED','REVERSED_ERROR','REVERSED_DENIED'))           payment_transaction   on payment.id = payment_transaction.payment_id
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_primeira_transacao_Paypal` PrimPaypal on PrimPaypal.order_id = tranx_d.own_id
left join(SELECT 
            distinct
            a.uuid as store_id
            ,a.name as Nome_loja
            ,a.document as CNPJ_CPF
            ,a.type as tipo_loja
            ,b.city as cidade
            ,b.state as UF
            ,left(b.latitude,7) as latitude
            ,left(b.longitude,7) as longitude

            FROM `eai-datalake-data-sandbox.backoffice.store` a
            join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id)  post on post.store_id = ord.store_id
    where
    --FORMAT_DATETIME('%Y',tranx_d.created_transaction_at) = '2022'
    date(tranx_d.created_transaction_at) >= current_date - 60
    --date(tranx_d.created_transaction_at) = '2023-01-10'
    --and tranx_d.status in ('APPROVED','FINISHED','DENIED','CANCELLED')
    --and ord.order_status ='CANCELED_BY_GATEWAY'
    --and code = 'REC-102921127'
    --and dppaypal.string_field_2 = 'IPIRANGABRL_TUDOAZUL'
    --and cl.customer_id = 'CUS-c92949c0-d054-4390-8a64-5f22a697a804'
    --and ord.store_id = 'STO-f8968b34-99ed-44d7-9f24-c888d9741794'
    --limit 10
   --order by 10
--) select * from base_pagamentos where Tipo_Pgto = 'VERIFICAR'
),base_pagamentos1 as (
    select
    Safra_Tranx
    ,Dt_Tranx
    ,Dia
    ,Hr_Tranx
    ,Flag_Filtro_Dia 
    ,Flag_Filtro_Mes
    ,Flag_TempConta
    ,Flag_Canal
    ,Flag_Operacao
    ,status as Status_Tranx
    ,Intervalo_Valor_Comportamento
    ,Flag_Trusted
    ,Flag_Status_Transacao
    ,sales_channel
    ,RegiaoPosto
    ,RegiaoCliente
    ,operation
    ,type
    ,order_code
    ,order_status
    ,StatusTranx
    ,Tipo_Pagto_App
    ,Tipo_Pgto
    ,Flag_Merchant_Account
    ,Flag_Tranx_Cliente
    ,sum(Flag_Valor) as Vl_Total
    ,count(Flag_Valor) as Volume_Total
    ,count(distinct customer_id) as ClienteTranx

    from base_pagamentos
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
)
select
*
,case
 when Tipo_Pgto is null then 'TIPO_N/A' 
 when Tipo_Pgto in ('VERIFICAR','') then 'TIPO_N/A' 
else Tipo_Pgto end as Flag_Tipo_Pgto
from base_pagamentos1

