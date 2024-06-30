
----------------------------------------- Base Primeira Transação PayPal ---------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Prim_trans_PayPal`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Prim_trans_PayPal` AS 

-- primeira_transacao_cliente 
select  
distinct 
c.uuid as customer -- id do cliente
,min (p.created_at) as menor_data -- data da primeira transação na vida dentro do app
,max (p.created_at) as maior_data -- data da ultima transação na vida dentro do app
from `eai-datalake-data-sandbox.payment.payment` p
left join `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
left join `eai-datalake-data-sandbox.core.customers` c on c.uuid = p.customer_id
WHERE pt.gateway = 'PAYPALL' -- Filtro que segmenta somente as transacoes e passaram pelo gateway da paypal cujo os meios de pagamento são: cartão de crédito, débito e digital wallet
group by 1

;

----------------------------------------- Base Transação PayPal ---------------------------------------------------


-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_trans_PayPal_P1` order by 1

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_trans_PayPal_P1` AS 


select 
distinct
    p.customer_id AS customer_id
    ,date(p.created_at) AS created_at
    ,FORMAT_DATETIME("%Y%m",p.created_at) as Safra_Transacao
    ,pt.payment_method as MeioPagto
    ,CASE WHEN date_trunc(date(ptc.menor_data),month) = date_trunc(date(p.created_at),month) then 'Prim_Trans' else 'Cliente Antigo' end as Flag_Prim_Trans
    ,o.sales_channel as Canal
    ,pt.status as status
    ,p.order_id 
----------------------------------------------------complemento qry ---------------------------------------------------

---------------------------------------------------- Perfil Cliente -------------------------------------------------

    ,cl.Flag_Trusted
    ,cl.Flag_Perfil
    ,cl.Flag_TempodeConta

---------------------------------------------------- Movimentações -------------------------------------------------

    ,CASE
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=0 THEN '01_MesmoDia'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=3 THEN '02_Até 3 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=5 THEN '03_Até 5 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=10 THEN '04_Até 10 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=20 THEN '05_Até 20 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=30 THEN '06_Até 30 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=60 THEN '07_Até 60 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=90 THEN '08_Até 90 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=120 THEN '09_Até 120 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) >120 THEN '10_Mais 120 dias'
    Else 'OutrasTransacao'END AS Flag_TempoTrans_Prim
    ,CASE
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=0 THEN '01_Hoje'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=3 THEN '02_Até 3 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=5 THEN '03_Até 5 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=10 THEN '04_Até 10 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=20 THEN '05_Até 20 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=30 THEN '06_Até 30 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=60 THEN '07_Até 60 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=90 THEN '08_Até 90 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=120 THEN '09_Até 120 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) >120 THEN '10_Mais 120 dias'
    Else 'OutrasTransacao'END AS Flag_TempoTrans_Ultima 
    ,Case
        When (pt.transaction_value)  <0   	Then '01 000 a 000 ' 
        When (pt.transaction_value) >=0   and (pt.transaction_value) <=5 	Then '02 000 a 005 ' 
        When (pt.transaction_value) >5    and (pt.transaction_value) <=10 	Then '03 006 a 010 '
        When (pt.transaction_value) >10   and (pt.transaction_value) <=20 	Then '04 011 a 020 '
        When (pt.transaction_value) > 20  and (pt.transaction_value) <=40 	Then '05 021 a 040 '
        When (pt.transaction_value) > 40  and (pt.transaction_value) <=60 	Then '06 041 a 060 '
        When (pt.transaction_value) > 60  and (pt.transaction_value) <=80 	Then '07 061 a 080 '
        When (pt.transaction_value) > 80  and (pt.transaction_value) <=100 Then '08 081 a 100'
        When (pt.transaction_value) > 100 and (pt.transaction_value) <=120 Then '09 101 a 120'
        When (pt.transaction_value) > 120 and (pt.transaction_value) <=140 Then '10 121 a 140'
        When (pt.transaction_value) > 140 and (pt.transaction_value) <=160 Then '11 141 a 160'
        When (pt.transaction_value) > 160 and (pt.transaction_value) <=180 Then '12 161 a 180'
        When (pt.transaction_value) > 180 and (pt.transaction_value) <=200 Then '13 181 a 200'
        When (pt.transaction_value) > 200 and (pt.transaction_value) <=220 Then '14 201 a 220'
        When (pt.transaction_value) > 220 and (pt.transaction_value) <=240 Then '15 221 a 240'
        When (pt.transaction_value) > 240 and (pt.transaction_value) <=260 Then '16 241 a 260'
        When (pt.transaction_value) > 260 and (pt.transaction_value) <=280 Then '17 261 a 280'
        When (pt.transaction_value) > 280 and (pt.transaction_value) <300 	Then '18 281 a 299'
        When (pt.transaction_value) = 300 	Then '19 300'
        When (pt.transaction_value) > 300 	and (pt.transaction_value) <600  Then '20 301 a 599'
        When (pt.transaction_value) = 600	Then '21 600'
        When (pt.transaction_value) > 600 	 and (pt.transaction_value) <=800	 Then '21 601 a 800'
        When (pt.transaction_value) > 800 	 and (pt.transaction_value) <=1000 Then '22 801 a 1000'
        When (pt.transaction_value) > 1000  and (pt.transaction_value) <=3000 Then '23 1001 a 3000'
        When (pt.transaction_value) > 3000  and (pt.transaction_value) <=5000 Then '24 3001 a 5000'
        When (pt.transaction_value) > 5000  and (pt.transaction_value) <=7000 Then '25 5001 a 7000'
        When (pt.transaction_value) > 7000  and (pt.transaction_value) <=9000 Then '26 7001 a 9000'
        When (pt.transaction_value) > 9000  and (pt.transaction_value) <=11000 Then '27 9001 a 11000'
        When (pt.transaction_value) > 11000 and (pt.transaction_value) <=13000 Then '28 11001 a 13000'
        When (pt.transaction_value) > 13000 and (pt.transaction_value) <=15000 Then '30 13001 a 15000'
        When (pt.transaction_value) > 15000 and (pt.transaction_value) <=17000 Then '31 15001 a 17000'
        When (pt.transaction_value) > 17000 and (pt.transaction_value) <=19000 Then '32 17001 a 19000'
        When (pt.transaction_value) > 19000 and (pt.transaction_value) <=20000 Then '33 19001 a 20000'
        When (pt.transaction_value) > 20000 Then '34 20000>' 
    End as Faixa_Valores

    ,te.error_code as Code
    ,te.error_message 
    ,te.error_message as Desc_Motivo
/*
    ,case
    when pt.status in ( "AUTHORIZED","SETTLEMENT") then 'Approved'
    when pt.status in ('CANCELLED_BY_GATEWAY',"PRE_AUTHORIZED_ERROR","REVERSED_ERROR") then r.error_message
    else 'NA' end as error_message

    ,case
    when pt.status in ( "AUTHORIZED","SETTLEMENT") then '1000'
    when pt.status in ('CANCELLED_BY_GATEWAY',"PRE_AUTHORIZED_ERROR","REVERSED_ERROR") then r.Code
    else 'NA' end as Code

    ,case
    when pt.status in ( "AUTHORIZED","SETTLEMENT") then 'Aprovado'
    when r.Motivo is null then 'Negado PayPal'
    when pt.status in ('CANCELLED_BY_GATEWAY',"PRE_AUTHORIZED_ERROR","REVERSED_ERROR") then r.Motivo
    else 'NA' end as Desc_Motivo
*/
    --,pt.transaction_value as valor
    --,if((pt.status = "AUTHORIZED" OR pt.status = "SETTLEMENT"), 1, 0) AS Tran_Apr
    --,if((pt.status = "CANCELLED_BY_GATEWAY" OR pt.status = "PRE_AUTHORIZED_ERROR" OR pt.status = "REVERSED_ERROR"), 1, 0) AS Tran_Neg

    ,sum(pt.transaction_value) as valor
    ,sum(if((pt.status = "AUTHORIZED" OR pt.status = "SETTLEMENT"), 1, 0)) AS Tran_Apr
    ,sum(if((pt.status = "CANCELLED_BY_GATEWAY" OR pt.status = "PRE_AUTHORIZED_ERROR" OR pt.status = "REVERSED_ERROR"), 1, 0)) AS Tran_Neg

FROM `eai-datalake-data-sandbox.payment.payment` p
join `eai-datalake-data-sandbox.payment.payment_instrument` pi on p.id = pi.id
join `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
join `eai-datalake-data-sandbox.core.orders` o on p.order_id = o.uuid
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Prim_trans_PayPal`  ptc on ptc.customer = p.customer_id
left join (
        select
        distinct
            CustomerID
            ,CPF_Cliente
            ,Dt_Abertura
            ,Faixa_Idade
            ,BairroCliente
            ,Cidade_Cliente
            ,UF_Cliente
            ,RegiaoCliente
            ,Flag_TempodeConta
            ,Flag_TempoBloqueado
            ,Flag_Trusted
            ,Flag_Email_NaoVal
            ,Flag_Celular_NaoVal
            ,Flag_Biometria
            ,Flag_Risco_Limit_Vol
            ,Flag_Risco_Limit_Val
            ,Flag_Risco_CBK
            ,Flag_Tetativas
            ,Flag_Bancos
            ,Flag_Card
            ,Flag_Ativo
            ,Flag_Perfil
            ,ScoreZaig
            ,count(*) as Qtd_Cliente
        from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
) cl on cl.CustomerID = p.customer_id
left join (
            select
            distinct
                store_id
                ,Nome_loja
                ,CNPJ_CPF
                ,BairroPosto
                ,cidade
                ,UF
                ,RegiaoPosto
                ,latitude||longitude as Lat_Log_Posto
                ,Post_Limt_Vol
                ,Post_Limt_Val
                ,Post_Cbk_Vol
                ,Post_Cbk_Val
                ,Flag_Risco_Local_Tran
                ,count(*) as Qtd_VIPs
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_Posto_Perfil` 
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13
) perf_post on perf_post.store_id = o.store_id
left join `eai-datalake-data-sandbox.payment.payment_transaction_error` te on p.order_id = te.order_id
left join `eai-datalake-data-sandbox.analytics.tb_motivos_recusa_paypal` r on te.error_code = r.Code
where pt.status IN ("CANCELLED_BY_GATEWAY","PRE_AUTHORIZED_ERROR","REVERSED_ERROR","AUTHORIZED","SETTLEMENT","COMPLETED")
and pt.payment_method IN ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET",'GOOGLE_PAY')
and date(pt.created_at) >= current_date - 180
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17


;

----------------- Cubo Transacional -------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_trans_PayPal_P2`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_trans_PayPal_P2` AS 

select 
    date(customer_payment.created_at) as data_transacao
    ,Flag_Trusted
    ,Flag_Perfil
    ,Flag_Prim_Trans
    ,sales_channel
    ,status
    ,MeioPagto
    ,Flag_TempodeConta
    ,Flag_TempoTrans_Prim
    ,Flag_TempoTrans_Ultima
    ,Code
    ,Desc_Motivo
    ,count(distinct customer_payment.customer_id) as clientes
    ,round(SUM(valor),2) as valor
    ,SUM(IF(customer_payment.status_confirmed > 0, customer_payment.status_confirmed, 0)) AS Trans_Apr
    ,SUM(IF(customer_payment.status_denied_paypall > 0, 1, 0)) AS Trans_Neg
from 
    (
        select 
        p.customer_id AS customer_id
        ,date(p.created_at) AS created_at
        ,criv.Flag_Trusted
        ,criv.Flag_Perfil
        ,pt.payment_method as MeioPagto
        ,criv.Flag_Prim_Trans
        ,o.sales_channel as sales_channel
        ,pt.status as status
        ,criv.Flag_TempodeConta
        ,criv.Flag_TempoTrans_Prim
        ,criv.Flag_TempoTrans_Ultima
        ,criv.Code
        ,criv.Desc_Motivo
        ,sum(pt.transaction_value) as valor
        ,sum(if((pt.status = "AUTHORIZED" OR pt.status = "SETTLEMENT"), 1, 0)) AS status_confirmed
        ,sum(if((pt.status = "CANCELLED_BY_GATEWAY" OR pt.status = "PRE_AUTHORIZED_ERROR" OR pt.status = "REVERSED_ERROR"), 1, 0)) AS status_denied_paypall
        FROM `eai-datalake-data-sandbox.payment.payment` p
        join `eai-datalake-data-sandbox.payment.payment_instrument` pi on p.id = pi.id
        join `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
        join `eai-datalake-data-sandbox.core.orders` o on p.order_id = o.uuid
        join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_trans_PayPal_P1`     criv on criv.order_id = p.order_id
        join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` c on c.CustomerID = p.customer_id

        where pt.status IN ("CANCELLED_BY_GATEWAY","PRE_AUTHORIZED_ERROR","REVERSED_ERROR","AUTHORIZED","SETTLEMENT","COMPLETED")
        and pt.payment_method IN ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET",'GOOGLE_PAY')
        --and pt.payment_method IN ("DIGITAL_WALLET")
        and date(pt.created_at) >= current_date - 180
        --and date(pt.created_at) = '2024-03-28'
        --and p.customer_id  = 'CUS-0893ba34-10ef-45db-a9d9-fe56b6caba9a'
        --and Desc_Motivo = 'Transação Negada sem motivo'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13--,14,15,16--,17--,18,19
        ORDER BY 1,2 ASC 
) AS customer_payment
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12--,13,14,15--,16--,17,18

;


----------------------------------------- Base Transação PayPal ---------------------------------------------------


-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_trans_PayPal_P3` order by 1

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_trans_PayPal_P3` AS 

with

Base_final as (

select 
distinct
    p.customer_id AS customer_id
    ,date(p.created_at) AS created_at
    ,FORMAT_DATETIME("%d",p.created_at) as Dia
    ,FORMAT_DATETIME("%Y%m",p.created_at) as Safra_Transacao
    ,pt.payment_method as MeioPagto
    ,CASE WHEN date_trunc(date(ptc.menor_data),month) = date_trunc(date(p.created_at),month) then 'Prim_Trans' else 'Cliente Antigo' end as Flag_Prim_Trans
    ,o.sales_channel as Canal
    ,pt.status as status
    ,p.order_id 
----------------------------------------------------complemento qry ---------------------------------------------------

---------------------------------------------------- Perfil Posto ---------------------------------------------------
    ,perf_post.store_id
    ,perf_post.Nome_loja
    ,perf_post.CNPJ_CPF
    ,perf_post.BairroPosto
    ,perf_post.cidade
    ,perf_post.UF
    ,perf_post.RegiaoPosto
    ,perf_post.Lat_Log_Posto
    ,perf_post.Post_Limt_Vol
    ,perf_post.Post_Limt_Val
    ,perf_post.Post_Cbk_Vol
    ,perf_post.Post_Cbk_Val
    ,perf_post.Flag_Risco_Local_Tran
---------------------------------------------------- Perfil Cliente -------------------------------------------------

    ,cl.CPF_Cliente
    ,cl.Faixa_Idade
    ,cl.BairroCliente
    ,cl.Cidade_Cliente
    ,cl.UF_Cliente
    ,cl.RegiaoCliente
    ,cl.Flag_TempodeConta
    ,cl.Flag_TempoBloqueado
    ,cl.Flag_Trusted
    ,cl.Flag_Risco_Limit_Vol
    ,cl.Flag_Risco_Limit_Val
    ,cl.Flag_Risco_CBK
    ,cl.Flag_Tetativas
    ,cl.Flag_Bancos
    ,cl.Flag_Card
    ,cl.Flag_Ativo
    ,cl.Flag_Perfil

---------------------------------------------------- Banco Emissor -------------------------------------------------

    ,Banco_Emissor.Bandeira
    ,Banco_Emissor.Banco_Emissor

---------------------------------------------------- Movimentações -------------------------------------------------
    ,CASE
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=0 THEN '01_MesmoDia'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=3 THEN '02_Até 3 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=5 THEN '03_Até 5 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=10 THEN '04_Até 10 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=20 THEN '05_Até 20 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=30 THEN '06_Até 30 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=60 THEN '07_Até 60 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=90 THEN '08_Até 90 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) <=120 THEN '09_Até 120 dias'
        WHEN DATETIME_DIFF(DATETIME(ptc.menor_data), DATETIME(cl.Dt_Abertura), DAY) >120 THEN '10_Mais 120 dias'
    Else 'OutrasTransacao'END AS Flag_TempoTrans_Prim
    ,CASE
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=0 THEN '01_Hoje'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=3 THEN '02_Até 3 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=5 THEN '03_Até 5 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=10 THEN '04_Até 10 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=20 THEN '05_Até 20 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=30 THEN '06_Até 30 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=60 THEN '07_Até 60 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=90 THEN '08_Até 90 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) <=120 THEN '09_Até 120 dias'
        WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ptc.maior_data), DAY) >120 THEN '10_Mais 120 dias'
    Else 'OutrasTransacao'END AS Flag_TempoTrans_Ultima 
    ,Case
        When (pt.transaction_value)  <0   	Then '01 000 a 000 ' 
        When (pt.transaction_value) >=0   and (pt.transaction_value) <=5 	Then '02 000 a 005 ' 
        When (pt.transaction_value) >5    and (pt.transaction_value) <=10 	Then '03 006 a 010 '
        When (pt.transaction_value) >10   and (pt.transaction_value) <=20 	Then '04 011 a 020 '
        When (pt.transaction_value) > 20  and (pt.transaction_value) <=40 	Then '05 021 a 040 '
        When (pt.transaction_value) > 40  and (pt.transaction_value) <=60 	Then '06 041 a 060 '
        When (pt.transaction_value) > 60  and (pt.transaction_value) <=80 	Then '07 061 a 080 '
        When (pt.transaction_value) > 80  and (pt.transaction_value) <=100 Then '08 081 a 100'
        When (pt.transaction_value) > 100 and (pt.transaction_value) <=120 Then '09 101 a 120'
        When (pt.transaction_value) > 120 and (pt.transaction_value) <=140 Then '10 121 a 140'
        When (pt.transaction_value) > 140 and (pt.transaction_value) <=160 Then '11 141 a 160'
        When (pt.transaction_value) > 160 and (pt.transaction_value) <=180 Then '12 161 a 180'
        When (pt.transaction_value) > 180 and (pt.transaction_value) <=200 Then '13 181 a 200'
        When (pt.transaction_value) > 200 and (pt.transaction_value) <=220 Then '14 201 a 220'
        When (pt.transaction_value) > 220 and (pt.transaction_value) <=240 Then '15 221 a 240'
        When (pt.transaction_value) > 240 and (pt.transaction_value) <=260 Then '16 241 a 260'
        When (pt.transaction_value) > 260 and (pt.transaction_value) <=280 Then '17 261 a 280'
        When (pt.transaction_value) > 280 and (pt.transaction_value) <300 	Then '18 281 a 299'
        When (pt.transaction_value) = 300 	Then '19 300'
        When (pt.transaction_value) > 300 	and (pt.transaction_value) <600  Then '20 301 a 599'
        When (pt.transaction_value) = 600	Then '21 600'
        When (pt.transaction_value) > 600 	 and (pt.transaction_value) <=800	 Then '21 601 a 800'
        When (pt.transaction_value) > 800 	 and (pt.transaction_value) <=1000 Then '22 801 a 1000'
        When (pt.transaction_value) > 1000  and (pt.transaction_value) <=3000 Then '23 1001 a 3000'
        When (pt.transaction_value) > 3000  and (pt.transaction_value) <=5000 Then '24 3001 a 5000'
        When (pt.transaction_value) > 5000  and (pt.transaction_value) <=7000 Then '25 5001 a 7000'
        When (pt.transaction_value) > 7000  and (pt.transaction_value) <=9000 Then '26 7001 a 9000'
        When (pt.transaction_value) > 9000  and (pt.transaction_value) <=11000 Then '27 9001 a 11000'
        When (pt.transaction_value) > 11000 and (pt.transaction_value) <=13000 Then '28 11001 a 13000'
        When (pt.transaction_value) > 13000 and (pt.transaction_value) <=15000 Then '30 13001 a 15000'
        When (pt.transaction_value) > 15000 and (pt.transaction_value) <=17000 Then '31 15001 a 17000'
        When (pt.transaction_value) > 17000 and (pt.transaction_value) <=19000 Then '32 17001 a 19000'
        When (pt.transaction_value) > 19000 and (pt.transaction_value) <=20000 Then '33 19001 a 20000'
        When (pt.transaction_value) > 20000 Then '34 20000>' 
    End as Faixa_Valores
  	
    ,te.error_code as Code
    ,te.error_message 
    ,te.error_message as Desc_Motivo
   
   /* ,case
    when pt.status in ( "AUTHORIZED","SETTLEMENT") then 'Approved'
    when pt.status in ('CANCELLED_BY_GATEWAY',"PRE_AUTHORIZED_ERROR","REVERSED_ERROR") then r.error_message
    else 'NA' end as error_message

    ,case
    when pt.status in ( "AUTHORIZED","SETTLEMENT") then '1000'
    when pt.status in ('CANCELLED_BY_GATEWAY',"PRE_AUTHORIZED_ERROR","REVERSED_ERROR") then r.Code
    else 'NA' end as Code

    ,case
    when pt.status in ( "AUTHORIZED","SETTLEMENT") then 'Aprovado'
    when r.Motivo is null then 'Negado PayPal'
    when pt.status in ('CANCELLED_BY_GATEWAY',"PRE_AUTHORIZED_ERROR","REVERSED_ERROR") then r.Motivo
    else 'NA' end as Desc_Motivo
*/
    ,if((p.order_id is not null), 1, 0) as Qtd_Trans
    ,pt.transaction_value as valor
    ,if((pt.status = "AUTHORIZED" OR pt.status = "SETTLEMENT"), 1, 0) AS Tran_Apr
    ,if((pt.status = "CANCELLED_BY_GATEWAY" OR pt.status = "PRE_AUTHORIZED_ERROR" OR pt.status = "REVERSED_ERROR"), 1, 0) AS Tran_Neg

    --,sum(pt.transaction_value) as valor
    --,sum(if((pt.status = "AUTHORIZED" OR pt.status = "SETTLEMENT"), 1, 0)) AS Tran_Apr
    --,sum(if((pt.status = "CANCELLED_BY_GATEWAY" OR pt.status = "PRE_AUTHORIZED_ERROR" OR pt.status = "REVERSED_ERROR"), 1, 0)) AS Tran_Neg

FROM `eai-datalake-data-sandbox.payment.payment` p
join `eai-datalake-data-sandbox.payment.payment_instrument` pi on p.id = pi.id
join `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
join `eai-datalake-data-sandbox.core.orders` o on p.order_id = o.uuid
left join `eai-datalake-data-sandbox.payment.customer_card` customer_card on pi.uuid = customer_card.uuid
left join (
        with 

        base_Bin as (

        SELECT
        distinct
        First_6_of_CC as Bin
        ,Card_Brand as Bandeira
        ,case
        when Issuing_Bank is null then 'NONE' 
        when Issuing_Bank = '' then 'NONE' 
        else Issuing_Bank end as Banco_Emissor
        ,RANK() OVER (PARTITION BY First_6_of_CC  ORDER BY Settlement_Date desc) AS Rank_Ult_Card

        FROM `eai-datalake-data-sandbox.paypal.transaction_level_fee_report` 
        order by 4

        ) select * from base_Bin where Rank_Ult_Card = 1
) Banco_Emissor on cast(Banco_Emissor.bin as numeric) = cast(customer_card.bin as numeric)
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Prim_trans_PayPal`  ptc on ptc.customer = p.customer_id
left join (
        select
        distinct
            CustomerID
            ,CPF_Cliente
            ,Dt_Abertura
            ,Faixa_Idade
            ,BairroCliente
            ,Cidade_Cliente
            ,UF_Cliente
            ,RegiaoCliente
            ,Flag_TempodeConta
            ,Flag_TempoBloqueado
            ,Flag_Trusted
            ,Flag_Email_NaoVal
            ,Flag_Celular_NaoVal
            ,Flag_Biometria
            ,Flag_Risco_Limit_Vol
            ,Flag_Risco_Limit_Val
            ,Flag_Risco_CBK
            ,Flag_Tetativas
            ,Flag_Bancos
            ,Flag_Card
            ,Flag_Ativo
            ,Flag_Perfil
            ,ScoreZaig
            ,count(*) as Qtd_Cliente
        from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
) cl on cl.CustomerID = p.customer_id
left join (
            select
            distinct
                store_id
                ,Nome_loja
                ,CNPJ_CPF
                ,BairroPosto
                ,cidade
                ,UF
                ,RegiaoPosto
                ,latitude||longitude as Lat_Log_Posto
                ,Post_Limt_Vol
                ,Post_Limt_Val
                ,Post_Cbk_Vol
                ,Post_Cbk_Val
                ,Flag_Risco_Local_Tran
                ,count(*) as Qtd_VIPs
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_Posto_Perfil` 
            group by 1,2,3,4,5,6,7,8,9,10,11,12,13
) perf_post on perf_post.store_id = o.store_id
left join `eai-datalake-data-sandbox.payment.payment_transaction_error` te on p.order_id = te.order_id
left join `eai-datalake-data-sandbox.analytics.tb_motivos_recusa_paypal` r on te.error_code = r.Code
--where pt.status IN ("CANCELLED_BY_GATEWAY","PRE_AUTHORIZED_ERROR","REVERSED_ERROR","AUTHORIZED","SETTLEMENT","COMPLETED")
where  pt.payment_method IN ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET",'GOOGLE_PAY')
and date(pt.created_at) >= current_date - 60
)

select * from Base_final where Flag_TempodeConta is not null and store_id <> 'STO-93792777-829f-47a8-bd63-d21c4dd23aa1'

;






