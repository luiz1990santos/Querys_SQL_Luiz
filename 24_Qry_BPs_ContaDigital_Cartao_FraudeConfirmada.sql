-- BASE TRANSAÇÕES COM FRAUDE CONFIRMADA E SUSPEITA

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_TPV_Total_FraudeConfirmada_0`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_TPV_Total_FraudeConfirmada_0` AS 

      
--base_dados_consulta as (

with
base as (
        select
        distinct
            cl.document as Cpf,
            cl.uuid as customer_id,
            cl.created_at as dt_abertura,
            ev.observation as motivo,
            ev.user_name,
            ev.event_date,
            case
            when ev.observation = 'Fraude confirmada' then 'Fraude confirmada'
            when ev.observation = 'Suspeita de fraude' then 'Suspeita de fraude'
            when ev.observation = 'Bloqueio de cadastro' then 'Bloqueio de cadastro'
            when ev.observation is null then 'Sem Bloqueio'
            when ev.observation = '' then 'Sem Bloqueio'
            else 'Outros' end as MotivoBloqueio,
            case
            when ev.observation In ('Fraude confirmada','Suspeita de fraude')  then 'Bloqueio Fraude'
            when ev.observation = 'Bloqueio de cadastro' then 'Bloqueio Preventivo'
            when ev.observation is null then 'Sem Bloqueio'
            when ev.observation = '' then 'Sem Bloqueio'
            else 'Outros' end as Flag_Bloqueio,
            RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
        from  `eai-datalake-data-sandbox.core.customers`   cl                                           
        left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id 
        where 
        --date(ev.event_date) between '2022-01-01' and '2023-12-31'
        --date(ev.event_date) >= current_date - 180
        --and  
        ev.observation in ('Fraude confirmada', 'Suspeita de fraude')
), base_1 as (
        select 
        * 
        from base 
        where Rank_Ult_Status = 1
),base_Saldo as (
        SELECT 
        distinct
        T.numerodocumento as  DOCUMENTO,                     -- CPF / CNPJ    
        round(sum(T.valor),2) as SALDO                   -- SOMA DOS VALORES QUE COMPÕEM O SALDO
FROM `eai-datalake-data-sandbox.orbitall.tb_topaz` T
where    status = 'MOVIMENTAÇÃO EXECUTADA' -- STATUS NECESSARIO DE TRANSACOES CONCLUIDAS    
and T.data <= CURRENT_DATE()-1   -- FIXAR COM A DATA COM A QUAL SE PRETENDE ANALISAR O SALDO    
and length(T.numerodocumento) <= 11  -- REMOVER CASO QUEIRA TRAZER TAMBÉM CNPJ
group by 1
)  select 
    distinct
    bs.* 
    ,sl.SALDO
from base_Saldo sl
join base_1 bs on sl.DOCUMENTO = bs.Cpf

;
-------------------------------------------------------------------------------------------------------------------------------
--- base Saldo da conta
-------------------------------------------------------------------------------------------------------------------------------
-- Ultimos bloqueios de 180 dias - Saldo Conta

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_TPV_Total_FraudeConfirmada_VlPreservado` AS 

select
base.*
,FORMAT_DATETIME("%Y%m",dt_abertura) as Safra_Abertura
,FORMAT_DATETIME("%Y%m",event_date) as Safra_Ev
,sld.SaldoConta

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_TPV_Total_FraudeConfirmada_0` base
left Join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Saldo_Preservado` sld on sld.CPF = base.Cpf
order by Cpf,SaldoConta desc


;
-------------------------------------------------------------------------------------------------------------------------------
--- base de clientes que tiveram contestação e historico transacional
-------------------------------------------------------------------------------------------------------------------------------

--SELECT * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_TPV_Total_FraudeConfirmada_1` where customer_id = 'CUS-627117ec-e4aa-43f0-9ff2-cf5a698542e9'

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_TPV_Total_FraudeConfirmada_1`  AS 

with
base_transacional as (       
    select
    distinct
        tranx_d.created_transaction_at
        ,payment_transaction.gateway_id
        ,FORMAT_DATE("%Y%m",tranx_d.created_transaction_at)as Safra_Tranx
        ,FORMAT_DATE("%Y",tranx_d.created_transaction_at)as Ano_Tranx
        ,date(tranx_d.created_transaction_at) as Dt_Tranx
        ,EXTRACT(HOUR FROM tranx_d.created_transaction_at)as Hr_Tranx
        ,payment.order_code as Order_ID
        ,cl.customer_id
        ,cl.Flag_Trusted
        ,cl.StatusConta
        ,tranx_d.operation
        ,tranx_d.status
        ,tranx_d.type
        ,tranx_d.amount/100 as Vl_Tranx
        ,payment_transaction.transaction_value
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
        when ord.uuid is null then tranx_d.flow
        when payment_transaction.payment_method	is null then 'Outros'
        when substring(payment.order_code,1,STRPOS(payment.order_code,'-'))	is null then 'Outros'
        when ord.uuid = payment.order_id and tranx_d.status in ('DENIED','CANCELLED') then 'TIPO_N/A'      
        else 'Outros' end as Tipo_Pgto

        ,case 
        when tranx_d.status in ('APPROVED','FINISHED') then 'Aprovado'
        when tranx_d.status in ('DENIED','CANCELLED') then 'Negado'
        when tranx_d.status in ('PROCESSING','SCHEDULED','PENDING') then 'Processando'
        end as Flag_Status_Transacao
        ,case
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 0 then 'M0'
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 1 then 'M-1'
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 2 then 'M-2'
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 3 then 'M-3'
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 4 then 'M-4'
        else 'Outros' end as Flag_Filt_Per
        ,case
        when dppaypal.string_field_0 <> substring(ord.code,1,STRPOS(ord.code,'-')) then tranx_d.type
        when dppaypal.string_field_0 = substring(ord.code,1,STRPOS(ord.code,'-')) then dppaypal.string_field_2
        Else tranx_d.type end as Flag_Merchant_Account
        ,case when base.customer_id = tranx_d.customer_id then 'FraudeConfrimada' else 'NaoFraude' end as Flag_Fraude
        ,substring(payment.order_code,1,STRPOS(payment.order_code,'-')) as Cod_ID
        ,base.MotivoBloqueio

    from `eai-datalake-data-sandbox.elephant.transaction`                                 tranx_d
    left join `eai-datalake-data-sandbox.payment.payment`                                 payment               on tranx_d.own_id = payment.order_id
    left join `eai-datalake-data-sandbox.payment.payment_transaction`                     payment_transaction   on payment.id = payment_transaction.payment_id
    left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.TB_DE_PARA_PAYPAL_PEDIDO`  dppaypal              on dppaypal.string_field_0 = substring(payment.order_code,1,STRPOS(payment.order_code,'-'))
    left join `eai-datalake-data-sandbox.core.orders`                                      ord                   on ord.uuid = tranx_d.own_id
    left join 
            (select distinct * 
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_TPV_Total_FraudeConfirmada_0` ) base on base.customer_id = tranx_d.customer_id
    Left Join 
            (Select cl.uuid as customer_id, case when cl.trusted = 1 then 'Trusted' else 'NoTrusted' end as Flag_Trusted, cl.status as StatusConta 
            from `eai-datalake-data-sandbox.core.customers`  cl) cl  on cl.customer_id = tranx_d.customer_id

    where 
    tranx_d.status in ('APPROVED','FINISHED')--,'DENIED','CANCELLED')
  --and date(tranx_d.created_transaction_at) >= current_date - 45
  --and date(tranx_d.created_transaction_at) between '2022-01-01' and '2023-01-31'
  --FORMAT_DATETIME('%Y',tranx_d.created_transaction_at) = '2022'
) --select * from base_transacional where Tipo_Pgto is null
select
distinct
tranx.Safra_Tranx
,tranx.Ano_Tranx
,tranx.Flag_Trusted
,tranx.StatusConta
,FORMAT_DATE("%Y%m",base.event_date) as Safra_Bloqueio
,tranx.status
,tranx.type
,tranx.Cod_ID
,tranx.sales_channel
,tranx.MotivoBloqueio
,tranx.flow
,tranx.Tipo_Pgto
,case when tranx.Tipo_Pgto in ('CREDIT_CARD','DEBIT_CARD','GOOGLE_PAY') then 'Cartao' else 'ContaDigital' end as Flag_Operacao
,tranx.Flag_Status_Transacao
,tranx.Flag_Filt_Per
,tranx.Flag_Merchant_Account
,tranx.Flag_Fraude
,Case 
    When tranx.Flag_Valor >=0 	  and tranx.Flag_Valor <=20 	Then '01 000 a 20 '
    When tranx.Flag_Valor > 20 	  and tranx.Flag_Valor <=40 	Then '02 021 a 40 '
    When tranx.Flag_Valor > 40 	  and tranx.Flag_Valor <=60 	Then '03 041 a 60 '
    When tranx.Flag_Valor > 60 	  and tranx.Flag_Valor <=80 	Then '04 061 a 80 '
    When tranx.Flag_Valor > 80 	  and tranx.Flag_Valor <=100 	Then '05 081 a 100'
    When tranx.Flag_Valor > 100 	and tranx.Flag_Valor <=120 	Then '06 101 a 120'
    When tranx.Flag_Valor > 120 	and tranx.Flag_Valor <=140 	Then '07 121 a 140'
    When tranx.Flag_Valor > 140 	and tranx.Flag_Valor <=160 	Then '08 141 a 160'
    When tranx.Flag_Valor > 160 	and tranx.Flag_Valor <=180 	Then '09 161 a 180'
    When tranx.Flag_Valor > 180 	and tranx.Flag_Valor <=200 	Then '10 181 a 200'
    When tranx.Flag_Valor > 200 	and tranx.Flag_Valor <=220 	Then '11 201 a 220'
    When tranx.Flag_Valor > 220 	and tranx.Flag_Valor <=240 	Then '12 221 a 240'
    When tranx.Flag_Valor > 240 	and tranx.Flag_Valor <=260 	Then '13 241 a 260'
    When tranx.Flag_Valor > 260 	and tranx.Flag_Valor <=280 	Then '14 261 a 280'
    When tranx.Flag_Valor > 280 	and tranx.Flag_Valor <300 	Then '15 281 a 299'
    When tranx.Flag_Valor = 300 							Then '16 300'
    When tranx.Flag_Valor > 300 	and tranx.Flag_Valor <=320 	Then '17 301 a 320'
    When tranx.Flag_Valor > 320 	and tranx.Flag_Valor <=500 	Then '18 321 a 500'
    When tranx.Flag_Valor > 500 	and tranx.Flag_Valor <=700 	Then '19 501 a 700'
    When tranx.Flag_Valor > 700 	and tranx.Flag_Valor <=1000 	Then '20 701 a 1000'
    When tranx.Flag_Valor > 1000 	and tranx.Flag_Valor <=1500 	Then '21 1001 a 1500'    
    When tranx.Flag_Valor > 1500 							Then '22 1501>' 
End as Faixa_Valor
,sum(tranx.Flag_Valor) as Vl_total_TPV
,sum(tranx.cashback_value) as Vl_total_Cashback
,case when tranx.Flag_Fraude = 'FraudeConfrimada' then sum(tranx.Flag_Valor) else 0 end  as Vl_Fraude
,case when tranx.Flag_Fraude = 'FraudeConfrimada' then sum(tranx.cashback_value) else 0 end  as Vl_Fraude_Cashback
,case when tranx.Flag_Fraude = 'FraudeConfrimada' then sum(base.Saldo) else 0 end  as Vl_Saldo_Conta
,case when tranx.Flag_Fraude = 'FraudeConfrimada' then Count(distinct tranx.customer_id) else 0 end  as Qtd_Clientes_Fraude
,Count(distinct tranx.customer_id) as Qtd_Clientes

from base_transacional tranx
 left join  (select distinct * 
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_TPV_Total_FraudeConfirmada_0` ) base on base.customer_id = tranx.customer_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

;

