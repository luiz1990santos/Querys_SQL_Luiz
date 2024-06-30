

/*
select min(Received_Date) as Primeiro_Registro, max(Received_Date) as Ultimo_Registro  FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico`
select min(Received_Date) as Primeiro_Registro, max(Received_Date) as Ultimo_Registro  FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake`



CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake` AS 

INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico`

SELECT
 distinct
 *
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_V2`

where Dispute_ID = '8gsqhgp4tzyzrsz5'

where Effective_Date > (select max(Effective_Date) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` )

*/



---============================================---------
-- Dash TPV + Chargeback - Visão 180 ano
---============================================---------
--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV` where Flag_Merchant_Account_Tranx = 'Abastecimento' and Safra_Tranx = '202403' and Dispute_ID is not null and Flag_Merchant_Account_Tranx = 'Futebol'

--Flag_Merchant_Account_Tranx = 'TudoAzul' and Safra_Tranx = '202401'



CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV` AS 


with

base_cbk_tpv as (
select
distinct
    tranx.gateway_id	
    ,tranx.status	as StatusTranx
    ,tranx.transaction_value	
    ,case when tranx_d.order_id = cbk.order_id then tranx.transaction_value else null end as Flag_Vl_Contestado
    ,ord.cashback_value/100 as cashback_value
    ,tranx_d.order_id	
    ,tranx_d.customer_id	
    ,substring(tranx_d.order_code,1,STRPOS(tranx_d.order_code,'-')) as order_code 	
    --,tranx_d.status	
    ,tranx_d.sales_channel	
    ,ord.pdv_token	
    ,ord.store_id as Cod_Loja	
    --,ord.payment_id
    ,locPost.Nome_loja
    ,locPost.CNPJ_CPF
    ,locPost.tipo_loja
    ,locPost.cidade
    ,locPost.UF as UF_Posto
    ,CASE
    WHEN locPost.UF IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
    WHEN locPost.UF IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
    WHEN locPost.UF IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
    WHEN locPost.UF IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
    WHEN locPost.UF IN ('SC','PR','RS') THEN 'SUL'
    ELSE 'NAOINDENTIFICADO'
    END AS RegiaoPosto
    ,locPost.latitude  as latitude_Posto
    ,locPost.longitude as longitude_Posto
    ,locPost.latitude||locPost.longitude as  latitude_longitude_Posto
    ,left(ord.latitude,7) as  latitude_Tranx
    ,left(ord.longitude,7) as  longitude_Tranx
    ,left(ord.latitude,7) ||left(ord.longitude,7) as  latitude_longitude_Tranx
    ,locTranx.cidade as Cidade_Trax
    ,locTranx.UF as UF_Tranx
    ,(cast(left(ord.latitude,7) as numeric) - cast(if(locPost.latitude = '', null, locPost.latitude) as numeric)) - (cast(left(ord.longitude,7)as numeric) - cast(if(locPost.longitude = '', null, locPost.longitude) as numeric)) as dif_geral
    ,case
    when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 0 then 'M0'
    when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 1 then 'M-1'
    when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 2 then 'M-2'
    when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 3 then 'M-3'
    when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 4 then 'M-4'
    else 'Outros' end as Flag_Filt_Per
    ,date(tranx.created_at) as Dt_Tranx
    ,FORMAT_DATE("%Y%m",tranx.created_at)as Safra_Tranx
    ,cbk.Effective_Date
    ,cbk.Dispute_ID	
    ,cbk.Transaction_Amount	
    --,cbk.Amount_Won
    ,cbk.Transaction_ID	
    ,case 
      when dppaypal.order_code = 'REC-' then 'Recarga'
      when dppaypal.order_code = 'LIV-' then 'Livelo'
      when dppaypal.order_code = 'AZU-' or tranx_d.sales_channel  in ('APP_TUDOAZUL','APP_MILES') then 'TudoAzul'
      when dppaypal.order_code = 'SMI-' then 'Smiles'
      when tranx_d.sales_channel = 'APP_ULTRAGAZ' then 'UltraGaz'
      when dppaypal.Merchant_Account = 'DRYWASHBRL' then 'DryWash'
      when dppaypal.order_code = 'FUT-' or dppaypal.Merchant_Account = 'fangoldenbrl' then 'Futebol'
      when dppaypal.Merchant_Account = 'satelitalbrl'  then 'Shopping'
      when tranx_d.sales_channel in ('APP','APP_AMPM','APP_JET_OIL','PDV_QRCODE') then 'Abastecimento'
      else 'Verificar' end as Flag_Merchant_Account_Tranx
/*
    ,Case 
      when tranx_d.sales_channel in ('APP','APP_AMPM','APP_JET_OIL','PDV_QRCODE') then 'Abastecimento'
      when dppaypal.string_field_2 = 'tudoazulbrl' and substring(tranx_d.order_code,1,STRPOS(tranx_d.order_code,'-')) = 'AZU-' then 'TudoAzul'
      when tranx_d.sales_channel = 'APP_MILES' then 'Smiles'
      when dppaypal.string_field_2 = 'DRYWASHBRL' then 'DryWash'
      when dppaypal.string_field_2 = 'fangoldenbrl' then 'Futebol'
      when dppaypal.string_field_2 = 'recargabrl' then 'Recarga'
      when dppaypal.string_field_2 = 'tudoazulbrl' then 'TudoAzul'
      when dppaypal.string_field_2 = 'satelitalbrl'  then 'Shopping'
      when dppaypal.string_field_2 = 'ubereaibrl' then 'Uber'
      when dppaypal.string_field_2 = 'ultragazbrl' or tranx_d.sales_channel = 'APP_ULTRAGAZ' then 'UltraGaz'
      when tranx_d.sales_channel = 'APP' then 'Abastecimento'
      when dppaypal.string_field_2 in ('ipirangaBRL','ipirangabrl30') and tranx_d.sales_channel = 'APP' then 'Abastecimento'
      else 'Outros' end as Flag_Merchant_Account_Tranx
*/
    ,cbk.Merchant_Account	
    ,substr(cbk.Credit_Card_Number, 4,4) as Card_Ult_4_cbk
    ,cbk.Card_Type	
    ,cbk.Customer_Name	
    ,cbk.Customer_Email	
    ,cbk.BIN
    ,cl.StatusConta
    --,cl.MotivoStatus
    ,cl.Flag_Trusted
    ,cl.CPF_Cliente
    ,case when tranx_d.order_id = cbk.order_id then 'Contestado' else 'NaoContestado' end as Flag_Contestacao
    ,date(cbk.Effective_Date) as Dt_Contestacao
    --,date(bloq.Dt_Bloqueio) as Dt_Bloqueio
    ,FORMAT_DATE("%Y%m",cbk.Effective_Date)as Safra_Contestacao
    
    ,case
   	when date(tranx.created_at ) < date(bloq.Dt_Bloqueio) and cbk.Effective_Date > date(bloq.Dt_Bloqueio)  and StatusConta = 'ACTIVE' then 'Cliente teve transação contestada pos desbloqueado/ Status atual ACTIVE'
    when cbk.Effective_Date > date(bloq.Dt_Bloqueio) and StatusConta = 'BLOCKED' then 'Cliente teve bloqueio antes da contesta/Status atual BLOCKED'
    when cbk.Effective_Date = date(bloq.Dt_Bloqueio) and StatusConta = 'BLOCKED' then 'Cliente bloqueado mesmo dia da contestação/Status atual BLOCKED'
    when cbk.Effective_Date = date(bloq.Dt_Bloqueio) and StatusConta = 'ACTIVE' then 'Cliente bloqueado mesmo dia da contestação/Status atual ACTIVE'
    when cbk.Effective_Date > date(bloq.Dt_Bloqueio) and StatusConta = 'ACTIVE' then 'Cliente teve bloqueio antes da contesta/Status atual ACTIVE'
    when cbk.Effective_Date is not null and date(bloq.Dt_Bloqueio) is null and StatusConta = 'ACTIVE' then 'Cliente sem bloqueio anterior com contestação em 180 dias'
    when FORMAT_DATE("%Y%m",cbk.Effective_Date) = FORMAT_DATE("%Y%m",date(tranx.created_at )) and StatusConta = 'ACTIVE' then 'Cliente teve transação contestada no mesmo mês/ Status atual ACTIVE'
	when date(tranx.created_at ) > date(bloq.Dt_Bloqueio) and cbk.Effective_Date is not null and StatusConta = 'ACTIVE' then 'Cliente teve transação contestada pos bloqueio/ Status atual ACTIVE'
	when date(tranx.created_at ) < date(bloq.Dt_Bloqueio) and cbk.Effective_Date is not null  and StatusConta = 'ACTIVE' then 'Cliente teve transação contestada desbloqueado/ Status atual ACTIVE'
    else 'NA' end as Flag_Bloqueio

      
from `eai-datalake-data-sandbox.payment.payment_transaction`  tranx
join `eai-datalake-data-sandbox.payment.payment`              tranx_d   on tranx_d.id = tranx.payment_id
left join `eai-datalake-data-sandbox.core.orders`             ord       on ord.uuid = tranx_d.order_id
left join (select
            Cpf
            ,customer_id
            ,dt_abertura
            ,Safra_Abertura
            ,Analista
            ,MotivoBloqueio
            ,Flag_Bloqueio
            ,Dt_Bloqueio
            ,Safra_Bloqueio
            ,Rank_Ult_Status
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CLientes_Bloqueados_Fraude` ) bloq on bloq.customer_id = tranx_d.customer_id
left join (   Select 
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
            join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id ) locPost on locPost.store_id = ord.store_id
left join (   Select 
            distinct
              b.city as cidade
              ,b.state as UF
              ,left(b.latitude,7) as latitude
              ,left(b.longitude,7) as longitude
            FROM `eai-datalake-data-sandbox.backoffice.store` a
            join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id ) locTranx 
            on locTranx.latitude like left(ord.latitude,7) and locTranx.longitude like left(ord.longitude,7)
left join ( select
            distinct
              Dispute_ID,
              Original_Dispute_ID,
              Received_Date,
              Effective_Date,
              --Last_Updated,
              Transaction_Date,
              Amount_Disputed,
              --Amount_Won,
              Transaction_Amount,
              Currency_ISO_Code,
              Kind,
              Reason,
              Status,
              Transaction_ID,
              Merchant_Account,
              Order_ID,
              Credit_Card_Number,
              Card_Type,
              Customer_Name,
              Customer_Email,
              Refunded,
              Reply_Before_Date,
              --Payment_Method_Token,
              Chargeback_Protection,
              BIN
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` 
            where Reason = 'Fraud'and Status = 'Open' and Kind = 'Chargeback'
          ) cbk on tranx_d.order_id = cbk.order_id
left join (select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.DE_PARA_PAYPAL_PEDIDO_v4`)  dppaypal on dppaypal.order_code = substring(tranx_d.order_code,1,STRPOS(tranx_d.order_code,'-'))
 left join (
              with
              base as (
                select
                distinct
                    cl.uuid as  CustomerID
                    ,cl.document as CPF_Cliente
                    ,cl.status as StatusConta
                    ,en.city as Cidade_Cliente
                    ,en.state as UF_Cliente
                    ,date(cl.created_at) as Dt_Abertura
                    ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
                    ,case
                    when cl.trusted = 1 then 'Trusted'
                    else 'NoTrusted' end as Flag_Trusted
                    ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual
                    ,CASE
						WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
						WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
						WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
						WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
						WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
                    ELSE 'NAOINDENTIFICADO'
                    END AS RegiaoCliente
                    ,Ev.status as StatusEvento
                    ,ev.observation as MotivoStatus
                    ,ev.event_date as DataStatus
                    ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Ev

                FROM `eai-datalake-data-sandbox.core.customers`             cl
                left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
                left join (select * from `eai-datalake-data-sandbox.core.customer_event` 
                where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT')
              )  Ev on ev.customer_id = cl.id
              order by 1
              )select
              distinct
                *
              from base
              where Rank_Ult_Atual = 1
              ) cl on cl.CustomerID = tranx_d.customer_id	

WHERE 

date(tranx.created_at) >= current_date - 180
--date (tranx.created_at) between '2024-01-01' and '2024-02-29'
and tranx.status in ('AUTHORIZED','COMPLETED','SETTLEMENT','COMPLETE')
and tranx.`payment_method` in ('CREDIT_CARD','DEBIT_CARD','GOOGLE_PAY')
--and dppaypal.order_code = 'LIV-'
)
select
distinct
cbk_tpv.*
,IF(cbk_tpv.Flag_Contestacao = 'Contestado' , 1, 0) as Qtd_Contestacao
,case
      when cbk_tpv.latitude_longitude_Posto = cbk_tpv.latitude_longitude_Tranx then '01_Transacao_no_Posto'
      when cbk_tpv.latitude_longitude_Posto = cbk_tpv.latitude_longitude_Tranx  or substring(cast(cbk_tpv.dif_geral as string),1,5)  in ('-0.026','-0.087','0.008','0.029','-0.001','-0.000','-0.00','0.001','-0.01','0.000','0.002','-0.002','0.003','-0.003') then '01_Transacao_no_Posto'
      when (cast(substring(cast(cbk_tpv.dif_geral as string),1,5) as numeric) between -1.000 and 1.000) or cbk_tpv.UF_Tranx = cbk_tpv.UF_Posto   then '02_Transacao_Proximo_Posto'
      when cast(substring(cast(cbk_tpv.dif_geral as string),1,5) as numeric) < -1.000 
      or cast(substring(cast(cbk_tpv.dif_geral as string),1,5) as numeric) > 1.000 
      or cbk_tpv.UF_Tranx <> cbk_tpv.UF_Posto   then '03_Fora_Posto'
      when cbk_tpv.UF_Tranx is null then '04_Transacao_nao_localizada'
      else '03_Fora_Posto' end as Flag_Local_Posto_Tranx  
from base_cbk_tpv cbk_tpv

;

---============================================---------
-- Dash TPV + Chargeback - Visão 180 ano
-- Cubo_CBKxTPV
---============================================---------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV` where Flag_Vl_Contestado > 0
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_CBKxTPV` where Flag_Merchant_Account_Tranx = 'TudoAzul' and 

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_CBKxTPV` AS 


select
Safra_Tranx
,Flag_Filt_Per
,Flag_Merchant_Account_Tranx
,20 as BPsSaudavel
,count(transaction_value) as Qtd_Transacao
,Sum(Qtd_Contestacao) as Qtd_Contestacao
,Sum(transaction_value) as TPV
,Sum(Flag_Vl_Contestado) as ValorContestado
,(Sum(Flag_Vl_Contestado) / Sum(transaction_value))*10000 as BPs
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV`
group by 1,2,3,4



;



---============================================---------
-- Dash TPV + Chargeback - Visão 180 ano
-- Cubo_Rank_TOP10_Postos
---============================================---------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Rank_TOP10_Postos`


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Rank_TOP10_Postos` AS 


with

base_rank_posto_Contestado as (

select 
Nome_loja
,Cod_Loja
,CNPJ_CPF
,Sum(Flag_Vl_Contestado) as ValorContestado
,count(transaction_value) as Qtd_Contestado


from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV`
where Flag_Filt_Per in ('M0','M-1','M-2')
and Flag_Contestacao = 'Contestado'

group by 1,2,3

),base_rank_posto_Contestado1 as (
select 
a.Nome_loja
,a.Cod_Loja
,a.CNPJ_CPF
--,a.RankPosto_Contestacao
,a.Qtd_Contestado
,a.ValorContestado
,sum(b.transaction_value) as TPV_Loja
,count(transaction_value) as QtdTransacao

from base_rank_posto_Contestado a
join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV` b on a.Cod_Loja = b.Cod_Loja
where Flag_Filt_Per in ('M0','M-1','M-2')
--and Flag_Contestacao != 'Contestado'
group by 1,2,3,4,5
order by 4
), base_rank_posto_Contestado2 as (
select
a.*
,ValorContestado/TPV_Loja as PercComprometido
from base_rank_posto_Contestado1 a
), base_rank_posto_Contestado3 as (
select
distinct
a.*
,DENSE_RANK() OVER (ORDER BY a.Qtd_Contestado desc) AS RankPosto_Contestacao
from base_rank_posto_Contestado2 a
order by 9 
)
select
distinct
	Nome_loja
,Cod_Loja
,CNPJ_CPF
,Qtd_Contestado
,ValorContestado
,TPV_Loja
,QtdTransacao
,PercComprometido
--,RankPosto_Contestacao
from base_rank_posto_Contestado3
where Qtd_Contestado > 5
order by 7
;

---============================================---------
-- Dash TPV + Chargeback - Visão 180 ano
-- Cubo_Rank_TOP10_Clientes
---============================================---------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Rank_TOP10_Clientes`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Rank_TOP10_Clientes` AS 

with

base_rank_posto_Contestado as (

select 
customer_id
,CPF_Cliente
,StatusConta
,Flag_Merchant_Account_Tranx
,Flag_Bloqueio
,Sum(Flag_Vl_Contestado) as ValorContestado
,count(transaction_value) as Qtd_Contestado
,count(distinct Cod_Loja) as Qtd_Cont_Postos


from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV`
--where Flag_Filt_Per in ('M0','M-1','M-2')
where StatusConta = 'ACTIVE'
and Flag_Contestacao = 'Contestado'

group by 1,2,3,4,5


),base_rank_posto_Contestado1 as (
select
a.*
,DENSE_RANK() OVER (ORDER BY a.ValorContestado desc) AS RankPosto_Contestacao
from base_rank_posto_Contestado a
Order by 4 desc

),base_rank_posto_Contestado2 as (
select
*
from base_rank_posto_Contestado1
where RankPosto_Contestacao <=200
order by 3 desc
), base_rank_posto_Contestado3 as (
select 
a.customer_id
,a.CPF_Cliente
,a.StatusConta
,a.Flag_Bloqueio
,a.Flag_Merchant_Account_Tranx
,a.ValorContestado
,a.Qtd_Contestado
,a.Qtd_Cont_Postos
,a.RankPosto_Contestacao
,sum(b.transaction_value) as TPV_Cliente
,count(transaction_value) as QtdTransacao

from base_rank_posto_Contestado2 a
join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV` b on a.customer_id = b.customer_id
--where Flag_Filt_Per in ('M0','M-1','M-2')
--and Flag_Contestacao != 'Contestado'
group by 1,2,3,4,5,6,7,8,9
order by 6
), base_rank_posto_Contestado4 as (
select
a.*
,ValorContestado/TPV_Cliente as PercComprometido
from base_rank_posto_Contestado3 a 
)
select
a.*
from base_rank_posto_Contestado4 a
--where PercComprometido > 0.05

;


---============================================---------
-- Dash TPV + Chargeback - PostosTop10
-- Cubo_Rank_PostosCriticos_Clientes
---============================================---------
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV`
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Rank_TOP10_Clientes`
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Rank_PostosCriticos_Clientes` where customer_id = 'CUS-438cfa47-10d9-4ee6-96c5-64d5427a8906'

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Rank_PostosCriticos_Clientes` AS 

with

base_rank_posto_Contestado as (

select 
base.customer_id
,base.CPF_Cliente
,Post.Nome_loja
,Post.Cod_Loja
,base.StatusConta
,Flag_Bloqueio
,Sum(base.Flag_Vl_Contestado) as ValorContestado
,count(base.transaction_value) as Qtd_Contestado
,count(distinct base.Cod_Loja) as Qtd_Cont_Postos


from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV` base
join (select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Rank_TOP10_Postos` 
      order by 4 desc) Post on Post.Cod_Loja = base.Cod_Loja

--where Flag_Filt_Per in ('M0','M-1','M-2')
where StatusConta = 'ACTIVE'
--and Flag_Contestacao = 'Contestado'
group by 1,2,3,4,5,6


),base_rank_posto_Contestado1 as (
select
a.*
,DENSE_RANK() OVER (ORDER BY a.ValorContestado desc) AS RankPosto_Contestacao
from base_rank_posto_Contestado a
Order by 4 desc

),base_rank_posto_Contestado2 as (
select
*
from base_rank_posto_Contestado1

order by 3 desc
), base_rank_posto_Contestado3 as (
select 
a.customer_id
,a.CPF_Cliente
,a.StatusConta
,a.Flag_Bloqueio
,a.Nome_loja
,a.Cod_Loja
,a.ValorContestado
,a.Qtd_Contestado
,a.Qtd_Cont_Postos
,a.RankPosto_Contestacao
,sum(b.transaction_value) as TPV_Cliente
,count(transaction_value) as QtdTransacao

from base_rank_posto_Contestado2 a
join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV` b on a.customer_id = b.customer_id
--where Flag_Filt_Per in ('M0','M-1','M-2')
--and Flag_Contestacao != 'Contestado'
group by 1,2,3,4,5,6,7,8,9,10
order by 6
), base_rank_posto_Contestado4 as (
select
a.*
,ValorContestado/TPV_Cliente as PercComprometido
from base_rank_posto_Contestado3 a 
)
select
a.customer_id
,a.CPF_Cliente
,a.StatusConta
,a.Flag_Bloqueio
,a.Nome_loja
,a.Cod_Loja
,a.ValorContestado
,a.Qtd_Contestado
,a.Qtd_Cont_Postos
,a.RankPosto_Contestacao
,a.TPV_Cliente
,a.QtdTransacao
,a.PercComprometido
,a.ValorContestado/a.Qtd_Contestado as TicktMedioCont
from base_rank_posto_Contestado4 a
where PercComprometido > 0.15

;



---============================================---------
-- Dash TPV + Chargeback Abastecimento - Visão 180 ano
---============================================---------
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV_Abastecimento`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV_Abastecimento` AS 


with

base_cbk_tpv as (
select
distinct
    tranx.gateway_id	
    ,tranx.status	as StatusTranx
    ,tranx.transaction_value	
    ,case when tranx_d.order_id = cbk.order_id then tranx.transaction_value else null end as Flag_Vl_Contestado
    ,ord.cashback_value/100 as cashback_value
    ,tranx_d.order_id	
    ,tranx_d.customer_id	
    ,substring(tranx_d.order_code,1,STRPOS(tranx_d.order_code,'-')) as order_code 	
    --,tranx_d.status	
    ,tranx_d.sales_channel	
    ,tranx.payment_method
    ,ord.pdv_token	
    ,ord.store_id as Cod_Loja	
    --,ord.payment_id
    ,locPost.Nome_loja
    ,locPost.CNPJ_CPF
    ,locPost.tipo_loja
    ,locPost.cidade
    ,locPost.UF as UF_Posto
    ,CASE
    WHEN locPost.UF IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
    WHEN locPost.UF IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
    WHEN locPost.UF IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
    WHEN locPost.UF IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
    WHEN locPost.UF IN ('SC','PR','RS') THEN 'SUL'
    ELSE 'NAOINDENTIFICADO'
    END AS RegiaoPosto
    ,locPost.latitude  as latitude_Posto
    ,locPost.longitude as longitude_Posto
    ,locPost.latitude||locPost.longitude as  latitude_longitude_Posto
    ,left(ord.latitude,7) as  latitude_Tranx
    ,left(ord.longitude,7) as  longitude_Tranx
    ,left(ord.latitude,7) ||left(ord.longitude,7) as  latitude_longitude_Tranx
    ,locTranx.cidade as Cidade_Trax
    ,locTranx.UF as UF_Tranx
    ,(cast(left(ord.latitude,7) as numeric) - cast(if(locPost.latitude = '', null, locPost.latitude) as numeric)) - (cast(left(ord.longitude,7)as numeric) - cast(if(locPost.longitude = '', null, locPost.longitude) as numeric)) as dif_geral
    ,case
    when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 0 then 'M0'
    when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 1 then 'M-1'
    when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 2 then 'M-2'
    when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 3 then 'M-3'
    when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 4 then 'M-4'
    else 'Outros' end as Flag_Filt_Per
    ,date(tranx.created_at) as Dt_Tranx
    ,FORMAT_DATE("%Y%m",tranx.created_at)as Safra_Tranx
    ,cbk.Effective_Date
    ,cbk.Dispute_ID	
    ,cbk.Transaction_Amount	
    --,cbk.Amount_Won
    ,cbk.Transaction_ID	
    ,Case 
      when tranx_d.sales_channel in ('APP','APP_AMPM','APP_JET_OIL','PDV_QRCODE') then 'Abastecimento'
      when tranx_d.sales_channel = 'APP_MILES' then 'Smiles'
      when dppaypal.string_field_2 = 'DRYWASHBRL' then 'DryWash'
      when dppaypal.string_field_2 = 'fangoldenbrl' then 'Futebol'
      when dppaypal.string_field_2 = 'recargabrl' then 'Recarga'
      when dppaypal.string_field_2 = 'tudoazulbrl' then 'TudoAzul'
      when dppaypal.string_field_2 = 'satelitalbrl'  then 'Shopping'
      when dppaypal.string_field_2 = 'ubereaibrl' then 'Uber'
      when dppaypal.string_field_2 = 'ultragazbrl' or tranx_d.sales_channel = 'APP_ULTRAGAZ' then 'UltraGaz'
      when tranx_d.sales_channel = 'APP' then 'Abastecimento'
      when dppaypal.string_field_2 in ('ipirangaBRL','ipirangabrl30') and tranx_d.sales_channel = 'APP' then 'Abastecimento'
      else 'Outros' end as Flag_Merchant_Account_Tranx
    ,cbk.Merchant_Account	
    ,substr(cbk.Credit_Card_Number, 4,4) as Card_Ult_4_cbk
    ,cbk.Card_Type	
    ,cbk.Customer_Name	
    ,cbk.Customer_Email	
    ,cbk.BIN
    ,cl.StatusConta
    ,cl.MotivoStatus
    ,cl.Flag_Trusted
    ,cl.CPF_Cliente
    ,case when tranx_d.order_id = cbk.order_id then 'Contestado' else 'NaoContestado' end as Flag_Contestacao
    ,date(cbk.Effective_Date) as Dt_Contestacao
    ,FORMAT_DATE("%Y%m",cbk.Effective_Date)as Safra_Contestacao

from `eai-datalake-data-sandbox.payment.payment_transaction`  tranx
join `eai-datalake-data-sandbox.payment.payment`              tranx_d   on tranx_d.id = tranx.payment_id
left join `eai-datalake-data-sandbox.core.orders`             ord       on ord.uuid = tranx_d.order_id
left join (   Select 
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
            join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id ) locPost on locPost.store_id = ord.store_id
left join (   Select 
            distinct
              b.city as cidade
              ,b.state as UF
              ,left(b.latitude,7) as latitude
              ,left(b.longitude,7) as longitude
            FROM `eai-datalake-data-sandbox.backoffice.store` a
            join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id ) locTranx 
            on locTranx.latitude like left(ord.latitude,7) and locTranx.longitude like left(ord.longitude,7)
left join ( select
            distinct
              Dispute_ID,
              Original_Dispute_ID,
              Received_Date,
              Effective_Date,
              --Last_Updated,
              Transaction_Date,
              Amount_Disputed,
              --Amount_Won,
              Transaction_Amount,
              Currency_ISO_Code,
              Kind,
              Reason,
              Status,
              Transaction_ID,
              Merchant_Account,
              Order_ID,
              Credit_Card_Number,
              Card_Type,
              Customer_Name,
              Customer_Email,
              Refunded,
              Reply_Before_Date,
              --Payment_Method_Token,
              Chargeback_Protection,
              BIN
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` 
            where Reason = 'Fraud'and Status = 'Open' and Kind = 'Chargeback'
          ) cbk on tranx_d.order_id = cbk.order_id
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.TB_DE_PARA_PAYPAL_PEDIDO`  dppaypal on dppaypal.string_field_0 = substring(tranx_d.order_code,1,STRPOS(tranx_d.order_code,'-'))
 left join (
              with
              base as (
                select
                distinct
                    cl.uuid as  CustomerID
                    ,cl.document as CPF_Cliente
                    ,cl.status as StatusConta
                    ,en.city as Cidade_Cliente
                    ,en.state as UF_Cliente
                    ,date(cl.created_at) as Dt_Abertura
                    ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
                    ,case
                    when cl.trusted = 1 then 'Trusted'
                    else 'NoTrusted' end as Flag_Trusted
                    ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual
                    ,CASE
						WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
						WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
						WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
						WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
						WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
                    ELSE 'NAOINDENTIFICADO'
                    END AS RegiaoCliente
                    ,Ev.status as StatusEvento
                    ,ev.observation as MotivoStatus
                    ,ev.event_date as DataStatus
                    ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Ev

                FROM `eai-datalake-data-sandbox.core.customers`             cl
                left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
                left join (select * from `eai-datalake-data-sandbox.core.customer_event` 
                where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT')
              )  Ev on ev.customer_id = cl.id
              order by 1
              )select
              distinct
                *
              from base
              where Rank_Ult_Atual = 1
              ) cl on cl.CustomerID = tranx_d.customer_id	

WHERE 
date(tranx.created_at) >= current_date - 180
and locPost.tipo_loja = 'POS'
and tranx.status in ('AUTHORIZED','COMPLETED','SETTLEMENT','COMPLETE')
),base_TPV_Abastecimento as (
select
distinct
cbk_tpv.*
,IF(cbk_tpv.Flag_Contestacao = 'Contestado' , 1, 0) as Qtd_Contestacao
,case
      when cbk_tpv.latitude_longitude_Posto = cbk_tpv.latitude_longitude_Tranx then '01_Transacao_no_Posto'
      when cbk_tpv.latitude_longitude_Posto = cbk_tpv.latitude_longitude_Tranx  or substring(cast(cbk_tpv.dif_geral as string),1,5)  in ('-0.026','-0.087','0.008','0.029','-0.001','-0.000','-0.00','0.001','-0.01','0.000','0.002','-0.002','0.003','-0.003') then '01_Transacao_no_Posto'
      when (cast(substring(cast(cbk_tpv.dif_geral as string),1,5) as numeric) between -1.000 and 1.000) or cbk_tpv.UF_Tranx = cbk_tpv.UF_Posto   then '02_Transacao_Proximo_Posto'
      when cast(substring(cast(cbk_tpv.dif_geral as string),1,5) as numeric) < -1.000 
      or cast(substring(cast(cbk_tpv.dif_geral as string),1,5) as numeric) > 1.000 
      or cbk_tpv.UF_Tranx <> cbk_tpv.UF_Posto   then '03_Fora_Posto'
      when cbk_tpv.UF_Tranx is null then '04_Transacao_nao_localizada'
      else '03_Fora_Posto' end as Flag_Local_Posto_Tranx  
from base_cbk_tpv cbk_tpv
)
select
Safra_Tranx
,Safra_Contestacao
,Flag_Filt_Per
,Nome_loja
,Cod_Loja
,CNPJ_CPF
,RegiaoPosto
,payment_method as MeioPagto
,Flag_Merchant_Account_Tranx
,Flag_Contestacao
,Flag_Local_Posto_Tranx
,Count(transaction_value) as Qtd_Transacao
,Sum(transaction_value) as TPV_Abastecimento
,Sum(Flag_Vl_Contestado) as Vl_Contestado

from base_TPV_Abastecimento
where Flag_Merchant_Account_Tranx = 'Abastecimento'
group by 1,2,3,4,5,6,7,8,9,10,11

;


-------------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------
-- TRANSACOES LATAM - CBK
-----------------------------------


-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_latam_TPV_CBK` where TransacaoId = ''

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_latam_TPV_CBK` AS

with
 
base_pedidos as (
 
SELECT * FROM (
SELECT DISTINCT
 
        date(pe.DataPedido) as DataPedido
        ,FORMAT_DATETIME("%Y%m",pe.DataPedido) as Safra_Tranx
        ,FORMAT_DATETIME("%d",pe.DataPedido) as Dia_Tranx
        ,case
         when pe.SituacaoID = 2 or pe.situacaoID = 4 or pe.SituacaoID = 14 then 'Entregue'
         when pe.SituacaoID = 5 or pe.SituacaoID = 1 then 'Cancelado' end as Situacao
        ,pr.CodigoExterno
        ,r.RedeOrigem as Origem
        ,pe.PaypalPaymentId as TransacaoId
        ,pe.PedidoID as Pedido
        ,pe.ValorReais
        ,RANK() OVER (PARTITION BY pe.PedidoID ORDER BY pe.DataPedido desc) AS Rank_trans
        ,pt.DatadeNascimento
        ,pt.CPF as CPF_Compra
        ,cl.full_name
        ,cl.email as Email_Cliente
        ,cl.document
        ,cl.uuid
        ,case when cl.trusted = 1 then 'Trusted' else 'NoTrusted' end as Flag_Trusted
        ,cl.created_at as Dt_Abertura
        ,cl.risk_analysis_status
        ,cl.status as StatusConta
        ,CASE
          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=0   THEN '01_00-Hoje'
          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=1   THEN '02_01-Ontem'
          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=10   THEN '03_00-10DIAS'
          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=30   THEN '04_11-30DIAS'
          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=60   THEN '05_31-60DIAS'
          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=90   THEN '06_61-90DIAS'
          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=180  THEN '07_91-180DIAS'
          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=364  THEN '08_180-1ANO'
          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) >=365  THEN '09_+1ANO'
        END AS Flag_TempConta
        ,case when cast(cbk.Order_ID as string) = cast(pe.PedidoID as string)  then 'Contestado' else 'Nao_Contestado' end as Flag_Contestacao
        ,forn.Descricao
        ,forn.NomeParceiro
 
 
  from `eai-datalake-data-sandbox.loyalty.tblPedidos` pe
  join `eai-datalake-data-sandbox.loyalty.tblProdutos` pr on pe.produtoid = pr.produtoid
  left join `eai-datalake-data-sandbox.loyalty.tblRedeOrigem` r on pe.RedeOrigemID = r.RedeOrigemId
  left join `eai-datalake-data-sandbox.loyalty.tblParticipantes`  pt on pt.ParticipanteID = pe.ParticipanteID
  join (SELECT * FROM `eai-datalake-data-sandbox.loyalty.tblProdutos`
        where NomeParceiro like '%Latam%'or NomeParceiro like '%LATAM%'and Inativo = false) forn on forn.ProdutoID = pr.ProdutoID
  left join `eai-datalake-data-sandbox.core.customers`            cl on pt.CPF =cl.document
  left join (select
            distinct
            cbk.*
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_V2` cbk
            where
            cbk.Reason = 'Fraud'
            and cbk.Status = 'Open'
            and cbk.Kind = 'Chargeback'
            and Merchant_Account like '%latam%' 
            ) cbk on cast(cbk.Order_ID as string) = cast(pe.PedidoID as string)
where
--date(pe.DataPedido) >= '2023-01-01' and date(pe.DataPedido) <= current_date
date (pe.DataPedido)  >= (current_date() - 180)
and pe.MeioPagamento = 'mundipagg'
and pe.MeioPagamento is not null
order by DataPedido asc)
), base_pedidos_2 as (
select * from base_pedidos where Rank_trans = 1
)
select
distinct
a.*
,latam.Transaction_ID
,case
when latam.Risk_Decision = 'Approve' then 'Aprovar'
when latam.Risk_Decision = 'Decline' then 'Negar'
when latam.Risk_Decision = 'Not Evaluated' then 'NaoAvaliado'
else 'NovoStatus' end as Flag_DecisaodeRisco
 
,case
when latam.Transaction_Status = 'gateway_rejected' then     'Rejeitado na entrada'
when latam.Transaction_Status = 'settling'      then 'Em processamento'
when latam.Transaction_Status = 'processor_declined'  then 'Negado pelo emissor'
when latam.Transaction_Status = 'settled' then 'Processado'
else 'NovoStatus' end as Flag_StatusTrans
 
,case
when latam.Processor_Response_Text = 'Approved' then 'Aprovado'
when latam.Processor_Response_Text = 'Card Issuer Declined CVV' then 'Negado pelo emissor CVV Inválido'
when latam.Processor_Response_Text = 'Declined' then 'Negado'
when latam.Processor_Response_Text = 'Declined - Call Issuer' then 'Negado - Entre em contato com o emissor'
when latam.Processor_Response_Text = 'Insufficient Funds' then 'Limite insuficiente'
when latam.Processor_Response_Text = 'Invalid Transaction' then   'Transação inválida'
when latam.Processor_Response_Text = 'Issuer or Cardholder has put a restriction on the card' then    'Emissor ou Usuário inseriu restrição no cartão'
when latam.Processor_Response_Text = 'Processed' then 'Processed'
when latam.Processor_Response_Text = 'Processor Declined' then 'Negado pelo emissor'
when latam.Processor_Response_Text = 'Processor Declined - Fraud Suspected' then 'Negado pelo emissor - Suspeita de fraude'
when latam.Processor_Response_Text = 'Unavailable' then     'Indisponível'
else 'NovoStatus' end as Flag_DescRespEmissor
 
,latam.Risk_Decision as Dec_Motor_PayPal
,latam.Transaction_Status as Status_Trans_PayPal
,latam.Processor_Response_Text  as Status_Trans_Emissor
,latam.Created_Datetime
,latam.Order_ID
,latam.Payment_Instrument_Type
,latam.Card_Type
,latam.Customer_ID
,latam.Customer_Email
,latam.Payment_Method_Token
,latam.Gateway_Rejection_Reason
,latam.Fraud_Detected
,latam.Issuing_Bank
,latam.Amount_Authorized/100 as Vl_PayPal
,case when latam.Processor_Response_Text = 'Approved' then latam.Amount_Authorized/100 else 0 end as TPV_Latam
,case when latam.Processor_Response_Text = 'Approved' and Flag_Contestacao = 'Contestado' then latam.Amount_Authorized/100 else 0 end as VlContestado
,case when latam.Processor_Response_Text = 'Approved' then 1 else 0 end as Qtd_Aprovada
,case when latam.Processor_Response_Text = 'Approved' and Flag_Contestacao = 'Contestado' then 1 else 0 end as Qtd_Contestada
 
from base_pedidos_2 a
join (Select
distinct * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2` 
where Merchant_Account like '%latam%' )
latam on cast(latam.Order_ID as string) = Cast(a.Pedido as string)
 
 ;