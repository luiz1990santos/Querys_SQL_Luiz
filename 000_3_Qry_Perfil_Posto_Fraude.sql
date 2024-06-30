--=========================================================================--
-- Todas analise realizadas em 180 dias
--=========================================================================--

--------------------------- Levantamento Postos Criticos  -------------------
-- analise volume transacional no limite de 300 reais regua teto
-- analise Valor transacional no limite de 300 reais regua teto
------------------------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Tran_Limit_Postos` order by 1


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Tran_Limit_Cliente` AS 

with

base_trans_limit as (

SELECT
p.customer_id
,p.order_id
,ord.store_id
,Flag_Trusted
,MotivoBloqueio
,Faixa_Idade
,Flag_TempodeConta
,RegiaoCliente
,Flag_Perfil

,case when pt.transaction_value = 300 then 1 else 0 end as Qtd_Tran_Limite
,Sum(IF(pt.transaction_value = 300, pt.transaction_value, 0)) as Vl_Tran_Limite
,Sum(pt.transaction_value) as TPV
,count(pt.transaction_value) as QtdTransacao
FROM `eai-datalake-data-sandbox.payment.payment` p
left join `eai-datalake-data-sandbox.payment.payment_instrument` pi on p.id = pi.id
left join `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
left join `eai-datalake-data-sandbox.core.orders` ord on p.order_id = ord.uuid
join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`  cl on cl.CustomerID = p.customer_id
join (
      SELECT 
      distinct
        a.id 
        ,c.PontoVendaID 
        ,a.uuid as store_id
        ,a.name as Razao
        ,post.name as NomeFantasia
        ,a.document as CNPJ_CPF
        ,left(a.document,12) as CNPJ
        ,post.address as End_Post
        ,post.searched_lat
        ,post.searched_lng
        ,a.type as tipo_loja
        ,b.city as cidade_Post
        ,b.state as UF_Post
        ,left(b.latitude,8) as latitude_Post
        ,left(b.longitude,8) as longitude_Post

      FROM `eai-datalake-data-sandbox.backoffice.store` a
      join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id
      join `eai-datalake-data-sandbox.loyalty.tblPontoVenda` c on c.CNPJ = left(a.document,12)
      join `eai-datalake-data-sandbox.maps.store_place_details` post on post.document = a.document
      where a.type = 'POS'
) Post on Post.store_id = ord.store_id
where
date(pt.created_at) >= current_date - 180
and pt.transaction_value > 0
and pt.payment_method in ("CASH")
--and p.customer_id = 'CUS-8922c83e-ea12-47fd-981d-ddfaa2d33c07'
group by 1,2,3,4,5,6,7,8,9,10
), Base_final_Crivo_Limite as (
select
limt.customer_id
,limt.Flag_Trusted
,limt.MotivoBloqueio
,limt.Faixa_Idade
,limt.Flag_TempodeConta
,limt.RegiaoCliente
,limt.Flag_Perfil
,Sum(limt.TPV) as TPV
,Sum(limt.QtdTransacao) as QtdTransacao
from base_trans_limit limt
group by 1,2,3,4,5,6,7
), base_Final_Crivo_Limite_1 as (
select
limt.customer_id
,limt.Flag_Trusted
,limt.MotivoBloqueio
,limt.Faixa_Idade
,limt.Flag_TempodeConta
,limt.RegiaoCliente
,limt.Flag_Perfil
,TPV
,QtdTransacao
,Post_Trigados.Vl_Tran_Limite
,Post_Trigados.Qtd_Tran_Limite
,(Post_Trigados.Qtd_Tran_Limite/limt.QtdTransacao)*100 as Perc_Vol_Trans_limt
,(Post_Trigados.Vl_Tran_Limite/limt.TPV)*100 as Perc_Val_Trans_limt

from Base_final_Crivo_Limite limt
join (
  select * from (
          with
          base_Analise as (
          select
          customer_id
          ,sum(Qtd_Tran_Limite) as Qtd_Tran_Limite
          ,sum(Vl_Tran_Limite) as Vl_Tran_Limite
          from base_trans_limit
          where Qtd_Tran_Limite >0
          group by 1
          )
          select
          *
          from base_Analise))Post_Trigados on Post_Trigados.customer_id = limt.customer_id
)
select
limt.customer_id
,limt.Flag_Trusted
,limt.MotivoBloqueio
,limt.Faixa_Idade
,limt.Flag_TempodeConta
,limt.RegiaoCliente
,limt.Flag_Perfil
,TPV
,QtdTransacao
,Vl_Tran_Limite
,Qtd_Tran_Limite
,Perc_Vol_Trans_limt
,Perc_Val_Trans_limt
,case 
      when Perc_Vol_Trans_limt <= 5 then 'Risco_Vol_Ate_5%'
      when Perc_Vol_Trans_limt <= 10 then 'Risco_Vol_Ate_10%'
      when Perc_Vol_Trans_limt <= 15 then 'Risco_Vol_Ate_15%'
      when Perc_Vol_Trans_limt <= 25 then 'Risco_Vol_Ate_25%'
      when Perc_Vol_Trans_limt > 25 then 'Risco_Vol_Maior_25%'
      else 'NC'
End as Flag_Risco_Limit_Vol 

,case 
      when Perc_Val_Trans_limt <= 5 then 'Risco_Vol_Ate_5%'
      when Perc_Val_Trans_limt <= 10 then 'Risco_Vol_Ate_10%'
      when Perc_Val_Trans_limt <= 15 then 'Risco_Vol_Ate_15%'
      when Perc_Val_Trans_limt <= 25 then 'Risco_Vol_Ate_25%'
      when Perc_Val_Trans_limt > 25 then 'Risco_Vol_Maior_25%'
      else 'NC'
End as Flag_Risco_Limit_Val

from base_Final_Crivo_Limite_1 limt
where customer_id is not null

;


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Tran_CBK_Cliente` AS 

with

Base_TPV_Com_CBK as (
select
p.customer_id
,cl.CPF_Cliente
,p.order_id
,ord.store_id
,Flag_Trusted
,MotivoBloqueio
,Faixa_Idade
,Flag_TempodeConta
,RegiaoCliente
,Flag_Perfil
,case when p.order_id = cbk.order_id and pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET","GOOGLE_PAY") then 'Contestado' else 'NaoContestado' end as Flag_Contestacao
,case when pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET","GOOGLE_PAY") then 1 else 0 end as Qtd_TPV_PayPal
,case when pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET","GOOGLE_PAY") then pt.transaction_value else 0 end as TPV_PayPal
,Sum(pt.transaction_value) as TPV
,count(pt.transaction_value) as QtdTransacao


FROM `eai-datalake-data-sandbox.payment.payment` p
left join `eai-datalake-data-sandbox.payment.payment_instrument` pi on p.id = pi.id
left join `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
left join `eai-datalake-data-sandbox.core.orders` ord on p.order_id = ord.uuid
join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`  cl on cl.CustomerID = p.customer_id
left join ( select
            distinct
            *
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` 
            where Reason = 'Fraud'and Status = 'Open' and Kind = 'Chargeback'
          ) cbk on p.order_id = cbk.order_id
join (
          select
          distinct
          a.uuid as store_id
          FROM `eai-datalake-data-sandbox.backoffice.store` a
          join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id
          join `eai-datalake-data-sandbox.loyalty.tblPontoVenda` c on c.CNPJ = left(a.document,12)
          join `eai-datalake-data-sandbox.maps.store_place_details` post on post.document = a.document
          where a.type = 'POS') Post on Post.store_id = ord.store_id
where 
date(pt.created_at) >= current_date - 180
and pt.transaction_value > 0
and pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET","GOOGLE_PAY")
--and p.customer_id = 'CUS-e7ef6b1d-67ae-4933-aba4-d2f77d851594'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13

), Base_TPV_Com_CBK_2 as (
select
	
customer_id
,order_id
,store_id
,Flag_Trusted
,MotivoBloqueio
,Faixa_Idade
,Flag_TempodeConta
,RegiaoCliente
,Flag_Perfil
,if((a.Flag_Contestacao = "Contestado"), a.TPV_PayPal, 0) AS Vl_Contestado
,if((a.Flag_Contestacao = "Contestado"),1, 0) AS Qtd_Contestado

,sum(TPV_PayPal) as TPV_PayPal
,sum(Qtd_TPV_PayPal) as Qtd_TPV_PayPal

from Base_TPV_Com_CBK a
group by 1,2,3,4,5,6,7,8,9,10,11

),Base_LojaPosto_Dist as (

select
a.customer_id
,Count(distinct a.store_id) as Qtd_LojaPosto_Dist
,sum(a.Qtd_Contestado) as Qtd_Contestado
,sum(a.Vl_Contestado) as Vl_Contestado
from Base_TPV_Com_CBK_2 a
where Qtd_Contestado = 1
group by 1 order by 2 desc

), base_final_crivo_client_Cbk as (
select
limt.customer_id
,limt.store_id
,limt.Flag_Trusted
,limt.MotivoBloqueio
,limt.Faixa_Idade
,limt.Flag_TempodeConta
,limt.RegiaoCliente
,limt.Flag_Perfil
,Cl_Trigados.Vol_Perc_Exposicao_Cbk
,Cl_Trigados.Vl_Perc_Exposicao_Cbk
,sum(Vl_Contestado) as Vl_Contestado
,sum(Qtd_Contestado) as Qtd_Contestado
,sum(TPV_PayPal) as TPV_PayPal
,sum(Qtd_TPV_PayPal) as Qtd_TPV_PayPal
from Base_TPV_Com_CBK_2 limt
left join (
        select
        customer_id
        ,store_id
        ,Flag_Trusted
        ,MotivoBloqueio
        ,Faixa_Idade
        ,Flag_TempodeConta
        ,RegiaoCliente
        ,Flag_Perfil
        ,(Qtd_Contestado/Qtd_TPV_PayPal)*100 as Vol_Perc_Exposicao_Cbk
        ,(Vl_Contestado/TPV_PayPal)*100 as Vl_Perc_Exposicao_Cbk  

        from Base_TPV_Com_CBK_2
        where Vl_Contestado >0
        
)Cl_Trigados on Cl_Trigados.customer_id = limt.customer_id
group by 1,2,3,4,5,6,7,8,9,10
order by 1

)
select
a.customer_id
,a.store_id
,a.Flag_Trusted
,a.MotivoBloqueio
,a.Faixa_Idade
,a.Flag_TempodeConta
,a.RegiaoCliente
,a.Flag_Perfil
,case 
      when cbk.Qtd_Contestado = 3 then 'Posto com 3 Contestacao'
      when cbk.Qtd_Contestado <= 5 and cbk.Vl_Contestado <=300  then 'Posto ate 5 Contestacao ate 300 reais Contestado'
      when cbk.Qtd_Contestado <= 5 and cbk.Vl_Contestado <=600 and cbk.Qtd_LojaPosto_Dist <=3 then 'Posto ate 5 Contestacao ate 600 reais Contestado e ate 3 postos distintos'
      when cbk.Qtd_Contestado <= 5 and cbk.Vl_Contestado <=600 and cbk.Qtd_LojaPosto_Dist <=10 then 'Posto ate 5 Contestacao ate 600 reais Contestado e ate 10 postos distintos'
      when cbk.Qtd_Contestado > 5 and cbk.Vl_Contestado >600 and cbk.Qtd_LojaPosto_Dist >10 then 'Posto ate 5 Contestacao maior 600 reais Contestado e ,mais de 10 postos distintos'
      else 'NaoAvaliado'
End as Flag_Risco_Cliente


,sum(a.Vl_Contestado) as Vl_Contestado
,sum(a.Qtd_Contestado) as Qtd_Contestado
,sum(a.TPV_PayPal) as TPV_PayPal
,sum(a.Qtd_TPV_PayPal) as Qtd_TPV_PayPal
from base_final_crivo_client_Cbk a
left join Base_LojaPosto_Dist cbk on cbk.customer_id = a.customer_id
group by 1,2,3,4,5,6,7,8,9
;



CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Tran_Limit_Postos` AS 

with

base_trans_limit as (

SELECT
p.order_id
,ord.store_id
,case when pt.transaction_value = 300 then 1 else 0 end as Qtd_Tran_Limite
,Sum(IF(pt.transaction_value = 300, pt.transaction_value, 0)) as Vl_Tran_Limite
,Sum(pt.transaction_value) as TPV
,count(pt.transaction_value) as QtdTransacao
FROM `eai-datalake-data-sandbox.payment.payment` p
left join `eai-datalake-data-sandbox.payment.payment_instrument` pi on p.id = pi.id
left join `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
left join `eai-datalake-data-sandbox.core.orders` ord on p.order_id = ord.uuid
where
date(pt.created_at) >= current_date - 180
and pt.transaction_value > 0
and pt.payment_method in ("CASH")
group by 1,2,3
), Base_final_Crivo_Limite as (
select
limt.store_id
,Sum(limt.TPV) as TPV
,Sum(limt.QtdTransacao) as QtdTransacao
from base_trans_limit limt
group by 1
), base_Final_Crivo_Limite_1 as (
select
limt.store_id
,TPV
,QtdTransacao
,Post_Trigados.Vl_Tran_Limite
,Post_Trigados.Qtd_Tran_Limite
,(Post_Trigados.Qtd_Tran_Limite/limt.QtdTransacao)*100 as Perc_Vol_Trans_limt
,(Post_Trigados.Vl_Tran_Limite/limt.TPV)*100 as Perc_Val_Trans_limt

from Base_final_Crivo_Limite limt
join (
  select * from (
          with
          base_Analise as (
          select
          store_id
          ,sum(Qtd_Tran_Limite) as Qtd_Tran_Limite
          ,sum(Vl_Tran_Limite) as Vl_Tran_Limite
          from base_trans_limit
          where Qtd_Tran_Limite >0
          group by 1
          )
          select
          *
          from base_Analise))Post_Trigados on Post_Trigados.store_id = limt.store_id
)
select
store_id
,TPV
,QtdTransacao
,Vl_Tran_Limite
,Qtd_Tran_Limite
,Perc_Vol_Trans_limt
,Perc_Val_Trans_limt
,case 
      when Perc_Vol_Trans_limt <= 5 then 'Risco_Vol_Ate_5%'
      when Perc_Vol_Trans_limt <= 10 then 'Risco_Vol_Ate_10%'
      when Perc_Vol_Trans_limt <= 15 then 'Risco_Vol_Ate_15%'
      when Perc_Vol_Trans_limt <= 25 then 'Risco_Vol_Ate_25%'
      when Perc_Vol_Trans_limt > 25 then 'Risco_Vol_Maior_25%'
      else 'NC'
End as Flag_Risco_Limit_Vol

,case 
      when Perc_Val_Trans_limt <= 5 then 'Risco_Vol_Ate_5%'
      when Perc_Val_Trans_limt <= 10 then 'Risco_Vol_Ate_10%'
      when Perc_Val_Trans_limt <= 15 then 'Risco_Vol_Ate_15%'
      when Perc_Val_Trans_limt <= 25 then 'Risco_Vol_Ate_25%'
      when Perc_Val_Trans_limt > 25 then 'Risco_Vol_Maior_25%'
      else 'NC'
End as Flag_Risco_Limit_Val

from base_Final_Crivo_Limite_1
where store_id is not null



;

--------------------------- Levantamento Postos Criticos  -------------------
-- analise volume transacional contestadas
-- analise Valor transacional contestadas
------------------------------------------------------------------------------


-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_CBK_Postos`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_CBK_Postos` AS 

with

Base_TPV_Com_CBK as (
select
p.order_id
,post.store_id
,case when p.order_id = cbk.order_id and pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET","GOOGLE_PAY") then 'Contestado' else 'NaoContestado' end as Flag_Contestacao
,case when pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET","GOOGLE_PAY") then 1 else 0 end as Qtd_TPV_PayPal
,case when pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET","GOOGLE_PAY") then pt.transaction_value else 0 end as TPV_PayPal
,Sum(pt.transaction_value) as TPV
,count(pt.transaction_value) as QtdTransacao

FROM `eai-datalake-data-sandbox.payment.payment` p
left join `eai-datalake-data-sandbox.payment.payment_instrument` pi on p.id = pi.id
left join `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
left join `eai-datalake-data-sandbox.core.orders` ord on p.order_id = ord.uuid
left join ( select
            distinct
            *
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` 
            where Reason = 'Fraud'and Status = 'Open' and Kind = 'Chargeback'
          ) cbk on p.order_id = cbk.order_id
    join (
          SELECT 
          distinct
          a.uuid as store_id
          FROM `eai-datalake-data-sandbox.backoffice.store` a
          join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id
          join `eai-datalake-data-sandbox.loyalty.tblPontoVenda` c on c.CNPJ = left(a.document,12)
          join `eai-datalake-data-sandbox.maps.store_place_details` post on post.document = a.document
          where a.type = 'POS') Post on Post.store_id = ord.store_id
where 
date(pt.created_at) >= current_date - 180
and pt.transaction_value > 0
and pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET","GOOGLE_PAY")
group by 1,2,3,4,5
), Base_TPV_Com_CBK_2 as (
select
	
order_id
,store_id
,Flag_Contestacao
,sum(if((a.Flag_Contestacao = "Contestado"), a.TPV_PayPal, 0)) AS Vl_Contestado
,sum(if((a.Flag_Contestacao = "Contestado"),a.QtdTransacao, 0)) AS Qtd_Contestado
,sum(Qtd_TPV_PayPal) as Qtd_TPV_PayPal
,sum(TPV_PayPal) as TPV_PayPal

from Base_TPV_Com_CBK a
group by 1,2,3

), base_final_Crivo_CBK as (
select 
*
from (
      with base_Postos_Criticos as (
      select 
      store_id
      ,Sum(Qtd_Contestado) as Qtd_Contestado
      ,Sum(Qtd_TPV_PayPal) as Qtd_TPV_PayPal
      ,Sum(Vl_Contestado) as Vl_Contestado_cal
      ,Sum(TPV_PayPal) as TPV_PayPal_cal 
      FROM Base_TPV_Com_CBK_2 group by 1
      )
      select
      store_id
      ,Vl_Contestado_cal
      ,TPV_PayPal_cal
      ,Qtd_Contestado
      ,Qtd_TPV_PayPal
      ,(Qtd_Contestado/Qtd_TPV_PayPal)*100 as Vol_Perc_Exposicao_Cbk
      ,(Vl_Contestado_cal/TPV_PayPal_cal)*100 as Vl_Perc_Exposicao_Cbk  
      from base_Postos_Criticos) 
      where Vl_Contestado_cal > 0 order by 4 desc

)
select
store_id
,Vl_Contestado_cal
,TPV_PayPal_cal
,Qtd_Contestado
,Vol_Perc_Exposicao_Cbk
,Qtd_TPV_PayPal
,Vl_Perc_Exposicao_Cbk
,case 
      when Vol_Perc_Exposicao_Cbk <= 5 then 'Risco_Vol_Ate_5%'
      when Vol_Perc_Exposicao_Cbk <= 10 then 'Risco_Vol_Ate_10%'
      when Vol_Perc_Exposicao_Cbk <= 15 then 'Risco_Vol_Ate_15%'
      when Vol_Perc_Exposicao_Cbk <= 25 then 'Risco_Vol_Ate_25%'
      when Vol_Perc_Exposicao_Cbk > 25 then 'Risco_Vol_Maior_25%'
End as Flag_Risco_CBK_Vol

,case 
      when Vl_Perc_Exposicao_Cbk <= 5 then 'Risco_Vol_Ate_5%'
      when Vl_Perc_Exposicao_Cbk <= 10 then 'Risco_Vol_Ate_10%'
      when Vl_Perc_Exposicao_Cbk <= 15 then 'Risco_Vol_Ate_15%'
      when Vl_Perc_Exposicao_Cbk <= 25 then 'Risco_Vol_Ate_25%'
      when Vl_Perc_Exposicao_Cbk > 25 then 'Risco_Vol_Maior_25%'
End as Flag_Risco_CBK_Val

from base_final_Crivo_CBK

;
------------------------- Perfil Movimentação local transação Loja/Posto -------------------------

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_Movimentacao_Posto` AS 

with

base_cbk_tpv as (
select
distinct
    tranx_d.order_id	
    ,tranx_d.customer_id	
    ,ord.store_id as Cod_Loja	
    ,substring(tranx_d.order_code,1,STRPOS(tranx_d.order_code,'-')) as order_code 	
    ,ord.pdv_token	
    ,tranx.`payment_method`
    ,tranx.transaction_value	
    ,locPost.UF as UF_Posto
    ,locPost.latitude  as latitude_Posto
    ,locPost.longitude as longitude_Posto
    ,locPost.latitude||locPost.longitude as  latitude_longitude_Posto
    ,left(ord.latitude,7) as  latitude_Tranx
    ,left(ord.longitude,7) as  longitude_Tranx
    ,left(ord.latitude,7) ||left(ord.longitude,7) as  latitude_longitude_Tranx
    ,locTranx.UF as UF_Tranx
    ,(cast(left(ord.latitude,7) as numeric) - cast(if(locPost.latitude = '', null, locPost.latitude) as numeric)) - (cast(left(ord.longitude,7)as numeric) - cast(if(locPost.longitude = '', null, locPost.longitude) as numeric)) as dif_geral
    ,case when tranx_d.order_id = cbk.order_id then 'Contestado' else 'NaoContestado' end as Flag_Contestacao

from `eai-datalake-data-sandbox.payment.payment_transaction`  tranx
join `eai-datalake-data-sandbox.payment.payment`              tranx_d   on tranx_d.id = tranx.payment_id
join `eai-datalake-data-sandbox.core.orders`             ord       on ord.uuid = tranx_d.order_id

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
            *
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` 
            where Reason = 'Fraud'and Status = 'Open' and Kind = 'Chargeback'
          ) cbk on tranx_d.order_id = cbk.order_id

WHERE 
date(tranx.created_at) >= current_date - 180
and tranx.status in ('AUTHORIZED','COMPLETED','SETTLEMENT','COMPLETE')
--and tranx.`payment_method` in ('CREDIT_CARD','DEBIT_CARD')
)
select
distinct
cbk_tpv.*
,IF(cbk_tpv.Flag_Contestacao = 'Contestado' , 1, 0) as Qtd_Contestacao
,case
      when cbk_tpv.latitude_longitude_Posto = cbk_tpv.latitude_longitude_Tranx then '01_Transacao_no_Posto'
      when cbk_tpv.latitude_longitude_Posto = cbk_tpv.latitude_longitude_Tranx  
      or substring(cast(cbk_tpv.dif_geral as string),1,5)  
      in ('-0.026','-0.087','0.008','0.029','-0.001','-0.000','-0.00','0.001','-0.01','0.000','0.002','-0.002','0.003','-0.003') then '01_Transacao_no_Posto'
      when (cast(substring(cast(cbk_tpv.dif_geral as string),1,5) as numeric) between -1.000 and 1.000) or cbk_tpv.UF_Tranx = cbk_tpv.UF_Posto  then '02_Transacao_Proximo_Posto'
      when cast(substring(cast(cbk_tpv.dif_geral as string),1,5) as numeric) < -1.000 
      or cast(substring(cast(cbk_tpv.dif_geral as string),1,5) as numeric) > 1.000 
      or cbk_tpv.UF_Tranx <> cbk_tpv.UF_Posto   then '03_Fora_Posto'
      when cbk_tpv.UF_Tranx is null then '04_Transacao_nao_localizada'
      else '04_Verificar' end as Flag_Local_Posto_Tranx
from base_cbk_tpv cbk_tpv

;

------------------------- Perfil Loja/Posto -------------------------
-- Avaliação de dados tabelas auxiliares 180 dias 

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_Posto_Perfil`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_Posto_Perfil` AS 

Select 
distinct
a.uuid as store_id
,a.name as Nome_loja
,a.document as CNPJ_CPF
,a.type as tipo_loja
,b.neighborhood as BairroPosto
,b.city as cidade
,b.state as UF
,case when opt.opt_in_id = 6 then 'GojetaVip' else 'NA' end as Flag_GojetaVip
,CASE
  WHEN b.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
  WHEN b.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
  WHEN b.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
  WHEN b.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
  WHEN b.state IN ('SC','PR','RS') THEN 'SUL'
ELSE 'NAOINDENTIFICADO'  END AS RegiaoPosto
,left(b.latitude,7) as latitude
,left(b.longitude,7) as longitude
,vip.Vip as Codigo_Vip
,vip.Nome as NomeVip_QPOS
,vip.CPF as CpfVip_QPOS
,Criv_LimtPostos.Flag_Risco_Limit_Vol as Post_Limt_Vol
,Criv_LimtPostos.Flag_Risco_Limit_Val as Post_Limt_Val
,Criv_CbkPostos.Flag_Risco_CBK_Vol as Post_Cbk_Vol
,Criv_CbkPostos.Flag_Risco_CBK_Val as Post_Cbk_Val

,case 
      when Mov.Perc_Trans_ForaPosto is null then 'NaoAvaliado'
      when Mov.Perc_Trans_ForaPosto <= 0 then 'NaoAvaliado'
      when Mov.Perc_Trans_ForaPosto <= 5 then 'Até_5%'
      when Mov.Perc_Trans_ForaPosto <= 10 then 'Até_10%'
      when Mov.Perc_Trans_ForaPosto <= 30 then 'Até_30%'
      when Mov.Perc_Trans_ForaPosto <= 50 then 'Até_50%'
      when Mov.Perc_Trans_ForaPosto > 50 then 'Maior_50%'
End as Flag_Risco_Local_Tran

FROM `eai-datalake-data-sandbox.backoffice.store` a
join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id 
left join `eai-datalake-data-sandbox.backoffice.category` cat on cat.id = a.category_id
left join `eai-datalake-data-sandbox.backoffice.store_opt_in` opt on opt.store_id = a.id
left join `eai-datalake-data-sandbox.core.orders` ord on a.uuid = ord.store_id
left join  `eai-datalake-data-sandbox.loyalty.tblAbasteceAiV2TransacaoControle`  tokenvip on tokenvip.pdvToken = ord.pdv_token
left join `eai-datalake-data-sandbox.loyalty.tblParticipantes`      vip on vip.ParticipanteID = tokenvip.VipParticipanteId
left join (select distinct * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Tran_Limit_Postos` ) Criv_LimtPostos on Criv_LimtPostos.store_id = ord.store_id
left join (select distinct * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_CBK_Postos` ) Criv_CbkPostos on Criv_CbkPostos.store_id = ord.store_id
left join (
            with

            Base_Perc_Mov_foraPosto as (

            select 
            distinct 
            Cod_Loja
            ,sum(if((Flag_Local_Posto_Tranx = "03_Fora_Posto"), 1, 0)) as Qtd_Trans_foraPosto
            ,Count(order_id ) as Qtd_Trans
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_Movimentacao_Posto`
            group by 1
            )
            select
            Cod_Loja
            ,Qtd_Trans_foraPosto
            ,Qtd_Trans
            ,(Qtd_Trans_foraPosto/Qtd_Trans)*100 as Perc_Trans_ForaPosto

            from Base_Perc_Mov_foraPosto
            order by 4 desc 
) Mov on Mov.Cod_Loja = ord.store_id
order by 1


