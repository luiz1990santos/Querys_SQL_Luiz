--=========================================================================--
-- Todas analise realizadas em 180 dias
--=========================================================================--
--tb_base_Posto_Perfil
--------------------------- Levantamento Postos -------------------
-- Analitico dos postos em 180 dias
------------------------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto` where Store_id_Ord ='STO-63f43608-8d9d-4366-b1d7-4574d7dedb9b'



CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto` AS 

select
distinct

p.created_at 
,date(p.created_at) as Dt_Trans
,FORMAT_DATE("%Y%m",p.created_at)as Safra_Trans
,case 
  when EXTRACT(HOUR FROM p.created_at) in (0,1,2,3,4,5,6) then '01 Madrugada'
  when EXTRACT(HOUR FROM p.created_at) in (7,8,9,10,11,12) then '02 Manhã'
  when EXTRACT(HOUR FROM p.created_at) in (13,14,15,16,17,18) then '03 Tarde'
  when EXTRACT(HOUR FROM p.created_at) in (19,20,21,22,23) then '04 Noite'
else 'NA' end as Periodo_Tranx
,case
  when DATE_DIFF(date(current_date),date(p.created_at), Month) = 0 then 'M0'
  when DATE_DIFF(date(current_date),date(p.created_at), Month) = 1 then 'M-1'
  when DATE_DIFF(date(current_date),date(p.created_at), Month) = 2 then 'M-2'
  when DATE_DIFF(date(current_date),date(p.created_at), Month) = 3 then 'M-3'
  when DATE_DIFF(date(current_date),date(p.created_at), Month) = 4 then 'M-4'
  when DATE_DIFF(date(current_date),date(p.created_at), Month) = 5 then 'M-5'
  when DATE_DIFF(date(current_date),date(p.created_at), Month) = 6 then 'M-6'
else 'M+6' end as Flag_Filt_Per
,p.order_id
,pt.gateway_id
,p.status as StatusPayment
,pt.status as Status_Paym_Inst
,ord.order_status as Status_Trans_Ord
,pi.method as Metodo_Pgato
,pt.transaction_value as Vl_Trans_Inst
,ord.order_value as Vl_Trans_Ord
,case when p.order_id = cbk.order_id then pt.transaction_value else 0 end as Flag_VlContestado
,case when p.order_id = cbk.order_id then 'Contestado' else 'NaoContestado' end as Flag_Contestacao
,ord.cashback_percentage as Perc_Cashback_Ord
,ord.cashback_value/100 as Vl_Cashback_Ord
,(pt.transaction_value*pi.commission_percentage)/100 as Comissao_Paga
--,pi.commission_percentage as Perc_Comissao
,p.sales_channel as Canal
,case
  when pt.payment_method = 'CASH' then 'Dinheiro'
  when pt.payment_method = 'CREDIT_CARD' then 'Credito'
  when pt.payment_method = 'DEBIT_CARD' then 'Debito'
  when pt.payment_method = 'BALANCE' then 'SaldoConta'
  when pt.payment_method = 'COUPON' then 'Cupom'
  when pt.payment_method = "DIGITAL_WALLET" then 'CarteiraDigital PayPal'
  when pt.payment_method = 'GOOGLE_PAY' then 'GooglePay'
else pt.payment_method end as Pagamento

,Case
    When (pt.transaction_value) >=0   and (pt.transaction_value) <=50 	Then '01 000 a 050' 
    When (pt.transaction_value) >50    and (pt.transaction_value) <=100 	Then '02 051 a 100'
    When (pt.transaction_value) >100    and (pt.transaction_value) <=150 	Then '03 101 a 150'
    When (pt.transaction_value) >150    and (pt.transaction_value) <=200 	Then '04 151 a 200'
    When (pt.transaction_value) >200    and (pt.transaction_value) <=250 	Then '05 201 a 250'
    When (pt.transaction_value) >250    and (pt.transaction_value) <300 	Then '06 251 a 299'
    When (pt.transaction_value) = 300 	Then '07 Limite'
    When (pt.transaction_value) >300    and (pt.transaction_value) <=350 	Then '08 301 a 350'
    When (pt.transaction_value) >350    and (pt.transaction_value) <=400 	Then '09 351 a 400'
    When (pt.transaction_value) >400    and (pt.transaction_value) <=450 	Then '10 401 a 450'
    When (pt.transaction_value) >450    and (pt.transaction_value) <=500 	Then '11 451 a 500'
    When (pt.transaction_value) >500    and (pt.transaction_value) <=550 	Then '12 501 a 550'
    When (pt.transaction_value) >550    and (pt.transaction_value) <600	Then '13 551 a 599'
    When (pt.transaction_value) = 600	Then '14 600'
    When (pt.transaction_value) >600    and (pt.transaction_value) <=700 	Then '15 601 a 700'
    When (pt.transaction_value) >700    and (pt.transaction_value) <=800 	Then '16 701 a 800'
    When (pt.transaction_value) >800    and (pt.transaction_value) <=900 	Then '17 801 a 900'
    When (pt.transaction_value) >900    and (pt.transaction_value) <=1000 	Then '18 901 a 1000'
    When (pt.transaction_value) > 1000  and (pt.transaction_value) <=3000 Then '19 1001 a 3000'
    When (pt.transaction_value) > 3000  and (pt.transaction_value) <=5000 Then '20 3001 a 5000'
    When (pt.transaction_value) > 5000  and (pt.transaction_value) <=7000 Then '21 5001 a 7000'
    When (pt.transaction_value) > 7000  and (pt.transaction_value) <=9000 Then '22 7001 a 9000'
    When (pt.transaction_value) > 9000  and (pt.transaction_value) <=11000 Then '23 9001 a 11000'
    When (pt.transaction_value) > 11000 and (pt.transaction_value) <=13000 Then '24 11001 a 13000'
    When (pt.transaction_value) > 13000 and (pt.transaction_value) <=15000 Then '25 13001 a 15000'
    When (pt.transaction_value) > 15000 and (pt.transaction_value) <=17000 Then '26 15001 a 17000'
    When (pt.transaction_value) > 17000 and (pt.transaction_value) <=19000 Then '27 17001 a 19000'
    When (pt.transaction_value) > 19000 and (pt.transaction_value) <=20000 Then '28 19001 a 20000'
    When (pt.transaction_value) > 20000 Then '29 20000>' 
  End as Faixa_Valores
,p.customer_id
,cl.CPF_Cliente
,cl.Dt_Abertura
,cl.StatusConta
,cl.Flag_Trusted
,cl.MotivoBloqueio
,cl.Faixa_Idade
,cl.Flag_TempodeConta
,cl.Flag_Perfil
,cl.UF_Cliente

,p.order_code
,ord.pdv_token
,ord.store_id as Store_id_Ord

,p.store_id
,Post.PontoVendaID
,Post.store_id as store_id_loja
,Post.Razao
,Post.NomeFantasia
,Post.CNPJ_CPF as CNPJ_Loja_Comp
,Post.CNPJ as CNPJ_Loja
,Post.End_Post
,ord.latitude as latitude_Tran
,ord.longitude as longitude_Tran
,Post.latitude_Post
,Post.longitude_Post
,Post.cidade_Post
,Post.UF_Post
,Post.RegiaoPosto
,vip.Vip as Codigo_Vip
,vip.Nome as NomeVip_QPOS
,vip.CPF as CpfVip_QPOS


FROM `eai-datalake-data-sandbox.payment.payment` p
join `eai-datalake-data-sandbox.payment.payment_instrument` pi on p.id = pi.id
join `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
join `eai-datalake-data-sandbox.core.orders` ord on p.order_id = ord.uuid
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` cl on cl.CustomerID = p.customer_id
left join  `eai-datalake-data-sandbox.loyalty.tblAbasteceAiV2TransacaoControle`  tokenvip on tokenvip.pdvToken = ord.pdv_token
left join `eai-datalake-data-sandbox.loyalty.tblParticipantes`      vip on vip.ParticipanteID = tokenvip.VipParticipanteId
left join ( select
            distinct
            *
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` 
            where Reason = 'Fraud'and Status = 'Open' and Kind = 'Chargeback'
          ) cbk on p.order_id = cbk.order_id
Left join (
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
        ,CASE
          WHEN b.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
          WHEN b.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
          WHEN b.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
          WHEN b.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
          WHEN b.state IN ('SC','PR','RS') THEN 'SUL'
        ELSE 'NAOINDENTIFICADO'  END AS RegiaoPosto
        ,left(b.latitude,8) as latitude_Post
        ,left(b.longitude,8) as longitude_Post

      FROM `eai-datalake-data-sandbox.backoffice.store` a
      join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id
      join `eai-datalake-data-sandbox.loyalty.tblPontoVenda` c on c.CNPJ = left(a.document,12)
      join `eai-datalake-data-sandbox.maps.store_place_details` post on post.document = a.document
      where a.type = 'POS'
) Post on Post.store_id = ord.store_id
where pt.status IN ("AUTHORIZED","SETTLEMENT","COMPLETED")
and date(pt.created_at) >= current_date - 180
--and ord.store_id  = 'STO-63f43608-8d9d-4366-b1d7-4574d7dedb9b'



;

-------------------------- Crivo Analise --------------------------
-- Analise e aplicação dos critérios
------------------------------------------------------------------------------
/*
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Crivo` order by Store_id_Ord

*/


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Crivo` AS 


select
distinct

Bd_An.created_at
,Bd_An.Dt_Trans
,Bd_An.Safra_Trans
,Bd_An.Periodo_Tranx
,Bd_An.Flag_Filt_Per
,Bd_An.order_id
,Bd_An.gateway_id
,Bd_An.StatusPayment
,Bd_An.Status_Paym_Inst
,Bd_An.Status_Trans_Ord
,Bd_An.Metodo_Pgato
,Bd_An.Vl_Trans_Inst
,Bd_An.Vl_Trans_Ord
,Bd_An.Flag_VlContestado
,Bd_An.Flag_Contestacao
,Bd_An.Perc_Cashback_Ord
,Bd_An.Vl_Cashback_Ord
,Bd_An.Comissao_Paga
,Bd_An.Canal
,Bd_An.Pagamento
,Bd_An.Faixa_Valores
,Bd_An.customer_id
,Bd_An.CPF_Cliente
,Bd_An.Dt_Abertura
,Bd_An.StatusConta
,Bd_An.Flag_Trusted
,Bd_An.MotivoBloqueio
,Bd_An.Faixa_Idade
,Bd_An.Flag_TempodeConta
,Bd_An.Flag_Perfil
,Bd_An.UF_Cliente
,Bd_An.order_code
,Bd_An.pdv_token
,Bd_An.Store_id_Ord
,Bd_An.store_id
,Bd_An.PontoVendaID
,Bd_An.store_id_loja
,Bd_An.Razao
,Bd_An.NomeFantasia
,Bd_An.CNPJ_Loja_Comp
,Bd_An.CNPJ_Loja
,Bd_An.End_Post
,Bd_An.latitude_Tran
,Bd_An.longitude_Tran
,Bd_An.latitude_Post
,Bd_An.longitude_Post
,Bd_An.cidade_Post
,Bd_An.UF_Post
,Bd_An.RegiaoPosto
,Bd_An.Codigo_Vip
,Bd_An.NomeVip_QPOS
,Bd_An.CpfVip_QPOS
,perf_post.Flag_Risco_Local_Tran

,Criv_LimtPostos.Flag_Risco_Limit_Vol as Post_Limt_Vol
,Criv_LimtPostos.Flag_Risco_Limit_Val as Post_Limt_Val

,Criv_CbkPostos.Flag_Risco_CBK_Vol as Post_Cbk_Vol
,Criv_CbkPostos.Flag_Risco_CBK_Val as Post_Cbk_Val

,Criv_LimtCl.Flag_Risco_Limit_Vol as Cl_Limt_Vol
,Criv_LimtCl.Flag_Risco_Limit_Val as Cl_Limt_Val

,Criv_CbktCl.Flag_Risco_Cliente as Cl_Cbk_Crivo

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto` Bd_An 
left join (select distinct * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Tran_Limit_Postos` ) Criv_LimtPostos on Criv_LimtPostos.store_id = Bd_An.Store_id_Ord
left join (select distinct * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_CBK_Postos` ) Criv_CbkPostos on Criv_CbkPostos.store_id = Bd_An.Store_id_Ord
left join (select distinct * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Tran_Limit_Cliente` ) Criv_LimtCl on Criv_LimtCl.customer_id = Bd_An.customer_id
left join (select distinct * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Tran_CBK_Cliente` where Flag_Risco_Cliente not in ('','ND') ) Criv_CbktCl on Criv_CbktCl.customer_id = Bd_An.customer_id
left join (select distinct * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_Posto_Perfil`) perf_post on perf_post.store_id = Bd_An.Store_id_Ord


;


-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Visao1`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Visao1` AS 

select 
distinct
Store_id_Ord
,Razao
,Safra_Trans
,Pagamento
,Sum(case when Metodo_Pgato in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET",'GOOGLE_PAY') and Flag_Contestacao = 'Contestado' then 1 else 0 end) as Qtd_Contestado
,Sum(Flag_VlContestado) as Vl_Contestado
,Sum(case when Metodo_Pgato in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET",'GOOGLE_PAY') then 1 else 0 end) as Qtd_TPV_PayPal
,Sum(case when Metodo_Pgato in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET",'GOOGLE_PAY') then Vl_Trans_Inst else 0 end) as TPV_PayPal

,sum(Vl_Trans_Inst) as TPV_Total
,Count(order_id) as QtdTransacao_Total

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Crivo` a


--where Store_id_Ord = 'STO-9a944fb2-f958-49cb-82b3-61e82a7f8540'
group by 1,2,3,4
;

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Visao1_1`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Visao1_1` AS 

select 
distinct
Store_id_Ord
,case when StatusPosto.STORE_ID = a.Store_id_Ord and StatusPosto.SITUACAO_BLOQUEIO in ('BLOQUEIO MANTIDO','DESBLOQUEADO PARCIALMENTE','BLOQUEADO') then 'Sim' else 'Não' end as Flag_Bloqueado
,case when StatusPosto.STORE_ID = a.Store_id_Ord then StatusPosto.FLAG_MOTIVO else 'N/A' end as Flag_Motivo
,Count(Store_id_Ord) as qtd

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Crivo` a
left join (
          select 
          DATA_DOSSIE
          ,NOME_DO_POSTO
          ,CNPJ
          ,STORE_ID
          ,FLAG_MOD_PAG_BLOQ
          ,FLAG_BLOQUEADO
          ,SITUACAO_BLOQUEIO
          ,DATA_BLOQUEIO
          ,FLAG_MOTIVO
          ,FLAG_OUTROS_MOTIVOS
          ,OBSERVACAO
          ,DATA_UPDATE
          ,RANK_UPDATE
          from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_Postos_Consolidado`
) StatusPosto on StatusPosto.STORE_ID = a.Store_id_Ord
group by 1,2,3

;

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Visao1_1`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Visao1_1` AS 

select 
distinct
Store_id_Ord
,case when StatusPosto.STORE_ID = a.Store_id_Ord and StatusPosto.SITUACAO_BLOQUEIO in ('BLOQUEIO MANTIDO','DESBLOQUEADO PARCIALMENTE','BLOQUEADO') then 'Sim' else 'Não' end as Flag_Bloqueado
,case when StatusPosto.STORE_ID = a.Store_id_Ord then StatusPosto.FLAG_MOTIVO else 'N/A' end as Flag_Motivo
,Count(Store_id_Ord) as qtd

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Crivo` a
left join (
          select 
          DATA_DOSSIE
          ,NOME_DO_POSTO
          ,CNPJ
          ,STORE_ID
          ,FLAG_MOD_PAG_BLOQ
          ,FLAG_BLOQUEADO
          ,SITUACAO_BLOQUEIO
          ,DATA_BLOQUEIO
          ,FLAG_MOTIVO
          ,FLAG_OUTROS_MOTIVOS
          ,OBSERVACAO
          ,DATA_UPDATE
          ,RANK_UPDATE
          from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_Postos_Consolidado`
) StatusPosto on StatusPosto.STORE_ID = a.Store_id_Ord
group by 1,2,3

;

-----------------------------------------------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Visao2`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Visao2` AS 

select 
distinct
Store_id_Ord
,Razao
,Post_Limt_Vol
,Post_Limt_Val
,Post_Cbk_Vol
,Post_Cbk_Val
,Cl_Limt_Vol
,Cl_Limt_Val
,Cl_Cbk_Crivo
,count(distinct Store_id_Ord) as qtdPostos
,count(distinct customer_id) as qtdClientes
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Crivo`
--where Store_id_Ord = 'STO-9a944fb2-f958-49cb-82b3-61e82a7f8540'
group by 1,2,3,4,5,6,7,8,9
order by 1


;
----------------------------------------------------------------------------------------------------------------
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Visao3`


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Visao3` AS 

select 
distinct
Store_id_Ord
,Razao
,RegiaoPosto
,Safra_Trans
,Pagamento
,Periodo_Tranx
,Faixa_Valores
,Codigo_Vip
,NomeVip_QPOS
,Post_Limt_Vol
,Post_Limt_Val
,Post_Cbk_Vol
,Post_Cbk_Val
,Flag_Risco_Local_Tran
,Cl_Limt_Vol
,Cl_Limt_Val
,Cl_Cbk_Crivo
,Sum(case when Metodo_Pgato in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET",'GOOGLE_PAY') and Flag_Contestacao = 'Contestado' then 1 else 0 end) as Qtd_Contestado
,Sum(Flag_VlContestado) as Vl_Contestado
,Sum(case when Metodo_Pgato in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET",'GOOGLE_PAY') then 1 else 0 end) as Qtd_TPV_PayPal
,Sum(case when Metodo_Pgato in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET",'GOOGLE_PAY') then Vl_Trans_Inst else 0 end) as TPV_PayPal

,sum(Vl_Trans_Inst) as TPV_Total
,Count(order_id) as QtdTransacao_Total

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Crivo`
--where Store_id_Ord = 'STO-9a944fb2-f958-49cb-82b3-61e82a7f8540'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
order by 4 desc

;
-------------------------- Analtico dos clientes  --------------------------
-- Ananalitico do clientes - postos criticos
------------------------------------------------------------------------------
/*

select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analitico_Clientes_Postos_Criticos` where Store_id_Ord = 'STO-63f43608-8d9d-4366-b1d7-4574d7dedb9b'

*/


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analitico_Clientes_Postos_Criticos` AS 


select 
distinct
Post_Criv.customer_id
,Post_Criv.CPF_Cliente
,Post_Criv.Dt_Abertura
,Post_Criv.StatusConta
,Post_Criv.Flag_Trusted
,Post_Criv.MotivoBloqueio
,Post_Criv.Faixa_Idade
,Post_Criv.Flag_TempodeConta
,Post_Criv.Flag_Perfil
,Post_Criv.Uf_Cliente
,Post_Criv.Flag_Contestacao
,Post_Criv.Canal
,Post_Criv.Dt_Trans
,Post_Criv.Pagamento
,Post_Criv.Faixa_Valores
,Post_Criv.Store_id_Ord
,Post_Criv.store_id_loja
,Post_Criv.Razao
,Post_Criv.NomeFantasia
,Post_Criv.CNPJ_Loja_Comp
,Post_Criv.End_Post
,Post_Criv.latitude_Tran
,Post_Criv.longitude_Tran
,Post_Criv.latitude_Post
,Post_Criv.longitude_Post
,Post_Criv.UF_Post
,Post_Criv.RegiaoPosto
,Post_Criv.Cl_Cbk_Crivo
,Post_Criv.Codigo_Vip
,Post_Criv.NomeVip_QPOS
,Post_Criv.CpfVip_QPOS
,Post_Criv.Post_Limt_Vol
,Post_Criv.Post_Limt_Val
,Post_Criv.Post_Cbk_Vol
,Post_Criv.Post_Cbk_Val
,Post_Criv.Cl_Limt_Val
,Post_Criv.Cl_Limt_Vol
,post.Flag_Risco_Local_Tran
,count(distinct order_id) as Qtd_Trans
,sum(Post_Criv.Vl_Trans_Inst) as Vl_Trans
,sum(Post_Criv.Flag_VlContestado) as Vl_Contestacao

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Analise_Loja_Posto_Crivo`  Post_Criv
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_Posto_Perfil` post on post.store_id = Post_Criv.store_id_loja

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38

