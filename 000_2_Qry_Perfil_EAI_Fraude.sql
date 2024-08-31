--------------------------- Clientes ------------------------------

------------------- Hitorico Bloqueios -----------------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Historico_Bloqueios`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Historico_Bloqueios` AS 
with
       base_cl as (
              select
              distinct
                     cl.uuid as  CustomerID
                     ,cl.document as CPF_Cliente
                     ,cl.status as StatusConta
                     ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual
                     ,Ev.status as StatusEvento
                     ,ev.observation as MotivoStatus
                     ,ev.sub_classification as SubClassificacao
                     ,ev.sub_classification_obs as Observacao
                     ,ev.user_name as Analista
                     ,ev.event_date as DataStatus
                     ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Ev

              FROM `eai-datalake-data-sandbox.core.customers`             cl
              left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
              left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id
              ), base2 as ( 
 select
    CustomerID,
    CPF_Cliente,
    CONCAT(
            COALESCE(MotivoStatus, ''),
            CASE WHEN MotivoStatus IS NOT NULL AND SubClassificacao IS NOT NULL THEN ' | ' ELSE ' | Sem SubClassificacao' END,
            COALESCE(SubClassificacao, ''),
            CASE WHEN SubClassificacao IS NOT NULL AND Observacao IS NOT NULL THEN ' | ' ELSE ' | Sem Observacao | ' END,
            COALESCE(Observacao, ''),
            CASE WHEN Observacao IS NOT NULL AND Analista IS NOT NULL THEN ' | ' ELSE '' END,
            COALESCE(Analista, '')
    ) AS Historico_Bloqueio,
    DataStatus as Dt_HistoricoBloq, 
    Rank_Ult_Atual
 from base_cl 
where Rank_Ult_Atual >= 2
and StatusEvento = 'BLOCK'
--and CPF_cliente in ('37608291800','03170313762')
              ), base3 as ( 
                
                select 
                      *, 
                      RANK() OVER (PARTITION BY CustomerID ORDER BY Dt_HistoricoBloq desc) AS Rank_bloqueio 
                from base2
) select * from base3 where Rank_bloqueio = 1
;


------------------- Crivo Cadastro Cartão -----------------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_trans_PayPal_Crivo_Cartao`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_trans_PayPal_Crivo_Cartao` AS 

with

base_Cartoes as (
select
card.document as CPF_Cliente
,cl.CustomerID
,count(distinct card.last_four_digits) as qtd_Tetativas
,count(distinct bin.Emissor_do_Banco) as qtd_banco
,count(distinct case when card.status = 'VERIFIED' then card.last_four_digits else null end) as qtd_cartao
,count(distinct card.document) as qtd_cliente

from `eai-datalake-data-sandbox.payment.customer_card` card
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos`  bin on CAST(bin.BIN AS STRING) = card.bin
left join(
select
distinct
cl.uuid as  CustomerID
,cl.document as CPF_Cliente
FROM `eai-datalake-data-sandbox.core.customers`  cl
)cl      on cl.CPF_Cliente = card.document
group by 1,2

)
select 

CPF_Cliente
,CustomerID
,case 
when qtd_Tetativas <= 0 or qtd_Tetativas is null then 'Sem Tentativas'
when qtd_Tetativas <= 4 then FORMAT('%d tentativa(s)', qtd_Tetativas)
when qtd_Tetativas <= 15 then FORMAT('%d tentativa(s)', qtd_Tetativas)
when qtd_Tetativas > 15 then FORMAT('%d tentativa(s)', qtd_Tetativas)
end as Flag_Tetativas
,case 
when qtd_banco <= 0 or qtd_banco is null then 'NC'
when qtd_banco <= 4 then FORMAT('%d banco(s) emissor(es)', qtd_banco)
when qtd_banco <= 15 then FORMAT('%d banco(s) emissor(es)', qtd_banco)
when qtd_banco > 15 then FORMAT('%d banco(s) emissor(es)', qtd_banco)
end as Flag_Bancos
,case 
when qtd_cartao <= 0 or qtd_cartao is null then 'Sem cartão cadastrado'
when qtd_cartao <= 4 then FORMAT('%d Cadastrado(s)', qtd_cartao)
when qtd_cartao <= 15 then FORMAT('%d Cadastrado(s)', qtd_cartao)
when qtd_cartao > 15 then FORMAT('%d Cadastrado(s)', qtd_cartao)
end as Flag_Card

from base_Cartoes

;
------------------- Crivo Biometria cadastrada -----------------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_bio_Capturada`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_bio_Capturada` AS 

with
  base_Bio as (
  SELECT 
  customer_id
  ,status
  ,validation_date
  ,RANK() OVER (PARTITION BY customer_id ORDER BY validation_date desc) AS Rank_Ult_Bio
  FROM `eai-datalake-data-sandbox.core.customer_facial_biometrics` 
  order by 1
  ) select * from base_Bio where Rank_Ult_Bio = 1

;

------------------- Crivo Dados Cadastrais Zaig ----------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_validar_dados_cadastral_Zaig`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_validar_dados_cadastral_Zaig` AS 

with 
  base as (
        select 
        distinct
        Cpf_Cliente
        ,esteira
        ,data_cadastro
        ,score as ScoreZaig
        ,decisao
        --,gps_latitude
        --,gps_longitude
        ,case when indicators like '%Not_validated_email%' then 'EmailNaoValidado' else 'NA' end as Flag_Email_NaoVal
        ,case when indicators like '%Not_validated_phone%' then 'CelularNaoValidado' else 'NA' end as Flag_Celular_NaoVal
        ,case when indicators like '%name_and_email_and_mother_name_full_uppercase%' then 'CaixaAltaNomeMae' else 'NA' end as Flag_NomeMae_CaixaAlta

        ,RANK() OVER (PARTITION BY cpf ORDER BY data_cadastro, natural_person_id  desc) AS Rank_Ult_Decisao

        from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Consolidado` 
        where
        decisao = "automatically_approved"
        ) 
        select 
        * 
        from base 
        where Rank_Ult_Decisao = 1
        --and Cpf_Cliente = '56701100805'


;


------------------- Crivo Transações Limite 300 clientes ----------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_tran_limt_cliente`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_tran_limt_cliente` AS 


with

base_trans_limit as (

SELECT
p.customer_id
,p.order_id
,ord.store_id

,case when pt.transaction_value = 300 then 1 else 0 end as Qtd_Tran_Limite
,Sum(IF(pt.transaction_value = 300, pt.transaction_value, 0)) as Vl_Tran_Limite
,Sum(pt.transaction_value) as TPV
,count(pt.transaction_value) as QtdTransacao
FROM `eai-datalake-data-sandbox.payment.payment` p
left join `eai-datalake-data-sandbox.payment.payment_instrument` pi on p.id = pi.id
left join `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
left join `eai-datalake-data-sandbox.core.orders` ord on p.order_id = ord.uuid
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
and pt.status IN ("AUTHORIZED","SETTLEMENT","COMPLETED")
--and p.customer_id = 'CUS-4845e325-150a-4b61-93f4-6e9b87068bb1'
group by 1,2,3,4
), Base_final_Crivo_Limite as (
select
limt.customer_id

,Sum(limt.TPV) as TPV
,Sum(limt.QtdTransacao) as QtdTransacao
from base_trans_limit limt
group by 1
), base_Final_Crivo_Limite_1 as (
select
limt.customer_id
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

------------------- Crivo Transações exposição CBK ----------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Tpv_Cbk_Clientes`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Tpv_Cbk_Clientes` AS 

with
Base_TPV_Com_CBK as (
select
p.customer_id
,cl.document as CPF_Cliente
,p.order_id
,ord.store_id
,case when p.order_id = cbk.order_id and pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET","GOOGLE_PAY","APPLE_PAY") then 'Contestado' else 'NaoContestado' end as Flag_Contestacao
,case when pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET","GOOGLE_PAY","APPLE_PAY") then 1 else 0 end as Qtd_TPV_PayPal
,case when pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET","GOOGLE_PAY","APPLE_PAY") then pt.transaction_value else 0 end as TPV_PayPal
,Sum(pt.transaction_value) as TPV
,count(pt.transaction_value) as QtdTransacao


FROM `eai-datalake-data-sandbox.payment.payment` p
left join `eai-datalake-data-sandbox.payment.payment_instrument` pi on p.id = pi.id
left join `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
left join `eai-datalake-data-sandbox.core.orders` ord on p.order_id = ord.uuid
join `eai-datalake-data-sandbox.core.customers`            cl on cl.uuid = p.customer_id
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
date(pt.created_at) >= current_date - 90
and pt.transaction_value > 0
and pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET","GOOGLE_PAY","APPLE_PAY")
--and p.customer_id = 'CUS-e7ef6b1d-67ae-4933-aba4-d2f77d851594'
group by 1,2,3,4,5,6,7

), Base_TPV_Com_CBK_2 as (
select
	
customer_id
,order_id
,store_id
,if((a.Flag_Contestacao = "Contestado"), a.TPV_PayPal, 0) AS Vl_Contestado
,if((a.Flag_Contestacao = "Contestado"),1, 0) AS Qtd_Contestado

,sum(TPV_PayPal) as TPV_PayPal
,sum(Qtd_TPV_PayPal) as Qtd_TPV_PayPal

from Base_TPV_Com_CBK a
group by 1,2,3,4,5

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
        ,(Qtd_Contestado/Qtd_TPV_PayPal)*100 as Vol_Perc_Exposicao_Cbk
        ,(Vl_Contestado/TPV_PayPal)*100 as Vl_Perc_Exposicao_Cbk  

        from Base_TPV_Com_CBK_2
        where Vl_Contestado >0
        
)Cl_Trigados on Cl_Trigados.customer_id = limt.customer_id
group by 1,2,3,4
order by 1

)
select
a.customer_id

,case 
      when cbk.Qtd_Contestado = 3 then 'Posto com 3 Contestacao'
      when cbk.Qtd_Contestado <= 5 and cbk.Vl_Contestado <=300  then 'Posto ate 5 Contestacao ate 300 reais Contestado'
      when cbk.Qtd_Contestado <= 5 and cbk.Vl_Contestado <=600 and cbk.Qtd_LojaPosto_Dist <=3 then 'Posto ate 5 Contestacao ate 600 reais Contestado e ate 3 postos distintos'
      when cbk.Qtd_Contestado <= 5 and cbk.Vl_Contestado <=600 and cbk.Qtd_LojaPosto_Dist <=10 then 'Posto ate 5 Contestacao ate 600 reais Contestado e ate 10 postos distintos'
      when cbk.Qtd_Contestado > 5 and cbk.Vl_Contestado >600 and cbk.Qtd_LojaPosto_Dist >10 then 'Posto ate 5 Contestacao maior 600 reais Contestado e ,mais de 10 postos distintos'
      else 'NaoAvaliado'
End as Flag_Risco_CBK

,count(distinct a.store_id) as Qtd_Lojas_Postos_Trans
,sum(a.Vl_Contestado) as Vl_Contestado
,sum(a.Qtd_Contestado) as Qtd_Contestado
,sum(a.TPV_PayPal) as TPV_PayPal
,sum(a.Qtd_TPV_PayPal) as Qtd_TPV_PayPal
from base_final_crivo_client_Cbk a
left join Base_LojaPosto_Dist cbk on cbk.customer_id = a.customer_id
group by 1,2

;

------------------- Crivo Transações exposição CBK ----------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_mov_App_clientes`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_mov_App_clientes` AS 

select
distinct
c.uuid as  CustomerID
,case  when o.sales_channel in ('TEF','POS','POS_QRCODE','APP_DELIVERY','ON_LINE','VOUCHER_UBER','APP_JET_OLIL','PDV_QRCODE','ECOMMERCE') THEN 'Outros Produtos' else 'ND' end as Flag_Produto_Outros
,case when o.sales_channel in ('APP_LATAMPASS','APP_MILES','APP_TUDOAZUL') THEN 'Pontos_Aerias' else 'ND' end as Flag_Produto_Pontos_Aerias
,case when o.sales_channel in ('APP') THEN 'Abastecimento' else 'ND' end as Flag_Produto_Abastecimento
,case when o.sales_channel in ('APP_JET_OIL') THEN 'Jet_Oil' else 'ND' end as Flag_Produto_JetOil
,case when o.sales_channel in ('APP_AMPM') THEN 'Ampm' else 'ND' end as Flag_Produto_AMPM
,case when o.sales_channel in ('SERVICE') THEN 'Recarga' else 'ND' end as Flag_Produto_Recarga
,case when o.sales_channel in ('APP_ULTRAGAZ') THEN 'Ultragaz' else 'ND' end as Flag_Produto_Ultragaz

,count(distinct o.uuid) as Transacoes
,round(sum(o.order_value),0) as TPV 

from `eai-datalake-data-sandbox.core.customers` c 
join `eai-datalake-data-sandbox.core.orders` o on c.id = o.customer_id
where 
o.order_status = 'CONFIRMED'
group by 1,2,3,4,5,6,7,8

;

------------------- Crivo Transações exposição CBK ----------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_mov_ContaDigital_clientes`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_mov_ContaDigital_clientes` AS 


SELECT

CASE WHEN flow = 'PIX' THEN
CASE WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN pix_in_payee.payee_id
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payer.payer_id END
WHEN flow = 'TED' THEN
CASE WHEN ted.type = 'CASH_IN' THEN ted_in_payee.payee_id
WHEN ted.type = 'CASH_OUT' THEN ted_payer.payer_id END
WHEN flow = 'BILLET' THEN billet.payee_id 
WHEN p2p.type = 'CASH_OUT' THEN p2p_payer.payer_id
WHEN p2p.type = 'CASH_OUT' THEN p2p_payee.payee_id
WHEN qpo.type = 'CASH_IN' THEN qpo.payee_document
ELSE flow 
END AS customer_id

,COUNT(DISTINCT cash_transaction.id) as qtdtransacoes
,ROUND(SUM(cash_transaction.amount)/100,2) as valor
from  `eai-datalake-data-sandbox.cashback.cash_transaction` as cash_transaction
LEFT join `eai-datalake-data-sandbox.cashback.pix`  pix on cash_transaction.id = pix.cash_transaction_id 
LEFT join `eai-datalake-data-sandbox.cashback.pix_payer` pix_payer on pix_payer.pix_id = pix.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_in_payee` pix_in_payee on pix_in_payee.pix_id = pix.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted` ted on cash_transaction.id = ted.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_payer` ted_payer on ted_payer.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_in_payee`ted_in_payee on ted_in_payee.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p` p2p on cash_transaction.id = p2p.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payer` p2p_payer on p2p_payer.p2p_id = p2p.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payee` p2p_payee on p2p_payee.p2p_id = p2p.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.billet` billet on cash_transaction.id = billet.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.qrcode_pix_out` qpo on cash_transaction.id = qpo.cash_transaction_id
WHERE 
--DATE(cash_transaction.created_at) >= '2023-01-01' --AND DATE(cash_transaction.created_at) <= CURRENT_DATE
--AND 
(pix.status = 'APPROVED' or ted.status = 'APPROVED' or p2p.status = 'APPROVED' or billet.status = 'APPROVED' or qpo.status = 'PAID')
GROUP BY 1
;


--------------------------- Clientes ------------------------------
/*

-- select Flag_Ativo, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` group by 1where CPF_Cliente = '02350784096'

select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` where CPF_Cliente = '02350784096'

select 
Flag_Risco_CBK, 
count(*) 
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` group by 1

select 
Flag_Funcionario, 
count(*) as Volume
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` group by 1

*/



CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` AS 

with
base_cliente as (
select
distinct
cl.id
,cl.uuid as  CustomerID
,cl.document as CPF_Cliente
,cl.full_name as Nome_Cliente
,cl.mother_name as Nome_Mae
,cl.birth_date as Dt_Nascimento
,Case 
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<18   Then '01  MenorIdade'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=20  Then '02  18a20anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=25  Then '04  21a25anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=30  Then '05  26a30anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=35  Then '06  31a35anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=40  Then '07  36a40anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=45  Then '08  41a45anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=50  Then '09  46a50anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=55  Then '10 51a55anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=60  Then '11 56a60anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=65  Then '12 61a65anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=70  Then '13 66a70anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=75  Then '14 71a75anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=80  Then '15 76a80anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=85  Then '16 81a85anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)>85   Then '17 >86anos'  
End as Faixa_Idade
,cl.status as StatusConta
,en.street as End_Cliente
,en.neighborhood as BairroCliente
,en.city as Cidade_Cliente
,en.state as UF_Cliente
,CASE
    WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
    WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
    WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
    WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
    WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
    ELSE 'NAOINDENTIFICADO'  
END AS RegiaoCliente
,cl.Email
,ph.area_code as DDD
,ph.number as Telefone
,ph.type as TelefoneTipo
,date(cl.created_at) as Dt_Abertura
,ClTrusted.Dt_AtualizacaoTrusted 
,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
,CASE
    WHEN DATETIME_DIFF(DATE(ClTrusted.Dt_AtualizacaoTrusted), DATE(cl.created_at), DAY) <=5 THEN '01_<5DIAS'
    WHEN DATETIME_DIFF(DATE(ClTrusted.Dt_AtualizacaoTrusted), DATE(cl.created_at), DAY) <=30 THEN '02_<30DIAS'
    WHEN DATETIME_DIFF(DATE(ClTrusted.Dt_AtualizacaoTrusted), DATE(cl.created_at), DAY) <=60 THEN '03_<60DIAS'
    WHEN DATETIME_DIFF(DATE(ClTrusted.Dt_AtualizacaoTrusted), DATE(cl.created_at), DAY) <=90 THEN '04_<90DIAS'
    WHEN DATETIME_DIFF(DATE(ClTrusted.Dt_AtualizacaoTrusted), DATE(cl.created_at), DAY) <=120 THEN '05_<120DIAS'
    WHEN DATETIME_DIFF(DATE(ClTrusted.Dt_AtualizacaoTrusted), DATE(cl.created_at), DAY) <=160 THEN '06_<160DIAS'
    WHEN DATETIME_DIFF(DATE(ClTrusted.Dt_AtualizacaoTrusted), DATE(cl.created_at), DAY) <=190 THEN '07_<190DIAS'
    WHEN DATETIME_DIFF(DATE(ClTrusted.Dt_AtualizacaoTrusted), DATE(cl.created_at), DAY) <=220 THEN '08_<220DIAS'
    WHEN DATETIME_DIFF(DATE(ClTrusted.Dt_AtualizacaoTrusted), DATE(cl.created_at), DAY) <=260 THEN '09_<260DIAS'
    WHEN DATETIME_DIFF(DATE(ClTrusted.Dt_AtualizacaoTrusted), DATE(cl.created_at), DAY) <=290 THEN '10_<290DIAS'
    WHEN DATETIME_DIFF(DATE(ClTrusted.Dt_AtualizacaoTrusted), DATE(cl.created_at), DAY) <=365 THEN '11_<=1ANO'
    WHEN DATETIME_DIFF(DATE(ClTrusted.Dt_AtualizacaoTrusted), DATE(cl.created_at), DAY) >365 THEN '12_+1ANO'
END AS Flag_Tempo_Trusted
,CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=5 THEN '01_<5DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=30 THEN '02_<30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=60 THEN '03_<60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=90 THEN '04_<90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=120 THEN '05_<120DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=160 THEN '06_<160DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=190 THEN '07_<190DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=220 THEN '08_<220DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=260 THEN '09_<260DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=290 THEN '10_<290DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=365 THEN '11_<=1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) >365 THEN '12_+1ANO'
END AS Flag_TempodeConta
,CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=5 THEN '01_<5DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=30 THEN '02_<30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=60 THEN '03_<60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=90 THEN '04_<90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=120 THEN '05_<120DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=160 THEN '06_<160DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=190 THEN '07_<190DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=220 THEN '08_<220DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=260 THEN '09_<260DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=290 THEN '10_<290DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=365 THEN '11_<=1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) >365 THEN '12_+1ANO'
END AS Flag_TempoBloqueado

,case when cl.trusted = 1 then 'Trusted'else 'NoTrusted' end as Flag_Trusted
,case when ClTrusted.Trusted = 'True' then 'Trusted' else 'NoTrusted' end as  flag_trusted_atualizado
,cl.risk_analysis_status as RiskAnalysis
,ev.observation as MotivoStatus
,ev.sub_classification
,ev.sub_classification_obs
,ev.event_date as DataStatus
,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Ev 
,ev.user_name as UsuarioStatus
,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual
,case
    when ev.observation = 'Fraude confirmada' then 'Fraude confirmada'
    when ev.observation = 'Suspeita de fraude' then 'Suspeita de fraude'
    when ev.observation = 'Bloqueio de cadastro' then 'Bloqueio de cadastro'
    when ev.observation is null then 'Sem Bloqueio'
    when ev.observation = '' then 'Sem Bloqueio'
    else 'Outros' 
end as MotivoBloqueio
,zaig.Flag_Email_NaoVal
,zaig.Flag_Celular_NaoVal
,zaig.Flag_NomeMae_CaixaAlta
,zaig.ScoreZaig
,case 
    when Bio.status = 'VALIDATED' then 'BioValidada'
    when Bio.status in ('REJECTED','NOT_VALIDATED') then 'BioRejeitada' else 'BioNaoCapturada' 
end as Flag_Biometria
,case 
    when cast(insp.CPF as INT64) = cast(cl.document as INT64) then 'Sim'    
    else 'Não'
end as Flag_Funcionario
,insp.STATUS as StatusFuncionario
,insp.TIPO as TipoFuncionario

  FROM `eai-datalake-data-sandbox.core.customers`               cl
  left join `eai-datalake-data-sandbox.core.address`            en on en.id = cl.address_id
  left join (
              with base_tel as (
                  select 
                      *, 
                      RANK() OVER (PARTITION BY id.customer_id ORDER BY ph.updated_at, id.phone_id  desc) AS Rank_Tel   
                  from `eai-datalake-data-sandbox.core.customer_phone` as id   
                  left join `eai-datalake-data-sandbox.core.phone`as ph  
                  on id.phone_id = ph.id 
                  where  type ='MOBILE' and area_code is not null )
                  select * from base_tel
                  where Rank_Tel = 1
                                                             ) as ph on cl.id = ph.customer_id
            --where cl.uuid = 'CUS-8835a147-0b0d-44eb-af25-ce6e0a1216bb'
  left join (select * from `eai-datalake-data-sandbox.core.customer_event` 
            where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT')
                                                            )   Ev on ev.customer_id = cl.id
  left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_bio_Capturada` Bio on Bio.customer_id = cl.id
  left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_validar_dados_cadastral_Zaig` zaig on zaig.Cpf_Cliente = cl.document
  ---------------------------------------- Funcionarios -----------------------------------------------------------------
  left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Base_Inspetoria_2` insp on cast(insp.CPF as INT64) = cast(cl.document as INT64)
  ----------------------------------------------------------------------------------------------
  ----------------------------------------Atualização do Trusted
  left join (
              with base_trusted as (
                  select distinct 
                      CustomerID,
                      Trusted,
                      Dt_AtualizacaoTrusted,
                      RANK() OVER (PARTITION BY CustomerID ORDER BY Dt_AtualizacaoTrusted desc) AS Rank_Trusted
                  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Mudanca_Trusted_NoTrusted_Final`
                ) select * from base_trusted where Rank_Trusted = 1  
                                              ) ClTrusted on ClTrusted.CustomerID = cl.uuid

--------------------------- Tabela consolidada clientes ------------------------------

)select
distinct
Cl.*
,c_orb.Dt_Conta
,BlMass.Lote as Dt_LoteMassivo
--,BlMass.Motivo as MotivoBloq_Massivo
,case
when sub_classification = 'Abuso' then 'Abusador Cashback'
else BlMass.Motivo end as MotivoBloq_Massivo
,block.Historico_Bloqueio
,block.Dt_HistoricoBloq
,Crivo_Lim.Flag_Risco_Limit_Vol
,Crivo_Lim.Flag_Risco_Limit_Val
,Cbk_Cli.Flag_Risco_CBK
,card.Flag_Tetativas
,card.Flag_Bancos
,card.Flag_Card
,case when APP.CustomerID = cl.CustomerID then 'MovimentouAPP' else 'ND' end Flag_APP
,gold.first_transaction_app as Prim_TransacaoAPP
,gold.last_transaction_app as Ult_TransacaoAPP
,case when chavepix.customer_id = cl.CustomerID then 'Sim' else 'Não' end as Tem_ChavePix
,chavepix.Tipo_Chaves
,chavepix.Qtd_Tipo
,chavepix.Chaves_Cadastradas
,case when CD.customer_id = cl.CustomerID then 'MovimentouContaDigital' else 'ND' end Flag_ContaDigital
,gold.wallet_balance as Saldo_Conta
,gold.kmv_balance as KMV_Acumulados
,gold2.Perfil_Consumidor
,case when rufra.Doc_Completo = cl.CPF_Cliente then 'Sim' else 'Não' end as Flag_Rufra
,rufra.Motivo as MotivoRufra
,rufra.Tipo_Envolvido
,rufra.Data_Cadastro as Dt_RegistroRufra
,rufra.LoginInclusao
,case 
 when APP.CustomerID = cl.CustomerID and CD.customer_id = cl.CustomerID then 'ClienteAtivo_APP_CD' 
 when APP.CustomerID = cl.CustomerID then 'ClienteAtivo_APP' 
 when CD.customer_id = cl.CustomerID then 'ClienteAtivo_CD' 
 else 'ClienteSemMovimento' end Flag_Ativo
,case 
when cast(vip.CPF as numeric) = cast(cl.CPF_Cliente as Numeric) then 'VIP'
when cast(uber.Cpf_Cliente as Numeric) = cast(cl.CPF_Cliente as Numeric) then 'UBER'
else 'URBANO'end as Flag_Perfil

from base_cliente cl
--------------------------- Bloqueio Massivo ---------------------------------
left join (
  with base_bloqueio_Massivo as (
            SELECT 
            CustomerID
            ,Lote
            ,Motivo
            ,RANK() OVER (PARTITION BY CustomerID ORDER BY Lote,Motivo desc) AS Rank_Bloqueio
            FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2` 
            order by 1,4
            )
          select
          *
          from base_bloqueio_Massivo where Rank_Bloqueio = 1  
          --CustomerID = 'CUS-20135c6e-79ca-485f-80fa-4ae584a8bf3d'
) BlMass on BlMass.CustomerID = cl.CustomerID 
--------------------------- Perfil clientes VIP ------------------------------

LEFT JOIN (
select 
distinct 
CPF 
from `eai-datalake-data-sandbox.loyalty.tblParticipantes` 
where Vip is not null and Inativo = false) 
as vip on cast(vip.CPF as numeric) = cast(cl.CPF_Cliente as Numeric)

--------------------------- Perfil clientes UBER ------------------------------

LEFT JOIN (
select
distinct 
cl.uuid as CustomerId
,cl.document as Cpf_Cliente
from `eai-datalake-data-sandbox.core.order_benefit` ordbnf
join `eai-datalake-data-sandbox.core.orders`              ord on ord.id = ordbnf.order_id
join `eai-datalake-data-sandbox.core.customers`            cl on ord.customer_id =cl.id
WHERE ordbnf.id >= 123900000
AND (ordbnf.origin_type  = 'EAI:UBER' or upper(ordbnf.description) LIKE '%UBER%')
AND ordbnf.status = 'FINISHED'

) uber on cast(uber.Cpf_Cliente as Numeric) = cast(cl.CPF_Cliente as Numeric)
--------------------- Chave Pix -------------------------------------------------
LEFT JOIN ( 
  with BASE_CHAVEPIX AS (  
    select
    distinct

      id_key.pix_key_id 
      ,key.key_value AS ChaveCadastrada
      ,clkey.customer_id
      ,RANK() OVER (PARTITION BY key.key_value ORDER BY key.created_at  desc) AS Rank_key
      ,id_key.payment_customer_account_id
      ,key.id
      ,FORMAT_DATE("%Y%m",key.created_at)as Safra_Cad_Key
      ,key.created_at
      ,key.type
      ,key.uuid
      ,key.reason
      ,key.status
      ,pca.payment_account_id
      

  FROM `eai-datalake-data-sandbox.payment.payment_customer_account_pix_key`   id_key
  join `eai-datalake-data-sandbox.payment.pix_key`                            key     on id_key.pix_key_id = key.id
  join `eai-datalake-data-sandbox.payment.customer_account`                   clkey   on clkey.payment_customer_account_id = id_key.payment_customer_account_id
  join `eai-datalake-data-sandbox.payment.payment_customer_account`           pca    on clkey.payment_customer_account_id = pca.id
  where 
  --key.type = 'CPF'
  key.status = 'COMPLETED'
  --and customer_id = 'CUS-bdd5ecf5-83eb-4938-a20a-b381a57ecff3'
  order by 3 desc

) select 
    customer_id,
    count(pix_key_id) as Chaves_Cadastradas,
    --Count(distinct pix_key_id) as Qtd_Chaves,
    count(distinct type) as Qtd_Tipo,
    string_agg(DISTINCT type,', ') as Tipo_Chaves
  from BASE_CHAVEPIX 
  group by 1

) chavepix on chavepix.customer_id = cl.CustomerID
--------------------- Base Rufra ------------------------------------------
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Relatorio_ReportRufra_DW2`  rufra on rufra.Doc_Completo = cl.CPF_Cliente
--------------------- Limit analise 180 dias ------------------------------------
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_tran_limt_cliente` Crivo_Lim on Crivo_Lim.customer_id = cl.CustomerID
--------------------- CBK analise 180 dias ------------------------------------
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Tpv_Cbk_Clientes` Cbk_Cli on Cbk_Cli.customer_id = cl.CustomerID
---------------------------------------- Transaçoes APP ------------------------------------------
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_mov_App_clientes`  APP on APP.CustomerID = cl.CustomerID
left join `eai-datalake-data-sandbox.gold.customers` gold on gold.customer_document = cl.Cpf_Cliente
left join `eai-datalake-data-sandbox.gold.clientes` as gold2 on gold2.CPF = cl.CPF_Cliente
---------------------------------------- Transaçoes ContaDigital ------------------------------------------
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_mov_ContaDigital_clientes` CD on CD.customer_id = cl.CustomerID
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_trans_PayPal_Crivo_Cartao` card on card.CustomerID = cl.CustomerID
------------------------------------------ Historico bloqueios ---------------------------------------------
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Historico_Bloqueios` block on block.CustomerID = cl.CustomerID
------------------------------------------ Data da Conta --------------------------------------------------------
left join (
    with base_ContaOrbital as (
      select 
        distinct
          cl.uuid
          ,cl.document
          ,CustAccount.created_at as Dt_Conta
          --,AccountEv.status
          ,RANK() OVER (PARTITION BY cl.document ORDER BY CustAccount.created_at  desc) AS Rank_Conta
          from `eai-datalake-data-sandbox.core.customers` Cl 
          join `eai-datalake-data-sandbox.payment.customer_account` CustAccount on CustAccount.customer_id = Cl.uuid
          --join `eai-datalake-data-sandbox.core.event` EV
          join (select distinct * from `eai-datalake-data-sandbox.payment.customer_account_event` 
          ) AccountEv on AccountEv.customer_account_id = CustAccount.id 
          where type = 'APPROVED' /* and document = '39046078809' and cl.status <> 'MINIMUM_ACCOUNT'*/
    ) select * from base_ContaOrbital where Rank_Conta = 1
    ) c_orb on cast(c_orb.document as numeric) = cast(cl.CPF_Cliente as numeric)
where cl.Rank_Ult_Atual = 1 and DDD is not null
--and cl.CustomerID = 'CUS-bdd5ecf5-83eb-4938-a20a-b381a57ecff3'
;


-----------------------------------------------
-- VALIDADOR DE DUPLICIDADE                   |
-----------------------------------------------
with validador as (
  select 
      CPF_Cliente, 
      count(*) as Quantidade 
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`
  group by 1
), base_duplicidade as ( select * from validador 
  where Quantidade > 1 order by 2 # 
) select * from base_duplicidade as a
  join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` as b
  on a.CPF_Cliente = b.CPF_Cliente
  order by CustomerID 
