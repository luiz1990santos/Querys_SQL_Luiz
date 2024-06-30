-- total clientes 108.352

with base_MG34 as (
select  
 distinct
  CustomerID,
  CPF_Cliente, 
  Nome_Cliente, 
  Dt_Nascimento, 
  StatusConta, 
  End_Cliente, 
  BairroCliente, 
  Cidade_Cliente, 
  UF_Cliente, 
  Email, 
  DDD, 
  Telefone, 
  Dt_Abertura, 
  Flag_TempodeConta, 
  Flag_TempoBloqueado, 
  MotivoStatus, 
  MotivoBloqueio
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` 
where --DDD = '34'
--and UF_Cliente = 'MG'
StatusConta = 'ACTIVE'
and MotivoBloqueio = 'Sem Bloqueio'
and TelefoneTipo = 'MOBILE'

), base_cartoes as (
  select
       distinct
       ba.CustomerID,
       ba.CPF_Cliente,
       CASE
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=1 THEN '1_<1DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=3 THEN '2_<3DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=6 THEN '3_<6DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=9 THEN '4_<9DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=12 THEN '5_<12DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=15 THEN '6_<15DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=20 THEN '7_<20DIAS'
              else 'Verificar'
       END AS Temp_Cadastro_Cartao,
       card.created_at as data_cadastro_cartao
       ,card.last_four_digits as qtd_Tentativas
       ,bin.Emissor_do_Banco as qtd_banco
       ,case 
          when card.status = 'VERIFIED' then last_four_digits
          else null
       end as quantidade_cadastrados
       ,card.document as qtd_cliente
       from base_MG34 as ba
       left join `eai-datalake-data-sandbox.payment.customer_card` as card
       on ba.CPF_Cliente = card.document 
       left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos` as bin
       on cast(card.bin as string) = cast(bin.BIN as string)
), quantidade_base_cartoes as (

  select 
        CPF_Cliente
        ,CustomerID
        ,count(distinct qtd_Tentativas) as qtd_Tentativas
        ,count(distinct qtd_banco) as qtd_banco
        ,count(distinct quantidade_cadastrados) as quantidade_cadastrados
        ,count(distinct qtd_cliente) as  qtd_cliente
          
  from base_cartoes
  group by 1,2
  order by 3 desc

), base_cbk as (
select
p.customer_id
,cl.document as CPF_Cliente
,p.order_id
,ord.store_id
,case when p.order_id = cbk.order_id and pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET") then 'Contestado' else 'NaoContestado' end as Flag_Contestacao
,case when pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET") then 1 else 0 end as Qtd_TPV_PayPal
,case when pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET") then pt.transaction_value else 0 end as TPV_PayPal
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
date(pt.created_at) >= current_date - 120
and pt.transaction_value > 0
and pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET",'GOOGLE_PAY')
--and p.customer_id = 'CUS-e7ef6b1d-67ae-4933-aba4-d2f77d851594'
group by 1,2,3,4,5,6,7

), Base_TPV_Com_CBK_2 as (
select
	
customer_id
,order_id
,store_id
,if((a.Flag_Contestacao = "Contestado"), a.TPV_PayPal, 0) AS Vl_Contestado
,if((a.Flag_Contestacao = "Contestado"),1, 0) AS Qtd_Contestado
,TPV_PayPal
,Qtd_TPV_PayPal

from base_cbk a
), Base_TPV_Com_CBK_3 as (
  select 
    customer_id
    ,sum(TPV_PayPal) as TPV_PayPal
    ,count(Qtd_TPV_PayPal) as Qtd_TPV_PayPal
    ,sum(Vl_Contestado) as Vl_Contestado
    ,sum(Qtd_Contestado) as Qtd_Contestado
  from Base_TPV_Com_CBK_2
  group by 1
), base_allowme as (
  select 
      user_id,
      case 
          when rules_matched like '%16%' then '16 - Atingiu o limite de usuários associados ao dispositivo móvel' 
          else 'Outros' 
      end as FlagRegra 
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_AllowMe`

), base_consolidada as ( 
   select
      distinct
        perfil.CustomerID,
        perfil.CPF_Cliente, 
        perfil.Nome_Cliente, 
        perfil.StatusConta, 
        perfil.UF_Cliente, 
        perfil.DDD, 
        perfil.Dt_Abertura, 
        perfil.Flag_TempodeConta, 
        perfil.Flag_TempoBloqueado, 
        ca.qtd_Tentativas,
        ca.qtd_banco,
        ca.quantidade_cadastrados,
        ca.qtd_cliente,
        cbk.Qtd_Contestado,
        cbk.Qtd_TPV_PayPal,
        cbk.Vl_Contestado,
        cbk.TPV_PayPal,
        allowme.FlagRegra

     from base_MG34 as perfil
     left join quantidade_base_cartoes as ca 
     on perfil.CustomerID = ca.CustomerID
     left join Base_TPV_Com_CBK_3 as cbk
     on perfil.CustomerID = cbk.customer_id
     left join base_allowme as allowme
     on perfil.CPF_Cliente = allowme.user_id

     --group by 1,2,3,4,5,6,7,8,9,10,11,12
)
  select * from base_consolidada
  where quantidade_cadastrados > 6
  and FlagRegra = '16 - Atingiu o limite de usuários associados ao dispositivo móvel' 
  -- and Qtd_Contestado > 0
  --and 
  --and CPF_Cliente = '75378353653'
  order by 12 desc
  
 

;
 
-- 1 1 1 1 0 30 0 1768.2


-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` limit 100