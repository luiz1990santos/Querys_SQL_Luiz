--==========================================================================--
-- Reporte Gerencial Prevenção a Fraudes
--==========================================================================--


-- Tabela Calendario 

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Safra` AS 
SELECT 
cast(Safra_Data as String) as Safra_Data
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Safra`

--==========================================================================--
-- Cubo Onboarding - Período 90 dias -- Volume desbloqueio de contas
-- Origem base de dados qry 001
--==========================================================================--
;

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Onb_Motor` AS 

select
Data_Cadastro
,FORMAT_DATE("%Y%m",Data_Cadastro)as Safra_Cadastro
,decisao
,razao
,esteira
,Flag_Fase
,Flag_Filtro_Periodo
,Flag_Decisao_Motor
,Flag_Decisao_Regra
,tree_score
,case when Flag_Fase = 'KMV - Full' and Flag_Decisao_Motor = 'Aprovado' Then qtd_proposta else 0 end Apr_Full
,case when Flag_Fase = 'KMV - Light' and Flag_Decisao_Motor = 'Aprovado' Then qtd_proposta else 0 end Apr_Light
,case when Flag_Fase = 'KMV - Full' and Flag_Decisao_Motor <> 'Aprovado' Then qtd_proposta else 0 end Neg_Full
,case when Flag_Fase = 'KMV - Light' and Flag_Decisao_Motor <> 'Aprovado' Then qtd_proposta else 0 end Neg_Light
,case when Flag_Decisao_Motor = 'Aprovado' then qtd_proposta else 0 end as Flag_Aprovado
,case when Flag_Decisao_Motor <> 'Aprovado' then qtd_proposta else 0 end as Flag_Negada
,qtd_cliente
,qtd_proposta
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao`

;
--==========================================================================--
-- Cubo Volume de Bloqueio de Contas
-- Origem base de dados qry 21_1
--==========================================================================--
-- select * From `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Vl_Bloqueios` order by 1 desc


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Vl_Bloqueios` AS

select
Safra_Abertura
,Safra_Bloqueio
,Safra_Chamado
,Safra_Massivo
,NR_OCORRENCIA
,RSP_EXTERNO
,Trusted
,Status_Conta
,Status_Conta_EV
,MotivoStatus
,sub_classification
,sub_classification_obs
,RegiaoCliente
,UF
,Flag_tratativa
,Flag_StatusCliente
,Flag_Cham
,Flag_Bloq_Massivo
,qtd_Cliente
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_bloqeuios_vs_Chamados`

;
--==========================================================================--
-- Cubo Volume de Bloqueio de Contas
-- Origem base de dados qry 21_1
--==========================================================================--

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Vl_Desbloqueios` AS

select 
MotivoBloqueio
,StatusConta_Atual
,Flag_Bloqueio
,Flag_Chamado
,flag_reversao
,flag_reversao_FC
,Flag_Aging_Reversao
,Flag_Aging_Bloqueio
,Safra_Abertura
,Safra_Bloqueio
,Safra_Reversao
,Volume
,QtdCliente
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Taxa_Reversao`

;
--==========================================================================--
-- Cubo Transacional App
-- Origem base de dados tabelas DataLake
--==========================================================================--
-- SELECT * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Transacional_App` 

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Transacional_App` AS

with
base as (

select
distinct
date(o.created_at) as Dt_Tranx
,FORMAT_DATE("%Y%m",o.created_at)as Safra_Tranx
,o.sales_channel as Canal
,o.uuid as Order_Id
,case 
      when dppaypal.order_code = 'REC-' then 'Recarga'
      when dppaypal.order_code = 'LIV-' then 'Livelo'
      when dppaypal.order_code = 'AZU-' or p.sales_channel  in ('APP_TUDOAZUL','APP_MILES') then 'TudoAzul'
      when dppaypal.order_code = 'SMI-' then 'Smiles'
      when p.sales_channel = 'APP_ULTRAGAZ' then 'UltraGaz'
      when dppaypal.Merchant_Account = 'DRYWASHBRL' then 'DryWash'
      when dppaypal.order_code = 'FUT-' or dppaypal.Merchant_Account = 'fangoldenbrl' then 'Futebol'
      when dppaypal.Merchant_Account = 'satelitalbrl'  then 'Shopping'
      when p.sales_channel in ('APP','APP_AMPM','APP_JET_OIL','PDV_QRCODE') then 'Abastecimento'
      else 'Verificar' end as Flag_Merchant_Account_Tranx
,pi.method as Metodo_Pagamento
,o.order_status as Status_Trans
,case 
  when o.order_status in ('CANCELED_BY_BACKOFFICE','CANCELED_BY_CUSTOMER','CANCELED_BY_GATEWAY','CANCELED_BY_PRE_AUTHORIZATION_TIMEOUT','CANCELED_BY_STORE') then 'Cancelada'
  when o.order_status in ('CONFIRMATION_WAITING','CONFIRMED','PRE_AUTHORIZATION_WAITING','PRE_AUTHORIZED_BY_GATEWAY') then 'Aprovada'
  when o.order_status in ('EXPIRED') then 'Expirada'
  when o.order_status in ('PENDING','PRE_AUTHORIZED_ERROR') then 'Erro'
  when o.order_status in ('REVERSED') then 'Revertida'
else 'ND' end as Flag_Status
,case 
  when pt.status in ("AUTHORIZED","COMPLETE","COMPLETED","COMPLETE","PRE_AUTHORIZED","REVERSED_DENIED","REVERSED_ERROR",'SETTLEMENT') then 'Aprovada'
  when pt.status in ("CANCELLED_BY_GATEWAY","PRE_AUTHORIZED_ERROR",'ERROR') then 'Cancelada'
  when pt.status in ('REVERSED') then 'Outras'
else 'ND' end as Flag_Status_Payment
,p.status
,(pi.value) as Vl_Transacao

FROM `eai-datalake-data-sandbox.core.orders` o
JOIN  `eai-datalake-data-sandbox.payment.payment` p on o.uuid = p.order_id
JOIN  `eai-datalake-data-sandbox.payment.payment_instrument` pi on p.generated_payment_instrument_id = pi.generated_payment_instrument_id
JOIN `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
join (select Merchant_Account,order_code,sales_channel from `eai-datalake-data-sandbox.analytics_prevencao_fraude.DE_PARA_PAYPAL_PEDIDO_v4`)  dppaypal 
on dppaypal.order_code = substring(p.order_code,1,STRPOS(p.order_code,'-'))

where
date(o.created_at) >= current_date - 90

)

select
Dt_Tranx
,Safra_Tranx
,order_id
,Flag_Status
,Flag_Status_Payment
,Flag_Merchant_Account_Tranx
,Metodo_Pagamento
,Status_Trans
,sum(Vl_Transacao) as Vl_Transacao
from base

group by 1,2,3,4,5,6,7,8

;

--==========================================================================--
-- Cubo Transacional Conta Digital
-- Origem base de dados tabelas DataLake
--==========================================================================--
-- SELECT * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Transacional_Conta_Digital` 

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Transacional_Conta_Digital` AS

with

base_trans as (

SELECT
    FORMAT_DATE("%Y%m",cash_transaction.created_at)as Safra_Tranx,
    cash_transaction.flow,
    CASE WHEN (pix.type in ('CASH_IN', 'CASH_IN_REFUND') or ted.type = 'CASH_IN' or p2p.type = 'CASH_IN' or qpo.type = 'CASH_IN' or flow = 'BILLET') THEN 'CASH-IN' 
         WHEN (pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') or  ted.type = 'CASH_OUT' or p2p.type = 'CASH_OUT') THEN 'CASH-OUT' 
         END AS type,
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
    END AS customer_id,
    CASE 
        WHEN cash_transaction.flow = 'PIX' AND PIX.TYPE IN ('CASH_IN', 'CASH_IN_REFUND') THEN 'PIX-IN'
        WHEN cash_transaction.flow = 'PIX' AND PIX.TYPE IN ('CASH_OUT', 'CASH_OUT_REFUND') THEN 'PIX-OUT'
        WHEN cash_transaction.flow = 'TED' AND TED.TYPE IN ('CASH_IN') THEN 'TED-IN'
        WHEN cash_transaction.flow = 'TED' AND TED.TYPE IN ('CASH_OUT') THEN 'TED-OUT'
        WHEN cash_transaction.flow = 'BILLET' AND BILLET.TYPE IN ('CASH_IN') THEN 'BOLETO'
        WHEN cash_transaction.flow = 'P2P' AND P2P.TYPE IN ('CASH_OUT') THEN 'P2P-OUT'
        WHEN cash_transaction.flow = 'P2P' AND P2P.TYPE IN ('CASH_IN') THEN 'P2P-IN'
        WHEN cash_transaction.flow = 'QRCODE_PIX_OUT' AND QPO.TYPE IN ('CASH_IN') THEN 'QRCODE_PIX_OUT'
        ELSE 'JUDICIAL_DEBT'
    END AS flowoperation,
    CASE 
        WHEN cash_transaction.flow = 'PIX' THEN pix.status
        WHEN cash_transaction.flow = 'TED' THEN ted.status
        WHEN cash_transaction.flow = 'BILLET' THEN billet.status
        WHEN cash_transaction.flow = 'P2P' THEN p2p.status
        WHEN cash_transaction.flow = 'QRCODE_PIX_OUT' THEN qpo.status
        ELSE 'JUDICIAL_DEBT'
    END AS Status_Trans,


    --,case when pix.status = 'APPROVED' or ted.status = 'APPROVED' or p2p.status = 'APPROVED' or billet.status = 'APPROVED' or qpo.status = 'PAID'
    COUNT(DISTINCT cash_transaction.id) as qtdtransacoes,
    ROUND(SUM(cash_transaction.amount)/100,2) as valor
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
DATE(cash_transaction.created_at) >= current_date - 90
--DATE(cash_transaction.created_at) >= '2023-01-01' --AND DATE(cash_transaction.created_at) <= CURRENT_DATE
--AND (pix.status = 'APPROVED' or ted.status = 'APPROVED' or p2p.status = 'APPROVED' or billet.status = 'APPROVED' or qpo.status = 'PAID')
GROUP BY 1,2,3,4,5,6
ORDER BY 1
)
select
Safra_Tranx
,flow
,type
--customer_id
,flowoperation
,Status_Trans
,case 
     when Status_Trans in ('SCHEDULED') then 'Agendada'
     when Status_Trans in ('APPROVED','PAID') then 'Aprovada'
     when Status_Trans in ('CANCELLED') then 'Cancelada'
     when Status_Trans in ('ERROR','PENDING','CANCELLED_ERROR','CREATED','PROCESSING','REVERSED_ERROR') then 'Erro'
     when Status_Trans in ('EXPIRED') then 'Expirada'
     when Status_Trans in ('DENIED') then 'Negada'
     when Status_Trans in ('REVERSED','REVERSED_PROCESSING') then 'Revertida'
Else 'NA' end as Flag_StatusTrans

,Sum(qtdtransacoes) as Qtd_Transacao
,Sum(valor) as Vl_Transacao

from base_trans  tranx
GROUP BY 1,2,3,4,5,6

;

--==========================================================================--
-- Cubo BPs Geral
-- Origem base de dados Qry 20
--==========================================================================--
-- SELECT * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_BPS_Geral` 


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_BPS_Geral` AS

select 
Safra_Tranx
,Flag_Filt_Per
,Flag_Merchant_Account_Tranx
,BPsSaudavel
,'TransacaoAPP' as Origem
,Sum(Qtd_Transacao) Qtd_Transacao
,Sum(Qtd_Contestacao) Qtd_Contestacao
,Sum(TPV) TPV
,Sum(ValorContestado) ValorContestado

--,BPs

 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_CBKxTPV` 
 group by 1,2,3,4
UNION ALL

select 
Safra_Tranx
,case
  when DATE_DIFF(date(current_date),date(DataPedido), Month) = 0 then 'M0'
  when DATE_DIFF(date(current_date),date(DataPedido), Month) = 1 then 'M-1'
  when DATE_DIFF(date(current_date),date(DataPedido), Month) = 2 then 'M-2'
  when DATE_DIFF(date(current_date),date(DataPedido), Month) = 3 then 'M-3'
  when DATE_DIFF(date(current_date),date(DataPedido), Month) = 4 then 'M-4'
else 'Outros' end as Flag_Filt_Per
,'Latam' as Flag_Merchant_Account_Tranx
,20 as BPsSaudavel
,'TransacaoWeb' as Origem
,Sum(Qtd_Aprovada) as Qtd_Transacao
,Sum(Qtd_Contestada) as Qtd_Contestacao
,Sum(TPV_Latam) as TPV
,Sum(VlContestado) as ValorContestado
--,(if(Sum(VlContestado) = 0, 1,(Sum(VlContestado) / Sum(TPV_Latam))))*10000 as BPs


from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_latam_TPV_CBK` 
group by 1,2,3,4


;

--==========================================================================--
-- Cubo Bloqueios Prevenção a Fraudes
-- Origem base de dados Qry 21
--==========================================================================--

-- SELECT * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Bloqueio_Chamados` 

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Bloqueio_Chamados` AS

select 
Safra_Abertura
,Safra_Bloqueio
,Safra_Chamado
,Safra_Massivo
,NR_OCORRENCIA
,RSP_EXTERNO
,Trusted
,Status_Conta
,Status_Conta_EV
,MotivoStatus
,RegiaoCliente
,UF
,Flag_tratativa
,Flag_StatusCliente
,Flag_Cham
,Flag_Bloq_Massivo
,Sum(qtd_Cliente) as qtd_Cliente
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_bloqeuios_vs_Chamados` 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16

;
--==========================================================================--
-- Cubo Chamados Prevenção a Fraudes
-- Origem base de dados datalake
--==========================================================================--
-- SELECT * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_CanalAtrito_Chamados` where Responsavel like '%FRAUDE%'

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_CanalAtrito_Chamados` AS

with loyalty_acumulos as (
  select  
    dep.depositoid
   ,date_trunc(date(dep.data),month) as AnoMes
   ,case when dep.tipokmacumuloid in ( 1, 2, 3, 8, 31, 34, 36, 115, 150, 161, 171) then "acumulo pos"
         when dep.tipokmacumuloid in (5,6,7,9,12,14,16,18,19,23,24,25,27,28,32,33,35,37,40,41,42,43,45,46,47,55,56,58,59,60,62,63,64,
                                      66,72,78,83,86,87,88,89,90,92,95,103,104,107,108,109,113,116,117,120,122,125,126,127,128,129,131,133,137,
                                      139,142,143,153,154,155,156,162,169,170,175,176,181,182,183,184,186,190,191,192,193,194,195,
                                      196,197,200,202,204,205,208,209,213,214,216,217,218,672) then "acumulo passivo"
         else "acumulo outros"
    end as tipo
    from `eai-datalake-data-sandbox.loyalty.tblDepositos` dep
    left join `eai-datalake-data-sandbox.loyalty.tblAbasteceAiV2TransacaoControle` tc on dep.depositoid = tc.depositoid
    where bloqueado = false
         and valorreais >= 0
         and tc.abasteceaiv2transacaocontroleid is null
         and date(data) >= "2023-01-01"
), loyalty_acumulos_qtd as (
  select  
      AnoMes
     ,count(distinct case when tipo = "acumulo pos" then depositoid end) as QtdAcumuloPOS
     ,count(distinct case when tipo = "acumulo passivo" then depositoid end) as QtdAcumuloPassivo
     ,count(distinct case when tipo = "acumulo outros" then depositoid end) as QtdAcumuloOutros
  from loyalty_acumulos
  group by 1
), loyalty_resgates_qtd as (
  select  
      date_trunc(date(ped.datapedido),month) as AnoMes
     ,count(distinct ped.pedidoid) as QtdResgates
  from `eai-datalake-data-sandbox.loyalty.tblPedidos` ped
  left join `eai-datalake-data-sandbox.loyalty.tblAbasteceAiV2TransacaoControle` tc on ped.pedidoid = tc.pedidoid    
  where ped.situacaoid not in (5,7)
     and ped.valormoeda > 0
     and tc.abasteceaiv2transacaocontroleid is null
     and date(ped.datapedido) >= "2023-01-01"
  group by 1
), abastecimentoAPP as (
  select
      date_trunc(date(o.created_at),month) as AnoMes
     ,count(distinct o.id)  as Abastecimento
  from `eai-datalake-data-sandbox.core.orders` o
  where
     o.order_status = 'CONFIRMED'
     and date(o.created_at) >= "2023-01-01" 
     and o.sales_channel = 'APP'
  group by 1   
), central as (
select
     date_trunc(date(ch.DT_CRIACAO), month) as AnoMes
    ,RSP_EXTERNO as Responsavel
    ,concat(ch.PRODUTO_SERVICO, CLASSIFICACAO, SUBCLASSIFICACAO) as PK_Area 
    ,case when RSP_EXTERNO in ('GRUPO DE PREVENÇÃO','GRUPO PREVENÇÃO','PREVENÇÃO A FRAUDE') then 'AreaFraude' else 'NaoFraude' end as Flag_Chamado
    ,case 
        when RSP_EXTERNO in ('GRUPO DE PREVENÇÃO','GRUPO PREVENÇÃO','PREVENÇÃO A FRAUDE') then 'Sim'
        else 'Não'
     end FlagFraude   
     ,avg(TMR_DIA) as TMR_Medio                          
    ,count(distinct ch.NR_OCORRENCIA) as QtdeChamados
from `eai-datalake-data-sandbox.siebel.chamados` ch
where 
    date(ch.DT_CRIACAO) >= '2023-01-01' 
    and ch.STATUS not in ('CANCELADO', 'FECHADO AUTOMÁTICO', 'ABANDONO CHATBOT')
    and ch.TIPO_CHAMADO = 'E AÍ'
group by 1,2,3,4,5
), resumo as ( 
select
   a.AnoMes
  ,a.QtdAcumuloPOS
  ,a.QtdAcumuloPassivo 
  ,a.QtdAcumuloOutros
  ,r.QtdResgates
  ,ab.Abastecimento 
  ,a.QtdAcumuloOutros + r.QtdResgates + ab.Abastecimento as TransacoesTotais

from loyalty_acumulos_qtd a 
left join loyalty_resgates_qtd r on a.AnoMes = r.AnoMes
left join abastecimentoAPP ab on a.AnoMes = ab.AnoMes and r.AnoMes = ab.AnoMes
)
select
    r.*
   ,c.PK_Area
   ,c.Responsavel
   ,c.Flag_Chamado
   ,FlagFraude  
   ,TMR_Medio 
   ,QtdeChamados
   ,QtdeChamados / TransacoesTotais as ContactRate
from resumo r 
join central c on r.AnoMes = c.AnoMes

;
--------------------- Canal de Atrito - Contact Rate -----------------------------------


-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_CanalAtrito_Chamados_1`  where Safra_Tranx = '202404' group by 1

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_CanalAtrito_Chamados_1` AS

select

FORMAT_DATE("%Y%m",AnoMes) as Safra_Tranx 
,sum(distinct TransacoesTotais) as QtdTranx
,sum(QtdeChamados) as QtdChamados
,sum(if(FlagFraude = 'Sim',QtdeChamados,0)) as QtdChamados_Fraude
,sum(ContactRate) as ContactRate_Geral
,sum(if(FlagFraude = 'Sim',ContactRate,0)) as ContactRate_Fraude
,avg(TMR_Medio) as TMRMedio_Geral
,avg(if(FlagFraude = 'Sim',TMR_Medio,0)) as TMRMedio_Fraude


from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_CanalAtrito_Chamados`
group by 1
order by 1

;
--------------------- Canal de Atrito - Contact Rate -----------------------------------
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_CanalAtrito_Chamados_Modalidade_2` where Safra_Tranx = '202404' group by 1

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_CanalAtrito_Chamados_Modalidade_2` AS

select

FORMAT_DATE("%Y%m",AnoMes) as Safra_Tranx 
,PK_Area
,FlagFraude 
,sum(distinct TransacoesTotais) as QtdTranx
,sum(QtdeChamados) as QtdChamados
,sum(if(FlagFraude = 'Sim',QtdeChamados,0)) as QtdChamados_Fraude
,sum(ContactRate) as ContactRate_Geral
,sum(if(FlagFraude = 'Sim',ContactRate,0)) as ContactRate_Fraude
,avg(TMR_Medio) as TMRMedio_Geral
,avg(if(FlagFraude = 'Sim',TMR_Medio,0)) as TMRMedio_Fraude


from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_CanalAtrito_Chamados`
group by 1,2,3
order by 1


;
--------------------- Conta Pagamento - Bloqueados Fraude -----------------------------------
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_ContaDigital_Fraude` order by 1 desc

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_ContaDigital_Fraude` AS

with

base_Cliente_Fraude as (

select 
CustomerID
,CPF_Cliente
,MotivoStatus
,sub_classification
,Flag_Perfil
,Safra_Abertura
,Safra_Ev

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`
where sub_classification in ('Golpe','Invasão de conta','Conta Laranja','Conta Fraudada','Fraude Cartão')
and DATE_DIFF(date(current_date),date(DataStatus), Month) = 1
order by 1

),base_trans as (

SELECT
    FORMAT_DATE("%Y%m",cash_transaction.created_at)as Safra_Tranx,
    cash_transaction.flow,
    CASE WHEN (pix.type in ('CASH_IN', 'CASH_IN_REFUND') or ted.type = 'CASH_IN' or p2p.type = 'CASH_IN' or qpo.type = 'CASH_IN' or flow = 'BILLET') THEN 'CASH-IN' 
         WHEN (pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') or  ted.type = 'CASH_OUT' or p2p.type = 'CASH_OUT') THEN 'CASH-OUT' 
         END AS type,
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
    END AS customer_id,
    CASE 
        WHEN cash_transaction.flow = 'PIX' AND PIX.TYPE IN ('CASH_IN', 'CASH_IN_REFUND') THEN 'PIX-IN'
        WHEN cash_transaction.flow = 'PIX' AND PIX.TYPE IN ('CASH_OUT', 'CASH_OUT_REFUND') THEN 'PIX-OUT'
        WHEN cash_transaction.flow = 'TED' AND TED.TYPE IN ('CASH_IN') THEN 'TED-IN'
        WHEN cash_transaction.flow = 'TED' AND TED.TYPE IN ('CASH_OUT') THEN 'TED-OUT'
        WHEN cash_transaction.flow = 'BILLET' AND BILLET.TYPE IN ('CASH_IN') THEN 'BOLETO'
        WHEN cash_transaction.flow = 'P2P' AND P2P.TYPE IN ('CASH_OUT') THEN 'P2P-OUT'
        WHEN cash_transaction.flow = 'P2P' AND P2P.TYPE IN ('CASH_IN') THEN 'P2P-IN'
        WHEN cash_transaction.flow = 'QRCODE_PIX_OUT' AND QPO.TYPE IN ('CASH_IN') THEN 'QRCODE_PIX_OUT'
        ELSE 'JUDICIAL_DEBT'
    END AS flowoperation,
    CASE 
        WHEN cash_transaction.flow = 'PIX' THEN pix.status
        WHEN cash_transaction.flow = 'TED' THEN ted.status
        WHEN cash_transaction.flow = 'BILLET' THEN billet.status
        WHEN cash_transaction.flow = 'P2P' THEN p2p.status
        WHEN cash_transaction.flow = 'QRCODE_PIX_OUT' THEN qpo.status
        ELSE 'JUDICIAL_DEBT'
    END AS Status_Trans,


    --,case when pix.status = 'APPROVED' or ted.status = 'APPROVED' or p2p.status = 'APPROVED' or billet.status = 'APPROVED' or qpo.status = 'PAID'
    COUNT(DISTINCT cash_transaction.id) as qtdtransacoes,
    ROUND(SUM(cash_transaction.amount)/100,2) as valor
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
--DATE(cash_transaction.created_at) >= current_date - 2
--DATE(cash_transaction.created_at) >= '2023-01-01' --AND DATE(cash_transaction.created_at) <= CURRENT_DATE
(pix.status = 'APPROVED' or ted.status = 'APPROVED' or p2p.status = 'APPROVED' or billet.status = 'APPROVED' or qpo.status = 'PAID')
GROUP BY 1,2,3,4,5,6
ORDER BY 1
),base_Fraude as (
     select
     trans.*
     ,ClFraude.*
     from base_trans trans
     join base_Cliente_Fraude ClFraude on trans.customer_id = ClFraude.CustomerID
     where type = 'CASH-IN'
     order by 4

)

select
Safra_Tranx
,flow
,type
,MotivoStatus
,sub_classification
,Safra_Abertura
,Safra_Ev
,Flag_Perfil
,flowoperation
,Status_Trans
,case 
     when Status_Trans in ('SCHEDULED') then 'Agendada'
     when Status_Trans in ('APPROVED','PAID') then 'Aprovada'
     when Status_Trans in ('CANCELLED') then 'Cancelada'
     when Status_Trans in ('ERROR','PENDING','CANCELLED_ERROR','CREATED','PROCESSING','REVERSED_ERROR') then 'Erro'
     when Status_Trans in ('EXPIRED') then 'Expirada'
     when Status_Trans in ('DENIED') then 'Negada'
     when Status_Trans in ('REVERSED','REVERSED_PROCESSING') then 'Revertida'
Else 'NA' end as Flag_StatusTrans
,Sum(qtdtransacoes) as Qtd_Transacao
,Sum(valor) as Vl_Transacao

from base_Fraude  tranx
GROUP BY 1,2,3,4,5,6,7,8,9,10,11


;
--------------------- Conta Digital - Saldo Bloqueados Fraude -----------------------------------
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_ContaDigitalSaldoBloq` order by 1 desc

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_ContaDigitalSaldoBloq` AS

with

base_Cliente_Fraude as (

select 
CustomerID
,CPF_Cliente
,MotivoStatus
,sub_classification
,Flag_Perfil
,Safra_Abertura
,Safra_Ev

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`
where sub_classification in ('Golpe','Invasão de conta','Conta Laranja','Conta Fraudada','Fraude Cartão')
and DATE_DIFF(date(current_date),date(DataStatus), Month) = 1
order by 1
)
     select
      ClFraude.CPF_Cliente
     ,Saldo.SaldoConta
     from base_Cliente_Fraude ClFraude
     left join (
    Select
        distinct
        T.numerodocumento as  DOCUMENTO                     -- CPF / CNPJ    
        ,round(sum(T.valor),2) as SaldoConta                   -- SOMA DOS VALORES QUE COMPÕEM O SALDO
        FROM `eai-datalake-data-sandbox.orbitall.tb_topaz` T
        where    status = 'MOVIMENTAÇÃO EXECUTADA'       -- STATUS NECESSARIO DE TRANSACOES CONCLUIDAS    
        and T.data <= CURRENT_DATE()-1          -- FIXAR COM A DATA COM A QUAL SE PRETENDE ANALISAR O SALDO    
        and length(T.numerodocumento) <= 11     -- REMOVER CASO QUEIRA TRAZER TAMBÉM CNPJ
        --and T.numerodocumento = '31001127846'
        group by 1
    )Saldo on Saldo.DOCUMENTO = ClFraude.CPF_Cliente


;

--------------------- Analise_Fraudador - Bloqueados Fraude -----------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Analise_Fraudador` order by 1 desc
/*
select 
sub_classification
,Flag_Contestacao
,TIPO_TRANX
,Faixa_Valores
,sum(VL_TRANX_APR)
,sum(status_confirmed) as Volume
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Analise_Fraudador` 
 where TIPO_TRANX in ('CASH','CREDIT_CARD','DEBIT_CARD','DIGITAL_WALLET')
 group by 1,2,3,4
*/


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Analise_Fraudador` AS

with

base_trans_App as (

        SELECT      
        distinct
        payment.customer_id AS customer_id, 
        date(payment.created_at) AS created_at, 
        payment.sales_channel as Canal,
        case when Client.trusted = 0 then 'No Trusted'else 'Trusted' end as trusted,
        case when payment.order_id = cbk.order_id and cbk.Transaction_Date = date(payment.created_at) then 'Contestado' else 'Nao Contestado' end as Flag_Contestacao,
        payment_transaction.status as StatusTransacao,
        payment_transaction.payment_method AS TIPO_TRANX,
        payment_transaction.gateway_id,
        Case
        When (payment_transaction.transaction_value) >=0   and   (payment_transaction.transaction_value) <=50 	Then '01 000 a 050' 
        When (payment_transaction.transaction_value) >50    and  (payment_transaction.transaction_value) <=100 	Then '02 051 a 100'
        When (payment_transaction.transaction_value) >100    and (payment_transaction.transaction_value) <=150 	Then '03 101 a 150'
        When (payment_transaction.transaction_value) >150    and (payment_transaction.transaction_value) <=200 	Then '04 151 a 200'
        When (payment_transaction.transaction_value) >200    and (payment_transaction.transaction_value) <=250 	Then '05 201 a 250'
        When (payment_transaction.transaction_value) >250    and (payment_transaction.transaction_value) <300 	Then '06 251 a 299'
        When (payment_transaction.transaction_value) = 300 	Then '07 Limite'
        When (payment_transaction.transaction_value) >300    and (payment_transaction.transaction_value) <=350 	Then '08 301 a 350'
        When (payment_transaction.transaction_value) >350    and (payment_transaction.transaction_value) <=400 	Then '09 351 a 400'
        When (payment_transaction.transaction_value) >400    and (payment_transaction.transaction_value) <=450 	Then '10 401 a 450'
        When (payment_transaction.transaction_value) >450    and (payment_transaction.transaction_value) <=500 	Then '11 451 a 500'
        When (payment_transaction.transaction_value) >500    and (payment_transaction.transaction_value) <=550 	Then '12 501 a 550'
        When (payment_transaction.transaction_value) >550    and (payment_transaction.transaction_value) <600	Then '13 551 a 599'
        When (payment_transaction.transaction_value) = 600	Then '14 600'
        When (payment_transaction.transaction_value) >600    and (payment_transaction.transaction_value) <=700 	Then '15 601 a 700'
        When (payment_transaction.transaction_value) >700    and (payment_transaction.transaction_value) <=800 	Then '16 701 a 800'
        When (payment_transaction.transaction_value) >800    and (payment_transaction.transaction_value) <=900 	Then '17 801 a 900'
        When (payment_transaction.transaction_value) >900    and (payment_transaction.transaction_value) <=1000 	Then '18 901 a 1000'
        When (payment_transaction.transaction_value) > 1000  and (payment_transaction.transaction_value) <=3000 Then '19 1001 a 3000'
        When (payment_transaction.transaction_value) > 3000  and (payment_transaction.transaction_value) <=5000 Then '20 3001 a 5000'
        When (payment_transaction.transaction_value) > 5000  and (payment_transaction.transaction_value) <=7000 Then '21 5001 a 7000'
        When (payment_transaction.transaction_value) > 7000  and (payment_transaction.transaction_value) <=9000 Then '22 7001 a 9000'
        When (payment_transaction.transaction_value) > 9000  and (payment_transaction.transaction_value) <=11000 Then '23 9001 a 11000'
        When (payment_transaction.transaction_value) > 11000 and (payment_transaction.transaction_value) <=13000 Then '24 11001 a 13000'
        When (payment_transaction.transaction_value) > 13000 and (payment_transaction.transaction_value) <=15000 Then '25 13001 a 15000'
        When (payment_transaction.transaction_value) > 15000 and (payment_transaction.transaction_value) <=17000 Then '26 15001 a 17000'
        When (payment_transaction.transaction_value) > 17000 and (payment_transaction.transaction_value) <=19000 Then '27 17001 a 19000'
        When (payment_transaction.transaction_value) > 19000 and (payment_transaction.transaction_value) <=20000 Then '28 19001 a 20000'
        When (payment_transaction.transaction_value) > 20000 Then '29 20000>' 
        End as Faixa_Valores,
        SUM(CASE WHEN payment_transaction.status in ('AUTHORIZED','COMPLETED','PRE_AUTHORIZED') THEN payment_transaction.transaction_value ELSE 0 END) AS VL_TRANX_APR,
        SUM(CASE WHEN payment_transaction.status in ('CANCELED_BY_STORE','CANCELED_BY_CUSTOMER','CANCELED_BY_GATEWAY','CANCELLED_BY_GATEWAY','CANCELED_BY_BACKOFFICE')THEN payment_transaction.transaction_value ELSE 0 END) AS VL_TRANX_NEG,
        --SUM(IF(payment_transaction.status in ('AUTHORIZED','SETTLEMENT','REVERSED'),  1, 0)) AS status_confirmed, 
        SUM(IF(payment_transaction.status in ('AUTHORIZED','COMPLETED','PRE_AUTHORIZED'),  1, 0)) AS status_confirmed, 
        SUM(IF(payment_transaction.status in ('CANCELED_BY_STORE','CANCELED_BY_CUSTOMER','CANCELED_BY_GATEWAY','CANCELLED_BY_GATEWAY','CANCELED_BY_BACKOFFICE'),  1, 0)) AS status_Negada, 
        AVG(IF(payment_transaction.status in  ('CANCELED_BY_STORE','CANCELED_BY_CUSTOMER','CANCELED_BY_GATEWAY','CANCELLED_BY_GATEWAY','CANCELED_BY_BACKOFFICE'), 1, 0)) AS status_denied

    FROM`eai-datalake-data-sandbox.payment.payment` payment
    JOIN `eai-datalake-data-sandbox.payment.payment_transaction` payment_transaction ON payment.id = payment_transaction.payment_id
    JOIN `eai-datalake-data-sandbox.payment.payment_instrument` payment_instrument on payment_transaction.payment_instrument_id = payment_instrument.id
    JOIN (SELECT distinct  uuid, trusted FROM `eai-datalake-data-sandbox.core.customers` ) Client on Client.uuid = payment.customer_id
    LEFT JOIN `eai-datalake-data-sandbox.payment.customer_card` customer_card on payment_instrument.uuid = customer_card.uuid
    LEFT JOIN (
        select 
        distinct
        * 
        from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` cbk
        where 
        cbk.Reason = 'Fraud'
        and cbk.Status = 'Open'
        and cbk.Kind = 'Chargeback'
        and date(Transaction_Date) >= current_date - 90 ) cbk on payment.order_id = cbk.order_id   
     
WHERE       

payment_transaction.status in ('AUTHORIZED','COMPLETED','PRE_AUTHORIZED') 

GROUP BY    1,2,3,4,5,6,7,8,9


) 

select
distinct
*
from base_trans_App analise

join (
  
with

base_Bloqueios as (

select 
distinct
CustomerID
,CPF_Cliente
,sub_classification
,Safra_Ev

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` 
where sub_classification in ('Golpe','Invasão de conta','Conta Laranja','Conta Fraudada','Fraude Cartão','Abuso')
and DATE_DIFF(date(current_date),date(DataStatus), Month) = 1
union all
select * from (
  with
  base_Bloqueio as (
          SELECT
          distinct
            a.* 
            ,FORMAT_DATE("%Y%m",date(Lote))as Safra_Massivo
            ,RANK() OVER (PARTITION BY 	CustomerID ORDER BY FORMAT_DATETIME("%d",Lote), Lote  desc) AS Rank_Ult_Bloq
          FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2` a
          order by 1
  ) select 
  CustomerID as CustomerID
,Cast(CPF as string) as CPF_Cliente
,Motivo as sub_classification
,Safra_Massivo as Safra_Ev
  from base_Bloqueio where Rank_Ult_Bloq = 1
)
), base_Bloqueios_Final as (
select 
CustomerID
,CPF_Cliente
,sub_classification
,Safra_Ev
,RANK() OVER (PARTITION BY 	CustomerID ORDER BY Safra_Ev desc) AS Rank_Ult_Bloq
from base_Bloqueios
order by 1
) 

select * from base_Bloqueios_Final 
where Rank_Ult_Bloq = 1 
and sub_classification in ('Abusador Cashback','Abuso','APR_CBK6 - Zaig','Contestacao_PostoRecorrente','Fraude Cartão','Inativos KMV - CBK Latam','NEG_APR_CBK - Zaig')

) base on base.CustomerID = analise.customer_id


;

--------------------- Abusador - Movimentação -----------------------------------
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Analise_Abusador` order by 1 desc

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Analise_Abusador` AS

select 
sub_classification
,Flag_Contestacao
,Canal
,TIPO_TRANX
,Faixa_Valores
,count(distinct customer_id) as QtdCliente
,sum(VL_TRANX_APR) as VLTransacao
,sum(status_confirmed) as Volume
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Analise_Fraudador` 
 where TIPO_TRANX in ('CASH','CREDIT_CARD','DEBIT_CARD','DIGITAL_WALLET','GOOGLE_PAY')
 and sub_classification in ('Abuso')
 group by 1,2,3,4,5

;
--------------------- Chargeback - Movimentação -----------------------------------
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Analise_Chargeback` order by 1 desc

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Analise_Chargeback` AS

select 
sub_classification
,Flag_Contestacao
,Canal
,TIPO_TRANX
,Faixa_Valores
,count(distinct customer_id) as QtdCliente
,sum(VL_TRANX_APR) as VLTransacao
,sum(status_confirmed) as Volume
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Report_Gerencial_Analise_Fraudador` 
 where TIPO_TRANX in ('CASH','CREDIT_CARD','DEBIT_CARD','DIGITAL_WALLET','GOOGLE_PAY')
 and sub_classification in ('Fraude Cartão')
 group by 1,2,3,4,5

