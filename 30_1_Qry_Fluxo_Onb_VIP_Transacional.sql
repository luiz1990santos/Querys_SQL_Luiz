
-- BASE FLUXO ONBOARDING - ALLOWME/ZAIG/ORBITAL
-- 30_1_Qry_Fluxo_Onb_VIP_Transacional

--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Fluxo_VIP_Gorjeta_Tranx` where Cliente_GorjetaVip = '80108884945'


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Fluxo_VIP_Gorjeta_Tranx` AS 

select
distinct

tranx_d.customer_id
,Posto.store_id
,Posto.CNPJ_Loja
,ord.pdv_token
,tranx_d.own_id
,tranx_d.type
,tranx_d.flow
,tranx_dx.payment_method	as Tipo_Pagto
,tranx_d.created_transaction_at as Dt_Trans
--,tranx_d.amount/100 as Vl_Tranx
, case when operation = 'SUBTRACTION' then tranx_d.amount/-100 
else  tranx_d.amount/100 end as Vl_Tranx
,ord.cashback_value/100 as Vl_Cashback


,Case 
  When tranx_d.amount/100 >=0	    and tranx_d.amount/100 <=100 	Then '01 0-100'
  When tranx_d.amount/100 >= 101 	and tranx_d.amount/100 <=250 	Then '02 101-250'
  When tranx_d.amount/100 >= 251 	and tranx_d.amount/100 <300 	Then '03 251-299'
  When tranx_d.amount/100 = 300 	 	                                  Then '04 300'
  When tranx_d.amount/100 >= 301 	and tranx_d.amount/100 <=350 	Then '05 301-350'
  When tranx_d.amount/100 >= 351 	and tranx_d.amount/100 <=600 	Then '05 351-600'
  When tranx_d.amount/100 >= 601 	 	Then '05 >601'
Else '06 Outros' End as Intervalo_Valor_Comportamento
,vip.Nome as NomeVip_QPOS
,vip.CPF as CpfVip_QPOS
,vip_cad.CPF_Vip as Cliente_GorjetaVip
,vip_cad.StatusConta as Status_Conta_GorjetaVip
,case 
when vip.CPF != vip_cad.CPF_Vip and flow = 'APP' then 'OutroVip'  
when vip.CPF = vip_cad.CPF_Vip and flow = 'APP' then 'ProprioVip'  
else 'NA' end as Flag_QueimaPOS
,case when vip_cad.Customer_id = tranx_d.customer_id then 'ClienteVIP' else 'NaoVIP' end as Flag_Cliente
,case
when flow = 'APP' then	'ABASTECIMENTO'
when flow = 'APP_JET_OIL' then	'APP'
when flow = 'APP_ULTRAGAZ' then	'APP'
when flow = 'POS'	then 'APP' 
when flow = 'APP_DELIVERY' then	'APP'
when flow = 'SERVICE' then	'APP'
when flow = 'VOUCHER_UBER' then	'APP'
when flow = 'APP_LATAMPASS' then	'APP_AERIAS'
when flow = 'APP_TUDOAZUL' then	'APP_AERIAS'
when flow = 'APP_AMPM' then	'APP_AMPM'
when flow = 'ON_LINE' then	'APP_OLINE'
when flow = 'ECOMMERCE' then	'APP_OLINE'
when flow = 'POS_QRCODE' then	'APP_QRCODE'
when flow = 'POS_BRCODE' then	'APP_QRCODE'
when flow = 'PDV_QRCODE' then	'APP_QRCODE'
when flow = 'VOUCHER' then	'CONTADIGITAL'
when flow = 'P2P' then	'CONTADIGITAL'
when flow = 'CONCESSION' then	'CONTADIGITAL'
when flow = 'JUDICIAL_DEBT' then	'CONTADIGITAL'
when flow = 'TEF' then	'CONTADIGITAL'
when flow = 'TED' then	'CONTADIGITAL'
when flow = 'PIX' then	'CONTADIGITAL'
when flow = 'BILLET' then	'CONTADIGITAL'
when flow = 'TIP' then	'CONTADIGITAL'
else 'Verificar' end as Flag_Origem
,Case 
when tranx_d.status in ('APPROVED','FINISHED') then 'Aprovada' 
else 'Negada' end as Flag_Trans
,case 
when tokenvip.SituacaoID = 1		then 'Autorizada'
when tokenvip.SituacaoID = 2		then 'Confirmada'
when tokenvip.SituacaoID = 3		then 'Desfeita'
when tokenvip.SituacaoID = 4		then 'Autorização em andamento'
when tokenvip.SituacaoID = 5		then 'Autorização negada pelo L1'
when tokenvip.SituacaoID = 6		then 'Autorização negada pelo L2'
when tokenvip.SituacaoID = 7		then 'Autorização falhou'
when tokenvip.SituacaoID = 8		then 'Confirmação em andamento'
when tokenvip.SituacaoID = 9		then 'Confirmação negada pelo L2'
when tokenvip.SituacaoID = 10		then 'Confirmação falhou'
when tokenvip.SituacaoID = 11		then 'Desfazimento em andamento'
when tokenvip.SituacaoID = 12		then 'Desfazimento negado pelo L2'
when tokenvip.SituacaoID = 13		then 'Desfazimento falhou'
else 'NA' end Flag_Status_Tranx

FROM `eai-datalake-data-sandbox.elephant.transaction`   tranx_d
left join (select * from `eai-datalake-data-sandbox.core.orders` ) ord  on ord.uuid = tranx_d.own_id
left join  `eai-datalake-data-sandbox.loyalty.tblAbasteceAiV2TransacaoControle`  tokenvip on tokenvip.pdvToken = ord.pdv_token
left join `eai-datalake-data-sandbox.loyalty.tblParticipantes`      vip on vip.ParticipanteID = tokenvip.VipParticipanteId
left join (SELECT 
            distinct
            a.uuid as store_id
            ,a.name as Nome_loja
            ,a.document as CNPJ_Loja
            ,a.type as tipo_loja
            ,b.city as cidade
            ,b.state as UF
            ,left(b.latitude,7) as latitude
            ,left(b.longitude,7) as longitude


            FROM `eai-datalake-data-sandbox.backoffice.store` a
            join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id) Posto on Posto.store_id = ord.store_id
join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Fluxo_VIP_Gorjeta` vip_cad on vip_cad.Customer_id = tranx_d.customer_id
left join (
        select 
        distinct
        * 
        from `eai-datalake-data-sandbox.payment.payment_transaction`  tranx  
        join `eai-datalake-data-sandbox.payment.payment`              tranx_d   on tranx_d.id = tranx.payment_id
) tranx_dx on ord.uuid = tranx_dx.order_id

where 
tranx_d.status in ('APPROVED','FINISHED')
--and date(tranx_d.created_transaction_at) >= current_date - 120
and date(tranx_d.created_transaction_at) >= '2023-04-25'
--status in ('APPROVED','FINISHED','CANCELLED')
--------------------------------------------------------------
;

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Fluxo_VIP_Gorjeta_Tranx_TPV` AS 

select
ord.store_id
,posto.Nome_loja
--,date(tranx_d.created_transaction_at) as Dt_Trans
,sum(tranx_d.amount/100) as TPV
,count(tranx_d.amount/100) as Qtd_Trans
,sum(cashback_value/100) as Vl_Cashback

FROM `eai-datalake-data-sandbox.elephant.transaction`   tranx_d
left join (select * from `eai-datalake-data-sandbox.core.orders` ) ord  on ord.uuid = tranx_d.own_id
join (select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Fluxo_VIP_Gorjeta` where FlagCadastroVip = 'CadastroGorjetaVip')posto on posto.store_id
 = ord.store_id
where date(tranx_d.created_transaction_at) >= '2023-04-25'
group by 1,2
;


-- BASE FLUXO ONBOARDING - ALLOWME/ZAIG/ORBITAL
-- 30_1_Qry_Fluxo_Onb_VIP_Transacional



/*
select
distinct
*
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Fluxo_VIP_Gorjeta`
*/


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Fluxo_VIP_Gorjeta` AS 


SELECT
DISTINCT
Vip.*
,Post.Nome_loja
,Post.store_id
,Post.CNPJ_CPF
,Post.tipo_loja
,Post.cidade
,Post.UF

,Case when cast(Vip.cnpj as numeric) =  cast(PostVip.cnpj as numeric) then 'CadastroGorjetaVip' ELSE 'NaoCadastroGorjetaVip' end as FlagCadastroVip
,Case when Zaig.Cpf_Cliente = Vip.CPF_Vip then 'ProcessadoZaig' else 'NaoProcessado' end as FlagMotorFraude
,Zaig.nome
,date(Zaig.data_cadastro) as Dt_CadastroZaig
,Zaig.Flag_Decisao_Motor
,case when clX.document = Vip.CPF_Vip then 'ProcessadoOrbitall' else 'NaoProcessado' end as FlagOrbitall
,clX.status as StatusConta
,clX.risk_analysis_status
,clX.uuid as Customer_id
,date(clX.created_at) as Dt_CadastroOrbitall


FROM ( 
        select 
        distinct
        vips.CPF as CPF_Vip
        ,Date(DT_ADMIS) as DT_ADMIS
        ,Date(DT_DESLIG) as DT_DESLIG
        ,Date(DataCadastro) as DtCadastroPart
        ,deparavip.PostoId
        ,deparavip.cnpj
        from `eai-datalake-data-sandbox.loyalty.tblParticipantes` vips	
        join `eai-datalake-data-sandbox.analytics_report.tb_depara_vips` deparavip on deparavip.CPF = vips.CPF
        where 
        Vip is not null and Inativo = false
) Vip

LEFT JOIN (SELECT DISTINCT * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.20230526_postos_habilitados_gorjetaViP` order by 2
)PostVip on cast(Vip.cnpj as numeric) =  cast(PostVip.cnpj as numeric)

left join (
            Select 
            distinct
              a.id as CodID
              ,a.uuid as store_id
              ,a.name as Nome_loja
              ,a.document as CNPJ_CPF
              ,a.type as tipo_loja
              ,b.city as cidade
              ,b.state as UF
              ,left(b.latitude,7) as latitude
              ,left(b.longitude,7) as longitude
            FROM `eai-datalake-data-sandbox.backoffice.store` a
            join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id) as Post on Post.CodID = PostVip.store_id

left join (
            with
            Base as (
            select
            distinct
              Zaig.*
              ,RANK() OVER (PARTITION BY Zaig.cpf ORDER BY date(data_cadastro) desc) AS Rank_Ult_Decisao
              ,case
              when decisao = "automatically_approved" then 'Aprovado'
              when decisao = "automatically_reproved" then 'Negado'
              else 'NA' end as Flag_Decisao_Motor

            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig`  Zaig
            ) select DISTINCT * from Base where Rank_Ult_Decisao = 1
) Zaig on Zaig.Cpf_Cliente = Vip.CPF_Vip

left join `eai-datalake-data-sandbox.core.customers`             clX on clX.document = Vip.CPF_Vip

          

