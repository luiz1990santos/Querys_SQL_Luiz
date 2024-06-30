--==========================================================================================================
-- Base Cadastro Cartão
--==========================================================================================================

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cadastro_Cartao` where Flag_Contestacao <> ''


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cadastro_Cartao` AS 

select
date (card.created_at)as Dt_Cadastro
,CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=1 THEN '00_<1DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=3 THEN '01_<3DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=6 THEN '02_<6DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=9 THEN '03_<9DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=12 THEN '04_<12DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=15 THEN '05_<15DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=20 THEN '06_<20DIAS'
    else 'Verificar'
END AS Temp_Cadastro
,cl.CustomerID
,cl.StatusConta
,CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.Dt_Abertura), DAY) <=1 THEN '00_<1DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.Dt_Abertura), DAY) <=5 THEN '01_<5DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.Dt_Abertura), DAY) <=30 THEN '02_<30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.Dt_Abertura), DAY) <=60 THEN '03_<60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.Dt_Abertura), DAY) <=90 THEN '04_<90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.Dt_Abertura), DAY) <=120 THEN '05_<120DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.Dt_Abertura), DAY) <=160 THEN '06_<160DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.Dt_Abertura), DAY) <=190 THEN '07_<190DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.Dt_Abertura), DAY) <=220 THEN '08_<220DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.Dt_Abertura), DAY) <=260 THEN '09_<260DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.Dt_Abertura), DAY) <=290 THEN '10_<290DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.Dt_Abertura), DAY) <=365 THEN '11_1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.Dt_Abertura), DAY) >=365 THEN '12_+1ANO'
    else '13_NaoTemConta'
END AS Temp_Conta

,cl.Nome as Nome_Cliente
,card.document as CPF_Cliente
,cl.Faixa_Idade
,card.status
,UF_DDD
,case 
when card.status = 'VERIFIED' then 'Cadastrado'
when card.status = 'EXCLUDED' then 'Excluido'
when card.status = 'PROCESSOR_DECLINED' then 'Negado Emissor'
when card.status = 'FAILED' then 'Erro'
when card.status = 'GATEWAY_REJECTED' then 'Negado PayPal'
else 'NA' end as Flag_Status
,card.bin
,bin.Emissor_do_Banco
,bin.Sub_marca
,bin.Tipo_de_Card
,last_four_digits
,cl.MotivoStatus
,cl.Analista
,cl.Dt_Bloqueio
,case when ord_cbk.customer_id = cl.id then 'Contestado' else 'NaoContestado' end as Flag_Contestacao
,case when cast(ord_cbk.Card_Ult_4_cbk as numeric) = cast(card.last_four_digits as numeric) then 'CartaoContestado' else 'NaoContestado' end as Flag_CartaoContestado
,count(distinct card.id) as qtd_Tetativas
,count(distinct last_four_digits) as qtd_cartao
,count(distinct card.document) as qtd_cliente

from `eai-datalake-data-sandbox.payment.customer_card` card
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos`  bin on CAST(bin.BIN AS STRING) = card.bin
left join (
with
    base_cl as (
    select
    distinct
    cl.id
    ,cl.uuid as  CustomerID
    ,cl.full_name as Nome
    ,cl.document as CPF_Cliente
    ,cl.birth_date as DataNacimento
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
    ,cl.email
    ,en.zipcode as Cep
    ,en.street || en.number  as endereco
    ,en.neighborhood as Bairro
    ,en.city as Cidade_Cliente
    ,en.state as UF_Cliente
    ,en.state ||'_'|| ph.area_code as UF_DDD
    ,ph.area_code ||'-'|| ph.number as DDD_Telefone
    ,ph.type 
    ,cl.created_at as Dt_Abertura
    ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
    ,case
    when cl.trusted = 1 then 'Trusted'
    else 'NaoTrusted' end as Flag_Trusted
    ,CASE
    WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
    WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
    WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
    WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
    WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
    ELSE 'NAOINDENTIFICADO'
    END AS RegiaoCliente
    ,cl.status as StatusConta
    ,cl.risk_analysis_status as StatusOrbitall
    ,Ev.status as StatusEvento
    ,ev.user_name as Analista
    ,ev.observation as MotivoStatus
    ,ev.event_date as Dt_bloqueio
    ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Ev
    ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual

    FROM `eai-datalake-data-sandbox.core.customers`             cl
    left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
    left join (select distinct * from `eai-datalake-data-sandbox.core.customer_phone` )   id on id.customer_id = cl.id
    join (select distinct * from `eai-datalake-data-sandbox.core.phone`  where type = 'MOBILE' and number is not null )  ph on id.phone_id = ph.id 
    left join (select * from `eai-datalake-data-sandbox.core.customer_event` 
    where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT'))Ev on ev.customer_id = cl.id
              )
              select 
              distinct
              *
               from base_cl where Rank_Ult_Atual = 1
)cl      on cl.CPF_Cliente = card.document
left join ( select
            distinct
            ord.*
            ,cbk.*
            ,substr(cbk.Credit_Card_Number, 13,4) as Card_Ult_4_cbk
            from `eai-datalake-data-sandbox.core.orders`             ord       
            join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico`  cbk on ord.uuid = cbk.order_id
            where Reason = 'Fraud'and Status = 'Open' and Kind = 'Chargeback' 
          ) ord_cbk on ord_cbk.customer_id = cl.id

where   
date(card.created_at) >= current_date - 7
--and cl.uuid = 'CUS-216d32e7-7ba4-4b43-84d1-701bcc9b2c8e'

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
order by 2