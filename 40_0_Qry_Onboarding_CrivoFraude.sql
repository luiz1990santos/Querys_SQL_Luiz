--==========================================================================================================
-- Base Crivo de Fraude - Contas Aprovadas/Negadas
--==========================================================================================================
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Fraude_Zaig_01` where Cpf_Cliente = '00187208972'

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Fraude_Zaig_01` AS 

select
distinct
    data_cadastro
    ,cl.CustomerID 
    ,case 
    when cl.StatusConta is not null then 'Zaig_Orbitall'
    when cl.StatusConta is null then 'So_Zaig'
    end as Flag_Processo
    ,case
    when zaig.decisao = "automatically_approved" then 'Aprovado'
    when zaig.decisao = "automatically_reproved" then 'Negado'
    else 'Pendente' end as Flag_Decisao_Motor
    ,zaig.esteira
    ,CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=0   THEN '01_00-Hoje'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=1   THEN '02_01-Ontem'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=10   THEN '03_00-10DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=30   THEN '04_11-30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=60   THEN '05_31-60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=90   THEN '06_61-90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=180  THEN '07_91-180DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=364  THEN '08_180-1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) >=365  THEN '09_+1ANO'
    END AS Flag_DtCadastro
    ,CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.Dt_Abertura)), DAY) <=0   THEN '01_00-Hoje'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.Dt_Abertura)), DAY) <=1   THEN '02_01-Ontem'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.Dt_Abertura)), DAY) <=10   THEN '03_00-10DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.Dt_Abertura)), DAY) <=30   THEN '04_11-30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.Dt_Abertura)), DAY) <=60   THEN '05_31-60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.Dt_Abertura)), DAY) <=90   THEN '06_61-90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.Dt_Abertura)), DAY) <=180  THEN '07_91-180DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.Dt_Abertura)), DAY) <=364  THEN '08_180-1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.Dt_Abertura)), DAY) >=365  THEN '09_+1ANO'
    END AS Flag_TempodeConta
    ,Case 
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<18   Then '01  MenorIdade'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=20  Then '02  18a20anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=25  Then '04  21a25anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=30  Then '05  26a30anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=35  Then '06  31a35anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=40  Then '07  36a40anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=45  Then '08  41a45anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=50  Then '09  46a50anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=55  Then '10 51a55anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=60  Then '11 56a60anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=65  Then '12 61a65anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=70  Then '13 66a70anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=75  Then '14 71a75anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=80  Then '15 76a80anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)<=85  Then '16 81a85anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(loyalty.Dt_Nascimento), year)>85   Then '17 >86anos'  
    End as Faixa_Idade
    ,case when loyalty.Dt_Nascimento = cl.Dt_Nascimento then 'OK_KMV_Dt_Nascimento' else 'No_Macht' end as Flag_Dt_Nascimento
    ,zaig.ScoreZaig
    ,case
    when zaig.ScoreZaig <= 7 then 'Aprovado'
    when zaig.ScoreZaig between 8 and 9 then 'Aprovado em situações especiais'
    when zaig.ScoreZaig >=10 then 'Negado'
    end as Flag_ScoreZaig
    ,zaig.Cpf_Cliente
    ,cl.NomeCliente
    ,zaig.email
    ,substring(zaig.email,1,STRPOS(zaig.email,'@')) as Desc_Email
    ,substring(zaig.email,STRPOS(zaig.email,'@'),25) as Provedor_Email
    ,zaig.ddd
    ,zaig.Num_Celuar
    ,zaig.Uf_DDD
    ,zaig.End_Num
    ,zaig.Bairro_Cep
    ,zaig.UF
    ,zaig.ip
    ,End_Completo
    ,cl.MotivoStatus
    ,cl.id
    ,cl.StatusConta
    ,gps_latitude
    ,gps_longitude
    ,Flag_Email_NaoVal
    ,Flag_Celular_NaoVal
    ,Flag_NomeMae_CaixaAlta
    ,case when cast(bio.customer_id as numeric) = cast(cl.id as numeric) then 'BioValidada' else 'BioNaoValidada' end as Flag_Bio
    ,case when  ord.uuid  = cbk.order_id then 'Contestado' else 'NaoContestado' end as Flag_Contestado
    ,RANK() OVER (PARTITION BY zaig.Cpf_Cliente ORDER BY data_cadastro desc) AS Rank_Ult_Decisao
from ( 
    with 
    base as (
          select 
          distinct
          Cpf_Cliente
          ,esteira
          ,data_cadastro
          ,natural_person_id
          ,email
          ,ddd
          ,ddd||'-'||numero as Num_Celuar
          ,estado ||'-'||ddd as Uf_DDD
          ,rua ||','|| numero_9 as End_Num
          ,bairro ||'-'|| cep as Bairro_Cep
          ,estado ||'-'|| cep as UF_Cep
          ,rua ||','|| numero_9 ||'-'|| bairro||'-'|| cep ||'-'|| estado as End_Completo
          ,estado as UF
          ,ip
          ,score_makrosystem
          ,tree_score as ScoreZaig
          ,decisao
          ,gps_latitude
          ,gps_longitude
          ,case when indicators like '%Not_validated_email%' then 'EmailNaoValidado' else 'NA' end as Flag_Email_NaoVal
          ,case when indicators like '%Not_validated_phone%' then 'CelularNaoValidado' else 'NA' end as Flag_Celular_NaoVal
          ,case when indicators like '%name_and_email_and_mother_name_full_uppercase%' then 'CaixaAltaNomeMae' else 'NA' end as Flag_NomeMae_CaixaAlta

          ,RANK() OVER (PARTITION BY cpf ORDER BY data_cadastro desc) AS Rank_Ult_Decisao

          from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` 
          where
          date(data_cadastro) >= current_date - 60
          --and decisao = "automatically_approved"
          --and Cpf_Cliente = '61969036672' 
          order by 2 desc
          ) 
          select 
          * 
          from base 
          where Rank_Ult_Decisao = 1
          ) zaig
left join (
          with
          base_cl as (
            select
            distinct
            cl.id
            ,cl.uuid as  CustomerID
            ,cl.full_name as Nome
            ,cl.document as CPF_Cliente
            ,cl.status as StatusConta
            ,cl.birth_date as Dt_Nascimento
            ,cl.full_name as NomeCliente
            ,cl.email
            ,en.zipcode as Cep
            ,en.street as Rua
            ,en.neighborhood as Bairro
            ,en.city as Cidade_Cliente
            ,en.state as UF_Cliente
            ,cl.created_at as Dt_Abertura
            ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
            ,case
            when cl.trusted = 1 then 'Trusted'
            else 'NaoTrusted' end as Flag_Trusted
            ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual
            ,CASE
            WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
            WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
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
            where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT'))Ev on ev.customer_id = cl.id
          --where cl.email like "%programa%"
          )
            select 
            distinct
            *
            from base_cl where Rank_Ult_Atual = 1) cl on cl.CPF_Cliente = zaig.Cpf_Cliente and Rank_Ult_Decisao = 1
left join (with
            base_bio as (
            select 
            customer_id
            ,status
            ,user_name as Analista
            ,event_date as Dt_Inclusao
            ,RANK() OVER (PARTITION BY customer_id ORDER BY event_date desc) AS Rank_Ult_Atual
            from `eai-datalake-data-sandbox.core.customer_event` where status = 'FACIAL_BIOMETRICS_VALIDATED'
            order by 1,5
            ) select * from base_bio where Rank_Ult_Atual = 1) bio on cast(bio.customer_id as numeric) = cast(cl.id as numeric)

left join `eai-datalake-data-sandbox.core.orders`             ord       on cast(ord.customer_id as numeric) = cast(cl.id as numeric)
left join ( select
            distinct
            *
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` 
            where Reason = 'Fraud'and Status = 'Open' and Kind = 'Chargeback'
          ) cbk on ord.uuid  = cbk.order_id
left join
        (select
        distinct
          loyalty.ParticipanteID
          ,loyalty.DataCadastro as Dt_CadastroLoyalty
          ,FORMAT_DATETIME("%Y%m",loyalty.DataCadastro) as Safra_KM
          ,loyalty.CPF
          ,loyalty.Nome
          ,loyalty.Email
          ,loyalty.DatadeNascimento as Dt_Nascimento
          ,loyalty.TipoOrigemID
          ,loyalty.flgPreCadastro
          ,loyalty.Vip
          ,loyalty.Inativo
        from `eai-datalake-data-sandbox.loyalty.tblParticipantes` loyalty
        where loyalty.Inativo = false ) loyalty on loyalty.CPF = zaig.Cpf_Cliente
--where
--cl.status in ('ACTIVE','BLOCKED')
--and  
--cl.uuid  = 'CUS-1d0b4a9f-2a34-43ab-9ba9-08064743cb48'
;

--==========================================================================================================
-- Base Crivo de Fraude - Contas Aprovadas 
--==========================================================================================================

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Fraude_Cubo_01` AS 

select
distinct
Cpf_Cliente
,date(data_cadastro) as Dt_Cadastro
,Flag_Decisao_Motor
,MotivoStatus
,StatusConta
,esteira
,Faixa_Idade
,Flag_Dt_Nascimento
,Flag_TempodeConta
,Flag_Contestado
,Flag_Bio
,End_Completo
,Uf_DDD
,email
,Desc_Email
,Flag_Email_NaoVal
,Flag_Celular_NaoVal
,Flag_NomeMae_CaixaAlta
,count(distinct Cpf_Cliente) Qtd_Cpf

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Fraude_Zaig_01`

where 
Flag_Decisao_Motor = 'Aprovado'
and Flag_Email_NaoVal  = 'EmailNaoValidado'
and Flag_Celular_NaoVal = 'CelularNaoValidado'

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
order by 1





