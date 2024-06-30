----------------------------------BLQOEUIOS VS CHAMADOS
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_bloqeuios_vs_Chamados`
-- select * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo` where CPF = 55145051549

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_bloqeuios_vs_Chamados` AS 

with

base_bloqueios as (

                    SELECT 
                    distinct
                    cl.uuid as  CustomerID
                    ,cl.full_name as Nome
                    ,cl.document as CPF
                    ,cl.email
                    ,en.street as Rua
                    ,en.neighborhood as Bairro
                    ,en.city as Cidade
                    ,en.state as UF
                    ,FORMAT_DATE("%d-%m-%Y",cl.created_at)as DataCriacao
                    ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
                    ,FORMAT_DATE("%Y",cl.created_at)as Ano
                    ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Bloqueio
                    --,cl.created_at as DataCriacao
                    ,ph.area_code as DDD
                    ,ph.number as Telefone
                    ,ph.type as Tipo_Telefone
                    --,cl.trusted as Trusted
                    ,case
                    when cl.trusted = 1 then 'Trusted' else 'No Trusted' end as Trusted
                    ,cl.status as Status_Conta
                    ,Ev.status as Status_Conta_EV
                    --,cl.risk_analysis_status as RiskAnalysis
                    ,ev.observation as MotivoStatus
                    ,ev.sub_classification
                    ,ev.sub_classification_obs
                    ,DATE(ev.event_date) as DataStatus
                    --,DATETIME(ev.event_date)
                    ,ev.user_name as UsuarioStatus
                    ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date ) AS Rank_Ult_Prim
                    ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual
                    ,CASE
                    WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
                    WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
                    WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
                    WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
                    WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
                    ELSE 'SUL'
                    END AS RegiaoCliente

            FROM `eai-datalake-data-sandbox.core.customers`             cl
            left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id 
            left join `eai-datalake-data-sandbox.core.customer_phone`   id on id.customer_id = cl.id
            left join `eai-datalake-data-sandbox.core.phone`            ph on id.phone_id = ph.id and ph.type = 'MOBILE'
            left join (select * from `eai-datalake-data-sandbox.core.customer_event`  where status not in ('FACIAL_BIOMETRICS_VALIDATED', 'TEMPORARY_PERMISSION_CASH_OUT','FACIAL_BIOMETRICS_REJECTED','BLOCK_LIST_UNBOUND','BLOCK_LIST_BOUND','FACIAL_BIOMETRICS_NOT_VALIDATED'))   Ev on ev.customer_id = cl.id 
            WHERE  
            ph.type = 'MOBILE'
            and FORMAT_DATE("%Y",ev.event_date) >= '2022'
            and Ev.status in ('BLOCK','UNBLOCK')
            --and cl.document ='04746805253'
            --and ev.observation ='Fraude confirmada'
            order by 1,20 desc
), base_clientes_bloqueado as (
            select
            *
            from base_bloqueios 
            where Rank_Ult_Atual = 1 
            --and Status_Conta = 'BLOCKED' 
            and Status_Conta_Ev = 'BLOCK'
), base_clientes_desbloqueado as (
            select
            *
            from base_bloqueios 
            where Rank_Ult_Atual = 1 
            --and Status_Conta = 'ACTIVE' 
            and Status_Conta_Ev = 'UNBLOCK'
), base_consolidado as (

            select
            a.*
            ,DATETIME_DIFF(DATETIME(b.DataStatus), DATETIME(a.DataStatus), DAY) as Flag_tratativa
            ,case when a.CustomerID = b.CustomerID then 'ClienteAtivo' else 'ClienteBloqueado' end as Flag_StatusCliente
            from base_clientes_bloqueado a
            left join base_clientes_desbloqueado b on a.CustomerID = b.CustomerID
), Base_Chamados as (     
            SELECT 
              distinct
              op.NR_OCORRENCIA
              ,op.TIPO_CHAMADO
              ,op.CPF
              ,op.CNPJ
              ,op.STATUS
              ,bl.Flag_StatusCliente
              ,FORMAT_DATE("%Y%m",op.DT_CRIACAO)as Safra_Chamado
              ,DATE(bl.DataStatus) as DataStatus
              ,DATE(op.DT_CRIACAO) as DT_CRIACAO
              ,op.DT_ENCERRAMENTO
              ,op.DT_ULT_ALTER_OCORR
              ,op.CENTRAL_RECEBIDA
              ,op.CANAL
              ,op.PRODUTO_SERVICO
              ,op.CLASSIFICACAO
              ,op.SUBCLASSIFICACAO
              ,op.MOTIVO as MOTIVO_CHAMADO
              ,op.TMR_DIA
              ,case 
              when op.TMR_DIA < 1 then '1_até1dia'
              when op.TMR_DIA < 2 then '2_até2dia'
              when op.TMR_DIA < 3 then '3_até3dia'
              when op.TMR_DIA < 4 then '4_até4dia'
              when op.TMR_DIA < 5 then '5_até5dia'
              when op.TMR_DIA > 6 then '6_meior5dia'
              else 'EmAberto' end as Flag_TMR
              ,op.IND_ATENDIDO
              ,op.NOTA_SATISFACAO
              ,op.NOTA_NPS
              ,op.DESCRICAO
              ,op.RSP_PRINCIPAL
              ,op.RSP_EXTERNO
              ,op.RSP_PONTUAL
              ,RANK() OVER (PARTITION BY op.CPF ORDER BY op.NR_OCORRENCIA desc ) AS Rank_Ult_Cham
            
            FROM `eai-datalake-data-sandbox.siebel.chamados`            op
            join base_consolidado                                       bl on bl.CPF  = op.CPF 
            --WHERE
            --date(DT_CRIACAO) >= current_date - 60
            --FORMAT_DATE("%Y",DT_CRIACAO) >= '2022'
            --AND 
            --RSP_EXTERNO in ('GRUPO DE PREVENÇÃO A FRAUDE','GRUPO DE PREVENÇÃO A FRAUDE','GRUPO PREVENÇÃO')
            --and op.NR_OCORRENCIA = '1-33331759030'
            order by 3, 24
), Base_Chamados_2 as (
            select
              op.*
            from Base_Chamados op
            join base_consolidado                                       bl on bl.CPF  = op.CPF and DATE(op.DT_CRIACAO)>= DATE(bl.DataStatus)
            where Rank_Ult_Cham = 1
), Base_BloqueioMassivo as (
  with
  base_Bloqueio as (
          SELECT
          distinct
            a.* 
            ,FORMAT_DATE("%Y%m",date(Lote))as Safra_Massivo
            ,RANK() OVER (PARTITION BY 	CustomerID ORDER BY FORMAT_DATETIME("%d",Lote), Lote  desc) AS Rank_Ult_Bloq
          FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo` a
          order by 1
  ) select * from base_Bloqueio where Rank_Ult_Bloq = 1 order by 5 desc
), base_consolidada as (
select
a.*
,Safra_Chamado
,RSP_EXTERNO
,b.NR_OCORRENCIA
,Safra_Massivo
,case when b.CPF = a.CPF then 'AbriuChamado' else 'NaoAbriuChamado' end as Flag_Cham
,case when bl.CustomerID = a.CustomerID then bl.Motivo else a.MotivoStatus end as Flag_Bloq_Massivo
from base_consolidado a
left join Base_Chamados_2 b on b.CPF = a.CPF
left join Base_BloqueioMassivo bl on bl.CustomerID = a.CustomerID
) 
select
distinct
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
,count(distinct CustomerID) qtd_Cliente
from base_consolidada
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

;

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cliente_AberturaVsBloqueio` AS 

with
base as (
  select distinct
    cl.document as Cpf,
    cl.uuid as customer_id,
    cl.created_at,
    FORMAT_DATETIME('%Y%m',cl.created_at) as Safra_Abertura,
    --FORMAT_DATETIME('%Y%m',ev.event_date) as Safra_Bloqueio,
    case
    when FORMAT_DATETIME('%Y%m',ev.event_date) is null then 'NaoBloqueado'
    else FORMAT_DATETIME('%Y%m',ev.event_date) end as Safra_Bloqueio,

    --ev.observation as MotivoBloqueio,

    ev.sub_classification,
    ev.sub_classification_obs,

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

    ev.user_name,
    ev.event_date,
    RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
  from  `eai-datalake-data-sandbox.core.customers`   cl                                           
  left join (select * from `eai-datalake-data-sandbox.core.customer_event`  where status not in ('FACIAL_BIOMETRICS_VALIDATED', 'TEMPORARY_PERMISSION_CASH_OUT','FACIAL_BIOMETRICS_REJECTED','BLOCK_LIST_UNBOUND','BLOCK_LIST_BOUND','FACIAL_BIOMETRICS_NOT_VALIDATED'))  Ev on ev.customer_id = cl.id 
  --where ev.observation in ('Fraude confirmada','Suspeita de fraude','Bloqueio de cadastro', null,'')
), base_consolidada as (

  select * 
  from base 
where Rank_Ult_Status = 1
)
select
Safra_Abertura
,Safra_Bloqueio
,MotivoBloqueio
,sub_classification
,sub_classification_obs
,Flag_Bloqueio
,count(distinct Cpf) as Qtd

from base 
group by 1,2,3,4,5,6

;

----------------------------------------------------------------------------------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_ContasAtivas_vs_Bloqueios` order by 2 desc


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_ContasAtivas_vs_Bloqueios` AS 

with

Base_Clientes as (

SELECT 
distinct
           cl.uuid as  CustomerID
          ,cl.full_name as Nome
          ,cl.document as CPF
          ,cl.email
          ,en.street as Rua
          ,en.neighborhood as Bairro
          ,en.city as Cidade
          ,en.state as UF
          ,FORMAT_DATE("%d-%m-%Y",cl.created_at)as DataCriacao
          ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
          ,FORMAT_DATE("%Y",cl.created_at)as Ano
          --,cl.created_at as DataCriacao
          ,ph.area_code as DDD
          ,ph.number as Telefone
          ,ph.type as Telefone
          ,cl.trusted as Trusted
          ,cl.status as Status_Conta
          ,Ev.status as Status_Evento
          ,cl.risk_analysis_status as RiskAnalysis
          ,ev.observation as MotivoStatus
          ,ev.event_date as DataStatus
          ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Bloqueio
          ,ev.user_name as UsuarioStatus
          ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date  desc) AS Rank_Ult_Atual
          ,CASE
           WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
           WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
           WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
           WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
           WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
           ELSE 'SUL'
           END AS REGIAO

          FROM `eai-datalake-data-sandbox.core.customers`             cl
          left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
          left join `eai-datalake-data-sandbox.core.customer_phone`   id on id.customer_id = cl.id
          left join `eai-datalake-data-sandbox.core.phone`            ph on id.phone_id = ph.id 
          left join (select * from `eai-datalake-data-sandbox.core.customer_event`  where status not in ('FACIAL_BIOMETRICS_VALIDATED', 'TEMPORARY_PERMISSION_CASH_OUT','FACIAL_BIOMETRICS_REJECTED','BLOCK_LIST_UNBOUND','BLOCK_LIST_BOUND','FACIAL_BIOMETRICS_NOT_VALIDATED'))  Ev on ev.customer_id = cl.id
where 
date(cl.created_at) >= '2022-01-01' 
--cl.uuid = 'CUS-f0589f17-810f-413b-b04a-ea6385af9ed2'


) select 
distinct
Safra_Abertura
,Safra_Bloqueio
,Status_Conta
,Status_Evento
,MotivoStatus
,RiskAnalysis
--,CustomerID
,count(distinct CustomerID) as Qtd_Total
,case when Status_Conta = 'ACTIVE' then count(distinct CustomerID)  ELSE 0 END Flag_Qtd_Ativos
,case when Status_Conta = 'BLOCKED' and MotivoStatus not in ('Fraude confirmada','Suspeita de fraude','Óbito','Blocklist','Perda e Roubo') then count(distinct CustomerID)  ELSE 0 END Flag_Qtd_Bloqueio
,case when MotivoStatus in ('Fraude confirmada','Suspeita de fraude','Óbito','Blocklist','Perda e Roubo') then count(distinct CustomerID)  ELSE 0 END Flag_Qtd_Bloq_Fraude
,case when Status_Conta = 'INACTIVE' then count(distinct CustomerID)   ELSE 0 END Flag_Qtd_Inativos

 from Base_Clientes 
 where Rank_Ult_Atual = 1 
 group by 1,2,3,4,5,6
 order by 1 desc
