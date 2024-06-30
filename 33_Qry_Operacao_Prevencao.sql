
-- Acompanhamento Operacao 
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_OperacaoPrevencao` where Flag_Ultima_Acao = 'Verificar'

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_OperacaoPrevencao`  AS 

with

Base_clientes_historico as (

select
    distinct
        cl.uuid as  customer_id
        ,cl.full_name as Nome
        ,cl.document as CPF_Cliente
        ,cl.status as StatusConta
        ,cl.email
        ,en.street as Rua
        ,en.neighborhood as Bairro
        ,en.city as Cidade_Cliente
        ,en.state as UF_Cliente
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
        ,Ev.status as StatusEvento
        ,ev.observation as MotivoBloqueio
        ,case
            when ev.observation in ('Suspeita de fraude','Fraude confirmada','Bloqueio de cadastro') then 'BloqueioFraude'
            when ev.observation not in ('Suspeita de fraude','Fraude confirmada','Bloqueio de cadastro') then 'OutrosBloqueio'
            when ev.observation is null then 'SemBloqueio'
        else 'Verificar' end as Flag_Bloqueio
        ,ev.user_name as Analista
        ,case 
        when ev.user_name in ('Pedro Paulo Jesus Dos Santos','Livia Soares Silva','Murilo Luis Gomes','Willian Araujo Cruz','Gabriela Da Silva Neves') then 'ACERT'
        when ev.user_name in ('Jair Cardoso Vieira','Pedro Paulo Jesus Dos Santos') then 'EAIMassivo'
        else 'EAI' end as FlagOperacao
        ,ev.event_date as DataStatus
        ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Ev
        ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual

    FROM `eai-datalake-data-sandbox.core.customers`             cl
    left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
    left join (select * from `eai-datalake-data-sandbox.core.customer_event` 
              where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT')
    )  Ev on ev.customer_id = cl.id
    where 
    date(ev.event_date) >= '2023-01-01'
    --date(ev.event_date) >= current_date - 90 
    --and cl.document = '14638337767'
    --and ev.user_name = 'Gabriela Da Silva Neves'
    --and ev.observation in ('Fraude confirmada','Suspeita de fraude','Bloqueio de cadastro')
and ev.user_name in ('Alessandro Fernandes Dos Santos',
                    'Daniela Vasconcelos De Aquino',
                    'Elisandra dos Santos Peres',
                    'Gabriela Da Silva Neves',
                    'Gislaine Juliana Da Paz Nogueira',
                    'Jair Cardoso Vieira',
                    'Livia Soares Silva',
                    'Pedro Paulo Jesus Dos Santos',
                    'Rafael Mendes Campos De Figueiredo Murta',
                    'Renato Monteiro de Oliveira Russo',
                    'Thalita Barbosa Silva Robello')


    order by 1
), Cliente_UltimoStatus as (
select
*
from Base_clientes_historico
Where Rank_Ult_Atual = 1
), Cliente_PenutimoStatus as (
select
*
from Base_clientes_historico
Where Rank_Ult_Atual = 2
), Base_Consolidada as (
select
distinct
base.CPF_Cliente
,base.customer_id
,base.Safra_Abertura
,base.StatusConta 
,base.Analista as Analista
,base.FlagOperacao as FlagOperacao
,base.MotivoBloqueio 
,base.Flag_Bloqueio

,Penst.StatusEvento as StatusEvento_P
,Penst.MotivoBloqueio as MotivoBloqueio_P
,case
    when Penst.MotivoBloqueio in ('Suspeita de fraude','Fraude confirmada','Bloqueio de cadastro') then 'BloqueioFraude'
    when Penst.MotivoBloqueio not in ('Suspeita de fraude','Fraude confirmada','Bloqueio de cadastro') then 'OutrosBloqueio'
    when Penst.MotivoBloqueio is null then 'SemBloqueio'
else 'Verificar' end as Flag_Bloqueio_P
,Penst.Analista as Analista_P
,Penst.FlagOperacao as FlagOperacao_P
,Penst.Safra_Ev as Safra_Ev_P
,Penst.Rank_Ult_Atual as Rank_Ult_Atual_P

,utst.StatusEvento as StatusEvento_U
,utst.MotivoBloqueio as MotivoBloqueio_U
,case
    when utst.MotivoBloqueio in ('Suspeita de fraude','Fraude confirmada','Bloqueio de cadastro') then 'BloqueioFraude'
    when utst.MotivoBloqueio not in ('Suspeita de fraude','Fraude confirmada','Bloqueio de cadastro') then 'OutrosBloqueio'
    when utst.MotivoBloqueio is null then 'SemBloqueio'
else 'Verificar' end as Flag_Bloqueio_U
,utst.Analista as Analista_U
,utst.FlagOperacao as FlagOperacao_U
,utst.Safra_Ev as Safra_Ev_U
,utst.Rank_Ult_Atual as Rank_Ult_Atual_U


,case 
when Penst.MotivoBloqueio is null and utst.MotivoBloqueio is not null Then 'Bloqueio'
when utst.MotivoBloqueio in ('Suspeita de fraude','Fraude confirmada','Bloqueio de cadastro') Then 'Bloqueio'
when Penst.MotivoBloqueio is not null and utst.MotivoBloqueio is null then 'Desbloqueio'
when Penst.MotivoBloqueio is null and utst.MotivoBloqueio is null then 'AcaoDesbloqueio'
else 'Verificar' end as Flag_Ultima_Acao

from Base_clientes_historico base
left join Cliente_UltimoStatus utst on utst.CPF_Cliente = base.CPF_Cliente
left join Cliente_PenutimoStatus Penst on Penst.CPF_Cliente = base.CPF_Cliente

) 
--select * FROM Base_Consolidada where Flag_Ultima_Acao = 'Verificar'
SELECT
*
FROM Base_Consolidada


