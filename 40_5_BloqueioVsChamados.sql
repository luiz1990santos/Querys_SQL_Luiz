--==========================================================================================================
-- Base Bloqueio vs Chamados
--==========================================================================================================

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_vs_Chamados_Reversao` where cpf = '00207200092'

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_vs_Chamados_Reversao` AS 


            with
                base as (
                select distinct
                cl.document as Cpf,
                cl.uuid as customer_id,
                cast(cl.created_at as TIMESTAMP) as dt_abertura,
                cl.status,
                ev.observation as motivo,
                ev.user_name,
                cast(ev.event_date as TIMESTAMP) as event_date,
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
                RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
            from  `eai-datalake-data-sandbox.core.customers`   cl                                           
            left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id 
            where cl.status = 'ACTIVE'
                ),base_sembloqueio as ( 
                    select * from base where Rank_Ult_Status = 1 
             ),base_reversao as (
             select
                cl.* ,
                cl_sembloqueio.MotivoBloqueio as StatusConta_Atual,
                cl_sembloqueio.event_date as Dt_Reversao,
                FORMAT_DATE("%Y%m",cl_sembloqueio.event_date)as Safra_Reversao,
            case when cl_sembloqueio.customer_id = cl.customer_id  and  cl_sembloqueio.event_date > cl.Dt_Bloqueio  then 'Reversao' else 'NaoRevertido' end as flag_reversao,
            case when cl_sembloqueio.customer_id = cl.customer_id  and  cl_sembloqueio.event_date > cl.Dt_Bloqueio and cl.MotivoBloqueio = 'Fraude confirmada'  then 'ReversaoFraudeConfirmada' else 'NA' end as flag_reversao_FC,
            CASE
                WHEN DATETIME_DIFF(TIMESTAMP(cl_sembloqueio.event_date), TIMESTAMP(cl.Dt_Bloqueio), DAY) <=0 THEN '01_<MesmoDia'
                WHEN DATETIME_DIFF(TIMESTAMP(cl_sembloqueio.event_date), TIMESTAMP(cl.Dt_Bloqueio), DAY) <=5  THEN '02_<1a5DIAS'
                WHEN DATETIME_DIFF(TIMESTAMP(cl_sembloqueio.event_date), TIMESTAMP(cl.Dt_Bloqueio), DAY) <=15 THEN '03_<6a15DIAS'
                WHEN DATETIME_DIFF(TIMESTAMP(cl_sembloqueio.event_date), TIMESTAMP(cl.Dt_Bloqueio), DAY) <=29 THEN '04_<16a29DIAS'
                WHEN DATETIME_DIFF(TIMESTAMP(cl_sembloqueio.event_date), TIMESTAMP(cl.Dt_Bloqueio), DAY) <=35 THEN '05_>30a35DIAS' 
                WHEN DATETIME_DIFF(TIMESTAMP(cl_sembloqueio.event_date), TIMESTAMP(cl.Dt_Bloqueio), DAY) <=60 THEN '06_<36a60DIAS' 
                WHEN DATETIME_DIFF(TIMESTAMP(cl_sembloqueio.event_date), TIMESTAMP(cl.Dt_Bloqueio), DAY) <=90 THEN '06_<61a90DIAS' 
                WHEN DATETIME_DIFF(TIMESTAMP(cl_sembloqueio.event_date), TIMESTAMP(cl.Dt_Bloqueio), DAY) <=180 THEN '06_<91a180DIAS' 
                WHEN DATETIME_DIFF(TIMESTAMP(cl_sembloqueio.event_date), TIMESTAMP(cl.Dt_Bloqueio), DAY) >181 THEN '06_>180DIAS' 
                WHEN cl_sembloqueio.event_date is null THEN '07_BloqueioMatido'
            END AS Flag_Aging_Reversao,
            CASE
                WHEN DATETIME_DIFF(TIMESTAMP(cl.Dt_Bloqueio), TIMESTAMP(cl.dt_abertura), DAY) =0 THEN '01_<MesmoDia'
                WHEN DATETIME_DIFF(TIMESTAMP(cl.Dt_Bloqueio), TIMESTAMP(cl.dt_abertura), DAY) <=5  THEN '02_<1a5DIAS'
                WHEN DATETIME_DIFF(TIMESTAMP(cl.Dt_Bloqueio), TIMESTAMP(cl.dt_abertura), DAY) <=15 THEN '03_<6a15DIAS'
                WHEN DATETIME_DIFF(TIMESTAMP(cl.Dt_Bloqueio), TIMESTAMP(cl.dt_abertura), DAY) <=29 THEN '04_<16a29DIAS'
                WHEN DATETIME_DIFF(TIMESTAMP(cl.Dt_Bloqueio), TIMESTAMP(cl.dt_abertura), DAY) <=36 THEN '05_>30a36DIAS' 
                WHEN DATETIME_DIFF(TIMESTAMP(cl.Dt_Bloqueio), TIMESTAMP(cl.dt_abertura), DAY) <=365 THEN '06_até 1 Ano' 
                WHEN DATETIME_DIFF(TIMESTAMP(cl.Dt_Bloqueio), TIMESTAMP(cl.dt_abertura), DAY) >365  THEN '07_>= + 1 Anos' 
                WHEN cl_sembloqueio.event_date is null THEN '08_BloqueioMatido'
            END AS Flag_Aging_Bloqueio,
            DATETIME_DIFF(TIMESTAMP(cl_sembloqueio.event_date), TIMESTAMP(cl.Dt_Bloqueio), DAY)  as DiasSemBloqueio
            ,cl_sembloqueio.event_date as Bl_Anterior
            ,cl.Dt_Bloqueio as Bl_Ultimo

             from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CLientes_Bloqueados_Fraude` cl
             left join base_sembloqueio cl_sembloqueio on cl_sembloqueio.customer_id = cl.customer_id  and  cl_sembloqueio.event_date > cl.Dt_Bloqueio
             --where cl_sembloqueio.customer_id = cl.customer_id  and  cl_sembloqueio.event_date > cl.Dt_Bloqueio
             
             ), baseConsolidada_Chamado1 as (
                 SELECT 
              distinct
              op.NR_OCORRENCIA
              ,op.TIPO_CHAMADO
              ,op.CPF
              ,op.CNPJ
              ,op.STATUS
              ,op.DT_CRIACAO
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
            --WHERE 
            --date(DT_CRIACAO) >= current_date - 180 
            --AND 
            --RSP_EXTERNO in ('GRUPO DE PREVENÇÃO A FRAUDE','GRUPO DE PREVENÇÃO A FRAUDE','GRUPO PREVENÇÃO')

            ), baseConsolidada_Chamado2 as (
                select 
                distinct
                * 
                from baseConsolidada_Chamado1 where Rank_Ult_Cham = 1
             ), baseConsolidada_Chamado3 as (
                 select
                 distinct
                 bl.* 
                 ,case
                 when bl.Cpf  = op.CPF and DATETIME(op.DT_ULT_ALTER_OCORR)>= DATETIME(bl.Dt_Bloqueio) then 'Com Contato' else 'Sem Contato' end as Flag_Chamado
                 ,op.NR_OCORRENCIA
                 ,op.DESCRICAO

                 from base_reversao bl
                 left join baseConsolidada_Chamado2 op on bl.Cpf  = op.CPF and DATETIME(op.DT_ULT_ALTER_OCORR)>= DATETIME(bl.Dt_Bloqueio)
             ), baseConsolidada_Chamado4 as (
             select 
             distinct
              a.*
              ,RANK() OVER (PARTITION BY a.Cpf ORDER BY Dt_Bloqueio  desc) AS Rank_Ult_Final
              from baseConsolidada_Chamado3 a 
             ) 
             select
             distinct
             * 
             from baseConsolidada_Chamado4 
             where Rank_Ult_Final = 1

             