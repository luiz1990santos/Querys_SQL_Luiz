--======================================================================================
--> MONITORAMENTO CHAMADOS OPERAÇÃO - CLIENTES COM BLOQUEIO DE PREVENÇÃO A FRAUDE
--======================================================================================

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_retorno_clientes_bloqueados_fraude` AS 
 with
           base_1 as (     
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
              ,op.MOTIVO
              ,op.TMR_DIA
              ,case 
              when op.TMR_DIA <= 1 then '1_até1dia'
              when op.TMR_DIA <= 2 then '2_até2dia'
              when op.TMR_DIA <= 3 then '3_até3dia'
              when op.TMR_DIA <= 4 then '4_até4dia'
              when op.TMR_DIA < 6 then '5_até5dia'
              when op.TMR_DIA >= 6 then '6_meior5dia'
              else 'EmAberto' end as Flag_TMR
              ,op.IND_ATENDIDO
              ,op.NOTA_SATISFACAO
              ,op.NOTA_NPS
              ,op.DESCRICAO
              ,op.RSP_PRINCIPAL
              ,op.RSP_EXTERNO
              ,case
              when RSP_EXTERNO in ('GRUPO DE PREVENÇÃO A FRAUDE','GRUPO DE PREVENÇÃO','GRUPO PREVENÇÃO') then 'Derivado Fraude'
              else 'Outras Areas' end as FlagAreaResp
              ,op.RSP_PONTUAL
              ,RANK() OVER (PARTITION BY op.CPF ORDER BY op.NR_OCORRENCIA desc ) AS Rank_Ult_Cham

            FROM `eai-datalake-data-sandbox.siebel.chamados`      op
            
            WHERE 
            --date(DT_CRIACAO) >= current_date - 180 
            date(DT_CRIACAO) >= '2022-01-01'
            AND RSP_EXTERNO in ('GRUPO DE PREVENÇÃO A FRAUDE','GRUPO DE PREVENÇÃO','GRUPO PREVENÇÃO')
            --AND CPF = '12760051803' 
            order by DT_CRIACAO desc
            ), base_2 as (
            select
            *
            from base_1 
            where Rank_Ult_Cham = 1 
            ), base_2_1 as (
            select
            CPF
            ,COUNT(DISTINCT MOTIVO) AS QTD_TIPOS_MOTIVO
            ,COUNT(NR_OCORRENCIA) AS QTD_CHAMADO_CLIENTE
            from base_1
            GROUP BY 1 ORDER BY 3 DESC       
            ),base_3 as (
                        with
                        baseCL as (
                        select distinct
                        chm.*,
                        cl.document,
                        cl.uuid,
                        cl.created_at,
                        ev.observation,
                        ev.user_name,
                        ev.event_date,
                        cl.status as Status_Conta,
                        RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
                        from  `eai-datalake-data-sandbox.core.customers`            cl    
                        JOIN base_2                                                 chm on chm.CPF = cl.document                                       
                        left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id 
                        where 
                        ev.observation in ('Fraude confirmada','Suspeita de fraude','Bloqueio de cadastro')
                        --and
                        --cl.status in ('BLOCK','BLOCKED')
                        )
                 select * from baseCL where Rank_Ult_Status = 1

            )--, base_3 as (
             select 
             base.* 
            ,case when QTD_TIPOS_MOTIVO > 1 then 'Mais de Um Motivo' else 'Unico Motivo' end as FLAG_MOTIVO
            ,case when QTD_CHAMADO_CLIENTE > 1 Then 'Cliente Recorrente' else '1ª Chamada' end as FLAG_RECORRENCIA
             from base_3 base
             LEFT JOIN base_2_1 cha on base.cpf = cha.cpf



--select observation, count(*) from `eai-datalake-data-sandbox.core.customer_event` group by 1
