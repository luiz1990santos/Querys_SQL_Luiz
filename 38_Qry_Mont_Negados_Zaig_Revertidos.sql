
------------------------------------------------------------------------------------------------
-- RELATÓRIO ZAIG 
------------------------------------------------------------------------------------------------
-- CLIENTES NEGADOS ZAIG REVERTIDOS
------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_Zaig_Neg_Rev` AS 

with

Base_dados_Zaig as (
          select
          distinct
          *
          from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` 
          where 
          --date(data_cadastro) >= '2023-10-01'
          date(data_cadastro) >= current_date - 180
          --and decisao <> 'pending'
          --and Cpf_Cliente = '09439678640'
),Base_Classificacao_decisao_Zaig as (
          select
          distinct
          REPLACE(REPLACE(cpf,'.', ''),'-', '') as CPF
          ,esteira
          ,data_cadastro
          --,DATE_DIFF(date(current_date),date(data_cadastro), Month) 

          ,case
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 0 then 'M0'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 1 then 'M-1'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 2 then 'M-2'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 3 then 'M-3'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 4 then 'M-4'
            else 'Outros' end as Flag_Filtro_Periodo

          ,RANK() OVER (PARTITION BY CPF ORDER BY EXTRACT(time FROM data_cadastro)) AS UltimaDecisao
          ,razao
          ,decisao
          ,case
            when decisao = "automatically_approved" then 'Aprovado'
            when decisao = "automatically_reproved" then 'Negado'
          else 'NA' end as Flag_Decisao_Motor
          ,case
            --when razao Like  "%ph3a%" then 'Negado PH3A'
            when razao Like  "%bureau_data%" then 'Negado Cadastro'
            --when razao Like  "%fa_risk%" then 'Negado Score Makro'
            when decisao = "automatically_approved" then 'Aprovado'
            --when decisao = "automatically_reproved" and razao not Like  "%fa_risk%" then 'Negado'
            when decisao = "automatically_reproved" and razao not Like  "%bureau_data%" then 'Negado'
            --when decisao = "automatically_reproved" and razao not Like  "%ph3a%" then 'Negado'
          else 'NA' end as Flag_Decisao_Regra
          ,tree_score	
          ,score_makrosystem
          ,case 
          when score_makrosystem <= 30 then 'Reprovado'
          when score_makrosystem <= 50 then 'Neutro'
          when score_makrosystem > 50 then 'Aprovado'
          else 'NA' end as Flag_Decisao_Makro

          from Base_dados_Zaig
          --where
          --Cpf_Cliente = '01503263606'
          --and
          -- decisao = "automatically_approved" 

), Base_Classificacao_decisao_Zaig2 as (
  select
  distinct
    bd.CPF
    ,esteira
    ,Date(data_cadastro) as data_cadastro
    ,Flag_Filtro_Periodo
    ,decisao
    ,razao
    ,Flag_Decisao_Motor
    ,Flag_Decisao_Regra
  from Base_Classificacao_decisao_Zaig bd
  where UltimaDecisao = 1
    --and bd.CPF = '09439678640'
), base_Zaig_Neg as (
  select
  distinct
    bd.CPF
    ,cl.StatusConta
    ,case when cl.StatusConta is null then 'SemContaCriada' else 'ComContaCriada' end as Flag_Conta
    ,bd.esteira
    ,bd.data_cadastro
    ,bd.Flag_Filtro_Periodo
    ,bd.decisao

  from Base_Classificacao_decisao_Zaig2 bd
  left join (
            with
                baseCL as (
                select distinct
                    cl.document,
                    cl.uuid,
                    cl.created_at,
                    cl.status as StatusConta,
                    ev.observation,
                    ev.user_name,
                    ev.event_date,
                    cl.status as Status_Conta,
                    RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
                from  `eai-datalake-data-sandbox.core.customers`            cl                                      
                left join (select * from `eai-datalake-data-sandbox.core.customer_event` where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT'))   Ev on ev.customer_id = cl.id 
                --where 
                --ev.observation in ('Fraude confirmada','Suspeita de fraude','Bloqueio de cadastro')
                --and
                --cl.status in ('BLOCK','BLOCKED')
            ) select * from baseCL where Rank_Ult_Status = 1) cl on  cast(bd.CPF as numeric) = cast(cl.document as numeric)
 where bd.decisao = 'automatically_reproved'
 --AND Flag_Filtro_Periodo IN ('M0','M-1','M-2','M-3')
 order by 1
),base_Zaig_Neg2 as (
select
bd.*
,cmd.CPF as cpf_cmd
,case 
  when cast(cmd.CPF as numeric) = cast(bd.CPF as numeric) then 'ComChamado' else 'SemChamado' end as FlagChamado
--,cmd.DESCRICAO

from base_Zaig_Neg bd
left join (
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

          --WHERE 
          --date(DT_CRIACAO) >= current_date - 180 
          --date(DT_CRIACAO) >= '2022-01-01'
          --AND RSP_EXTERNO in ('GRUPO DE PREVENÇÃO A FRAUDE','GRUPO DE PREVENÇÃO','GRUPO PREVENÇÃO')
           --CPF = '12760051803' 
          --order by DT_CRIACAO desc
          ) cmd on cmd.CPF = bd.CPF and date(bd.data_cadastro) >= date(cmd.DT_CRIACAO)
)
  select
  distinct
  * 
  from base_Zaig_Neg2

;

------------------------------------------------------------------------------------------------
-- RELATÓRIO ZAIG CUBO 
------------------------------------------------------------------------------------------------
-- BASE ANALITICA - CLIENTES NEGADOS ZAIG REVERTIDOS
------------------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Consolidado_Zaig_Neg_Rev` AS 

select
distinct
decisao
,date(data_cadastro) as Dt_Cadastros
,StatusConta
,Flag_Conta
,FlagChamado
,count(distinct CPF) as qtdCliente
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_Zaig_Neg_Rev` 
group by 1,2,3,4,5



