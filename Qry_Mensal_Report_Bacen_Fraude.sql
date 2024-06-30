with

baseConsolidada_Chamado1 as (

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
            WHERE 
            date(DT_CRIACAO) between '2024-03-01' and '2024-04-30'
            and SUBCLASSIFICACAO in ('NÃO RECONHEÇO A CHAVE CADASTRADA', 'CLIENTE NÃO RECONHECE A CONTA CRIADA')
            --and
             --CPF = '08940255623'
            --date(DT_CRIACAO) >= current_date - 180 
            --AND 
            --RSP_EXTERNO in ('GRUPO DE PREVENÇÃO A FRAUDE','GRUPO DE PREVENÇÃO A FRAUDE','GRUPO PREVENÇÃO')

), base_final as (
select
Op.NR_OCORRENCIA
,op.STATUS as Status_NR_Ocorrencia
,op.CLASSIFICACAO
,op.SUBCLASSIFICACAO
,Op.CPF
,cl.Nome
,cl.status
,cl.motivo
,cl.event_date
,Op.DT_CRIACAO
,Op.DT_ENCERRAMENTO
,Op.DT_ULT_ALTER_OCORR
,case 
when cl.Cpf = Op.CPF then 'ClienteEAI'
when cast(Km.CPF as numeric) = cast(Op.CPF as numeric) then 'ClienteKMV' 
Else 'NaoCliente' end as Flag_Cliente
from baseConsolidada_Chamado1 Op 
left join ( with base as (
                select distinct
                cl.document as Cpf,
                cl.uuid as customer_id,
                cl.full_name as Nome,
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
            left join `eai-datalake-data-sandbox.core.customer_event`   ev on ev.customer_id = cl.id 
            where ev.observation = 'Fraude confirmada'
            --and cl.document = '62688809350'
            )
             select * from base where Rank_Ult_Status = 1 
) cl on cl.Cpf = Op.CPF
left join (
  SELECT
distinct 
CPF
,SobreNome
,Email
,DatadeNascimento
,NomedaMae
,Inativo
,date (DataCadastro) as dt_cadastro
,date(DataAtualizacao) as dt_atualizacao

,PostoID
,Vip
,Chave
,flgPreCadastro
,NO_SEQ_PESS_FUNC_POSTO
,CartaoFidTAM
,DigCartaoFidTAM


FROM `eai-datalake-data-sandbox.loyalty.tblParticipantes` 
where
Inativo = false
) Km on cast(Km.CPF as numeric) = cast(Op.CPF as numeric)
where Rank_Ult_Cham = 1
)
select
CPF	
,Nome
,SUBCLASSIFICACAO as MotivoChamado
,motivo as MotivoBloqueio
--,event_date as DtBloqueio
--,DT_CRIACAO as flag_Dt_Aberturax
--,DT_ULT_ALTER_OCORR as flag_Dt_Fechamentox

,case 
when DT_CRIACAO < DT_ULT_ALTER_OCORR then DT_CRIACAO
else DT_ULT_ALTER_OCORR end as flag_Dt_Abertura
,case 
when DT_ULT_ALTER_OCORR > DT_CRIACAO then DT_ULT_ALTER_OCORR
else DT_CRIACAO end as flag_Dt_Fechamento
--,DATETIME_DIFF(TIMESTAMP(DT_ULT_ALTER_OCORR), TIMESTAMP(DT_CRIACAO), DAY) Qtd_dias

/*
,DT_ENCERRAMENTO as DtFechamento
,DT_ULT_ALTER_OCORR as DtAtualizacao
*/
From base_final
where
Flag_Cliente = 'ClienteEAI'
