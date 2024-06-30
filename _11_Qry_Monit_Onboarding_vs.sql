--======================================================================================
--> MONITORAMENTO CONTESTACAO VS ZAIG - CLIENTES NEGADOS
--======================================================================================
--SELECT * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_zaig_vs_cbk`
--select decisao, count(*)  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` group by 
--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_chargeback_enriquecido_v2`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_zaig_vs_cbk_negados` AS 

with

Base as (

select
distinct
  date(zag.data_cadastro) as data_cadastro
  ,zag.Cpf_Cliente
  ,zag.nome
  ,zag.decisao
  ,zag.razao
  ,zag.tree_score
  ,zag.score_makrosystem 
  ,case 
  when zag.score_makrosystem <= 30 then 'Reprovado'
  when zag.score_makrosystem <= 50 then 'Neutro'
  when zag.score_makrosystem > 50 then 'Aprovado'
  else 'NA' end as Flag_Decisao_Makro
  ,RANK() OVER (PARTITION BY zag.Cpf_Cliente ORDER BY zag.data_cadastro desc) AS Rank_Ult_Decisao  
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig`                              zag
 where date(zag.data_cadastro) >= '2022-04-25'
 and decisao = 'automatically_reproved' -- Reprovado
 
 order by 1,2,9 desc

), base_2 as  ( 
   select 
   *
   from Base where Rank_Ult_Decisao = 1
   ), base_3 as (
     select
        zag.*
        ,cbk.Customer_id
        ,cbk.Flag_Cliente
        ,cbk.Posto
        ,case
        when cbk.Posto_Recorrencia <=50 then '1-Risco Baixo'
        when cbk.Posto_Recorrencia <=100 then '1-Risco Medio'
        when cbk.Posto_Recorrencia <=300 then '1-Risco Alto'
        when cbk.Posto_Recorrencia >301 then '1-Risco Critico'
        else 'NA' end as Flag_Risco_Recorrencia
        ,cbk.Dt_Tranx
        ,date(cbk.data_abertura_conta) as data_abertura_conta
        ,cbk.trusted
        ,cbk.status_Conta
        ,cbk._PARAMETRO_REGRA
        ,cbk.Credit_Card_Number
        ,cbk.Card_Brand
        ,cbk.Banco_Emissor
      from base_2                                                                                      zag
      left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_chargeback_enriquecido_v2`    cbk on zag.Cpf_Cliente = cbk.CPF
 where Dt_Tranx >= '2022-04-25'
   )
   select
   distinct
   data_cadastro
   ,Cpf_Cliente
   ,Customer_id
   ,nome
   ,decisao
   ,razao
   ,Flag_Decisao_Makro
   ,Rank_Ult_Decisao
   ,Flag_Cliente
   ,Flag_Risco_Recorrencia
   ,data_abertura_conta
   ,trusted
   ,status_Conta
   ,_PARAMETRO_REGRA
   ,max(tree_score) as tree_score
   ,max(score_makrosystem) as score_makrosystem
   ,count(distinct Dt_Tranx) as qtd_cbk
   ,count(distinct Posto) as qtd_posto
   ,count(distinct Credit_Card_Number) as qtd_cartao
   ,count(distinct Card_Brand) as qtd_bandeira
   ,count(distinct Banco_Emissor) as qtd_banco
   from base_3
   group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
;
   --======================================================================================
--> MONITORAMENTO CONTESTACAO VS ZAIG - CLIENTES APROVACAO 
--======================================================================================
--SELECT * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_zaig_vs_cbk`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_zaig_vs_cbk` AS 

with

Base as (

select
distinct
  date(zag.data_cadastro) as data_cadastro
  ,zag.Cpf_Cliente
  ,zag.nome
  ,zag.decisao
  ,zag.razao
  ,zag.tree_score
  ,zag.score_makrosystem 
  ,case 
  when zag.score_makrosystem <= 30 then 'Reprovado'
  when zag.score_makrosystem <= 50 then 'Neutro'
  when zag.score_makrosystem > 50 then 'Aprovado'
  else 'NA' end as Flag_Decisao_Makro
  ,RANK() OVER (PARTITION BY zag.Cpf_Cliente ORDER BY zag.data_cadastro desc) AS Rank_Ult_Decisao  
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig`                              zag
 where date(zag.data_cadastro) >= '2022-04-25'
 and decisao = 'automatically_approved' -- Aprovado
 order by 1,2,9 desc

), base_2 as  ( 
   select 
   *
   from Base where Rank_Ult_Decisao = 1
   )
     select
        zag.*
        ,cbk.Customer_id
        ,cbk.Flag_Cliente
        ,cbk.Posto
        ,case
        when cbk.Posto_Recorrencia <=50 then '1-Risco Baixo'
        when cbk.Posto_Recorrencia <=100 then '1-Risco Medio'
        when cbk.Posto_Recorrencia <=300 then '1-Risco Alto'
        when cbk.Posto_Recorrencia >301 then '1-Risco Critico'
        else 'NA' end as Flag_Risco_Recorrencia
        ,cbk.Dt_Tranx
        ,date(cbk.data_abertura_conta) as data_abertura_conta
        ,cbk.trusted
        ,cbk.status_Conta
        ,cbk._PARAMETRO_REGRA
        ,cbk.Credit_Card_Number
        ,cbk.Card_Brand
        ,cbk.Banco_Emissor
      from base_2                                                                                      zag
      left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_chargeback_enriquecido_v2`    cbk on zag.Cpf_Cliente = cbk.CPF
 where Dt_Tranx >= '2022-04-25'

