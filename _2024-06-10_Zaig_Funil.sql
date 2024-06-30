
/*



*/
----------------------------------------------------------------
-- CADASTROS FASE LIGHT                                        |
----------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao_Light` AS 

with

Base_dados_Zaig as (
          select
          distinct
          *
          from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` 
          where date(data_cadastro) >= current_date - 90
          and decisao <> 'pending'
          and esteira = 'Abastece Aí - Light'
), Base_Classificacao_decisao_Zaig as (
          select
          distinct
           REPLACE(REPLACE(cpf,'.', ''),'-', '') as CPF
          ,cpf as CPF_CLiente
          ,esteira
          ,data_cadastro
          ,case
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 0 then 'M0'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 1 then 'M-1'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 2 then 'M-2'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 3 then 'M-3'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 4 then 'M-4'
            else 'Outros' end as Flag_Filtro_Periodo
          ,decisao
          ,razao
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
          /*,nome
          ,email
          ,ddd
          ,numero
          ,nome_da_mae
          ,rua
          ,numero_9
          ,bairro
          ,cidade
          ,estado
          ,cep
          ,pais
          ,session_id
          ,modelo_do_dispositivo
          ,plataforma
          ,ip
          ,pais_do_ip
          ,ip_tor
          ,gps_latitude
          ,gps_longitude
          ,data_device_scan*/
          ,tree_score	
          ,score_makrosystem
          ,case 
          when score_makrosystem <= 30 then 'Reprovado'
          when score_makrosystem <= 50 then 'Neutro'
          when score_makrosystem > 50 then 'Aprovado'
          else 'NA' end as Flag_Decisao_Makro
          /*,case 
              when esteira = 'Abastece Aí' then 'KMV - Full'
              when esteira = 'Abastece Aí - Light' then 'KMV - Light'
            end as Flag_Fase
          */

          from Base_dados_Zaig
          --where
          --Cpf_Cliente = '31423729897'
) , Base_Classificacao_decisao_Zaig_2 as (
select 

cast(a.CPF as NUMERIC) as CPF
,a.esteira
,a.data_cadastro
,a.Flag_Filtro_Periodo
,a.decisao
,a.razao
,a.Flag_Decisao_Motor
,a.Flag_Decisao_Regra
,a.tree_score
,a.score_makrosystem
,a.Flag_Decisao_Makro
,CAST(a.CPF AS STRING) AS CPF_Completo
--,a.Flag_Fase
 
from Base_Classificacao_decisao_Zaig a

), Base_Classificacao_decisao_Zaig_3 as (
select
a.* 
,RANK() OVER (PARTITION BY a.CPF,date(a.data_cadastro),esteira ORDER BY EXTRACT(time FROM a.data_cadastro) desc) AS Rank_Ult_Decisao
from Base_Classificacao_decisao_Zaig_2 a
)
  select * from Base_Classificacao_decisao_Zaig_3
  WHERE Rank_Ult_Decisao = 1
;

----------------------------------------------------------------
-- CADASTROS FASE FULL                                         |
----------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao_Full` AS 

with

Base_dados_Zaig as (
          select
          distinct
          *
          from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` 
          where date(data_cadastro) >= current_date - 90
          and decisao <> 'pending'
          and esteira = 'Abastece Aí'
), Base_Classificacao_decisao_Zaig as (
          select
          distinct
           REPLACE(REPLACE(cpf,'.', ''),'-', '') as CPF
           ,esteira
          ,data_cadastro
          ,case
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 0 then 'M0'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 1 then 'M-1'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 2 then 'M-2'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 3 then 'M-3'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 4 then 'M-4'
            else 'Outros' end as Flag_Filtro_Periodo
          ,decisao
          ,razao
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
          /*,nome
          ,email
          ,ddd
          ,numero
          ,nome_da_mae
          ,rua
          ,numero_9
          ,bairro
          ,cidade
          ,estado
          ,cep
          ,pais
          ,session_id
          ,modelo_do_dispositivo
          ,plataforma
          ,ip
          ,pais_do_ip
          ,ip_tor
          ,gps_latitude
          ,gps_longitude
          ,data_device_scan*/
          ,tree_score	
          ,score_makrosystem
          ,case 
          when score_makrosystem <= 30 then 'Reprovado'
          when score_makrosystem <= 50 then 'Neutro'
          when score_makrosystem > 50 then 'Aprovado'
          else 'NA' end as Flag_Decisao_Makro
          /*,case 
              when esteira = 'Abastece Aí' then 'KMV - Full'
              when esteira = 'Abastece Aí - Light' then 'KMV - Light'
            end as Flag_Fase
          */

          from Base_dados_Zaig
          --where
          --Cpf_Cliente = '31423729897'
) , Base_Classificacao_decisao_Zaig_2 as (
select 

cast(a.CPF as NUMERIC) as CPF
,a.esteira
,a.data_cadastro
,a.Flag_Filtro_Periodo
,a.decisao
,a.razao
,a.Flag_Decisao_Motor
,a.Flag_Decisao_Regra
,a.tree_score
,a.score_makrosystem
,a.Flag_Decisao_Makro
,CAST(a.CPF AS STRING) AS CPF_Completo
--,a.Flag_Fase
 
from Base_Classificacao_decisao_Zaig a

), Base_Classificacao_decisao_Zaig_3 as (
select
a.* 
,RANK() OVER (PARTITION BY a.CPF,date(a.data_cadastro),esteira ORDER BY EXTRACT(time FROM a.data_cadastro) desc) AS Rank_Ult_Decisao
from Base_Classificacao_decisao_Zaig_2 a
)
  select * from Base_Classificacao_decisao_Zaig_3
  WHERE Rank_Ult_Decisao = 1
;

---------------------------------------------------------------------------------------------------------

----------------------------------------------------------------
-- ANALITICO CONSOLIDADO                                       |
----------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_Consolidada` AS 
  with base_consolidada as (
  SELECT 
        Fase_Light.*,
        Perfil.StatusConta,
        case 
          when Fase_Light.CPF_Completo = Perfil.CPF_Cliente and Perfil.StatusConta = 'ACTIVE' then 'Light - Conta Completa - Ativa'
          when Fase_Light.CPF_Completo = Perfil.CPF_Cliente and Perfil.StatusConta = 'BLOCKED' then 'Light - Conta Completa - Bloqueada'
          when Fase_Light.CPF_Completo = Perfil.CPF_Cliente and Perfil.StatusConta = 'MINIMUM_ACCOUNT' then 'Light - Conta Básica'
          when decisao = 'automatically_reproved' then 'Light - Conta básica Negada'
          else 'Light - Oportunidade'
        end as Flag_Status
    FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao_Light` as Fase_Light
    left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` as Perfil
    on Fase_Light.CPF_Completo = Perfil.CPF_Cliente
  UNION ALL
  SELECT  
        Fase_Full.*,
        Perfil.StatusConta,
        case 
          when Fase_Full.CPF_Completo = Perfil.CPF_Cliente and Perfil.StatusConta = 'ACTIVE' then 'Full - Conta Completa - Ativa'
          when Fase_Full.CPF_Completo = Perfil.CPF_Cliente and Perfil.StatusConta = 'BLOCKED' then 'Full - Conta Completa - Bloqueada'
          when Fase_Full.CPF_Completo = Perfil.CPF_Cliente and Perfil.StatusConta = 'MINIMUM_ACCOUNT' then 'Full - Conta Básica'
          when decisao = 'automatically_reproved' then 'Full - Conta Completa Negada'
          else 'Full - Oportunidade'
        end as Flag_Status
    FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao_Full` as Fase_Full
    left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` as Perfil
    on Fase_Full.CPF_Completo = Perfil.CPF_Cliente
  ) select * from base_consolidada
  /*
  select 
    StatusConta, 
    Flag_Status, 
    count(*) VolumePropostas, count(distinct CPF_Completo) as VolumeCPFs 
  from base_consolidada group by 1,2
  order by 2 desc
  */


  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_Consolidada` where Flag_Status = 'Light - Oportunidade'