--======================================================================================
--> CONSOLIDAÇÃO BASE ZAIG UPLOAD
--======================================================================================


/*
with data_zaig as (
SELECT 
    cast(created_at as timestamp) as data_cadastro
FROM `eai-datalake-data-sandbox.onboarding.zaig` 
), fuso as ( select TIMESTAMP(DATETIME(data_cadastro,'America/Sao_Paulo')) as data_cadastro from data_zaig )
select min(data_cadastro), max(data_cadastro) from fuso;
*/

-------------------------------------------------------------------
-- TABELA DA API ZAIG                                             |
-------------------------------------------------------------------

-- CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_API_Zaig`  AS
insert into `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_API_Zaig`
 with base_api_zaig as (
    SELECT 
        JSON_VALUE(api, '$.company_name') AS esteira,
        JSON_VALUE(api, '$.id') AS natural_person_id,
        JSON_VALUE(api, '$.document_number') AS cpf,
        JSON_VALUE(api, '$.name') AS nome,
        JSON_VALUE(api, '$.birthdate') AS nascimento,
        JSON_VALUE(api, '$.natural_person_key') AS natural_person_key,
        JSON_VALUE(api, '$.emails[0].email') AS email,
        JSON_VALUE(api, '$.phones[0].area_code') AS ddd,
        JSON_VALUE(api, '$.phones[0].number') AS numero,
        JSON_VALUE(api, '$.mother_name') AS nome_da_mae,
        JSON_VALUE(api, '$.address.street') AS rua,
        JSON_VALUE(api, '$.address.number') AS numero_9,
        JSON_VALUE(api, '$.address.neighborhood') AS bairro,
        JSON_VALUE(api, '$.address.city') AS cidade,
        JSON_VALUE(api, '$.address.uf') AS estado,
        JSON_VALUE(api, '$.address.postal_code') AS cep,
        JSON_VALUE(api, '$.address.country') AS pais,
        JSON_VALUE(api, '$.registration_date') AS data_cadastro,
        JSON_VALUE(api, '$.created_at') AS created_at,
        JSON_VALUE(api, '$.analysis_status_events[0].new_status') AS decisao,
        JSON_VALUE(api, '$.analysis_status_events[0].analysis_output.reason') AS razao,
        --JSON_VALUE(api, '$.analysis_status_events[0].analysis_output.credilink.death.is_dead') AS is_dead,
        JSON_VALUE(api, '$.analysis_status_events[0].analysis_output.credilink.basic_data.gender') AS sexo,
        CONCAT(
            (SELECT STRING_AGG(
                CONCAT(
                    JSON_VALUE(variable, '$.enum')
                ), 
                ' | '
            ) FROM UNNEST(JSON_EXTRACT_ARRAY(api, '$.analysis_status_events[0].analysis_output.indicators')) AS variable)
        ) AS indicators,
        JSON_VALUE(api, '$.analysis_status_events.analysis_output.ph3a.title') || ' | ' || JSON_VALUE(api, '$.analysis_status_events[0].analysis_output.ph3a.description') AS analisys_ph3a,
        JSON_VALUE(api, '$.source.session_id') AS session_id,
        JSON_VALUE(api, '$.source.platform') AS platform,
        JSON_VALUE(api, '$.source.ip') AS ip,
        JSON_VALUE(api, '$.analysis_status_events[0].analysis_output.device_scan_data') AS device_scan_data,
        JSON_VALUE(api, '$.analysis_status_events[0].analysis_output.score') AS score,
        JSON_VALUE(api, '$.analysis_status_events[0].analysis_output.checker_rufra_data.score') AS score_Rufra,
        JSON_VALUE(api, '$.analysis_status_events[0].analysis_output.checker_rufra_data.pepvip.user.pep_indicator') AS indicador_pep,
        JSON_VALUE(api, '$.analysis_status_events[0].analysis_output.checker_rufra_data.pepvip.user.pep_indicator') AS indicador_vip,
        JSON_VALUE(api, '$.analysis_status_events[0].analysis_output.checker_rufra_data.risk_level') AS level_risco,
        JSON_VALUE(api, '$.analysis_status_events[0].analysis_output.checker_rufra_data.rufra_reason') AS decisao_rufra,
        JSON_VALUE(api, '$.analysis_status_events[0].analyst_name') AS nome_analista,
        JSON_VALUE(api, '$.analysis_status_events[0].analyst_email') AS email_analista,
        JSON_VALUE(api, '$.analysis_status_events[0].analyst_key') AS chave_analista,
        document as Cpf_Cliente,
        -- api
    FROM (
        SELECT 
            REPLACE(REPLACE(REPLACE(api, "None", "'None'"), "True", "true"), "False", "false") as api,
            document
        FROM `eai-datalake-prd.onboarding.zaig`

    ) 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38
    ) select * from base_api_zaig 
    where TIMESTAMP(data_cadastro) >= (select max(TIMESTAMP(data_cadastro)) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_API_Zaig` )
    --where esteira = 'Abastece Aí'
    --and date(data_cadastro) >= '2024-08-12' 
    --order by data_cadastro
;



-------------------------------------------------------------------
-- TABELA DW ZAIG V2                                              |
-------------------------------------------------------------------
-- CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_V2`  AS
insert into `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_V2`
    select 
        distinct 
           esteira, 
           natural_person_id, 
           cpf, 
           nome, 
           nascimento, 
           natural_person_key, 
           email, 
           cast(ddd as int64) as ddd, 
           cast(numero as int64) as numero, 
           nome_da_mae, 
           rua, 
           numero_9, 
           bairro, 
           cidade, 
           estado, 
           cep, 
           pais, 
           cast(data_cadastro as timestamp) as DT_REGISTO_ZAIG_HRAMERICANO,
           DATETIME(TIMESTAMP(data_cadastro), 'America/Sao_Paulo') AS data_cadastro, 
           decisao, 
           razao, 
           sexo, 
           indicators, 
           analisys_ph3a, 
           session_id, 
           null as modelo_do_dispositivo,
           platform as plataforma,
           ip,
           --null as pais_do_ip,
           --null as ip_tor,
           --null as gps_latitude,
           --null as gps_longitude,
           device_scan_data as data_device_scan, 
           cast(score as INTEGER) as score, 
           score_Rufra, 
           indicador_pep, 
           indicador_vip, 
           level_risco, 
           decisao_rufra,
           nome_analista, 
           email_analista, 
           chave_analista, 
           null as score_makrosystem,
           Cpf_Cliente

    from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_API_Zaig` 
    where TIMESTAMP(data_cadastro) >= (select max(TIMESTAMP(data_cadastro)) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_V2` )
;


-------------------------------------------------------------------
-- TABELA DW FASE LIGHT                                           |
-------------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Light`  AS
    select 
        * 
    from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_V2` 
    where esteira = 'Abastece Aí - Light'
    and data_cadastro >= '2024-08-12'
;


-------------------------------------------------------------------
-- TABELA DW FASE FULL                                            |
-------------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Full`  AS
    select 
        * 
    from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_V2` 
    where esteira = 'Abastece Aí'
    and data_cadastro >= '2024-08-12'
;


-------------------------------------------------------------------
-- TABELA DW CONSOLIDADO                                          |
-------------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Consolidado` as 
    SELECT 
        DISTINCT 
            esteira, 
            natural_person_id, 
            cpf, 
            nome, 
            CAST(NULL AS STRING) AS nascimento, 
            CAST(NULL AS STRING) AS natural_person_key, 
            email, 
            ddd, 
            numero, 
            nome_da_mae, 
            rua, 
            numero_9, 
            bairro, 
            cidade, 
            estado, 
            cep, 
            pais, 
            DT_REGISTO_ZAIG_HRAMERICANO, 
            CAST(data_cadastro AS TIMESTAMP) AS data_cadastro, 
            decisao, 
            razao, 
            --CAST(NULL AS STRING) AS sexo, 
            indicators, 
            CAST(NULL AS STRING) AS analisys_ph3a, 
            session_id, 
            CAST(modelo_do_dispositivo AS STRING) AS modelo_do_dispositivo, 
            plataforma, 
            ip, 
            data_device_scan, 
            tree_score AS score, 
            CAST(NULL AS STRING) AS score_Rufra, 
            CAST(NULL AS STRING) AS indicador_pep, 
            CAST(NULL AS STRING) AS indicador_vip, 
            CAST(NULL AS STRING) AS level_risco, 
            CAST(NULL AS STRING) AS decisao_rufra, 
            CAST(NULL AS STRING) AS nome_analista, 
            CAST(NULL AS STRING) AS email_analista, 
            score_makrosystem, 
            Cpf_Cliente
    FROM 
        `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` 
    WHERE 
        DATE(data_cadastro) <= '2024-08-11'

    UNION ALL 

    SELECT 
        esteira, 
        natural_person_id, 
        cpf, 
        nome, 
        nascimento, 
        natural_person_key, 
        email, 
        ddd, 
        numero, 
        nome_da_mae, 
        rua, 
        numero_9, 
        bairro, 
        cidade, 
        estado, 
        cep, 
        pais, 
        DT_REGISTO_ZAIG_HRAMERICANO, 
        CAST(data_cadastro AS TIMESTAMP) AS data_cadastro, 
        decisao, 
        razao, 
        --sexo, 
        indicators, 
        analisys_ph3a, 
        session_id, 
        CAST(modelo_do_dispositivo AS STRING) AS modelo_do_dispositivo, 
        plataforma, 
        ip, 
        data_device_scan, 
        score, 
        score_Rufra, 
        indicador_pep, 
        indicador_vip, 
        level_risco, 
        decisao_rufra, 
        nome_analista, 
        email_analista, 
        --chave_analista, 
        score_makrosystem, 
        Cpf_Cliente
    FROM 
        `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Light`

    UNION ALL 

    SELECT 
        esteira, 
        natural_person_id, 
        cpf, 
        nome, 
        nascimento, 
        natural_person_key, 
        email, 
        ddd, 
        numero, 
        nome_da_mae, 
        rua, 
        numero_9, 
        bairro, 
        cidade, 
        estado, 
        cep, 
        pais, 
        DT_REGISTO_ZAIG_HRAMERICANO, 
        CAST(data_cadastro AS TIMESTAMP) AS data_cadastro, 
        decisao, 
        razao, 
        --sexo, 
        indicators, 
        analisys_ph3a, 
        session_id, 
        CAST(modelo_do_dispositivo AS STRING) AS modelo_do_dispositivo, 
        plataforma, 
        ip, 
        data_device_scan, 
        score, 
        score_Rufra, 
        indicador_pep, 
        indicador_vip, 
        level_risco, 
        decisao_rufra, 
        nome_analista, 
        email_analista, 
        --chave_analista, 
        score_makrosystem, 
        Cpf_Cliente
    FROM 
        `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Full`
;


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Atualizacao` as
    SELECT 
      min(data_cadastro) as primeiro_registro,
      max(data_cadastro) as ultimo_registro
    from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Consolidado`
;


--============================================================================================
--> MONITORAMENTO MOTOR PREVENÇAO FRAUDE - ONBOARDING  MES - 90 DIAS - PRIMEIRA DECISÃO CPF
--============================================================================================

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_PrimairaDecisao` 
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Consolidado`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_PrimairaDecisao` AS 

with

Base_dados_Zaig as (
          select
          distinct
          *
          from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Consolidado` 
          where date(data_cadastro) >= current_date - 120
          and decisao <> 'pending'
          --and Cpf_Cliente = '01503263606'
),
Base_Classificacao_decisao_Zaig as (
          select
          distinct
           REPLACE(cpf,'.', '') as CPF
          ,esteira
          ,data_cadastro
          ,DATE_DIFF(date(current_date),date(data_cadastro), Month) 

          ,case
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 0 then 'M0'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 1 then 'M-1'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 2 then 'M-2'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 3 then 'M-3'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 4 then 'M-4'
            else 'Outros' end as Flag_Filtro_Periodo

          ,RANK() OVER (PARTITION BY cpf,date(data_cadastro) ORDER BY EXTRACT(time FROM data_cadastro)) AS Rank_PrimeiraDecisao
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
          ,score	
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
  *
  from Base_Classificacao_decisao_Zaig
  where Rank_PrimeiraDecisao = 1
)
select
distinct
date(data_cadastro) as Data_Cadastro
,Flag_Filtro_Periodo
,esteira
,FORMAT_DATETIME("%Y-%m",data_cadastro) as Safra_Cadastro
,decisao
,razao
,Flag_Decisao_Motor
,Flag_Decisao_Regra
,score	
,score_makrosystem
,Flag_Decisao_Makro
,count(distinct cpf) as qtd_cliente
,count(*) as qtd_proposta
from Base_Classificacao_decisao_Zaig2
group by 1,2,3,4,5,6,7,8,9,10,11


;
--======================================================================================
--> MONITORAMENTO MOTOR PREVENÇAO FRAUDE - ONBOARDING  MES - 90 DIAS - ULTIMA DECISÃO CPF
--======================================================================================

-- select date(data_cadastro)as data_cadastro, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao` group by 1 order by 1 desc
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao`


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao` AS 

with

Base_dados_Zaig as (
          select
          distinct
          *
          from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Consolidado` 
          where date(data_cadastro) >= current_date - 120
          and decisao <> 'pending'
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
          ,score	
          ,score_makrosystem
          ,case 
          when score_makrosystem <= 30 then 'Reprovado'
          when score_makrosystem <= 50 then 'Neutro'
          when score_makrosystem > 50 then 'Aprovado'
          else 'NA' end as Flag_Decisao_Makro
          ,case 
              when esteira = 'Abastece Aí' then 'KMV - Full'
              when esteira = 'Abastece Aí - Light' then 'KMV - Light'
            end as Flag_Fase

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
,a.score
,a.score_makrosystem
,a.Flag_Decisao_Makro
,a.Flag_Fase
 
from Base_Classificacao_decisao_Zaig a

), Base_Classificacao_decisao_Zaig_3 as (
select
a.* 
,RANK() OVER (PARTITION BY a.CPF,date(a.data_cadastro),esteira ORDER BY EXTRACT(time FROM a.data_cadastro) desc) AS Rank_Ult_Decisao
from Base_Classificacao_decisao_Zaig_2 a
--where esteira <> 'Abastece Aí' 
--and date(Data_Cadastro) = '2023-12-15'
--and CPF = 00791578151
)
select
distinct
date(data_cadastro) as Data_Cadastro
,FORMAT_DATETIME("%Y-%m",data_cadastro) as Safra_Cadastro
,EXTRACT(HOUR FROM data_cadastro)as Hr_Cadastro
,case
  when DATE_DIFF(date(current_date),date(data_cadastro), DAY) = 0 then 'D0'
  when DATE_DIFF(date(current_date),date(data_cadastro), DAY) = 1 then 'D-01'
  when DATE_DIFF(date(current_date),date(data_cadastro), DAY) = 7 then 'D-07'
  when DATE_DIFF(date(current_date),date(data_cadastro), DAY) = 14 then 'D-14'
  when DATE_DIFF(date(current_date),date(data_cadastro), DAY) = 21 then 'D-21'
  --when DATE_DIFF(date(current_date),date(data_cadastro), DAY) = 60 then 'D-60'
  else 'Outros' 
end as Flag_Filtro_Dias
,decisao
,razao
,esteira
,Flag_Fase
,Flag_Filtro_Periodo
,Flag_Decisao_Motor
,Flag_Decisao_Regra
,score	
,score_makrosystem
,Flag_Decisao_Makro
,count(distinct cpf) as qtd_cliente
,count(*) as qtd_proposta
from Base_Classificacao_decisao_Zaig_3
where Rank_Ult_Decisao = 1
and esteira not like '%PJ%'
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

;


--======================================================================================
--> ZAIG - DESVIO PADRÃO 30 DIAS - ULTIMA DECISÃO                                      |
--======================================================================================


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltDesc_30dias` AS 

with
base as (
select 
Count(distinct Data_Cadastro) as qtdDias
,sum(qtd_proposta) as QtdProposta
,sum(qtd_proposta)/Count(distinct Data_Cadastro) as Media30dias
,STDDEV(qtd_proposta) as DesvioPadrao
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao`
where 
date(data_cadastro) >= current_date - 120
and Flag_Decisao_Motor = 'Aprovado'
)
select
 a.*
 ,(a.Media30dias + a.DesvioPadrao) as Med_DesvMais
 ,(a.Media30dias - a.DesvioPadrao) as Med_DesvMenos
 
from base a
;

#Média % aprovação 30 dias 

/*

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltDesc_Perc_Apr` AS 


with
base as (
select 
Data_Cadastro as qtdDias
,sum(qtd_proposta) as QtdProposta
,case when Flag_Decisao_Motor = 'Aprovado' then sum(qtd_proposta) end as QtdApr
,sum(qtd_proposta)/Count(distinct Data_Cadastro) as Media30dias

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao`
where 
date(data_cadastro) >= current_date - 29
group by Flag_Decisao_Motor, Data_Cadastro
), base2 as (
  select
qtdDias
,sum(QtdProposta) as QtdProposta
,Sum(QtdApr) as QtdApr
,Sum(Media30dias) as Media30dias
  from base
  group by 1

 ), base3 as (
select
qtdDias 
,QtdProposta
,QtdApr
,QtdApr/QtdProposta as PercApr

from base2 a
 ), base4 as (
  select
 avg(PercApr) as Med_Perc
 ,stddev(PercApr) as Desv_Pad_Perc
  from base3
)
select
 a.*
 ,(a.Med_Perc + a.Desv_Pad_Perc) as Med_PercDesvMais
 ,(a.Med_Perc - a.Desv_Pad_Perc) as Med_PercDesvMenos
 
from base4 a


;
*/


--======================================================================================
--> ZAIG - DESVIO PADRÃO - ULTIMA DECISÃO FASE LIGHT                                   |
--======================================================================================

create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltDesc_Perc_Apr_Light` as
  with
  base as (
  select 
  Data_Cadastro as qtdDias
  ,sum(qtd_proposta) as QtdProposta
  ,case when Flag_Decisao_Motor = 'Aprovado' then sum(qtd_proposta) end as QtdApr
  ,sum(qtd_proposta)/Count(distinct Data_Cadastro) as Media30dias

  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao`
  where 
  date(data_cadastro) >= current_date - 29
  and esteira = 'Abastece Aí - Light'
  group by Flag_Decisao_Motor, Data_Cadastro
  ), base2 as (
    select
  qtdDias
  ,sum(QtdProposta) as QtdProposta
  ,Sum(QtdApr) as QtdApr
  ,Sum(Media30dias) as Media30dias
    from base
    group by 1

  ), base3 as (
  select
  qtdDias 
  ,QtdProposta
  ,QtdApr
  ,QtdApr/QtdProposta as PercApr

  from base2 a
  ), base4 as (
    select
  avg(PercApr) as Med_Perc_Light
  ,stddev(PercApr) as Desv_Pad_Perc_Light
    from base3
  )
  select
  a.*
  ,(a.Med_Perc_Light + a.Desv_Pad_Perc_Light) as Med_PercDesvMais_Light
  ,(a.Med_Perc_Light - a.Desv_Pad_Perc_Light) as Med_PercDesvMenos_Light
  
  from base4 a

;


--======================================================================================
--> ZAIG - DESVIO PADRÃO - ULTIMA DECISÃO FASE FULL                                    |
--======================================================================================

  create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltDesc_Perc_Apr_Full` as
  with
  base as (
  select 
  Data_Cadastro as qtdDias
  ,sum(qtd_proposta) as QtdProposta
  ,case when Flag_Decisao_Motor = 'Aprovado' then sum(qtd_proposta) end as QtdApr
  ,sum(qtd_proposta)/Count(distinct Data_Cadastro) as Media30dias

  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao`
  where 
  date(data_cadastro) >= current_date - 120
  and esteira = 'Abastece Aí'
  group by Flag_Decisao_Motor, Data_Cadastro
  ), base2 as (
    select
  qtdDias
  ,sum(QtdProposta) as QtdProposta
  ,Sum(QtdApr) as QtdApr
  ,Sum(Media30dias) as Media30dias
    from base
    group by 1

  ), base3 as (
  select
  qtdDias 
  ,QtdProposta
  ,QtdApr
  ,QtdApr/QtdProposta as PercApr

  from base2 a
  ), base4 as (
    select
  avg(PercApr) as Med_Perc_Full
  ,stddev(PercApr) as Desv_Pad_Perc_Full
    from base3
  )
  select
  a.*
  ,(a.Med_Perc_Full + a.Desv_Pad_Perc_Full) as Med_PercDesvMais_Full
  ,(a.Med_Perc_Full - a.Desv_Pad_Perc_Full) as Med_PercDesvMenos_Full
  
  from base4 a

;
--======================================================================================
--> MONITORAMENTO MOTOR PREVENÇAO FRAUDE - ONBOARDING CHAMADAS ZAIG - 120 DIAS
--======================================================================================
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Chamada_Zaig`
--  select Safra_Cadastro, sum(qtd_Chamada) as qtd_Chamada from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Chamada_Zaig` group by 1

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Chamada_Zaig` AS 

select
distinct
           date(data_cadastro) as Data_Cadastro
          ,FORMAT_DATETIME("%Y-%m",data_cadastro) as Safra_Cadastro
          ,esteira
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
          ,count(*) as qtd_Chamada
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Consolidado`
where date(data_cadastro) >= current_date - 365
--and decisao <> 'pending'
group by 1,2,3,4,5

;
--======================================================================================
--> TIRAR DUPLICIDADES TB_BD_ZAIG
--======================================================================================
/*
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig` AS 

with
base as (
SELECT
 distinct
 *
FROM
  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig`
)
SELECT
 *   
FROM base
*/
/*  
select
date (data_cadastro), count(*)
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig`
group by 1
order by 1 desc


;
*/

----------------------------------------------------------------
--======================================================================================
--> ZAIG vs ORBITAL - POR HORA COMPARAR ULTIMOS 90 - ULTIMA DECISÃO
--======================================================================================
/*

select esteira,Status_Conta,RiskAnalysis,Flag_Conta, count(*)
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Zaig_vs_Orbitall` where data_cadastro  between '2024-02-01' and '2024-02-29' 
 group by 1,2,3,4
 order by 1


select *
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Zaig_vs_Orbitall` 


*/

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Zaig_vs_Orbitall` AS 

with
base as (
        select
          distinct
           Cpf_Cliente
          ,esteira
          ,date(data_cadastro) as data_cadastro
          ,FORMAT_DATE("%d",data_cadastro)as Dia
          ,EXTRACT(HOUR FROM data_cadastro)as Hr_Cadastro
          ,case
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 0 then 'M0'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 1 then 'M-1'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 2 then 'M-2'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 3 then 'M-3'
            when DATE_DIFF(date(current_date),date(data_cadastro), Month) = 4 then 'M-4'
            else 'Outros' end as Flag_Filtro_Periodo

          ,RANK() OVER (PARTITION BY Cpf_Cliente ORDER BY data_cadastro desc) AS Rank_Ult_Decisao
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
          ,case 
            when score_makrosystem <= 30 then 'Reprovado'
            when score_makrosystem <= 50 then 'Neutro'
            when score_makrosystem > 50 then 'Aprovado'
          else 'NA' end as Flag_Decisao_Makro
          ,Case 
                When EXTRACT(HOUR FROM data_cadastro) >=0 	AND EXTRACT(HOUR FROM data_cadastro) <=2 	  Then '1 00a02'
                When EXTRACT(HOUR FROM data_cadastro) > 2 	AND EXTRACT(HOUR FROM data_cadastro) <=5 	  Then '2 03a05'
                When EXTRACT(HOUR FROM data_cadastro) > 5 	AND EXTRACT(HOUR FROM data_cadastro) <=8 	  Then '3 06a08'
                When EXTRACT(HOUR FROM data_cadastro) > 8	  AND EXTRACT(HOUR FROM data_cadastro) <=11 	Then '4 09a11'
                When EXTRACT(HOUR FROM data_cadastro) > 11  AND EXTRACT(HOUR FROM data_cadastro) <=14 	Then '5 12a14'
                When EXTRACT(HOUR FROM data_cadastro) > 14  AND EXTRACT(HOUR FROM data_cadastro) <=17 	Then '6 15a17'
                When EXTRACT(HOUR FROM data_cadastro) > 17  AND EXTRACT(HOUR FROM data_cadastro) <=20 	Then '7 18a20'	  
                When EXTRACT(HOUR FROM data_cadastro) > 20  AND EXTRACT(HOUR FROM data_cadastro) <=23 	Then '8 21a23'	  
          End as Faixa_Hora
          ,CASE
                WHEN DATETIME_DIFF(DATETIME(data_cadastro), DATETIME(cl_orb.DataStatus), DAY) <=0 
                and cl_orb.MotivoStatus in ('Fraude confirmada','Suspeita de fraude') THEN '01_<MesmoDia'
                WHEN DATETIME_DIFF(DATETIME(data_cadastro), DATETIME(cl_orb.DataStatus), DAY) <=5 
                and cl_orb.MotivoStatus in ('Fraude confirmada','Suspeita de fraude') THEN '02_<1a5Dias'
                WHEN DATETIME_DIFF(DATETIME(data_cadastro), DATETIME(cl_orb.DataStatus), DAY) <=15 
                and cl_orb.MotivoStatus in ('Fraude confirmada','Suspeita de fraude') THEN '03_<6a15Dias'
                WHEN DATETIME_DIFF(DATETIME(data_cadastro), DATETIME(cl_orb.DataStatus), DAY) <=29 
                and cl_orb.MotivoStatus in ('Fraude confirmada','Suspeita de fraude') THEN '04_<16a29Dias'
                WHEN DATETIME_DIFF(DATETIME(data_cadastro), DATETIME(cl_orb.DataStatus), DAY) >=30 
                and cl_orb.MotivoStatus in ('Fraude confirmada','Suspeita de fraude') THEN '05_>30Dias' 
                WHEN DATETIME_DIFF(DATETIME(current_date),  DATETIME(cl_orb.DataStatus), DAY) is null and cl_orb.Status_Conta = 'INACTIVE' THEN '07_<Inativo'
                WHEN cl_orb.Status_Conta = 'BLOCKED' and cl_orb.RiskAnalysis in ('DENIED','APPROVED','') THEN '08_<BloqueioOrbitall'
                WHEN cl_orb.Status_Conta is null THEN '09_<SemContaDigital'
                WHEN DATETIME_DIFF(DATETIME(current_date),  DATETIME(cl_orb.DataStatus), DAY) is null 
                and cl_orb.Status_Conta = 'ACTIVE' and cl_orb.RiskAnalysis in ('APPROVED') THEN '06_<SemBloqueio'
                WHEN cl_orb.MotivoStatus not in ('Fraude confirmada','Suspeita de fraude','') THEN '10_<OutrosBloqueios' 
                when cl_orb.MotivoStatus is null THEN '06_<SemBloqueio'
          END AS Flag_Aging_Bloq_Fraude
          ,CASE WHEN cl_orb.MotivoStatus in ('Fraude confirmada','Suspeita de fraude') then 'FRAUDE' else 'NAO FRAUDE' end as Flag_Fraude
          ,CASE
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=0  THEN '01_<D0'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=1  THEN '02_<D-1'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=2  THEN '03_<D-2'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=3  THEN '04_<D-3'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) >3   THEN '05_<OutrosDias'   
          END AS Flag_Dia
          ,CASE
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=5 THEN '01_<5DIAS'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=30 THEN '02_<30DIAS'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=60 THEN '03_<60DIAS'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=90 THEN '04_<90DIAS'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=120 THEN '05_<120DIAS'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=160 THEN '06_<160DIAS'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=190 THEN '07_<190DIAS'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=220 THEN '08_<220DIAS'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=260 THEN '09_<260DIAS'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=290 THEN '10_<290DIAS'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) <=365 THEN '11_1ANO'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(data_cadastro), DAY) >=365 THEN '12_+1ANO'
          END AS Flag_TempodeConta
          ,DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_orb.DataNacimento), year) as tempo 
          ,Case 
                When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_orb.DataNacimento), year)<=25  Then '01  18a25anos'
                When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_orb.DataNacimento), year)<=35  Then '02  26a35anos'
                When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_orb.DataNacimento), year)<=45  Then '03  36a45anos'
                When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_orb.DataNacimento), year)<=55  Then '04  46a55anos'
                When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_orb.DataNacimento), year)<=65  Then '05  56a65anos'
                When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_orb.DataNacimento), year)<=75  Then '06  66a75anos'
                When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_orb.DataNacimento), year)<=85  Then '07  76a85anos'
                When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_orb.DataNacimento), year)>85   Then '08  >86anos'  
            End as Faixa_Idade
            ,cl_orb.Regiao
            ,cl_orb.MotivoStatus
            ,cl_orb.DataStatus
            ,cl_orb.DataNacimento
            ,cl_orb.Trusted
            ,cl_orb.Status_Conta
            ,cl_orb.RiskAnalysis
            ,case 
             when cl_orb.CPF = a.Cpf_Cliente then 'Zaig|Orbitall' 
             when a.Cpf_Cliente is not null and cl_orb.CPF is null then 'Zaig' 
             when a.Cpf_Cliente is null and cl_orb.CPF is not null then 'Orbitall' 
            else 'Verificar' end as Flag_Processamento

          from (with
                Base_dados_Zaig as (
                select
                distinct
                *
                from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Consolidado` 
                where date(data_cadastro) >= current_date - 90
                and decisao = "automatically_approved"
                and esteira <> 'Abastece Aí - PJ'
                
                ), Base_Classificacao_decisao_Zaig_3 as (
                select
                a.* 
                ,RANK() OVER (PARTITION BY a.CPF,esteira ORDER BY date(a.data_cadastro) desc) AS Rank_Ult_Decisao
                from Base_dados_Zaig a
                )
                select
                distinct
                *
                from Base_Classificacao_decisao_Zaig_3
                where Rank_Ult_Decisao = 1) a
left join (with
  Base_Clientes as (

  SELECT 
  cl.uuid as  CustomerID
  ,cl.full_name as Nome
  ,cl.document as CPF
  ,cl.email
  ,en.street as Rua
  ,en.neighborhood as Bairro
  ,en.city as Cidade
  ,en.state as UF
  --,FORMAT_DATE("%d-%m-%Y",cl.created_at)as DataCriacao
  ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
  ,FORMAT_DATE("%Y",cl.created_at)as Ano
  ,cl.birth_date as DataNacimento
  ,cl.created_at as DataCriacao
  ,ph.area_code as DDD
  ,ph.number as Telefone
  ,ph.type as TelefoneTipo
  ,cl.trusted as Trusted
  ,cl.status as Status_Conta
  ,cl.risk_analysis_status as RiskAnalysis
  ,ev.observation as MotivoStatus
  ,ev.event_date as DataStatus
  ,ev.user_name as UsuarioStatus
  ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual
  ,CASE
  WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
  WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
  WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
  WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
  WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
  ELSE 'SUL'
  END AS Regiao

  FROM `eai-datalake-data-sandbox.core.customers`               cl
  left join `eai-datalake-data-sandbox.core.address`            en on en.id = cl.address_id
  left join `eai-datalake-data-sandbox.core.customer_phone`     id on id.customer_id = cl.id
  left join `eai-datalake-data-sandbox.core.phone`              ph on id.phone_id = ph.id 
  left join (select * from `eai-datalake-data-sandbox.core.customer_event` 
            where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT')
                                                            )   Ev on ev.customer_id = cl.id

  ) select 
  *

  from Base_Clientes where Rank_Ult_Atual = 1 
  --and CPF = '00441244734'
  
  ) cl_orb on cl_orb.CPF = a.Cpf_Cliente
)select 
	
data_cadastro
,esteira
--,Dia
--,Hr_Cadastro
,Flag_Filtro_Periodo
,Rank_Ult_Decisao
,Flag_Fraude
,decisao
,razao
,Flag_Decisao_Motor
,Flag_Decisao_Regra
--,Flag_Decisao_Makro
,Faixa_Hora
,Flag_Aging_Bloq_Fraude
,Flag_Dia
,Flag_TempodeConta
,Faixa_Idade
,Regiao
,MotivoStatus
--,DataStatus
,case
  when esteira = 'Abastece Aí' and Status_Conta in ('ACTIVE', 'BLOCKED') and RiskAnalysis in ('APPROVED','BLOCKED') then 'ContaCriada'
  when esteira = 'Abastece Aí' and Status_Conta in ('ACTIVE', 'BLOCKED') and RiskAnalysis in ('IN_ANALYSIS') then 'ContaNaoCriada'
  when esteira = 'Abastece Aí' and Status_Conta in ('MINIMUM_ACCOUNT','INACTIVE') and RiskAnalysis in ('PARTIAL_APPROVED','IN_ANALYSIS') then 'ContaNao_Processada'
  when esteira = 'Abastece Aí' and Status_Conta is null and RiskAnalysis is null then 'ContaNao_Processada'


  when esteira = 'Abastece Aí - Light' and Status_Conta in ('ACTIVE', 'BLOCKED','MINIMUM_ACCOUNT') and RiskAnalysis in ('APPROVED','BLOCKED','PARTIAL_APPROVED') then 'FaseLight_Aprovada'
  when esteira = 'Abastece Aí - Light' and Status_Conta in ('ACTIVE', 'BLOCKED') and RiskAnalysis in ('IN_ANALYSIS') then 'FaseLightNao_Processada'
  when esteira = 'Abastece Aí - Light' and Status_Conta in ('MINIMUM_ACCOUNT','INACTIVE') and RiskAnalysis in ('PARTIAL_APPROVED','IN_ANALYSIS') then 'FaseLightNao_Processada'
  when esteira = 'Abastece Aí - Light' and Status_Conta in ('BLOCKED') and RiskAnalysis in ('DENIED') then 'FaseLight_NegadaOrbitall'
  when esteira = 'Abastece Aí - Light' and Status_Conta is null and RiskAnalysis is null then 'FaseLightNao_Processada'
  
end as Flag_Conta

,case 
  when pf.Cpf_Resticao_Mot = cast(c.Cpf_Cliente as numeric) then 'OPERACAOPF'
  when vip.CPF = c.Cpf_Cliente then 'VIP'
  when cast(uber.Cpf_Cliente as Numeric) = cast(c.Cpf_Cliente as Numeric) then 'UBER'
  else 'URBANO'end as Flag_Perfil
,case when Trusted = 1 then 'Trusted' else 'No Trusted' end as FlagTrusted
,Status_Conta
,RiskAnalysis
,Flag_Processamento
,count(distinct C.Cpf_Cliente) as Qtd_Cliente

from base c
LEFT JOIN (select distinct CPF from `eai-datalake-data-sandbox.loyalty.tblParticipantes` where Vip is not null and Inativo = false) as vip on vip.CPF = c.Cpf_Cliente
LEFT JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.TB_MONIT_CPF_OP_PF` pf on pf.Cpf_Resticao_Mot = cast(c.Cpf_Cliente as numeric) 
LEFT JOIN (
      select
      distinct 
        cl.uuid as CustomerId
        ,cl.document as Cpf_Cliente
        from `eai-datalake-data-sandbox.core.order_benefit` ordbnf
        join `eai-datalake-data-sandbox.core.orders`              ord on ord.id = ordbnf.order_id
        join `eai-datalake-data-sandbox.core.customers`            cl on ord.customer_id =cl.id
      WHERE ordbnf.id >= 123900000
      AND (ordbnf.origin_type  = 'EAI:UBER' or upper(ordbnf.description) LIKE '%UBER%')
      AND ordbnf.status = 'FINISHED'
        ) uber on cast(uber.Cpf_Cliente as Numeric) = cast(c.Cpf_Cliente as Numeric)



where Rank_Ult_Decisao = 1 

group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22;


--select * from base where Rank_Ult_Decisao = 1 and Flag_Aging_Bloq_Fraude is null
--select Flag_Aging_Bloq_Fraude, count(*) from base where Rank_Ult_Decisao = 1 group by 1


--------------------------------------------------------------------------------
--> CONSOLIDAÇÃO BASE ZAIG PJ UPLOAD                                           |
--------------------------------------------------------------------------------

-------------------------------------
--                                  |
-- Tb_bd_zaig_PJ_novo               |
--                                  |
-------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Consolidado_PJ`

/*
SELECT 
    COUNT(DISTINCT legal_person_id) AS Volume_Total, 
    COUNT(DISTINCT CASE WHEN razao = 'automatically_approved' THEN legal_person_id END) AS Aprovados, 
    COUNT(DISTINCT CASE WHEN razao = 'partner_automatically_reproved' THEN legal_person_id END) AS Negados, 
    COUNT(DISTINCT CASE WHEN razao IS NULL THEN legal_person_id END) AS Pendentes, 
    CONCAT(ROUND(COALESCE(COUNT(DISTINCT CASE WHEN razao = 'automatically_approved' THEN legal_person_id END) * 100.0 / NULLIF(COUNT(DISTINCT legal_person_id), 0), 0)), '%') AS `%_Aprovado`, 
    CONCAT(ROUND(COALESCE(COUNT(DISTINCT CASE WHEN razao = 'partner_automatically_reproved' THEN legal_person_id END) * 100.0 / NULLIF(COUNT(DISTINCT legal_person_id), 0), 0)), '%') AS `%_Negado`, 
    CONCAT(ROUND(COALESCE(COUNT(DISTINCT CASE WHEN razao IS NULL THEN legal_person_id END) * 100.0 / NULLIF(COUNT(DISTINCT legal_person_id), 0), 1)), '%') AS `%_Pendentes` 
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Consolidado_PJ` 
-- WHERE DATE(data_cadastro) >= CURRENT_DATE();

*/

-- select count(*) as Volume, min(data_cadastro) as Prim_Registro, max(data_cadastro) as Ult_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_PJ`

-- select count(*) as Volume, min(data_cadastro) as Prim_Registro, max(data_cadastro) as Ult_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_PJ_novo`

-- select count(*) as Volume, min(data_cadastro) as Prim_Registro, max(data_cadastro) as Ult_Registro from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_Consolidado_PJ`


--------------------------------------------------------------------------------------------------------------------------------------------------------------------------


--======================================================================================
--> MONITORAMENTO MOTOR PREVENÇAO FRAUDE - ONBOARDING  MES - 90 DIAS - ULTIMA DECISÃO CPF
--======================================================================================

-- select date(data_cadastro)as data_cadastro, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao_PJ` group by 1 order by 1 desc
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao_PJ`


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao_PJ` AS 

with

Base_dados_Zaig_PJ as (
          select
          distinct
          *
          from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig_PJ` 
          where --date(data_cadastro) >= current_date - 365 and
          decisao <> 'Pendente'
          and legal_person_id like 'STO%'
), Base_Classificacao_decisao_Zaig_PJ as (
          select
          distinct
          REPLACE(REPLACE(REPLACE(cnpj,'.', ''),'-', ''), '/', '') as CNPJ
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
            when decisao = "Aprovado Automaticamente" then 'Aprovado'
            when decisao = "Reprovado Automaticamente" then 'Negado'
          else 'NA' end as Flag_Decisao_Motor
          ,case
            -- when razao Like  "%bureau_data%" then 'Negado Cadastro'
            when razao = "automatically_approved" then 'Aprovado'
            when razao = "partner_automatically_reproved" then 'Negado'
          else 'NA' end as Flag_Decisao_Regra
          ,tree_score	
          from Base_dados_Zaig_PJ

) , Base_Classificacao_decisao_Zaig_PJ_2 as (
select 

cast(a.CNPJ as NUMERIC) as CNPJ
,a.esteira
,a.data_cadastro
,a.Flag_Filtro_Periodo
,a.decisao
,a.razao
,a.Flag_Decisao_Motor
,a.Flag_Decisao_Regra
,a.tree_score
from Base_Classificacao_decisao_Zaig_PJ a

), Base_Classificacao_decisao_Zaig_PJ_3 as (
select
a.* 
,RANK() OVER (PARTITION BY a.CNPJ,date(a.data_cadastro),esteira ORDER BY EXTRACT(time FROM a.data_cadastro) desc) AS Rank_Ult_Decisao_PJ
from Base_Classificacao_decisao_Zaig_PJ_2 a
)
select
distinct
date(data_cadastro) as Data_Cadastro
,FORMAT_DATETIME("%Y-%m",data_cadastro) as Safra_Cadastro
,decisao
,razao
,esteira
,Flag_Filtro_Periodo
,Flag_Decisao_Motor
,Flag_Decisao_Regra
,tree_score	
,count(distinct CNPJ) as qtd_CNPJs
,count(*) as qtd_proposta
from Base_Classificacao_decisao_Zaig_PJ_3
where Rank_Ult_Decisao_PJ = 1
group by 1,2,3,4,5,6,7,8,9

;


-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao_Analise` 

--======================================================================================
--> MONITORAMENTO MOTOR PREVENÇAO FRAUDE - ONBOARDING  MES - 90 DIAS - ULTIMA DECISÃO CPF
--======================================================================================


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao_Analise` AS 

with

Base_Zaig as (

select 
Hr_Cadastro
,Flag_Filtro_Dias
,Flag_Fase

,Sum(if(Flag_Decisao_Motor = 'Aprovado', qtd_cliente,0)) as Qtd_Apr
,Sum(if(Flag_Decisao_Motor = 'Negado', qtd_cliente,0)) as Qtd_Neg
,sum(qtd_cliente) as qtd_Total
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Motor_Zaig_UltimaDecisao`
group by 1,2,3

)

select
Hr_Cadastro
,Flag_Filtro_Dias
,Flag_Fase
,Qtd_Apr
,Qtd_Apr/qtd_Total as PercApr
,Qtd_Neg
,Qtd_Neg/qtd_Total as PercNeg
,qtd_Total
from Base_Zaig




