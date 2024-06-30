
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------
-- importar bloqueios realizados massivamene               |
-- Tb_Clientes_Bloqueio_Massivo                            |
------------------------------------------------------------

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo` order by 3 desc



--------------------------------------
-- Menor e Maior data de Bloqueio    | 
-------------------------------------- 

/*
 
 select 
    min(Lote) as Primeira_Dt_Bloqueios, 
    max(Lote) as Ultima_Dt_Bloqueios,
    count(*) as Quantidade_Bloqueios
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2`

*/

create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2` as 
  with 
  base_bloqueios_massivo as (
  select *, LPAD(CAST(CPF AS STRING), 11, '0') AS cpf_completo from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo`
)select distinct * from base_bloqueios_massivo where cpf_completo <> '00000000000'

;
/*
 
 select 
    min(Lote) as Primeira_Dt_Bloqueios, 
    max(Lote) as Ultima_Dt_Bloqueios,
    count(*) as Quantidade_Bloqueios
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2`

*/


/*
with
base_bloqueioMassivo as (
SELECT
 distinct
 CustomerID
,CPF
,Lote
,Motivo
,RANK() OVER (PARTITION BY CPF ORDER BY Lote desc) AS Rank_Bloqueio

FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo` 
order by 2,3 desc
) select * from base_bloqueioMassivo where Rank_Bloqueio = 1
*/

select distinct * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2`
where cpf_completo = '00569667097'
;


-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------
-- importar bloqueios realizados massivamene               |
-- Tb_Clientes_Desbloqueio_Massivo                         |
------------------------------------------------------------


create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Desbloqueio_Massivo_2` as 
  with 
  base_bloqueios_massivo as (
  select *, LPAD(CAST(CPF AS STRING), 11, '0') AS cpf_completo from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Desbloqueio_Massivo`
)select distinct * from base_bloqueios_massivo where cpf_completo <> '00000000000'
order by 5
;

/*
 
 select 
    min(Lote) as Primeira_Dt_Bloqueios, 
    max(Lote) as Ultima_Dt_Bloqueios,
    count(*) as Quantidade_Bloqueios
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Desbloqueio_Massivo_2`

*/


/*
with
base_bloqueioMassivo as (
SELECT
 distinct
 CustomerID
,CPF
,Lote
,Motivo
,RANK() OVER (PARTITION BY CPF ORDER BY Lote desc) AS Rank_Bloqueio

FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Desbloqueio_Massivo` 
order by 2,3 desc
) select * from base_bloqueioMassivo where Rank_Bloqueio = 1
*/

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Desbloqueio_Massivo_2`
