-- BASE BIOMETRIA CADASTRADA
--- select * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone` 


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_1` AS 

with
base as (

    select 
    distinct
    cl.document as CPF
    ,FORMAT_DATE("%Y%m",ev.created_at)as Safra_Ev
    ,cast(ev.created_at as TIMESTAMP) as created_at
    ,Ev.status as Status_Conta_EV
    ,case 
      when Ev.status in ('FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_REJECTED') then 'Negado' 
      else 'Aprovado' end as Resultado
    ,'Backoffice' as Origem
    ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date  desc) AS Rank_Ult_Atual

    FROM `eai-datalake-data-sandbox.core.customers`             cl
    left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id
    where Ev.status in ('FACIAL_BIOMETRICS_VALIDATED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_REJECTED')
    --and cl.document in ('35397950904','70058874178')
    --and cl.document= '00000025852'
    order by 1 
)
select * from base where Rank_Ult_Atual = 1


;

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_2` order by 1


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_2` AS 

with
base as (
SELECT 
Distinct
REPLACE(REPLACE(CPF,'.', ''),'-', '') as CPF
,FORMAT_DATE("%Y%m",DATA)as Safra_Ev
,DATA as created_at
,LIVENESS as Status_Conta_EV
,RESULTADO as Resultado
,'ResetSenha' as Origem
,RANK() OVER (PARTITION BY CPF ORDER BY DATA desc) AS Rank_Ult_Atual


FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_BIO_Staging_Area` 
where DATA is not null
order by 1 

)
select * from base where Rank_Ult_Atual = 1

;
--select * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone` where CPF = 100038433


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_3` AS 

with
base_UltimoScore as (
select
CPF
,max(DATA_ENVIO_SMS) as DATA_ENVIO_SMS
,max(SCORE) as SCORE
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_SMS_Staging_Area` 
group by 1
)
select 
Distinct
cast(ultSocre.CPF as STRING) as CPF
,FORMAT_DATE("%Y%m",ultSocre.DATA_ENVIO_SMS)as Safra_Ev
,ultSocre.DATA_ENVIO_SMS as created_at
,Bio.LIVENESS as Status_Conta_EV
--,ultSocre.SCORE
,case 
  when ultSocre.SCORE < 0 then 'Negado'
  when ultSocre.SCORE = 0 then 'Neutro'
  when ultSocre.SCORE > 0 then 'Aprovado'
  else 'SemRetorno' end as Resultado
,Bio.TEMPLATE as Origem
,RANK() OVER (PARTITION BY ultSocre.CPF ORDER BY ultSocre.DATA_ENVIO_SMS desc) AS Rank_Ult_Atual
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Unico_SMS_Staging_Area` Bio
join base_UltimoScore ultSocre on ultSocre.CPF = Bio.CPF and ultSocre.SCORE = Bio.SCORE
--where Bio.CPF = 10069686440
order by 1,2 desc

;

--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_4` where Origem != 'ResetSenha' and Flag_Cadastro = 'NaoCadastrada' and 	Flag_Cliente = 'Verificar' order by 1,2
--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_4` where Origem != 'Backoffice'  order by 1,2
--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_4` where CPF = '00004527500'
-- select Origem,StatusConta,Resultado,Flag_Cadastro,Flag_Cliente, count(distinct CPF) as QtdCliente  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_4` where Origem != 'Backoffice' group by 1,2,3,4,5

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_4` AS 


with

base as (

  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_1` 
  union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_2` 
    union all
  select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_3` 

),base2 as (

  select 
  distinct
    replace(replace(CPF,".",""),"-","") as CPF
    ,Safra_Ev
    ,created_at
    ,Status_Conta_EV
    ,Resultado
    ,Origem
    ,RANK() OVER (PARTITION BY CPF ORDER BY created_at ) AS Rank_Ult_AtualX
  from base
  --where 	CPF = '00000025852'
) select
  bs.* 
  ,cl.status as StatusConta
  ,case 
  when cast(bko_1.CPF as NUMERIC ) = cast(bs.CPF as NUMERIC ) then 'CadastradoBKO'
  else 'NaoCadastrada' end as Flag_Cadastro
  ,case
    when cast(kmv.CPF as NUMERIC ) = cast(bs.CPF as NUMERIC ) and cast(bko_1.CPF as NUMERIC ) = cast(bs.CPF as NUMERIC ) then 'EAI'
    when cast(cl.document as NUMERIC ) = cast(bs.CPF as NUMERIC ) then 'EAI'   
    when cast(kmv.CPF as NUMERIC ) = cast(bs.CPF as NUMERIC ) then 'KMV'
   else 'Verificar' end as Flag_Cliente

  from base2 bs
  left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_1`  bko_1 on cast(bko_1.CPF as NUMERIC ) = cast(bs.CPF as NUMERIC )
  left join `eai-datalake-data-sandbox.loyalty.tblParticipantes`  kmv on cast(kmv.CPF as NUMERIC ) = cast(bs.CPF as NUMERIC )
  left join `eai-datalake-data-sandbox.core.customers`             cl on cast(cl.document as NUMERIC ) = cast(bs.CPF as NUMERIC )
  
  where Rank_Ult_AtualX = 1
  --where bs.CPF = 	'00000025852'
  order by 1,2 desc

  ;
----------------------------------------------------------------------------------------------------------------------------------
--- Clientes negados na Zaig com bio Realizada


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bio_Zaig_Negados` AS 

with
baseZaig as (
select
a.*
--,RANK() OVER (PARTITION BY cpf ORDER BY date(data_cadastro)desc) AS Rank_Ult_Decisao
,RANK() OVER (PARTITION BY cpf,date(data_cadastro) ORDER BY EXTRACT(time FROM data_cadastro) desc) AS Rank_Ult_Decisao
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` a

), baseZaigBio as (
select
 distinct 
  Zaig.* 
  ,case
  when  Zaig.decisao = "automatically_approved" then 'Aprovado'
  when  Zaig.decisao = "automatically_reproved" then 'Negado'
  else 'Pendente' end as Flag_Decisao_Motor
  ,case
  when  Zaig.razao Like  "%ph3a%" then 'Negado PH3A'
  when  Zaig.razao Like  "%fa_risk%" then 'Negado Score Makro'
  when  Zaig.decisao = "automatically_approved" then 'Aprovado'
  when  Zaig.decisao = "automatically_reproved" and  Zaig.razao not Like  "%fa_risk%" then 'Negado'
  when  Zaig.decisao = "automatically_reproved" and  Zaig.razao not Like  "%ph3a%" then 'Negado'
  else 'Pendente' end as Flag_Decisao_Regra
  ,case when bio.CPF = Zaig.Cpf_Cliente then 'BioRealizada' else 'BioNaoRealizada' end as Flag_Bio
  ,bio.Resultado as ResultadoBio
  ,bio.Origem
  ,bio.Flag_Cliente

 from baseZaig Zaig
 left join (select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_4` 
            where Origem != 'Backoffice'
            and Resultado = 'Aprovado'
            --and Flag_Cliente = 'KMV'
            and Flag_Cadastro = 'NaoCadastrada'
            ) bio on bio.CPF = Zaig.Cpf_Cliente
 where Zaig.Rank_Ult_Decisao = 1
),baseZaigBio2 as (
  select * from baseZaigBio where Flag_Bio = 'BioRealizada'
) --select * from baseZaigBio2 where Flag_Decisao_Motor = 'Negado'


select
distinct
--FORMAT_DATETIME("%Y-%m",data_cadastro) as Safra_Cadastro
  date(data_cadastro) as data_cadastro
  ,REPLACE(REPLACE(cpf,'.', ''),'-', '') as cpf
  ,Flag_Decisao_Motor
  ,Flag_Decisao_Regra
  ,razao
  ,Flag_Bio
  ,ResultadoBio
  ,Origem
  ,Flag_Cliente
  ,count(distinct cpf) as QtdCliente

from baseZaigBio2
--where 	
--Flag_Decisao_Motor = 'Negado'
group by 1,2,3,4,5,6,7,8,9


/*

select
distinct
CPF
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_4` 
where 
Resultado = 'Aprovado'
and Flag_Cliente = 'KMV'
and Flag_Cadastro = 'NaoCadastrada'

*/