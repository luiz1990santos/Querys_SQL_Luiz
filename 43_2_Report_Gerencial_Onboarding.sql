--===================================================================--
-- Fluxo do Onboarding - AllowMe - Fluxo inicial Cadastro Contextual
--===================================================================--

--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.FunilOnboarding_0`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.FunilOnboarding_0` AS 

with
Base_AllowMe as (

select
distinct
'AllowMe' as Onboarding
,a.user_id as CPF_Cliente
,date(created_at_dtAllowme) as Dt_Cadastro_AllowMe
,FORMAT_DATETIME("%Y%m",created_at_dtAllowme) as Safra_AllowMe
,FORMAT_DATETIME("%d",created_at_dtAllowme) as Dia_Cadastro_AllowMe
,RANK() OVER (PARTITION BY a.user_id  ORDER BY  date(created_at_dtAllowme) desc) AS Rank_Ult_Decisao
,case when a.fraud = 'false' then 'Aprova' else 'Negado' end as Flag_Dec_1RegraAllowMe

FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_AllowMe_Cadastro` a 
where integration = 'cadastro'
and date(created_at_dtAllowme) >= current_date - 90
--and a.user_id = '28095821810'

), Base_Inicial_Fluxo_AllowMe  as (
SELECT
*
from Base_AllowMe 
where Rank_Ult_Decisao = 1
)
select
etap1.*
,etap2.*
,etap3.*
,etap4.*

from Base_Inicial_Fluxo_AllowMe etap1
--================================================================--
-- Fluxo do Onboarding - QIThech Zaig - Fluxo Fase1 Ligth
--================================================================--
left join (
with
Base_Zaig_fase1 as (
select
distinct
'KMV - Light' as Onboarding1
,REPLACE(REPLACE(cpf,'.', ''),'-', '') as CPF_fase1
,date(data_cadastro) as data_cadastro_fase1
,FORMAT_DATETIME("%Y%m",data_cadastro) as Safra_fase1
,esteira as esteirafase1
,case 
when esteira = 'Abastece Aí' then 'KMV - Full'
when esteira = 'Abastece Aí - Light' then 'KMV - Light'
end as Flag_Fase_fase1
,case
when decisao = "automatically_approved" then 'Aprovado'
when decisao = "automatically_reproved" then 'Negado'
else 'NA' end as Flag_Decisao_Motor_fase1
,RANK() OVER (PARTITION BY cpf ORDER BY data_cadastro desc) AS Rank_Ult_Decisao_fase1
from (select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` where esteira = 'Abastece Aí - Light') 
--where date(data_cadastro) >= current_date - 90

)
select
a.*
from Base_Zaig_fase1 a
join Base_Inicial_Fluxo_AllowMe etap1 on etap1.CPF_Cliente = a.CPF_fase1 
where a.Rank_Ult_Decisao_fase1 = 1

)etap2 on etap2.CPF_fase1 = etap1.CPF_Cliente
--================================================================--
-- Fluxo do Onboarding - QIThech Zaig - Fluxo Fase2 Full
--================================================================--
left join (
with
Base_Zaig_fase1 as (
select
distinct
'KMV - Full' as Onboarding2
,REPLACE(REPLACE(cpf,'.', ''),'-', '') as CPF_fase2
,date(data_cadastro) as data_cadastro_fase2
,FORMAT_DATETIME("%Y%m",data_cadastro) as Safra_fase2
,esteira as esteirafase2
,case 
when esteira = 'Abastece Aí' then 'KMV - Full'
when esteira = 'Abastece Aí - Light' then 'KMV - Light'
end as Flag_Fase_fase2
,case
when decisao = "automatically_approved" then 'Aprovado'
when decisao = "automatically_reproved" then 'Negado'
else 'NA' end as Flag_Decisao_Motor_fase2
,RANK() OVER (PARTITION BY cpf ORDER BY data_cadastro desc) AS Rank_Ult_Decisao_fase2
from (select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` where esteira = 'Abastece Aí') 

)
select
a.*
from Base_Zaig_fase1 a
join Base_Inicial_Fluxo_AllowMe etap1 on etap1.CPF_Cliente = a.CPF_fase2 
where a.Rank_Ult_Decisao_fase2 = 1

)etap3 on etap3.CPF_fase2 = etap1.CPF_Cliente

--================================================================--
-- Fluxo do Onboarding - Orbitall - Fluxo Criação de Conta
--================================================================--
left join (

with

base_orbitall as (
select 
distinct
'Conta Orbitall' as Onboarding3
,CustAccount.customer_id
,cl.status as StatusConta
,date(cl.created_at) as Dt_Orbitall 
,FORMAT_DATETIME("%Y%m",cl.created_at) as Safra_Orbitall
,CustAccount.status
,CustAccount.created_at as Dt_Cadastro
,Cl.document as cpf_Orbitall
,Cl.full_name as NomeCliente
,AccountEv.type
,AccountEv.message
,RANK() OVER (PARTITION BY Cl.document ORDER BY  AccountEv.id, AccountEv.created_at desc) AS Rank_Ult_Decisao_Orbitall

from `eai-datalake-data-sandbox.core.customers` Cl 
join `eai-datalake-data-sandbox.payment.customer_account` CustAccount on CustAccount.customer_id = Cl.uuid
join (select distinct * from `eai-datalake-data-sandbox.payment.customer_account_event` ) AccountEv on AccountEv.customer_account_id = CustAccount.id 

)
select
a.*
from base_orbitall a
join Base_Inicial_Fluxo_AllowMe etap1 on etap1.CPF_Cliente = a.cpf_Orbitall 
where a.Rank_Ult_Decisao_Orbitall = 1
) etap4 on etap4.cpf_Orbitall = etap1.CPF_Cliente
;
-----------------------------------------------------------------------------------------------------------

--================================================================--
-- Fluxo do Onboarding - cubo - Fluxo Criação de Conta
--================================================================--

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Funil_FluxoOnboardig` AS 


select
distinct

Onboarding
,CPF_Cliente
,Dt_Cadastro_AllowMe
,Safra_AllowMe
,Dia_Cadastro_AllowMe
,Rank_Ult_Decisao
,Flag_Dec_1RegraAllowMe

,Onboarding1
,CPF_fase1
,data_cadastro_fase1
,Safra_fase1
,Flag_Fase_fase1
,Flag_Decisao_Motor_fase1


,Onboarding2
,CPF_fase2
,data_cadastro_fase2
,Safra_fase2
,Flag_Fase_fase2
,Flag_Decisao_Motor_fase2

,Onboarding3
,StatusConta
,status
,Dt_Orbitall
,Safra_Orbitall
,cpf_Orbitall
,NomeCliente
,type
,message


,Case 
  when 
  Dt_Cadastro_AllowMe = data_cadastro_fase1 
  and Dt_Cadastro_AllowMe = data_cadastro_fase2 
  and Dt_Cadastro_AllowMe = Dt_Orbitall 
  then 'Fluxo AllowMe,Fase1,Fase2,FaseConta'
  when 
  Dt_Cadastro_AllowMe = data_cadastro_fase1 
  and Dt_Cadastro_AllowMe > data_cadastro_fase2
  and Dt_Cadastro_AllowMe = Dt_Orbitall 
  then 'Fluxo Reprocessamento AllowMe, Fase1, Antigo Fase2, FaseConta '
  when 
  Dt_Cadastro_AllowMe = data_cadastro_fase1 
  and Dt_Cadastro_AllowMe > data_cadastro_fase2
  and Dt_Orbitall is null
  then 'Fluxo AllowMe, Fase1, Antigo Fase2, Parou '
  when 
  CPF_fase1 is null and CPF_fase2 is null and cpf_Orbitall is null 
  then 'Fluxo Parou na AllowMe'
  when 
  Dt_Cadastro_AllowMe = data_cadastro_fase1 
  and data_cadastro_fase2 is null
  and Dt_Orbitall is null
  then 'Fluxo AllowMe,Fase1,Parou'
  when 
  Dt_Cadastro_AllowMe > data_cadastro_fase1 
  and data_cadastro_fase2 is null
  and Dt_Orbitall is null
  then 'Fluxo Reprocessamento AllowMe, Antigo Fase1,Parou'
  when 
  Dt_Cadastro_AllowMe < data_cadastro_fase1 
  and data_cadastro_fase2 is null
  and Dt_Orbitall is null
  then 'Fluxo Pendente AllowMe, Fase1,Parou'
  when 
  Dt_Cadastro_AllowMe > Dt_Orbitall 
  and data_cadastro_fase1 is null
  and data_cadastro_fase2 is null
  then 'Fluxo Reprocessamento AllowMe, Sem Motor,FaseConta'
  when 
  Dt_Cadastro_AllowMe = data_cadastro_fase1 
  and data_cadastro_fase2 is null
  and Dt_Cadastro_AllowMe > Dt_Orbitall 
  then 'Fluxo Reprocessamento AllowMe, Fase1,FaseConta Antiga'
  when 
  Dt_Cadastro_AllowMe > data_cadastro_fase1 
  and data_cadastro_fase2 is null
  and data_cadastro_fase1 = Dt_Orbitall 
  then 'Fluxo Reprocessamento AllowMe, Fase1,FaseConta Antiga'
  when 
  Dt_Cadastro_AllowMe > data_cadastro_fase1 
  and data_cadastro_fase2 is null
  and data_cadastro_fase1 > Dt_Orbitall 
  then 'Fluxo Reprocessamento AllowMe, Reprocessamento Fase1,FaseConta Antiga'
  when 
  Dt_Cadastro_AllowMe > data_cadastro_fase2 
  and data_cadastro_fase1 is null
  and data_cadastro_fase2 = Dt_Orbitall 
  then 'Fluxo Reprocessamento AllowMe, Antiga Fase2,FaseConta Antiga'
  when 
  Dt_Cadastro_AllowMe >= data_cadastro_fase1
  and Dt_Cadastro_AllowMe > data_cadastro_fase2
  and data_cadastro_fase2 >= Dt_Orbitall 
  then 'Fluxo Reprocessamento AllowMe, Antiga Fase2,FaseConta Antiga'
  when 
  Dt_Cadastro_AllowMe >= data_cadastro_fase2
  and data_cadastro_fase1 is null
  and Dt_Orbitall is null
  then 'Fluxo Reprocessamento AllowMe, Antiga Fase2,Parou'
  when 
  Dt_Cadastro_AllowMe = data_cadastro_fase1
  and data_cadastro_fase1 = Dt_Orbitall
  and data_cadastro_fase2 is null
  then 'Fluxo AllowMe, Fase1,Parou Fase2,FaseConta'
  when 
  Dt_Cadastro_AllowMe >= data_cadastro_fase1
  and Dt_Cadastro_AllowMe >= data_cadastro_fase2
  and Dt_Orbitall is null
  then 'Fluxo Reprocessamento AllowMe, Fase1,Fase2,Parou'
  when 
  data_cadastro_fase1 is not null
  and data_cadastro_fase2 is not null
  and Dt_Orbitall is null
  then 'Fluxo Reprocessamento AllowMe,Passou no motor, Parou'
  when 
  data_cadastro_fase1 is null
  and data_cadastro_fase2 is not null
  and Dt_Orbitall is null
  then 'Fluxo Reprocessamento AllowMe,Fase2, Parou'
  when 
  data_cadastro_fase1 is null
  and data_cadastro_fase2 is not null
  and Dt_Orbitall is not null
  then 'Fluxo Reprocessamento AllowMe,Fase2, FaseConta'
  when 
  data_cadastro_fase1 is null
  and data_cadastro_fase2 is null
  and Dt_Orbitall is not null
  then 'Fluxo Reprocessamento AllowMe,Fase1, FaseConta'
    when 
  data_cadastro_fase1 is null
  and data_cadastro_fase2 is null
  and Dt_Orbitall is not null
  then 'Fluxo Reprocessamento AllowMe,Verifcar base Zaig, FaseConta'
  when 
  data_cadastro_fase1 is not null
  and data_cadastro_fase2 is not null
  and Dt_Orbitall is not null
  then 'Fluxo Reprocessamento AllowMe,Fase1,Fase2, FaseConta'
  when 
  data_cadastro_fase1 is not null
  and data_cadastro_fase2 is null
  and Dt_Orbitall is not null
  then 'Fluxo Reprocessamento AllowMe,Fase1, FaseConta'


 
  else 'NA' end as Flag_Funil_Desc
  
  ,case
  when 
  Dt_Cadastro_AllowMe = data_cadastro_fase1 
  and Dt_Cadastro_AllowMe = data_cadastro_fase2 
  and Dt_Cadastro_AllowMe = Dt_Orbitall 
  then 'Fluxo AllowMe,Fase1,Fase2,FaseConta'
  when 
  Dt_Orbitall is not null
  then 'Fluxo Reprocessamento,FaseConta'

   else 'Reprocessamento' end as Flag_Funil

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.FunilOnboarding_0`

;
/*
select
Flag_Funil
,Flag_Funil_Desc
,count(distinct CPF_Cliente) as qtd
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Funil_FluxoOnboardig`
group by 1,2
order by 2 desc

;

select
*
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cubo_Funil_FluxoOnboardig`
where Flag_Funil = 'NA'
*/

/*
Dt_Cadastro_AllowMe
data_cadastro_fase1
data_cadastro_fase2
Dt_Orbiatall

*/

