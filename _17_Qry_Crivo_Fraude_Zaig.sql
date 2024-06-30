--> TABELA CRIVO DE FRAUDE NO ONBOARDING - VISÃO ZAIG
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Fraude_Zaig` where Flag_RiscoCadastro = '5_NA' -- Cpf_Cliente = '10792237625'
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` where Cpf_Cliente = '37140259879'

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_Fraude_Zaig` AS 

with

Base_Cadastro as (
select
distinct
    data_cadastro
    ,cl.uuid as Customer_ID
    ,cl.status
    ,case 
    when cl.status is not null then 'Zaig_Orbitall'
    when cl.status is null then 'So_Zaig'
    end as Flag_Processo
    ,case
            when zaig.decisao = "automatically_approved" then 'Aprovado'
            when zaig.decisao = "automatically_reproved" then 'Negado'
          else 'Pending' end as Flag_Decisao_Motor
    ,CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=0   THEN '01_00-Hoje'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=1   THEN '02_01-Ontem'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=10   THEN '03_00-10DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=30   THEN '04_11-30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=60   THEN '05_31-60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=90   THEN '06_61-90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=180  THEN '07_91-180DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) <=364  THEN '08_180-1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",data_cadastro)), DAY) >=365  THEN '09_+1ANO'
    END AS Flag_DtCadastro
    ,CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=0   THEN '01_00-Hoje'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=1   THEN '02_01-Ontem'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=10   THEN '03_00-10DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=30   THEN '04_11-30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=60   THEN '05_31-60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=90   THEN '06_61-90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=180  THEN '07_91-180DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=364  THEN '08_180-1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) >=365  THEN '09_+1ANO'
    END AS Flag_TempodeConta
        ,Case 
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<18   Then '01  MenorIdade'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=20  Then '02  18a20anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=25  Then '04  21a25anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=30  Then '05  26a30anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=35  Then '06  31a35anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=40  Then '07  36a40anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=45  Then '08  41a45anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=50  Then '09  46a50anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=55  Then '10 51a55anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=60  Then '11 56a60anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=65  Then '12 61a65anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=70  Then '13 66a70anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=75  Then '14 71a75anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=80  Then '15 76a80anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=85  Then '16 81a85anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)>85   Then '17 >86anos'  
    End as Faixa_Idade
    ,case when loyalty.Dt_Nascimento = cl.birth_date then 'OK_Dt_Nascimento' else 'No_Macht' end as Flag_Dt_Nascimento
    ,case 
    when zaig.score_makrosystem <= 30 then 'Reprovado'
    when zaig.score_makrosystem <= 50 then 'Neutro'
    when zaig.score_makrosystem > 50 then 'Aprovado'
    else 'NA' end as Flag_Decisao_Makro
    ,zaig.score_makrosystem
    ,zaig.ScoreZaig
    ,case
    when zaig.ScoreZaig <= 7 then 'Aprovado'
    when zaig.ScoreZaig between 8 and 9 then 'Aprovado em situações especiais'
    when zaig.ScoreZaig >=10 then 'Negado'
    end as Flag_ScoreZaig

    ,zaig.Cpf_Cliente
    ,cl.full_name as NomeCliente
    ,zaig.email
    ,substring(zaig.email,1,STRPOS(zaig.email,'@')) as Desc_Email
    ,substring(zaig.email,STRPOS(zaig.email,'@'),25) as Provedor_Email
    ,zaig.ddd
    ,zaig.Num_Celuar
    ,zaig.Uf_Celuar
    ,zaig.End_Num
    ,zaig.Bairro_Cep
    ,zaig.UF_Cep
    ,zaig.UF
    ,zaig.ip
    ,End_Completo
    ,gps_latitude
    ,gps_longitude

from ( with base as (select 
          distinct
          Cpf_Cliente
          ,data_cadastro
          ,natural_person_id
          ,email
          ,ddd
          ,ddd||'-'||numero as Num_Celuar
          ,estado ||'-'||ddd||'-'||numero as Uf_Celuar
          ,rua ||','|| numero_9 as End_Num
          ,bairro ||'-'|| cep as Bairro_Cep
          ,estado ||'-'|| cep as UF_Cep
          ,rua ||','|| numero_9 ||'-'|| bairro||'-'|| cep ||'-'|| estado as End_Completo
          ,estado as UF
          ,ip
          ,score_makrosystem
          ,tree_score as ScoreZaig
          ,decisao
          ,gps_latitude
          ,gps_longitude
          ,RANK() OVER (PARTITION BY cpf,date(data_cadastro) ORDER BY EXTRACT(time FROM data_cadastro) desc) AS Rank_Ult_Decisao
          --,RANK() OVER (PARTITION BY cpf ORDER BY data_cadastro desc) AS Rank_Ult_Decisao
      from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` 
      where
      date(data_cadastro) >= current_date - 30
      --and decisao = "automatically_approved"
      --and Cpf_Cliente = '61969036672' 
      order by 2 desc) select * from base where Rank_Ult_Decisao = 1
      ) zaig
left join `eai-datalake-data-sandbox.core.customers`   cl                on cl.document = zaig.Cpf_Cliente and Rank_Ult_Decisao = 1
left join
(select
distinct
loyalty.ParticipanteID
,loyalty.DataCadastro as Dt_CadastroLoyalty
,FORMAT_DATETIME("%Y%m",loyalty.DataCadastro) as Safra_KM
,loyalty.CPF
,loyalty.Nome
,loyalty.Email
,loyalty.DatadeNascimento as Dt_Nascimento
,loyalty.TipoOrigemID
,loyalty.flgPreCadastro
,loyalty.Vip
,loyalty.Inativo
from `eai-datalake-data-sandbox.loyalty.tblParticipantes` loyalty
where loyalty.Inativo = false) loyalty on loyalty.CPF = zaig.Cpf_Cliente
--where
--cl.status in ('ACTIVE','BLOCKED')
--and  
--cl.uuid  = 'CUS-1d0b4a9f-2a34-43ab-9ba9-08064743cb48'
), Qtd_Email_CPF as (
select
distinct
Desc_Email
,count(distinct Customer_ID) as Qtd_CPF_Email
from Base_Cadastro
group by 1 order by 2 desc
), Qtd_End_CPF as (
select
distinct
End_Completo
,count(distinct Customer_ID) as Qtd_CPF_End
from Base_Cadastro
group by 1 order by 2 desc
), Qtd_Celular_CPF as (
select
distinct
Num_Celuar
,count(distinct Customer_ID) as Qtd_CPF_Celular
from Base_Cadastro
group by 1 order by 2 desc
), Qtd_ip_CPF as (
select
distinct
ip
,count(distinct Customer_ID) as Qtd_CPF_ip
from Base_Cadastro
group by 1 order by 2 desc
), base_composicao_risco as (
select
distinct
--base.data_cadastro
base.Customer_ID
,base.Flag_DtCadastro
,base.Cpf_Cliente
,base.status
,base.Faixa_Idade
,Flag_Dt_Nascimento
,base.Flag_Processo
,base.Flag_TempodeConta
,base.Flag_Decisao_Makro
,base.Flag_ScoreZaig
,base.Flag_Decisao_Motor
--,base.NomeCliente
--,base.email
,base.UF
,base.ddd
,base.Desc_Email
,base.Provedor_Email
,base.Num_Celuar
,base.Uf_Celuar
,base.End_Num
,base.Bairro_Cep
,base.UF_Cep
,base.ip
,base.End_Completo
,left(cast(base.gps_latitude as String),10)  as gps_latitude
,left(cast(base.gps_longitude as String),10) as gps_longitude
,left(cast(base.gps_latitude as String),6)||','|| left(cast(base.gps_longitude as String),6) as Local_Gps
--,score_makrosystem
,Qtd_CPF_End
,Qtd_CPF_Celular
,Qtd_CPF_Email
,Qtd_CPF_ip
,case
when Qtd_CPF_End=1 and Qtd_CPF_Celular=1 and Qtd_CPF_Email=1 and Qtd_CPF_ip<=30 and Flag_Decisao_Makro = 'Aprovado' and base.ScoreZaig <=7 then  '0_RiscoBaixíssimo'
when Qtd_CPF_End<=10 and Qtd_CPF_Celular=1 and Qtd_CPF_Email=1 and Qtd_CPF_ip<=30 and Flag_Decisao_Makro in ('Aprovado','Neutro') or ScoreZaig between 8 and 9 then '1_RiscoBaixo'
when Qtd_CPF_End<=100 and Qtd_CPF_Celular=1 and Qtd_CPF_Email>1 and Qtd_CPF_ip<=30 and Flag_Decisao_Makro in ('Aprovado','Neutro') or ScoreZaig between 8 and 9  then '2_RiscoMedio'
when Qtd_CPF_End<=200 and Qtd_CPF_Celular=1 and Qtd_CPF_Email<=10 and Qtd_CPF_ip<=60 and Flag_Decisao_Makro <> 'Aprovado' or base.ScoreZaig >=10 then '3_RiscoAlto'
when Qtd_CPF_End>=1 and Qtd_CPF_Celular>=1 and Qtd_CPF_Email>=1 and Qtd_CPF_ip>=1 and Flag_Decisao_Makro <> 'Aprovado' and base.ScoreZaig >=10 then '4_RiscoAltissimo'
else '5_RiscoCritico' end as Flag_RiscoCadastro
,case 
  when pf.Cpf_Resticao_Mot = cast(base.Cpf_Cliente as numeric) then 'OpercaoPF'
  when vip.CPF = base.Cpf_Cliente then 'VIP'
  when uber.cpf = base.Cpf_Cliente then 'UBER'
  else 'URBANO'end as Flag_Perfil

from Base_Cadastro      base
join Qtd_End_CPF        Qtd_End_CPF     on Qtd_End_CPF.End_Completo = base.End_Completo
join Qtd_Celular_CPF    Qtd_Celular_CPF on Qtd_Celular_CPF.Num_Celuar = base.Num_Celuar
join Qtd_Email_CPF      Qtd_Email_CPF   on Qtd_Email_CPF.Desc_Email = base.Desc_Email
join Qtd_ip_CPF         Qtd_ip_CPF      on Qtd_ip_CPF.ip = base.ip
LEFT JOIN (select distinct CPF from `eai-datalake-data-sandbox.loyalty.tblParticipantes` where Vip is not null and Inativo = false) as vip on vip.CPF = base.Cpf_Cliente
LEFT JOIN (select cpf from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias`) as uber on uber.cpf = base.Cpf_Cliente
LEFT JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.TB_MONIT_CPF_OP_PF` pf on pf.Cpf_Resticao_Mot = cast(base.Cpf_Cliente as numeric) 
)

select
*
from base_composicao_risco