--==========================================================================================================
-- Base Cadastro Chave PIX
--==========================================================================================================

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cadastro_ChavePIX` 


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cadastro_ChavePIX` AS 
with

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
      ,cl.created_at as Dt_AberturaConta

      ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
      ,FORMAT_DATE("%Y",cl.created_at)as Ano
      ,'+55'||ph.area_code||ph.number as Phone
      ,ph.area_code as DDD
      ,ph.number as Telefone
      ,ph.type as TipoTelefone
      ,cl.trusted as Trusted
      ,Ev.status as Status_Conta
      ,cl.risk_analysis_status as RiskAnalysis
      ,ev.observation as MotivoStatus
      ,ev.event_date as DataStatus
      ,ev.user_name as UsuarioStatus
      ,RANK() OVER (PARTITION BY cl.uuid ORDER BY en.id, id.phone_id  desc) AS Rank_Ult_Atual
      ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date  desc) AS Rank_StatusConta
      ,Case 
            When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<18  Then '01  MenorIdade'
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
      ,CASE
            WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=1 THEN '00_<1DIAS'
            WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=5 THEN '01_<5DIAS'
            WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=30 THEN '02_<30DIAS'
            WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=60 THEN '03_<60DIAS'
            WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=90 THEN '04_<90DIAS'
            WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=120 THEN '05_<120DIAS'
            WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=160 THEN '06_<160DIAS'
            WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=190 THEN '07_<190DIAS'
            WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=220 THEN '08_<220DIAS'
            WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=260 THEN '09_<260DIAS'
            WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=290 THEN '10_<290DIAS'
            WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=365 THEN '11_1ANO'
            WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) >=365 THEN '12_+1ANO'
      else '13_NaoTemConta'
      END AS Temp_Conta
      ,case
        when ev.observation In ('Fraude confirmada','Suspeita de fraude')  then 'Bloqueio Fraude'
        when ev.observation = 'Bloqueio de cadastro' then 'Bloqueio Preventivo'
        when ev.observation is null then 'Sem Bloqueio'
        when ev.observation = '' then 'Sem Bloqueio'
      else 'Outros' end as Flag_Bloqueio
      ,CASE
      WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
      WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
      WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
      WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
      WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
      ELSE 'SUL'
      END AS REGIAO

      FROM `eai-datalake-data-sandbox.core.customers`             cl
      left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
      left join `eai-datalake-data-sandbox.core.customer_phone`   id on id.customer_id = cl.id
      left join `eai-datalake-data-sandbox.core.phone`            ph on id.phone_id = ph.id 
      left join (select * from `eai-datalake-data-sandbox.core.customer_event` 
                 where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT')) Ev on ev.customer_id = cl.id
      where 
      date(cl.created_at) >= '2020-01-01' 
      and
       ph.type = 'MOBILE'
), Base_Clientes1 as (
      select
      *
      from Base_Clientes
      where Rank_Ult_Atual = 1
),Base_Clientes2 as (
      select 
      *
      from Base_Clientes1 where Rank_StatusConta = 1 
),Base_Key as (
      SELECT 
      distinct

      id_key.pix_key_id 
      ,key.key_value
      ,clkey.customer_id
      ,RANK() OVER (PARTITION BY key.key_value ORDER BY key.created_at  desc) AS Rank_key
      ,id_key.payment_customer_account_id
      ,key.id
      ,FORMAT_DATE("%Y%m",key.created_at)as Safra_Cad_Key
      ,key.created_at
      ,key.type
      ,key.uuid
      ,key.reason
      ,key.status
      ,pca.payment_account_id

      FROM `eai-datalake-data-sandbox.payment.payment_customer_account_pix_key`   id_key
      join `eai-datalake-data-sandbox.payment.pix_key`                            key     on id_key.pix_key_id = key.id
      join `eai-datalake-data-sandbox.payment.customer_account`                   clkey   on clkey.payment_customer_account_id = id_key.payment_customer_account_id
      join `eai-datalake-data-sandbox.payment.payment_customer_account`           pca     on clkey.payment_customer_account_id = pca.id
),Base_Key1 as (
    select
    distinct
    *
    from Base_Key
    where status not in ('EXCLUDED','ERROR')
    and Rank_key = 1
    order by 2,3
), Base_Key2 as (
  select
  distinct

     Temp_Conta
      ,CustomerID
      ,Nome
      ,CPF
      ,email
      ,Phone
      ,Faixa_Idade
      ,MotivoStatus
      ,Flag_Bloqueio
      ,type
      ,key_value
      ,key.status as StatusKey
      ,Status_Conta
      ,key.created_at as Dt_Key
      ,FORMAT_DATE("%d",key.created_at)as Dia_Key
      ,DATE_DIFF(date(key.created_at),date(cl.Dt_AberturaConta), DAY) as TempoCadastroChave
      ,case
      when type = 'CPF' and key.key_value = cl.CPF then 'CHAVE_CPF_CLIENTE_OK'
      when type = 'CPF' and key.key_value <> cl.CPF then 'CHAVE_CPF_CLIENTE_NOOK'
      when type = 'EMAIL' and key.key_value = cl.email then 'CHAVE_EMAIL_CLIENTE_OK'
      when type = 'EMAIL' and key.key_value <> cl.email then 'CHAVE_EMAIL_CLIENTE_NOOK'
      when type = 'PHONE' and key.key_value = cl.Phone then 'CHAVE_PHONE_CLIENTE_OK'
      when type = 'PHONE' and key.key_value <> cl.Phone then 'CHAVE_PHONE_CLIENTE_NOOK'
      when type is null then 'SEM_CHAVE_CLIENTE'
      else 'EVP' end as Flag_Key_Cliente 

  from Base_Clientes2     cl
  join Base_Key1     key on key.customer_id = cl.CustomerID
  order by 3
)   select
    distinct
      a.* 
      ,case
        when TempoCadastroChave = 0 then '00 MesmoDia'
        when TempoCadastroChave <= 5 then '01 1_5 Dia'
        when TempoCadastroChave <= 10 then '02 6_10 Dia'
        when TempoCadastroChave <= 20 then '03 11_20 Dia'
        when TempoCadastroChave <= 30 then '04 21_30 Dia'
        when TempoCadastroChave <= 60 then '05 31_60 Dia'
        when TempoCadastroChave <= 90 then '06 61_90 Dia'
        when TempoCadastroChave <= 120 then '07 91_120 Dia'
        when TempoCadastroChave <= 150 then '08 121_150 Dia'
        when TempoCadastroChave <= 180 then '09 151_180 Dia'
        when TempoCadastroChave > 180 then '10 >181 Dia'
      end as Flag_TempCadastro
      ,case
        when DATE_DIFF(date(current_date),date(Dt_Key), Month) = 0 then 'M0'
        when DATE_DIFF(date(current_date),date(Dt_Key), Month) = 1 then 'M-1'
        when DATE_DIFF(date(current_date),date(Dt_Key), Month) = 2 then 'M-2'
        when DATE_DIFF(date(current_date),date(Dt_Key), Month) = 3 then 'M-3'
        when DATE_DIFF(date(current_date),date(Dt_Key), Month) = 4 then 'M-4'
        when DATE_DIFF(date(current_date),date(Dt_Key), Month) = 5 then 'M-5'
        when DATE_DIFF(date(current_date),date(Dt_Key), Month) = 6 then 'M-6'
      else 'Outros' end as Flag_Filtro_Periodo

    from Base_Key2 a
    where StatusKey = 'COMPLETED'

;

--==========================================================================================================
-- Base Cadastro Chave PIX - Volume
--==========================================================================================================

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cadastro_ChavePIX_Vol` 


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cadastro_ChavePIX_Vol` AS 

with
base_chavePix as (
select 
distinct
CustomerID
,CPF
,Temp_Conta
,Faixa_Idade
,Flag_Bloqueio
,MotivoStatus
,Flag_TempCadastro
,Flag_Key_Cliente
,type as TipoChave
,count(distinct key_value) QtdChaveCadastrada

from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Cadastro_ChavePIX` 
--where Flag_Bloqueio = 'Sem Bloqueio'
group by 1,2,3,4,5,6,7,8,9
order by 10 desc
)
select
*
from base_chavePix
where QtdChaveCadastrada > 2

