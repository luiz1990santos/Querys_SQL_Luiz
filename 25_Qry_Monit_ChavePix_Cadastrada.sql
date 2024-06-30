-- BASE CHAVE PIX CADASTRADA


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_ChavePix_Cadastrada_CPF_0` AS 

  with

  BASE_CHAVEPIX_CPF AS (  
  
    select
    distinct

      id_key.pix_key_id 
      ,key.key_value AS ChaveCadastra
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
  where 
  key.type = 'CPF'
  and key.status = 'COMPLETED'
  order by 3 desc

),BASE_DADOS_CLIENTE as (

    select
    distinct
      cl.uuid as  CustomerID
      ,cl.full_name as Nome
      ,cl.document as CPF
      ,cl.email
      ,en.street as Rua
      ,en.neighborhood as Bairro
      ,en.city as Cidade
      ,en.state as UF
      ,FORMAT_DATE("%d-%m-%Y",cl.created_at)as DataCriacao
      ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
      ,FORMAT_DATE("%Y",cl.created_at)as Ano
      --,cl.created_at as DataCriacao
      ,ph.area_code as DDD
      ,ph.number as Telefone
      ,ph.type as TipoTelefone
      ,cl.trusted as Trusted
      ,cl.status as Status_Conta
      ,Ev.status as Status_Evento
      ,cl.risk_analysis_status as RiskAnalysis
      ,ev.observation as MotivoStatus
      ,ev.event_date as DataStatus
      ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Bloqueio
      ,ev.user_name as UsuarioStatus
      ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date  desc) AS Rank_Ult_Atual
      ,CASE
      WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
      WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
      WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
      WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
      WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
      ELSE 'SUL'
      END AS REGIAO
      ,CVPIX.ChaveCadastra
      ,CVPIX.reason
      ,CVPIX.Safra_Cad_Key
      ,case when CVPIX.ChaveCadastra = cl.document then 'CHAVE_CPF_CLIENTE_OK' else 'CHAVE_CPF_CLIENTE_NOOK' end as Flag_ValidarChave
      ,DATE_DIFF(date(CVPIX.created_at),date(cl.created_at), DAY) as TempoCadastroChave

  FROM `eai-datalake-data-sandbox.core.customers`             cl
  JOIN BASE_CHAVEPIX_CPF                                      CVPIX ON CVPIX.customer_id = cl.uuid
  left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
  left join  `eai-datalake-data-sandbox.core.customer_phone`     id on id.customer_id = cl.id
  left join (select * from`eai-datalake-data-sandbox.core.phone` where type = 'MOBILE')       ph on id.phone_id = ph.id 
  left join (select * from `eai-datalake-data-sandbox.core.customer_event`  
  where status not in ('FACIAL_BIOMETRICS_VALIDATED', 'TEMPORARY_PERMISSION_CASH_OUT','FACIAL_BIOMETRICS_REJECTED','BLOCK_LIST_UNBOUND','BLOCK_LIST_BOUND','FACIAL_BIOMETRICS_NOT_VALIDATED'))  Ev on ev.customer_id = cl.id
),BASE_DADOS_CLIENTE_1 as (
  select 
  *
  from BASE_DADOS_CLIENTE 
  where Rank_Ult_Atual = 1 
  --and Status_Conta = 'ACTIVE'
  --and CustomerID = 'CUS-3feb825f-92dd-4e8e-8058-af0007526e12'
  and DDD is not null
  --group by 1
  order by 1 desc 

),base_transacional as (
    select
    distinct
        cvpix.*
        ,tranx_d.customer_id
        ,tranx_d.created_transaction_at
        ,payment_transaction.gateway_id
        ,FORMAT_DATE("%Y%m",tranx_d.created_transaction_at)as Safra_Tranx
        ,FORMAT_DATE("%Y",tranx_d.created_transaction_at)as Ano_Tranx
        ,date(tranx_d.created_transaction_at) as Dt_Tranx
        ,EXTRACT(HOUR FROM tranx_d.created_transaction_at)as Hr_Tranx
        ,payment.order_code as Order_ID
        ,tranx_d.operation
        ,tranx_d.status
        ,tranx_d.type
        ,tranx_d.amount/100 as Vl_Tranx
        ,payment_transaction.transaction_value
        ,case
        when payment_transaction.transaction_value is null then (tranx_d.amount/100)
        else payment_transaction.transaction_value end as Flag_Valor
        ,ord.cashback_percentage
        ,ord.cashback_value/100 as cashback_value
        ,payment_transaction.status	as StatusTranx
        ,ord.code
        ,ord.sales_channel
        ,payment_transaction.payment_method	as Tipo_Pagto_App
        ,tranx_d.flow
        ,ord.order_status
        ,case
        when ord.uuid = payment.order_id then payment_transaction.payment_method
        when ord.uuid is null then tranx_d.flow
        when payment_transaction.payment_method	is null then 'Outros'
        when substring(payment.order_code,1,STRPOS(payment.order_code,'-'))	is null then 'Outros'
        when ord.uuid = payment.order_id and tranx_d.status in ('DENIED','CANCELLED') then 'TIPO_N/A'      
        else 'Outros' end as Tipo_Pgto

        ,case 
        when tranx_d.status in ('APPROVED','FINISHED') then 'Aprovado'
        when tranx_d.status in ('DENIED','CANCELLED') then 'Negado'
        when tranx_d.status in ('PROCESSING','SCHEDULED','PENDING') then 'Processando'
        end as Flag_Status_Transacao
        ,case
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 0 then 'M0'
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 1 then 'M-1'
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 2 then 'M-2'
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 3 then 'M-3'
        when DATE_DIFF(date(current_date),date(tranx_d.created_transaction_at), Month) = 4 then 'M-4'
        else 'Outros' end as Flag_Filt_Per
        ,case
        when dppaypal.string_field_0 <> substring(ord.code,1,STRPOS(ord.code,'-')) then tranx_d.type
        when dppaypal.string_field_0 = substring(ord.code,1,STRPOS(ord.code,'-')) then dppaypal.string_field_2
        Else tranx_d.type end as Flag_Merchant_Account
        ,substring(payment.order_code,1,STRPOS(payment.order_code,'-')) as Cod_ID
        ,case when cvpix.CustomerID = tranx_d.customer_id and tranx_d.amount > 0 then 'ComTransacao' else 'SemTransacao' end as Flag_Transacao_Cliente
        ,case when cvpix.CustomerID = tranx_d.customer_id then 'ChavePix_CPF_Cadast' else 'Nao_ChavePix_CPF_Cadast' end as Flag_Cad_Chave

    from `eai-datalake-data-sandbox.elephant.transaction`                                 tranx_d
    left join `eai-datalake-data-sandbox.payment.payment`                                 payment               on tranx_d.own_id = payment.order_id
    left join `eai-datalake-data-sandbox.payment.payment_transaction`                     payment_transaction   on payment.id = payment_transaction.payment_id
    left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.TB_DE_PARA_PAYPAL_PEDIDO`  dppaypal              on dppaypal.string_field_0 = substring(payment.order_code,1,STRPOS(payment.order_code,'-'))
    left join `eai-datalake-data-sandbox.core.orders`                                      ord                   on ord.uuid = tranx_d.own_id
    left join BASE_DADOS_CLIENTE_1                                                        cvpix                  on cvpix.CustomerID = tranx_d.customer_id

    where 
    tranx_d.status in ('APPROVED','FINISHED')--,'DENIED','CANCELLED')
    --and tranx_d.amount > 0
) select
    a.*
    ,case when Flag_Merchant_Account not in ('CASH_OUT','CASH_IN') then 'APP' else 'ContaDigital' end as Flag_TipoConta
    ,case when Flag_Merchant_Account not in ('CASH_OUT','CASH_IN') then 0 else 1 end as Flag_ContaDigital
    ,case when Flag_Merchant_Account not in ('CASH_OUT','CASH_IN') then 1 else 0 end as Flag_APP
  from base_transacional a
    ;

----------------------------------======================================================----------
-- Quantidade de clientes chave pix cadastrada tipo CPF com transação só no APP
----------------------------------======================================================----------

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_ChavePix_Cadastrada_CPF_1` AS 

with

base as (

select
distinct
customer_id
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
,Flag_Cad_Chave
,sum(Flag_ContaDigital) as Flag_ContaDigital
,sum(Flag_APP) as Flag_APP
,count(distinct customer_id) as QtdCliente
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_ChavePix_Cadastrada_CPF_0`  
where 
Flag_Cad_Chave = 'ChavePix_CPF_Cadast'
--and Flag_ContaDigital <> 1
group by 1,2,3

) select * from base where  Flag_ContaDigital = 0 order by 5 desc

;

----------------------------------======================================================----------
-- Volume Chaves pix Cadastradas tipo CPF 
----------------------------------======================================================----------

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_ChavePix_Cadastrada_CPF_2` AS 

  with

  BASE_CHAVEPIX_CPF AS (  
  
    select
    distinct

      id_key.pix_key_id 
      ,key.key_value AS ChaveCadastra
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
  where 
  key.type = 'CPF'
  and key.status = 'COMPLETED'
  order by 3 desc

),BASE_DADOS_CLIENTE as (

    select
    distinct
      cl.uuid as  CustomerID
      ,cl.full_name as Nome
      ,cl.document as CPF
      ,cl.email
      ,en.street as Rua
      ,en.neighborhood as Bairro
      ,en.city as Cidade
      ,en.state as UF
      ,FORMAT_DATE("%d-%m-%Y",cl.created_at)as DataCriacao
      ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
      ,FORMAT_DATE("%Y",cl.created_at)as Ano
      --,cl.created_at as DataCriacao
      ,ph.area_code as DDD
      ,ph.number as Telefone
      ,ph.type as TipoTelefone
      ,cl.trusted as Trusted
      ,cl.status as Status_Conta
      ,Ev.status as Status_Evento
      ,cl.risk_analysis_status as RiskAnalysis
      ,ev.observation as MotivoStatus
      ,ev.event_date as DataStatus
      ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Bloqueio
      ,ev.user_name as UsuarioStatus
      ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date  desc) AS Rank_Ult_Atual
      ,CASE
      WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
      WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
      WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
      WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
      WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
      ELSE 'SUL'
      END AS REGIAO
      ,CVPIX.ChaveCadastra
      ,CVPIX.reason
      ,CVPIX.Safra_Cad_Key
      ,date(CVPIX.created_at) as Dt_Key
      ,case when CVPIX.ChaveCadastra = cl.document then 'CHAVE_CPF_CLIENTE_OK' else 'CHAVE_CPF_CLIENTE_NOOK' end as Flag_ValidarChave
      ,DATE_DIFF(date(CVPIX.created_at),date(cl.created_at), DAY) as TempoCadastroChave

  FROM `eai-datalake-data-sandbox.core.customers`             cl
  JOIN BASE_CHAVEPIX_CPF                                      CVPIX ON CVPIX.customer_id = cl.uuid
  left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
  left join  `eai-datalake-data-sandbox.core.customer_phone`     id on id.customer_id = cl.id
  left join (select * from`eai-datalake-data-sandbox.core.phone` where type = 'MOBILE')       ph on id.phone_id = ph.id 
  left join (select * from `eai-datalake-data-sandbox.core.customer_event`  
  where status not in ('FACIAL_BIOMETRICS_VALIDATED', 'TEMPORARY_PERMISSION_CASH_OUT','FACIAL_BIOMETRICS_REJECTED','BLOCK_LIST_UNBOUND','BLOCK_LIST_BOUND','FACIAL_BIOMETRICS_NOT_VALIDATED'))  Ev on ev.customer_id = cl.id
) --,BASE_DADOS_CLIENTE_1 as (
  select 
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

  from BASE_DADOS_CLIENTE a
  where Rank_Ult_Atual = 1 
  --and Status_Conta = 'ACTIVE'
  --and CustomerID = 'CUS-3feb825f-92dd-4e8e-8058-af0007526e12'
  and DDD is not null
  --group by 1
  order by 1 desc 

;

----------------------------------======================================================----------
-- Validar Chaves PIX cadastradas 
----------------------------------======================================================----------
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_ChavePix_Cadastrada_CPF_3`
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_ChavePix_Cadastrada_CPF_3` AS 

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
      ,cl.created_at as DataCriacao
      --,FORMAT_DATE("%d-%m-%Y",cl.created_at)as DataCriacao
      ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
      ,FORMAT_DATE("%Y",cl.created_at)as Ano
      --,cl.created_at as DataCriacao
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
      --date(cl.created_at) >= '2021-01-01' 
      --ph.area_code = '34'
      --and en.state ='MG'
      --and cl.status = 'ACTIVE'
      --and en.state is null
      --and
      --cl.uuid = 'CUS-f0589f17-810f-413b-b04a-ea6385af9ed2'
      --ev.observation = 'Fraude confirmada'
      --and
       ph.type = 'MOBILE'
      --cl.full_name like 'ISMAEL%'
      --cl.email = 'carlafluhr4@gmail.com'
      --cl.document = '12760051803'
      --cl.id = 4779956
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
      Safra_Abertura
      ,Safra_Cad_Key
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
      ,payment_account_id
      ,key.status as StatusKey
      ,Status_Conta
      ,key.created_at as Dt_Key
      ,FORMAT_DATE("%d",key.created_at)as Dia_Key
      ,DATE_DIFF(date(key.created_at),date(cl.DataCriacao), DAY) as TempoCadastroChave
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

