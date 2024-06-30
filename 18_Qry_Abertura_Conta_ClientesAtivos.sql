
-------------------------******************************************-------------------------
-- ABERTURA DE CONTAS

--select * from `eai-datalake-data-sandbox.core.phone` 
--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_volume_contas`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_volume_contas` AS 

with

Base_Clientes as (

SELECT
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
          ,ph.type as Telefone
          ,cl.trusted as Trusted
          ,cl.status
          ,cl.risk_analysis_status as RiskAnalysis
          ,ev.observation as MotivoStatus
          ,ev.event_date as DataStatus
          ,ev.user_name as UsuarioStatus
          ,RANK() OVER (PARTITION BY cl.uuid ORDER BY en.id, id.phone_id  desc) AS Rank_Ult_Atual
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
          left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id
where 
date(cl.created_at) >= '2020-01-01' 
--and cl.status = 'ACTIVE'
--and en.state is null
--cl.uuid = 'CUS-92e2882c-7532-4d8f-9965-9864d1cab015'
--cl.full_name like 'ISMAEL%'
--cl.email = 'carlafluhr4@gmail.com'
--cl.document = '12760051803'
--cl.id = 4779956
) select 
Safra_Abertura
,Ano
,status
,REGIAO
,count(distinct CustomerID) as VolumeContas
 from Base_Clientes where Rank_Ult_Atual = 1 group by 1,2,3,4

;
-------------------------******************************************-------------------------
-- CLIENTES ATIVOS 

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_clientes_ativos` AS 

select
    format_date("%Y%m", o.created_at) as Safra_Ativacao
   ,format_date("%Y", o.created_at) as Ano
   ,o.sales_channel as Canal
   ,count(distinct c.document)  as Clientes
   ,count(distinct o.pdv_token) as Transacoes
   ,round(sum(o.order_value),0) as TPV 

from `eai-datalake-data-sandbox.core.customers` c
join `eai-datalake-data-sandbox.core.orders` o on c.id = o.customer_id
where 
   --date(o.created_at) between '2021-01-01' and '2022-06-30'
   date(o.created_at) >= '2020-01-01' 
   and o.order_status = 'CONFIRMED'
   and c.risk_analysis_status = 'APPROVED'
   and c.Status = 'ACTIVE'
group by 1,2,3
order by 1 desc

;

--------------------------------------------------------------------
-------------------------******************************************-------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_ClientesAtivos_Tranx` AS 

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
          ,FORMAT_DATE("%Y-%m-%d",cl.created_at)as DataCriacao
          ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
          ,FORMAT_DATE("%Y",cl.created_at)as Ano
          --,cl.created_at as DataCriacao
          ,ph.area_code as DDD
          ,ph.number as Telefone
          ,ph.type as Telefone
          ,cl.trusted as Trusted
          ,cl.status
          ,cl.risk_analysis_status as RiskAnalysis
          ,ev.observation as MotivoStatus
          ,ev.event_date as DataStatus
          ,ev.user_name as UsuarioStatus
          ,RANK() OVER (PARTITION BY cl.uuid ORDER BY en.id, id.phone_id  desc) AS Rank_Ult_Atual
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
          left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id
where 
date(cl.created_at) >= '2020-01-01' 
and cl.risk_analysis_status = 'APPROVED'
and cl.Status = 'ACTIVE'
), Base_Clientes_2 as (
   select
   CustomerID
   ,CPF
   ,Trusted
   ,REGIAO
   ,Safra_Abertura
   ,DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) as DiasdeConta
   ,CASE
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=5 THEN '01_<5DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=30 THEN '02_<30DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=60 THEN '03_<60DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=90 THEN '04_<90DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=120 THEN '05_<120DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=160 THEN '06_<160DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=190 THEN '07_<190DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=220 THEN '08_<220DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=260 THEN '09_<260DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=290 THEN '10_<290DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=365 THEN '11_1ANO'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) >=365 THEN '12_+1ANO'
   END AS Flag_TempodeConta

   from Base_Clientes
),Base_cadastro_cartao as (
                     select
                     distinct
                     CustomerID
                     ,CPF
                     ,count(distinct card.id) as qtd_Tetativas
                     ,count(distinct last_four_digits) as qtd_cartao
                     ,count(distinct card.document) as qtd_cliente

                     from Base_Clientes_2 base
                     join `eai-datalake-data-sandbox.payment.customer_card`  card  on base.CPF = card.document
                     where
                     card.status = 'VERIFIED'
                     group by 1,2
),Base_Chavepix_CPF AS (  
  
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
  join Base_Clientes_2 base on base.CustomerID = clkey.customer_id
  where 
   key.status = 'COMPLETED'
  order by 3 desc
),Base_Tranx as (
   select
   distinct
      c.uuid as  CustomerID
      ,o.sales_channel as Canal
      ,format_date("%Y%m", o.created_at) as Safra_Ativacao
      ,format_date("%Y", o.created_at) as Ano
      ,count(distinct c.document)  as Clientes
      ,count(distinct o.pdv_token) as Transacoes
      ,round(sum(o.order_value),0) as TPV 

   from Base_Clientes cl
   join `eai-datalake-data-sandbox.core.customers` c on cl.CustomerID = c.uuid
   join `eai-datalake-data-sandbox.core.orders` o on c.id = o.customer_id
   where 
      --date(o.created_at) between '2021-01-01' and '2022-06-30'
      --date(o.created_at) >= '2020-01-01' 
      --and 
      o.order_status = 'CONFIRMED'

   group by 1,2,3,4
   order by 1 desc
   )--, Base_Contas_Ativa_SemTranx as (
         select
         distinct
            --cli.CustomerID
            Flag_TempodeConta
            ,case when Canal is null then 'SEM_TRANSACAO' else Canal end as Flag_Canal
            ,Safra_Abertura
            ,case
            when tranx.CustomerID = cli.CustomerID then 'ComTranx'
            else 'SemTranx' end as Flag_Tranx
            ,case when card.CustomerID = cli.CustomerID then 'ComCartao' else 'SemCartao' end as Flag_CadastroCard
            ,case when Chv_pix.customer_id = cli.CustomerID then 'ComChave' else 'SemChave' end as Flag_ChavePix
            ,count(cli.CustomerID) as Qtd_Tranx
            ,count(distinct cli.CustomerID) as Qtd_Cliente
         from Base_Clientes_2   cli
         left join Base_Tranx   tranx on tranx.CustomerID = cli.CustomerID
         left join Base_cadastro_cartao card on card.CustomerID = cli.CustomerID
         left join Base_Chavepix_CPF Chv_pix on Chv_pix.customer_id = cli.CustomerID
         group by 1,2,3,4,5,6
    --) select distinct Canal from Base_Contas_Ativa_SemTranx
;

-------------------------******************************************-------------------------
-- Cliente não ativaram a conta e tentaram cadastrar cartão

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_clientes_naoativados_tentativa_cadastro_cartao` AS 
with
base as (
         select
               date (card.created_at)as Dt_Cadastro
               ,FORMAT_DATETIME("%Y%m",card.created_at) as Safra_Cadastro
               ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
               ,cl.uuid as CustomerID
               ,cl.status as Status_Conta
               ,cl.full_name as Nome_Cliente
               ,card.document
               ,card.status
               ,case 
                     when card.status = 'VERIFIED' then 'Cadastrado'
                     when card.status = 'EXCLUDED' then 'Excluido'
                     when card.status = 'PROCESSOR_DECLINED' then 'NegadoEmissor'
                     when card.status = 'FAILED' then 'NegadoErro'
                     when card.status = 'GATEWAY_REJECTED' then 'NegadoPayPal'
               else 'NA' end as Flag_Status
               ,card.bin
               ,bin.Emissor_do_Banco
               ,bin.Sub_marca
               ,bin.Tipo_de_Card
               ,last_four_digits
               ,count(distinct card.id) as qtd_Tetativas
               ,count(distinct last_four_digits) as qtd_cartao
               ,count(distinct card.document) as qtd_cliente

         from `eai-datalake-data-sandbox.payment.customer_card`                              card
         left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos`      bin on CAST(bin.BIN AS STRING) = card.bin
         join `eai-datalake-data-sandbox.core.customers`                                     cl  on cl.document = card.document
         where   
         date(card.created_at) >= current_date - 200
         --date(card.created_at) >= '2021-01-01'
         group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14

),Base_Tranx as (
         select
         distinct
            c.uuid as  CustomerID
            ,o.sales_channel as Canal
            ,format_date("%Y%m", o.created_at) as Safra_Ativacao
            ,format_date("%Y", o.created_at) as Ano
            ,count(distinct c.document)  as Clientes
            ,count(distinct o.pdv_token) as Transacoes
            ,round(sum(o.order_value),0) as TPV 

         from `eai-datalake-data-sandbox.core.customers` c
         join `eai-datalake-data-sandbox.core.orders`    o     on c.id = o.customer_id
         join base                                       base  on base.CustomerID=c.uuid
         where 
         --date(o.created_at) between '2021-01-01' and '2022-06-30'
         date(o.created_at) >= '2020-01-01' 
         and o.order_status = 'CONFIRMED'
         and c.risk_analysis_status = 'APPROVED'
         and c.Status = 'ACTIVE'
         group by 1,2,3,4
         order by 1 desc
), clientes_ativos_tranx as (
         select
            cli.CustomerID
            ,case
            when tranx.CustomerID = cli.CustomerID then 'ComTranx'
            else 'SemTranx' end as Flag_Tranx
            ,count(cli.CustomerID) as Qtd_Tranx
            ,count(distinct cli.CustomerID) as Qtd_Cliente
            from base                             cli
            left join Base_Tranx                  tranx on tranx.CustomerID = cli.CustomerID
            group by 1,2
) 
select 
Safra_Cadastro
,Safra_Abertura
,Status_Conta
,status
,Flag_Status
,Emissor_do_Banco
,Sub_marca
,Tipo_de_Card
,sum(qtd_Tetativas) as qtd_tentativa
,count(distinct last_four_digits) as  qtd_cartao
,count(distinct base.CustomerID) as qtd_cliente
from base base 
join clientes_ativos_tranx tranx on base.CustomerID=tranx.CustomerID and Flag_Tranx ='SemTranx'
group by 1,2,3,4,5,6,7,8
;

-------------------------******************************************-------------------------
-- Monitorar contas com mais de 90 dias abertas com a primeira transação


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_tmp_ativacao_Conta` AS 


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
          ,date(cl.created_at) <= current_date - 90 as teste
          ,EXTRACT(MONTH FROM date(cl.created_at)) as Mes
          ,FORMAT_DATE("%Y-%m-%d",cl.created_at)as DataCriacao
          ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
          ,FORMAT_DATE("%Y",cl.created_at)as Ano
          --,cl.created_at as DataCriacao
          ,ph.area_code as DDD
          ,ph.number as Telefone
          ,ph.type as Telefone
          ,cl.trusted as Trusted
          ,cl.status
          ,cl.risk_analysis_status as RiskAnalysis
          ,ev.observation as MotivoStatus
          ,ev.event_date as DataStatus
          ,ev.user_name as UsuarioStatus
          ,RANK() OVER (PARTITION BY cl.uuid ORDER BY en.id, id.phone_id  desc) AS Rank_Ult_Atual
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
          left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id
where 
date(cl.created_at) <= (select max(date(created_at))-90  FROM `eai-datalake-data-sandbox.core.customers` )
and cl.status = 'ACTIVE'
), Base_Clientes_2 as (
   select
   CustomerID
   ,CPF
   ,DataCriacao
   ,Trusted
   ,REGIAO
   ,Safra_Abertura
   ,DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) as DiasdeConta
   ,CASE
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=5 THEN '01_<5DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=30 THEN '02_<30DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=60 THEN '03_<60DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=90 THEN '04_<90DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=120 THEN '05_<120DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=160 THEN '06_<160DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=190 THEN '07_<190DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=220 THEN '08_<220DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=260 THEN '09_<260DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=290 THEN '10_<290DIAS'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=365 THEN '11_1ANO'
   WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) >=365 THEN '12_+1ANO'
   END AS Flag_TempodeConta

   from Base_Clientes
   where Rank_Ult_Atual = 1
   --and DATETIME_DIFF(DATETIME(current_date), DATETIME(DataCriacao), DAY) <=190
),Base_Tranx as (
select
cl.*
,DATETIME_DIFF(DATETIME(tranx.created_at), DATETIME(DataCriacao), DAY) as TempTranx
--DataCriacao
,format_date("%Y%m", tranx.created_at) as Safra_Ativacao
,tranx.status
,tranx.type
,tranx.flow
,tranx.amount/100 as VlTranx
,RANK() OVER (PARTITION BY cl.CustomerID ORDER BY tranx.created_at) AS Rank_Tranx

from `eai-datalake-data-sandbox.elephant.transaction`    tranx
join Base_Clientes_2                                     cl on cl.CustomerID = tranx.customer_id
where tranx.status in ('APPROVED','FINISHED')

), base_Consolidada_Abertura_1Tranx as (
select
*
from
Base_Tranx
where 
Rank_Tranx = 1
and TempTranx >=90


)
select

REGIAO
,Safra_Abertura
,Flag_TempodeConta
,Safra_Ativacao
,Trusted
,status as StatusTranx
,type as OrigTranx
,flow as Tipo_Tranx
,case 
   when TempTranx <= 30 then '01_30Dias'
   when TempTranx <= 60 then '02_60Dias'
   when TempTranx <= 90 then '03_90Dias'
   when TempTranx <= 120 then '04_120Dias'
   when TempTranx > 120 then '05_+120Dias' end as Flag_1Tranx
,count(distinct CPF ) as qtdCliente

from base_Consolidada_Abertura_1Tranx
group by 1,2,3,4,5,6,7,8,9







/*




),Base_Tranx as (
   select
   distinct
      c.uuid as  CustomerID
      ,o.sales_channel as Canal
      ,format_date("%Y%m", o.created_at) as Safra_Ativacao
      ,format_date("%Y", o.created_at) as Ano
      ,count(distinct c.document)  as Clientes
      ,count(distinct o.pdv_token) as Transacoes
      ,round(sum(o.order_value),0) as TPV 

   from `eai-datalake-data-sandbox.core.customers` c
   join `eai-datalake-data-sandbox.core.orders` o on c.id = o.customer_id
   where 
      --date(o.created_at) between '2021-01-01' and '2022-06-30'
      date(o.created_at) >= '2021-01-01' 
      and o.order_status = 'CONFIRMED'
      and c.risk_analysis_status = 'APPROVED'
      and c.Status = 'ACTIVE'
   group by 1,2,3,4
   order by 1 desc
   )--Base_Contas_Ativa_SemTranx
    select
    --cli.CustomerID
    Flag_TempodeConta
    ,Canal
    ,Safra_Abertura
     ,case
    when tranx.CustomerID = cli.CustomerID then 'ComTranx'
    else 'SemTranx' end as Flag_Tranx
    ,count(cli.CustomerID) as Qtd_Tranx
    ,count(distinct cli.CustomerID) as Qtd_Cliente
    from Base_Clientes_2   cli
    left join Base_Tranx   tranx on tranx.CustomerID = cli.CustomerID
    group by 1,2,3,4
*/