
create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_OBITO` AS 
with base as (
SELECT distinct 

                            cl.uuid as  CustomerID
                            ,cl.full_name as Nome_Completo
                            ,cl.document as CPF_Cliente
                            --,cl.status as StatusConta
                            ,case 
                               when cl.status = 'ACTIVE' then 'ATIVA'
                               when cl.status = 'BLOCKED' then 'BLOQUEADA'
                               when cl.status = 'INACTIVE' then 'INATIVA'
                               when cl.status = 'MINIMUM_ACCOUNT' then 'MINIMUM_ACCOUNT'
                               else cl.status
                             end as StatusConta
                            ,cl.birth_date as Nascimento
                            ,cl.email as email
                            ,ph.area_code as DDD
                            ,ph.number as Telefone
                            ,ph.type as Tipo_Tel
                            ,en.zipcode as Cep
                            ,en.street as Rua
                            ,en.neighborhood as Bairro
                            ,en.city as Cidade_Cliente
                            ,en.state as UF_Cliente
                            ,cl.created_at as Dt_Abertura
                            ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
                            ,case
                            when cl.trusted = 1 then 'Trusted'
                            else 'NaoTrusted' end as Flag_Trusted
                            ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual
                            ,CASE
                            WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
                            WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
                            WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
                            WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
                            WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
                            ELSE 'NAOINDENTIFICADO'
                            END AS RegiaoCliente
                            ,ev.status as StatusEvento
                            ,ev.observation as MotivoStatus
                            ,ev.event_date as DataStatus
                            ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Ev

                     FROM `eai-datalake-data-sandbox.core.customers`  as cl
                     left join `eai-datalake-data-sandbox.core.address` as en 
                     on en.id = cl.address_id
                     left join (select * from `eai-datalake-data-sandbox.core.customer_event` 
                            where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED',   
                            'FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT'))as ev 
                     on ev.customer_id = cl.id
                     left join `eai-datalake-data-sandbox.core.customer_phone` as cus_ph 
                     on ev.customer_id = cus_ph.customer_id
                     left join `eai-datalake-data-sandbox.core.phone` as ph 
                     on cus_ph.phone_id = ph.id
), base2 as ( 
       select 
          distinct
           CustomerID,
           Nome_Completo,
           CPF_Cliente,
           StatusConta,
           MotivoStatus,
           DataStatus,
           CASE
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY) <=5 THEN '01_<5DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY) <=30 THEN '02_<30DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY) <=60 THEN '03_<60DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY) <=90 THEN '04_<90DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY) <=120 THEN '05_<120DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY) <=160 THEN '06_<160DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY) <=190 THEN '07_<190DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY) <=220 THEN '08_<220DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY) <=260 THEN '09_<260DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY) <=290 THEN '10_<290DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY) <=365 THEN '11_<=1ANO'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY) >365 THEN '12_+1ANO'
           END AS Flag_TempoBloqueado,
           Safra_Ev,
           Dt_Abertura,
           Safra_Abertura,
           CASE
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=5 THEN '01_<5DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=30 THEN '02_<30DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=60 THEN '03_<60DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=90 THEN '04_<90DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=120 THEN '05_<120DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=160 THEN '06_<160DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=190 THEN '07_<190DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=220 THEN '08_<220DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=260 THEN '09_<260DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=290 THEN '10_<290DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) <=365 THEN '11_1ANO'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) >=365 THEN '12_+1ANO'
           END AS Flag_TempodeConta,
           email,
           Tipo_Tel,
           DDD,
           Telefone,
           Nascimento,
           CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(Nascimento), SECOND) / (60*60*24*365) AS INT64) AS idade,
           Cep,
           Rua,
           Bairro,
           Cidade_Cliente,
           UF_Cliente,
           Flag_Trusted,
           Rank_Ult_Atual,
           RegiaoCliente,
           StatusEvento,
           DDD ||'_'|| UF_Cliente as DDD_UF 

       from base
       where Rank_Ult_Atual = 1
), base_obito as (
select LPAD(cast(obt.CPF as string), 11, '0') as CPF, obt.OBITO as obito, cus.* FROM `eai-datalake-data-sandbox.credilink.customers` as obt
join base2 as cus
on obt.CPF = cast(cus.CPF_Cliente as int64)
) 
select * from base_obito
--SELECT StatusConta,MotivoStatus, obito, count(*) FROM base_obito
where obito = 'SIM'
--and Tipo_Tel = 'MOBILE'

--group by 1,2,3
;

SELECT obito, count(*) FROM `eai-datalake-data-sandbox.credilink.customers`
group by 1
;

select distinct StatusConta,MotivoStatus, obito, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_OBITO`
group by 1,2,3
;


-------------------------------------------------------------------------------------------------------
-- LUIZ LEMBRANDO QUE AQUI PRECISA SER EXTRAIDO PARA ÓBITO, AS CONTAS ATIVAS E SUSPEITA DE FRAUDE     |
-------------------------------------------------------------------------------------------------------
create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_OBITO_ChavePIX` as
WITH base AS (
    SELECT 
        *,
        CASE 
            WHEN MotivoStatus IS NULL THEN 'Conta ativa'
            ELSE MotivoStatus
        END AS Flag_MotivoStatus
    FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_OBITO`
),
Base_Key AS (
    SELECT 
        DISTINCT
        key.key_value,
        clkey.customer_id,
        RANK() OVER (PARTITION BY key.key_value ORDER BY key.created_at  desc) AS Rank_key,
        id_key.payment_customer_account_id,
        key.id,
        FORMAT_DATE("%Y%m", key.created_at) AS Safra_Cad_Key,
        key.created_at,
        key.type,
        key.uuid,
        key.reason,
        key.status,
        pca.payment_account_id
    FROM `eai-datalake-data-sandbox.payment.payment_customer_account_pix_key` id_key
    JOIN `eai-datalake-data-sandbox.payment.pix_key` key ON id_key.pix_key_id = key.id
    JOIN `eai-datalake-data-sandbox.payment.customer_account` clkey ON clkey.payment_customer_account_id = id_key.payment_customer_account_id
    JOIN `eai-datalake-data-sandbox.payment.payment_customer_account` pca ON clkey.payment_customer_account_id = pca.id
),
Base_Key1 AS (
    SELECT 
        DISTINCT *
    FROM Base_Key
    WHERE status NOT IN ('EXCLUDED', 'ERROR')
    AND Rank_key = 1
)
SELECT *,
   case 
      when reason is null and type is null then 'Sem chave PIX'
      else 'Com chave PIX'
   end as Flag_ChavePIX
 FROM base AS b1
LEFT JOIN Base_Key1 AS bk ON b1.CustomerID = bk.customer_id;
--select MotivoStatus, count(*) from base
--where MotivoStatus in ('Suspeita de fraude','Bloqueio de cadastro','Conta ativa')
--group by rollup(1)



-- Conta ativa 26.788
-- Suspeita de fraude 2.325
-- Bloqueio de cadastro 11
