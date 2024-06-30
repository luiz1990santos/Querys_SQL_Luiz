/*
 select 
    *
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2`
 where Motivo = 'Chargeback' and Lote >= '2024-04-10'
;
*/

/*
 select MotivoStatus, sub_classification, date(DataStatus), count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`
 where UsuarioStatus like 'Luiz Dos Santos' and sub_classification = 'Fraude Cartão'
group by 1,2,3
;
*/

-- volume 478;


with base_chargeback_desbloqueio as (
 select CustomerID from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`
 where UsuarioStatus like 'Luiz Dos Santos' and sub_classification = 'Fraude Cartão'
), base_todos_ranks as (
    select
              distinct
                     cl.uuid as  CustomerID
                     ,cl.full_name as Nome
                     ,cl.document as CPF_Cliente
                     ,cl.status as StatusConta
                     ,cl.email
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
                     ,Ev.status as StatusEvento
                     ,ev.observation as MotivoStatus
                     ,ev.event_date as DataStatus
                     ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Ev

              FROM `eai-datalake-data-sandbox.core.customers`             cl
              left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
              left join (select * from `eai-datalake-data-sandbox.core.customer_event` 
                     where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT'))Ev on ev.customer_id = cl.id)

select * from base_chargeback_desbloqueio as cbk
inner join base_todos_ranks as ranks
on cbk.CustomerID = ranks.CustomerID

/*
select MotivoStatus, date(DataStatus), count(*) from base_chargeback_desbloqueio as cbk
inner join base_todos_ranks as ranks
on cbk.CustomerID = ranks.CustomerID
group by 1,2
*/
;