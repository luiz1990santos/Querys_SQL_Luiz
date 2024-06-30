with
base as (
    select
    distinct
        cl.uuid as  CustomerID
        ,cl.full_name as Nome
        ,cl.document as CPF_Cliente
        ,cl.status as StatusConta
        ,cl.email
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
              where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT')
    )  Ev on ev.customer_id = cl.id
    order by 1
    

)
select
Safra_Abertura
,Safra_Ev
,StatusConta
,MotivoStatus
,case 
when StatusConta = 'ACTIVE' and MotivoStatus is null then 'SemBloqueio'
when MotivoStatus in ('Fraude confirmada','Suspeita de fraude') then 'BloqueioFraude'
when MotivoStatus not in ('Fraude confirmada','Suspeita de fraude') then 'BloqueioPreventivo'
else 'Outros' end as FlagStatusConta
,StatusEvento
,Flag_Trusted
,count(distinct CustomerID) as QtdCliente

from base 
where Rank_Ult_Atual = 1 
group by 1,2,3,4,5,6,7
order by 1
                        
                        
                        --) cl on cl.CustomerID = tranx_d.customer_id	