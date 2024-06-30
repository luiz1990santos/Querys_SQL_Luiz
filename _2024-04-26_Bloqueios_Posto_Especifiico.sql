--`eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_Postos_Consolidado`

with
       base_cl as (
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
                     ,ev.sub_classification as Subclassificacao
                     ,ev.event_date as DataStatus
                     ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Ev

              FROM `eai-datalake-data-sandbox.core.customers`             cl
              left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
              left join (select * from `eai-datalake-data-sandbox.core.customer_event` 
                     where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT'))Ev on ev.customer_id = cl.id
              ), base_cliente_posto as (
select
  cl.CustomerID,
  cl.CPF_Cliente,
  cl.Nome,
  cl.MotivoStatus,
  cl.Subclassificacao,
  cl.DataStatus,
  store_BKO.uuid as Store_ID,  
  store_BKO.name as Posto,
  orders.created_at as DataPedido

FROM `eai-datalake-data-sandbox.payment.payment` as pay 
join `eai-datalake-data-sandbox.core.orders` as orders 
on pay.order_id = orders.uuid 
join `eai-datalake-data-sandbox.backoffice.store` as store_BKO
on store_BKO.uuid = orders.store_id 
left join base_cl as cl 
on cl.CustomerID = pay.customer_id
where Rank_Ult_Atual = 1
and StatusConta = 'BLOCKED'
and store_BKO.uuid = 'STO-dd426fd1-3090-48a7-863c-335d6b6d7200'
), saldo_clientes_posto as (
SELECT 
  distinct
  CustomerID,
  CPF_Cliente,
  Nome,
  MotivoStatus,
  -- Subclassificacao,
  -- DataStatus,
  Store_ID,  
  Posto,
  -- DataPedido,
  round(sum(T.valor),2) as SALDO                   -- SOMA DOS VALORES QUE COMPÕEM O SALDO
FROM `eai-datalake-data-sandbox.orbitall.tb_topaz` T
left join base_cliente_posto c 
on c.CPF_Cliente = T.numerodocumento
where    t.status = 'MOVIMENTAÇÃO EXECUTADA'       -- STATUS NECESSARIO DE TRANSACOES CONCLUIDAS    
and T.data <= CURRENT_DATE()-1          -- FIXAR COM A DATA COM A QUAL SE PRETENDE ANALISAR O SALDO    
and length(T.numerodocumento) <= 11     -- REMOVER CASO QUEIRA TRAZER TAMBÉM CNPJ
group by 1,2,3,4,5,6

--SALDO = 590.00 
--and NomeCliente like 'Rodrigo'
order by 2
) select * from saldo_clientes_posto
where SALDO > 0
and CustomerID is not null;
-- and date(cl.DataStatus) >= '2024-03-01'
--and date(orders.created_at) >= '2024-03-01'
-- join `eai-datalake-data-sandbox.core.orders` ord on p.order_id = ord.uuid

-- select * from `eai-datalake-data-sandbox.core.orders` limit 100
-- select * from `eai-datalake-data-sandbox.backoffice.store` limit 100

-- select * from `eai-datalake-data-sandbox.payment.payment` limit 100

with
base as (
SELECT 
T.numerodocumento as CPF,   
c.uuid as CustomerID,                
c.full_name as NomeCliente,
round(sum(T.valor),2) as SALDO                   -- SOMA DOS VALORES QUE COMPÕEM O SALDO
FROM `eai-datalake-data-sandbox.orbitall.tb_topaz` T
left join `eai-datalake-data-sandbox.core.customers` c on c.document = T.numerodocumento
where    t.status = 'MOVIMENTAÇÃO EXECUTADA'       -- STATUS NECESSARIO DE TRANSACOES CONCLUIDAS    
and T.data <= CURRENT_DATE()-1          -- FIXAR COM A DATA COM A QUAL SE PRETENDE ANALISAR O SALDO    
and length(T.numerodocumento) <= 11     -- REMOVER CASO QUEIRA TRAZER TAMBÉM CNPJ
group by 1,2,3
) select 
* 
from base 
where 
CPF in (
  '00577285858',
'01886017085',
'06191323611',
'08926674860',
'11777724856',
'12529848807',
'14401395808',
'15739807816',
'16216468820',
'16708226804',
'17688995841',
'17876669808',
'21982584807',
'26799721803',
'27072997822',
'28907614873',
'29540732840',
'29670478804',
'30489791824',
'30647242826',
'33389574832',
'33423251867',
'34863653808',
'36763309864',
'38578729854',
'39887450812',
'40013204874',
'40644716878',
'41834336813',
'41916229867',
'42105415830',
'42611200890',
'43834638862',
'44040428846',
'44434942867',
'45001168856',
'45902740860',
'46291737817',
'47146860892',
'47765642898',
'47984156859',
'49445594886',
'50197710824',
'50198558805',
'50762582839',
'51332538851',
'51497953804',
'53813143880',
'55707345892',
'57830585806'
)
--SALDO = 590.00 
--and NomeCliente like 'Rodrigo'
order by 2;