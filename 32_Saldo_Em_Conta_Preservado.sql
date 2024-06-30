
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Saldo_Preservado` AS 

with

base_cliente as (

              select
              distinct
                     cl.uuid as  CustomerID
                     ,cl.full_name as Nome
                     ,cl.document as CPF_Cliente
                     ,cl.status as StatusConta
                     ,Ev.status as StatusEvento
                     ,ev.observation as MotivoStatus
                     ,ev.event_date as DataStatus
                     ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Ev
                     ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual

              FROM `eai-datalake-data-sandbox.core.customers`             cl
              left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
              left join (select * from `eai-datalake-data-sandbox.core.customer_event` 
                     where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT'))Ev on ev.customer_id = cl.id

where cl.status  = 'BLOCKED'
and ev.observation in ('Suspeita de fraude','Fraude confirmada')

),base as (
SELECT 
T.numerodocumento as  DOCUMENTO                     -- CPF / CNPJ    
--,T.data as Dt_Saldo
,cl.statusconta
,cl.MotivoStatus
,cl.DataStatus
,cl.Safra_Ev
,round(sum(T.valor),2) as SaldoConta                   -- SOMA DOS VALORES QUE COMPÕEM O SALDO
FROM `eai-datalake-data-sandbox.orbitall.tb_topaz` T
join (select * from  base_cliente where Rank_Ult_Atual = 1) cl on cl.CPF_Cliente= T.numerodocumento
where    status = 'MOVIMENTAÇÃO EXECUTADA'       -- STATUS NECESSARIO DE TRANSACOES CONCLUIDAS    
and T.data <= CURRENT_DATE()-1          -- FIXAR COM A DATA COM A QUAL SE PRETENDE ANALISAR O SALDO    
and length(T.numerodocumento) <= 11     -- REMOVER CASO QUEIRA TRAZER TAMBÉM CNPJ
--and T.numerodocumento = '07066706986'

group by 1,2,3,4,5
), base_acumuloKM as (

select
distinct
p.CPF
,c.uuid
,s.saldo_km
,case
when s.saldo_km =0        then '00- 0 KM'
when s.saldo_km <=1000    then '01- 1 ate 1000'
when s.saldo_km <=2000    then '02- 1001 ate 2000'
when s.saldo_km <=3000    then '03- 2001 ate 3000'
when s.saldo_km <=4000    then '04- 3001 ate 4000'
when s.saldo_km <=5000    then '05- 4.001 ate 5000'
when s.saldo_km <=5000    then '05- 4.001 ate 5000'
when s.saldo_km <=10000   then '06- 5.001 ate 10.000'
when s.saldo_km <=30000   then '07- 1.0001 ate 30.000'
when s.saldo_km <=50000   then '08- 30.001 ate 50.000'
when s.saldo_km <=100000  then '09- 50.001 ate 100.000'
when s.saldo_km <=500000  then '10- 100.001 ate 500.000'
when s.saldo_km >500000   then '10- Maio 500.000'
end as Faixa_KM
,CASE WHEN p.CPF = c.document THEN 'Eai' ELSE 'KM' END AS Flag_Origem


FROM `eai-datalake-data-sandbox.loyalty.tblSaldoKmAtual` s
JOIN `loyalty.tblParticipantes`                          p on s.ParticipanteID = p.ParticipanteID
join (select * from  base_cliente where Rank_Ult_Atual = 1) cl1 on p.CPF = cl1.CPF_Cliente
LEFT JOIN (select * from`eai-datalake-data-sandbox.core.customers`  )  c on p.CPF = c.document
where s.saldo_km >0
--and  p.CPF  = '07066706986'
group by 1,2,3,4,5
order by 2 desc

),base_acumuloKM2 as (
select
CPF
,saldo_km
from base_acumuloKM
order by 2 desc
)
select * 
from base b
left join base_acumuloKM2 skm on skm.CPF = b.DOCUMENTO
--where 
--statusconta = 'ACTIVE' 
order by 1,3 desc
