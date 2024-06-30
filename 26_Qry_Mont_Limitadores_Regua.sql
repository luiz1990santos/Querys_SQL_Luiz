-- BASE LIMITADORES NOVA REGUA

-- select max(Data_Atualizacao), min(Data_Atualizacao) FROM `eai-datalake-data-sandbox.loyalty.tblSaldoKmAtual` 

---------- =============================================== ----------
-- Saldo KM - por cliente
---------- =============================================== ----------


SELECT 
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
,CASE WHEN p.CPF = c.document THEN 'Eai' ELSE 'KM' END AS Flag_Origem
,Case 
When vip.CPF = c.document then 'VIP'
when uber.cpf = c.document then 'Uber'
else 'Urbano' end as Perfil
,max(s.Data_Atualizacao) as Data_Atualizacao

FROM `eai-datalake-data-sandbox.loyalty.tblSaldoKmAtual`        as s
JOIN `loyalty.tblParticipantes`                                 as p on s.ParticipanteID = p.ParticipanteID
LEFT JOIN (select * from`eai-datalake-data-sandbox.core.customers`  where status = 'ACTIVE')  as c on p.CPF = c.document
LEFT JOIN (select distinct CPF from `eai-datalake-data-sandbox.loyalty.tblParticipantes` where Vip is not null and Inativo = false) vip on vip.CPF = c.document
LEFT JOIN (select cpf from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias`) uber on uber.cpf = c.document

group by 1,2,3,4,5,6
order by 2 desc

;

---------- =============================================== ----------
-- Crivo KM - Avaliação de exposição CashBack
---------- =============================================== ----------


with Saldo_Alto as (

          SELECT 
              p.CPF
              ,s.saldo_km
              ,s.Data_Atualizacao
              ,case when s.saldo_km = 0 then '0_Saldo'
              when s.saldo_km > 0 AND s.saldo_km < 1250 then 'Ate_1'
              when s.saldo_km >= 1250 AND s.saldo_km < 2500 then 'Ate_2'
              when s.saldo_km >= 2500 AND s.saldo_km < 6250 then 'Ate_5'
              when s.saldo_km >= 6250 AND s.saldo_km < 18750 then 'Ate_15'
              when s.saldo_km >= 18750 AND s.saldo_km < 37500 then 'Ate_30'
              when s.saldo_km >= 37500 AND s.saldo_km < 75000 then 'Ate_60'
              when s.saldo_km >= 75000 AND s.saldo_km < 112500 then 'Ate_90'
              when s.saldo_km >= 112500 AND s.saldo_km < 187500 then 'Ate_150'
              else 'Mais_150' end as Faixa_Resgates
              ,case
                when s.saldo_km =0        then '00- 0 KM'
                when s.saldo_km <=1000    then '01- 1 ate 1000'
                when s.saldo_km <=2000    then '02- 1001 ate 2000'
                when s.saldo_km <=3000    then '03- 2001 ate 3000'
                when s.saldo_km <=4000    then '04- 3001 ate 4000'
                when s.saldo_km <=5000    then '05- 4.001 ate 5000'
                when s.saldo_km <=10000   then '06- 5.001 ate 10.000'
                when s.saldo_km <=30000   then '07- 1.0001 ate 30.000'
                when s.saldo_km <=50000   then '08- 30.001 ate 50.000'
                when s.saldo_km <=100000  then '09- 50.001 ate 100.000'
                when s.saldo_km <=500000  then '10- 100.001 ate 500.000'
                when s.saldo_km >500000   then '10- Maio 500.000'
              end as Faixa_KM

          FROM `eai-datalake-data-sandbox.loyalty.tblSaldoKmAtual` as s
          JOIN `loyalty.tblParticipantes` as p on s.ParticipanteID = p.ParticipanteID
          WHERE (1=1)
          --AND s.saldo_km >= 10000
          AND Data_Atualizacao >= '2021-01-01'
)

      SELECT
          Perfil,
          Faixa_Resgates,
          Faixa_KM,
          count(CPF) as U,
          avg(SAldoMedioKM) as K,
          avg(ValorMedio) as V,
          avg(Acumulos) as A,
          avg(Resgates) as R,
          avg(DescontoMedio) as D
          FROM (
            SELECT 
                c.document as CPF,
                Case When vip.CPF = c.document then 'VIP'
                when uber.cpf = c.document then 'Uber'
                else 'Urbano' end as Perfil,
                sa.Faixa_Resgates,
                Faixa_KM,
                Avg(sa.saldo_km) as SaldoMedioKM,
                Sum(case when b.action = 'EARNING' then 1 else 0 end) as Acumulos,
                Sum(case when b.action = 'BURNING' then 1 else 0 end) as Resgates,
                Avg(o.order_value) as ValorMedio,
                Avg(b.percentage) as DescontoMedio,
                Min(b.percentage) as MenorDesconto,
                Max(b.percentage) as MaiorDesconto
            FROM `core.orders` as o
            JOIN `core.order_benefit` as b on o.id = b.order_id
            JOIN `core.customers` as c on o.customer_id = c.id
            LEFT JOIN (select distinct CPF from `eai-datalake-data-sandbox.loyalty.tblParticipantes` where Vip is not null and Inativo = false) as vip on vip.CPF = c.document
            LEFT JOIN (select cpf from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias`) as uber on uber.cpf = c.document
            LEFT JOIN Saldo_Alto as sa on sa.CPF = c.document
            WHERE o.created_at >= '2023-02-01' AND o.created_at <= '2023-03-01'
            AND o.sales_channel = 'APP'
            AND o.order_status = 'CONFIRMED'
            AND c.status = 'ACTIVE'
            GROUP BY CPF, Perfil, Faixa_Resgates,Faixa_KM
            --HAVING Resgates > 0
)

GROUP BY Perfil, Faixa_Resgates,Faixa_KM

;
---------- =============================================== ----------
-- Analitico Historico KM - Monitoramento 
---------- =============================================== ----------

WITH

BASE AS (

SELECT 
ord.*
FROM `eai-datalake-data-sandbox.core.order_benefit`    ordbnf
join `eai-datalake-data-sandbox.core.orders`              ord on ord.id = ordbnf.order_id
join `eai-datalake-data-sandbox.core.customers`            cl on ord.customer_id =cl.id
where
cl.document = '04743599806'
and sales_channel = 'APP'
and order_status = 'CONFIRMED'
AND date(ord.created_at) >= '2023-01-01'

) SELECT * FROM BASE ORDER BY 10 DESC