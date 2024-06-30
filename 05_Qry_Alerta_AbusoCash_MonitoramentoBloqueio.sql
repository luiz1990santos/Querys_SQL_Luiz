--======================================================================================
--> MONITORAMENTO ALERTA ABUSO CASHBACK TRANSAÇÕES LIMITE  MES - 30 DIAS
--======================================================================================


---drop table `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_abuso_cashback`
---select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_abuso_cashback`
---select * from `eai-datalake-data-sandbox.payment.payment` where  date(created_at) >= current_date - 5

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_abuso_cashback` AS 

with 

-- Dados Faturamento total
base_tpv_Cash as (
          SELECT 
          FORMAT_DATE("%d-%m-%Y",pt.created_at) as Dt_tranx,
          DATE_DIFF(CURRENT_DATE(), DATE(pt.created_at), WEEK) AS DiasDesdeTransacao,
          sum(pt.transaction_value) as Tpv_Total,

          FROM `eai-datalake-data-sandbox.payment.payment_transaction` as pt
          WHERE 
          date(pt.created_at) >= current_date - 30 AND date(pt.created_at) <= (current_date() - 1)
          AND pt.payment_method in ('CASH')
          AND pt.status in ('AUTHORIZED', 'COMPLETED')
          GROUP by 1,2
          order by 2),
-- Dados de clientes que estão com buso de cashback
base_clientes_suspeito as (
          SELECT
          cli.document as CPF,
          p.customer_id AS `Usuario`,
          SUM(pt.transaction_value) AS `Compras_Total`,
          SUM(IF(pt.transaction_value >= 300 , pt.transaction_value, 0)) AS `Compras_Limite`,
          SUM(IF(pt.transaction_value >= 300 , 1, 0)) AS `Quantidade_Limite`,
          SUM(IF(pt.transaction_value >= 300 , pt.transaction_value, 0))/SUM(pt.transaction_value) AS `Percentual_Suspeito`,
          COUNT(distinct p.store_id) AS `Lojas_Distintas`,
          COUNT(distinct pt.id) as QtdTrans
            
          FROM `eai-datalake-data-sandbox.payment.payment_transaction` as pt
          JOIN `eai-datalake-data-sandbox.payment.payment` as p ON p.id = pt.payment_id
          JOIN (SELECT uuid, document  FROM `eai-datalake-data-sandbox.core.customers` ) cli on cli.uuid = p.customer_id
          WHERE
          date(pt.created_at) >= (current_date() - 30)
          AND date(pt.created_at) <= (current_date() - 1)
          AND date(p.created_at) >= (current_date() - 30)
          AND date(p.created_at) <= (current_date() - 1)
          AND pt.payment_method = 'CASH'
          AND pt.status = 'COMPLETED'
          --AND cli.document = '08831740814'
          --AND p.store_id in (18,4938,4627,3090)
          GROUP BY 1,2
          --HAVING `Quantidade_Limite` >= 7
          --AND `Lojas_Distintas` = 1
          --AND `Percentual_Suspeito` >= 0.85
),Consolidado as (
-- Dados Cadastrais dos clientes

select 
sta.*
,bd.QtdTrans
,bd.`Compras_Total`*0.03 as Total_CashBack
,bd.`Compras_Total`
,bd.`Compras_Limite`
,bd.`Quantidade_Limite`
,bd.`Percentual_Suspeito`
,bd.`Lojas_Distintas`
from base_clientes_suspeito bd
left join (SELECT 
           cl.uuid as  CustomerID
          ,cl.full_name as Nome
          ,cl.document as CPF
          ,en.street||en.neighborhood||en.city||en.state as End_Completo
          ,FORMAT_DATE("%d-%m-%Y",cl.created_at)as DataCriacao
          ,'' as Telefone
          ,cl.trusted as Trusted
          ,cl.status
          ,cl.risk_analysis_status as RiskAnalysis
          ,ev.observation as MotivoStatus
          ,ev.event_date as DataStatus
          ,ev.user_name as UsuarioStatus
          ,RANK() OVER (PARTITION BY cl.uuid ORDER BY en.id, id.phone_id  desc) AS Rank_Ult_Atual
          ,case 
          when vip.CPF = cl.document then 'VIP' 
          when ub.cpf = cl.document then 'Uber'
          else 'Urbano' end as FlagPerfilCliente
          ,CASE
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
          END AS Flag_TempodeConta

          FROM `eai-datalake-data-sandbox.core.customers`             cl
          left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
          left join `eai-datalake-data-sandbox.core.customer_phone`   id on id.customer_id = cl.id
          left join `eai-datalake-data-sandbox.core.phone`            ph on id.phone_id = ph.id 
          left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id
          left join (select 
                  distinct
                  CPF 
                from `eai-datalake-data-sandbox.loyalty.tblParticipantes` where 	
                Vip is not null and Inativo = false) vip on vip.CPF = cl.document
          left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias` ub on ub.cpf = cl.document
          
          --where 
            --and cl.uuid =  'CUS-fb4d680f-dfc0-4721-b869-256ef192abc6'
          ) sta on sta.CustomerID = bd.Usuario)
select 
base.*
,tpv.Tpv_Total
,DiasDesdeTransacao
from Consolidado         base
join base_tpv_Cash       tpv on tpv.Dt_tranx = base.DataCriacao
where 
status not in ('BLOCK','BLOCKED','UNBLOCK')
and Rank_Ult_Atual = 1
;

--======================================================================================
--> MONITORAMENTO BLOQUEIOS REALIZADOS NOS ULTIMOS 180 DIAS
--======================================================================================
--select * from `eai-datalake-data-sandbox.core.customer_event` where customer_id = 5165542
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_clientes_com_Bloqueio30dias` 

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_clientes_com_Bloqueio30dias` AS 

with 
-- Clientes com bloqueio nos ultimos 30 dias
Consolidado as (
          SELECT 
          distinct
           cl.id
          ,ev.id as idEvento
          ,cl.uuid as  CustomerID
          ,cl.full_name as Nome
          ,cl.document as CPF
          ,en.street as Rua
          ,en.neighborhood as Bairro
          ,en.city as Cidade
          ,en.state as UF
          ,cl.created_at as DataCriacao
          ,'' as Telefone
          ,cl.trusted as Trusted
          ,cl.status
          ,Ev.status as Status_Conta
          ,ev.observation as MotivoStatus
          ,ev.event_date as DataStatus
          ,ev.user_name as UsuarioStatus
          ,RANK() OVER (PARTITION BY cl.uuid ORDER BY  ev.id desc) AS Rank_Ult_Atual


          FROM `eai-datalake-data-sandbox.core.customers`             cl
          left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
          left join `eai-datalake-data-sandbox.core.customer_phone`   id on id.customer_id = cl.id
          left join `eai-datalake-data-sandbox.core.phone`            ph on id.phone_id = ph.id
          left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id
          where 
          date(cl.created_at) >= current_date - 180
          and ev.observation in ('Fraude confirmada','Suspeita de fraude','Bloqueio de cadastro','')
          --and cl.uuid = 'CUS-f9a920c6-e289-4948-91bc-91411b216851'
        
)

select 
a.*
,Sum(Rank_Ult_Atual) as Qtd_Bloq
from Consolidado a
where 
status in ('BLOCK','BLOCKED','UNBLOCK')
--and Rank_Ult_Atual = 1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18

;


