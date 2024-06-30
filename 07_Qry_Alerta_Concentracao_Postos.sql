--======================================================================================
--> MONITORAMENTO ALERTA CONCENTRAÇÃO POSTOS TRANSAÇÕES LIMITE  MES - 30 DIAS
--======================================================================================


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Concetracao_tran_limite` AS 

              SELECT
              distinct
              orders.store_id AS `CodigoPosto`,
              p.store_id,
              COUNT(orders.id) AS `TransacoesTotais`,
              SUM(IF(orders.order_value = 300 OR orders.order_value = 350, 1, 0)) AS `TransacoesLimite`,
              SUM(orders.order_value) AS `Montante`,
              SUM(IF(orders.order_value = 300 OR orders.order_value = 350, orders.order_value, 0)) AS `MontanteLimite`,
              SUM(IF(orders.order_value = 300 OR orders.order_value = 350, orders.order_value, 0))/SUM(orders.order_value) AS `indice_Concentracao`
              FROM `eai-datalake-data-sandbox.core.orders` orders
              join `eai-datalake-data-sandbox.payment.payment` p on orders.uuid = p.order_id
              WHERE orders.order_status = 'CONFIRMED'
              AND date(orders.created_at) >= (current_date() - 30)
              GROUP BY 1,2
              HAVING `TransacoesTotais` >= 100 AND `indice_Concentracao` > 0.75
              ORDER BY 6 desc

;
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_crivo_abusadores_cashback`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_crivo_abusadores_cashback` AS 

with 
-- Dados Faturamento total
base_tpv_Cash as (
          SELECT 
          FORMAT_DATE("%d-%m-%Y",pt.created_at) as Dt_tranx,
          sum(pt.transaction_value) as Tpv_Total

          FROM `eai-datalake-data-sandbox.payment.payment_transaction` as pt
          WHERE 
          date(pt.created_at) >= current_date - 30 AND date(pt.created_at) <= (current_date() - 1)
          AND pt.payment_method in ('CASH')
          AND pt.status in ('AUTHORIZED', 'COMPLETED')
          GROUP by 1),
-- Dados de clientes que estão com buso de cashback
base_clientes_suspeito as (
          SELECT
          cli.document as CPF,
          p.customer_id AS `Usuario`,
          post.CodigoPosto,
          p.store_id,
          SUM(pt.transaction_value) AS `Compras_Total`,
          SUM(IF(pt.transaction_value = 300 OR pt.transaction_value = 350, pt.transaction_value, 0)) AS `Compras_Limite`,
          SUM(IF(pt.transaction_value = 300 OR pt.transaction_value = 350, 1, 0)) AS `Quantidade_Limite`,
          SUM(IF(pt.transaction_value = 300 OR pt.transaction_value = 350, pt.transaction_value, 0))/SUM(pt.transaction_value) AS `Percentual_Suspeito`,
          COUNT(distinct p.store_id) AS `Lojas_Distintas`
            
          FROM `eai-datalake-data-sandbox.payment.payment_transaction` as pt
          JOIN `eai-datalake-data-sandbox.payment.payment` as p ON p.id = pt.payment_id
          JOIN (SELECT uuid, document  FROM `eai-datalake-data-sandbox.core.customers` ) cli on cli.uuid = p.customer_id
          JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Concetracao_tran_limite` post on post.store_id = p.store_id
          WHERE
          date(pt.created_at) >= (current_date() - 30)
          AND date(pt.created_at) <= (current_date() - 1)
          AND date(p.created_at) >= (current_date() - 30)
          AND date(p.created_at) <= (current_date() - 1)
          AND pt.payment_method = 'CASH'
          AND pt.status = 'COMPLETED'
          --AND p.store_id in (4935,18,4938,3090,4627,2758)
          GROUP BY 1,2,3,4
          --HAVING `Quantidade_Limite` >= 7
          --AND `Lojas_Distintas` = 1
          --AND `Percentual_Suspeito` >= 0.85
),
-- Dados Cadastrais dos clientes
Consolidado as (
select 
sta.*
,bd.CodigoPosto
,bd.store_id
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
          ,en.street as Rua
          ,en.neighborhood as Bairro
          ,en.city as Cidade
          ,en.state as UF
          ,FORMAT_DATE("%d-%m-%Y",cl.created_at)as DataCriacao
          --,cl.created_at as DataCriacao
          ,'' as Telefone
          ,cl.trusted as Trusted
          ,cl.status
          ,cl.risk_analysis_status as RiskAnalysis
          ,ev.observation as MotivoStatus
          ,ev.event_date as DataStatus
          ,ev.user_name as UsuarioStatus
          ,RANK() OVER (PARTITION BY cl.uuid ORDER BY en.id, id.phone_id  desc) AS Rank_Ult_Atual

          FROM `eai-datalake-data-sandbox.core.customers`             cl
          left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
          left join `eai-datalake-data-sandbox.core.customer_phone`   id on id.customer_id = cl.id
          left join `eai-datalake-data-sandbox.core.phone`            ph on id.phone_id = ph.id 
          left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id
          ) sta on sta.CustomerID = bd.Usuario)
select 
base.*
,tpv.Tpv_Total

from Consolidado         base
join base_tpv_Cash       tpv on tpv.Dt_tranx = base.DataCriacao
where 
status not in ('BLOCK','BLOCKED','UNBLOCK')
and Rank_Ult_Atual = 1
