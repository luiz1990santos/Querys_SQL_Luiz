
-- select * from `eai-datalake-data-sandbox.gold.clientes` limit 100

-----------------------------------------------------------------------------------
-- QUERY CONSULTA UNICO PARA SUBMETER EM LOTE (Acompanhamento de Mensagens)       |
-----------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Lote_Acomp_Mens_Unico` AS
  WITH BASE_LOTE_MENS_UNICO AS (
    SELECT 
      DISTINCT
      cl.document AS CPF, 
      UPPER(REGEXP_EXTRACT(cl.first_name, r'^(\S+)')) AS Nome, 
      "M" AS Sexo,
      ph.country_code||ph.area_code||ph.number AS Telefone,  
      ev.observation AS MotivoBloqueio,
      'Bloqueio de cadastro' as Motivo,
      ph.type AS Tipo_tel,
      ev.event_date AS Dt_Evento,
      RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual
    FROM `eai-datalake-data-sandbox.core.customers`  as cl
    left join `eai-datalake-data-sandbox.core.customer_event` as ev 
    on ev.customer_id = cl.id
    left join `eai-datalake-data-sandbox.core.customer_phone` as cus_ph 
    on ev.customer_id = cus_ph.customer_id
    left join `eai-datalake-data-sandbox.core.phone` as ph 
    on cus_ph.phone_id = ph.id
  ) SELECT *  FROM BASE_LOTE_MENS_UNICO 
  WHERE MotivoBloqueio in ('Bloqueio de cadastro', 'Suspeita de fraude') 
  AND Tipo_tel = 'MOBILE'
  AND Rank_Ult_Atual = 1
  --AND CPF = '52669165087'



 -- select observation, count(*) from `eai-datalake-data-sandbox.core.customer_event` group by 1