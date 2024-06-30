-- create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_MFA_AllowMe_Details_2` as
  with MFA as(
    SELECT 
      string_field_1 as Token,
      string_field_2 as Details
    FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_MFA_AllowMe_Details` 
  )
  SELECT
    Token,
    JSON_EXTRACT_SCALAR(json, '$.log') AS log,
    JSON_EXTRACT_SCALAR(json, '$.created_at') AS created_at
  FROM
    MFA,
    UNNEST(JSON_EXTRACT_ARRAY(Details, '$')) AS json;





  WITH VolumePorSafra AS (
    SELECT
        allowme.Safra_EnvioToken,
        COUNT(DISTINCT allowme.token) AS VolumeGeral
    FROM
        `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_AllowMe_MFA` AS allowme
    GROUP BY
       1
)select * from VolumePorSafra
;


with 
RecorrenciaPorCPF AS (
    SELECT
        allowme.username AS CPF,
        COUNT(DISTINCT allowme.token) AS QuantidadeTokens
    FROM
        `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_AllowMe_MFA` AS allowme
    GROUP BY 1
    order by 2 desc

) select * from RecorrenciaPorCPF where QuantidadeTokens > 1
;