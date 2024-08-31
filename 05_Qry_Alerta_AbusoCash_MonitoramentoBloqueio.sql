
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_abuso_cashback_semanal` as
    
WITH Tranx300porDia AS (
    SELECT
        p.customer_id,
        DATE(pt.created_at) AS data_transacao,
        COUNT(*) AS Qtd_tranx300dia
    FROM `eai-datalake-data-sandbox.payment.payment_transaction` AS pt
    JOIN `eai-datalake-data-sandbox.payment.payment` AS p ON p.id = pt.payment_id
    WHERE pt.transaction_value = 300
        AND pt.payment_method = 'CASH'
        AND pt.status IN ('AUTHORIZED', 'COMPLETED')
        AND DATE(pt.created_at) >= current_date() - 7
    GROUP BY p.customer_id, DATE(pt.created_at)
), base_abusadores_limiteSemanal as (

SELECT
    cli.document AS CPF,
    cli.uuid AS CustomerID, 
    SUM(pt.transaction_value) AS DinheiroSemana,
    SUM(IF(pt.transaction_value = 300, 300, NULL)) AS Tranx300, 
    COUNT(IF(pt.transaction_value = 300, 1, NULL)) AS Qtd_Tranx300, 
    COUNT(pt.transaction_value) AS Qtd_TranxDinheiro,
    COUNT(DISTINCT p.store_id) AS LojasDistintas,
    SUM(CASE WHEN Tranx300porDia.Qtd_tranx300dia > 1 THEN 1 ELSE 0 END) AS DiasComMaisDeUmaTranx300
FROM `eai-datalake-data-sandbox.payment.payment_transaction` AS pt
JOIN `eai-datalake-data-sandbox.payment.payment` AS p ON p.id = pt.payment_id
JOIN (SELECT uuid, document FROM `eai-datalake-data-sandbox.core.customers`) cli ON cli.uuid = p.customer_id
LEFT JOIN Tranx300porDia ON p.customer_id = Tranx300porDia.customer_id AND DATE(pt.created_at) = Tranx300porDia.data_transacao
WHERE DATE(pt.created_at) >= current_date() - 7
AND pt.payment_method = 'CASH'
AND pt.status IN ('AUTHORIZED', 'COMPLETED')
GROUP BY 1, 2
), base_abusadores_limiteSemanal2 as ( 
       SELECT 
            CPF,
            CustomerID,
            DinheiroSemana,
            Qtd_TranxDinheiro,
            Tranx300,
            Qtd_Tranx300,
            DiasComMaisDeUmaTranx300,
            LojasDistintas
        FROM base_abusadores_limiteSemanal
        WHERE DinheiroSemana >= 750
        and Qtd_Tranx300 >= 3
    ) ,IntervaloDias as (

    SELECT 
        *,
        SAFE_DIVIDE(Qtd_Tranx300, Qtd_TranxDinheiro) * 100 AS percentualTransacoes300
    FROM base_abusadores_limiteSemanal2
    ), base_final as (
    SELECT 
        distinct
        CPF,
        CustomerID,
        COALESCE(DinheiroSemana, 0) AS DinheiroSemana,
        DinheiroSemana * 0.03 as Total_CashBack,
        Qtd_TranxDinheiro,
        Tranx300,
        Qtd_Tranx300,
        DiasComMaisDeUmaTranx300,
        percentualTransacoes300,
        LojasDistintas
    FROM IntervaloDias 
    -- order by b.DinheiroSemana desc
    ), base_final2 as ( select 
        distinct
        f.*, 
        p.StatusConta,
        p.Flag_Biometria,
        p.UsuarioStatus,
        p.Flag_TempoBloqueado,
        p.Flag_TempodeConta,
        p.Perfil_Consumidor,
        p.UF_Cliente||'_'||p.DDD as UF_DDD,
        p.Historico_Bloqueio,
        p.Dt_HistoricoBloq
    from base_final as f
    left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`as p
    on f.CPF = p.CPF_Cliente 
    --where CPF = '04225825574' 
    ), historico_abuso as (
        SELECT uuid, document, * FROM `eai-datalake-data-sandbox.core.customers` as cus
        left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2` as lot
        on cus.document = lot.cpf_completo
        where Motivo like '%Abus%' or Motivo like '%abus%'

    ) select 
            a.*, 
            case 
                when a.CPF = b.cpf_completo then 'Com historico de bloqueio por abuso'
                else 'Sem histórico de bloqueio por abuso'
            end flag_bloqueio,
            case 
                when Perfil_Consumidor = 'Caminhoneiro' and DiasComMaisDeUmaTranx300 >= 2 then 'Perfil caminhoneiro +1 no limite'
                else 'NC'
            end as flag_LimiteCaminhoneiro
            
     from base_final2 as a
     left join historico_abuso as b
     on a.CPF = b.cpf_completo

;




-- select flag_LimiteCaminhoneiro, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_abuso_cashback_semanal` group by 1
create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_abuso_cashback_media_bloqueios` as
WITH BloqueiosPorSemana AS (
    SELECT 
        DATE_TRUNC(Lote, WEEK(SUNDAY)) AS semana_inicio,
        COUNT(*) AS volume,
        COUNT(DISTINCT DATE(Lote)) AS dias_com_bloqueio
    FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2` 
    WHERE Motivo LIKE 'Abus%' 
      --AND Lote >= '2024-07-23'
    GROUP BY semana_inicio
),
SemanasFormatadas AS (
    SELECT 
        FORMAT_DATE('%d/%m', semana_inicio) AS inicio_semana,
        FORMAT_DATE('%d/%m', semana_inicio + INTERVAL 6 DAY) AS fim_semana,
        LPAD(CAST(EXTRACT(WEEK FROM semana_inicio) AS STRING), 2, '0') AS numero_semana,
        EXTRACT(YEAR FROM semana_inicio) AS ano,
        volume,
        dias_com_bloqueio,
        volume / 7.0 AS media_bloqueios_por_dia
    FROM BloqueiosPorSemana
)

SELECT 
    CONCAT(numero_semana, ' - ', ano) AS semana,
    volume,
    media_bloqueios_por_dia,
    (SELECT AVG(volume) FROM SemanasFormatadas) AS media_bloqueios_por_semana,
    (SELECT AVG(dias_com_bloqueio) FROM SemanasFormatadas) AS media_dias_com_bloqueio
FROM SemanasFormatadas
ORDER BY numero_semana;
