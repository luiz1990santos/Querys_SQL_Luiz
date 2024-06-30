-- select * from `eai-datalake-data-sandbox.business_monitoring.Tb_Alerta_PixOut_DiarioAcimaDoLimite`

DECLARE data_atual DATE;
DECLARE inicio_mes DATE;
DECLARE fim_mes DATE;

-- Obtém a data atual
SET data_atual = CURRENT_DATE();

-- Verifica o mês atual e define o início e fim do mês anterior
IF EXTRACT(MONTH FROM data_atual) = 1 THEN
    -- Caso seja janeiro, pegamos dezembro do ano anterior
    SET inicio_mes = DATE_SUB(DATE_TRUNC(DATE_SUB(data_atual, INTERVAL 1 YEAR), YEAR), INTERVAL 11 MONTH);
    SET fim_mes = DATE_SUB(DATE_TRUNC(data_atual, YEAR), INTERVAL 1 DAY);
ELSE
    -- Para os demais meses
    SET inicio_mes = DATE_TRUNC(DATE_SUB(data_atual, INTERVAL 1 MONTH), MONTH);
    SET fim_mes = LAST_DAY(DATE_SUB(data_atual, INTERVAL 1 MONTH));
END IF;

--create or replace table `eai-datalake-data-sandbox.business_monitoring.Tb_Alerta_PixOut_DiarioAcimaDoLimite` as
with

base_Cash_In_Out  as (

SELECT
cash_transaction.created_at as created_at,
cash_transaction.flow,
CASE 
  WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN 'CASH-IN' 
  WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN 'CASH-OUT' 
END AS type,

--------------ORIGEM---------------------------------------------------------------------------------	

CASE WHEN flow = 'PIX' THEN
CASE 
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') and cl_pix_in_payer.uuid = pix_in_payer.document THEN cl_pix_in_payer.document
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN pix_in_payer.document
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') and cl_pix_payer.uuid = pix_payer.payer_id THEN cl_pix_payer.document 
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payer.payer_id
END
WHEN flow = 'TED' THEN
CASE 
WHEN ted.type in ('CASH_IN') and cl_ted_in_payer.uuid = ted_in_payer.document THEN cl_ted_in_payer.document
WHEN ted.type in ('CASH_IN') THEN ted_in_payer.document
WHEN ted.type in ('CASH_OUT') and cl_ted_payer.uuid = ted_payer.payer_id THEN cl_ted_payer.document 
WHEN ted.type in ('CASH_OUT') THEN ted_payer.payer_id
END

ELSE 'VERIFICAR'  END AS CPF_CNPJ_Origem,

--------------ORIGEM - NOME ---------------------------------------------------------------------------------	

CASE WHEN flow = 'PIX' THEN
CASE 
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') and cl_pix_in_payer.uuid = pix_in_payer.document THEN cl_pix_in_payer.full_name
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN pix_in_payer.name
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') and cl_pix_payer.uuid = pix_payer.payer_id THEN cl_pix_payer.full_name
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payer.name
END
WHEN flow = 'TED' THEN
CASE 
WHEN ted.type in ('CASH_IN') and cl_ted_in_payer.uuid = ted_in_payer.document THEN cl_ted_in_payer.full_name
WHEN ted.type in ('CASH_IN') THEN ted_in_payer.name
WHEN ted.type in ('CASH_OUT') and cl_ted_payer.uuid = ted_payer.payer_id THEN cl_ted_payer.full_name
WHEN ted.type in ('CASH_OUT') THEN ted_payer.name
END
END AS Origem_Nome,

CASE WHEN flow = 'PIX' THEN
CASE 
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN pix_in_payer.bank_name 
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN 'ABASTECE-AI'
END
WHEN flow = 'TED' THEN
CASE WHEN ted.type in ('CASH_IN', 'CASH_IN_REFUND') THEN ted_in_payer.bank_name 
WHEN ted.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN 'ABASTECE-AI'
END
ELSE 'VERIFICAR' END AS Origem_Banco,

--------------DESTINO - NOME ---------------------------------------------------------------------------------	

CASE WHEN flow = 'PIX' THEN
CASE 
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') and cl_pix_in_payee.uuid = pix_in_payee.payee_id THEN cl_pix_in_payee.document 
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payee.document
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') and cl_pix_in_payee.uuid = pix_in_payee.payee_id THEN cl_pix_in_payee.document 
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN pix_in_payee.payee_id 
END
WHEN flow = 'TED' THEN
CASE 
WHEN ted.type in ('CASH_OUT', 'CASH_OUT_REFUND') and cl_ted_in_payee.uuid = ted_in_payee.payee_id THEN cl_ted_in_payee.document 
WHEN ted.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN ted_payee.document
WHEN ted.type in ('CASH_IN', 'CASH_IN_REFUND') and cl_ted_in_payee.uuid = ted_in_payee.payee_id THEN cl_ted_in_payee.document 
WHEN ted.type in ('CASH_IN', 'CASH_IN_REFUND') THEN ted_in_payee.payee_id 
END
END AS CPF_CNPJ_Destino,

CASE WHEN flow = 'PIX' THEN
CASE 
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payee.name
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN cl_pix_in_payee.full_name 
END
WHEN flow = 'TED' THEN
CASE 
WHEN ted.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN ted_payee.name
WHEN ted.type in ('CASH_IN', 'CASH_IN_REFUND') THEN cl_ted_in_payee.full_name 
END
END AS Destino_Nome,

CASE WHEN flow = 'PIX' THEN
CASE 
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payee.bank_name
WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN 'ABASETECE-AI'
END
WHEN flow = 'TED' THEN
CASE 
WHEN ted.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN ted_payee.bank_name
WHEN ted.type in ('CASH_IN', 'CASH_IN_REFUND') THEN 'ABASETECE-AI'
END
END AS Destino_Banco,

-----------OPERÇÃO----------------------------------------------------------------------------------------------

    CASE 
        WHEN cash_transaction.flow = 'PIX' AND pix.type IN ('CASH_IN', 'CASH_IN_REFUND') THEN 'PIX-IN'
        WHEN cash_transaction.flow = 'PIX' AND pix.type IN ('CASH_OUT', 'CASH_OUT_REFUND') THEN 'PIX-OUT'
        WHEN cash_transaction.flow = 'TED' AND ted.type IN ('CASH_IN') THEN 'TED-IN'
        WHEN cash_transaction.flow = 'TED' AND ted.type IN ('CASH_OUT') THEN 'TED-OUT'
        ELSE 'JUDICIAL_DEBT'
    END AS OPERACAO,
    pix.end_to_end_id as codigo_transacao,
    COUNT(DISTINCT cash_transaction.id) as qtdtransacoes,
    SUM(cash_transaction.amount)/100 as valor,
    

    --ROUND(SUM(cash_transaction.amount)/100,2) as valor
FROM  `eai-datalake-data-sandbox.cashback.cash_transaction` as cash_transaction

LEFT join `eai-datalake-data-sandbox.cashback.pix`  pix on cash_transaction.id = pix.cash_transaction_id 
LEFT join `eai-datalake-data-sandbox.cashback.pix_payer` pix_payer on pix_payer.pix_id = pix.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_in_payee` pix_in_payee on pix_in_payee.pix_id = pix.id 
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_payee` pix_payee   ON pix.id = pix_payee.pix_id --- RECEBEDOR
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_in_payer` pix_in_payer ON pix_in_payer.pix_id = pix.id --- EMISSOR

LEFT JOIN `eai-datalake-data-sandbox.cashback.ted` ted on cash_transaction.id = ted.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_in_payee` ted_in_payee on ted_in_payee.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_in_payer` ted_in_payer on ted_in_payer.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_payee`  ted_payee on ted_payee.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_payer` ted_payer on ted_payer.ted_id = ted.id

LEFT JOIN `eai-datalake-data-sandbox.core.customers`    cl_pix_in_payer on cl_pix_in_payer.uuid = pix_in_payer.document
LEFT JOIN `eai-datalake-data-sandbox.core.customers`    cl_pix_payer on cl_pix_payer.uuid = pix_payer.payer_id
LEFT JOIN `eai-datalake-data-sandbox.core.customers`    cl_pix_in_payee on cl_pix_in_payee.uuid = pix_in_payee.payee_id

LEFT JOIN `eai-datalake-data-sandbox.core.customers` cl_ted_in_payee on cl_ted_in_payee.uuid = ted_in_payee.payee_id
LEFT JOIN `eai-datalake-data-sandbox.core.customers` cl_ted_payer on cl_ted_payer.uuid = ted_payer.payer_id
LEFT JOIN `eai-datalake-data-sandbox.core.customers` cl_ted_in_payer on cl_ted_in_payer.uuid = ted_in_payee.payee_id



WHERE 
date(pix.created_at) between inicio_mes and fim_mes 
-- and cash_transaction.flow = 'QRCODE_PIX_OUT'
AND (pix.status = 'APPROVED')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
ORDER BY 1


), base_Consolidada as (
select 
date(a.created_at) as Dt_Transacao
,a.created_at as Dt_TransacaoHora
,cl_CPF_CNPJ_Origem.status as StatusConta
,flow
,type
,cl_CPF_CNPJ_Origem.created_at as Dt_Abert_Origem
,CPF_CNPJ_Origem
,Origem_Nome
,Origem_Banco
,CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Origem.created_at), DAY) <=1 THEN '00_<1DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Origem.created_at), DAY) <=5 THEN '01_<5DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Origem.created_at), DAY) <=30 THEN '02_<30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Origem.created_at), DAY) <=60 THEN '03_<60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Origem.created_at), DAY) <=90 THEN '04_<90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Origem.created_at), DAY) <=120 THEN '05_<120DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Origem.created_at), DAY) <=160 THEN '06_<160DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Origem.created_at), DAY) <=190 THEN '07_<190DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Origem.created_at), DAY) <=220 THEN '08_<220DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Origem.created_at), DAY) <=260 THEN '09_<260DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Origem.created_at), DAY) <=290 THEN '10_<290DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Origem.created_at), DAY) <=365 THEN '11_1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Origem.created_at), DAY) >=365 THEN '12_+1ANO'
    else '13_NaoTemConta'
END AS Temp_Conta_Origem
,cl_CPF_CNPJ_Destino.created_at as Dt_Abert_Destino
,CPF_CNPJ_Destino
,Destino_Nome
,Destino_Banco
,CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Destino.created_at), DAY) <=1 THEN '00_<1DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Destino.created_at), DAY) <=5 THEN '01_<5DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Destino.created_at), DAY) <=30 THEN '02_<30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Destino.created_at), DAY) <=60 THEN '03_<60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Destino.created_at), DAY) <=90 THEN '04_<90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Destino.created_at), DAY) <=120 THEN '05_<120DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Destino.created_at), DAY) <=160 THEN '06_<160DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Destino.created_at), DAY) <=190 THEN '07_<190DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Destino.created_at), DAY) <=220 THEN '08_<220DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Destino.created_at), DAY) <=260 THEN '09_<260DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Destino.created_at), DAY) <=290 THEN '10_<290DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Destino.created_at), DAY) <=365 THEN '11_1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl_CPF_CNPJ_Destino.created_at), DAY) >=365 THEN '12_+1ANO'
    else '13_NaoTemConta'
END AS Temp_Conta_Destino
,case
when CPF_CNPJ_Origem = CPF_CNPJ_Destino then 'MesmaTitularidade'
else 'OutraTitularidade' end as Flag_Titularidade
,Case 
When valor <100 Then '1-0 a 99'
When valor >=100 and valor <=300 Then '2-101 a 300'
When valor > 300 and valor <=500 Then '3-301 a 500'
When valor > 500 and valor <=1000 Then '4-501 a 1.000'
When valor > 1000 and valor <=1500 Then '5-1001 a 1500'
When valor > 1500 and valor <=2000 Then '6-1501 a 2000'
When valor > 2000 and valor <=3000 Then '7-2001 a 3000'
When valor > 3000 and valor <=5000 Then '8-3001 a 5000'
When valor > 5000                  Then '9-5001>'
End as Intervalo_Valor
,case 
when EXTRACT(HOUR FROM a.created_at) in (0,1,2,3,4,5,6) then '01 Madrugada'
when EXTRACT(HOUR FROM a.created_at) in (7,8,9,10,11,12) then '02 Manhã'
when EXTRACT(HOUR FROM a.created_at) in (13,14,15,16,17,18) then '03 Tarde'
when EXTRACT(HOUR FROM a.created_at) in (19,20,21,22,23) then '04 Noite'
else 'NA' end as Periodo_Tranx
,OPERACAO
,qtdtransacoes
,valor
,codigo_transacao
from base_Cash_In_Out a
left join `eai-datalake-data-sandbox.core.customers`  cl_CPF_CNPJ_Origem on cl_CPF_CNPJ_Origem.document = a.CPF_CNPJ_Origem
left join `eai-datalake-data-sandbox.core.customers`  cl_CPF_CNPJ_Destino on cl_CPF_CNPJ_Destino.document = a.CPF_CNPJ_Destino

), base_valor_limite as (
select
a.Dt_TransacaoHora,
case   
    when a.StatusConta is null then 'Não é Cliente KMV'
    else a.StatusConta 
end as StatusConta 
--,a.flow
--,a.type
,a.CPF_CNPJ_Origem
,a.Origem_Nome
,date(a.Dt_Abert_Origem) as Dt_Abert_Origem
,a.Temp_Conta_Origem
,a.Origem_Banco
--,a.CPF_CNPJ_Destino
--,a.Destino_Nome
--,date(a.Dt_Abert_Destino) as Dt_Abert_Destino
--,a.Temp_Conta_Destino
--,a.Destino_Banco
--,a.Flag_Titularidade
--,a.Intervalo_Valor,
--,a.Periodo_Tranx
,a.OPERACAO
--,a.qtdtransacoes
--,a.codigo_transacao
,sum(qtdtransacoes) as qtdtransacoes
,sum(a.valor) as Valor  -- CONFERIR A SOMA!!!!!!
from base_Consolidada a 
where codigo_transacao NOT LIKE 'D%'
and a.StatusConta = 'ACTIVE'
group by 
    a.Dt_TransacaoHora,
    a.StatusConta,
    a.type,
    a.CPF_CNPJ_Origem,
    a.Origem_Nome,
    a.Dt_Abert_Origem,
    a.Temp_Conta_Origem,
    a.Origem_Banco,
    --a.CPF_CNPJ_Destino,
    --a.Destino_Nome,
    --a.Dt_Abert_Destino,
    --a.Temp_Conta_Destino,
    --*a.Destino_Banco,
    --a.Flag_Titularidade,
    --,a.Intervalo_Valor
    --a.Periodo_Tranx,
    a.OPERACAO
    --a.qtdtransacoes
    --,a.codigo_transacao
   HAVING sum(a.valor) > 5000
  
), baseConsolidada_Chamado1 as (
        SELECT 
              distinct
              op.NR_OCORRENCIA
              ,op.CPF
              ,op.MOTIVO as MOTIVO_CHAMADO
              ,op.TMR_DIA
              ,case 
              when op.TMR_DIA < 1 then '1_até1dia'
              when op.TMR_DIA < 2 then '2_até2dia'
              when op.TMR_DIA < 3 then '3_até3dia'
              when op.TMR_DIA < 4 then '4_até4dia'
              when op.TMR_DIA < 5 then '5_até5dia'
              when op.TMR_DIA > 6 then '6_meior5dia'
              else 'EmAberto' end as Flag_TMR
              ,op.DESCRICAO
              ,RANK() OVER (PARTITION BY op.CPF ORDER BY op.NR_OCORRENCIA desc ) AS Rank_Ult_Cham
            
            FROM `eai-datalake-data-sandbox.siebel.chamados` AS op

            ), baseConsolidada_Chamado2 as (
                select 
                distinct
                * 
                from baseConsolidada_Chamado1 where Rank_Ult_Cham = 1
            ),  base_Bio as (
              select 
              distinct 
                CPF_Cliente,
                Flag_Biometria
              from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`
            ), base_Inspetoria as (
              select *, LPAD(CAST(CPF AS STRING), 11, '0') AS cpf_completo  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Base_Inspetoria_2`
            

) select 
    distinct
      valor_limite.Dt_TransacaoHora,
      valor_limite.StatusConta,
      valor_limite.Dt_Abert_Origem,
      valor_limite.Temp_Conta_Origem,
      valor_limite.CPF_CNPJ_Origem,
      valor_limite.Origem_Nome,
      valor_limite.Origem_Banco,
      --valor_limite.Dt_Abert_Destino,
      --valor_limite.Temp_Conta_Destino,
      --valor_limite.CPF_CNPJ_Destino,
      --valor_limite.Destino_Nome,
      --valor_limite.Destino_Banco,
      --valor_limite.Flag_Titularidade,
      valor_limite.OPERACAO,
      --valor_limite.codigo_transacao,
      valor_limite.Valor,
      valor_limite.qtdtransacoes,
      /*
      case 
        when valor_limite.CPF_CNPJ_Origem = ins.cpf_completo then 'Sim'
        else 'Não'
      end as Flag_Inspetoria_Origem,
      case 
        when bc.CPF_CNPJ_Destino = ins.cpf_completo then 'Sim'
        else 'Não'
      end as Flag_Inspetoria_Destino,
      */
      case when upper(cha.DESCRICAO) like upper('%limi%') then 'Chamada relacionada a limitador' else 'NC' end as Flag_Chamada, 
      bio.Flag_Biometria as Biometria_Origem 
      --bio2.Flag_Biometria as Biometria_Destino
      -- bio2.Flag_Biometria AS Flag_Biometria_Destino
from base_valor_limite AS valor_limite
left join baseConsolidada_Chamado2 AS cha 
ON valor_limite.CPF_CNPJ_Origem = cha.CPF
left join base_Bio AS bio 
ON valor_limite.CPF_CNPJ_Origem = bio.CPF_Cliente
/*
left join base_Bio AS bio2 
ON bc.CPF_CNPJ_Destino = bio2.CPF_Cliente
left join base_Inspetoria as ins
ON valor_limite.CPF_CNPJ_Origem = ins.cpf_completo
left join base_Inspetoria as ins2
ON bc.CPF_CNPJ_Destino = ins2.cpf_completo
*/
--left join base_qtd_transacoes as bqt
--on valor_limite.CPF_CNPJ_Origem = bqt.CPF_CNPJ_Origem
WHERE valor_limite.OPERACAO = 'PIX-OUT'
and valor_limite.CPF_CNPJ_Origem = '98030442653'
