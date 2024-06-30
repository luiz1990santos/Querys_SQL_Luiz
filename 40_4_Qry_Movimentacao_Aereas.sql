--==========================================================================================================
-- Base Movimentações - Aereas
--==========================================================================================================

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Movimentacao_Aereas` 

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Movimentacao_Aereas` AS 



with Base_Transacional_Aereas as (
 
 
select
distinct
cl.uuid as Customer_ID
,PayPal.Transaction_ID
,case 
    when PayPal.Transaction_Type = 'credit' then 'Estorno'
    else 'Venda'
 end as Tipo
,case
 when PayPal.Merchant_Account like '%latam%' then 'Latam'
 when PayPal.Merchant_Account like '%tudoazul%' then 'TudoAzul'
 when PayPal.Merchant_Account like '%livelo%' then 'Livelo'
 when PayPal.Merchant_Account like '%smiles%' then 'Smiles'
 else 'Verificar' end as Flag_Merchant

,PayPal.Merchant_Account
,PayPal.Risk_Decision as Dec_Motor_PayPal
,PayPal.Transaction_Status as Status_Trans_PayPal
,PayPal.Processor_Response_Text  as Status_Trans_Emissor
,PayPal.Created_Datetime
,RANK() OVER (PARTITION BY PayPal.Transaction_ID ORDER BY PayPal.Created_Datetime ,PayPal.Transaction_Status  desc) AS Rank_trans
,date (PayPal.Created_Datetime) as Dt_Tranx
,PayPal.Order_ID
,PayPal.Payment_Instrument_Type
,PayPal.Card_Type
,case
  when PayPal.Merchant_Account like '%latam%' then pt.CPF
  else cl.document end as CPF_Cliente

,case
  when PayPal.Merchant_Account like '%latam%' then
    Case 
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<18   Then '01  MenorIdade'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=20  Then '02  18a20anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=25  Then '04  21a25anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=30  Then '05  26a30anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=35  Then '06  31a35anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=40  Then '07  36a40anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=45  Then '08  41a45anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=50  Then '09  46a50anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=55  Then '10 51a55anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=60  Then '11 56a60anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=65  Then '12 61a65anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=70  Then '13 66a70anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=75  Then '14 71a75anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=80  Then '15 76a80anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)<=85  Then '16 81a85anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(pt.DatadeNascimento), year)>85   Then '17 >86anos'  
    end
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<18   Then '01  MenorIdade'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=20  Then '02  18a20anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=25  Then '04  21a25anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=30  Then '05  26a30anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=35  Then '06  31a35anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=40  Then '07  36a40anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=45  Then '08  41a45anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=50  Then '09  46a50anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=55  Then '10 51a55anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=60  Then '11 56a60anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=65  Then '12 61a65anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=70  Then '13 66a70anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=75  Then '14 71a75anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=80  Then '15 76a80anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=85  Then '16 81a85anos'
    When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)>85   Then '17 >86anos'  
    End as Faixa_Idade

,case
  when PayPal.Merchant_Account like '%latam%' then
    CASE
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",pt.DataCadastro)), DAY) <=0   THEN '01_00-Hoje'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",pt.DataCadastro)), DAY) <=1   THEN '02_01-Ontem'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",pt.DataCadastro)), DAY) <=10   THEN '03_00-10DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",pt.DataCadastro)), DAY) <=30   THEN '04_11-30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",pt.DataCadastro)), DAY) <=60   THEN '05_31-60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",pt.DataCadastro)), DAY) <=90   THEN '06_61-90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",pt.DataCadastro)), DAY) <=180  THEN '07_91-180DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",pt.DataCadastro)), DAY) <=364  THEN '08_180-1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",pt.DataCadastro)), DAY) >=365  THEN '09_+1ANO'
    END
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=0   THEN '01_00-Hoje'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=1   THEN '02_01-Ontem'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=10   THEN '03_00-10DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=30   THEN '04_11-30DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=60   THEN '05_31-60DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=90   THEN '06_61-90DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=180  THEN '07_91-180DIAS'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=364  THEN '08_180-1ANO'
    WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) >=365  THEN '09_+1ANO'
END AS Flag_TempodeConta

,Case 
When (PayPal.Amount_Authorized) <100 Then '1-0 a 99'
When (PayPal.Amount_Authorized) >=100 and (PayPal.Amount_Authorized) <=300 Then '2-101 a 300'
When (PayPal.Amount_Authorized) > 300 and (PayPal.Amount_Authorized) <=500 Then '3-301 a 500'
When (PayPal.Amount_Authorized) > 500 and (PayPal.Amount_Authorized) <=1000 Then '4-501 a 1.000'
When (PayPal.Amount_Authorized) > 1000 and (PayPal.Amount_Authorized) <=1500 Then '5-1001 a 1500'
When (PayPal.Amount_Authorized) > 1500 and (PayPal.Amount_Authorized) <=2000 Then '6-1501 a 2000'
When (PayPal.Amount_Authorized) > 2000 and (PayPal.Amount_Authorized) <=3000 Then '7-2001 a 3000'
When (PayPal.Amount_Authorized) > 3000 and (PayPal.Amount_Authorized) <=5000 Then '8-3001 a 5000'
When (PayPal.Amount_Authorized) > 5000                  Then '9-5001>'
End as Faixa_Valores

,PayPal.Customer_Email
,PayPal.Payment_Method_Token
,PayPal.Gateway_Rejection_Reason
,PayPal.Fraud_Detected
,PayPal.First_Six_of_Credit_Card
,PayPal.Issuing_Bank
,PayPal.Amount_Authorized as Vl_PayPal
,onb.Flag_Email_NaoVal 
,onb.Flag_Celular_NaoVal

 
from (Select
distinct * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2` ) as PayPal
left join`eai-datalake-data-sandbox.loyalty.tblPedidos` as pe 
on cast(pe.PedidoID as String) = cast(PayPal.Order_ID as String)
left join `eai-datalake-data-sandbox.loyalty.tblParticipantes` as pt 
on pt.ParticipanteID = pe.ParticipanteID
left join `eai-datalake-data-sandbox.core.orders` as ord       
on ord.uuid = PayPal.Order_ID
left join `eai-datalake-data-sandbox.core.customers` as cl
on cl.id = ord.customer_id
left join (
        with base_onboaring as (
          select 
              distinct
              Cpf_Cliente
              ,cl.uuid as Customer_ID
              ,case when indicators like '%Not_validated_email%' then 'EmailNaoValidado' else 'NA' end as Flag_Email_NaoVal
              ,case when indicators like '%Not_validated_phone%' then 'CelularNaoValidado' else 'NA' end as Flag_Celular_NaoVal

              ,RANK() OVER (PARTITION BY cpf ORDER BY data_cadastro desc) AS Rank_Ult_Decisao

              from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` as zaig
              left join `eai-datalake-data-sandbox.core.customers` as cl
              on zaig.Cpf_Cliente = cl.document
              ---where
              --date(data_cadastro) >= current_date - 20
              --and decisao = "automatically_approved"
              --and Cpf_Cliente = '61969036672' 
              order by 2 desc
    ) select * from base_onboaring where Rank_Ult_Decisao = 1
          ) as onb on cl.uuid = onb.Customer_ID


where
date (PayPal.Created_Datetime)  >= (current_date() - 90)
order by 1

), base_aereas as (
select
distinct
*
from Base_Transacional_Aereas
where Rank_trans = 1
), Base_chargeback as (
  
  select
  distinct
        order_id,
        customer_id,
        CPF_Cliente,
        Dt_Contestacao,
        Flag_Contestacao
        --Flag_Bloqueio  
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CBK_TPV`
)
  select 
      distinct
      aereas.Customer_ID, 
      aereas.Transaction_ID,
      aereas.Tipo, 
      aereas.Flag_Merchant, 
      aereas.Merchant_Account, 
      aereas.Dec_Motor_PayPal, 
      aereas.Status_Trans_PayPal, 
      aereas.Status_Trans_Emissor, 
      aereas.Created_Datetime, 
      aereas.Rank_trans, 
      aereas.Dt_Tranx, 
      perfil.Flag_Ativo,
      aereas.Order_ID, 
      aereas.Payment_Instrument_Type, 
      aereas.Card_Type, 
      aereas.CPF_Cliente, 
      perfil.UF_Cliente,
      perfil.Flag_ContaDigital,
      perfil.Flag_Tetativas as Flag_tentativas,
      perfil.Flag_Biometria,
      perfil.Flag_Risco_CBK,
      perfil.ScoreZaig,
      aereas.Faixa_Idade, 
      aereas.Flag_TempodeConta, 
      aereas.Faixa_Valores, 
      aereas.Customer_Email, 
      aereas.Payment_Method_Token, 
      aereas.Gateway_Rejection_Reason, 
      aereas.Fraud_Detected, 
      aereas.First_Six_of_Credit_Card, 
      aereas.Issuing_Bank, 
      aereas.Vl_PayPal, 
      aereas.Flag_Email_NaoVal, 
      aereas.Flag_Celular_NaoVal,
      cbk.order_id as order_id_CBK,
      case 
        when cbk.Flag_Contestacao is null or cbk.Flag_Contestacao = 'NaoContestado' then 'Não contestado'
        else 'Contestado'
      end as Flag_Contestacao,
      cbk.Dt_Contestacao
    /* 
     case 
        when cbk.Flag_Bloqueio is null or cbk.Flag_Bloqueio = 'NA' then 'NA'
        else cbk.Flag_Bloqueio
      end as Flag_Bloqueio
    */

  from Base_Transacional_Aereas as aereas
  left join Base_chargeback as cbk
  on aereas.Order_ID = cbk.order_id
  left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` as perfil 
  on perfil.CustomerID = aereas.Customer_ID

