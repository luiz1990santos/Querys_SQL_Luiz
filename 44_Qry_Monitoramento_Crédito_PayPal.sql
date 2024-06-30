create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Credito_PayPal` as
  SELECT 
  case 
    when Merchant_Account_ID like '%drywash%' then 'Drywash'
    when Merchant_Account_ID like '%fangolden%' then 'Futebol'
    when Merchant_Account_ID like '%ipiranga%' then 'Ipiranga'
    when Merchant_Account_ID like '%latampass%' then 'Latam'
    when Merchant_Account_ID like '%livelo%' then 'Livelo'
    when Merchant_Account_ID like '%recarga%' then 'Recarga'
    when Merchant_Account_ID like '%tudoazul%' then 'Tudo Azul'
    when Merchant_Account_ID like '%ubereai%' then 'Uber'
    when Merchant_Account_ID like '%ultragaz%' then 'Ultragaz'
    when Merchant_Account_ID like '%smiles%' then 'Smiles'
    else Merchant_Account_ID
  end as Flag_Merchant
  ,date(Settlement_Date) as Settlement_Date
  ,FORMAT_DATE('%Y-%m', DATE(Settlement_Date)) AS MesSafra
  ,case 
    when Transaction_Type = 'sale' then 'Venda'
    when Transaction_Type = 'credit' then 'Crédito'
    else Transaction_Type
  end as Flag_Tipo_Transacao
  ,case
    when Record_Type = 'installment' then 'Parcelamento'
    when Record_Type = 'refund' then 'Reembolso'
    when Record_Type = 'sale' then 'Venda'
    else Record_Type
  end as Flag_Tipo_Registro
  ,case 
    when Record_Subtype = 'adjustment' then 'Ajuste'
    when Record_Subtype = 'disbursement' then 'Desembolso'
    when Record_Subtype = 'non_referenced' then 'Não referenciado'
    when Record_Subtype = 'referenced' then 'Referenciado'
    else Record_Subtype
  end as Flag_Subtipo_Registro
  ,Issuing_Bank as Banco_Emissor
  ,abs(sum(Total_Fee_Amount)) as Total_Fee_Amount
  ,abs(sum(Braintree_Total_Amount)) as Braintree_Total_Amount
  FROM `eai-datalake-data-sandbox.paypal.transaction_level_fee_report` 
  where date(Settlement_Date) >= '2023-05-10' and Record_Type = 'refund'
  group by 1,2,3,4,5,6,7



  -- select max(Settlement_Date) from `eai-datalake-data-sandbox.paypal.transaction_level_fee_report` 