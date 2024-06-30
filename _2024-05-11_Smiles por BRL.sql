with Base_Transacional_smiles as (
 
select
distinct
smiles.Transaction_ID
,smiles.Risk_Decision as Dec_Motor_PayPal
,smiles.Transaction_Status as Status_Trans_PayPal
,smiles.Processor_Response_Text  as Status_Trans_Emissor
,smiles.Created_Datetime
,RANK() OVER (PARTITION BY Transaction_ID ORDER BY smiles.Created_Datetime ,smiles.Transaction_Status  desc) AS Rank_trans
,date (smiles.Created_Datetime) as Dt_Tranx
,smiles.Order_ID
,smiles.Payment_Instrument_Type
,smiles.Card_Type
,smiles.CPF
,smiles.Customer_ID
,smiles.Customer_Email
,smiles.Merchant_Account
--,smiles.Payment_Method_Token
,smiles.Gateway_Rejection_Reason
,smiles.Fraud_Detected
,smiles.First_Six_of_Credit_Card
,smiles.Issuing_Bank
,smiles.Amount_Authorized/100 as Vl_PayPal
 
 
from (Select
distinct * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2`
where Merchant_Account like '%smiles%'
) smiles
 
where
date (smiles.Created_Datetime)  >= (current_date() - 60)
order by 1
)
select
Merchant_Account, Dec_Motor_PayPal,  count(*)
from Base_Transacional_smiles
where Rank_trans = 1
and Dt_Tranx = '2024-05-11'
group by 1,2
;