create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Rock_In_Rio` as
select
distinct
o.created_at as Dt_Transacao
,date(o.created_at) as Dt_Tranx
,o.uuid
,o.order_status
,pt.status
,o.order_value
,o.sales_channel
,pt.payment_method
,o.code as codigo
,Case
When (o.order_value)  <0   	Then '01 000 a 000 ' 
When (o.order_value) >=0   and (o.order_value) <=5 	Then '02 000 a 005 ' 
When (o.order_value) >5    and (o.order_value) <=10 	Then '03 006 a 010 '
When (o.order_value) >10   and (o.order_value) <=20 	Then '04 011 a 020 '
When (o.order_value) > 20  and (o.order_value) <=40 	Then '05 021 a 040 '
When (o.order_value) > 40  and (o.order_value) <=60 	Then '06 041 a 060 '
When (o.order_value) > 60  and (o.order_value) <=80 	Then '07 061 a 080 '
When (o.order_value) > 80  and (o.order_value) <=100 Then '08 081 a 100'
When (o.order_value) > 100 and (o.order_value) <=120 Then '09 101 a 120'
When (o.order_value) > 120 and (o.order_value) <=140 Then '10 121 a 140'
When (o.order_value) > 140 and (o.order_value) <=160 Then '11 141 a 160'
When (o.order_value) > 160 and (o.order_value) <=180 Then '12 161 a 180'
When (o.order_value) > 180 and (o.order_value) <=200 Then '13 181 a 200'
When (o.order_value) > 200 and (o.order_value) <=220 Then '14 201 a 220'
When (o.order_value) > 220 and (o.order_value) <=240 Then '15 221 a 240'
When (o.order_value) > 240 and (o.order_value) <=260 Then '16 241 a 260'
When (o.order_value) > 260 and (o.order_value) <=280 Then '17 261 a 280'
When (o.order_value) > 280 and (o.order_value) <300 	Then '18 281 a 299'
When (o.order_value) = 300 	Then '19 300'
When (o.order_value) > 300 	and (o.order_value) <600  Then '20 301 a 599'
When (o.order_value) = 600	Then '21 600'
When (o.order_value) > 600 	 and (o.order_value) <=800	 Then '21 601 a 800'
When (o.order_value) > 800 	 and (o.order_value) <=1000 Then '22 801 a 1000'
When (o.order_value) > 1000  and (o.order_value) <=3000 Then '23 1001 a 3000'
When (o.order_value) > 3000  and (o.order_value) <=5000 Then '24 3001 a 5000'
When (o.order_value) > 5000  and (o.order_value) <=7000 Then '25 5001 a 7000'
When (o.order_value) > 7000  and (o.order_value) <=9000 Then '26 7001 a 9000'
When (o.order_value) > 9000  and (o.order_value) <=11000 Then '27 9001 a 11000'
When (o.order_value) > 11000 and (o.order_value) <=13000 Then '28 11001 a 13000'
When (o.order_value) > 13000 and (o.order_value) <=15000 Then '30 13001 a 15000'
When (o.order_value) > 15000 and (o.order_value) <=17000 Then '31 15001 a 17000'
When (o.order_value) > 17000 and (o.order_value) <=19000 Then '32 17001 a 19000'
When (o.order_value) > 19000 and (o.order_value) <=20000 Then '33 19001 a 20000'
When (o.order_value) > 20000 Then '34 20000>' 
End as Faixa_Valores
,case 
when o.code like '%REC%' then 'Recarga'
when o.code like '%LIV%' then 'Livelo'
when o.code like '%AZU%' then 'TudoAzul'
when o.code like '%SMI%'  then 'Smiles'
when o.sales_channel = 'APP_ULTRAGAZ' then 'UltraGaz'
when o.sales_channel = 'DRYWASHBRL' then 'DryWash'
when o.code like '%FUT%' then 'Futebol'
when o.sales_channel = 'ECOMMERCE'  then 'Shopping'
when o.sales_channel in ('APP','APP_AMPM','APP_JET_OIL','PDV_QRCODE') then 'Abastecimento'
else o.sales_channel end as Flag_Merchant_Account_Tranx

,te.error_code as Code
,te.error_message 
,te.error_message as Desc_Motivo
,CustomerID
,CPF_Cliente
,Dt_Abertura
,Faixa_Idade
,StatusConta
,RegiaoCliente
,DDD
,Flag_TempodeConta
,Flag_TempoBloqueado
,flag_trusted_atualizado
,MotivoStatus
,sub_classification
,sub_classification_obs
,Safra_Ev
,UsuarioStatus
,MotivoBloqueio
,Flag_Email_NaoVal
,Flag_Celular_NaoVal
,ScoreZaig
,Flag_Biometria
,Dt_LoteMassivo
,MotivoBloq_Massivo
,Flag_Risco_Limit_Vol
,Flag_Risco_Limit_Val
,Flag_Risco_CBK
,Flag_Tetativas
,Flag_Bancos
,Flag_Card
,Flag_Ativo
,Flag_Perfil
--,aer.Issuing_Bank as Banco_Emissor
FROM `eai-datalake-data-sandbox.core.orders` o
join `eai-datalake-data-sandbox.payment.payment` p on p.order_id = o.uuid
join `eai-datalake-data-sandbox.payment.payment_instrument` pi on p.id = pi.id
join `eai-datalake-data-sandbox.payment.payment_transaction` pt on pt.payment_id = p.id
left join `eai-datalake-data-sandbox.payment.payment_transaction_error` te on p.order_id = te.order_id
left join `eai-datalake-data-sandbox.analytics.tb_motivos_recusa_paypal` r on te.error_code = r.Code
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` Cl_Pef on Cl_Pef.id = o.customer_id
--left join `eai-datalake-data-sandbox.paypal.transaction_level_fee_report` as aer on aer.Order_ID = o.uuid

where o.code like 'ROC%'
--payment_method = 'GOOGLE_PAY'
--date(o.created_at) >= '2024-05-08'
--and o.code like '%SMI%'
;

/*
-- Create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Rock_In_Rio_Braintree_insert` as
INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Rock_In_Rio_Braintree_insert`
WITH base_aereas AS (
    SELECT DISTINCT
        Transaction_ID, 
        Subscription_ID, 
        Transaction_Type, 
        Transaction_Status, 
        Escrow_Status,
        PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', Created_Datetime) AS Created_Datetime,
        CAST(Created_Timezone AS INT64) AS Created_Timezone, 
        PARSE_DATE('%m/%d/%Y', Settlement_Date) AS Settlement_Date,
        PARSE_DATE('%m/%d/%Y', Disbursement_Date) AS Disbursement_Date, 
        Merchant_Account, 
        Currency_ISO_Code,
        CAST(Amount_Authorized AS FLOAT64) AS Amount_Authorized,
        CAST(Amount_Submitted_For_Settlement AS FLOAT64) AS Amount_Submitted_For_Settlement,
        Service_Fee, 
        Tax_Amount, 
        Tax_Exempt, 
        Purchase_Order_Number, 
        Order_ID, 
        Descriptor_Name, 
        Descriptor_Phone, 
        Descriptor_URL, 
        Refunded_Transaction_ID, 
        Payment_Instrument_Type, 
        Card_Type, 
        Cardholder_Name,
        CAST(First_Six_of_Credit_Card AS INT64) AS First_Six_of_Credit_Card,
        CAST(Last_Four_of_Credit_Card AS INT64) AS Last_Four_of_Credit_Card,
        Credit_Card_Number, 
        Expiration_Date, 
        Credit_Card_Customer_Location, 
        Customer_ID, 
        Payment_Method_Token, 
        Credit_Card_Unique_Identifier, 
        Customer_First_Name, 
        Customer_Last_Name, 
        Customer_Company, 
        Customer_Email, 
        Customer_Phone, 
        Customer_Fax, 
        Customer_Website, 
        Billing_Address_ID, 
        Billing_First_Name, 
        Billing_Last_Name, 
        Billing_Company, 
        Billing_Street_Address, 
        Billing_Extended_Address, 
        Billing_City__Locality_, 
        Billing_State_Province__Region_, 
        Billing_Postal_Code, 
        Billing_Country, 
        Shipping_Address_ID, 
        Shipping_First_Name, 
        Shipping_Last_Name, 
        Shipping_Company, 
        Shipping_Street_Address, 
        Shipping_Extended_Address, 
        Shipping_City__Locality_, 
        Shipping_State_Province__Region_, 
        Shipping_Postal_Code, 
        Shipping_Country, 
        User, 
        IP_Address, 
        Creating_Using_Token, 
        Transaction_Source, 
        Authorization_Code,
        CAST(Processor_Response_Code AS INT64) AS Processor_Response_Code,
        Processor_Response_Text, 
        Gateway_Rejection_Reason, 
        Postal_Code_Response_Code, 
        Street_Address_Response_Code, 
        AVS_Response_Text, 
        CVV_Response_Code, 
        CVV_Response_Text,
        CAST(Settlement_Amount AS FLOAT64) AS Settlement_Amount,
        Settlement_Currency_ISO_Code,
        CAST(Settlement_Currency_Exchange_Rate AS FLOAT64) AS Settlement_Currency_Exchange_Rate,
        Settlement_Base_Currency_Exchange_Rate, 
        Settlement_Batch_ID, 
        Fraud_Detected, 
        Disputed_Date, 
        Authorized_Transaction_ID,
        Bairro_do_Endere__o AS Bairro_do_Endereco, 
        CEP, 
        CPF,
        Complemento_do_Endere__o AS Complemento_do_Endereco, 
        Endere__o AS Endereco, 
        Estado_do_Endere__o AS Estado_do_Endereco, 
        N__mero_do_Endere__o AS Numero_do_Endereco, 
        Pa__s_do_Endere__o AS Pais_do_Endereco, 
        Country_of_Issuance, 
        Issuing_Bank, 
        Durbin_Regulated, 
        Commercial, 
        Prepaid, 
        Payroll, 
        Healthcare, 
        Affluent_Category, 
        Debit, 
        Product_ID, 
        _3DS___Status, 
        _3DS___PARes_Status, 
        _3DS___ECI_Flag, 
        _3DS___CAVV, 
        _3DS___Signature_Verification, 
        _3DS___Version, 
        _3DS___XID, 
        _3DS___DS_Transaction_ID, 
        _3DS___Challenge_Requested, 
        _3DS___Exemption_Requested, 
        _3DS___Merchant_Requested_Exemption_Type, 
        _3DS___Rule_Summary, 
        _3DS___SCA_Exemption_Type, 
        _3DS___Merchant_Requested_SCA_exemption, 
        PayPal_Payer_Email, 
        PayPal_Payment_ID, 
        PayPal_Authorization_ID, 
        PayPal_Debug_ID, 
        PayPal_Capture_ID, 
        PayPal_Refund_ID, 
        PayPal_Custom_Field, 
        PayPal_Payer_ID, 
        PayPal_Payer_First_Name, 
        PayPal_Payer_Last_Name, 
        PayPal_Seller_Protection_Status, 
        PayPal_Transaction_Fee_Amount, 
        PayPal_Transaction_Fee_Currency_ISO_Code, 
        PayPal_Refund_From_Transaction_Fee_Amount, 
        PayPal_Refund_From_Transaction_Fee_Currency_ISO_Code, 
        PayPal_Payee_Email, 
        PayPal_Payee_ID, 
        Apple_Pay_Card_Last_Four, 
        Apple_Pay_Card_Expiration_Month, 
        Apple_Pay_Card_Expiration_Year, 
        Apple_Pay_Cardholder_Name, 
        Android_Pay_Source_Card_Last_Four, 
        Android_Pay_Source_Card_Type, 
        Source_Card_Last_Four, 
        Risk_ID, 
        Risk_Decision, 
        Device_Data_Captured, 
        Decision_Reasons, 
        Fraud_Protection_Chargeback_Protections, 
        Acquirer_Reference_Number, 
        Venmo_Username, 
        Venmo_Profile_ID, 
        ACH_Reason_Code  
    FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Rock_In_Rio_Braintree_Stagging_Area`  
)
SELECT * FROM base_aereas 
WHERE Created_Datetime > (SELECT MAX(Created_Datetime) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Rock_In_Rio_Braintree_insert`);




create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Rock_In_Rio_Braintree` as 
 
select
distinct
Transaction_ID
--,Transaction_Type as Tipo
,Merchant_Account 
,Risk_Decision as Dec_Motor_PayPal
,Transaction_Status Status_Trans_PayPal
,Processor_Response_Text as Status_Trans_Emissor
,Created_Datetime
--,RANK() OVER (PARTITION BY Transaction_ID ORDER BY Created_Datetime ,Transaction_Status  desc) AS Rank_trans
,date (Created_Datetime) as Dt_Tranx
,Order_ID
,Payment_Instrument_Type
,Card_Type
,CPF
,Customer_ID
,Customer_Email
--,Payment_Method_Token
,Gateway_Rejection_Reason
,Fraud_Detected
,First_Six_of_Credit_Card
,Issuing_Bank
,Amount_Authorized as Vl_PayPal
 
 
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Rock_In_Rio_Braintree_insert`

 
where
--Merchant_Account = 'fangoldenbrl' 
-- and date (Created_Datetime)  >= (current_date() - 2)
Amount_Authorized > 500
order by 1

*/


/*


select distinct
      case 
            when Merchant_Account like '%tudoazul%' then 'Tudo Azul'
            when Merchant_Account like '%latam%' then 'Latam'
            when Merchant_Account like '%smiles%' then 'Smiles'
            when Merchant_Account like '%livelo%' then 'Livelo'
      end as Nome_Parceiro,
      count(distinct Customer_Email) as Volume_Total, 
      count(distinct case when Dec_Motor_PayPal = 'Approve' then Customer_Email end) as Aprovados , 
      count(distinct case when Dec_Motor_PayPal = 'Decline' then Customer_Email end) as Negados, 
      count(distinct case when Dec_Motor_PayPal = 'Not Evaluated' then Customer_Email end) as Em_Analise, 
      concat(round(count(distinct case when Dec_Motor_PayPal = 'Approve' then Customer_Email end) / count(distinct Customer_Email) * 100, 0), '%') as `%_Aprovado`,
      concat(round(count(distinct case when Dec_Motor_PayPal = 'Decline' then Customer_Email end) / count(distinct Customer_Email) * 100, 0), '%') as `%_Negado`,
      concat(round(count(distinct case when Dec_Motor_PayPal = 'Not Evaluated' then Customer_Email end) / count(distinct Customer_Email) * 100, 1), '%') as `%_Analise` 
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Rock_In_Rio_Braintree` 
where date (Created_Datetime) = '2024-05-24'
-- where date (Created_Datetime)  >= (current_date())
group by 1


*/


