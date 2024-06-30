
-----------------------------------------------------------------------
-- QUERY TRANSACOES AEREAS(LATAM, TUDO AZUL, LIVELO, SMILES)          |
-----------------------------------------------------------------------

-- Tb_Transacional_Aereas_PayPal

/*
  select distinct 
      min(Created_Datetime) as Primeiro_Registro, 
      max(Created_Datetime) as Ultimo_Registro 
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal`

*/

/*

select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal`
order by Created_Datetime desc

*/

/*
create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal` as 
select 
  Transaction_ID	
  ,Subscription_ID
  ,Transaction_Type		
  ,Transaction_Status		
  ,Escrow_Status		
  ,cast(Created_Datetime as timestamp) as Created_Datetime		
  ,Created_Timezone		
  ,Settlement_Date	
  ,cast(Disbursement_Date as date) as Disbursement_Date		
  ,Merchant_Account		
  ,Currency_ISO_Code		
  ,Amount_Authorized		
  ,Amount_Submitted_For_Settlement		
  ,Service_Fee		
  ,Tax_Amount		
  ,Tax_Exempt		
  ,Purchase_Order_Number		
  ,Order_ID		
  ,Descriptor_Name		
  ,Descriptor_Phone		
  ,Descriptor_URL		
  ,Refunded_Transaction_ID		
  ,Payment_Instrument_Type		
  ,Card_Type		
  ,Cardholder_Name		
  ,First_Six_of_Credit_Card		
  ,Last_Four_of_Credit_Card		
  ,Credit_Card_Number		
  ,Expiration_Date
  ,Credit_Card_Customer_Location		
  ,Customer_ID		
  ,Payment_Method_Token		
  ,Credit_Card_Unique_Identifier		
  ,Customer_First_Name		
  ,Customer_Last_Name		
  ,Customer_Company		
  ,Customer_Email		
  ,Customer_Phone		
  ,Customer_Fax		
  ,Customer_Website	
  ,Billing_Address_ID			
  ,Billing_First_Name			
  ,Billing_Last_Name			
  ,Billing_Company			
  ,Billing_Street_Address			
  ,Billing_Extended_Address			
  ,Billing_City__Locality_			
  ,Billing_State_Province__Region_			
  ,Billing_Postal_Code			
  ,Billing_Country			
  ,Shipping_Address_ID			
  ,Shipping_First_Name			
  ,Shipping_Last_Name			
  ,Shipping_Company			
  ,Shipping_Street_Address			
  ,Shipping_Extended_Address			
  ,Shipping_City__Locality_			
  ,Shipping_State_Province__Region_			
  ,Shipping_Postal_Code			
  ,Shipping_Country			
  ,User			
  ,IP_Address			
  ,Creating_Using_Token			
  ,Transaction_Source
  ,Authorization_Code		
  ,Processor_Response_Code		
  ,Processor_Response_Text		
  ,Gateway_Rejection_Reason		
  ,Postal_Code_Response_Code		
  ,Street_Address_Response_Code		
  ,AVS_Response_Text		
  ,CVV_Response_Code		
  ,CVV_Response_Text		
  ,cast(Settlement_Amount	as int) as Settlement_Amount
  ,Settlement_Currency_ISO_Code		
  ,cast(Settlement_Currency_Exchange_Rate as int) as Settlement_Currency_Exchange_Rate		
  ,Settlement_Base_Currency_Exchange_Rate		
  ,Settlement_Batch_ID		
  ,Fraud_Detected		
  ,Authorized_Transaction_ID		
  ,Bairro_do_Endere__o		
  ,CEP		
  ,CPF		
  ,Complemento_do_Endere__o		
  ,Endere__o		
  ,Estado_do_Endere__o		
  ,N__mero_do_Endere__o		
  ,Pa__s_do_Endere__o		
  ,Country_of_Issuance		
  ,Issuing_Bank		
  ,Durbin_Regulated		
  ,Commercial		
  ,Prepaid		
  ,Payroll		
  ,Healthcare	
  ,Affluent_Category	
  ,Debit	
  ,Product_ID	
  ,_3DS___Status	
  ,_3DS___PARes_Status	
  ,_3DS___ECI_Flag	
  ,_3DS___CAVV	
  ,_3DS___Signature_Verification	
  ,_3DS___Version	
  ,_3DS___XID	
  ,_3DS___DS_Transaction_ID	
  ,_3DS___Challenge_Requested	
  ,_3DS___Exemption_Requested	
  ,_3DS___Merchant_Requested_Exemption_Type	
  ,_3DS___SCA_Exemption_Type	
  ,_3DS___Merchant_Requested_SCA_exemption	
  ,PayPal_Payer_Email	
  ,PayPal_Payment_ID	
  ,PayPal_Authorization_ID	
  ,PayPal_Debug_ID	
  ,PayPal_Capture_ID	
  ,PayPal_Refund_ID	
  ,PayPal_Custom_Field	
  ,PayPal_Payer_ID	
  ,PayPal_Payer_First_Name	
  ,PayPal_Payer_Last_Name	
  ,PayPal_Seller_Protection_Status	
  ,PayPal_Transaction_Fee_Amount	
  ,PayPal_Transaction_Fee_Currency_ISO_Code	
  ,PayPal_Refund_From_Transaction_Fee_Amount	
  ,PayPal_Refund_From_Transaction_Fee_Currency_ISO_Code	
  ,PayPal_Payee_Email	
  ,PayPal_Payee_ID	
  ,Apple_Pay_Card_Last_Four	
  ,Apple_Pay_Card_Expiration_Month	
  ,Apple_Pay_Card_Expiration_Year	
  ,Apple_Pay_Cardholder_Name	
  ,Android_Pay_Source_Card_Last_Four	
  ,Android_Pay_Source_Card_Type	
  ,Source_Card_Last_Four	
  ,Risk_ID	
  ,Risk_Decision	
  ,Device_Data_Captured	
  ,Decision_Reasons	
  ,Fraud_Protection_Chargeback_Protections	
  ,Acquirer_Reference_Number	
  ,Venmo_Username	
  ,Venmo_Profile_ID	
  ,ACH_Reason_Code	
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal` 
*/

/*

Transaction_ID		
,Subscription_ID		
,Transaction_Type		
,Transaction_Status		
,Escrow_Status		
,Created_Datetime		
,Created_Timezone		
,Settlement_Date:DATE		
,Disbursement_Date		
,Merchant_Account		
,Currency_ISO_Code		
,Amount_Authorized		
,Amount_Submitted_For_Settlement		
,Service_Fee		
,Tax_Amount		
,Tax_Exempt		
,Purchase_Order_Number		
,Order_ID		
,Descriptor_Name		
,Descriptor_Phone		
,Descriptor_URL		
,Refunded_Transaction_ID		
,Payment_Instrument_Type		
,Card_Type		
,Cardholder_Name		
,First_Six_of_Credit_Card		
,Last_Four_of_Credit_Card		
,Credit_Card_Number		
,Expiration_Date:DATE		
,Credit_Card_Customer_Location		
,Customer_ID		
,Payment_Method_Token		
,Credit_Card_Unique_Identifier		
,Customer_First_Name		
,Customer_Last_Name		
,Customer_Company		
,Customer_Email		
,Customer_Phone		
,Customer_Fax		
,Customer_Website	
,Billing_Address_ID			
,Billing_First_Name			
,Billing_Last_Name			
,Billing_Company			
,Billing_Street_Address			
,Billing_Extended_Address			
,Billing_City__Locality_			
,Billing_State_Province__Region_			
,Billing_Postal_Code			
,Billing_Country			
,Shipping_Address_ID			
,Shipping_First_Name			
,Shipping_Last_Name			
,Shipping_Company			
,Shipping_Street_Address			
,Shipping_Extended_Address			
,Shipping_City__Locality_			
,Shipping_State_Province__Region_			
,Shipping_Postal_Code			
,Shipping_Country			
,User			
,IP_Address			
,Creating_Using_Token			
,Transaction_Source
,Authorization_Code		
,Processor_Response_Code		
,Processor_Response_Text		
,Gateway_Rejection_Reason		
,Postal_Code_Response_Code		
,Street_Address_Response_Code		
,AVS_Response_Text		
,CVV_Response_Code		
,CVV_Response_Text		
,Settlement_Amount		
,Settlement_Currency_ISO_Code		
,Settlement_Currency_Exchange_Rate		
,Settlement_Base_Currency_Exchange_Rate		
,Settlement_Batch_ID		
,Fraud_Detected		
,Authorized_Transaction_ID		
,Bairro_do_Endere__o		
,CEP		
,CPF		
,Complemento_do_Endere__o		
,Endere__o		
,Estado_do_Endere__o		
,N__mero_do_Endere__o		
,Pa__s_do_Endere__o		
,Country_of_Issuance		
,Issuing_Bank		
,Durbin_Regulated		
,Commercial		
,Prepaid		
,Payroll		
,Healthcare	
,Affluent_Category	
,Debit	BOOLEAN	
,Product_ID	
,_3DS___Status	
,_3DS___PARes_Status	
,_3DS___ECI_Flag	
,_3DS___CAVV	
,_3DS___Signature_Verification	
,_3DS___Version	
,_3DS___XID	
,_3DS___DS_Transaction_ID	
,_3DS___Challenge_Requested	
,_3DS___Exemption_Requested	
,_3DS___Merchant_Requested_Exemption_Type	
,_3DS___SCA_Exemption_Type	
,_3DS___Merchant_Requested_SCA_exemption	
,PayPal_Payer_Email	
,PayPal_Payment_ID	
,PayPal_Authorization_ID	
,PayPal_Debug_ID	
,PayPal_Capture_ID	
,PayPal_Refund_ID	
,PayPal_Custom_Field	
,PayPal_Payer_ID	
,PayPal_Payer_First_Name	
,PayPal_Payer_Last_Name	
,PayPal_Seller_Protection_Status	
,PayPal_Transaction_Fee_Amount	
,PayPal_Transaction_Fee_Currency_ISO_Code	
,PayPal_Refund_From_Transaction_Fee_Amount	
,PayPal_Refund_From_Transaction_Fee_Currency_ISO_Code	
,PayPal_Payee_Email	
,PayPal_Payee_ID	
,Apple_Pay_Card_Last_Four	
,Apple_Pay_Card_Expiration_Month	
,Apple_Pay_Card_Expiration_Year	
,Apple_Pay_Cardholder_Name	
,Android_Pay_Source_Card_Last_Four	
,Android_Pay_Source_Card_Type	
,Source_Card_Last_Four	
,Risk_ID	
,Risk_Decision	
,Device_Data_Captured	
,Decision_Reasons	
,Fraud_Protection_Chargeback_Protections	
,Acquirer_Reference_Number	
,Venmo_Username	
,Venmo_Profile_ID	
,ACH_Reason_Code	

*/



-------------------------------------------------------------------------------------------------------------------------------------------------------



CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_monit_aereas` AS
 
with
 
Base_Transacional as (
 
 
select
distinct
Transaction_ID
,case 
    when Transaction_Type = 'credit' then 'Estorno'
    else 'Venda'
 end as Tipo
,case
      when Merchant_Account like '%latam%' then 'Latam'
      when Merchant_Account like '%tudoazul%' then 'Tudo Azul'
      when Merchant_Account like '%livelo%' then 'Livelo'
      when Merchant_Account like '%smiles%' then 'Smiles'
end as Flag_Produtos
,case 
      when Risk_Decision = 'Approve' then 'Aprovado'
      when Risk_Decision = 'Decline' then 'Negado'
      when Risk_Decision = 'Not Evaluated' then 'Não avaliado'
end as Dec_Motor_PayPal
,case 
      when Transaction_Status = 'settling' then 'Liquidação' 
      when Transaction_Status = 'gateway_rejected' then 'Gateway recusou'
      when Transaction_Status = 'processor_declined' then 'Processador recusou'
      when Transaction_Status = 'settled' then 'Concluído'
      when Transaction_Status = 'failed' then 'Falhou'
      when Transaction_Status = 'voided' then 'Cancelado'
      else Transaction_Status
end as Status_Trans_PayPal
,case
      when Processor_Response_Text = 'Approved' then 'Aprovado'  
      when Processor_Response_Text = 'Unavailable' then 'Indisponível'
      when Processor_Response_Text = 'Declined' then 'Recusado'
      when Processor_Response_Text = 'Do Not Honor' then 'Não Honrar'
      when Processor_Response_Text = 'Insufficient Funds' then 'Fundos Insuficientes'
      when Processor_Response_Text = 'Transaction Not Allowed' then 'Transação Não Permitida'
      when Processor_Response_Text = 'Cannot Authorize at this time (Life cycle)' then 'Não é possível autorizar neste momento'
      when Processor_Response_Text = 'Cannot Authorize at this time (Policy)' then 'Não é possível realizar a autorização neste momento devido a políticas do emissor'
      when Processor_Response_Text = 'Declined - Call Issuer' then 'Negado - Entre em Contato com o Emissor'
      when Processor_Response_Text = 'Processed' then 'Processado'
      when Processor_Response_Text = 'Expired Card' then 'Cartão Expirado'
      when Processor_Response_Text = 'Set Up Error - Amount' then 'Erro de Configuração - Valor'
      when Processor_Response_Text = 'Limit Exceeded' then 'Limite Excedido'
      when Processor_Response_Text = 'Card Type Not Enabled' then 'Tipo de Cartão Não Habilitado'
      when Processor_Response_Text = 'Issuer or Cardholder has put a restriction on the card' then 'O emissor ou titular do cartão aplicou uma restrição ao cartão'
      when Processor_Response_Text = 'Processor Network Unavailable - Try Again' then 'Rede do Processador Indisponível - Tente Novamente'
      when Processor_Response_Text = 'Processor Declined' then 'Transação Recusada pelo Processador'
      when Processor_Response_Text = 'Processor Does Not Support This Feature' then 'O Processador Não Oferece Suporte a Este Recurso'
      when Processor_Response_Text = 'Inconsistent Data' then 'Dados Inconsistentes'
      when Processor_Response_Text = 'Invalid Merchant Number' then 'Número de Comerciante Inválido'
      when Processor_Response_Text = 'Incorrect PIN' then 'PIN Incorreto'
      when Processor_Response_Text = 'Security Violation' then 'Violação de Segurança'
      when Processor_Response_Text = 'PIN Try Exceeded' then 'Limite de Tentativas de PIN Excedido'
      when Processor_Response_Text = 'Call Issuer. Pick Up Card.' then 'Ligar para o Emissor. Retirar o Cartão'
      when Processor_Response_Text = 'Offline Issuer Declined' then 'Emissor Offline Recusou'
      else Processor_Response_Text
end as Status_Trans_Emissor
,Created_Datetime
,RANK() OVER (PARTITION BY Transaction_ID ORDER BY Created_Datetime ,Transaction_Status  desc) AS Rank_trans
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
 
 
from (Select
distinct * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2` 
) 
 
where
date (Created_Datetime)  >= (current_date() - 90)
order by 1
)
select
*
from Base_Transacional
where Rank_trans = 1
;


-- select Status_Trans_Emissor, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_monit_aereas` group by 1

-----------------------
-- TRANSACOES LATAM   |
-----------------------
 
 
-- select distinct * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_monit_Latam_PayPal_2023`
 
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_monit_Latam_PayPal_2023` AS
 
with
 
Base_Transacional_Latam as (
 
 
select
distinct
latam.Transaction_ID
,case 
    when latam.Transaction_Type = 'credit' then 'Estorno'
    else 'Venda'
 end as Tipo
,latam.Risk_Decision as Dec_Motor_PayPal
,latam.Transaction_Status as Status_Trans_PayPal
,latam.Processor_Response_Text  as Status_Trans_Emissor
,latam.Created_Datetime
,RANK() OVER (PARTITION BY Transaction_ID ORDER BY latam.Created_Datetime ,latam.Transaction_Status  desc) AS Rank_trans
,date (latam.Created_Datetime) as Dt_Tranx
,latam.Order_ID
,latam.Payment_Instrument_Type
,latam.Card_Type
,latam.CPF
,latam.Customer_ID
,latam.Customer_Email
--,latam.Payment_Method_Token
,latam.Gateway_Rejection_Reason
,latam.Fraud_Detected
,latam.First_Six_of_Credit_Card
,latam.Issuing_Bank
,latam.Amount_Authorized as Vl_PayPal
 
 
from (Select
distinct * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2`
where Merchant_Account like '%latam%'
) latam
 
where
date (latam.Created_Datetime)  >= (current_date() - 90)
order by 1
)
select
*
from Base_Transacional_Latam
where Rank_trans = 1
;


---------------------------------------------
-- CUBO COMPARATIVO D-3 VOLUME LATAM        |
---------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Comparativo_Dia_Latam_PayPal` AS 

with
base as (
        select
          distinct
          Transaction_ID
          ,Merchant_Account as Parceiro
          ,date(Created_Datetime) as Data_Transacao
          ,FORMAT_DATE("%d",Created_Datetime)as Dia
          ,EXTRACT(HOUR FROM Created_Datetime)as Hr_transacao
          ,RANK() OVER (PARTITION BY Transaction_ID ORDER BY Created_Datetime ,Transaction_Status  desc) AS Rank_trans
          ,Case 
                When EXTRACT(HOUR FROM Created_Datetime) >=0 	AND EXTRACT(HOUR FROM Created_Datetime) <=2 	  Then '1 00a02'
                When EXTRACT(HOUR FROM Created_Datetime) > 2 	AND EXTRACT(HOUR FROM Created_Datetime) <=5 	  Then '2 03a05'
                When EXTRACT(HOUR FROM Created_Datetime) > 5 	AND EXTRACT(HOUR FROM Created_Datetime) <=8 	  Then '3 06a08'
                When EXTRACT(HOUR FROM Created_Datetime) > 8	  AND EXTRACT(HOUR FROM Created_Datetime) <=11 	Then '4 09a11'
                When EXTRACT(HOUR FROM Created_Datetime) > 11  AND EXTRACT(HOUR FROM Created_Datetime) <=14 	Then '5 12a14'
                When EXTRACT(HOUR FROM Created_Datetime) > 14  AND EXTRACT(HOUR FROM Created_Datetime) <=17 	Then '6 15a17'
                When EXTRACT(HOUR FROM Created_Datetime) > 17  AND EXTRACT(HOUR FROM Created_Datetime) <=20 	Then '7 18a20'	  
                When EXTRACT(HOUR FROM Created_Datetime) > 20  AND EXTRACT(HOUR FROM Created_Datetime) <=23 	Then '8 21a23'	  
          End as Faixa_Hora
          ,CASE
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=0  THEN '01_<D0'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=1  THEN '02_<D-1'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=2  THEN '03_<D-2'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=3  THEN '04_<D-3'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) >3   THEN '05_<OutrosDias'   
          END AS Flag_Dia

          from (select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2` where date(Created_Datetime) >= current_date - 30)

)select 
Parceiro	
,Data_Transacao
,Dia
,Hr_transacao
,Rank_trans
,Faixa_Hora
,Flag_Dia
,count(distinct Transaction_ID) as Qtd_Trancacao
from base 
where Rank_trans = 1 
and Parceiro like '%latam%'
and Flag_Dia in ('01_<D0','02_<D-1','03_<D-2','04_<D-3')
group by 1,2,3,4,5,6,7
;




-------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------
-- TRANSACOES TUDOAZUL   |
--------------------------

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_monit_TudoAzul` AS
 
with
 
Base_Transacional_TudoAzul as (
 
 
select
distinct
tudoazul.Transaction_ID
,case 
    when tudoazul.Transaction_Type = 'credit' then 'Estorno'
    else 'Venda'
 end as Tipo
,tudoazul.Risk_Decision as Dec_Motor_PayPal
,tudoazul.Transaction_Status as Status_Trans_PayPal
,tudoazul.Processor_Response_Text  as Status_Trans_Emissor
,tudoazul.Created_Datetime
,RANK() OVER (PARTITION BY Transaction_ID ORDER BY tudoazul.Created_Datetime ,tudoazul.Transaction_Status  desc) AS Rank_trans
,date (tudoazul.Created_Datetime) as Dt_Tranx
,tudoazul.Order_ID
,tudoazul.Payment_Instrument_Type
,tudoazul.Card_Type
,tudoazul.CPF
,tudoazul.Customer_ID
,tudoazul.Customer_Email
--,tudoazul.Payment_Method_Token
,tudoazul.Gateway_Rejection_Reason
,tudoazul.Fraud_Detected
,tudoazul.First_Six_of_Credit_Card
,tudoazul.Issuing_Bank
,tudoazul.Amount_Authorized as Vl_PayPal
 
 
from (Select
distinct * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2`
where Merchant_Account like '%tudoazul%'
) tudoazul
 
where
date (tudoazul.Created_Datetime)  >= (current_date() - 90)
order by 1
)
select
*
from Base_Transacional_TudoAzul
where Rank_trans = 1
;


------------------------------------------------
-- CUBO COMPARATIVO D-3 VOLUME TUDOAZUL        |
------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Comparativo_Dia_Tudoazul_PayPal` AS 

with
base as (
        select
          distinct
          Transaction_ID
          ,Merchant_Account as Parceiro
          ,date(Created_Datetime) as Data_Transacao
          ,FORMAT_DATE("%d",Created_Datetime)as Dia
          ,EXTRACT(HOUR FROM Created_Datetime)as Hr_transacao
          ,RANK() OVER (PARTITION BY Transaction_ID ORDER BY Created_Datetime ,Transaction_Status  desc) AS Rank_trans
          ,Case 
                When EXTRACT(HOUR FROM Created_Datetime) >=0 	AND EXTRACT(HOUR FROM Created_Datetime) <=2 	  Then '1 00a02'
                When EXTRACT(HOUR FROM Created_Datetime) > 2 	AND EXTRACT(HOUR FROM Created_Datetime) <=5 	  Then '2 03a05'
                When EXTRACT(HOUR FROM Created_Datetime) > 5 	AND EXTRACT(HOUR FROM Created_Datetime) <=8 	  Then '3 06a08'
                When EXTRACT(HOUR FROM Created_Datetime) > 8	  AND EXTRACT(HOUR FROM Created_Datetime) <=11 	Then '4 09a11'
                When EXTRACT(HOUR FROM Created_Datetime) > 11  AND EXTRACT(HOUR FROM Created_Datetime) <=14 	Then '5 12a14'
                When EXTRACT(HOUR FROM Created_Datetime) > 14  AND EXTRACT(HOUR FROM Created_Datetime) <=17 	Then '6 15a17'
                When EXTRACT(HOUR FROM Created_Datetime) > 17  AND EXTRACT(HOUR FROM Created_Datetime) <=20 	Then '7 18a20'	  
                When EXTRACT(HOUR FROM Created_Datetime) > 20  AND EXTRACT(HOUR FROM Created_Datetime) <=23 	Then '8 21a23'	  
          End as Faixa_Hora
          ,CASE
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=0  THEN '01_<D0'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=1  THEN '02_<D-1'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=2  THEN '03_<D-2'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=3  THEN '04_<D-3'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) >3   THEN '05_<OutrosDias'   
          END AS Flag_Dia

          from (select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2` where date(Created_Datetime) >= current_date - 30)

)select 
Parceiro	
,Data_Transacao
,Dia
,Hr_transacao
,Rank_trans
,Faixa_Hora
,Flag_Dia
,count(distinct Transaction_ID) as Qtd_Trancacao
from base 
where Rank_trans = 1 
and Parceiro like '%tudoazul%'
and Flag_Dia in ('01_<D0','02_<D-1','03_<D-2','04_<D-3')
group by 1,2,3,4,5,6,7
;


-------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------
-- TRANSACOES LIVELO     |
--------------------------


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_monit_livelo_2` AS
 
with Base_Transacional_livelo as (
 
select
distinct
livelo.Transaction_ID
,case 
    when livelo.Transaction_Type = 'credit' then 'Estorno'
    else 'Venda'
 end as Tipo
,livelo.Risk_Decision as Dec_Motor_PayPal
,livelo.Transaction_Status as Status_Trans_PayPal
,livelo.Processor_Response_Text  as Status_Trans_Emissor
,livelo.Created_Datetime
,RANK() OVER (PARTITION BY Transaction_ID ORDER BY livelo.Created_Datetime ,livelo.Transaction_Status  desc) AS Rank_trans
,date (livelo.Created_Datetime) as Dt_Tranx
,livelo.Order_ID
,livelo.Payment_Instrument_Type
,livelo.Card_Type
,livelo.CPF
,livelo.Customer_ID
,livelo.Customer_Email
--,livelo.Payment_Method_Token
,livelo.Gateway_Rejection_Reason
,livelo.Fraud_Detected
,livelo.First_Six_of_Credit_Card
,livelo.Issuing_Bank
,livelo.Amount_Authorized as Vl_PayPal
 
 
from (Select
distinct * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2`
where Merchant_Account like '%livelo%'
) livelo
 
where
date (livelo.Created_Datetime)  >= (current_date() - 90)
order by 1
)
select
*
from Base_Transacional_livelo
where Rank_trans = 1
;

------------------------------------------------
-- CUBO COMPARATIVO D-3 VOLUME LIVELO          |
------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Comparativo_Dia_Livelo_PayPal` AS 

with
base as (
        select
          distinct
          Transaction_ID
          ,Merchant_Account as Parceiro
          ,date(Created_Datetime) as Data_Transacao
          ,FORMAT_DATE("%d",Created_Datetime)as Dia
          ,EXTRACT(HOUR FROM Created_Datetime)as Hr_transacao
          ,RANK() OVER (PARTITION BY Transaction_ID ORDER BY Created_Datetime ,Transaction_Status  desc) AS Rank_trans
          ,Case 
                When EXTRACT(HOUR FROM Created_Datetime) >=0 	AND EXTRACT(HOUR FROM Created_Datetime) <=2 	  Then '1 00a02'
                When EXTRACT(HOUR FROM Created_Datetime) > 2 	AND EXTRACT(HOUR FROM Created_Datetime) <=5 	  Then '2 03a05'
                When EXTRACT(HOUR FROM Created_Datetime) > 5 	AND EXTRACT(HOUR FROM Created_Datetime) <=8 	  Then '3 06a08'
                When EXTRACT(HOUR FROM Created_Datetime) > 8	  AND EXTRACT(HOUR FROM Created_Datetime) <=11 	Then '4 09a11'
                When EXTRACT(HOUR FROM Created_Datetime) > 11  AND EXTRACT(HOUR FROM Created_Datetime) <=14 	Then '5 12a14'
                When EXTRACT(HOUR FROM Created_Datetime) > 14  AND EXTRACT(HOUR FROM Created_Datetime) <=17 	Then '6 15a17'
                When EXTRACT(HOUR FROM Created_Datetime) > 17  AND EXTRACT(HOUR FROM Created_Datetime) <=20 	Then '7 18a20'	  
                When EXTRACT(HOUR FROM Created_Datetime) > 20  AND EXTRACT(HOUR FROM Created_Datetime) <=23 	Then '8 21a23'	  
          End as Faixa_Hora
          ,CASE
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=0  THEN '01_<D0'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=1  THEN '02_<D-1'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=2  THEN '03_<D-2'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=3  THEN '04_<D-3'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) >3   THEN '05_<OutrosDias'   
          END AS Flag_Dia

          from (select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2` where date(Created_Datetime) >= current_date - 30)
)select 
Parceiro	
,Data_Transacao
,Dia
,Hr_transacao
,Rank_trans
,Faixa_Hora
,Flag_Dia
,count(distinct Transaction_ID) as Qtd_Trancacao
from base 
where Rank_trans = 1 
and Parceiro like '%livelo%'
and Flag_Dia in ('01_<D0','02_<D-1','03_<D-2','04_<D-3')
group by 1,2,3,4,5,6,7
;

-------------------------------------------------------------------------------------------------------------------------------------------------------

--------------------------
-- TRANSACOES SMILES     |
--------------------------

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_monit_smiles_2` AS
 
with Base_Transacional_smiles as (
 
select
distinct
smiles.Transaction_ID
,case 
    when smiles.Transaction_Type = 'credit' then 'Estorno'
    else 'Venda'
 end as Tipo
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
--,smiles.Payment_Method_Token
,smiles.Gateway_Rejection_Reason
,smiles.Fraud_Detected
,smiles.First_Six_of_Credit_Card
,smiles.Issuing_Bank
,smiles.Amount_Authorized as Vl_PayPal
 
 
from (Select
distinct * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2`
where Merchant_Account like '%smiles%'
) smiles
 
where
date (smiles.Created_Datetime)  >= (current_date() - 90)
order by 1
)
select
*
from Base_Transacional_smiles
where Rank_trans = 1
;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------

------------------------------------------------
-- CUBO COMPARATIVO D-3 VOLUME SMILES          |
------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Comparativo_Dia_Smiles_PayPal` AS 

with
base as (
        select
          distinct
          Transaction_ID
          ,Merchant_Account as Parceiro
          ,date(Created_Datetime) as Data_Transacao
          ,FORMAT_DATE("%d",Created_Datetime)as Dia
          ,EXTRACT(HOUR FROM Created_Datetime)as Hr_transacao
          ,RANK() OVER (PARTITION BY Transaction_ID ORDER BY Created_Datetime ,Transaction_Status  desc) AS Rank_trans
          ,Case 
                When EXTRACT(HOUR FROM Created_Datetime) >=0 	AND EXTRACT(HOUR FROM Created_Datetime) <=2 	  Then '1 00a02'
                When EXTRACT(HOUR FROM Created_Datetime) > 2 	AND EXTRACT(HOUR FROM Created_Datetime) <=5 	  Then '2 03a05'
                When EXTRACT(HOUR FROM Created_Datetime) > 5 	AND EXTRACT(HOUR FROM Created_Datetime) <=8 	  Then '3 06a08'
                When EXTRACT(HOUR FROM Created_Datetime) > 8	  AND EXTRACT(HOUR FROM Created_Datetime) <=11 	Then '4 09a11'
                When EXTRACT(HOUR FROM Created_Datetime) > 11  AND EXTRACT(HOUR FROM Created_Datetime) <=14 	Then '5 12a14'
                When EXTRACT(HOUR FROM Created_Datetime) > 14  AND EXTRACT(HOUR FROM Created_Datetime) <=17 	Then '6 15a17'
                When EXTRACT(HOUR FROM Created_Datetime) > 17  AND EXTRACT(HOUR FROM Created_Datetime) <=20 	Then '7 18a20'	  
                When EXTRACT(HOUR FROM Created_Datetime) > 20  AND EXTRACT(HOUR FROM Created_Datetime) <=23 	Then '8 21a23'	  
          End as Faixa_Hora
          ,CASE
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=0  THEN '01_<D0'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=1  THEN '02_<D-1'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=2  THEN '03_<D-2'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) <=3  THEN '04_<D-3'
                WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(Created_Datetime), DAY) >3   THEN '05_<OutrosDias'   
          END AS Flag_Dia

          from (select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2` where date(Created_Datetime) >= current_date - 30)
)select 
Parceiro	
,Data_Transacao
,Dia
,Hr_transacao
,Rank_trans
,Faixa_Hora
,Flag_Dia
,count(distinct Transaction_ID) as Qtd_Trancacao
from base 
where Rank_trans = 1 
and Parceiro like '%smiles%'
and Flag_Dia in ('01_<D0','02_<D-1','03_<D-2','04_<D-3')
group by 1,2,3,4,5,6,7
;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



----------------------------------------------------------------------------------------------
-- Contagem dos volumes totais, aprovados, negados, em analise e % dos parceiros (Dia)       |
----------------------------------------------------------------------------------------------

/*

select distinct
      case 
            when Merchant_Account like '%tudoazul%' then 'Tudo Azul'
            when Merchant_Account like '%latam%' then 'Latam'
            when Merchant_Account like '%smiles%' then 'Smiles'
            when Merchant_Account like '%livelo%' then 'Livelo'
      end as Nome_Parceiro,
      count(distinct Transaction_ID) as Volume_Total, 
      count(distinct case when Risk_Decision = 'Approve' then Transaction_ID end) as Aprovados , 
      count(distinct case when Risk_Decision = 'Decline' then Transaction_ID end) as Negados, 
      count(distinct case when Risk_Decision = 'Not Evaluated' then Transaction_ID end) as Em_Analise, 
      concat(round(count(distinct case when Risk_Decision = 'Approve' then Transaction_ID end) / count(distinct Transaction_ID) * 100, 0), '%') as `%_Aprovado`,
      concat(round(count(distinct case when Risk_Decision = 'Decline' then Transaction_ID end) / count(distinct Transaction_ID) * 100, 0), '%') as `%_Negado`,
      concat(round(count(distinct case when Risk_Decision = 'Not Evaluated' then Transaction_ID end) / count(distinct Transaction_ID) * 100, 1), '%') as `%_Analise` 
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal` 
where date (Created_Datetime) = current_date()
-- where date (Created_Datetime)  >= (current_date())
group by 1

*/

