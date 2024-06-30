select
            distinct
            Order_ID
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` 


where Customer_Email = 'andersoncesar50@gmail.com'
and Reason = 'Fraud'and Status = 'Open' and Kind = 'Chargeback'
and date(Received_Date) >= current_date - 90



