--======================================================================================
--> TABELA TANSAÇÕES PAYPAL UTIMOS 90 DIAS
--======================================================================================

--drop table `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_transacao_paypal`
--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_transacao_paypal`


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_transacao_paypal` AS 


SELECT
distinct
            customer_payment.created_at, 
            customer_payment.trusted,
            customer_payment.Flag_Contestacao,
            customer_payment.TIPO_TRANX,
            COUNT(DISTINCT customer_id) AS QT_CLIENTE,
            SUM(customer_payment.status_confirmed) AS CONFIRMED,
            SUM(IF(customer_payment.status_denied_paypall > 0, 1, 0)) AS CANCELLED_BY_GATEWAY,
            SUM (customer_payment.VL_TRANX_APR) AS VL_TRANX_APR,
            SUM(customer_payment.VL_TRANX_NEG) AS VL_TRANX_NEG

FROM        ( 
                SELECT      payment.customer_id AS customer_id, 
                            date(payment.created_at) AS created_at, 
                            case when Client.trusted = 0 then 'No Trusted'else 'Trusted' end as trusted,
                            case when payment.order_id = cbk.order_id then 'Contestado' else 'Nao Contestado' end as Flag_Contestacao,
                            payment_transaction.payment_method AS TIPO_TRANX,
                            SUM(CASE WHEN payment_transaction.status in ('AUTHORIZED','SETTLEMENT','REVERSED') THEN payment_transaction.transaction_value ELSE 0 END) AS VL_TRANX_APR,
                            SUM(CASE WHEN payment_transaction.status in ('CANCELLED_BY_GATEWAY') THEN payment_transaction.transaction_value ELSE 0 END) AS VL_TRANX_NEG,
                            SUM(IF(payment_transaction.status in ('AUTHORIZED','SETTLEMENT','REVERSED'),  1, 0)) AS status_confirmed, 
                            AVG(IF(payment_transaction.status = 'CANCELLED_BY_GATEWAY', 1, 0)) AS status_denied_paypall 
                FROM        `eai-datalake-data-sandbox.payment.payment` payment
                JOIN        (SELECT distinct  uuid, trusted FROM `eai-datalake-data-sandbox.core.customers` ) Client on Client.uuid = payment.customer_id
                JOIN        `eai-datalake-data-sandbox.payment.payment_transaction` payment_transaction ON payment.id = payment_transaction.payment_id
                JOIN        `eai-datalake-data-sandbox.payment.payment_instrument` payment_instrument on payment_transaction.payment_instrument_id = payment_instrument.id
                left JOIN        `eai-datalake-data-sandbox.payment.customer_card` customer_card on payment_instrument.uuid = customer_card.uuid
                LEFT JOIN (         select 
                                    distinct
                                    * 
                                    from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` cbk
                                    where 
                                    cbk.Reason = 'Fraud'
                                    and cbk.Status = 'Open'
                                    and cbk.Kind = 'Chargeback'
                                    and date(Transaction_Date) >= current_date - 90 ) cbk on payment.order_id = cbk.order_id                                     


                WHERE       
                date(payment_transaction.created_at) >= current_date - 90
                --AND customer_card.bin not in ('650422', '650429')
                AND payment_transaction.payment_method in ('DIGITAL_WALLET','CREDIT_CARD','DEBIT_CARD',"GOOGLE_PAY")
                AND         payment_transaction.status in ('PRE_AUTHORIZED','AUTHORIZED','CANCELLED_BY_GATEWAY','SETTLEMENT') ---'SETTLEMENT','REVERSED',
                GROUP BY    1,2,3,4,5
                ORDER BY    1,2,3,4,5,6
            ) AS customer_payment
GROUP BY    1,2,3,4
ORDER BY    1,2,3,4


/*

select 
FORMAT_DATE("%Y%m",created_at)as Safra_Tranx , 
sum(CONFIRMED) as qtdApr,
sum(CANCELLED_BY_GATEWAY) as qtdConf_Neg,
sum(VL_TRANX_APR) as Apr, 
sum(VL_TRANX_NEG)as Neg,
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_transacao_paypal` group by 1 order by 1 desc



select * from `eai-datalake-data-sandbox.payment.customer_card` where bin in ('650422', '650429')

*/
