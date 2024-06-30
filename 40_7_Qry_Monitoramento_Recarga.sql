-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Estudo_Recarga`


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Estudo_Recarga`  AS

with
Base_PayPal as (
            select
            distinct
            
                --tranx.id	
                --,tranx.uuid
                tranx.payment_id	
                ,tranx.payment_instrument_id
                ,tranx.gateway
                ,tranx.gateway_id	
                ,tranx.status	as StatusTranx
                ,tranx.payment_method	
                ,tranx.transaction_value	
                ,ord.cashback_percentage
                --,tranx.installment_count	
                --,tranx.updated_at	
                --,tranx.version	
                --,tranx_d.id	
                --,tranx_d.uuid	
                ,tranx_d.order_id	
                --,tranx_d.store_id	
                ,tranx_d.customer_id	
                ,tranx_d.generated_payment_instrument_id	
                ,tranx_d.order_code	as code_tranx
                ,substring(tranx_d.order_code,1,STRPOS(tranx_d.order_code,'-')) as order_code 	
                ,tranx_d.status	
                ,tranx_d.sales_channel	
                --,tranx_d.created_at
                --,tranx_d.updated_at	
                --,tranx_d.version	
                --,ord.id	
                --,ord.uuid	
                --,ord.own_id	
                ,ord.pdv_token	
                ,ord.notification_id	
                --,ord.code	
                --,ord.nsu	
                --,ord.customer_id
                --,ord.category_id	
                ,ord.store_id as Cod_Loja	
                --,ord.order_value	
                --,ord.discount	
                --,ord.cashback_percentage	
                --,ord.cashback_value/100 as cashback_value
                --,ord.order_status	
                --,ord.sales_channel
                --,ord.payment_id
                --,ord.expiration_time	
                --,ord.benefit_id_old	
                --,ord.latitude	
                --,ord.longitude	
                --,locPost.store_id
                ,locPost.Nome_loja
                ,locPost.CNPJ_CPF
                ,locPost.tipo_loja
                ,locPost.cidade
                ,locPost.UF as UF_Posto
                ,CASE
                  WHEN locPost.UF IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
                  WHEN locPost.UF IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
                  WHEN locPost.UF IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
                  WHEN locPost.UF IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
                  WHEN locPost.UF IN ('SC','PR','RS') THEN 'SUL'
                  ELSE 'NAOINDENTIFICADO'
                END AS RegiaoPosto
                ,locPost.latitude  as latitude_Posto
                ,locPost.longitude as longitude_Posto
                ,locPost.latitude||locPost.longitude as  latitude_longitude_Posto

                ,left(ord.latitude,7) as  latitude_Tranx
                ,left(ord.longitude,7) as  longitude_Tranx
                ,left(ord.latitude,7) ||left(ord.longitude,7) as  latitude_longitude_Tranx
                ,locTranx.cidade as Cidade_Trax
                ,locTranx.UF as UF_Tranx
                ,(cast(left(ord.latitude,7) as numeric) - cast(locPost.latitude  as numeric)) - (cast(left(ord.longitude,7)as numeric) - cast(locPost.longitude as numeric)) as dif_geral
                --,ord.created_at
                --,ord.updated_at
                --,ord.version
                ,cbk.Dispute_ID	
                --,cbk.Original_Dispute_ID	
                --,cbk.Received_Date	
                --,cbk.Transaction_Date	
                --,cbk.Amount_Disputed	
                ,cbk.Transaction_Amount	
                --,cbk.Currency_ISO_Code	
                --,cbk.Kind	
                --,cbk.Reason	
                --,cbk.Status
                --,cbk.Case_Number	
                ,cbk.Transaction_ID	
                ,cbk.Merchant_Account	
                --,cbk.Order_ID
                --,cbk.Credit_Card_Number	
                ,substr(cbk.Credit_Card_Number, 4,4) as Card_Ult_4_cbk
                --,card.qtd_Tetativas
                --,card.qtd_cartao
                ,cbk.Card_Type	
                ,cbk.Customer_Name	
                ,cbk.Customer_Email	
                --,cbk.Refunded	
                --,cbk.Fraud_Detected	
                --,cbk.Reply_Before_Date	
                --,cbk.Disputed_Date	
                --,cbk.Customer_ID_CPF	
                --,cbk.Payment_Method_Token	
                ,cbk.BIN
                ,case 
                  when cbk.Transaction_Amount >0 and cbk.Amount_Won = 0  then 'PayPal_Perdeu'
                  else 'PayPal_Ganho' end as Flag_StatusCBK
                ,case when tranx.gateway_id = cbk.Transaction_ID then 'Contestado' else 'NaoContestado' end as FlagContestacao
                ,cl.CPF as CPF_Cliente
                ,cl.Cidade as Cidade_Cliente
                ,cl.UF as UF_Cliente
                ,cl.Flag_Trusted
                ,cl.RegiaoCliente
                ,date(cl.DataCriacao) as Dt_Abertura
                ,cl.Safra_Abertura
                ,date(tranx.created_at) as Dt_Tranx
                ,FORMAT_DATE("%Y%m",tranx.created_at)as Safra_Tranx
                --,card.Dt_Cadast_Card
                ,case when cast(cl.DDD as numeric) = ro.Phone_Area_Code then 'MesmoDDD' else 'OutroDDD' end as Flag_DDD
                ,cl.DDD||'-'||cl.Telefone as Telefone_Cliente
                ,cl.Tipo_Telefone
                ,ro.Phone_Area_Code||'-'||ro.Phone_Number as Telefone_Recarga
                --,ro.Carrier_Name
                ,case 
                  when ro.Carrier_Name like '%VIVO%' then 'VIVO'
                  when ro.Carrier_Name like '%CLARO%' then 'CLARO'
                  when ro.Carrier_Name like '%TIM%' then 'TIM'
                  when ro.Carrier_Name like '%OI%' then 'OI'
                  when ro.Carrier_Name like '%OI%' then 'OI'
                  else ro.Carrier_Name end as Flag_Operadora

                ,Flag_Bio
                
                ,case when co.device = 'iphone' THEN 'Ios' Else 'Android' END AS Sist_Operacional

                ,date(cbk.Effective_Date) as Dt_Contestacao
                ,FORMAT_DATE("%Y%m",cbk.Effective_Date)as Safra_Contestacao
                ,DATE_DIFF(date(cbk.Effective_Date), date(cl.DataCriacao), DAY) as Temp_ContVsAbertura

                ,DATE_DIFF(date(cbk.Effective_Date),date(tranx.created_at), DAY) as Temp_ContVsTranx

            from `eai-datalake-data-sandbox.payment.payment_transaction`  tranx
            join `eai-datalake-data-sandbox.payment.payment`              tranx_d   on tranx_d.id = tranx.payment_id
            left join `eai-datalake-data-sandbox.core.orders`             ord       on ord.uuid = tranx_d.order_id
            join `eai-datalake-data-sandbox.partner_services.order_provider` op on ord.uuid = op.order_id
            join `eai-datalake-data-sandbox.partner_services.recharge_order` ro on op.id = ro.order_provider_id
            join (
                  with
                   base as (
                        select
                        distinct
                            cl.id
                            ,cl.uuid as  CustomerID
                            ,cl.full_name as Nome
                            ,cl.document as CPF
                            ,cl.email
                            ,en.street as Rua
                            ,en.neighborhood as Bairro
                            ,en.city as Cidade
                            ,en.state as UF
                            ,cl.created_at as DataCriacao
                            ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
                            ,case
                              when cl.trusted = 1 then 'Trusted'
                              else 'NaoTrusted' end as Flag_Trusted
                            ,RANK() OVER (PARTITION BY cl.uuid ORDER BY en.id desc) AS Rank_Ult_Atual
                            ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ph.number desc) AS Rank_Ult_Telef
                            ,CASE
                            WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
                            WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
                            WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
                            WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
                            WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
                            ELSE 'NAOINDENTIFICADO'
                            END AS RegiaoCliente
                            ,ph.area_code as DDD
                            ,ph.number as Telefone
                            ,ph.type as Tipo_Telefone
                            ,case when ev.CPF = cl.document  then 'BioValidada' else 'SemBio' end as Flag_Bio

                        FROM `eai-datalake-data-sandbox.core.customers`             cl
                        left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
                        left join `eai-datalake-data-sandbox.core.customer_phone`   id on id.customer_id = cl.id
                        left join `eai-datalake-data-sandbox.core.phone`            ph on id.phone_id = ph.id 
                        left join (                      
                          select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_4`
                          where Resultado = 'Aprovado'
                         ) ev on ev.CPF = cl.document 
                        where ph.type = 'MOBILE'
                        --and cl.document = '14501560886'
                        )
                        select * from base where Rank_Ult_Atual = 1 and Rank_Ult_Telef = 1 ) cl on cl.CustomerID = tranx_d.customer_id	
            join `eai-datalake-data-sandbox.core.customer_opt_in` co on cl.id = co.customer_id
            left join (
                        select 
                        distinct
                        * 
                        EXCEPT (Chargeback_Protection,Last_Updated)
                        from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake`  cbk
                        where 
                        cbk.Reason = 'Fraud'
                        and cbk.Status = 'Open'
                        and cbk.Kind = 'Chargeback' 
                        ---and Transaction_ID = '2fgp3q25'
                        ) cbk on tranx.gateway_id = cbk.Transaction_ID
            left join (   Select 
                                distinct
                                a.uuid as store_id
                                ,a.name as Nome_loja
                                ,a.document as CNPJ_CPF
                                ,a.type as tipo_loja
                                ,b.city as cidade
                                ,b.state as UF
                                ,left(b.latitude,7) as latitude
                                ,left(b.longitude,7) as longitude
                          FROM `eai-datalake-data-sandbox.backoffice.store` a
                          join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id ) locPost on locPost.store_id = ord.store_id
                          left join (   Select 
                                              distinct
                                              b.city as cidade
                                              ,b.state as UF
                                              ,left(b.latitude,7) as latitude
                                              ,left(b.longitude,7) as longitude
                                        FROM `eai-datalake-data-sandbox.backoffice.store` a
                                        join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id ) locTranx 
                                        on locTranx.latitude like left(ord.latitude,7) and locTranx.longitude like left(ord.longitude,7)
   
            WHERE 
            date(tranx.created_at) >= current_date - 80
            --FORMAT_DATETIME('%Y',tranx.created_at) = '2022'
            and tranx.status in ('AUTHORIZED','COMPLETED','SETTLEMENT','COMPLETE') --- 'PRE_AUTHORIZED','AUTHORIZED','CANCELLED_BY_GATEWAY','SETTLEMENT'
            and substring(tranx_d.order_code,1,STRPOS(tranx_d.order_code,'-')) = 'REC-'
            --and op.order_type = 'RECHARGE'
            --and tranx.`payment_method` = 'CREDIT_CARD'
            --and tranx_d.sales_channel = 'APP'
            --and cl.CPF = '03002422550'
            --and tranx_d.customer_id = 'CUS-f70c2062-bea8-4c35-8e9d-0826e0ab1475'
          ) select 
            distinct
                a.*
                ,case
                  when Telefone_Cliente = Telefone_Recarga then 'Titular' else 'OutraTitularidade' end as Flag_Operacao
                ,dppaypal.string_field_2 as Flag_Merchant_Account
            from Base_PayPal a  
            left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.TB_DE_PARA_PAYPAL_PEDIDO`  dppaypal on dppaypal.string_field_0 = a.order_code

/*

select
distinct
*
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Estudo_Recarga`
where code_tranx = 'REC-66953660'
order by 2 desc
*/
