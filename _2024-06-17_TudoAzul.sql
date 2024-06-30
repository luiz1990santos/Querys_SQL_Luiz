select
distinct
Date(o.created_at) as Dt_Transacao
,o.uuid
,o.order_status
,pt.status
,o.order_value
,o.sales_channel
,pt.payment_method
,o.code 
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

where o.code like '%AZU%'
--payment_method = 'GOOGLE_PAY'
and date(o.created_at) >= '2024-06-17'
--and o.code like '%SMI%'