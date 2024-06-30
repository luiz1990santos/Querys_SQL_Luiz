select 
  cus.uuid as customerID,
  tudo.Order_ID,
  tudo.Created_Datetime,
  ord.uuid,
  cus.document,
  cus.full_name,
  cus.status,
  tudo.Transaction_ID,
  tudo.Dec_Motor_PayPal,
  tudo.Status_Trans_PayPal,
  tudo.Status_Trans_Emissor,
  tudo.Customer_Email,
  cus.email,
  --ord.own_id, 
  --ord.pdv_token, 
  --ord.notification_id, 
  ord.code, 
  --ord.nsu, 
  ord.customer_id, 
  --ord.category_id, 
  --ord.store_id, 
  --ord.partner_id, 
  ord.order_value, 
  --ord.amount_tip, 
  --ord.discount, 
  --ord.cashback_percentage, 
  --ord.cashback_value,
  ord.order_status, 
  ord.sales_channel, 
  --ord.payment_id, 
  ord.expiration_time, 
  ord.created_at




from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_monit_TudoAzul` as tudo
left join `eai-datalake-data-sandbox.core.customers` as cus
on tudo.Customer_Email = cus.email
left join `eai-datalake-data-sandbox.core.orders` ord
on ord.uuid = tudo.Order_ID

where --Transaction_ID = 'dbycq913'
Dt_Tranx = '2024-03-19'


