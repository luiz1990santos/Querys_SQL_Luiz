
--======================================================================================
--> TABELA TANSAÇÕES TETO LIMITE UTIMOS 60 DIAS
--======================================================================================

-- select * FROM `eai-datalake-data-sandbox.payment.payment_transaction` WHERE date(created_at) >= current_date - 5
-- select * FROM `eai-datalake-data-sandbox.payment.payment` WHERE date(created_at) >= current_date - 5
-- select * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias`
-- drop TABLE analytics_prevencao_fraude.tb_temp_teto_faixa_vl
-- select min(created_at) as min,max(created_at) as max from`eai-datalake-data-sandbox.payment.payment_transaction` WHERE date(created_at) >= current_date - 0

CREATE OR REPLACE TABLE analytics_prevencao_fraude.tb_temp_teto_faixa_vl AS 

SELECT 
date (pt.created_at)as Dt_tranx,
pt.payment_method as Pagamento,

case
            when DATE_DIFF(date(current_date),date(pt.created_at), Month) = 0 then 'M0'
            when DATE_DIFF(date(current_date),date(pt.created_at), Month) = 1 then 'M-1'
            when DATE_DIFF(date(current_date),date(pt.created_at), Month) = 2 then 'M-2'
            when DATE_DIFF(date(current_date),date(pt.created_at), Month) = 3 then 'M-3'
            when DATE_DIFF(date(current_date),date(pt.created_at), Month) = 4 then 'M-4'
            else 'Outros' end as Flag_Filtro_Periodo,

Case 
		When pt.transaction_value <185 Then '0 a 185'
		When pt.transaction_value >=185 and pt.transaction_value <=190 Then '185 a 190'
		When pt.transaction_value > 190 and pt.transaction_value <=195 Then '191 a 195'
		When pt.transaction_value > 195 and pt.transaction_value <=200 Then '195 a 200'
		When pt.transaction_value > 200 and pt.transaction_value <=205 Then '200 a 205'
		When pt.transaction_value > 205 and pt.transaction_value <=210 Then '206 a 210'
		When pt.transaction_value > 210 and pt.transaction_value <=215 Then '211 a 215'
		When pt.transaction_value > 215 and pt.transaction_value <=220 Then '216 a 220'
		When pt.transaction_value > 220 and pt.transaction_value <=225 Then '221 a 225'
		When pt.transaction_value > 225 and pt.transaction_value <=230 Then '226 a 230'
		When pt.transaction_value > 230 and pt.transaction_value <=235 Then '231 a 235'
		When pt.transaction_value > 235 and pt.transaction_value <=240 Then '236 a 240'
		When pt.transaction_value > 240 and pt.transaction_value <=245 Then '241 a 245'
		When pt.transaction_value > 245 and pt.transaction_value <=250 Then '246 a 250'
		When pt.transaction_value > 250 and pt.transaction_value <=255 Then '251 a 255'
		When pt.transaction_value > 255 and pt.transaction_value <=260 Then '256 a 260'
		When pt.transaction_value > 260 and pt.transaction_value <=265 Then '261 a 265'
		When pt.transaction_value > 265 and pt.transaction_value <=270 Then '266 a 270'
		When pt.transaction_value > 270 and pt.transaction_value <=275 Then '271 a 275'
		When pt.transaction_value > 275 and pt.transaction_value <=280 Then '276 a 280'
		When pt.transaction_value > 280 and pt.transaction_value <=285 Then '281 a 285'
		When pt.transaction_value > 285 and pt.transaction_value <=290 Then '286 a 290'
		When pt.transaction_value > 290 and pt.transaction_value <=295 Then '291 a 295'
		When pt.transaction_value > 295 and pt.transaction_value <=300 Then '296 a 300'
		When pt.transaction_value > 300 and pt.transaction_value <=350 Then '301 a 350'
		When pt.transaction_value > 350                                Then '351>'
End as Intervalo_Valor_5,

Case 
		When pt.transaction_value >=0 		and pt.transaction_value <=20 	Then '000 a 20 '
		When pt.transaction_value > 20 	and pt.transaction_value <=40 	Then '021 a 40 '
		When pt.transaction_value > 40 	and pt.transaction_value <=60 	Then '041 a 60 '
		When pt.transaction_value > 60 	and pt.transaction_value <=80 	Then '061 a 80 '
		When pt.transaction_value > 80 	and pt.transaction_value <=100 	Then '081 a 100'
		When pt.transaction_value > 100 	and pt.transaction_value <=120 	Then '101 a 120'
		When pt.transaction_value > 120 	and pt.transaction_value <=140 	Then '121 a 140'
		When pt.transaction_value > 140 	and pt.transaction_value <=160 	Then '141 a 160'
		When pt.transaction_value > 160 	and pt.transaction_value <=180 	Then '161 a 180'
		When pt.transaction_value > 180 	and pt.transaction_value <=200 	Then '181 a 200'
		When pt.transaction_value > 200 	and pt.transaction_value <=220 	Then '201 a 220'
		When pt.transaction_value > 220 	and pt.transaction_value <=240 	Then '221 a 240'
		When pt.transaction_value > 240 	and pt.transaction_value <=260 	Then '241 a 260'
		When pt.transaction_value > 260 	and pt.transaction_value <=280 	Then '261 a 280'
		When pt.transaction_value > 280 	and pt.transaction_value <300 	Then '281 a 299'
		When pt.transaction_value = 300 	Then '300'
		When pt.transaction_value > 300 	and pt.transaction_value <=320 	Then '301 a 320'
		When pt.transaction_value > 320 								 	Then '321>' 
End as Intervalo_Valor_20,      

Case 
		When pt.transaction_value >=195 	and pt.transaction_value <=200 	Then '01 Comportamento - 195-200'
		When pt.transaction_value >= 245 	and pt.transaction_value <=250 	Then '02 Comportamento - 245-250'
		When pt.transaction_value >= 295 	and pt.transaction_value <=300 	Then '03 Comportamento - 295-300'
		When pt.transaction_value >= 301 	and pt.transaction_value <=350 	Then '03 Comportamento - 301-350'
Else '04 Outros' End as Intervalo_Valor_Comportamento,   

case 
when ub.customer_id = b.customer_id then 'ClienteUber' 
when vip.customer_id = b.customer_id then 'Vip'
else 'ClienteUrbano' end as Flag_Cliente,
 
count(pt.id) as Quantidade,

sum(pt.transaction_value) as Montante

FROM `eai-datalake-data-sandbox.payment.payment_transaction` as pt
join `eai-datalake-data-sandbox.payment.payment` b on b.id = pt.payment_id
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias` ub on ub.customer_id = b.customer_id
left join (select distinct CPF, c.uuid as customer_id
					from (select distinct * from `eai-datalake-data-sandbox.loyalty.tblParticipantes` 
					where
						Vip is not null 
						and Inativo = false)		vip
					join (select * from`eai-datalake-data-sandbox.core.customers`  ) c on vip.CPF = c.document) vip on vip.customer_id = b.customer_id

WHERE 
date(pt.created_at) >= current_date - 90
--pt.created_at >= '2022-04-03' AND pt.created_at <= '2022-04-10'
AND pt.payment_method in ('CASH','CREDIT_CARD')
AND pt.status in ('AUTHORIZED', 'COMPLETED')
GROUP BY 1,2,3,4,5,6,7


-- select * from analytics_prevencao_fraude.tb_temp_teto_faixa_vl