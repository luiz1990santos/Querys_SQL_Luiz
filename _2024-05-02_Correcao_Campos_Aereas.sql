select date(Created_Datetime),Risk_Decision, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Transacional_Aereas_PayPal_V2` 
group by 1,2
order by 1 desc


;
