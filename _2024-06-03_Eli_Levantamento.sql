select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` 
where date(DataStatus) in (
'2023-08-16',
'2023-08-21',
'2023-08-23',
'2023-08-29',
'2023-09-05',
'2023-09-12',
'2023-09-22',
'2023-09-23',
'2023-09-25',
'2023-10-26',
'2023-10-30',
'2023-11-10',
'2023-12-07',
'2023-12-08'              ) 
and MotivoStatus = 'Fraude confirmada'
--group by 1,2,3
--order by 1
