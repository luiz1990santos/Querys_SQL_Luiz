/*
SELECT * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Zaig_Hitorico_2021_2023_V1` where data_cadastro in ('Oeste','BRA','75690-000','13455-401') or data_cadastro is null
;
*/






-- Ajuste dos campos errados com filtro coluna data_cadastro like '%BRA%' 

/*

create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico2` as
 select 
    esteira, 
    natural_person_id, 
    cpf, 
    nome, 
    email, 
    ddd, 
    numero, 
    nome_da_mae, 
    rua, 
    numero_9||bairro as numero_9, 
    cidade as bairro, 
    estado as cidade, 
    cep as estado, 
    pais as cep, 
    data_cadastro as pais, 
    decisao as data_cadastro, 
    razao as decisao, 
    indicators as razao, 
    session_id as indicators, 
    modelo_do_dispositivo as session_id, 
    plataforma as modelo_do_dispositivo,
    ip as plataforma,
    pais_do_ip as ip, 
    ip_tor as pais_do_ip,  
    gps_latitude as ip_tor,  
    gps_longitude as gps_latitude, 
    device_scan_date as gps_longitude, 
    tree_score as device_scan_date, 
    makrosystem_score as tree_score, 
    null as makrosystem_score 
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico` 
 where data_cadastro like '%BRA%' 
;

*/


-- Ajuste dos campos errados com filtro coluna data_cadastro = 'Oeste'

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico3`

/*

create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico3` as
 select 
    esteira, 
    natural_person_id, 
    cpf, 
    nome, 
    email, 
    ddd, 
    numero, 
    nome_da_mae, 
    rua, 
    numero_9, 
    bairro, 
    cidade, 
    decisao as estado, 
    razao as cep, 
    indicators as pais, 
    session_id as data_cadastro, 
    modelo_do_dispositivo as decisao, 
    plataforma as razao, 
    ip as indicators, 
    pais_do_ip as session_id, 
    ip_tor as modelo_do_dispositivo,
    gps_latitude as plataforma,
    gps_longitude as ip, 
    device_scan_date as pais_do_ip,  
    tree_score as ip_tor,  
    null as gps_latitude, 
    null as gps_longitude, 
    null as device_scan_date, 
    null as tree_score, 
    null as makrosystem_score 
 from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Zaig_Hitorico_2021_2023_V1` 
where data_cadastro in ('Oeste');
*/



-- Ajuste dos campos errados com filtro coluna data_cadastro data_cadastro = '13455-401' 

/*
create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico4` as
 select 
    esteira, 
    natural_person_id, 
    cpf, 
    nome, 
    email, 
    ddd, 
    numero, 
    nome_da_mae, 
    rua, 
    numero_9, 
    bairro, 
    cidade, 
    decisao as estado, 
    data_cadastro as cep, 
    decisao as pais, 
    razao as data_cadastro, 
    indicators as decisao, 
    session_id as razao, 
    modelo_do_dispositivo as indicators, 
    plataforma as session_id, 
    ip as modelo_do_dispositivo,
    pais_do_ip as plataforma,
    ip_tor as ip, 
    gps_latitude as pais_do_ip,  
    gps_longitude as ip_tor,  
    device_scan_date as gps_latitude, 
    tree_score as gps_longitude, 
    razao as device_scan_date, 
    makrosystem_score as tree_score, 
    null as makrosystem_score 
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Zaig_Hitorico_2021_2023_V1` 
where data_cadastro in ('13455-401');

*/


-- Ajuste dos campos errados com filtro coluna data_cadastro data_cadastro = '75690-000'

/*
create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico5` as
 select 
    esteira, 
    natural_person_id, 
    cpf, 
    nome, 
    email, 
    ddd, 
    numero, 
    nome_da_mae, 
    rua, 
    numero_9, 
    estado as bairro, 
    cep as cidade, 
    pais as estado, 
    data_cadastro as cep, 
    decisao as pais, 
    razao as data_cadastro, 
    indicators as decisao, 
    session_id as razao, 
    modelo_do_dispositivo as indicators, 
    plataforma as session_id, 
    ip as modelo_do_dispositivo,
    pais_do_ip as plataforma,
    ip_tor as ip, 
    gps_latitude as pais_do_ip,  
    gps_longitude as ip_tor,  
    device_scan_date as gps_latitude, 
    tree_score as gps_longitude, 
    razao as device_scan_date, 
    makrosystem_score as tree_score, 
    null as makrosystem_score 
from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Zaig_Hitorico_2021_2023_V1` 
where data_cadastro in ('75690-000');
*/


/*
create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico` as
  SELECT 
    * 
  FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Zaig_Hitorico_2021_2023_V1` 
  where data_cadastro not in ('Oeste','75690-000','13455-401')
  and data_cadastro is not null 
  and data_cadastro not like '%BRA%'
*/



--  select date(data_cadastro), count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico` group by 1 order by 1;

/*
insert into `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico` 
  select 
    esteira, 
    natural_person_id, 
    cpf, 
    nome, 
    email, 
    ddd, 
    numero, 
    nome_da_mae, 
    rua, 
    numero_9, 
    bairro, 
    cidade, 
    estado, 
    cep, 
    pais, 
    data_cadastro, 
    decisao, 
    razao, 
    indicators, 
    session_id, 
    modelo_do_dispositivo, 
    plataforma, 
    ip, 
    pais_do_ip, 
    ip_tor, 
    gps_latitude, 
    gps_longitude, 
    device_scan_date, 
    tree_score, 
    cast(makrosystem_score as string) as makrosystem_score
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico2`;
*/

/*
insert into `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico` 
  select 
    esteira, 
    natural_person_id, 
    cpf, 
    nome, 
    email, 
    ddd, 
    numero, 
    nome_da_mae, 
    rua, 
    numero_9, 
    bairro, 
    cidade, 
    estado, 
    cep, 
    pais, 
    data_cadastro, 
    decisao, 
    razao, 
    indicators, 
    session_id, 
    modelo_do_dispositivo, 
    plataforma, 
    ip, 
    pais_do_ip, 
    ip_tor, 
    cast(gps_latitude as string) as gps_latitude, 
    cast(gps_longitude as string) as gps_longitude, 
    cast(device_scan_date as string) as device_scan_date, 
    cast(tree_score as string) as tree_score,
    cast(makrosystem_score as string) as makrosystem_score
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico3`;
*/

/*
insert into `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico` 
  select 
    esteira, 
    natural_person_id, 
    cpf, 
    nome, 
    email, 
    ddd, 
    numero, 
    nome_da_mae, 
    rua, 
    numero_9, 
    bairro, 
    cidade, 
    estado, 
    cep, 
    pais, 
    data_cadastro, 
    decisao, 
    razao, 
    indicators, 
    session_id, 
    modelo_do_dispositivo, 
    plataforma, 
    ip, 
    pais_do_ip, 
    ip_tor, 
    cast(gps_latitude as string) as gps_latitude, 
    cast(gps_longitude as string) as gps_longitude, 
    cast(device_scan_date as string) as device_scan_date, 
    cast(tree_score as string) as tree_score,
    cast(makrosystem_score as string) as makrosystem_score
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico4`;
*/

/*
  insert into `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico` 
  select 
    esteira, 
    natural_person_id, 
    cpf, 
    nome, 
    email, 
    ddd, 
    numero, 
    nome_da_mae, 
    rua, 
    numero_9, 
    bairro, 
    cidade, 
    estado, 
    cep, 
    pais, 
    data_cadastro, 
    decisao, 
    razao, 
    indicators, 
    session_id, 
    modelo_do_dispositivo, 
    plataforma, 
    ip, 
    pais_do_ip, 
    ip_tor, 
    cast(gps_latitude as string) as gps_latitude, 
    cast(gps_longitude as string) as gps_longitude, 
    cast(device_scan_date as string) as device_scan_date, 
    cast(tree_score as string) as tree_score,
    cast(makrosystem_score as string) as makrosystem_score
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico5`;

  */



  insert into `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Zaig_Staging_Area`
  select 
    esteira, 
    natural_person_id, 
    cpf, 
    nome, 
    email, 
    cast(ddd as int64) as ddd, 
    cast(numero as int64) as numero, 
    nome_da_mae, 
    rua, 
    numero_9, 
    bairro, 
    cidade, 
    estado, 
    cep, 
    pais, 
    cast(data_cadastro as timestamp) as data_cadastro, 
    decisao, 
    razao, 
    indicators, 
    session_id, 
    modelo_do_dispositivo, 
    plataforma, 
    ip, 
    pais_do_ip, 
    cast(ip_tor as bool) as ip_tor, 
    cast(gps_latitude as float64) as gps_latitude, 
    cast(gps_longitude as float64) as gps_longitude, 
    cast(device_scan_date as timestamp) as device_scan_date, 
    cast(tree_score as int64) as tree_score,
    cast(makrosystem_score as string) as makrosystem_score
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico` 
  --where gps_longitude is not null;


-- ESTOU TENTANDO FAZER O UPDATE DOS DADOS QUE NAO ENTRAM 

 -- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico`  where device_scan_date like '%NULL%' 
  
  /*
  select tree_score, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico` 
  where tree_score like '%NULL%' 
  group by 1
  */
  
  /*
  update `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_zaig_Historico` 
  set tree_score = null
  where tree_score like '%NULL%' 
  */



