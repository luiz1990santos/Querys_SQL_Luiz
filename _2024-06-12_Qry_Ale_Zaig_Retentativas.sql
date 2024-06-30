
create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_Monitoramento_Zaig_Retentativas` as
with base_zaig as (
SELECT 
  Distinct 
    esteira, 
    natural_person_id, 
    cpf, 
    data_cadastro, 
    rua, 
    numero_9,
    bairro, 
    cidade, 
    estado, 
    cep, 
    pais,
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
    data_device_scan, 
    tree_score, 
    score_makrosystem, 
    Cpf_Cliente as CPF_Zaig
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig`
), base_ultima_decisao as (
  SELECT *,
    RANK() OVER (PARTITION BY cpf ORDER BY data_cadastro desc) as ranking
  FROM base_zaig
), base_zaig_light as (
SELECT 
    CPF_Zaig, 
    CustomerID, 
    esteira, 
    Nome_Cliente,  
    StatusConta, 
    rua||numero_9||bairro||cidade||estado||cep||pais as Endereco_Completo, 
    Email, 
    DDD,
    DDD||estado as UF_DDD, 
    Telefone, 
    Dt_Abertura, 
    Safra_Abertura, 
    Faixa_Idade, 
    Flag_TempodeConta, 
    Flag_Email_NaoVal, 
    Flag_Celular_NaoVal, 
    Flag_Biometria,
    --Flag_Funcionario, 
    data_cadastro, 
    tree_score 
  FROM base_ultima_decisao AS ult
LEFT JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` as cus
ON ult.CPF_Zaig = cus.CPF_Cliente
WHERE
  ult.esteira = "Abastece Aí" 
  and ult.ranking = 1
  and cus.StatusConta = "MINIMUM_ACCOUNT"
  and date(ult.data_cadastro) >= CURRENT_DATE() - 1
  
), base_tentativas as ( 
  /*
  select
    a.Cpf_Cliente,
    case when a.Cpf_Cliente = b.CPF_Zaig then 'Tentativa' else 'Sem tentativa' end as Flag_Tentativas from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` as a
  left join base_zaig_light as b
  on a.Cpf_Cliente = b.CPF_Zaig
  */

  select
    Cpf_Cliente,
    case 
        when esteira = 'Abastece Aí' then 'Tentativa'
        else 'Sem tentativa'
    end as Flag_Tentativas 
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` 
  
), base_contagem_tentativas1 as( 
  select 
      Cpf_Cliente, 
      IF(Flag_Tentativas = 'Tentativa' , 1, 0) as Qtd_Tentativas
  from base_tentativas 
), base_contagem_tentativas2 as ( 
  select 
      Cpf_Cliente,
      sum(Qtd_Tentativas) as Qtd_Tentativas from base_contagem_tentativas1
        --where Cpf_Cliente = '60624663272'
group by 1
), base_consolidada as ( 
  select distinct light.*, tentat.Qtd_Tentativas  from base_zaig_light as light 
  left join base_contagem_tentativas2 as tentat
  on light.CPF_Zaig = tentat.CPF_Cliente
  where Qtd_Tentativas > 5
) select * from base_consolidada 
  -- where CPF_Zaig = '60624663272'



-- COMPARAR COM A QUERY DO ALE, TENTATIVAS A PARTIR DAS 5 E DA UNS 200 E POUCO, ESTOU COM PROBLEMA NO AGRUPAMENTO
-- USAR O CPF NO FILTRO COMO EXEMPLO TENTOU PASSAR 37 VEZES PARA FULL