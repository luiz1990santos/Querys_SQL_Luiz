create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Base_Inspetoria_2` as 
  with 
  base_inspetoria as (
  select  
    NOME as nome_completo, 
    CLT, 
    TIPO_MODELO as Modelo, 
    E_MAIL_ as email, 
    CPF, 
    RUA as rua, 
    NUMERO_ as numero, 
    COMPLEMENTO as complemento, 
    Bairro_ as bairro, 
    CEP, 
    CIDADE as cidade, 
    ESTADO as UF, 
    ENDERE__O_COMPLETO as endereco_completo, 
    TELEFONE as telefone,
    LPAD(CAST(CPF AS STRING), 11, '0') AS cpf_completo from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Base_Inspetoria`
)select distinct * from base_inspetoria where cpf_completo <> '00000000000'
;