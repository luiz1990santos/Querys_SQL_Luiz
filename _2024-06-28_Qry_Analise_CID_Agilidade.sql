CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.TB_Base_Avaliacao_CID` as

with base_CPFs_Agilidade as (

  SELECT 
    DISTINCT * 
  FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_CPFs_Agilidade` 

), base_CPFs_Agilidade2 as (

  SELECT DISTINCT *, LPAD(CAST(CPF_SQL AS STRING), 11, '0') AS cpf_completo from base_CPFs_Agilidade 
) 
select DISTINCT
    CustomerID, CPF_Cliente, Nome_Cliente, StatusConta, MotivoStatus, sub_classification, sub_classification_obs, DataStatus, MotivoBloqueio, ScoreZaig, Flag_Biometria, Flag_Funcionario, StatusFuncionario, TipoFuncionario, Dt_LoteMassivo, MotivoBloq_Massivo, Flag_Perfil 
from base_CPFs_Agilidade2 AS AGI
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` AS PER      
ON AGI.cpf_completo = PER.CPF_Cliente