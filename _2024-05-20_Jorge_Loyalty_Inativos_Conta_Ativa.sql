

/*

  !!!  LEVANTAMENTO DA BASE DE CLIENTES COM CargoID <> 1  E CargoID <> 1  COM BIOMETRIA


*/


-- 81050402987

-- idparticipante 3207011
with base_loyalty_KMV as (
  SELECT distinct 
  legado.ParticipanteID,
  perfil.CustomerID, 
  perfil.CPF_Cliente, 
  perfil.Nome_Cliente, 
  perfil.Dt_Nascimento,
  perfil.Email, 
  --perfil.DDD, 
  --perfil.Telefone,
  --perfil.TelefoneTipo,   
  perfil.StatusConta, 
  legado.Inativo as Inativo_Legado,
  date(legado.DataCadastro) as DataCadastro_Loyalty,
  perfil.Dt_Abertura as DataCadastro_KMV,
  perfil.Flag_TempodeConta, 
  perfil.RiskAnalysis, 
  perfil.Flag_Biometria,
  -- perfil.Flag_Perfil,
  legado.Observacao
  from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` as perfil
  -- FROM `eai-datalake-data-sandbox.loyalty.tblParticipantes` as legado
  left join `eai-datalake-data-sandbox.loyalty.tblParticipantes` as legado
on legado.CPF = perfil.CPF_Cliente 
where Inativo = true and StatusConta = 'ACTIVE' and RiskAnalysis = 'APPROVED' and CargoID = 1 and MotivoBloqueio = 'Sem Bloqueio'
-- and Flag_Biometria = 'BioRejeitada'
-- and CPF_Cliente = '05286282911'
) -- select Flag_Biometria, count(*) as Volume from base_loyalty_KMV group by 1
select distinct * from base_loyalty_KMV

-- CUS-d65da079-3c3e-4d2e-bc7a-0750a3589ed6 28211966875 Maria Do Rosario


/*
select * from `eai-datalake-data-sandbox.loyalty.tblParticipantes`
where ParticipanteID = 11429653


select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`
where CPF_Cliente = '07796231695'
*/



