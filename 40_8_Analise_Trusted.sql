
--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Mudanca_Trusted`;
--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Mudanca_Trusted_final`;

create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Mudanca_Trusted` as 
    select 
      distinct
      perfil.CustomerID,
      perfil.CPF_Cliente, 
      perfil.Nome_Cliente, 
      perfil.Dt_Nascimento, 
      perfil.Faixa_Idade, 
      perfil.StatusConta, 
      perfil.End_Cliente, 
      perfil.BairroCliente, 
      perfil.Cidade_Cliente, 
      perfil.UF_Cliente, 
      perfil.RegiaoCliente, 
      perfil.Email, 
      perfil.DDD, 
      perfil.Telefone,
      perfil.TelefoneTipo,
      perfil.Dt_Abertura, 
      perfil.Safra_Abertura, 
      perfil.Flag_TempodeConta, 
      perfil.Flag_TempoBloqueado, 
      perfil.Flag_Trusted, 
      perfil.RiskAnalysis, 
      perfil.MotivoStatus, 
      perfil.DataStatus, 
      perfil.Safra_Ev, 
      perfil.UsuarioStatus, 
      perfil.Rank_Ult_Atual, 
      perfil.MotivoBloqueio, 
      perfil.Flag_Email_NaoVal, 
      perfil.Flag_Celular_NaoVal, 
      perfil.Flag_NomeMae_CaixaAlta, 
      perfil.ScoreZaig, 
      perfil.Flag_Biometria, 
      perfil.Dt_LoteMassivo, 
      perfil.MotivoBloq_Massivo, 
      perfil.Flag_Risco_Limit_Vol, 
      perfil.Flag_Risco_Limit_Val, 
      perfil.Flag_Risco_CBK, 
      perfil.Flag_Tetativas, 
      perfil.Flag_Bancos, 
      perfil.Flag_Card, 
      perfil.Flag_APP, 
      perfil.Flag_ContaDigital, 
      perfil.Flag_Ativo, 
      perfil.Flag_Perfil
    from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` as perfil
    left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` as zaig
    on perfil.CPF_Cliente = zaig.Cpf_Cliente
    where zaig.tree_score <= 2 
    and date(zaig.data_cadastro) >= '2024-02-23'
    and zaig.decisao = 'automatically_approved'   
    and zaig.esteira =  'Abastece Aí'
    and Flag_Trusted = 'NoTrusted'
    and StatusConta = 'ACTIVE'
    and CustomerID NOT IN ('CUS-2727353d-490e-4907-b238-9ab93cf3641c')
;

create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Mudanca_Trusted_final` as 
    select 
        distinct 
        CustomerID,  
        'True' as Trusted

    from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Mudanca_Trusted` 
    
;
