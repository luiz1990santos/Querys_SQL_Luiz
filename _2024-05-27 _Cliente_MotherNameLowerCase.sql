Select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`
where Flag_NomeMae_CaixaAlta = 'CaixaAltaNomeMae'
and Flag_Biometria = 'BioNaoCapturada'
and StatusConta = 'ACTIVE'
