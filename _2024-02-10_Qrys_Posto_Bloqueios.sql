
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_Postos` where data_update = '2024-05-09'
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_Postos_Consolidado`




-- select max(DATA_UPDATE) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_Postos_Consolidado` 

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_Postos_Consolidado` as
WITH BASE_DOSSIE AS (
     SELECT
     DATA_DOSSIE 	
     ,NOME_DO_POSTO 
     ,CNPJ
     ,STORE_ID 
     ,CASE
     WHEN DINHEIRO_ = true AND CART__O_CREDITO = true AND CART__O_DEBITO = true AND QRCODE_PIX_ = true AND CUPOM_ = true AND CARTEIRA_DIGITAL = true AND SALDO = true 
     THEN 'TODOS OS MEIOS DE PAGAMENTO BLOQUEADOS' 
     WHEN DINHEIRO_ = true AND CART__O_CREDITO = false AND CART__O_DEBITO = false AND QRCODE_PIX_ = false AND CUPOM_ = false AND CARTEIRA_DIGITAL = false AND SALDO = false 
     THEN 'PAGAMENTO EM DINHEIRO BLOQUEADO' 
     WHEN DINHEIRO_ = true AND CART__O_CREDITO = true AND CART__O_DEBITO = false AND QRCODE_PIX_ = false AND CUPOM_ = false AND CARTEIRA_DIGITAL = false AND SALDO = false 
     THEN 'PAGAMENTO EM DINHEIRO E CRÉDITO BLOQUEADOS' 
     WHEN DINHEIRO_ = true AND CART__O_CREDITO = true AND CART__O_DEBITO = true AND QRCODE_PIX_ = false AND CUPOM_ = false AND CARTEIRA_DIGITAL = false AND SALDO = false 
     THEN 'PAGAMENTO EM DINHEIRO, CRÉDITO E DÉBITO BLOQUEADOS' 
     WHEN DINHEIRO_ = true AND CART__O_CREDITO = true AND CART__O_DEBITO = true AND QRCODE_PIX_ = true AND CUPOM_ = false AND CARTEIRA_DIGITAL = false AND SALDO = false 
     THEN 'PAGAMENTO EM DINHEIRO, CRÉDITO, DÉBITO E QRCODE BLOQUEADOS' 
     WHEN DINHEIRO_ = true AND CART__O_CREDITO = true AND CART__O_DEBITO = true AND QRCODE_PIX_ = true AND CUPOM_ = true AND CARTEIRA_DIGITAL = false AND SALDO = false 
     THEN 'PAGAMENTO EM DINHEIRO_, CRÉDITO, DÉBITO, QRCODE E CUPOM_ BLOQUEADOS'  
     WHEN DINHEIRO_ = true AND CART__O_CREDITO = true AND CART__O_DEBITO = true AND QRCODE_PIX_ = true AND CUPOM_ = true AND CARTEIRA_DIGITAL = true AND SALDO = false 
     THEN 'PAGAMENTO EM DINHEIRO, CRÉDITO, DÉBITO, QRCODE, CUPOM_ E CARTEIRA BLOQUEADOS' 
     WHEN DINHEIRO_ = false AND CART__O_CREDITO = true AND CART__O_DEBITO = false AND QRCODE_PIX_ = false AND CUPOM_ = false AND CARTEIRA_DIGITAL = false AND SALDO = false 
     THEN 'PAGAMENTO EM CARTÃO DE CRÉDITO BLOQUEADO' 
     WHEN DINHEIRO_ = false AND CART__O_CREDITO = true AND CART__O_DEBITO = true AND QRCODE_PIX_ = false AND CUPOM_ = false AND CARTEIRA_DIGITAL = false AND SALDO = false 
     THEN 'PAGAMENTO EM CRÉDITO E DÉBITO BLOQUEADOS' 
     WHEN DINHEIRO_ = false AND CART__O_CREDITO = true AND CART__O_DEBITO = true AND QRCODE_PIX_ = true AND CUPOM_ = false AND CARTEIRA_DIGITAL = false AND SALDO = false 
     THEN 'PAGAMENTO EM CRÉDITO, DÉBITO E QRCODE BLOQUEADOS' 
     WHEN DINHEIRO_ = false AND CART__O_CREDITO = true AND CART__O_DEBITO = true AND QRCODE_PIX_ = true AND CUPOM_ = true AND CARTEIRA_DIGITAL = false AND SALDO = false 
     THEN 'PAGAMENTO EM CRÉDITO, DÉBITO, QRCODE E CUPOM_ BLOQUEADOS' 
     WHEN DINHEIRO_ = false AND CART__O_CREDITO = true AND CART__O_DEBITO = true AND QRCODE_PIX_ = true AND CUPOM_ = true AND CARTEIRA_DIGITAL = true AND SALDO = false 
     THEN 'PAGAMENTO EM CRÉDITO, DÉBITO, QRCODE, CUPOM_ E CARTEIRA BLOQUEADOS' 
     WHEN DINHEIRO_ = false AND CART__O_CREDITO = true AND CART__O_DEBITO = true AND QRCODE_PIX_ = true AND CUPOM_ = true AND CARTEIRA_DIGITAL = true AND SALDO = true 
     THEN 'PAGAMENTO EM CRÉDITO, DÉBITO, QRCODE, CUPOM_, CARTEIRA 2 E SALDO BLOQUEADOS' 
     WHEN DINHEIRO_ = false AND CART__O_CREDITO = false AND CART__O_DEBITO = true AND QRCODE_PIX_ = false AND CUPOM_ = false AND CARTEIRA_DIGITAL = false AND SALDO = false 
     THEN 'PAGAMENTO EM CARTÃO DE DÉBITO BLOQUEADO' 
     WHEN DINHEIRO_ = false AND CART__O_CREDITO = false AND CART__O_DEBITO = false AND QRCODE_PIX_ = true AND CUPOM_ = false AND CARTEIRA_DIGITAL = false AND SALDO = false 
     THEN 'PAGAMENTO EM QRCODE BLOQUEADO' 
     WHEN DINHEIRO_ = false AND CART__O_CREDITO = false AND CART__O_DEBITO = false AND QRCODE_PIX_ = false AND CUPOM_ = true AND CARTEIRA_DIGITAL = false AND SALDO = false 
     THEN 'PAGAMENTO EM CUPOM BLOQUEADO' 
     WHEN DINHEIRO_ = false AND CART__O_CREDITO = false AND CART__O_DEBITO = false AND QRCODE_PIX_ = false AND CUPOM_ = false AND CARTEIRA_DIGITAL = true AND SALDO = false 
     THEN 'PAGAMENTO EM CARTEIRA DIGITAL BLOQUEADO' 
     WHEN DINHEIRO_ = false AND CART__O_CREDITO = false AND CART__O_DEBITO = false AND QRCODE_PIX_ = false AND CUPOM_ = false AND CARTEIRA_DIGITAL = false AND SALDO = true 
     THEN 'PAGAMENTO EM SALDO BLOQUEADO' 
     WHEN DINHEIRO_ = false AND CART__O_CREDITO = false AND CART__O_DEBITO = false AND QRCODE_PIX_ = false AND CUPOM_ = false AND CARTEIRA_DIGITAL = false AND SALDO = false 
     THEN 'NENHUM MÉTODO BLOQUEADO'
     END FLAG_MOD_PAG_BLOQ
     ,CASE 
          WHEN BLOQUEADO_ = true THEN 'Sim'
          ELSE 'Não'
     END FLAG_BLOQUEADO
     ,SITUA____O_BLOQUEIO as SITUACAO_BLOQUEIO
     ,DATA_BLOQUEIO 
     ,CASE
    WHEN CHARGEBACK = true AND CONLUIO_COM_VIP = false AND ABUSO_CASHBACK_ = false AND FORA_DO_POSTO_ = false THEN 'Chargeback'
     WHEN CHARGEBACK = true AND CONLUIO_COM_VIP = false AND ABUSO_CASHBACK_ = false AND FORA_DO_POSTO_ = true THEN 'Chargeback e Transações fora do posto'
     WHEN CHARGEBACK = true AND CONLUIO_COM_VIP = false AND ABUSO_CASHBACK_ = true AND FORA_DO_POSTO_ = false THEN 'Chargeback e Abuso de Cashback'
     WHEN CHARGEBACK = true AND CONLUIO_COM_VIP = false AND ABUSO_CASHBACK_ = true AND FORA_DO_POSTO_ = true THEN 'Chargeback, Abuso de Cashback e Transações fora do posto'
     WHEN CHARGEBACK = true AND CONLUIO_COM_VIP = true AND ABUSO_CASHBACK_ = false AND FORA_DO_POSTO_ = false THEN 'Chargeback e Conluio com VIP'
     WHEN CHARGEBACK = true AND CONLUIO_COM_VIP = true AND ABUSO_CASHBACK_ = false AND FORA_DO_POSTO_ = true THEN 'Chargeback, Conluio com VIP e Transações fora do posto'
     WHEN CHARGEBACK = true AND CONLUIO_COM_VIP = true AND ABUSO_CASHBACK_ = true AND FORA_DO_POSTO_ = false THEN 'Chargeback, Conluio com VIP e Abuso de Cashback'
     WHEN CHARGEBACK = true AND CONLUIO_COM_VIP = true AND ABUSO_CASHBACK_ = true AND FORA_DO_POSTO_ = true THEN 'Chargeback, Conluio com VIP, Abuso de Cashback e Transações fora do posto'
     WHEN CHARGEBACK = false AND CONLUIO_COM_VIP = false AND ABUSO_CASHBACK_ = false AND FORA_DO_POSTO_ = false THEN 'Nenhuma das condições'
     WHEN CHARGEBACK = false AND CONLUIO_COM_VIP = false AND ABUSO_CASHBACK_ = false AND FORA_DO_POSTO_ = true THEN 'Transações fora do posto'
     WHEN CHARGEBACK = false AND CONLUIO_COM_VIP = false AND ABUSO_CASHBACK_ = true AND FORA_DO_POSTO_ = false THEN 'Abuso de Cashback'
     WHEN CHARGEBACK = false AND CONLUIO_COM_VIP = false AND ABUSO_CASHBACK_ = true AND FORA_DO_POSTO_ = true THEN 'Abuso de Cashback e Transações fora do posto'
     WHEN CHARGEBACK = false AND CONLUIO_COM_VIP = true AND ABUSO_CASHBACK_ = false AND FORA_DO_POSTO_ = false THEN 'Conluio com VIP'
     WHEN CHARGEBACK = false AND CONLUIO_COM_VIP = true AND ABUSO_CASHBACK_ = false AND FORA_DO_POSTO_ = true THEN 'Conluio com VIP e Transações fora do posto'
     WHEN CHARGEBACK = false AND CONLUIO_COM_VIP = true AND ABUSO_CASHBACK_ = true AND FORA_DO_POSTO_ = false THEN 'Conluio com VIP e Abuso de Cashback'
     WHEN CHARGEBACK = false AND CONLUIO_COM_VIP = true AND ABUSO_CASHBACK_ = true AND FORA_DO_POSTO_ = true THEN 'Conluio com VIP, Abuso de Cashback e Transações fora do posto'
     END FLAG_MOTIVO
     ,CASE 
          WHEN OUTROS_ = true THEN 'VER OBSERVAÇÃO'
          ELSE 'NENHUMA OBSERVAÇÃO'
     END FLAG_OUTROS_MOTIVOS
     ,OBSERVA____O AS OBSERVACAO
     ,DATA_UPDATE
     ,ROW_NUMBER() OVER(PARTITION BY COALESCE(STORE_ID) ORDER BY DATA_UPDATE DESC) AS RANK_UPDATE
     FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_Postos`
) SELECT * FROM BASE_DOSSIE 
WHERE RANK_UPDATE = 1
and DATA_DOSSIE is not null

/*
select SITUACAO_BLOQUEIO, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_Postos_Consolidado`
group by 1
*/


