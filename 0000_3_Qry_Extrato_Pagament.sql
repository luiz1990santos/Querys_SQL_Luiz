--Extrato Latam

/*
select max(Dt_Transacao),min(Dt_Transacao) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_Pagamento_Latam` 
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_Pagamento_Latam` 
*/
-- CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_Pagamento_Latam` AS 

INSERT INTO `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_Pagamento_Latam` 

with
 
base_pedidos as (
 
SELECT * FROM (
        SELECT 
        DISTINCT
 
        pe.DataPedido as Dt_Transacao
        ,'CASH-OUT' as type
        ,FORMAT_DATETIME("%Y%m",pe.DataPedido) as Safra_Tranx
        ,case
         when pe.SituacaoID = 2 or pe.situacaoID = 4 or pe.SituacaoID = 14 then 'Aprovado'
         when pe.SituacaoID = 5 or pe.SituacaoID = 1 then 'Cancelado' end as Situacao
        ,pr.CodigoExterno
        ,r.RedeOrigem as Origem
        ,pe.PaypalPaymentId as TransacaoId
        ,pe.PedidoID as Pedido
        ,pe.ValorReais
        ,RANK() OVER (PARTITION BY pe.PedidoID ORDER BY pe.DataPedido desc) AS Rank_trans
        ,pt.CPF as CPF_Cliente
        ,case
        when cl.CPF_Cliente = pt.CPF then 'KMV'else 'AntigoKMV' end as Flag_ClienteKMV
        ,pt.nome||''||pt.SobreNome as Nome_Cliente
        ,pt.Email as Email_Cliente
        ,forn.Descricao
        ,forn.NomeParceiro
 
 
  from `eai-datalake-data-sandbox.loyalty.tblPedidos` pe
  join `eai-datalake-data-sandbox.loyalty.tblProdutos` pr on pe.produtoid = pr.produtoid
  left join `eai-datalake-data-sandbox.loyalty.tblRedeOrigem` r on pe.RedeOrigemID = r.RedeOrigemId
  left join `eai-datalake-data-sandbox.loyalty.tblParticipantes`  pt on pt.ParticipanteID = pe.ParticipanteID
  join (SELECT * FROM `eai-datalake-data-sandbox.loyalty.tblProdutos`
        where NomeParceiro like '%Latam%'or NomeParceiro like '%LATAM%'and Inativo = false) forn on forn.ProdutoID = pr.ProdutoID
  left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` cl on cl.CPF_Cliente = pt.CPF
 
where
--date(pe.DataPedido) >= '2023-01-01' and date(pe.DataPedido) <= current_date
pe.DataPedido > (select max(Dt_Transacao) as Dt_Transacao from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_extrato_Pagamento_Latam` ) and 
pe.MeioPagamento = 'mundipagg'and 
pe.MeioPagamento is not null
order by DataPedido asc)
)--, base_pedidos_2 as (
select * from base_pedidos where Rank_trans = 1