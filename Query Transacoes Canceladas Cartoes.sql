
---------------------------------------------------------
-- QUERY TRANSAÇÕES CANCELADAS CARTÕES                  |
---------------------------------------------------------

-- 'CANCELLED_BY_GATEWAY','REVERSED_ERROR','ERROR','PRE_AUTHORIZED_ERROR','REVERSED_DENIED','REVERSED'


declare dtInicio, dtFim date;

set dtInicio = '2023-12-01';
set dtFim = '2023-12-31';


with base_cancelados_cartao as (
select 
    date(o.created_at)                                                                                     as DataTransacao
   ,o.uuid           
   ,o.id                                                                                                   as order_id
   ,o.order_status as Status    
   ,o.order_value as Valor        
   ,te.error_message as ErrorMessage
   ,case 
      when error_message = 'Declined - Call Issuer' then 'Recusado – emissor da chamada'
      when error_message = 'Call Issuer. Pick Up Card.' then 'Emissor de chamada. Pegue o cartão.' 
      when error_message = 'Set Up Error - Amount' then 'Erro de configuração - valor' 
      when error_message = 'Issuer or Cardholder has put a restriction on the card' then 'O emissor ou titular do cartão impôs uma restrição ao cartão'
      when error_message = 'Invalid Merchant Number' then 'Número de comerciante inválido'
      when error_message = 'Invalid Credit Card Number' then 'Número de cartão de crédito inválido'
      when error_message = 'Cannot Authorize at this time (Life cycle)' then 'Não é possível autorizar neste momento (ciclo de vida)'
      when error_message = 'Card Type Not Enabled' then 'Tipo de cartão não ativado'
      when error_message = 'PIN Try Exceeded' then 'Tentativa de PIN excedida'
      when error_message = 'Offline Issuer Declined' then 'Emissor off-line recusado'
      when error_message = 'Card Issuer Declined CVV' then 'Emissor do cartão recusou CVV'
      when error_message = 'Incorrect PIN' then 'Incorrect PIN'
      when error_message = 'Inconsistent Data' then 'Dados inconsistentes'
      when error_message = 'Declined' then 'Recusada'
      when error_message = 'Limit Exceeded' then 'Limite excedido'
      when error_message = 'Do Not Honor' then 'Não honrado'
      when error_message = 'Cannot Authorize at this time (Policy)' then 'Não é possível autorizar neste momento (política)'
      when error_message = 'Processor Network Unavailable - Try Again' then 'Rede do processador indisponível – tente novamente'
      when error_message = 'Insufficient Funds' then 'Fundos insuficientes'
      when error_message = 'Expired Card' then 'Cartão expirado'
      when error_message = 'Unavailable' then 'Indisponível(Recusado PayPal)'
      when error_message = 'Processor Declined' then 'Processador recusado'
      when error_message = 'Security Violation' then 'Violação de segurança'
      when error_message = 'Invalid Transaction Data' then 'Dados de transação inválidos'
      when error_message = 'Processor Declined - Fraud Suspected' then 'Processador recusado – suspeita de fraude'
      when error_message = 'Invalid Transaction' then 'Transação inválida'
      when error_message = 'Transaction Not Allowed' then 'Transação não permitida'                 
      else error_message
   end Mensagem_Motivo                                                              
   ,count(distinct case when pt.payment_method = 'CASH' then o.id end)                                     as Dinheiro
   ,count(distinct case when pt.payment_method = 'CREDIT_CARD' then o.id end)                              as CartaoCredito   
   ,count(distinct case when pt.payment_method = 'BALANCE' then o.id end)                                  as Saldo                    
   ,count(distinct case when pt.payment_method = 'DIGITAL_WALLET' then o.id end)                           as CarteiraPaypal
   ,count(distinct case when pt.payment_method = 'COUPON' then o.id end)                                   as Cupom      
   ,count(distinct case when pt.payment_method = 'DEBIT_CARD' then o.id end)                               as CartaoDebito   
from `eai-datalake-prd.payment.payment` p
-- join `eai-datalake-data-sandbox.core.customers` clt  
join `eai-datalake-data-sandbox.core.orders` o on p.order_id = o.uuid
join `eai-datalake-data-sandbox.core.items` i on o.id = i.order_id
join `eai-datalake-prd.payment.payment_transaction` pt on p.id = pt.payment_id
left join `eai-datalake-data-sandbox.payment.payment_transaction_error` te on o.uuid = te.order_id
where
   date(o.created_at)              between '2023-12-01' and '2023-12-17'
   and date(p.created_at)          between '2023-12-01' and '2023-12-17'
   and date(pt.created_at)         between '2023-12-01' and '2023-12-17'
   and o.order_status = 'CANCELED_BY_GATEWAY'
   and o.sales_channel = 'APP'
   --and o.customer_id = 6655081
group by 1,2,3,4,5,6
), Cliente as (
select
   cc.* 
from base_cancelados_cartao cc
)
select
   cl.DataTransacao
  ,Status
  ,Valor 
  ,Mensagem_Motivo
  ,case 
     when cl.Dinheiro = 1 and CartaoCredito = 0 and Saldo = 0 and CarteiraPaypal = 0 and Cupom = 0 and CartaoDebito = 0 then 'Dinheiro'  
     when cl.Dinheiro = 0 and CartaoCredito = 1 and Saldo = 0 and CarteiraPaypal = 0 and Cupom = 0 and CartaoDebito = 0 then 'Cartao Crédito'
     when cl.Dinheiro = 0 and CartaoCredito = 0 and Saldo = 1 and CarteiraPaypal = 0 and Cupom = 0 and CartaoDebito = 0 then 'Saldo'
     when cl.Dinheiro = 0 and CartaoCredito = 0 and Saldo = 0 and CarteiraPaypal = 1 and Cupom = 0 and CartaoDebito = 0 then 'Carteira Paypal'
     when cl.Dinheiro = 0 and CartaoCredito = 0 and Saldo = 1 and CarteiraPaypal = 1 and Cupom = 0 and CartaoDebito = 0 then 'Carteira Paypal + Saldo'
     when cl.Dinheiro = 0 and CartaoCredito = 0 and Saldo = 0 and CarteiraPaypal = 0 and Cupom = 1 and CartaoDebito = 0 then 'Cupom'
     when cl.Dinheiro = 0 and CartaoCredito = 0 and Saldo = 0 and CarteiraPaypal = 0 and Cupom = 0 and CartaoDebito = 1 then 'Cartão Débito'
     when cl.Dinheiro = 0 and CartaoCredito = 0 and Saldo = 0 and CarteiraPaypal = 1 and Cupom = 1 and CartaoDebito = 0 then 'Cupom + CarteiraPaypal'
     when cl.Dinheiro = 1 and CartaoCredito = 0 and Saldo = 1 and CarteiraPaypal = 0 and Cupom = 0 and CartaoDebito = 0 then 'Dinheiro + Saldo'
     when cl.Dinheiro = 1 and CartaoCredito = 0 and Saldo = 0 and CarteiraPaypal = 0 and Cupom = 1 and CartaoDebito = 0 then 'Dinheiro + Cupom'
     when cl.Dinheiro = 0 and CartaoCredito = 0 and Saldo = 1 and CarteiraPaypal = 0 and Cupom = 1 and CartaoDebito = 0 then 'Cupom + Saldo'
     when cl.Dinheiro = 0 and CartaoCredito = 0 and Saldo = 1 and CarteiraPaypal = 1 and Cupom = 1 and CartaoDebito = 0 then 'Cupom + Saldo + CarteiraPaypal'
     when cl.Dinheiro = 1 and CartaoCredito = 0 and Saldo = 1 and CarteiraPaypal = 0 and Cupom = 1 and CartaoDebito = 0 then 'Dinheiro + Cupom + Saldo'
     when cl.Dinheiro = 0 and CartaoCredito = 1 and Saldo = 1 and CarteiraPaypal = 0 and Cupom = 0 and CartaoDebito = 0 then 'Cartão Crédito + Saldo'
     when cl.Dinheiro = 0 and CartaoCredito = 1 and Saldo = 0 and CarteiraPaypal = 1 and Cupom = 0 and CartaoDebito = 0 then 'Cartão Crédito + CarteiraPaypal'
     when cl.Dinheiro = 0 and CartaoCredito = 1 and Saldo = 0 and CarteiraPaypal = 0 and Cupom = 1 and CartaoDebito = 0 then 'Cartão Crédito + Cupom'
     when cl.Dinheiro = 0 and CartaoCredito = 1 and Saldo = 1 and CarteiraPaypal = 0 and Cupom = 1 and CartaoDebito = 0 then 'Cartão Crédito + Cupom + Saldo'
     when cl.Dinheiro = 0 and CartaoCredito = 0 and Saldo = 1 and CarteiraPaypal = 0 and Cupom = 0 and CartaoDebito = 1 then 'Cartão Débito + Saldo'
     when cl.Dinheiro = 0 and CartaoCredito = 0 and Saldo = 0 and CarteiraPaypal = 0 and Cupom = 1 and CartaoDebito = 1 then 'Cartão Débito + Cupom'
     when cl.Dinheiro = 0 and CartaoCredito = 0 and Saldo = 1 and CarteiraPaypal = 0 and Cupom = 1 and CartaoDebito = 1 then 'Cartão Débito + Cupom + Saldo'       
   end TipoPagamento   
  ,cl.order_id 
from Cliente cl
;


select count(*), error_message from `eai-datalake-data-sandbox.payment.payment_transaction_error`
group by 2


select * from `eai-datalake-data-sandbox.core.customers` 
where id = 6655081