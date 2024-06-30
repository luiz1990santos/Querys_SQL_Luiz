--select * from  `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_estudo_cartoes_tranx`;

create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_estudo_cartoes_tranx` as 
with base_clientes as (
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil`
where StatusConta = 'ACTIVE'
),base_Cartoes as (
select
cli.CPF_Cliente
,cli.CustomerID
,cli.StatusConta
,card.last_four_digits as qtd_Tentativas
,bin.Emissor_do_Banco as qtd_banco
,case 
    when card.status = 'EXCLUDED' then last_four_digits
    else null
end as qtd_excluidos
,case 
    when card.status = 'FAILED' then last_four_digits
    else null
end as qtd_falha_cadastro
,case 
    when card.status in ('PROCESSOR_DECLINED','GATEWAY_REJECTED') then last_four_digits
    else null
end as qtd_negados
,case 
    when card.status = 'VERIFIED' then last_four_digits
    else null
end as qtd_cadastrados
,card.document as qtd_cliente

from base_clientes as cli
left join `eai-datalake-data-sandbox.payment.customer_card` card
on cli.CPF_Cliente = card.document
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos`  bin 
on CAST(bin.BIN AS STRING) = card.bin


), base_cartoes_consolidado as (
select 
distinct
CPF_Cliente
,CustomerID
,StatusConta
,count(distinct qtd_Tentativas) as qtd_Tentativas
,count(distinct qtd_falha_cadastro) as qtd_falha_cadastro
,count(distinct qtd_excluidos) as qtd_excluidos
,count(distinct qtd_negados) as qtd_negados
,count(distinct qtd_banco) as qtd_banco
,count(distinct qtd_cadastrados) as qtd_cadastrados
,count(distinct qtd_cliente) as  qtd_cliente
from base_Cartoes
group by 1,2,3
--order by 4 desc
), transacoes_cartoes as (
SELECT 
distinct
    pt.payment_method as MetodoPagamento,
    pt.status as Status,
    pt.transaction_value as Valor,
    b.order_id as Pedido,
    b.customer_id as CustomerID,
    b.order_code as TipoPedido,
    b.status as StatusPagamento,
    b.sales_channel as CanalVenda,
    ord.order_status as StatusPedido,
    payment_instrument.uuid as IdCartao,
    card.last_four_digits as FinalCartao,
    card.bin as binCard,
    card.card_type as TipoCartao,
    card.status as StatusCartao,
    ban.Emissor_do_Banco




FROM `eai-datalake-data-sandbox.payment.payment_transaction`                                          pt
join `eai-datalake-data-sandbox.payment.payment`                                                      b     on b.id = pt.payment_id
join `eai-datalake-data-sandbox.core.orders`                                                            ord   on ord.uuid = b.order_id
join `eai-datalake-data-sandbox.payment.payment_instrument`                              payment_instrument   on pt.payment_instrument_id = payment_instrument.id
join `eai-datalake-data-sandbox.payment.customer_card`                                                 card   on payment_instrument.uuid = card.uuid
join (select distinct * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos` )     ban   on cast(ban.BIN as string) = card.bin


WHERE 
pt.payment_method in ('CREDIT_CARD','DEBIT_CARD','DIGITAL_WALLET','GOOGLE_PAY')
and date (pt.created_at) between '2024-01-01' and '2024-04-30'
and pt.status in ('AUTHORIZED')
and pt.transaction_value > 0
-- and customer_id ='''CUS-258b2108-056b-410b-9d99-d5f8da483cc1'
-- and CustomerID
--and cl.document = '22768693804'
--and pt.gateway_id = '07vfvncg'
--limit 10000
), transacoes_cartoes_consolidada as (
select 
CustomerID
,sum(Valor) as TPV_cartoes
,count(distinct FinalCartao) as qtd_cartoes_usados
,count(distinct Pedido) as qtd_transacoes
from transacoes_cartoes
group by 1
) 
select
    distinct
    bcc.CPF_Cliente
    ,bcc.CustomerID
    ,bcc.qtd_Tentativas
    ,bcc.qtd_falha_cadastro
    ,bcc.qtd_excluidos
    ,bcc.qtd_negados
    ,bcc.qtd_banco
    ,bcc.qtd_cadastrados
    ,bcc.qtd_cliente
    ,tcc.qtd_cartoes_usados
    ,tcc.qtd_Transacoes
    ,tcc.TPV_cartoes
    ,case 
        when tcc.qtd_cartoes_usados <= 3 then '01 - Até 3 cartões usados'
        when tcc.qtd_cartoes_usados <= 5 then '02 - Até 5 cartões usados'
        when tcc.qtd_cartoes_usados <= 10 then '03 - Até 10 cartões usados'
        when tcc.qtd_cartoes_usados <= 15 then '04 - Até 15 cartões usados'
        when tcc.qtd_cartoes_usados > 15 then '05 - Mais de 15 cartões usados'
        else 'NC'
    end as Flag_cartoes_usados
    ,per.UF_Cliente
    ,per.Flag_TempodeConta
    --,per.ScoreZaig
    ,per.Flag_Risco_CBK
from base_cartoes_consolidado as bcc
left join transacoes_cartoes_consolidada as tcc 
on bcc.CustomerID = tcc.CustomerID
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` as per
on per.CustomerID = bcc.CustomerID
where -- qtd_cadastrados > 0 and
qtd_cartoes_usados > 0
order by 10 desc
;








