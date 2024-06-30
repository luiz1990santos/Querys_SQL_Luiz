with
base as (
SELECT 
T.numerodocumento as  DOCUMENTO,                     -- CPF / CNPJ    
c.first_name as NomeCliente,
round(sum(T.valor),2) as SALDO                   -- SOMA DOS VALORES QUE COMPÕEM O SALDO
FROM `eai-datalake-data-sandbox.orbitall.tb_topaz` T
left join `eai-datalake-data-sandbox.core.customers` c on c.document = T.numerodocumento
where    t.status = 'MOVIMENTAÇÃO EXECUTADA'       -- STATUS NECESSARIO DE TRANSACOES CONCLUIDAS    
and T.data <= CURRENT_DATE()-1          -- FIXAR COM A DATA COM A QUAL SE PRETENDE ANALISAR O SALDO    
and length(T.numerodocumento) <= 11     -- REMOVER CASO QUEIRA TRAZER TAMBÉM CNPJ
group by 1,2
) select 
* 
from base 
where 
DOCUMENTO = '31001127846'
--SALDO = 590.00 
--and NomeCliente like 'Rodrigo'
order by 2