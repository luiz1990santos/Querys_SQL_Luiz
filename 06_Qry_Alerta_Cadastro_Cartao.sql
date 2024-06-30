--======================================================================================
--> MONITORAMENTO ALERTA CADASTRO CARTÃO PLATAFORMA  MES - 3 DIAS
--======================================================================================
-- select min(created_at) min_, max(created_at) max_ from `eai-datalake-data-sandbox.payment.customer_card`
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_alerta_card`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_alerta_card` AS 

select
date (card.created_at)as Dt_Cadastro
,cl.uuid as CustumerID
,cl.status as Status_Conta
,FORMAT_DATETIME("%Y-%m",cl.created_at) as Safra_Abertura
,cl.full_name as Nome_Cliente
,card.document
,card.status
,case 
when card.status = 'VERIFIED' then 'Cadastrado'
when card.status = 'EXCLUDED' then 'Excluido'
when card.status = 'PROCESSOR_DECLINED' then 'Negado Emissor'
when card.status = 'FAILED' then 'Erro'
when card.status = 'GATEWAY_REJECTED' then 'Negado PayPal'
else 'NA' end as Flag_Status
,card.bin
,bin.Emissor_do_Banco
,bin.Sub_marca
,bin.Tipo_de_Card
,last_four_digits
,count(distinct card.id) as qtd_Tetativas
,count(distinct last_four_digits) as qtd_cartao
,count(distinct card.document) as qtd_cliente

from `eai-datalake-data-sandbox.payment.customer_card` card
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos`  bin on CAST(bin.BIN AS STRING) = card.bin
join `eai-datalake-data-sandbox.core.customers`                                 cl      on cl.document = card.document
where   
date(card.created_at) >= current_date - 3
--and cl.uuid = 'CUS-216d32e7-7ba4-4b43-84d1-701bcc9b2c8e'

group by 1,2,3,4,5,6,7,8,9,10,11,12,13

;

------- Recriar sempre a tabelas para não expirar a tabela no DataLaker

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos` AS 
select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos`

;

------- Evolução cadastro cartão na base

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_evolucao_CartaoBase` AS 

with

base as (
                  select
                        date (card.created_at)as Dt_Cadastro
                        ,FORMAT_DATETIME("%Y-%m",card.created_at) as Safra_Cadastro
                        ,cl.uuid as CustumerID
                        ,cl.status as Status_Conta
                        ,cl.full_name as Nome_Cliente
                        ,card.document
                        ,card.status
                        ,case 
                              when card.status = 'VERIFIED' then 'Cadastrado'
                              when card.status = 'EXCLUDED' then 'Excluido'
                              when card.status = 'PROCESSOR_DECLINED' then 'NegadoEmissor'
                              when card.status = 'FAILED' then 'NegadoErro'
                              when card.status = 'GATEWAY_REJECTED' then 'NegadoPayPal'
                        else 'NA' end as Flag_Status
                        ,card.bin
                        ,bin.Emissor_do_Banco
                        ,bin.Sub_marca
                        ,bin.Tipo_de_Card
                        ,last_four_digits
                        ,count(distinct card.id) as qtd_Tetativas
                        ,count(distinct last_four_digits) as qtd_cartao
                        ,count(distinct card.document) as qtd_cliente

                  from `eai-datalake-data-sandbox.payment.customer_card` card
                  left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos`  bin on CAST(bin.BIN AS STRING) = card.bin
                  join `eai-datalake-data-sandbox.core.customers`                                 cl      on cl.document = card.document
                  where   date(card.created_at) >= current_date - 180

                  group by 1,2,3,4,5,6,7,8,9,10,11,12,13
                  )
                  
                  SELECT

                        Safra_Cadastro
                        ,Flag_Status
                        ,Status_Conta
                        ,Emissor_do_Banco
                        ,Sub_marca
                        ,Tipo_de_Card
                        ,case when Flag_Status = 'Cadastrado' then count(distinct last_four_digits) end as qtd_cadastrado
                        ,case when Flag_Status = 'Excluido' then count(distinct last_four_digits) end  as qtd_excluido
                        ,case when Flag_Status = 'NegadoEmissor' then count(distinct last_four_digits) end as qtd_Negado_Emissor
                        ,case when Flag_Status = 'NegadoErro' then count(distinct last_four_digits) end as qtd_Negado_Erro
                        ,case when Flag_Status = 'NegadoPayPal' then count(distinct last_four_digits) end as qtd_Negado_PayPal
                        ,case when Flag_Status in ('NegadoEmissor','NegadoErro','NegadoPayPal') then count(distinct last_four_digits) end as qtd_Negado
                        ,case when Flag_Status in ('Cadastrado','Excluido','NegadoEmissor','NegadoErro','NegadoPayPal') then count(distinct last_four_digits) end as qtd_Cartao
                        ,count(qtd_Tetativas) as qtd_Tetativas
                        ,count(distinct CustumerID) as Qtd_Cliente
                  from base
                  group by 1,2,3,4,5,6
                  order by 1 desc







/*

-- pesquisar clientes - especificos

select 
distinct

last_four_digits as ult_4Dig
,date (card.created_at)as Dt_Cadastro
,card.document as CPF
,cl.uuid as CustumerID
,date(cl.created_at) as Dt_Conta
,cl.status as Status_Conta
,cl.full_name as Nome_Cliente
,bin.Emissor_do_Banco
,bin.Sub_marca
,bin.Tipo_de_Card
,case 
when card.status = 'VERIFIED' then 'Cadastrado'
when card.status = 'EXCLUDED' then 'Excluido'
when card.status = 'PROCESSOR_DECLINED' then 'Negado Emissor'
when card.status = 'FAILED' then 'Erro'
when card.status = 'GATEWAY_REJECTED' then 'Negado PayPal'
else 'NA' end as Flag_Status

from `eai-datalake-data-sandbox.payment.customer_card`                            card
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos`    bin     on CAST(bin.BIN AS STRING) = card.bin
left join `eai-datalake-data-sandbox.core.customers`                              cl      on cl.document = card.document
where
cl.document in ('31001127846')
--last_four_digits in ('5447','3183','6868','4037')

*/