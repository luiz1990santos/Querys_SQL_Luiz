--======================================================================================
--> TABELA CRIVO DE FRAUDE - ANALISE UTIMOS 60 DIAS
-- Analise de Clientes contas aprovadas
-- Analise Crivo de Contestação  - Contestações superior 70% do volume transacionado e contestados
-- Analise Crivo de Cadastro - critérios
--when Qtd_CPF_End=1 and Qtd_CPF_Celular=1 and Qtd_CPF_Email=1 and Qtd_CPF_ip=1 or Flag_Decisao_Makro = 'Aprovado' then 'RiscoBaixo'
--when Qtd_CPF_End<=100 and Qtd_CPF_Celular=1 and Qtd_CPF_Email=1 and Qtd_CPF_ip<=30 and Flag_Decisao_Makro <> 'Aprovado' then 'RiscoMedio'
--when Qtd_CPF_End<=200 and Qtd_CPF_Celular<=1 and Qtd_CPF_Email<=10 and Qtd_CPF_ip<=60 and Flag_Decisao_Makro <> 'Aprovado' then 'RiscoAlto'
--else 'RiscoAltissimo' end as Flag_RiscoCadastro

--======================================================================================

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Avaliacao_Risco_Fraude` AS 


with
 Base_Cadastro as (
              select
              distinct
              data_cadastro
              ,cl.uuid as Customer_ID
              ,Ev.status as StatusConta_ev
              ,cl.status as StatusConta
              ,CASE
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=10   THEN '01_00-10DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=30   THEN '02_11-30DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=60   THEN '03_31-60DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=90   THEN '04_61-90DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=180  THEN '05_91-180DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=364  THEN '06_180-1ANO'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) >=365  THEN '07_+1ANO'
              END AS Flag_TempodeConta
              ,case 
              when zaig.score_makrosystem <= 30 then 'Reprovado'
              when zaig.score_makrosystem <= 50 then 'Neutro'
              when zaig.score_makrosystem > 50 then 'Aprovado'
              else 'NA' end as Flag_Decisao_Makro
              ,zaig.score_makrosystem
              ,zaig.ScoreZaig
              ,case
              when zaig.ScoreZaig <= 7 then 'Aprovado'
              when zaig.ScoreZaig between 8 and 9 then 'Aprovado em situações especiais'
              when zaig.ScoreZaig >=10 then 'Negado'
              end as Flag_ScoreZaig

              ,zaig.Cpf_Cliente
              ,cl.full_name as NomeCliente
              ,zaig.email
              ,substring(zaig.email,1,STRPOS(zaig.email,'@')) as Desc_Email
              ,substring(zaig.email,STRPOS(zaig.email,'@'),25) as Provedor_Email
              ,zaig.Num_Celuar
              ,zaig.Uf_Celuar
              ,zaig.End_Num
              ,zaig.Bairro_Cep
              ,zaig.UF_Cep
              ,zaig.ip
              ,End_Completo
              ,gps_latitude
              ,gps_longitude

              from (select 
                     distinct
                     Cpf_Cliente
                     ,data_cadastro
                     ,natural_person_id
                     ,email
                     ,ddd||'-'||numero as Num_Celuar
                     ,estado ||'-'||ddd||'-'||numero as Uf_Celuar
                     ,rua ||','|| numero_9 as End_Num
                     ,bairro ||'-'|| cep as Bairro_Cep
                     ,estado ||'-'|| cep as UF_Cep
                     ,rua ||','|| numero_9 ||'-'|| bairro||'-'|| cep ||'-'|| estado as End_Completo
                     ,ip
                     ,score_makrosystem
                     ,tree_score as ScoreZaig
                     ,gps_latitude
                     ,gps_longitude
                     ,RANK() OVER (PARTITION BY cpf ORDER BY data_cadastro desc) AS Rank_Ult_Decisao
              from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` 
              where decisao = "automatically_approved"
              and date(data_cadastro) >= current_date - 80
              --and Cpf_Cliente = '26262673687' 
              order by 2 desc
              ) zaig
              join `eai-datalake-data-sandbox.core.customers`                cl      on cl.document = zaig.Cpf_Cliente and Rank_Ult_Decisao = 1
              left join `eai-datalake-data-sandbox.core.customer_event`      Ev      on ev.customer_id = cl.id
              where cl.status <> 'BLOCKED'
              --and  cl.uuid  = 'CUS-cd120e8e-ab2c-41c1-85c3-86ee5cd8ef3d'
), Qtd_Email_CPF as (
              select
              distinct
              Desc_Email
              ,count(distinct Customer_ID) as Qtd_CPF_Email
              from Base_Cadastro
              group by 1 order by 2 desc
), Qtd_End_CPF as (
              select
              distinct
              End_Completo
              ,count(distinct Customer_ID) as Qtd_CPF_End
              from Base_Cadastro
              group by 1 order by 2 desc
), Qtd_Celular_CPF as (
              select
              distinct
              Num_Celuar
              ,count(distinct Customer_ID) as Qtd_CPF_Celular
              from Base_Cadastro
              group by 1 order by 2 desc
), Qtd_ip_CPF as (
              select
              distinct
              ip
              ,count(distinct Customer_ID) as Qtd_CPF_ip
              from Base_Cadastro
              group by 1 order by 2 desc
), base_composicao_risco as (
              select
                     base.Customer_ID
                     ,base.Cpf_Cliente
                     ,base.StatusConta
                     ,Flag_TempodeConta
                     ,Qtd_CPF_End
                     ,Qtd_CPF_Celular
                     ,Qtd_CPF_Email
                     ,Qtd_CPF_ip
                     ,case
                     when Qtd_CPF_End=1 and Qtd_CPF_Celular=1 and Qtd_CPF_Email=1 and Qtd_CPF_ip<=30 and Flag_Decisao_Makro = 'Aprovado' and base.ScoreZaig <=7 then  '0_RiscoBaixíssimo'
                     when Qtd_CPF_End<=10 and Qtd_CPF_Celular=1 and Qtd_CPF_Email=1 and Qtd_CPF_ip<=30 and Flag_Decisao_Makro in ('Aprovado','Neutro') or ScoreZaig between 8 and 9 then '1_RiscoBaixo'
                     when Qtd_CPF_End<=100 and Qtd_CPF_Celular=1 and Qtd_CPF_Email>1 and Qtd_CPF_ip<=30 and Flag_Decisao_Makro in ('Aprovado','Neutro') or ScoreZaig between 8 and 9  then '2_RiscoMedio'
                     when Qtd_CPF_End<=200 and Qtd_CPF_Celular=1 and Qtd_CPF_Email<=10 and Qtd_CPF_ip<=60 and Flag_Decisao_Makro <> 'Aprovado' or base.ScoreZaig >=10 then '3_RiscoAlto'
                     when Qtd_CPF_End>=1 and Qtd_CPF_Celular>=1 and Qtd_CPF_Email>=1 and Qtd_CPF_ip>=1 and Flag_Decisao_Makro <> 'Aprovado' and base.ScoreZaig >=10 then '4_RiscoAltissimo'
                     else '5_RiscoCritico' end as Flag_RiscoCadastro

                     from Base_Cadastro      base
                     join Qtd_End_CPF        Qtd_End_CPF     on Qtd_End_CPF.End_Completo = base.End_Completo
                     join Qtd_Celular_CPF    Qtd_Celular_CPF on Qtd_Celular_CPF.Num_Celuar = base.Num_Celuar
                     join Qtd_Email_CPF      Qtd_Email_CPF   on Qtd_Email_CPF.Desc_Email = base.Desc_Email
                     join Qtd_ip_CPF         Qtd_ip_CPF      on Qtd_ip_CPF.ip = base.ip

),base_final_Risco_Cadastro as (
                     select
                     distinct
                     Customer_ID
                     ,Cpf_Cliente
                     ,StatusConta
                     ,Flag_RiscoCadastro
                     ,count(distinct Customer_ID) as qtd_Dist
              from base_composicao_risco
              group by 1,2,3,4
              order by 3 desc
), Saidas_PIX as (
              SELECT 
              distinct
                     pix_payer.payer_id as  CustomerID
                     ,cl.document as CPF_Origem
                     ,pix_payer.name as ClienteOrigem
                     ,pix.end_to_end_id
                     ,pix.status
                     ,pix.key_value
                     ,pix.key_type
                     ,pix.type
                     ,pix.scheduled_date
                     ,Cash_Transaction.flow
                     ,Cash_Transaction.amount/100 as Vl_Tranx
                     ,Cash_Transaction.description
                     ,Cash_Transaction.created_at AS DT_TRANX
                     ,pix_payee.name AS Favorecido
                     ,pix_payee.document AS CPF_Favorecido
                     ,case when pix_payee.document = cl.document then 'MesmaTitularidade' else 'OutraTitularidade' end as  Flag_Titularidade
                     ,pix_payee.bank_number 
                     ,pix_payee.agency_number as Agencia_Favorecido
                     ,pix_payee.account_number||"-"||pix_payee.account_check_number as Conta_Favorecido
                     ,pix_payee.bank_name as Banco_Favorecido
                     ,pix_payee.bank_ispb as Cod_Banco_ISPB
              
              FROM `eai-datalake-data-sandbox.cashback.pix_payer`               pix_payer
              LEFT JOIN `eai-datalake-data-sandbox.core.customers`              cl                ON pix_payer.payer_id = cl.uuid
              LEFT JOIN `eai-datalake-data-sandbox.cashback.pix`                pix               ON pix_payer.pix_id = pix.id 
              LEFT JOIN `eai-datalake-data-sandbox.cashback.cash_transaction`   Cash_Transaction  ON pix.cash_transaction_id = Cash_Transaction.id
              LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_payee`          pix_payee         ON pix.id = pix_payee.pix_id
              join base_final_Risco_Cadastro                                    base              on base.customer_id = cl.uuid
              WHERE 
              pix.`type` = 'CASH_OUT'
              AND pix.`status` = 'APPROVED'
), Saidas_PIX_2 as (
              select
              distinct
                     CustomerID
                     ,count(distinct CPF_Favorecido) as Qtd_Favorecido
                     ,count(distinct Cod_Banco_ISPB) as Qtd_BancoDesitino
                     ,sum(Vl_Tranx) as Vl_Pix_Env
              from Saidas_PIX
              group by 1
), Entrada_PIX as (
              select  
              distinct    
                     pix.id, 
                     pix.key_value as chave,
                     pix.key_type as tipo_chave,
                     pix.type as cash_in_out,
                     date(pix.created_at) as Dt_Recebimento,
                     pix.end_to_end_id as e2e,
                     pix_in_payer.document as Cpf_Cnpj_Origem,
                     pix_in_payer.name as NomeOrigem,
                     pix_in_payer.ispb as IPSB_Origem, 
                     pix_in_payer.bank_name as PSP_Origem,
                     Cash_Transaction.flow as Tipo_Tranx,
                     Cash_Transaction.amount/100 as Vl_Tranx,
                     Cash_Transaction.created_at as Dt_Tranx,
                     pix_in_payee.payee_id as customer_id,
                     pix_in_payee.name as NomeDestino

              from        `eai-datalake-data-sandbox.cashback.pix`                  pix
              inner join  `eai-datalake-data-sandbox.cashback.pix_in_payee`         pix_in_payee         on pix.id = pix_in_payee.pix_id
              inner join  `eai-datalake-data-sandbox.cashback.pix_in_payer`         pix_in_payer         on pix.id = pix_in_payer.pix_id
              inner join `eai-datalake-data-sandbox.cashback.cash_transaction`      Cash_Transaction     ON pix.cash_transaction_id = Cash_Transaction.id
              join base_final_Risco_Cadastro                                                 base                 on base.customer_id = pix_in_payee.payee_id
              where       
              pix.status = 'APPROVED'
              and pix.type = 'CASH_IN'
              and key_type in ('CNPJ', 'CPF', 'EMAIL', 'PHONE', 'EVP')
), Entrada_PIX_2 as (
              select
              distinct
                     customer_id
                     ,count(distinct Cpf_Cnpj_Origem) as Qtd_Origem
                     ,count(distinct IPSB_Origem) as Qtd_BancoOrigem
                     ,sum(Vl_Tranx) as Vl_Pix_Rec
              from Entrada_PIX
              group by 1
), Base_Outros_Postos as (
              SELECT 
              distinct
                     date (pt.created_at)as Dt_tranx
                     ,FORMAT_DATETIME("%Y-%m",pt.created_at) as Safra_Tranx
                     ,date(cl.created_at) as Dt_Abertura
                     ,EXTRACT(HOUR FROM pt.created_at)as Hr_tranx
                     ,pt.payment_method as Pagamento
                     ,b.store_id
                     ,ord.store_id as Cod_Posto
                     ,case 
                     when ub.customer_id = b.customer_id then 'ClienteUber' else 'ClienteUrbano' end as Flag_Cliente
                     ,pt.transaction_value as Vl_Tranx
                     ,b.customer_id
                     ,cl.document as CPF
                     ,case
                     when pt.payment_method = 'CREDIT_CARD' then pt.transaction_value
                     else 0 end as Pgto_Cartao
                                   ,case
                     when pt.payment_method = 'CASH' then pt.transaction_value
                     else 0 end as Pgto_Dinheiro

              FROM `eai-datalake-data-sandbox.payment.payment_transaction`                                          pt
              join `eai-datalake-data-sandbox.payment.payment`                                                      b   on b.id = pt.payment_id
              left join ( 
                     with
                     base as (
                     select distinct
                            cl.document,
                            cl.uuid,
                            cl.created_at,
                            ev.observation,
                            ev.user_name,
                            ev.event_date,
                            RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
                     from  `eai-datalake-data-sandbox.core.customers`   cl                                           
                     left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id 
                     )
                     select * from base where Rank_Ult_Status = 1
                     ) cl  on b.customer_id = cl.uuid
              left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias`       ub   on ub.customer_id = b.customer_id
              left join `eai-datalake-data-sandbox.core.orders`                                                     ord  on ord.uuid = b.order_id
              join base_final_Risco_Cadastro                                                                        base on base.customer_id = b.customer_id

              WHERE 
              date(pt.created_at) >= current_date - 80
              --AND pt.payment_method in ('CREDIT_CARD') --'CASH'
              AND 
              pt.status in ('AUTHORIZED', 'COMPLETED')
              order by 1 desc
),Base_Outros_Postos_2 as(
              Select 
              distinct
                     customer_id 
                     ,sum(Pgto_Cartao) as Pgto_Cartao
                     ,sum(Pgto_Dinheiro) as Pgto_Dinheiro
                     ,count(distinct Cod_Posto) as qtd_posto
              from Base_Outros_Postos  
              group by 1
), Base_CBK as (
              select
              distinct
                     cbk.Customer_id
                     ,cbk.Flag_Risco     
                     ,cbk.Vl_Tranx
                     
              from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_chargeback_enriquecido_v2` cbk
              join base_final_Risco_Cadastro                                              base              on base.customer_id = cbk.customer_id
              --where cbk.Dt_tranx >= current_date - 60

),Base_CBK_2 as (
              select
              distinct
                     cbk.Customer_id
                     ,cbk.Flag_Risco  
                     ,Sum(cbk.Vl_Tranx) as Vl_Tranx_Cont
              from Base_CBK cbk
              group by 1,2
), Base_Crivo_Final as (
              select
              distinct
                     Cad.customer_id
                     ,Cad.Cpf_Cliente
                     ,Cad.StatusConta
                     ,cbk.Flag_Risco
                     ,(CASE WHEN outpos.Pgto_Cartao is null THEN 0 ELSE outpos.Pgto_Cartao END) as Pgto_Cartao
                     ,(CASE WHEN outpos.Pgto_Dinheiro is null THEN 0 ELSE outpos.Pgto_Dinheiro END) as Pgto_Dinheiro
                     ,(CASE WHEN cbk.Vl_Tranx_Cont is null THEN 0 ELSE cbk.Vl_Tranx_Cont END) as Vl_Tranx_Cont
                     ,outpos.qtd_posto
                     --,cbk.Vl_Tranx_Cont / outpos.Pgto_Cartao as Perc_Contestado          
                     --,outpos.Pgto_Cartao + outpos.Pgto_Dinheiro as Total
                     --,(outpos.Pgto_Cartao + outpos.Pgto_Dinheiro)*0.03 as ExposicaoCahback
                     ,entpix.Qtd_Origem
                     ,entpix.Qtd_BancoOrigem
                     ,entpix.Vl_Pix_Rec

                     ,pixsaida.Qtd_Favorecido
                     ,pixsaida.Vl_Pix_Env
                     ,pixsaida.Qtd_BancoDesitino
                     ,Flag_RiscoCadastro

              from base_final_Risco_Cadastro            Cad           
              left join Base_Outros_Postos_2            outpos        on Cad.customer_id = outpos.customer_id
              left join Saidas_PIX_2                    pixsaida      on Cad.customer_id = pixsaida.CustomerID
              left join Entrada_PIX_2                   entpix        on Cad.customer_id = entpix.customer_id
              left join Base_CBK_2                      cbk           on Cad.customer_id = cbk.Customer_id

              order by 1 desc
), Base_Crivo_Final_1 as (
              select 
                     a.* 
                     --,a.Vl_Tranx_Cont / a.Pgto_Cartao as Perc_Contestado
                     ,(CASE WHEN a.Vl_Tranx_Cont > 0 and a.Pgto_Cartao > 0 THEN a.Vl_Tranx_Cont / a.Pgto_Cartao ELSE 0 END) as Perc_Contestado

                     ,a.Pgto_Cartao + a.Pgto_Dinheiro as Total
                     ,(a.Pgto_Cartao + a.Pgto_Dinheiro)*0.03 as ExposicaoCahback
              from Base_Crivo_Final a
)
select
*
from Base_Crivo_Final_1
where 
Perc_Contestado >= 0.40
;

--------------------------------------------------------------------------------------------------------------------------------------
-- Clientes com transação fora do posto

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_Tranx_ForaPosto` where Flag_Contestacao =  'Com_Contestacao'
-- select Flag_Local_Posto_Tranx, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_Tranx_ForaPosto` group by 1

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_Tranx_ForaPosto` 


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_Tranx_ForaPosto` AS 

 with
 base_estudo as (
   select
   distinct
   cl.uuid as customer_id
   from  `eai-datalake-data-sandbox.core.customers`   cl 

), base_pagamentos as (
       SELECT * from (
              with
              base_pagamentos2 as (
                     SELECT 
                            distinct
                            date (pt.created_at)as Dt_tranx
                            ,FORMAT_DATETIME("%Y-%m",pt.created_at) as Safra_Tranx
                            ,date(cl.created_at) as Dt_Abertura
                            --,pt.payment_method as Pagamento
                            ,case
                                   when pt.payment_method = 'CASH' then 'Dinheiro'
                                   when pt.payment_method = 'CREDIT_CARD' then 'Credito'
                                   when pt.payment_method = 'DEBIT_CARD' then 'Debito'
                                   when pt.payment_method = 'BALANCE' then 'SaldoConta'
                                   when pt.payment_method = 'COUPON' then 'Voucher'
                                   when pt.payment_method = 'DIGITAL_WALLET' then 'CarteiraDigital'
                                   when pt.payment_method = 'GOOGLE_PAY' then 'GooglePay'
                                   else pt.payment_method end as Pagamento
                            ,locPost.store_id
                            ,locPost.Nome_loja
                            ,locPost.CNPJ_CPF
                            ,locPost.tipo_loja
                            ,locPost.cidade
                            ,locPost.UF as UF_Posto
                            ,locPost.latitude  as latitude_Posto
                            ,locPost.longitude as longitude_Posto
                            ,locPost.latitude||locPost.longitude as  latitude_longitude_Posto

                            ,left(ord.latitude,7) as  latitude_Tranx
                            ,left(ord.longitude,7) as  longitude_Tranx
                            ,left(ord.latitude,7) ||left(ord.longitude,7) as  latitude_longitude_Tranx
                            ,locTranx.cidade as Cidade_Trax
                            ,locTranx.UF as UF_Tranx
                            ,(cast(left(ord.latitude,7) as numeric) - cast(if(locPost.latitude = '', null, locPost.latitude) as numeric)) - (cast(left(ord.longitude,7)as numeric) - cast(if(locPost.longitude = '', null, locPost.longitude) as numeric)) as dif_geral
                            ,b.sales_channel
                            ,case 
                                   when ub.customer_id = b.customer_id then 'ClienteUber' else 'ClienteUrbano' end as Flag_Cliente
                            ,pt.transaction_value as Vl_Tranx
                            ,b.customer_id
                            ,cl.document as CPF
                            ,cl.Status_Conta
                            ,cl.Status_Conta_Cl
                            ,b.order_id


                     FROM `eai-datalake-data-sandbox.payment.payment_transaction`                                          pt
                     join `eai-datalake-data-sandbox.payment.payment`                                                      b   on b.id = pt.payment_id
                     join base_estudo                                                                                      est on est.customer_id = b.customer_id
                     left join ( 
                                   with
                                   base as (
                                   select distinct
                                          cl.document,
                                          cl.uuid,
                                          cl.created_at,
                                          cl.status as Status_Conta_Cl,
                                          --Ev.status as Status_Conta,
                                          case 
                                          when Ev.status = 'BLOCK' then 'Bloqueado'
                                          else 'Ativo' end as Status_Conta,
                                          ev.observation,
                                          ev.user_name,
                                          ev.event_date,
                                          RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
                                   from  `eai-datalake-data-sandbox.core.customers`   cl                                           
                                   left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id 
                                   )
                                   select * from base where Rank_Ult_Status = 1
                                   ) cl  on b.customer_id = cl.uuid
                            left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias`       ub  on ub.customer_id = b.customer_id
                            left join `eai-datalake-data-sandbox.core.orders`                                                     ord on ord.uuid = b.order_id
                            left join (   Select 
                                                 distinct
                                                 a.uuid as store_id
                                                 ,a.name as Nome_loja
                                                 ,a.document as CNPJ_CPF
                                                 ,a.type as tipo_loja
                                                 ,b.city as cidade
                                                 ,b.state as UF
                                                 ,left(b.latitude,7) as latitude
                                                 ,left(b.longitude,7) as longitude
                                          FROM `eai-datalake-data-sandbox.backoffice.store` a
                                          join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id ) locPost on locPost.store_id = ord.store_id
                                          left join (   Select 
                                                               distinct
                                                                b.city as cidade
                                                               ,b.state as UF
                                                               ,left(b.latitude,7) as latitude
                                                               ,left(b.longitude,7) as longitude
                                                        FROM `eai-datalake-data-sandbox.backoffice.store` a
                                                        join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id ) locTranx 
                                          on locTranx.latitude like left(ord.latitude,7) and locTranx.longitude like left(ord.longitude,7)

                     WHERE 
                     date(pt.created_at) >= current_date - 180
                     --AND pt.payment_method in ('CREDIT_CARD') --'CASH'
                     AND 
                     pt.status in ('AUTHORIZED','COMPLETED','SETTLEMENT','COMPLETE')
                     
              ) select 
                     distinct
                     a.*
                     ,case
                     when a.latitude_longitude_Posto = a.latitude_longitude_Tranx then '01_Transacao_no_Posto'
                     when a.latitude_longitude_Posto = a.latitude_longitude_Tranx  or substring(cast(a.dif_geral as string),1,5)  in ('-0.026','-0.087','0.008','0.029','-0.001','-0.000','-0.00','0.001','-0.01','0.000','0.002','-0.002','0.003','-0.003') then '01_Transacao_no_Posto'
                     when (cast(substring(cast(a.dif_geral as string),1,5) as numeric) between -1.000 and 1.000) or a.UF_Tranx = a.UF_Posto   then '02_Transacao_Proximo_Posto'
                     when cast(substring(cast(a.dif_geral as string),1,5) as numeric) < -1.000 
                     or cast(substring(cast(a.dif_geral as string),1,5) as numeric) > 1.000 
                     or a.UF_Tranx <> a.UF_Posto   then '03_Fora_Posto'
                     when a.UF_Tranx is null then '04_Transacao_nao_localizada'
                     else '03_Fora_Posto' end as Flag_Local_Posto_Tranx
              from base_pagamentos2 a) --where Flag_Local_Posto_Tranx in ('03_Fora_Posto','02_Transacao_Proximo_Posto')
), Base_CBK as (
              select
                     distinct
                     cbk.Dt_Tranx
                     ,cbk.Customer_id
                     ,cbk.Posto
                     ,cbk.Order_ID
                     ,cbk.Flag_Risco     
                     ,cbk.Vl_Tranx
              from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_chargeback_enriquecido_v2`    cbk
              join base_pagamentos                                                                        est on est.store_id = cbk.Posto and
              est.customer_id = cbk.customer_id 
              --and Flag_Local_Posto_Tranx in('03_Fora_Posto','02_Transacao_Proximo_Posto') 
              and  cbk.Order_ID =est.order_id 
              
),Base_CBK_2 as (
              select
                     distinct
                     cbk.Customer_id
                     ,cbk.Posto
                     ,cbk.Order_ID
                     --,cbk.Flag_Risco  
                     ,Sum(cbk.Vl_Tranx) as Vl_Tranx_Cont
              from Base_CBK cbk
              group by 1,2,3

)--,Base_Consolidada as (
              select 
              distinct
                     b.Safra_Tranx
                     ,DATETIME_DIFF(DATETIME(b.Dt_tranx), DATETIME(FORMAT_DATE("%Y-%m-%d",b.Dt_Abertura)), DAY) as Qtd_Dias_Tranx
                     ,b.Pagamento
                     ,b.store_id
                     ,b.order_id
                     ,b.Nome_loja
                     ,b.CNPJ_CPF
                     --b.tipo_loja
                     ,b.cidade as Cidade_Posto
                     ,b.UF_Posto
                     ,b.latitude_Posto
                     ,b.longitude_Posto
                     --b.latitude_longitude_Posto
                     ,b.latitude_Tranx
                     ,b.longitude_Tranx
                     --b.latitude_longitude_Tranx
                     ,b.Cidade_Trax
                     ,b.UF_Tranx
                     ,b.dif_geral
                     ,b.sales_channel
                     ,b.Flag_Cliente
                     ,b.customer_id
                     ,b.CPF
                     ,b.Status_Conta
                     ,case 
                            when b.store_id = cbk.Posto and b.Pagamento in ('Credito','Debito','Google Pay') then 'Com Contestação'
                     else 'Sem Contestação' end as Flag_Contestacao
                     ,case 
                            when b.Pagamento = 'Credito' then cbk.Vl_Tranx_Cont
                     else 0 end as Vl_Tranx_Cont
                     ,b.Flag_Local_Posto_Tranx
                     ,Sum(b.Vl_Tranx) as Vl_total_tranx
                     ,count(b.Vl_Tranx) as Qtd_total_tranx
                     ,count(distinct b.customer_id) as Qtd_cliente

              from base_pagamentos b
              left join Base_CBK_2                 cbk on b.Order_ID = cbk.Order_ID
              where
              --Flag_Local_Posto_Tranx in ('03_Fora_Posto','02_Transacao_Proximo_Posto')
              --and 
              sales_channel = 'APP'
              group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24
;
------------ base de dados - TPV Loja - ETAPA1
-- tb_temp_tpv_Posto_2

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_temp_tpv_Posto_2` where Cod_Posto = 'STO-72b53824-b919-4582-869c-fbf4217bc7d0'

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_temp_tpv_Posto_2` AS 

with

Base_TPV as (

            select
            distinct
            
                 tranx.gateway_id	
                ,tranx.status	as StatusTranx
                ,tranx.transaction_value	
                ,ord.cashback_percentage
                ,tranx_d.order_id	
                ,tranx_d.store_id	
                ,tranx_d.customer_id	
                ,substring(tranx_d.order_code,1,STRPOS(tranx_d.order_code,'-')) as order_code 	
                ,tranx_d.status	
                ,tranx_d.sales_channel	
                ,ord.pdv_token	
                ,ord.store_id as Cod_Loja	
                ,locPost.Nome_loja
                ,locPost.CNPJ_CPF
                ,locPost.tipo_loja
                ,locPost.cidade
                ,locPost.UF as UF_Posto
                ,CASE
                  WHEN locPost.UF IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
                  WHEN locPost.UF IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
                  WHEN locPost.UF IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
                  WHEN locPost.UF IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
                  WHEN locPost.UF IN ('SC','PR','RS') THEN 'SUL'
                  ELSE 'NAOINDENTIFICADO'
                END AS RegiaoPosto

                ,locPost.latitude  as latitude_Posto
                ,locPost.longitude as longitude_Posto
                ,locPost.latitude||locPost.longitude as  latitude_longitude_Posto

                ,left(ord.latitude,7) as  latitude_Tranx
                ,left(ord.longitude,7) as  longitude_Tranx
                ,left(ord.latitude,7) ||left(ord.longitude,7) as  latitude_longitude_Tranx

                ,(cast(left(ord.latitude,7) as numeric) - cast(if(locPost.latitude = '', null, locPost.latitude) as numeric)) - (cast(left(ord.longitude,7)as numeric) - cast(if(locPost.longitude = '', null, locPost.longitude) as numeric)) as dif_geral

                ,case
                  when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 0 then 'M0'
                  when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 1 then 'M-1'
                  when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 2 then 'M-2'
                  when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 3 then 'M-3'
                  when DATE_DIFF(date(current_date),date(tranx.created_at), Month) = 4 then 'M-4'
                else 'Outros' end as Flag_Filt_Per
                ,date(tranx.created_at) as Dt_Tranx
                ,FORMAT_DATE("%Y%m",tranx.created_at)as Safra_Tranx

            from `eai-datalake-data-sandbox.payment.payment_transaction`  tranx
            join `eai-datalake-data-sandbox.payment.payment`              tranx_d   on tranx_d.id = tranx.payment_id
            left join `eai-datalake-data-sandbox.core.orders`             ord       on ord.uuid = tranx_d.order_id
            left join (   Select 
                                distinct
                                a.uuid as store_id
                                ,a.name as Nome_loja
                                ,a.document as CNPJ_CPF
                                ,a.type as tipo_loja
                                ,b.city as cidade
                                ,b.state as UF
                                ,left(b.latitude,7) as latitude
                                ,left(b.longitude,7) as longitude
                          FROM `eai-datalake-data-sandbox.backoffice.store` a
                          join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id ) locPost on locPost.store_id = ord.store_id

   
            WHERE 
            date(tranx.created_at) >= current_date - 180
            --FORMAT_DATETIME('%Y',tranx.created_at) = '2022'
            and tranx.status in ('AUTHORIZED','COMPLETED','SETTLEMENT','COMPLETE') --- 'PRE_AUTHORIZED','AUTHORIZED','CANCELLED_BY_GATEWAY','SETTLEMENT'
            and tranx.`payment_method` = 'CREDIT_CARD'
)

SELECT
--Dt_Tranx
Safra_Tranx
,Cod_Loja as Cod_Posto
,Nome_loja
,sum(transaction_value) as tpv_Posto
from Base_TPV
group by 1,2,3

