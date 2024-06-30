-- select count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio` 
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio` 
-- select status, count(*) from `eai-datalake-data-sandbox.core.customers` group by 1
-- select Flag_TempodeConta, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio` group by 1


-------------------------------------------------------------------
-- TODOS OS CLIENTES BLOQUEADOS POR SUSPEITA DE FRAUDE            |
-------------------------------------------------------------------
--CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio` AS 
       with
              base_clientes_bloqueados as (
                     select
                     distinct
                            cl.uuid as  CustomerID
                            ,cl.full_name as Nome_Completo
                            ,cl.document as CPF_Cliente
                            --,cl.status as StatusConta
                            ,case 
                               when cl.status = 'ACTIVE' then 'ATIVA'
                               when cl.status = 'BLOCKED' then 'BLOQUEADA'
                               when cl.status = 'MINIMUM_ACCOUNT' then 'CONTA BÁSICA'
                               when cl.status = 'INACTIVE' then 'INATIVA'
                             end as StatusConta
                            ,cl.birth_date as Nascimento
                           ,CAST(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(cl.birth_date), SECOND) / (60*60*24*365) AS INT64) AS idade
                           ,Case 
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<18   Then '1  MenorIdade'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=20  Then '2  18a20anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=25  Then '3  21a25anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=30  Then '4  26a30anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=35  Then '5  31a35anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=40  Then '6  36a40anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=45  Then '7  41a45anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=50  Then '8  46a50anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=55  Then '9 51a55anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=60  Then '10 56a60anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=65  Then '11 61a65anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=70  Then '12 66a70anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=75  Then '13 71a75anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=80  Then '14 76a80anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)<=85  Then '15 81a85anos'
                                 When DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.birth_date), year)>85   Then '16 >86anos'  
                              End as Faixa_Idade
                            ,cl.email as email
                           /*
                            ,case 
                                 when email like '%teste%' or email like '%TESTE%' or email like '%pix%' or 
                                       REGEXP_CONTAINS(email, r'^[0-9]+@') or email like '%bancodobrasil%' or email like '%investimento%' or 
                                       email like '%mercadolivre%' or email like '%casasbahia%' or email like '%bradesco%' or 
                                       email like '%gympass%' or email like '%iphone%' or email like '%picpay%' or 
                                       email like 'olx' or email like 'enjoeei' or email like 'vapo' or email like 'dpvat' or 
                                       email like 'nubank' or email like 'fgts' or email like 'ipva' or email like 'santander' or 
                                       email like 'itau' or email like 'irrf' or email like 'enjoei' or email like 'restituicao' or 
                                       email like 'testar' or email like 'sofisa' or email like 'teste' or email like 'aposta' or 
                                       email like 'elo7' or email like 'magazineluiza' or email like 'magalu' or 
                                       email like 'aplicativo' or email like 'malote' or email like 'vazio' or email like 'golp' or 
                                       email like 'axax' or email like 'kkk' or email like 'xaxa' or email like 'SX' or 
                                       email like 'hacker' or email like 'hi2.in' then 'Email Suspeito' 
                                 else 'NA'
                              end as Flag_Validacao_email
                           */
                            ,ph.area_code as DDD
                            ,ph.number as Telefone
                            ,ph.type as Tipo_Tel
                            ,en.zipcode as Cep
                            ,en.street as Rua
                            ,en.neighborhood as Bairro
                            ,en.city as Cidade_Cliente
                            ,en.state as UF_Cliente
                            ,cl.created_at as Dt_Abertura
                            ,CASE
                                 WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=5 THEN '01_<5DIAS'
                                 WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=30 THEN '02_<30DIAS'
                                 WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=60 THEN '03_<60DIAS'
                                 WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=90 THEN '04_<90DIAS'
                                 WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=120 THEN '05_<120DIAS'
                                 WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=160 THEN '06_<160DIAS'
                                 WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=190 THEN '07_<190DIAS'
                                 WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=220 THEN '08_<220DIAS'
                                 WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=260 THEN '09_<260DIAS'
                                 WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=290 THEN '10_<290DIAS'
                                 WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) <=365 THEN '11_1ANO'
                                 WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(cl.created_at), DAY) >=365 THEN '12_+1ANO'
                              END AS Flag_TempodeConta
                            ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
                            ,case
                            when cl.trusted = 1 then 'Trusted'
                            else 'NaoTrusted' end as Flag_Trusted
                            ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual
                            ,CASE
                            WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
                            WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
                            WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
                            WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
                            WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
                            ELSE 'NAOINDENTIFICADO'
                            END AS RegiaoCliente
                            ,ev.status as StatusEvento
                            ,ev.observation as MotivoStatus
                            ,ev.event_date as DataStatus
                                      ,CASE
                                          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=5 THEN '01_<5DIAS'
                                          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=30 THEN '02_<30DIAS'
                                          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=60 THEN '03_<60DIAS'
                                          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=90 THEN '04_<90DIAS'
                                          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=120 THEN '05_<120DIAS'
                                          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=160 THEN '06_<160DIAS'
                                          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=190 THEN '07_<190DIAS'
                                          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=220 THEN '08_<220DIAS'
                                          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=260 THEN '09_<260DIAS'
                                          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=290 THEN '10_<290DIAS'
                                          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) <=365 THEN '11_<=1ANO'
                                          WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(ev.event_date), DAY) >365 THEN '12_+1ANO'
                                       END AS Flag_TempoBloqueado
                            ,FORMAT_DATE("%Y%m",ev.event_date)as Safra_Ev
                            ,zaig.Flag_Email
                            ,zaig.Flag_Celular
                            ,zaig.ScoreZaig
                            ,case 
                              when Bio.status = 'VALIDATED' then 'Bio Validada'
                              when Bio.status in ('REJECTED','NOT_VALIDATED') then 'Bio Rejeitada' else 'Bio Não Capturada' end as Flag_Biometria

                     FROM `eai-datalake-data-sandbox.core.customers`  as cl
                     left join `eai-datalake-data-sandbox.core.address` as en 
                     on en.id = cl.address_id
                     left join (select * from `eai-datalake-data-sandbox.core.customer_event` 
                            where status not in ('FACIAL_BIOMETRICS_REJECTED','FACIAL_BIOMETRICS_NOT_VALIDATED',   
                            'FACIAL_BIOMETRICS_VALIDATED','TEMPORARY_PERMISSION_CASH_OUT'))as ev 
                     on ev.customer_id = cl.id
                     left join `eai-datalake-data-sandbox.core.customer_phone` as cus_ph 
                     on ev.customer_id = cus_ph.customer_id
                     left join `eai-datalake-data-sandbox.core.phone` as ph 
                     on cus_ph.phone_id = ph.id
                     left join (
                                 with
                                 base_Bio as (
                                 SELECT 
                                 customer_id
                                 ,status
                                 ,validation_date
                                 ,RANK() OVER (PARTITION BY customer_id ORDER BY validation_date desc) AS Rank_Ult_Bio
                                 FROM `eai-datalake-data-sandbox.core.customer_facial_biometrics` 
                                 order by 1
                                 ) select * from base_Bio where Rank_Ult_Bio = 1
                                 ) Bio on Bio.customer_id = cl.id
                                 left join (
                                 with 
                                 base as (
                                       select 
                                       distinct
                                       Cpf_Cliente
                                       ,esteira
                                       ,data_cadastro
                                       ,tree_score as ScoreZaig
                                       ,decisao
                                       ,gps_latitude
                                       ,gps_longitude
                                       ,case when indicators like '%Not_validated_email%' then 'Email Não Validado' else 'Email Validado' end as Flag_Email
                                       ,case when indicators like '%Not_validated_phone%' then 'Celular Não Validado' else 'Celular Validado' end as Flag_Celular
                                       --,case when indicators like '%name_and_email_and_mother_name_full_uppercase%' then 'CaixaAltaNomeMae' else 'NA' end as Flag_NomeMae_CaixaAlta

                                       ,RANK() OVER (PARTITION BY cpf ORDER BY data_cadastro desc) AS Rank_Ult_Decisao

                                       from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` 
                                       where
                                       decisao = "automatically_approved"
                                       --date(data_cadastro) >= current_date - 20
                                       --and decisao = "automatically_approved"
                                       --and Cpf_Cliente = '61969036672' 
                                       --order by 2 desc
                                       ) 
                                       select 
                                       * 
                                       from base 
                                       where Rank_Ult_Decisao = 1) zaig on zaig.Cpf_Cliente = cl.document
) 
       select 
          distinct
           CustomerID,
           Nome_Completo,
           CPF_Cliente,
           StatusConta,
           MotivoStatus,
           DataStatus,
           Flag_TempoBloqueado,
           idade,
           Faixa_Idade,
           Safra_Ev,
           Dt_Abertura,
           Safra_Abertura,
           Flag_TempodeConta,
           email,
           (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) as Dias_Bloqueio,
           CASE
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) =0 THEN '01_NO_MESMO_DIA'
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) <=5 THEN '02_<=5_DIAS'
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) <=10 THEN '03_<=10_DIAS'
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) <=20 THEN '04_<=20_DIAS'
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) <=30 THEN '05_<=30_DIAS'
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) <=40 THEN '06_<=40_DIAS'
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) <=50 THEN '07_<=50_DIAS'
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) <=100 THEN '08_<=100_DIAS'
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) <=150 THEN '09_<=150_DIAS'
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) <=200 THEN '10_<=200_DIAS'
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) <=250 THEN '11_<=250_DIAS'
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) <=300 THEN '12_<=300_DIAS'
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) <=365 THEN '12_<=365_DIAS'
              WHEN (DATETIME_DIFF(DATETIME(current_date), DATETIME(Dt_Abertura), DAY) - DATETIME_DIFF(DATETIME(current_date), DATETIME(DataStatus), DAY)) >365 THEN '12_+1ANO'

           END AS Flag_Temp_Abert_Bloq,
           DDD,
           Telefone,
           Nascimento,
           Cep,
           Rua,
           Bairro,
           Cidade_Cliente,
           UF_Cliente,
           Flag_Trusted,
           Rank_Ult_Atual,
           RegiaoCliente,
           StatusEvento,
           DDD ||'_'|| UF_Cliente as DDD_UF 
          ,Flag_Email
          ,Flag_Celular
          ,ScoreZaig
          ,Flag_Biometria

       from base_clientes_bloqueados
       where Rank_Ult_Atual = 1
       and date_diff(current_date(), Dt_Abertura, DAY) <= 365
       and StatusConta = 'BLOQUEADA'
       and MotivoStatus = 'Suspeita de fraude'
       and Tipo_Tel = 'MOBILE'
       order by Dt_Abertura 

;

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_cartoes` order by qtd_Cadastro_Tentativas_Cartoes desc 



-------------------------------------------------------------------
-- 1 - BASE ABUSADORES                                            |
-------------------------------------------------------------------
create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_abusadores` as
   with base_trans_limit as (
      SELECT
         base.CustomerID,
         base.Nome_Completo,
         base.CPF_Cliente,
         p.order_id,
         ord.store_id,
         CASE WHEN pt.transaction_value >= 300 THEN 1 ELSE 0 END AS Qtd_Tran_Limite,
         SUM(IF(pt.transaction_value >= 300, pt.transaction_value, 0)) AS Vl_Tran_Limite,
         SUM(pt.transaction_value) AS TPV,
         COUNT(pt.transaction_value) AS QtdTransacao
      FROM  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio` as base 
      LEFT JOIN`eai-datalake-data-sandbox.payment.payment` as p
      ON base.CustomerID = p.customer_id
      LEFT JOIN `eai-datalake-data-sandbox.payment.payment_instrument` as pi 
      ON p.id = pi.id
      LEFT JOIN `eai-datalake-data-sandbox.payment.payment_transaction` as pt 
      ON pt.payment_id = p.id
      LEFT JOIN `eai-datalake-data-sandbox.core.orders` as ord 
      ON p.order_id = ord.uuid
      WHERE
         DATE(pt.created_at) >= CURRENT_DATE() - 365
         AND pt.transaction_value > 0
         AND pt.payment_method IN ("CASH")
         AND pt.status IN ("AUTHORIZED", "SETTLEMENT", "COMPLETED")
         --AND CPF_Cliente = '47833190810'
      group by 1,2,3,4,5,6
   
   ), base_2 as (select 
         distinct
         li.CustomerID,
         li.Nome_Completo,
         li.CPF_Cliente,
         sum(Qtd_Tran_Limite) as Qtd_Tran_Limite,
         sum(Vl_Tran_Limite) as Vl_Tran_Limite,
         sum(TPV) as TPV,
         sum(QtdTransacao) as QtdTransacao
      from base_trans_limit as li
      group by 1,2,3
   )
   select
         *,
         (Qtd_Tran_Limite / QtdTransacao) * 100 as Percent_Tranx_Limit
         /*
         case 
            when pos.store_id is null then 'NA'
            else 'Transação em posto ofensor'
         end as Flag_posto
         */
         

   from base_2 
   /*left join  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_Postos_Consolidado` as pos
   on li.store_id = pos.STORE_ID*/
   

   ;



-------------------------------------------------------------------
-- 2 - BASE CHARGEBACK                                            |
-------------------------------------------------------------------
create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_CBK` as
   with Base_TPV_Com_CBK as (
   select
   base.CustomerID,
   base.Nome_Completo,
   base.CPF_Cliente,
   ord.store_id,
   case when p.order_id = cbk.order_id and pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET") then pt.transaction_value else 0 end as Valor_Contestado,
   case when p.order_id = cbk.order_id and pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET") then 1 else 0 end as Qtd_Constestado,
   case when pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET") then pt.transaction_value else 0 end as TPV_PayPal,
   sum(pt.transaction_value) as QtdTransacao


   FROM  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio` as base 
   LEFT JOIN`eai-datalake-data-sandbox.payment.payment` as p
   ON base.CustomerID = p.customer_id
   left join `eai-datalake-data-sandbox.payment.payment_instrument` as pi 
   on p.id = pi.id
   left join `eai-datalake-data-sandbox.payment.payment_transaction` as pt 
   on pt.payment_id = p.id
   left join `eai-datalake-data-sandbox.core.orders` as ord 
   on p.order_id = ord.uuid
   left join ( select
               distinct
               *
               from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Base_Consolidada_PayPal_DataLake_cbk_historico` 
               where Reason = 'Fraud'and Status = 'Open' and Kind = 'Chargeback'
            ) cbk on p.order_id = cbk.order_id
   join (
            select
            distinct
            a.uuid as store_id
            FROM `eai-datalake-data-sandbox.backoffice.store` a
            join `eai-datalake-data-sandbox.backoffice.address` b on a.address_id = b.id
            join `eai-datalake-data-sandbox.loyalty.tblPontoVenda` c on c.CNPJ = left(a.document,12)
            join `eai-datalake-data-sandbox.maps.store_place_details` post on post.document = a.document
            where a.type = 'POS') Post on Post.store_id = ord.store_id
   where 
   date(pt.created_at) >= current_date - 365
   and pt.transaction_value > 0
   and pt.payment_method in ("CREDIT_CARD","DEBIT_CARD","DIGITAL_WALLET")
   --and p.customer_id = 'CUS-e7ef6b1d-67ae-4933-aba4-d2f77d851594'
   --and CPF_Cliente = '47833190810'
   group by 1,2,3,4,5,6,7
   ), base_CBK as ( select 
      CustomerID,
      Nome_Completo,
      CPF_Cliente,
      sum(Valor_Contestado) as Valor_Contestado,
      sum(Qtd_Constestado) as Qtd_Constestado,
      sum(TPV_PayPal) as TPV_PayPal,
      count(QtdTransacao) as QtdTransacao
   from Base_TPV_Com_CBK as li
      left join  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_Postos_Consolidado` as pos
      on li.store_id = pos.STORE_ID
   group by 1,2,3
   ) select
         *,
         (Qtd_Constestado / QtdTransacao) * 100 as Percent_Tranx_PayPal
         /*
         case 
            when pos.store_id is null then 'NA'
            else 'Transação em posto ofensor'
         end as Flag_posto
         */
         

   from base_CBK 
   /*left join  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Bloqueio_Postos_Consolidado` as pos
   on li.store_id = pos.STORE_ID*/
   limit 100

   ; 




-------------------------------------------------------------------
-- 3 - BASE TRANSAÇÃO APP                                         |
-------------------------------------------------------------------
create or replace table `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_Tranx_APP` as
   with base_produtos_tranx as (
   select
   distinct
   base.CustomerID
   ,case  when ord.sales_channel in ('TEF','POS','POS_QRCODE','APP_DELIVERY','ON_LINE','VOUCHER_UBER','APP_JET_OLIL','PDV_QRCODE','ECOMMERCE') THEN 'Outros Produtos' else 'ND' end as Flag_Produto_Outros
   ,case when ord.sales_channel in ('APP_LATAMPASS','APP_MILES','APP_TUDOAZUL') THEN 'Pontos_Aerias' else 'ND' end as Flag_Produto_Pontos_Aerias
   ,case when ord.sales_channel in ('APP') THEN 'Abastecimento' else 'ND' end as Flag_Produto_Abastecimento
   ,case when ord.sales_channel in ('APP_JET_OIL') THEN 'Jet_Oil' else 'ND' end as Flag_Produto_JetOil
   ,case when ord.sales_channel in ('APP_AMPM') THEN 'Ampm' else 'ND' end as Flag_Produto_AMPM
   ,case when ord.sales_channel in ('SERVICE') THEN 'Recarga' else 'ND' end as Flag_Produto_Recarga
   ,case when ord.sales_channel in ('APP_ULTRAGAZ') THEN 'Ultragaz' else 'ND' end as Flag_Produto_Ultragaz

   ,count(distinct ord.uuid) as Transacoes
   ,round(sum(ord.order_value),0) as TPV 

   FROM  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio` as base 
   LEFT JOIN`eai-datalake-data-sandbox.payment.payment` as p
   ON base.CustomerID = p.customer_id
   left join `eai-datalake-data-sandbox.payment.payment_instrument` as pi 
   on p.id = pi.id
   left join `eai-datalake-data-sandbox.payment.payment_transaction` as pt 
   on pt.payment_id = p.id
   left join `eai-datalake-data-sandbox.core.orders` as ord 
   on p.order_id = ord.uuid
   where  ord.order_status = 'CONFIRMED'
   group by 1,2,3,4,5,6,7,8
   ) select* from base_produtos_tranx
   ;



-------------------------------------------------------------------
-- 4 - BASE TRANSAÇÃO CONTA DIGITAL                               |
-------------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_Tranx_Conta` AS 
   with tranx_conta_digital as (
   SELECT

   CASE WHEN flow = 'PIX' THEN
   CASE WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN pix_in_payee.payee_id
   WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payer.payer_id END
   WHEN flow = 'TED' THEN
   CASE WHEN ted.type = 'CASH_IN' THEN ted_in_payee.payee_id
   WHEN ted.type = 'CASH_OUT' THEN ted_payer.payer_id END
   WHEN flow = 'BILLET' THEN billet.payee_id 
   WHEN p2p.type = 'CASH_OUT' THEN p2p_payer.payer_id
   WHEN p2p.type = 'CASH_OUT' THEN p2p_payee.payee_id
   WHEN qpo.type = 'CASH_IN' THEN qpo.payee_document
   ELSE flow 
   END AS customer_id

   ,COUNT(DISTINCT cash_transaction.id) as qtdtransacoes
   ,ROUND(SUM(cash_transaction.amount)/100,2) as valor
   FROM `eai-datalake-data-sandbox.cashback.cash_transaction` as cash_transaction
   LEFT join `eai-datalake-data-sandbox.cashback.pix`  AS pix 
   on cash_transaction.id = pix.cash_transaction_id 
   LEFT join `eai-datalake-data-sandbox.cashback.pix_payer` AS pix_payer 
   on pix_payer.pix_id = pix.id
   LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_in_payee` AS pix_in_payee 
   on pix_in_payee.pix_id = pix.id
   LEFT JOIN `eai-datalake-data-sandbox.cashback.ted` AS ted 
   on cash_transaction.id = ted.cash_transaction_id
   LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_payer` AS ted_payer 
   on ted_payer.ted_id = ted.id
   LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_in_payee` AS ted_in_payee 
   on ted_in_payee.ted_id = ted.id
   LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p` AS p2p 
   on cash_transaction.id = p2p.cash_transaction_id
   LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payer` AS p2p_payer 
   on p2p_payer.p2p_id = p2p.id
   LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payee` AS p2p_payee 
   on p2p_payee.p2p_id = p2p.id
   LEFT JOIN `eai-datalake-data-sandbox.cashback.billet` AS billet 
   on cash_transaction.id = billet.cash_transaction_id
   LEFT JOIN `eai-datalake-data-sandbox.cashback.qrcode_pix_out`  AS qpo 
   on cash_transaction.id = qpo.cash_transaction_id
   WHERE 
   --DATE(cash_transaction.created_at) >= '2023-01-01' --AND DATE(cash_transaction.created_at) <= CURRENT_DATE
   --AND 
   (pix.status = 'APPROVED' or ted.status = 'APPROVED' or p2p.status = 'APPROVED' or billet.status = 'APPROVED' or qpo.status = 'PAID')
   GROUP BY 1
   )
   SELECT conta.* FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio` as base  
   LEFT JOIN tranx_conta_digital AS conta
   on base.CustomerID = conta.customer_id

   ;





-------------------------------------------------------------------
-- 6 - TODAS AS TENTATIVAS DE CADASTRO DE CARTÕES                 |
-------------------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_cartoes` AS 
   select
       distinct
       base.CustomerID,
       base.Nome_Completo,
       base.CPF_Cliente,
       CASE
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=1 THEN '1_<1DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=3 THEN '2_<3DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=6 THEN '3_<6DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=9 THEN '4_<9DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=12 THEN '5_<12DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=15 THEN '6_<15DIAS'
              WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(card.created_at), DAY) <=20 THEN '7_<20DIAS'
              else 'Verificar'
       END AS Temp_Cadastro_Cartao,
       card.created_at as data_cadastro_cartao,
       card.id as card_id,
       bin.Emissor_do_Banco as Banco_Emissor,
       card.last_four_digits as Ultimos_Digitos
       from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio`  as base
       left join `eai-datalake-data-sandbox.payment.customer_card` as card
       on base.CPF_Cliente = card.document 
       left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos` as bin
       on cast(card.bin as string) = cast(bin.BIN as string)
;


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_cartoes_Cubo` as
WITH BASE_CARTOES AS (
   select 
    distinct
       CustomerID,
       CPF_Cliente,
       count(distinct card_id) as qtd_Cadastro_Tentativas_Cartoes,
       count(distinct Banco_Emissor) AS qtd_banco_emissor,
       count(distinct Ultimos_Digitos) as qtd_cartao
   from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_cartoes`     
   GROUP BY 1, 2
   order by 3 desc
) SELECT 
      *,
       CASE
              WHEN qtd_Cadastro_Tentativas_Cartoes <= 5 THEN '01-Ate_5_Tentativas'
              WHEN qtd_Cadastro_Tentativas_Cartoes <=10 THEN '02-Ate_10_Tentativas'
              WHEN qtd_Cadastro_Tentativas_Cartoes <=20 THEN '03-Ate_20_Tentativas'
              WHEN qtd_Cadastro_Tentativas_Cartoes <=30 THEN '04-Ate_30_Tentativas'
              WHEN qtd_Cadastro_Tentativas_Cartoes <=50 THEN '05-Ate_50_Tentativas'
              WHEN qtd_Cadastro_Tentativas_Cartoes <=100 THEN '06-Ate_100_Tentativas'
              WHEN qtd_Cadastro_Tentativas_Cartoes <=200 THEN '07-Ate_200_Tentativas'
              WHEN qtd_Cadastro_Tentativas_Cartoes <=300 THEN '08-Ate_300_Tentativas'
              WHEN qtd_Cadastro_Tentativas_Cartoes >300 THEN '09-Mais_de_300_Tentativas'
        
       END AS Flag_Qtd_Tentativas
   from BASE_CARTOES
;

/*
SELECT Flag_Qtd_Tentativas ,count(*) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_cartoes_Cubo` 
group by 1
order by 1

select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_cartoes_Cubo` order by 3 desc

select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_cartoes_Cubo`
where Flag_Qtd_Tentativas in ('06-Ate_100_Tentativas','07-Ate_200_Tentativas','08-Ate_300_Tentativas','09-Mais_de_300_Tentativas')
*/

-- select count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bin_Bancos`



-----------------------------------------------------
-- 7 - PASSARAM PELO MASSIVO                        |
-----------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_massivo` AS 
with base_massivo as (
       select 
       distinct
       base.CustomerID,
       base.Nome_Completo,
       base.CPF_Cliente,
       case 
         when Motivo is null then 'Não'
         else 'Sim' 
       end as Flag_Massivo,
       massivo.Motivo as Motivo_Massivo,
       massivo.Lote as Lote_Massivo,
       from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio` as base
       left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo_2`  as massivo
       on base.CPF_Cliente = massivo.cpf_completo

       
), base_rank_massivo as (

select 
   distinct
   *,
   RANK() OVER (PARTITION BY CPF_Cliente ORDER BY Lote_Massivo desc) AS Rank_massivo
from base_massivo
)
select 
  distinct
   *
from base_rank_massivo
where Rank_massivo = 1

-- where Flag_Motivo_Massivo = 'Não passou pelo massivo' 
;



-----------------------------------------------------
-- 8 - COM CHAMADO NA CENTRAL                       |
-----------------------------------------------------

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_chamados` AS
with base_Chamado1 as (
            SELECT 
              distinct
              op.NR_OCORRENCIA
              ,op.TIPO_CHAMADO
              ,base.CustomerID
              ,base.Nome_Completo
              ,base.CPF_Cliente
              ,base.DataStatus
              ,op.CNPJ
              ,op.STATUS
              ,op.DT_CRIACAO
              ,op.DT_ENCERRAMENTO
              ,op.DT_ULT_ALTER_OCORR
              ,op.CENTRAL_RECEBIDA
              ,op.CANAL
              ,op.PRODUTO_SERVICO
              ,op.CLASSIFICACAO
              ,op.SUBCLASSIFICACAO
              ,op.MOTIVO as MOTIVO_CHAMADO
              ,op.TMR_DIA
              ,case 
              when op.TMR_DIA < 1 then '1_até1dia'
              when op.TMR_DIA < 2 then '2_até2dia'
              when op.TMR_DIA < 3 then '3_até3dia'
              when op.TMR_DIA < 4 then '4_até4dia'
              when op.TMR_DIA < 5 then '5_até5dia'
              when op.TMR_DIA > 6 then '6_meior5dia'
              else 'EmAberto' end as Flag_TMR
              ,op.IND_ATENDIDO
              ,op.NOTA_SATISFACAO
              ,op.NOTA_NPS
              ,op.DESCRICAO
              ,op.RSP_PRINCIPAL
              ,op.RSP_EXTERNO
              ,op.RSP_PONTUAL
              ,RANK() OVER (PARTITION BY op.CPF ORDER BY op.NR_OCORRENCIA desc ) AS Rank_Ult_Cham
            from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio` as base  
            left join `eai-datalake-data-sandbox.siebel.chamados`            op
            on base.CPF_Cliente = op.CPF
            --WHERE 
            --date(DT_CRIACAO) >= current_date - 180 
            --AND 
            --RSP_EXTERNO in ('GRUPO DE PREVENÇÃO A FRAUDE','GRUPO DE PREVENÇÃO A FRAUDE','GRUPO PREVENÇÃO')

            ), baseConsolidada_Chamado2 as (
                select 
                distinct
                * 
                from base_Chamado1 where Rank_Ult_Cham = 1
             ), baseConsolidada_Chamado3 as (
                 select
                 distinct
                 *,
                 case
                  when op.NR_OCORRENCIA is null then 'Sem Contato'
                  else 'Com Contato' 
                  end as Flag_Chamado

                 from baseConsolidada_Chamado2  as op 
             ), baseConsolidada_Chamado4 as (
             select 
             distinct
              a.*
              ,RANK() OVER (PARTITION BY CPF_Cliente ORDER BY date(DT_ULT_ALTER_OCORR) desc) AS Rank_Ult_Final
              from baseConsolidada_Chamado3 a 
             ) 
             select
             distinct
               NR_OCORRENCIA,
               TIPO_CHAMADO,
               CPF_Cliente,
               Nome_Completo,
               CustomerID,
               STATUS,
               CENTRAL_RECEBIDA,
               CANAL,
               PRODUTO_SERVICO,
               CLASSIFICACAO,
               SUBCLASSIFICACAO,
               MOTIVO_CHAMADO,
               TMR_DIA,
               Flag_TMR,
               IND_ATENDIDO,
               DESCRICAO,
               Flag_Chamado,
               RSP_EXTERNO,

             from baseConsolidada_Chamado4 
             where Rank_Ult_Final = 1

;

-----------------------------------------------------
-- 8 - PIX IN                                       |
-----------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_PIXIN` AS 

SELECT 
distinct


  DATETIME(Cash_Transaction.created_at) as Data_do_Evento
  ,cast(cc1.bank_token as String) as Cliente_conta_evento
  ,FORMAT_DATETIME("%Y%m%d",cl_fav.created_at) as Inicio_do_Relacionamento
  ,cc1.bank_account_agency as Agencia
  ,cc1.bank_account_number||cc1.bank_account_check_number as Conta
  ,Cash_Transaction.amount/100 as Valor_do_Evento
  ,cast(pix_payer.document as string) as CPF_CNPJ_Cliente_conta_credito
  ,pix_payer.agency_number as Agencia_Fav
  ,pix_payer.account_number||"-"||pix_payer.account_check_number as Conta_Fav
  ,key_type as Tipo_de_chave_PIX
  ,pix.key_value as Chave_PIX
  ,pix_payer.name as Favorecido
  ,pix.type || '_PIX' as Tipo_Transacao
  ,FORMAT_DATE("%Y%m",Cash_Transaction.created_at)as Safra_Tranx
  ,FORMAT_DATE("%Y%m",cl_fav.created_at)as Safra_Abertura
  ,FORMAT_DATE("%Y%m",bd.DataStatus)as Safra_Bloqueio

FROM `eai-datalake-data-sandbox.cashback.pix_in_payer` as pix_payer 
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix` as pix               
ON pix_payer.pix_id = pix.id 
LEFT JOIN `eai-datalake-data-sandbox.cashback.cash_transaction` as Cash_Transaction  
ON pix.cash_transaction_id = Cash_Transaction.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_in_payee` as pix_payee         
ON pix.id = pix_payee.pix_id
LEFT JOIN `eai-datalake-data-sandbox.core.customers` as cl_fav            
ON pix_payee.payee_id = cl_fav.uuid
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio` as bd                
ON cast(bd.CPF_Cliente as string) = cl_fav.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account` as cc1               
ON cc1.bank_token = cast(bd.CPF_Cliente as string)
where pix.status IN ('APPROVED')
and Cash_Transaction.amount > 0
and pix.type in ( 'CASH_IN','CASH_OUT') 
and pix_payer.name in ('MINISTERIO DO TRABALHO E PREVIDENCIA','PASEP','SECR. DA RECEITA FEDERAL -  LOTE 2023 05 - RMS 0115')
;


--------------------------------------------------------
-- BASE CONSOLIDADA                                   |
-------------------------------------------------------

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_Consolidada` AS

WITH BASE_CONSOLIDADO AS (
   SELECT
      DISTINCT
      BASE.CustomerID,
      BASE.CPF_Cliente,
      BASE.Nome_Completo,
      BASE.Dt_Abertura,
      BASE.DataStatus,
      BASE.Dias_Bloqueio AS Dias_Ate_Bloqueio,
      BASE.idade,
      BASE.Cidade_Cliente,
      BASE.UF_Cliente,
      BASE.Flag_Trusted,
      BASE.Flag_Email,
      BASE.Flag_Celular,
      BASE.ScoreZaig,
      BASE.Flag_Biometria,
      --ABUSO.Flag_posto,
      CARTOES.qtd_Cadastro_Tentativas_Cartoes,
      CARTOES.qtd_banco_emissor,
      CARTOES.qtd_cartao,
      CARTOES.Flag_Qtd_Tentativas,
      ABUSO.Qtd_Tran_Limite,
      ABUSO.Vl_Tran_Limite,
      ABUSO.TPV,
      ABUSO.QtdTransacao,
      ABUSO.Percent_Tranx_Limit,
      CBK.Valor_Contestado,
      CBK.Qtd_Constestado,
      CBK.TPV_PayPal,
      CBK.QtdTransacao AS QtdTransacaoPayPal,
      CBK.Percent_Tranx_PayPal,
      TRANX_CONTA.qtdtransacoes AS Qtd_Transacoes_Conta,
      TRANX_CONTA.VALOR AS Valor_Transacoes_Conta,
      --MASSIVO.Motivo_Massivo
      CHAMADOS.NR_OCORRENCIA,
      CHAMADOS.CANAL,
      CHAMADOS.CLASSIFICACAO,
      CHAMADOS.SUBCLASSIFICACAO,
      CHAMADOS.MOTIVO_CHAMADO,
      CHAMADOS.DESCRICAO,
      CHAMADOS.Flag_Chamado


   FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio` AS BASE
   LEFT JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_abusadores` AS ABUSO
   ON BASE.CustomerID = ABUSO.CustomerID
   LEFT JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_CBK` AS CBK
   ON BASE.CustomerID = CBK.CustomerID
   LEFT JOIN  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_massivo` AS MASSIVO
   ON BASE.CustomerID = MASSIVO.CustomerID
   LEFT JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_Tranx_Conta` AS TRANX_CONTA
   ON BASE.CustomerID = TRANX_CONTA.customer_id
   LEFT JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_chamados`AS CHAMADOS
   ON BASE.CustomerID = CHAMADOS.CustomerID
   LEFT JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_cartoes_Cubo` AS CARTOES
   ON BASE.CustomerID = CARTOES.CustomerID
   
--LIMIT 100

) SELECT
      *,
      CASE 
         WHEN Qtd_Tran_Limite IS NULL OR Qtd_Tran_Limite = 0 THEN 'Não'
         else 'Sim'
      end as Flag_Limite_300,
      CASE 
         WHEN Qtd_Constestado IS NULL OR Qtd_Constestado = 0 THEN 'Não'
         else 'Sim'
      end as Flag_Contestacao,

  from BASE_CONSOLIDADO
;




-----------------------------------------------------------------------------------------------------------------------------



-----------------------------------------------------------------------------------------------------------------------------

-- CUS-89daa458-cf1d-4b53-83b7-9286e13bb42c 70097762610 Tais Amaral Tavares

/*

select Flag_Limite_300,  Flag_Contestacao, count(*) volume from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_base_desbloqueio_Consolidada` 
group by 1,2


SELECT * FROM `eai-datalake-data-sandbox.cashback.cash_transaction` LIMIT 100

*/




--------------------------------------------------------
-- MONITORAMENTO DE CONTAS DESBLOQUEADAS               |
--------------------------------------------------------
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_desbloqueados_ContaDigital` AS
with base_conta_digital as (
SELECT

CASE WHEN flow = 'PIX' THEN
CASE WHEN pix.type in ('CASH_IN', 'CASH_IN_REFUND') THEN pix_in_payee.payee_id
WHEN pix.type in ('CASH_OUT', 'CASH_OUT_REFUND') THEN pix_payer.payer_id END
WHEN flow = 'TED' THEN
CASE WHEN ted.type = 'CASH_IN' THEN ted_in_payee.payee_id
WHEN ted.type = 'CASH_OUT' THEN ted_payer.payer_id END
WHEN flow = 'BILLET' THEN billet.payee_id 
WHEN p2p.type = 'CASH_OUT' THEN p2p_payer.payer_id
WHEN p2p.type = 'CASH_OUT' THEN p2p_payee.payee_id
WHEN qpo.type = 'CASH_IN' THEN qpo.payee_document
ELSE flow 
END AS customer_id
,cash_transaction.created_at as DataTransacao
,pix_pay.bank_name as Banco_Favorecido
,COUNT(DISTINCT cash_transaction.id) as qtdtransacoes
,ROUND(SUM(cash_transaction.amount)/100,2) as valor
from  `eai-datalake-data-sandbox.cashback.cash_transaction` as cash_transaction
LEFT join `eai-datalake-data-sandbox.cashback.pix`  pix 
on cash_transaction.id = pix.cash_transaction_id 
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_in_payer` as pix_pay             
ON pix_pay.pix_id = pix.id 
LEFT join `eai-datalake-data-sandbox.cashback.pix_payer` pix_payer 
on pix_payer.pix_id = pix.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_in_payee` pix_in_payee 
on pix_in_payee.pix_id = pix.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted` ted 
on cash_transaction.id = ted.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_payer` ted_payer 
on ted_payer.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.ted_in_payee`ted_in_payee 
on ted_in_payee.ted_id = ted.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p` p2p 
on cash_transaction.id = p2p.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payer` p2p_payer 
on p2p_payer.p2p_id = p2p.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.p2p_payee` p2p_payee 
on p2p_payee.p2p_id = p2p.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.billet` billet 
on cash_transaction.id = billet.cash_transaction_id
LEFT JOIN `eai-datalake-data-sandbox.cashback.qrcode_pix_out` qpo 
on cash_transaction.id = qpo.cash_transaction_id
WHERE 
DATE(cash_transaction.created_at) >= '2024-04-16' 
AND 
(pix.status = 'APPROVED' or ted.status = 'APPROVED' or p2p.status = 'APPROVED' or billet.status = 'APPROVED' or qpo.status = 'PAID')
--and pix_pay.bank_name in ('MINISTERIO DO TRABALHO E PREVIDENCIA','PASEP','SECR. DA RECEITA FEDERAL -  LOTE 2023 05 - RMS 0115')
GROUP BY 1,2,3
), base_conta_digital_final as (
SELECT 
   desbloqueados.CustomerID,
   desbloqueados.cpf_completo,
   case 
      when contas.qtdtransacoes is null then 0
      else contas.qtdtransacoes
   end as qtdtransacoes,
   case 
      when contas.valor is null then 0
      else contas.valor
   end as valor,
   contas.DataTransacao,
   date(contas.DataTransacao) as Dia_Transacao,
   contas.Banco_Favorecido
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Desbloqueio_Massivo_2` as desbloqueados
left join base_conta_digital as contas
on desbloqueados.CustomerID = contas.customer_id
--where qtdtransacoes > 0
) select final.*, perfil.StatusConta, perfil.DataStatus, perfil.MotivoBloqueio from base_conta_digital_final as final
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` as perfil
on final.CustomerID = perfil.CustomerID
;



CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_desbloqueados_APP` AS
with base_app as (
   select
distinct
c.uuid as  CustomerID
,case  when o.sales_channel in ('TEF','POS','POS_QRCODE','APP_DELIVERY','ON_LINE','VOUCHER_UBER','APP_JET_OLIL','PDV_QRCODE','ECOMMERCE') THEN 'Outros Produtos' else 'ND' end as Flag_Produto_Outros
,case when o.sales_channel in ('APP_LATAMPASS','APP_MILES','APP_TUDOAZUL') THEN 'Pontos_Aerias' else 'ND' end as Flag_Produto_Pontos_Aerias
,case when o.sales_channel in ('APP') THEN 'Abastecimento' else 'ND' end as Flag_Produto_Abastecimento
,case when o.sales_channel in ('APP_JET_OIL') THEN 'Jet_Oil' else 'ND' end as Flag_Produto_JetOil
,case when o.sales_channel in ('APP_AMPM') THEN 'Ampm' else 'ND' end as Flag_Produto_AMPM
,case when o.sales_channel in ('SERVICE') THEN 'Recarga' else 'ND' end as Flag_Produto_Recarga
,case when o.sales_channel in ('APP_ULTRAGAZ') THEN 'Ultragaz' else 'ND' end as Flag_Produto_Ultragaz
,o.created_at as DataPedido
,count(distinct o.uuid) as Transacoes
,round(sum(o.order_value),0) as TPV 

from `eai-datalake-data-sandbox.core.customers` c 
join `eai-datalake-data-sandbox.core.orders` o on c.id = o.customer_id
where 
o.order_status = 'CONFIRMED'
and DATE(o.created_at) >= '2024-04-16' 
group by 1,2,3,4,5,6,7,8,9

), base_app_final as (
SELECT 
   desbloqueados.CustomerID,
   desbloqueados.cpf_completo,
   app.Flag_Produto_Outros,
   app.Flag_Produto_Pontos_Aerias,
   app.Flag_Produto_Abastecimento,
   app.Flag_Produto_JetOil,
   app.Flag_Produto_AMPM,
   app.Flag_Produto_Recarga,
   app.Flag_Produto_Ultragaz,
   case 
      when app.Transacoes is null then 0
      else app.Transacoes
   end as Transacoes, 
      case 
      when app.TPV is null then 0
      else app.TPV
   end as TPV, 
   app.DataPedido,
   date(app.DataPedido) as Dia_Pedido
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Desbloqueio_Massivo_2` as desbloqueados
left join base_app as app
on desbloqueados.CustomerID = app.CustomerID
--where Transacoes > 0
) select final.*, perfil.StatusConta, perfil.DataStatus, perfil.MotivoBloqueio from base_app_final as final
left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_cliente_Perfil` as perfil
on final.CustomerID = perfil.CustomerID
;

select StatusConta, date(DataStatus), CustomerID, Transacoes, sum(TPV) as TPV from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_desbloqueados_APP`
where Transacoes > 0
group by 1,2,3,4
;
-----------------------------------------------------------------------------
-- VISÃO APP, QUANTIDADE DE CLIENTES QUE TRANSACIONARAM E STATUS DA CONTA   |
-----------------------------------------------------------------------------
select StatusConta, count(*) as Quantidade_Contas, round(Sum(TPV),2) as TPV from  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_desbloqueados_APP` 
where Transacoes > 0
group by 1
order by 3
;
---------------------------------------------------------------------------------------
-- VISÃO CONTA DIGITAL, QUANTIDADE DE CLIENTES QUE TRANSACIONARAM E STATUS DA CONTA   |
---------------------------------------------------------------------------------------
select StatusConta, count(*) as Quantidade_Contas, round(Sum(valor),2) as TPV from  `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_desbloqueados_ContaDigital`
where qtdtransacoes > 0
group by 1
order by 3
;
--------------------------------------------------------------------------------------------------------------------

SELECT 
distinct


  DATETIME(Cash_Transaction.created_at) as Data_do_Evento
  ,cast(cc1.bank_token as String) as Cliente_conta_evento
  ,FORMAT_DATETIME("%Y%m%d",cl_fav.created_at) as Inicio_do_Relacionamento
  ,cc1.bank_account_agency as Agencia
  ,cc1.bank_account_number||cc1.bank_account_check_number as Conta
  ,Cash_Transaction.amount/100 as Valor_do_Evento
  ,cast(pix_payer.document as string) as CPF_CNPJ_Cliente_conta_credito
  ,pix_payer.agency_number as Agencia_Fav
  ,pix_payer.account_number||"-"||pix_payer.account_check_number as Conta_Fav
  ,key_type as Tipo_de_chave_PIX
  ,pix.key_value as Chave_PIX
  ,pix_payer.name as Banco_Favorecido
  ,pix.type || '_PIX' as Tipo_Transacao

FROM `eai-datalake-data-sandbox.cashback.pix_in_payer` as pix_payer 
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix` as pix               
ON pix_payer.pix_id = pix.id 
LEFT JOIN `eai-datalake-data-sandbox.cashback.cash_transaction` as Cash_Transaction  
ON pix.cash_transaction_id = Cash_Transaction.id
LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_in_payee` as pix_payee         
ON pix.id = pix_payee.pix_id
LEFT JOIN `eai-datalake-data-sandbox.core.customers` as cl_fav            
ON pix_payee.payee_id = cl_fav.uuid
JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Desbloqueio_Massivo_2` as bd                
ON bd.cpf_completo = cl_fav.document
LEFT JOIN `eai-datalake-data-sandbox.payment.payment_customer_account` as cc1               
ON cc1.bank_token = bd.cpf_completo
where pix.status IN ('APPROVED')
and Cash_Transaction.amount > 0
and pix.type in ( 'CASH_IN','CASH_OUT') 
and pix_payer.name in ('MINISTERIO DO TRABALHO E PREVIDENCIA','PASEP','SECR. DA RECEITA FEDERAL -  LOTE 2023 05 - RMS 0115')
;