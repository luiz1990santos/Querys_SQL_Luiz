
--======================================================================================
--> MONITORAMENTO SAIDAS PIX CPF/CNPJ RECEBENDO MAIS DE UM CLIENTE - ULTIMOS 30 DIAS
--======================================================================================

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_alerta_pix_cpf_cnpj_distintos` 

-------------- consultar saida de PIX clientes

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_alerta_pix_cpf_cnpj_distintos` AS 

With
Base_base as (
            SELECT 
            distinct
            pix_payer.payer_id as  CustomerID
            ,cl.document as CPF_Origem
            ,pix_payer.name as ClienteOrigem
            ,pix_payee.name AS NomeFavorecido
            ,pix_payee.document AS CPF_Favorecido
            ,pix_payee.bank_name as Banco_Favorecido
            ,case
            when pix_payee.document = cl_Rec.document then 'ClienteEAI'
            else 'NaoClienteEAI' end as Flag_Cliente
            FROM `eai-datalake-data-sandbox.cashback.pix_payer`               pix_payer
            LEFT JOIN `eai-datalake-data-sandbox.core.customers`              cl                ON pix_payer.payer_id = cl.uuid
            LEFT JOIN `eai-datalake-data-sandbox.cashback.pix`                pix               ON pix_payer.pix_id = pix.id 
            LEFT JOIN `eai-datalake-data-sandbox.cashback.cash_transaction`   Cash_Transaction  ON pix.cash_transaction_id = Cash_Transaction.id
            LEFT JOIN `eai-datalake-data-sandbox.cashback.pix_payee`          pix_payee         ON pix.id = pix_payee.pix_id
            LEFT JOIN `eai-datalake-data-sandbox.core.customers`              cl_Rec            ON pix_payee.document = cl_Rec.document
            WHERE 
            date (Cash_Transaction.created_at)  >= (current_date() - 60)
            AND pix.`type` = 'CASH_OUT'
            AND pix.`status` = 'APPROVED'
            AND cl.status not in ('BLOCK','BLOCKED','UNBLOCK')
            --AND pix_payee.document = '04097820508'
            --AND pix_payer.payer_id in ('CUS-2e414b71-f8ac-47f9-a842-0aa3826e302b')
), Base_ClienteEai as (
            select
            CustomerID
            from Base_base where Flag_Cliente = 'ClienteEAI'
), Base_NaoClienteEai as (
            select
            CustomerID
            from Base_base where Flag_Cliente <> 'ClienteEAI'                
), Base_Consulta as (      
            select * from Base_ClienteEai   
            union all
            select * from Base_NaoClienteEai 
),Base_Favorecidos as (
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
                ,case
                when	char_length(pix_payee.document) >=14 then 'CNPJ'
                when	char_length(pix_payee.document) <=11 then 'CPF'
                else 'N/A' end as Flag_Favorecida
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
                join Base_Consulta                                                base              ON base.CustomerID = pix_payer.payer_id
                WHERE 
                date (Cash_Transaction.created_at)  >= (current_date() - 60)
                AND pix.`type` = 'CASH_OUT'
                AND pix.`status` = 'APPROVED'
                AND cl.status not in ('BLOCK','BLOCKED','UNBLOCK')
                --AND pix_payee.document = '04097820508'
                --AND pix_payer.payer_id in ('CUS-2e414b71-f8ac-47f9-a842-0aa3826e302b')
),base_favorecidos_10 as (
              select 
              FORMAT_DATE("%Y%m",DT_TRANX)as Safra_Tranx
              ,Favorecido
              ,CPF_Favorecido
              ,Flag_Favorecida
              ,Banco_Favorecido
              ,sum(Vl_Tranx) as Vl_Tranx
              ,count(distinct CPF_Origem) as Qtd_CPF_DIST
              ,(sum(Vl_Tranx)/ count(distinct CPF_Origem)) as TicketMedio
              from Base_Favorecidos
              group by 1,2,3,4,5
              order by 6 desc
),Base_Favorecidos_recebedor as (
            SELECT 

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
            ,case
            when	char_length(pix_payee.document) >=14 then 'CNPJ'
            when	char_length(pix_payee.document) <=11 then 'CPF'
            else 'N/A' end as Flag_Favorecida
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
            join base_favorecidos_10                                          receb             ON receb.CPF_Favorecido =  cl.document

            WHERE 
            date (Cash_Transaction.created_at)  >= (current_date() - 60)
            AND pix.`type` = 'CASH_OUT'
            AND pix.`status` = 'APPROVED'
            AND cl.status not in ('BLOCK','BLOCKED','UNBLOCK')
            --AND pix_payee.document = '04097820508'
            --AND pix_payer.payer_id in ('CUS-2e414b71-f8ac-47f9-a842-0aa3826e302b')
),base_favorecidos_11 as (
          select 
          FORMAT_DATE("%Y%m",DT_TRANX)as Safra_Tranx
          ,Favorecido
          ,CPF_Favorecido
          ,Flag_Favorecida
          ,Banco_Favorecido
          ,sum(Vl_Tranx) as Vl_Tranx
          ,count(distinct CPF_Origem) as Qtd_CPF_DIST
          ,(sum(Vl_Tranx)/ count(distinct CPF_Origem)) as TicketMedio
          from Base_Favorecidos_recebedor
          group by 1,2,3,4,5
          order by 6 desc
)
select
* 
from base_favorecidos_11 
where Qtd_CPF_DIST > 5 order by 6 desc


;
----------------------------------------------------------------------------------------------------------------------
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_relacao_clientes_origem_tranx`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_relacao_clientes_origem_tranx` AS 


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
                ,case
                when	char_length(pix_payee.document) >=14 then 'CNPJ'
                when	char_length(pix_payee.document) <=11 then 'CPF'
                else 'N/A' end as Flag_Favorecida
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
                join `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_alerta_pix_cpf_cnpj_distintos` rel on rel.CPF_Favorecido = pix_payee.document 
                WHERE 
                date (Cash_Transaction.created_at)  >= (current_date() - 100)
                AND pix.`type` = 'CASH_OUT'
                AND pix.`status` = 'APPROVED'
                AND cl.status not in ('BLOCK','BLOCKED','UNBLOCK')




