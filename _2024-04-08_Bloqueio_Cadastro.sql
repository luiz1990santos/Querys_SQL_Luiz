       with
              base_clientes_bloqueados as (
                     select
                     distinct
                            cl.uuid as  CustomerID
                            ,cl.full_name as Nome_Completo
                            ,cl.document as CPF_Cliente
                            ,cl.birth_date as Nascimento
                            ,cl.email as email
                            ,case 
                               when cl.status = 'ACTIVE' then 'ATIVA'
                               when cl.status = 'BLOCKED' then 'BLOQUEADA'
                               when cl.status = 'MINIMUM_ACCOUNT' then 'CONTA BÁSICA'
                               when cl.status = 'INACTIVE' then 'INATIVA'
                             end as StatusConta
                            ,ph.area_code as DDD
                            ,ph.number as Telefone
                            ,ph.type as Tipo_Tel
                            ,en.zipcode as Cep
                            ,en.street as Rua
                            ,en.neighborhood as Bairro
                            ,en.city as Cidade_Cliente
                            ,en.state as UF_Cliente
                            ,cl.created_at as Dt_Abertura
                            ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
                            ,case
                            when cl.trusted = 1 then 'Trusted'
                            else 'NaoTrusted' end as Flag_Trusted
                            ,RANK() OVER (PARTITION BY cl.uuid ORDER BY ev.event_date desc) AS Rank_Ult_Atual
                            ,ev.status as StatusEvento
                            ,ev.observation as MotivoStatus
                            ,ev.event_date as DataStatus
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

              ), base_3 as ( 
       select 
          distinct
           CustomerID,
           Nome_Completo,
           CPF_Cliente,
           StatusConta,
           Flag_Email,
           Flag_Celular,
           ScoreZaig,
           MotivoStatus,
           DataStatus,
           Dt_Abertura,
           UF_Cliente,
           Rank_Ult_Atual,
           StatusEvento,
           Flag_Biometria

       from base_clientes_bloqueados
       where Rank_Ult_Atual = 1
      -- and date_diff(current_date(), Dt_Abertura, DAY) <= 365
       and StatusConta = 'BLOQUEADA'
       and MotivoStatus = 'Bloqueio de cadastro'
       and Tipo_Tel = 'MOBILE'
       --
       -- order by Dt_Abertura 
), base_4 as (
   
select 
      b3.CPF_Cliente,
      b3.MotivoStatus,
      b3.DataStatus,
      b3.Flag_Email,
      b3.Flag_Celular,
      b3.Flag_Biometria,
      round(sum(T.valor),2) as SALDO

                -- SOMA DOS VALORES QUE COMPÕEM O SALDO
from base_3 as b3
left join `eai-datalake-data-sandbox.orbitall.tb_topaz` as T

on b3.Cpf_cliente = T.numerodocumento
where t.status = 'MOVIMENTAÇÃO EXECUTADA'       -- STATUS NECESSARIO DE TRANSACOES CONCLUIDAS    
and T.data <= CURRENT_DATE()-1          -- FIXAR COM A DATA COM A QUAL SE PRETENDE ANALISAR O SALDO    
and length(T.numerodocumento) <= 11     -- REMOVER CASO QUEIRA TRAZER TAMBÉM CNPJ
group by 1,2,3,4,5,6
 ) SELECT 
       *,       
       CASE 
         WHEN SALDO > 0 THEN 'Sim'
         ELSE 'Não'
      END as FLAG_SALDO
  FROM BASE_4 
  where date(DataStatus) >= '2024-05-13' 
-- select count(*) from base_3

-- volume 2.503
