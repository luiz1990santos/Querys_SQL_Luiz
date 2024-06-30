
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
                            ,ph.country_code as code
                            ,ph.area_code as DDD
                            ,ph.number as Telefone
                            ,ph.country_code||ph.area_code||ph.number as tel_completo 
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
), base1 as ( 
       select 
          distinct
           tel_completo,
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
       --and ddd = '31'
       
      --and Tipo_Tel = 'MOBILE'
      and CPF_Cliente in ( 
'19960368823',
'21568435894',
'05135751890',
'44372289871',
'35047611809',
'97192104604',
'29804726890',
'34221968893',
'09497338651',
'02570213705',
'15825191100',
'05881519809',
'11439959889',
'07375957959',
'01389613607',
'03839073650',
'02851489666',
'30944072852',
'02966257954',
'06407493994',
'38865828846',
'05217623675',
'05692131854',
'95508520830',
'89633326915',
'18215036864',
'36828540883',
'25301277863',
'51228319120',
'74672096815',
'08303817833',
'23145892894',
'83133429949',
'96631350697',
'88166430720',
'07767337880',
'31607864800',
'01288190140',
'37493755850',
'94171203872',
'00586771948',
'04106480190',
'00727786954',
'87069539487',
'69749698134',
'05491120812',
'26285665087',
'04410204963',
'05287696930',
'20440553687',
'32477910272'
)

 ) ,Base_Key as (
      SELECT 
      distinct

      id_key.pix_key_id 
      ,key.key_value
      ,clkey.customer_id
      ,RANK() OVER (PARTITION BY key.key_value ORDER BY key.created_at  desc) AS Rank_key
      ,id_key.payment_customer_account_id
      ,key.id
      ,FORMAT_DATE("%Y%m",key.created_at)as Safra_Cad_Key
      ,key.created_at
      ,key.type
      ,key.uuid
      ,key.reason
      ,key.status
      ,pca.payment_account_id

      FROM `eai-datalake-data-sandbox.payment.payment_customer_account_pix_key`   id_key
      join `eai-datalake-data-sandbox.payment.pix_key`                            key     on id_key.pix_key_id = key.id
      join `eai-datalake-data-sandbox.payment.customer_account`                   clkey   on clkey.payment_customer_account_id = id_key.payment_customer_account_id
      join `eai-datalake-data-sandbox.payment.payment_customer_account`           pca     on clkey.payment_customer_account_id = pca.id
), Base_Key1 as (
    select
    distinct
    *
    from Base_Key
    where status not in ('EXCLUDED','ERROR')
    and Rank_key = 1
    order by 2,3
 ) select * from base1 as b1
left join Base_Key1  as bk
on b1.CustomerID = bk.customer_id

 ;