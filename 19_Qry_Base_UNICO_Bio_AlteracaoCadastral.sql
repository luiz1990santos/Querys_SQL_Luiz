--======================================================================================
--> Importar dados Unico (Validação Biometria e Atualização Cadastral (Telefone))

--> Importar Acompanhamento de Mensagens - Unico - Critério Ultima extração -2dias

-- Tabela_Unico_SMS_BIO_Telefone
--======================================================================================

/*select distinct DATE(DATA_ENVIO_SMS), COUNT(*) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone_Hist`
where date(DATA_ENVIO_SMS) >= '2024-03-01'
group by 1
order by 1 desc

select distinct DATE(DATA_ENVIO_SMS), COUNT(*) FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone`
where date(DATA_ENVIO_SMS) >= '2024-03-01'
group by 1
order by 1 desc*/

-- select max(DATA_ENVIO_SMS)as max, min(DATA_ENVIO_SMS) as min FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone`
-- select * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone`
-- select max(DATA_ENVIO_SMS)as max, min(DATA_ENVIO_SMS) as min  FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone`
-- select max(DATA_ENVIO_SMS)as max, min(DATA_ENVIO_SMS) as min FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone_Hist`




--========================================================================================================
--> LE OS DADOS IMPORTADOS DO CSV DE ANÁLISE E MARCA A APROVAÇÃO OU REPROVAÇÃO DA BIOMETRIA
--========================================================================================================

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_analise_Aprovacao_Unico`
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_class_Unico` WHERE CPF = 2292160056


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_class_Unico` AS  

with
Base as (

        SELECT DISTINCT
        a.*

        ,CASE WHEN LIVENESS <> 'Liveness validado com sucesso' THEN 'REPROVADO'
        ELSE 
        CASE         
          WHEN FACEMATCH = 'FaceMatch validado com sucesso' AND SCORE >=10 THEN 'APROVADO'
          WHEN FACEMATCH <> 'FaceMatch validado com sucesso' AND SCORE >=50 THEN 'APROVADO'
        else 'REPROVADO'
        end 
        end as STATUS_BIOMETRIA
        --,RANK() OVER (PARTITION BY a.CPF ORDER BY a.DATA_ENVIO_SMS desc) AS Rank_Ult_Atual
        from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tabela_Unico_SMS_BIO_Telefone_Hist` a
        --WHERE ENVIO_SMS ='Captura Concluída'
        Where SCORE is not null
        --and CPF = 11472796756

), Base_1 as (
        select 
        a.*  
        ,RANK() OVER (PARTITION BY a.CPF ORDER BY a.DATA_ENVIO_SMS desc) AS Rank_Ult_Atual  
        from Base a
        --where STATUS_BIOMETRIA = 'APROVADO' 
) select * from Base_1        
  where Rank_Ult_Atual = 1 
  --and cpf = 100064
  order by 2 
;

--======================================================================================
--> ENRIQUECE COM DADOS DO KMV E DA CONTA EAI
--======================================================================================

-- select SCORE, count(*) from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Enrequecidas_BIO_Alteracao_Cadastro` group by 1

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Enrequecidas_BIO_Alteracao_Cadastro` AS 

with
  base as (

      SELECT 
      distinct
        novo_tel.DATA_ENVIO_SMS
        ,novo_tel.CPF
        --,novo_tel.CPF_COMPLETO
        ,novo_tel.NOME
        --,novo_tel.E_MAIL
        --,novo_tel.PEDIDO
        ,novo_tel.TEMPLATE
        ,novo_tel.ENVIO_SMS
        ,novo_tel.ETAPA_SMS
        ,novo_tel.LIVENESS
        ,novo_tel.TIPIFICACAO
        ,novo_tel.FACEMATCH
        ,novo_tel.OCRCODE
        ,novo_tel.RESULTADO_ANALISE
        ,novo_tel.DURACAO
        ,novo_tel.SCORE
        --,novo_tel.DataInclusaoBase
        ,novo_tel.STATUS_BIOMETRIA
        --,novo_tel.Rank_Ult_Atual
        --,novo_tel.TELEFONE as New_Telefone
        ,LPAD(CAST(kmv.CPF AS STRING),11,'0') as CPFKMV
        ,kmv.Nome as NomeKMV
        ,kmv.Email as EmailKMV
        ,kmv.DatadeNascimento AS DataNascimentoKMV
        ,eai.CustomerID as CustomerIDEAI
        ,eai.status as StatusEAI
        ,case
          when eai.CustomerID is null then 'ClienteKMV'
          when eai.CustomerID is not null then 'ClienteEAI'
          end as Flag_Cliente
        ,replace(novo_tel.TELEFONE,"-","") as New_Telefone
        ,concat("+55 (", kmv_detalhe.DDDCel,") ",replace(kmv_detalhe.Celular,"-","")) as CelularKMV
        ,concat("+55 (", eai.DDD,") ",replace(eai.Telefone,"-","")) as CelularEAI
        
        ,case
        when replace(novo_tel.TELEFONE,"-","") = concat("+55 (", kmv_detalhe.DDDCel,") ",replace(kmv_detalhe.Celular,"-",""))  then '01' 
        else '00' end as Flag_Atualizacao_KMV

        ,case
        when replace(novo_tel.TELEFONE,"-","") = concat("+55 (", eai.DDD,") ",replace(eai.Telefone,"-",""))  then '01' 
        else '00' end as Flag_Atualizacao_EAI
        ,case
        when replace(novo_tel.TELEFONE,"-","") = concat("+55 (", kmv_detalhe.DDDCel,") ",replace(kmv_detalhe.Celular,"-","")) 
        and replace(novo_tel.TELEFONE,"-","") = concat("+55 (", eai.DDD,") ",replace(eai.Telefone,"-","")) then '01'
        else '00' end as Flag_Atualizacao_todos



      FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Crivo_class_Unico`  novo_tel
      
      LEFT JOIN `eai-datalake-data-sandbox.loyalty.tblParticipantes` kmv 
      ON  CAST(kmv.CPF AS numeric) = novo_tel.CPF 
          AND kmv.CargoID = 1

      LEFT JOIN `eai-datalake-data-sandbox.loyalty.tblParticipantesDetalhes` kmv_detalhe
      ON kmv.ParticipanteID = kmv_detalhe.ParticipanteID

      LEFT JOIN (
            with 
            base_cliente_EAI as (
            SELECT 
            distinct
            cl.own_id
            ,cl.uuid as  CustomerID
            ,cl.full_name as Nome
            ,cl.document as CPF
            ,cl.email
            ,en.street as Rua
            ,en.neighborhood as Bairro
            ,en.city as Cidade
            ,en.state as UF
            ,FORMAT_DATE("%d-%m-%Y",cl.created_at)as DataCriacao
            ,FORMAT_DATE("%Y%m",cl.created_at)as Safra_Abertura
            ,FORMAT_DATE("%Y",cl.created_at)as Ano
            --,cl.created_at as DataCriacao
            ,ph.area_code||ph.number as DDD_Celular
            ,ph.area_code as DDD
            ,ph.number as Telefone
            ,ph.type as Tipo_Telefone
            ,cl.trusted as Trusted
            ,cl.status
            ,cl.risk_analysis_status as RiskAnalysis
            ,ev.observation as MotivoStatus
            ,ev.event_date as DataStatus
            ,ev.user_name as UsuarioStatus
            ,RANK() OVER (PARTITION BY cl.uuid ORDER BY en.id, id.phone_id  desc) AS Rank_Ult_Atual
            ,CASE
            WHEN en.state IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
            WHEN en.state IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
            WHEN en.state IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
            WHEN en.state IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
            WHEN en.state IN ('SC','PR','RS') THEN 'SUL'
            ELSE 'SUL'
            END AS REGIAO

            FROM `eai-datalake-data-sandbox.core.customers`             cl
            left join `eai-datalake-data-sandbox.core.address`          en on en.id = cl.address_id
            left join `eai-datalake-data-sandbox.core.customer_phone`   id on id.customer_id = cl.id
            left join `eai-datalake-data-sandbox.core.phone`            ph on id.phone_id = ph.id 
            left join `eai-datalake-data-sandbox.core.customer_event`   Ev on ev.customer_id = cl.id
            where 
            ph.type = 'MOBILE'
            and cl.status = 'ACTIVE'
            ) select * from base_cliente_EAI where Rank_Ult_Atual = 1
        )eai  on cast(kmv.ParticipanteID as string)= eai.own_id

  ), base_1 as (
    select 
    distinct
    a.* 
    ,case
    when a.Flag_Atualizacao_KMV  = '01' and a.Flag_Atualizacao_EAI = '01' and a.Flag_Atualizacao_todos = '01' then 'Atualizado'
    when a.Flag_Atualizacao_KMV  = '00' and a.Flag_Atualizacao_EAI = '00' and a.Flag_Atualizacao_todos = '00' then 'Nao_Atualizado'
    when a.Flag_Atualizacao_KMV  = '00' and a.Flag_Atualizacao_EAI = '01'  then 'Atualizado_EAI_Pend_KMV'
    when a.Flag_Atualizacao_KMV  = '01' and a.CustomerIDEAI is null then 'Atualizado_KMV'
    when a.Flag_Atualizacao_KMV  = '01' and a.Flag_Atualizacao_EAI = '00' then 'Atualizado_KMV_Pend_EAI'
    when a.Flag_Atualizacao_KMV  = '01' and a.Flag_Atualizacao_EAI = '01'  then 'Atualizado_KMV_EAI'
    else 'Verificar' end as flag_Atualizacao
    from base a

  ) select
  DATA_ENVIO_SMS as DATA
  ,CPF
  --,CPF_COMPLETO
  ,NOME
  ,TEMPLATE
  ,ENVIO_SMS
  ,ETAPA_SMS
  ,LIVENESS
  ,TIPIFICACAO
  ,FACEMATCH
  ,OCRCODE
  ,RESULTADO_ANALISE
  ,DURACAO
  ,case 
    when SCORE = -9000 then -90
    when SCORE = -1000 then -10
    when SCORE = 5000 then 50
    when SCORE = 6000 then 60
    when SCORE = 7000 then 70
    when SCORE = 8000 then 80
    when SCORE = 9000 then 90
    when SCORE = 9500 then 95
    else SCORE
  end as SCORE
  ,STATUS_BIOMETRIA
  ,CPFKMV
  ,NomeKMV
  ,EmailKMV
  ,DataNascimentoKMV
  ,CustomerIDEAI
  ,StatusEAI
  ,Flag_Cliente
  ,New_Telefone
  ,CelularKMV
  ,CelularEAI
  --,Flag_Atualizacao_KMV
  --,Flag_Atualizacao_EAI
  --,Flag_Atualizacao_todos
  ,flag_Atualizacao
  ,CASE 
          -- REPROVADOS BIO
          WHEN STATUS_BIOMETRIA = 'REPROVADO' THEN '0. REPROVADO'
      ELSE 
          CASE
              -- CONTA BLOQUEADA ABASTECE   
              WHEN StatusEAI ='BLOCKED' THEN '4. CONTA ABASTECE-AI BLOQUEADA'

              -- CADASTRO INCOMPLETO NO KMV   
              WHEN CustomerIDEAI is NULL AND (NomeKMV IS NULL OR EmailKMV IS NULL OR DataNascimentoKMV IS NULL) THEN '1. CADASTRO INCOMPLETO KMV'

              -- TELEFONE JÁ OK NO KMV 
              WHEN replace(New_Telefone,"-","") = CelularKMV AND replace(New_Telefone,"-","") = CelularEAI THEN '2. TELEFONE JÁ ATUALIZADO'
              WHEN replace(New_Telefone,"-","") = CelularKMV  and CustomerIDEAI IS NULL THEN '2. TELEFONE JÁ ATUALIZADO'
              WHEN replace(New_Telefone,"-","") <> CelularKMV and CustomerIDEAI IS NULL THEN '3. ATUALIZAR TELEFONE NO KMV'
              WHEN replace(New_Telefone,"-","") <> CelularEAI AND CustomerIDEAI IS NOT NULL THEN '5. ATUALIZAR TELEFONE NO BACKOFFICE'
              ELSE 
                  CASE 
                      WHEN CustomerIDEAI IS NULL AND replace(New_Telefone,"-","") <> CelularKMV THEN '3. ATUALIZAR TELEFONE NO KMV'
                      WHEN CustomerIDEAI <> CelularKMV AND replace(New_Telefone,"-","") <> CelularKMV THEN '3. ATUALIZAR TELEFONE NO KMV'
                      ELSE '5. ATUALIZAR TELEFONE NO BACKOFFICE'
                  END
          END
      END AS ACAO_FINAL
  from base_1
  ;

--======================================================================================
--> MONITORAMENTO CLIENTES TRANSACIONANDO OUT >=1500 QUANTIDADE NO MES - 30 DIAS
--======================================================================================


--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_biometria_resultado_historico_2`
--select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_alerta_bloqueio_BV` where cpf = 2021398323 

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_base_alerta_bloqueio_BV` AS 

with
    Base_Face_Cadastrada as (

                    select
                    distinct

                      cl.uuid as CustomerId
                      ,cl.document as Cpf
                      ,cl.status as Status_Conta 
                      ,ev.event_date as Dt_Evento
                      ,ev.user_name as Analista
                      ,Ev.status as Status_Ev
                      ,case
                        when ev.status = 'FACIAL_BIOMETRICS_VALIDATED' then 'BIO_APR'
                        when ev.status = 'FACIAL_BIOMETRICS_REJECTED' then 'BIO_NEG'
                        else 'BIO_SEM' end as Flag_Bio
                      ,FORMAT_DATETIME("%Y%m",ev.event_date) as Safra_Evento
                      ,RANK() OVER (PARTITION BY cl.document ORDER BY ev.event_date  desc) AS Rank_Ult_Status
                    from `eai-datalake-data-sandbox.core.customers`             cl
                    left join  `eai-datalake-data-sandbox.core.customer_event`  Ev on ev.customer_id = cl.id
                    where 
                    ev.status in ('FACIAL_BIOMETRICS_VALIDATED', 'FACIAL_BIOMETRICS_REJECTED')
                    --and cl.document = '04267903913'
                    order by 1
    ), Base_Face_Cadastrada1 as (
                    select
                    *
                    from Base_Face_Cadastrada
                    where Rank_Ult_Status = 1
    ),
    Base_Dados as (
    SELECT
      DISTINCT
      FORMAT_DATETIME('%Y%m',a.created_transaction_at) as Safra
      ,a.customer_id
      ,cast(cl.document as INTEGER) as cpf
      --,date(cl.created_at) as dt_conta
      --,current_date as mis_date
      ,CASE
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=10   THEN '01_00-10DIAS'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=30   THEN '02_11-30DIAS'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=60   THEN '03_31-60DIAS'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=90   THEN '04_61-90DIAS'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=180  THEN '05_91-180DIAS'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) <=364  THEN '06_180-1ANO'
      WHEN DATETIME_DIFF(DATETIME(current_date), DATETIME(FORMAT_DATE("%Y-%m-%d",cl.created_at)), DAY) >=365  THEN '07_+1ANO'
      END AS Temp_Conta
      ,case
      when ub.customer_id = a.customer_id then 'ClienteUber'
      else 'ClienteUrbano' end as Flag_Cliente
      ,cl.status as Status_Conta
      ,a.flow
      ,Count (a.uuid) AS Quantidade
      ,Sum(a.amount)/100 as Vl_Tranx

    FROM `eai-datalake-data-sandbox.elephant.transaction` a
    join `eai-datalake-data-sandbox.core.customers`       cl on cl.uuid = a.customer_id
    left join `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias` ub on ub.customer_id = a.customer_id
    WHERE 
      date(a.created_transaction_at) >= current_date - 30
      AND a.status IN ('APPROVED')
      AND a.type = 'CASH_OUT' 
      AND a.flow IN ('BILLET', 'PIX', 'TED') 
      GROUP BY 1,2,3,4,5,6,7
      --HAVING Quantidade >= 4
      ORDER BY 2 desc 
    ), Base_Valor as (
    select * from Base_Dados 
    where
    Status_Conta = 'ACTIVE'
    and Vl_Tranx >= 1500
    ), base_Cadastro_aprovado as (
    -- validacao cadastral
                          with
                          base as (
                          select 
                          distinct
                          a.* 
                          ,case when CustomerIDEAI is null then 'ClienteKMV' else 'ClienteEAI' end as Flag_Cliente_Origem
                          ,case 
                              when a.LIVENESS = 'Liveness validado com sucesso' and (a.FACEMATCH = 'FaceMatch validado com sucesso' and a.SCORE >=10) then 'Aprovado'
                              else 'Negado' end as Flag_Decisao_Crivo
                          ,RANK() OVER (PARTITION BY a.CPF ORDER BY DATA  desc) AS Rank_Ult_Atualx
                          from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Enrequecidas_BIO_Alteracao_Cadastro` a
                          where LIVENESS = 'Liveness validado com sucesso'
                          and FACEMATCH = 'FaceMatch validado com sucesso' 
                          and ACAO_FINAL in ('2. TELEFONE JÁ ATUALIZADO', '4. CONTA ABASTECE-AI BLOQUEADA')
                          --and cpf = 11555457614
                          ) select * from base where Rank_Ult_Atualx = 1 
    ), base_consolidada as (select
                            a.*
                            --,b.CPF_COMPLETO
                            ,case when a.cpf = b.cpf then 'Documento/Liveness/FaceMatch - OK'
                            else 'Bloquear_Cadastro' end as Flag_Bloquear_BV
                            ,case
                            when Vl_Tranx <= 1500 then '01-até1.500'
                            when Vl_Tranx <= 2000 then '02-até2.000'
                            when Vl_Tranx <= 2500 then '03-até2.500'
                            when Vl_Tranx <= 3000 then '04-até3.000'
                            when Vl_Tranx <= 3500 then '05-até3.500'
                            when Vl_Tranx <= 4000 then '06-até4.500'
                            when Vl_Tranx <= 4500 then '07-até4.500'
                            when Vl_Tranx > 4500  then '08-Maior4.500'
                            end as Flag_Faixa_Valor
                            ,Vl_Tranx/Quantidade as TicketMedio
                            ,case when blm.CPF = a.cpf then 'BloqueioMassivo' else 'NaoBloqueioMassivo' end as Flag_Bloq_Massivo
                            ,blm.Motivo


                            from Base_Valor a 
                            left join base_Cadastro_aprovado b on a.cpf = b.cpf
                            left join (with
                                base_bloqueioMassivo as (
                                SELECT
                                distinct
                                CustomerID
                                ,CPF
                                ,Lote
                                ,Motivo
                                ,RANK() OVER (PARTITION BY CPF ORDER BY Lote desc) AS Rank_Bloqueio

                                FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Clientes_Bloqueio_Massivo` 
                                order by 1
                                ) select * from base_bloqueioMassivo where Rank_Bloqueio = 1) blm on blm.CPF = a.cpf
                            
                             )
    select 
    base.* 
    ,bio.Flag_Bio
    ,bio.Safra_Evento
    from base_consolidada base
    left join Base_Face_Cadastrada1 bio on cast(bio.Cpf as String) = cast(base.cpf as String)
    ;

--======================================================================================
--> BASE DADOS ALTERACAO CADASTRAL TELEFONE - VALIDACAO BIO PARA ALTERACAO

--======================================================================================

-- SELECT * FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Base_Alteracao_Telefone` 

-- SELECT MAX (DATA )FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Enrequecidas_BIO_Alteracao_Cadastro`


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Base_Alteracao_Telefone`  AS

SELECT 
  DISTINCT
  DATA
  ,FORMAT_DATETIME('%R',DATA) AS hh_mm
  ,CPF
  --,CPF_COMPLETO
  ,NOME
  ,TEMPLATE
  ,ENVIO_SMS
  ,ETAPA_SMS
  ,LIVENESS
  ,TIPIFICACAO
  ,FACEMATCH
  ,OCRCODE
  ,RESULTADO_ANALISE
  ,DURACAO
  ,SCORE
  ,STATUS_BIOMETRIA
  ,CPFKMV
  ,NomeKMV
  ,EmailKMV
  ,DataNascimentoKMV
  ,CustomerIDEAI
  ,StatusEAI
  ,Flag_Cliente
  ,New_Telefone
  ,CelularKMV
  ,CelularEAI
  ,flag_Atualizacao
  ,ACAO_FINAL

  FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Enrequecidas_BIO_Alteracao_Cadastro` 
  WHERE TEMPLATE = 'atualizacao telefone'
  and DATE(DATA) >= '2022-01-01' 
  --AND FORMAT_DATETIME('%R',DATA) > '17:17'
    --AND  CPF = 36944363836
  ORDER BY 1

;
--======================================================================================
--> INSERE OS DADOS NA TABELA PARA SUBIDA NO BKO
--======================================================================================
-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_biometria_subida_BKO`

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.tb_biometria_subida_BKO` AS 

    SELECT  
     distinct
        CPF as CPF_COMPLETO,
        CASE STATUS_BIOMETRIA
            WHEN "APROVADO" THEN "A"
            WHEN "REPROVADO" THEN "B"
        END AS BiometriaFacial,
        CONCAT(FORMAT_DATE("%d/%m/%Y",CURRENT_DATE)," 00:00:00") as DataHoraValidacao,
        DATA as Dt_Env_SMS,
        case when ev.CustomerIDEAI = cl.CustomerIDEAI then 'BioCadastraBKO' else
        'NaoCadastrada' end as FlagBio_BKO,
        Flag_Cliente

 
        
    FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Enrequecidas_BIO_Alteracao_Cadastro` cl

    left join (SELECT cl.uuid as CustomerIDEAI, ev.status as StatusEv from `eai-datalake-data-sandbox.core.customer_event` ev
    join  `eai-datalake-data-sandbox.core.customers`       cl on ev.customer_id = cl.id
     where ev.status in ('FACIAL_BIOMETRICS_VALIDATED','FACIAL_BIOMETRICS_NOT_VALIDATED','FACIAL_BIOMETRICS_REJECTED')) Ev on ev.CustomerIDEAI = cl.CustomerIDEAI
    where DATE(DATA) >= '2022-01-01' 
    --AND FORMAT_DATETIME('%R',DATA) > '17:17'
    ORDER BY 2 DESC
;




