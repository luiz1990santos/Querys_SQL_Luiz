-- BASE FLUXO ONBOARDING - ALLOWME/ZAIG/ORBITAL
-- 29_Qry_Fluxo_Onboarding_v1


/*
select distinct user_id as CPF,Flag_Perfil from `eai-datalake-data-sandbox.analytics_prevencao_fraude.FunilOnboarding_0` where Flag_Decisao_Orbitall is null and Flag_Decisao_Zaig is null

select distinct Flag_Perfil, count(Distinct user_id) qtd from `eai-datalake-data-sandbox.analytics_prevencao_fraude.FunilOnboarding_0` where Flag_Decisao_Orbitall is null and Flag_Decisao_Zaig is null group by 1
*/

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.FunilOnboarding_0` AS 

with

base as (

select
distinct
FORMAT_DATETIME("%Y%m",created_at_dtAllowme) as Safra_AllowMe
,date(created_at_dtAllowme) as Dt_Cadastro_AllowMe
,FORMAT_DATETIME("%d",created_at_dtAllowme) as Dia_Cadastro_AllowMe
,a.user_id
,case 
  when pf.Cpf_Resticao_Mot = cast(a.user_id as numeric)  then 'OperacaoPF'
  when vip.CPF = a.user_id then 'VIP'
  when uber.cpf = a.user_id then 'UBER'
  else 'URBANO'end as Flag_Perfil
,case when cast(zaig.cpf_zaig as numeric) = Cast(a.user_id as numeric) then 'AllowMe/Zaig' else 'AllowMe' end as Flag_Origem
,case when a.fraud = 'false' then 'Aprova' else 'Negado' end as Flag_Dec_1RegraAllowMe
,case when loyalty.CPF = a.user_id then 'Loyalty_KMV' Else 'SemCadastro' end as Flag_Cadastro_Loyalty
--,case when Cod_Regra_1Regra in ('9','12','16','17','28','34','39','48','51') then 'Nega' else 'Aprova' end as Flag_Dec_1RegraAllowMe
,FORMAT_DATETIME("%Y%m",zaig.Dt_Zaig) as Safra_Zaig
,Flag_Decisao_Motor as Flag_Decisao_Zaig
,FORMAT_DATETIME("%Y%m",created_at) as Safra_Conta
,cl.type as Flag_Decisao_Orbitall
,cl.StatusConta
,cl.message
,case when Cast(bio.CPF as numeric) = Cast(a.user_id as numeric) then 'BioAprovada' else 'N/A' end as Flag_Bio
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_bd_AllowMe_Cadastro` a 
left join (
                SELECT 
                distinct
                        date(idcl.created_at) as Dt_Abertura
                        ,idcl.uuid as customer_id
                        ,idcl.document as cpf
                        ,idcl.full_name as Nome
                        ,idcl.status as Status_Conta

                from `eai-datalake-data-sandbox.core.customers`  idcl
                join `eai-datalake-data-sandbox.payment.payment`  trn on trn.customer_id = idcl.uuid
                join `eai-datalake-data-sandbox.core.orders` orders on trn.order_id = orders.uuid
                join `eai-datalake-data-sandbox.core.order_benefit`  Order_Benefit ON orders.`id` = Order_Benefit.`order_id`
                where  
                --date(orders.created_at) >= current_date - 90
                date(orders.created_at) >= current_date - 90
                and (Order_Benefit.origin_type = 'EAI:UBER'  or upper(Order_Benefit.description) LIKE '%UBER%')
                and order_status = 'CONFIRMED'
) uber on uber.cpf = a.user_id
left join (select distinct CPF from `eai-datalake-data-sandbox.loyalty.tblParticipantes` where Vip is not null and Inativo = false) as vip on vip.CPF = a.user_id
left join  `eai-datalake-data-sandbox.analytics_prevencao_fraude.TB_MONIT_CPF_OP_PF` pf on pf.Cpf_Resticao_Mot = cast(a.user_id as numeric) 
left join `eai-datalake-data-sandbox.loyalty.tblParticipantes` loyalty on loyalty.CPF = a.user_id
left join (
            with
            base as (
                    select
                    distinct
                    zaig.Cpf_Cliente as cpf_zaig
                    ,zaig.data_cadastro as Dt_Zaig
                    ,RANK() OVER (PARTITION BY zaig.cpf ORDER BY  zaig.data_cadastro desc) AS Rank_Ult_Decisao
                    ,case
                    when decisao = "automatically_approved" then 'Aprovado'
                    when decisao = "automatically_reproved" then 'Negado'
                    else 'EP' end as Flag_Decisao_Motor
                    from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` zaig
            )select * from base where Rank_Ult_Decisao = 1
) zaig on  cast(zaig.cpf_zaig as numeric) = Cast(a.user_id as numeric)
left join (
            select 
            distinct
            CustAccount.customer_id
            ,cl.status as StatusConta
            ,cl.created_at
            ,CustAccount.status
            ,CustAccount.created_at as Dt_Orbiatall
            ,Cl.document as cpf_Orbitall
            ,Cl.full_name as NomeCliente
            ,AccountEv.type
            ,AccountEv.message

            from `eai-datalake-data-sandbox.core.customers` Cl 
            join `eai-datalake-data-sandbox.payment.customer_account` CustAccount on CustAccount.customer_id = Cl.uuid
            join (select distinct * from `eai-datalake-data-sandbox.payment.customer_account_event` 
                  where type in ('APPROVED','DENIED')) AccountEv on AccountEv.customer_account_id = CustAccount.id 
          ) Cl on Cast(a.user_id as numeric) = Cast(Cl.cpf_Orbitall as numeric) 
left join (select 
                distinct
                CPF
           from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_4`
           where Resultado = 'Aprovado' 
           ) bio on Cast(bio.CPF as numeric) = Cast(a.user_id as numeric)
where date(created_at_dtAllowme) >=  current_date - 90
) --select  Safra_CadastroAllowMe,Flag_Decisao1Regra, Flag_Origem, count(distinct user_id) as qtdCpf from base 
--group by 1,2,3
select * from base 
--where 
--Flag_Dec_1RegraAllowMe = 'Aprova' 
--and Flag_Origem = 'AllowMe' ---AllowMe/Zaig
--and Flag_Decisao_Orbitall is null

order by 1 desc


;

--- informações do KM cadastro no KM

-- select * from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Base_CadastroKM`


CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Base_CadastroKM` AS 

select
distinct
 
loyalty.ParticipanteID
,loyalty.DataCadastro as Dt_CadastroLoyalty
,FORMAT_DATETIME("%Y%m",loyalty.DataCadastro) as Safra_KM
,loyalty.CPF
,loyalty.Nome
,loyalty.Email
,loyalty.DatadeNascimento
,loyalty.TipoOrigemID
,loyalty.flgPreCadastro
,loyalty.Vip
,loyalty.Inativo
,detalh.CEP
,detalh.Endereco
,detalh.Numero
,detalh.Complemento
,detalh.Cidade
,detalh.Estado
,detalh.DDDCel
,detalh.Celular
,detalh.PostoFavoritoID
,detalh.PerfilGeralID
,detalh.CompletouCadastro
,detalh.AceitouRegulamento
,detalh.AceitouTermo
,detalh.vipFlag
,Cat.Descricao
,Cat.Observacao
,Cat.Ativo
,Cat.DataCriacao as Dt_CadastroKM
,Orig_part.TipoOrigem
,case
when loyalty.CPF = Cl.document then 'Cliente_EAI' else 'KM' end as Flag_Plataforma
,Cl.Status as StatusContaEAI

from `eai-datalake-data-sandbox.loyalty.tblParticipantes` loyalty
left join `eai-datalake-data-sandbox.loyalty.tblParticipantesDetalhes` detalh on detalh.ParticipanteID = loyalty.ParticipanteID
left join `eai-datalake-data-sandbox.loyalty.tblCategoriaConsumidorParticipante` cat_Cons on cat_Cons.ParticipanteId = loyalty.ParticipanteID
left join `eai-datalake-data-sandbox.loyalty.tblCategoriaConsumidor` Cat on cat_Cons.CategoriaConsumidorId = Cat.CodigoExterno
left join `eai-datalake-data-sandbox.loyalty.tblParticipantesTipoOrigem` Orig_part on Orig_part.TipoOrigemID = loyalty.TipoOrigemID
left join `eai-datalake-data-sandbox.core.customers` Cl on loyalty.CPF = Cl.document

where 
date(loyalty.DataCadastro) >= '2023-01-01'
--and loyalty.Inativo = false
--and loyalty.CPF = '31001127846'
order by 2

;


--- informações do KM cubo para alimentar o Dash

CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Base_CadastroKM_Cubo` AS 


select
distinct
Safra_KM
,CompletouCadastro
,flgPreCadastro
,case when Vip is not null then 'Vip'else 'Urbano' end as Flag_Perfil
,Descricao
,Inativo
,TipoOrigem
,Flag_Plataforma
,StatusContaEAI
,count(distinct CPF) as Qtd_Cliente_KM

From `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Base_CadastroKM` 
group by 1,2,3,4,5,6,7,8,9