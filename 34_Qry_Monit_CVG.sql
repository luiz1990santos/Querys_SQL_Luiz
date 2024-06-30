
CREATE OR REPLACE TABLE `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_Monit_CVG_Ult` AS 

with

Base_CVG as (

SELECT 
distinct
 simul.id as Id_Simul
--simul.uuid
--simul.partner_id
,simul.customer_id
,simul.value_loan as VlSimulacao
--,simul.count_installments
--,simul.due_date_first_installments
,simul.product_type
,simul.partner_quote_id
--,simul.waiting_day
--,simul.payment_type
--,simul.first_instalment
--,simul.zero_km_vehicle
,simul.flow_simulation_id
--,simul.vehicle_id
--,simul.proposal_loan_id
--,simul.accept_protocol
--,simul.customer_loan_id
,simul.status as Status_Simul
,simul.created_at as Dt_Simul
,FORMAT_DATETIME("%Y-%m",simul.created_at) as Safra_Simul
,simul.updated_at as Dt_SimulUpdated
,FORMAT_DATETIME("%Y-%m",simul.updated_at) as Safra_SimulUpdated
--,simul.version
,RANK() OVER (PARTITION BY simul.customer_id ORDER BY Resut.date) AS Rank_PrimeiraProposta
,RANK() OVER (PARTITION BY simul.customer_id ORDER BY Resut.date desc) AS Rank_UltimaProposta

,Resut.id as ResutId
--,Resut.uuid
,Resut.status as StatusResut
,Resut.date as DT_Resut
,Resut.value_total_installments
,Resut.value_installment
,Resut.total_loan as VlEmprestimo
,Resut.tax_loan
,Resut.installment_without_Insurance
,Resut.cost_price
,Resut.value_insurance
,Resut.value_total_installment_without_insurance
,Resut.value_percent_cet_annual
,Resut.interest_rate_first_installment
,Resut.iof
,Resut.tc
,Resut.tax_annual_finance
,Resut.value_percent_cet_month
,Resut.property_fee
,Resut.coeficiente_code
,Resut.simulation_quote_id
,Resut.tax_month_finance
,Resut.registration_fee
--,Resut.created_at 
--,Resut.updated_at
--,Resut.version

--,Clloan.id
--,Clloan.uuid
,Clloan.document as Cpf_Cliente
--,Clloan.type_people
--,Clloan.first_name
,Clloan.full_name as NomeCliente
--,Clloan.last_name
,Clloan.total_patrimony as Patrimonio
--,Clloan.nacionality
,Clloan.place_of_birth_city as Cidade
,Clloan.place_of_birth_state as UF
,Clloan.monthly_income as RendaMensal
--,Clloan.current_situation
,Clloan.gender as Genero
,Clloan.birth_date as Dt_Nascimento
,Case 
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<18   Then '01  MenorIdade'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=20  Then '02  18a20anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=25  Then '04  21a25anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=30  Then '05  26a30anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=35  Then '06  31a35anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=40  Then '07  36a40anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=45  Then '08  41a45anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=50  Then '09  46a50anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=55  Then '10 51a55anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=60  Then '11 56a60anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=65  Then '12 61a65anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=70  Then '13 66a70anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=75  Then '14 71a75anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=80  Then '15 76a80anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)<=85  Then '16 81a85anos'
  When DATETIME_DIFF(DATETIME(current_date), DATETIME(Clloan.birth_date), year)>85   Then '17 >86anos'  
End as Faixa_Idade

--,Clloan.marital_status
,Clloan.email
--,Clloan.mother_name
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
,case 
  when pf.Cpf_Resticao_Mot = cast(Clloan.document as numeric) then 'OpercaoPF'
  when vip.CPF = Clloan.document then 'VIP'
  when uber.cpf = Clloan.document then 'UBER'
  else 'URBANO'end as Flag_Perfil
,case when cl.Trusted = 1 then 'Trusted' else 'No Trusted' end as FlagTrusted
--,Clloan.occupation_code
--,Clloan.occupation_type
--,Clloan.created_at
--,Clloan.updated_at
--,Clloan.version

--,prop.id
--,prop.uuid
,prop.date as Dt_Prop
,FORMAT_DATETIME("%Y-%m",prop.date) as Safra_Prop
--,prop.product_type
,prop.number_proposal as NProp
,prop.status as StatusProp
,case 
when prop.status in ('CREDIT_APPROVED','PAYMENT_DONE','BV_CREDIT_CONTRACT','PENDING') then 'Aprovado'
when prop.status in ('BV_CREDIT_REFUSED','BV_WARRANTY_REFUSED') then 'Negado_BV'
when prop.status in ('DISAPPROVED') then 'Prop_Expirada'
when prop.status in ('DEKRA_REFUSED') then 'Negado_Garantia'
else 'Verificar' end as Flag_StatusProp
--,prop.accept_loan_id
,prop.number_protocol as NProtoc
,prop.number_contract as NContrato
--,prop.created_at
--,prop.updated_at
--,prop.version

--,vehic.id
--,vehic.uuid
,vehic.model_year
,vehic.manufactore_year
,vehic.category_description
,vehic.brand as Marca
,vehic.model as Modelo
,vehic.state_license_vehicle as UF_Veitculo
,case when Clloan.place_of_birth_state = vehic.state_license_vehicle then 'Mesmo_UF' else 'Diferente' end as Flag_Valida_Cliente_Veiculo
--,vehic.molicar_code
,vehic.renavan
,vehic.license_plate as PlacaVeiculo
,vehic.model_version as VersaoModelo
--,vehic.crv
--,vehic.created_at
--,vehic.updated_at
--,vehic.version

FROM `eai-datalake-data-sandbox.personal_loans.simulation_quote` simul
join `eai-datalake-data-sandbox.personal_loans.result_quote`      Resut on simul.id = Resut.simulation_quote_id
join `eai-datalake-data-sandbox.personal_loans.customer_loan`  Clloan on Clloan.id = simul.customer_loan_id
left join `eai-datalake-data-sandbox.core.customers`               cl on cl.document = Clloan.document
join `eai-datalake-data-sandbox.personal_loans.proposal_loan` prop on prop.id = simul.proposal_loan_id
join `eai-datalake-data-sandbox.personal_loans.vehicle` vehic on vehic.id = simul.vehicle_id
LEFT JOIN (select distinct CPF from `eai-datalake-data-sandbox.loyalty.tblParticipantes` where Vip is not null and Inativo = false) as vip on vip.CPF = Clloan.document
LEFT JOIN (select cpf from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_clientes_Uber_ultimos30dias`) as uber on uber.cpf = Clloan.document
LEFT JOIN `eai-datalake-data-sandbox.analytics_prevencao_fraude.TB_MONIT_CPF_OP_PF` pf on pf.Cpf_Resticao_Mot = cast(Clloan.document as numeric) 

where
date(prop.date) >= current_date - 120
--and 
--simul.status = 'OPENED'
--and Clloan.document = '31001127846'
--and 
--simul.customer_id = 'CUS-0e7b9bc9-c9f5-4c9b-93f1-f19c1f48d497'

order by 2,1

), Base_CVG_Simul as (
select
customer_id
,count(ResutId) as Qtd_Simul
from Base_CVG
group by 1
order by 2 desc

)

SELECT
Id_Simul
,customer_id
,Cpf_Cliente
,CASE
  WHEN UF IN ('AM','RR','AP','PA','TO','RO','AC') THEN 'NORTE'
  WHEN UF IN ('MA','PI','CE','RN','PE','PB','SE','AL','BA') THEN 'NORDESTE'
  WHEN UF IN ('MT','MS','GO','DF') THEN 'CENTRO-OESTE'
  WHEN UF IN ('SP','RJ','ES','MG') THEN 'SUDESTE'
  WHEN UF IN ('SC','PR','RS') THEN 'SUL'
  ELSE 'SUL'
END AS Regiao
,RendaMensal
,Genero
,email
,Faixa_Idade
,Flag_TempodeConta
,Flag_Perfil
,FlagTrusted

,date(Dt_SimulUpdated) as Dt_SimulUpdated
,Safra_SimulUpdated

,Status_Simul
,StatusResut
,StatusProp
,date(Dt_Prop) as Dt_Prop
,Safra_Prop
,Flag_StatusProp
,NProp

,Marca
,Modelo
,UF_Veitculo
,Flag_Valida_Cliente_Veiculo
,renavan
,PlacaVeiculo
,VersaoModelo

,VlEmprestimo


from Base_CVG
where Rank_UltimaProposta = 1


/*
SELECT
Flag_StatusProp
,count(*)
from Base_CVG
where Rank_UltimaProposta = 1
group by 1
*/
