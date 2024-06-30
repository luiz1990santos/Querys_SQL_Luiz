

---------------------------------------------------
-- Processo de promoção Trusted                   |
--------------------------------------------------- 

with trusted as (
    select 
        distinct 
        cus.uuid as CustomerID, 
        cus.document as CPF_BKO, 
        zaig.* 
    from `eai-datalake-data-sandbox.analytics_prevencao_fraude.Dw_Zaig` as zaig
    left join `eai-datalake-data-sandbox.core.customers` as cus
    on cus.document = zaig.Cpf_Cliente
    where zaig.tree_score <= 2 
    and date(zaig.data_cadastro) >= '2024-02-23'
    and zaig.decisao = 'automatically_approved'   
    and zaig.esteira =  'Abastece Aí'
    and cus.trusted = 0
    and cus.status = 'ACTIVE'
 

)
    select 
        distinct 
        CustomerID,  
        'True' as Trusted

    from trusted
;
