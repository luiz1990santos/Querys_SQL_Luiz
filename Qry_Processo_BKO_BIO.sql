DECLARE ANO STRING;
DECLARE MES STRING;
DECLARE DIA STRING; 

SET ANO = FORMAT_DATE('%Y', CURRENT_DATE());
SET MES = FORMAT_DATE('%m', CURRENT_DATE());
SET DIA = FORMAT_DATE('%d', CURRENT_DATE());

WITH BASE_BKO_BIO AS ( 
  SELECT 
    distinct
    LPAD(cpf, 11, '0') AS cpf_formatado,
    * 
  FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_4`
  WHERE Resultado = 'Aprovado'
  AND StatusConta != 'INACTIVE'
  AND Flag_Cadastro != 'CadastradoBKO'
  AND Flag_Cliente = 'EAI'
 )SELECT 
    CONCAT(
      SUBSTRING(BASE_BKO_BIO.cpf_formatado, 1, 3), '.', 
      SUBSTRING(BASE_BKO_BIO.cpf_formatado, 4, 3), '.', 
      SUBSTRING(BASE_BKO_BIO.cpf_formatado, 7, 3), '-', 
      SUBSTRING(BASE_BKO_BIO.cpf_formatado, 10, 2)
    ) AS CPF,
    'A' AS Biometria_Facial,
    CONCAT(DIA,'/',MES,'/',ANO,'00:00:02') AS Data_Hora_da_validacao
  FROM BASE_BKO_BIO

-- SELECT CURRENT_DATETIME() AS data_hora_atual;




/*

SELECT distinct
    * 
FROM `eai-datalake-data-sandbox.analytics_prevencao_fraude.Tb_BIO_Cadastrada_4`
WHERE Resultado = 'Aprovado'
AND StatusConta != 'INACTIVE'
AND Flag_Cadastro != 'CadastradoBKO'
AND Flag_Cliente = 'EAI'

*/
