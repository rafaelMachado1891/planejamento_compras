from urllib.parse import quote_plus
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2 
from dotenv import load_dotenv
import os
import pyodbc
from datetime import datetime

load_dotenv()

DATA_BASE= os.getenv('DB')
USUARIO= os.getenv('USER')
PASSWORD= quote_plus(os.getenv('PASS'))
HOST_NAME= os.getenv('HOST')

DATA_BASE_URL = f"mssql+pyodbc://{USUARIO}:{PASSWORD}@{HOST_NAME}/{DATA_BASE}?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(DATA_BASE_URL)

query = """
WITH notas_fiscais AS (
	SELECT  
		Data_EM AS data_emissao,
		Numero AS numero,
		Codigo_OPF AS cfop,
		Descricao_OPF AS natureza,
		Codigo_Terceiro AS  codigo_terceiro,
		Razao AS cliente
	FROM NotaS1
	WHERE
		  Data_EM >= '01/01/2023' AND
		  estorno <> 1 AND
		  Codigo_OPF NOT IN ('1.101', '1.554','1.902','1.949','2.101','2.91','5.201',
			'6.201','5.556','2.915','3.101','6.911','6.949','5.949',
			'5.949','6.949','6.949','5.554','5.91','6.91','5.915','5.901',
			'5.901','6.901','5.916','6.916','6.949')
		),
-- 
produtos_notas AS (
SELECT
	Numero AS numero,
	Codigo AS codigo,
	Descricao AS descricao,
	Quantidade AS quantidade,
	Preco AS preco,
	P_Desconto AS desconto_percentual,
	VDesconto AS valor_desconto
FROM NotaS2
),
notas_agregadas AS (
	SELECT 
		a.data_emissao,
		a.cfop,
		a.natureza,
		a.numero,
		a.codigo_terceiro,
		b.codigo,
		b.descricao,
		b.quantidade,
		b.preco,
		b.desconto_percentual,
		ROUND((b.quantidade * b.preco) * (1-(b.desconto_percentual/100)),2) AS valor_total
	FROM notas_fiscais a
	JOIN produtos_notas b
	ON A.numero = b.numero
),
faturamento_ano_mes AS (
	SELECT 
		YEAR(data_emissao) AS ano,
		MONTH(data_emissao) AS mes,
		--numero,
		--codigo_terceiro,
		--cfop,
		--natureza,
		ROUND(SUM(valor_total),2) AS faturamento
	FROM notas_agregadas
	GROUP BY 
	YEAR(data_emissao),
	MONTH(data_emissao)
	--cfop,
	--natureza
	--numero,
	--codigo_terceiro
),
variacao AS (
	SELECT 
		ano,
		mes,
		faturamento,
		LAG(faturamento) OVER(ORDER BY mes) as fat_ano_anterior,
		round((faturamento - LAG(faturamento) OVER(ORDER BY mes)) / LAG(faturamento) OVER(ORDER BY mes),4) AS variacao
	FROM faturamento_ano_mes
)
select * from variacao

"""

with engine.connect() as connection:
    result = connection.execute(text(query))
    
    df = pd.DataFrame(result.fetchall(), columns=result.keys()) 


print(df)