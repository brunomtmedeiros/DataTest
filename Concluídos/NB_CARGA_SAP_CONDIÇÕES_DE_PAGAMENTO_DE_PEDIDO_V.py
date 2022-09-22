# Databricks notebook source
# MAGIC %md
# MAGIC ### NB_CARGA_SAP_CONDIÇÕES_DE_PAGAMENTO_DE_PEDIDO ###
# MAGIC Tabela com condições de pagamentos de pedido do SAP com a visao de suprimentos.
# MAGIC 
# MAGIC 
# MAGIC Data de criação: 15/09/2022 -- Responsavel: Bruno Martins Medeiros

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Criaçao de variaveis
owner = 'DVRY_SUPRIMENTOS'
table = 'CONDIÇÕES_DE_PAGAMENTO_DE_PEDIDO'
pthDestino = pthDestino = '/mnt/dvryzone-slc/' + owner + '/' + table

print('owner ----> '+owner)
print('table ----> '+table)
print('pthDestino ----> '+pthDestino)

# COMMAND ----------

# DBTITLE 1,Criaçao de schema
spark.sql("CREATE DATABASE IF NOT EXISTS " + owner)

# COMMAND ----------

# DBTITLE 1,Importa funções genéricas
# MAGIC %run "/Engenharia/Functions/Functions_global"

# COMMAND ----------

def gera_tabela_T052():
  df = spark.sql("""
SELECT DISTINCT
       ZTERM,
       ZTAGG
FROM TRUSTED_SAP.T052
""")
  df.createOrReplaceTempView("T052")

# COMMAND ----------

def gera_tabela_T052U():
  df = spark.sql("""
SELECT DISTINCT
       TEXT1,
       ZTAGG
FROM TRUSTED_SAP.T052U
""")
  df.createOrReplaceTempView("T052U")

# COMMAND ----------

# DBTITLE 1,Dataset de saída
def gera_tabela_saida():
  df = spark.sql(f"""
SELECT DISTINCT
       T.ZTERM  AS CPGT,
       TU.TEXT1 AS EXPLICACAO_PROPRIA
FROM T052 AS T
INNER JOIN T052U AS TU
  ON T.ZTAGG = TU.ZTAGG
""")
  return df

# COMMAND ----------

gera_tabela_T052()
gera_tabela_T052U()
df = gera_tabela_saida()
display(df)

# COMMAND ----------

# DBTITLE 1,Orquestraçao de carga
# geraçao de tabelas tratadas
gera_tabela_MARA()
gera_tabela_MAKT()
gera_tabela_T024()

# geraçao de tabela de saida
df = gera_tabela_saida()

#Grava saida de dataframes
status = False
x = 0
while status == False and x <10:
  status = insere_dados_tabela_populada_dvry(df, owner, table, 'overwrite', 'parquet', pthDestino)
  x=x+1
  print(status)
