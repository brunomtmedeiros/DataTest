# Databricks notebook source
# MAGIC %md
# MAGIC ### NB_CARGA_SAP_ESTOQUE_PRODUTO ###
# MAGIC Tabela com informações de estoque de produtos do SAP com a visao de suprimentos.
# MAGIC 
# MAGIC 
# MAGIC Data de criação: 12/09/2022 -- Responsavel: Bruno Martins Medeiros

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Criaçao de variaveis
owner = 'DVRY_SUPRIMENTOS'
table = 'REQUISICAO_COMPRAS'
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

def gera_tabela_MARC():
  df = spark.sql("""
SELECT DISTINCT
       WERKS,
       MATNR
FROM TRUSTED_SAP.MARC
""")
  df.createOrReplaceTempView("MARC")

# COMMAND ----------

# DBTITLE 1,Dataset de saída
def gera_tabela_saida():
  df = spark.sql(f"""
SELECT DISTINCT
       MA.MATNR                          AS COD_MATERIAL,
       MT.MAKTX                          AS DESCRICAO,
       MC.WERKS                          AS CENTRO,
       MA.MEINS                          AS UNIDADE_MEDIDA
FROM MARA AS MA
INNER JOIN MAKT AS MT
  ON MA.MATNR = MT.MATNR
INNER JOIN MARC AS MC
  ON MA.MATNR = MC.MATNR
""")
  return df

# COMMAND ----------

gera_tabela_MARA()
gera_tabela_MAKT()
gera_tabela_MARC()
df = gera_tabela_saida()
display(df)

# COMMAND ----------

# DBTITLE 1,Orquestraçao de carga
# geraçao de tabelas tratadas
gera_tabela_MARA()
gera_tabela_MAKT()
gera_tabela_MARC()

# geraçao de tabela de saida
df = gera_tabela_saida()

#Grava saida de dataframes
status = False
x = 0
while status == False and x <10:
  status = insere_dados_tabela_populada_dvry(df, owner, table, 'overwrite', 'parquet', pthDestino)
  x=x+1
  print(status)
