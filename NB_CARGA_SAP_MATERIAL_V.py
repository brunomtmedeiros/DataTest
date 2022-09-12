# Databricks notebook source
# MAGIC %md
# MAGIC ### NB_CARGA_SAP_MATERIAL ###
# MAGIC Tabela com informações de materiais do SAP com a visao de suprimentos.
# MAGIC 
# MAGIC 
# MAGIC Data de criação: 12/09/2022 -- Responsavel: Bruno Martins Medeiros

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Criaçao de variaveis
owner = 'DVRY_SUPRIMENTOS'
table = 'MATERIAL'
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

def gera_tabela_MARA():
  df = spark.sql("""
SELECT DISTINCT
       MATNR,
       MATKL,
       MTART,
       MANDT
FROM TRUSTED_SAP.MARA
""")
  df.createOrReplaceTempView("MARA")

# COMMAND ----------

def gera_tabela_MAKT():
  df = spark.sql("""
SELECT DISTINCT
       MAKTX,
       MATNR
FROM TRUSTED_SAP.MAKT
""")
  df.createOrReplaceTempView("MAKT")

# COMMAND ----------

def gera_tabela_T024():
  df = spark.sql("""
SELECT DISTINCT
       EKGRP,
       MANDT
FROM TRUSTED_SAP.T024
""")
  df.createOrReplaceTempView("T024")

# COMMAND ----------

# DBTITLE 1,Dataset de saída
def gera_tabela_saida():
  df = spark.sql(f"""
SELECT DISTINCT
       MA.MATNR                          AS COD_MATERIAL,
       MT.MAKTX                          AS DESCRICAO,
       CONCAT(MA.MATNR, " - ", MT.MAKTX) AS CODIGO_DESCRICAO,
       T.EKGRP                           AS GRUPO_COMPRAS,
       MA.MATKL                          AS GRUPO_MERCADORIA,
       MA.MTART                          AS TIPO
FROM MARA AS MA
INNER JOIN MAKT AS MT
  ON MA.MATNR = MT.MATNR
INNER JOIN T024 AS T
  ON MA.MANDT = T.MANDT
""")
  return df

# COMMAND ----------

gera_tabela_MARA()
gera_tabela_MAKT()
gera_tabela_T024()
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
