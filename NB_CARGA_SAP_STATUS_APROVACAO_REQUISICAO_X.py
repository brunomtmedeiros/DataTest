# Databricks notebook source
# MAGIC %md
# MAGIC ### NB_CARGA_SAP_STATUS_APROVACAO_REQUISICAO ###
# MAGIC Tabela com status de aprovação de requisição do SAP com a visao de suprimentos.
# MAGIC 
# MAGIC 
# MAGIC Data de criação: 14/09/2022 -- Responsavel: Bruno Martins Medeiros

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Criaçao de variaveis
owner = 'DVRY_SUPRIMENTOS'
table = 'STATUS_APROVACAO_REQUISICAO'
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

def gera_tabela_EBAN():
  df = spark.sql("""
SELECT DISTINCT
       BANFN,
       BNFPO,
       FRGGR,
       FRGST,
       FRGKZ
FROM TRUSTED_SAP.EBAN AS EBA
  """)
  df.createOrReplaceTempView("EBAN")

# COMMAND ----------

def gera_tabela_CDHDR():
  df = spark.sql("""
SELECT DISTINCT
       *
FROM TRUSTED_SAP.CDHDR
  """)
  df.createOrReplaceTempView("CDHDR")

# COMMAND ----------

gera_tabela_CDHDR()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TRUSTED_SAP.CDPOS AS CDPOS
# MAGIC LEFT JOIN TRUSTED_SAP.CDHDR AS CDH
# MAGIC   ON CDPOS.MANDANT = CDH.MANDANT
# MAGIC WHERE CDH.TCODE = 'ME54N'

# COMMAND ----------

# MAGIC %sql
# MAGIC select TCODE FROM trusted_sap.CDHDR AS CDH
# MAGIC WHERE CDH.TCODE = 'ME54N'

# COMMAND ----------

def gera_tabela_CDPOS():
  df = spark.sql("""
SELECT DISTINCT
       CHANGENR,
       FNAME
FROM TRUSTED_SAP.CDPOS AS CDP
  """)
  df.createOrReplaceTempView("CDPOS")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from CDHDR

# COMMAND ----------

# DBTITLE 1,Dataset de saída
def gera_tabela_saida():
  df = spark.sql("""
SELECT DISTINCT
       EBA.BANFN AS REQUISICAO,
       EBA.BNFPO AS ITEM_REQUISICAO,
       EBA.FRGGR AS GRUPO_LIBERACAO,
       EBA.FRGST AS ESTRATEGIA_LIBERACAO,
       EBA.FRGKZ AS COD_LIBERACAO,
       CASE EBA.FRGKZ
         WHEN 'X' THEN CDH.OBJECTID = 'ME54N'
FROM EBAN AS EBA
""")
  return df

# COMMAND ----------

gera_tabela_EBAN()
gera_tabela_CDHDR()
gera_tabela_CDPOS()
df = gera_tabela_saida()
display(df)

# COMMAND ----------

# DBTITLE 1,Orquestraçao de carga
# geraçao de tabela de saida
df = gera_tabela_saida()

#Grava saida de dataframes
status = False
x = 0
while status == False and x <10:
  status = insere_dados_tabela_populada_dvry(df, owner, table, 'overwrite', 'parquet', pthDestino)
  x=x+1
  print(status)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trusted_sap.CDHDR
