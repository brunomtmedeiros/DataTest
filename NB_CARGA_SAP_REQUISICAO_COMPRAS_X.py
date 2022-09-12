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

def gera_tabela_EBAN():
  df = spark.sql("""
SELECT DISTINCT
       BEDAT,
       EBELN,
       EBELP,
       BADAT,
       BANFIN,
       BNFPO,
       BSART,
       MATNR,
       TXZ01,
       MENGE,
       MEINS,
       AFNAM,
       MATKL,
       FISTL,
       EKGRP,
       ERNAM,
       RLWRT,
       PEINH,
       LOEKZ,
       STATU,
       ESTKZ,
       FRGKZ,
       FRGST,
       ERDAT,
       BEDNR,
       BADAT,
       EKORG,
       BLCKD,
       BLCKT,
       FRGDT,
       BSMNG,
       BMEIN
       
FROM TRUSTED_SAP.EBAN
""")
  df.createOrReplaceTempView("EBAN")

# COMMAND ----------

def gera_tabela_EBNK():
  df = spark.sql("""
SELECT DISTINCT
       AUFNR,
       AUFNR,
       SAKTO,
       PRCTR,
       KOSTL
FROM TRUSTED_SAP.EBNK
""")
  df.createOrReplaceTempView("EBNK")

# COMMAND ----------

def gera_tabela_EKKO():
  df = spark.sql("""
SELECT DISTINCT
       BSART,
       ERNAM,
       RLWRT,
       LIFNR
FROM TRUSTED_SAP.EKKO
""")
  df.createOrReplaceTempView("EKKO")

# COMMAND ----------

def gera_tabela_LFA1():
  df = spark.sql("""
SELECT DISTINCT
       NAME1
FROM TRUSTED_SAP.LFA1
""")
  df.createOrReplaceTempView("LFA1")

# COMMAND ----------

# DBTITLE 1,Dataset de saída
def gera_tabela_saida():
  df = spark.sql(f"""
SELECT DISTINCT
       BEDAT,
       EBELN,
       EBELP,
       BADAT,
       BANFIN,
       BNFPO,
       BSART,
       MATNR,
       TXZ01,
       MENGE,
       MEINS,
       AFNAM,
       MATKL,
       FISTL,
       EKGRP,
       ERNAM,
       RLWRT,
       PEINH,
       LOEKZ,
       STATU,
       ESTKZ,
       FRGKZ,
       FRGST,
       ERDAT,
       BEDNR,
       BADAT,
       EKORG,
       BLCKD,
       BLCKT,
       FRGDT,
       BSMNG,
       SUM(MENGE - BSMNG) AS QUANTIDADE_PENDENTE
       BSART,
       ERNAM,
       RLWRT,
       BMEIN
       LIFNR
       NAME1 AS NOME_FORNECEDOR
       SUM(FRGDT - BEDAT) AS CALCULO
FROM EBAN AS EBA
INNER JOIN EBKN AS EBK
  ON EBA.MANDT = EBK.MANDT
INNER JOIN EKKO AS EKK
  ON EBA.MANDT = EKK.MANDT
INNER JOIN LFA1 AS LFA
  ON EBA.MANDT = LFA.MANDT
""")
  return df

# COMMAND ----------

gera_tabela_EBAN()
gera_tabela_EBKN()
gera_tabela_EKKO()
gera_tabela_LFA1()
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
