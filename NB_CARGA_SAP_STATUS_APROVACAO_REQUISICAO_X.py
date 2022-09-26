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
       MAX(UDATE) AS DATA_APROVACAO,
       OBJECTID,
       MANDANT,
       OBJECTCLAS,
       TCODE,
       CHANGENR
FROM TRUSTED_SAP.CDHDR
GROUP BY
       UDATE,
       OBJECTID,
       MANDANT,
       OBJECTCLAS,
       TCODE,
       CHANGENR
  """)
  df.createOrReplaceTempView("CDHDR")

# COMMAND ----------

def gera_tabela_CDPOS():
  df = spark.sql("""
SELECT DISTINCT
       CHANGENR,
       FNAME,
       MANDANT,
       OBJECTID,
       OBJECTCLAS,
       VALUE_NEW
FROM TRUSTED_SAP.CDPOS AS CDP
  """)
  df.createOrReplaceTempView("CDPOS")

# COMMAND ----------

# DBTITLE 1,Dataset de saída
def gera_tabela_saida():
  df = spark.sql("""
SELECT DISTINCT
       EBA.BANFN                                   AS REQUISICAO,
       EBA.BNFPO                                   AS ITEM_REQUISICAO,
       EBA.FRGGR                                   AS GRUPO_LIBERACAO,
       EBA.FRGST                                   AS ESTRATEGIA_LIBERACAO,
       EBA.FRGKZ                                   AS COD_LIBERACAO,
       TO_DATE(CDH.DATA_APROVACAO,'yyyyMMdd')      AS DATA_APROVACAO
FROM EBAN AS EBA
INNER JOIN CDHDR AS CDH
  ON CDH.OBJECTID = EBA.BANFN
INNER JOIN CDPOS AS CDP
  ON CDP.MANDANT = CDH.MANDANT
    AND CDH.CHANGENR = CDP.CHANGENR
    AND CDP.OBJECTCLAS = CDH.OBJECTCLAS
    AND CDP.OBJECTID = CDH.OBJECTID
WHERE EBA.FRGKZ != 'X'
  AND CDP.FNAME = 'FRGKZ'
  AND CDP.VALUE_NEW != 'X'
  AND CDH.TCODE = 'ME54N'
""")
  return df

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT * 
# MAGIC FROM TRUSTED_SAP.CDHDR AS CDH
# MAGIC INNER JOIN TRUSTED_SAP.CDPOS AS CDP
# MAGIC   ON CDH.CHANGENR = CDP.CHANGENR
# MAGIC WHERE CDH.TCODE = 'ME54N'
# MAGIC   AND CDP.VALUE_NEW = 'X'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC        EB.BANFN,
# MAGIC        EB.BNFPO,
# MAGIC        EB.FRGGR,
# MAGIC        EB.FRGST,
# MAGIC        EB.FRGKZ,
# MAGIC        TO_DATE(CH.UDATE,'yyyyMMdd') AS UDATE
# MAGIC FROM TRUSTED_SAP.EBAN AS EB
# MAGIC INNER JOIN TRUSTED_SAP.CDHDR AS CH
# MAGIC   ON CAST(CH.OBJECTID AS BIGINT) = CAST(EB.BANFN AS BIGINT)
# MAGIC     AND OBJECTCLAS = 'BANF'
# MAGIC INNER JOIN trusted_sap.CDPOS AS CD
# MAGIC   ON CD.MANDANT = CH.MANDANT
# MAGIC     AND CD.OBJECTCLAS = 'BANF'
# MAGIC     AND CAST(CD.OBJECTID AS BIGINT) = CAST(CH.OBJECTID AS BIGINT)
# MAGIC WHERE 0=0
# MAGIC   AND EB.FRGKZ != 'X'
# MAGIC   AND CD.FNAME = 'FRGKZ'
# MAGIC   AND CD.VALUE_NEW = 'X'
# MAGIC   AND CH.TCODE = 'ME54N'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC        EBA.BANFN                                AS REQUISICAO,
# MAGIC        EBA.BNFPO                                AS ITEM_REQUISICAO,
# MAGIC        EBA.FRGGR                                AS GRUPO_LIBERACAO,
# MAGIC        EBA.FRGST                                AS ESTRATEGIA_LIBERACAO,
# MAGIC        EBA.FRGKZ                                AS COD_LIBERACAO,
# MAGIC        CDH.UDATE                                AS DATA_APROVACAO
# MAGIC FROM TRUSTED_SAP.EBAN AS EBA
# MAGIC INNER JOIN TRUSTED_SAP.CDHDR AS CDH
# MAGIC   ON CDH.OBJECTID = EBA.BANFN
# MAGIC INNER JOIN TRUSTED_SAP.CDPOS AS CDP
# MAGIC   ON CDP.MANDANT = CDH.MANDANT
# MAGIC     AND CDH.CHANGENR = CDP.CHANGENR
# MAGIC     AND CDP.OBJECTCLAS = CDH.OBJECTCLAS
# MAGIC     AND CDP.OBJECTID = CDH.OBJECTID
# MAGIC     
# MAGIC WHERE EBA.FRGKZ != 'X'
# MAGIC   AND CDP.FNAME = 'FRGKZ'
# MAGIC   AND CDP.VALUE_NEW != 'X'
# MAGIC   AND CDH.TCODE = 'ME54N'

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
# MAGIC select * from trusted_sap.CDHDR AS CDH
# MAGIC   INNER JOIN TRUSTED_SAP.CDPOS AS CDP
# MAGIC     ON CDH.CHANGENR = CDP.CHANGENR
# MAGIC WHERE CDH.TCODE = 'ME54N'
# MAGIC AND CDP.VALUE_NEW != 'X'
