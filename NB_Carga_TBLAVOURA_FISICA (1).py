# Databricks notebook source
# MAGIC %md
# MAGIC ### DOMÍNIO LAVOURA FÍSICA ###
# MAGIC Criação do dompinio de Lavoura Física (origem PIMS).
# MAGIC 
# MAGIC 
# MAGIC Data de criação: 29/04/2022 -- Responsável: Arthur Eyng
# MAGIC 
# MAGIC Revisado em: 09/05/2022 -- Responsável: Luan Borges da Fonseca

# COMMAND ----------

from pyspark.sql.functions import *
import os

# COMMAND ----------

# DBTITLE 1,Criaçao de variaveis
owner = 'DVRY_PRODUCAO'
table = 'LAVOURA_FISICA'
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

# DBTITLE 1, Criaçao de views tratadas
def gera_tabela_UPN3():
  df = spark.sql("""
SELECT DISTINCT
       CAST(ID_UPN3_FISICO AS BIGINT)        AS ID_DOMAIN,
       2000                                  AS ID_ORIGEM_DB,
       CD_UPN3_FISICO                        AS COD_LAVOURA,
       DE_UPN3_FISICO                        AS DESC_LAVOURA,
       CAST(QT_AREA_PROD AS numeric(18,2))   AS AREA_FISICA,
       TO_DATE(DT_INATIVACAO,'DD/MM/YYYY')   AS DATA_INATIVACAO,
       CAST(ID_UPNIVEL2 AS INT)              AS ID_SEDE
FROM trusted_tezkk0_pims2.Upn3_fisico""")
  df.createOrReplaceTempView("upn3_fisico")

# COMMAND ----------

def gera_tabela_saida_pims():
  df = spark.sql(f"""
  SELECT DISTINCT
       u.ID_DOMAIN,
       u.ID_ORIGEM_DB,
       CONCAT(b.ID_DOMAIN, u.ID_DOMAIN)            AS ID_LAV_FISICA,
       b.DESC_BANCO_DADOS                          AS ORIGEM_DB,
       u.COD_LAVOURA,
       u.DESC_LAVOURA,
       u.AREA_FISICA,
       u.DATA_INATIVACAO,
       CONCAT(b.ID_DOMAIN,u.ID_SEDE)               AS ID_SEDE
FROM upn3_fisico AS u
INNER JOIN DVRY_CADASTROS.tb_banco_dados AS b
  ON u.ID_ORIGEM_DB = b.ID_DOMAIN""")
  return df 


# COMMAND ----------

def gera_tabela_ga_saf_divi3():
  df = spark.sql("""
SELECT DISTINCT 
       CAST(ID_DIVI3 AS BIGINT)            AS ID_DIVI3,
       1000                                AS ID_ORIGEM_DB,
       COD_DIVI3,
       DSC_DIVI3,
       CAST(DV3_AREA_LIQ AS DECIMAL(18,6)) AS DV3_AREA_LIQ,
       CAST(COD_EMPR AS INT)               AS COD_EMPR
FROM trusted_gatec_saf_legado.ga_saf_divi3
WHERE LEFT(COD_DIVI1,1) = 'P'""")
  
  df.createOrReplaceTempView("ga_saf_divi3")

# COMMAND ----------

def gera_tabela_saida_gatec():
  df = spark.sql(f"""
  SELECT DISTINCT
       d.ID_DIVI3                                  AS ID_DOMAIN,
       d.ID_ORIGEM_DB,
       CONCAT(b.ID_DOMAIN, d.ID_DIVI3)             AS ID_LAV_FISICA,
       b.DESC_BANCO_DADOS                          AS ORIGEM_DB,
       d.COD_DIVI3                                 AS COD_LAVOURA ,
       d.DSC_DIVI3                                 AS DESC_LAVOURA,
       d.DV3_AREA_LIQ                              AS AREA_FISICA,
       CAST(NULL AS TIMESTAMP)                     AS DATA_INATIVACAO,
       CONCAT(b.ID_DOMAIN,d.COD_EMPR)              AS ID_SEDE
FROM ga_saf_divi3 AS d
INNER JOIN DVRY_CADASTROS.tb_banco_dados AS b
  ON d.ID_ORIGEM_DB = b.ID_DOMAIN""")
  return df 


# COMMAND ----------

gera_tabela_ga_saf_divi3()
df_gatec = gera_tabela_saida_gatec()

# COMMAND ----------

display(df_gatec)

# COMMAND ----------

df_gatec.createOrReplaceTempView("df_gatec")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM df_gatec
# MAGIC WHERE ID_SEDE = 100035

# COMMAND ----------

# DBTITLE 1,Orquestraçao de carga
# geraçao de tabelas tratadas
#pims
gera_tabela_UPN3()

#gatec
gera_tabela_ga_saf_divi3()

# geraçao de tabela de saida
df_pims = gera_tabela_saida_pims()

# se ja esta gravado dados consolidados do legado, tras estes dados prontos, senao, grava
#if not os.path.isdir('/dbfs/mnt/dvryzone-slc/GATEC_SAF_LEGADO/'+table):
  df_gatec = gera_tabela_saida_gatec()
  df_gatec.write.format('parquet').mode('overwrite').save('/mnt/dvryzone-slc/GATEC_SAF_LEGADO/'+table)
  
df_gatec = spark.read.parquet('/mnt/dvryzone-slc/GATEC_SAF_LEGADO/'+table)

# consolida os dataframes
df = df_pims.union(df_gatec).dropDuplicates()

#Grava saida de dataframes
#status = False
#x = 0
#while status == False and x <10:
#  status = insere_dados_tabela_populada_dvry(df, owner, table, 'overwrite', 'parquet', pthDestino)
#  x=x+1
#  print(status)

