# Databricks notebook source
# MAGIC %md
# MAGIC ### DOMÍNIO VARIEDADE ###
# MAGIC Criação do domínio de Variedades (origem PIMS).
# MAGIC 
# MAGIC 
# MAGIC Data de criação: 02/05/2022 -- Responsável: Arthur Eyng
# MAGIC 
# MAGIC Revisado em: 10/05/2022 -- Responsável: Luan Borges da Fonseca

# COMMAND ----------

from pyspark.sql.functions import *
import os

# COMMAND ----------

# DBTITLE 1,Criaçao de variaveis
owner = 'DVRY_PRODUCAO'
table = 'VARIEDADE'
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
def gera_tabela_variedade():
  df = spark.sql("""
  SELECT DISTINCT
       CAST(ID_VARIEDADE AS BIGINT)    AS ID_VARIEDADE,
       2000                            AS ID_ORIGEM_DB,
       CD_VARIEDADE                    AS COD_VARIEDADE,
       DA_VARIEDADE                    AS TIPO_VARIEDADE,
       INITCAP(DE_VARIEDADE)           AS DESC_COMPLETA_VARIEDADE,
       ID_OCUPACAO                     AS ID_CULTURA,
       CASE 
          WHEN FG_TRANSGENIA = 'S' 
          THEN 'Sim'
          ELSE 'Não'                  
       END                             AS TRANSGENIA,
       CAST(ID_GRPMATURACAO AS BIGINT) AS ID_GRPMATURACAO,
       CAST(ID_GRPVARIEDADE AS BIGINT) AS ID_GRPVARIEDADE
FROM trusted_tezkk0_pims2.variedade""")
  df.createOrReplaceTempView("variedade")

# COMMAND ----------

def gera_tabela_variedade_ep():
  df = spark.sql("""
  SELECT DISTINCT 
       CAST(ID_VARIEDADE AS BIGINT)                AS ID_VARIEDADE,
       CAST(CASE 
               WHEN QT_DIAS_MATUR > 0
               THEN QT_DIAS_MATUR
               ELSE NULL
            END AS INT)                            AS QT_DIAS_MATUR,
       COALESCE(CAST(NO_DIA_PLAN_EMER AS INT),0)   AS DIAS_PARA_EMERGERGENCIA,
       CAST (QT_PROD_ESTIM AS DECIMAL(10,2))       AS PRODUTIVIDADE_PADRAO
FROM trusted_tezkk0_pims2.Variedade_ep
  """)
  df.createOrReplaceTempView("variedade_ep")

# COMMAND ----------

def gera_tabela_grpmaturacao():
  df = spark.sql("""
  SELECT DISTINCT
       CAST(ID_GRPMATURACAO AS BIGINT)        AS ID_GRPMATURACAO,
       COALESCE(CAST(QT_DIAS_MATUR AS INT),0)   AS QT_DIAS_MATUR
FROM trusted_tezkk0_pims2.Grpmaturacao
  """)
  df.createOrReplaceTempView("grpmaturacao")

# COMMAND ----------

def gera_tabela_grpvariedade():
  df = spark.sql("""
  SELECT DISTINCT
       CAST(ID_GRPVARIEDADE AS BIGINT) AS ID_GRPVARIEDADE,
       DA_GRP_VARIEDADE                AS DESC_GRUPO_VARIEDADE,
       INITCAP(DE_GRP_VARIEDADE)       AS DESC_COMPLETA_GRUPO_VARIEDADE
  FROM trusted_tezkk0_pims2.Grpvariedade
  """)
  df.createOrReplaceTempView("grpvariedade")

# COMMAND ----------

def gera_tabela_saida_pims():
  df = spark.sql("""
  SELECT DISTINCT
       v.ID_VARIEDADE                                 AS ID_DOMAIN,
       v.ID_ORIGEM_DB,
       CONCAT(d.ID_DOMAIN, v.ID_VARIEDADE)            AS ID_VARIEDADE,
       d.DESC_BANCO_DADOS                             AS ORIGEM_DB,
       v.COD_VARIEDADE,
       v.TIPO_VARIEDADE,
       v.DESC_COMPLETA_VARIEDADE,
       CONCAT(d.ID_DOMAIN,v.ID_CULTURA)               AS ID_CULTURA,
       v.TRANSGENIA,
       COALESCE(e.QT_DIAS_MATUR,m.QT_DIAS_MATUR)      AS CICLO_VARIEDADE,
       e.DIAS_PARA_EMERGERGENCIA,
       g.DESC_GRUPO_VARIEDADE,
       g.DESC_COMPLETA_GRUPO_VARIEDADE,
       e.PRODUTIVIDADE_PADRAO,
       'TBD'                                          AS FORNECEDOR,
       0                                              AS PRODUTIVIDADE_HISTORICA_MEDIA,
       0                                              AS ESPACAMENTO_SEMENTES 
FROM variedade AS v
INNER JOIN variedade_ep AS e
  ON e.ID_VARIEDADE = v.ID_VARIEDADE
INNER JOIN grpmaturacao AS m
  ON m.ID_GRPMATURACAO = v.ID_GRPMATURACAO
INNER JOIN grpvariedade AS g
  ON g.ID_GRPVARIEDADE = v.ID_GRPVARIEDADE
INNER JOIN DVRY_CADASTROS.tb_banco_dados AS d
  ON v.ID_ORIGEM_DB = d.ID_DOMAIN""")
  return df 


# COMMAND ----------

gera_tabela_variedade()
gera_tabela_variedade_ep()
gera_tabela_grpmaturacao()
gera_tabela_grpvariedade()
df_pims = gera_tabela_saida_pims()

# COMMAND ----------

df_pims.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM df
# MAGIC 
# MAGIC WHERE DESC_COMPLETA_VARIEDADE LIKE '%83HO113%'

# COMMAND ----------

def gera_tabela_ga_saf_variedade():
  df = spark.sql("""
  SELECT DISTINCT
       CAST(COD_VARIE AS INT)                   AS COD_VARIE,
       1000                                     AS ID_ORIGEM_DB,
       DSC_VARIE,
       COALESCE(CAST(VAR_TRANSGENICO AS INT),0) AS VAR_TRANSGENICO,
       COD_CULTURA,
       CAST(COD_GRUPO_VARIE AS INT)             AS COD_GRUPO_VARIE,
       COD_FABRICANTE
FROM trusted_gatec_saf_legado.GA_SAF_VARIEDADE
  """)
  df.createOrReplaceTempView("ga_saf_variedade")

# COMMAND ----------

def gera_tabela_ga_saf_grupo_varie():
  df = spark.sql("""
  SELECT DISTINCT
       CAST(COD_GRUPO_VARIE AS INT) AS COD_GRUPO_VARIE,
       INITCAP(DSC_GRUPO_VARIE)     AS DSC_GRUPO_VARIE,
       ABV_GRUPO_VARIE
FROM trusted_gatec_saf_legado.GA_SAF_GRUPO_VARIE
  """)
  df.createOrReplaceTempView("ga_saf_grupo_varie")

# COMMAND ----------

def gera_tabela_saida_gatec():
  df = spark.sql("""
  SELECT DISTINCT
       v.COD_VARIE                                 AS ID_DOMAIN,
       v.ID_ORIGEM_DB,
       CONCAT(d.ID_DOMAIN, v.COD_VARIE)            AS ID_VARIEDADE,
       d.DESC_BANCO_DADOS                          AS ORIGEM_DB,
       v.COD_VARIE,
       'TBD'                                       AS TIPO_VARIEDADE,
       v.DSC_VARIE,
       CONCAT(d.ID_DOMAIN,v.COD_CULTURA)           AS ID_CULTURA,
       CASE
           WHEN VAR_TRANSGENICO = 1
           THEN 'SIM'
           ELSE 'NAO'
       END                                         AS TRANSGENIA,
       0                                           AS CICLO_VARIEDADE,
       0                                           AS DIAS_PARA_EMERGERGENCIA,
       g.ABV_GRUPO_VARIE,
       g.DSC_GRUPO_VARIE,
       0                                           AS PRODUTIVIDADE_PADRAO,
       COD_FABRICANTE                              AS FORNECEDOR,
       0                                           AS PRODUTIVIDADE_HISTORICA_MEDIA,
       0                                           AS ESPACAMENTO_SEMENTES 
FROM ga_saf_variedade AS v
INNER JOIN ga_saf_grupo_varie AS g
  ON g.COD_GRUPO_VARIE = v.COD_GRUPO_VARIE
INNER JOIN DVRY_CADASTROS.tb_banco_dados AS d
  ON v.ID_ORIGEM_DB = d.ID_DOMAIN""")
  return df 


# COMMAND ----------

# DBTITLE 1,Orquestraçao de carga
# geraçao de tabelas tratadas
#pims
gera_tabela_variedade()
gera_tabela_variedade_ep()
gera_tabela_grpmaturacao()
gera_tabela_grpvariedade()

#gatec
gera_tabela_ga_saf_grupo_varie()
gera_tabela_ga_saf_variedade()

# geraçao de tabela de saida
df_pims = gera_tabela_saida_pims()

# se ja esta gravado dados consolidados do legado, tras estes dados prontos, senao, grava
# if not os.path.isdir('/dbfs/mnt/dvryzone-slc/GATEC_SAF_LEGADO/'+table):
df_gatec = gera_tabela_saida_gatec()
#df_gatec.write.format('parquet').mode('overwrite').save('/mnt/dvryzone-slc/GATEC_SAF_LEGADO/'+table)
#df_gatec = spark.read.parquet('/mnt/dvryzone-slc/GATEC_SAF_LEGADO/'+table)


# consolida os dataframes
df = df_pims.union(df_gatec).dropDuplicates()

#Grava saida de dataframes
status = False
x = 0
#while status == False and x <10:
#  status = insere_dados_tabela_populada_dvry(df, owner, table, 'overwrite', 'parquet', pthDestino)
#  x=x+1
#  print(status)


# COMMAND ----------

df.createOrReplaceTempView('df')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM trusted_tezkk0_pims2.VARIEDADE
# MAGIC 
# MAGIC WHERE  CD_VARIEDADE = 1000479
