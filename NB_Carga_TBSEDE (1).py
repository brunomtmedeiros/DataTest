# Databricks notebook source
# MAGIC %md
# MAGIC ### DOMÍNIO SEDE ###
# MAGIC Criação do domínio de Sedes (origem PIMS).
# MAGIC 
# MAGIC 
# MAGIC Data de criação: 29/04/2022 -- Responsável: Arthur Eyng
# MAGIC 
# MAGIC Revisado em: 11/05/2022 -- Responsável: Luan Borges da Fonseca

# COMMAND ----------

from pyspark.sql.functions import *
import os

# COMMAND ----------

# DBTITLE 1,Criaçao de variaveis
owner = 'DVRY_PRODUCAO'
table = 'SEDE'
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
def gera_tabela_upnivel2():
  df = spark.sql("""
SELECT DISTINCT
       CAST(ID_UPNIVEL2 AS BIGINT) AS ID_UPNIVEL2,
       2000                        AS ID_ORIGEM_DB,
       DA_UPNIVEL2                 AS COD_SEDE,
       DE_UPNIVEL2                 AS DESC_SEDE,
       CAST(ID_UPNIVEL1 AS BIGINT) AS ID_UPNIVEL1
FROM trusted_tezkk0_pims2.Upnivel2
""")
  df.createOrReplaceTempView("upnivel2")

# COMMAND ----------

def gera_tabela_upnivel1():
  df = spark.sql("""
SELECT DISTINCT
       CAST(ID_UPNIVEL1 AS BIGINT) AS ID_UPNIVEL1,
       CD_INT_ERP                  AS COD_FAZENDA,
       CAST(ID_FILIAL AS BIGINT)   AS ID_FILIAL
FROM trusted_tezkk0_pims2.Upnivel1
""")
  df.createOrReplaceTempView("upnivel1")

# COMMAND ----------

def gera_tabela_unifederacao():
  df = spark.sql("""
SELECT DISTINCT
       CAST(ID_UNIFEDERACAO AS BIGINT) AS ID_UNIFEDERACAO,
       CD_UF                           AS UF
FROM trusted_tezkk0_pims2.unifederacao
""")
  df.createOrReplaceTempView("unifederacao")

# COMMAND ----------

def gera_tabela_municipio():
  df = spark.sql("""
SELECT DISTINCT
       CAST(ID_MUNICIPIO AS BIGINT)    AS ID_MUNICIPIO,
       CAST(ID_UNIFEDERACAO AS BIGINT) AS ID_UNIFEDERACAO,
       INITCAP(DE_MUNICIPIO)           AS CIDADE
FROM trusted_tezkk0_pims2.municipio
""")
  df.createOrReplaceTempView("municipio")

# COMMAND ----------

def gera_tabela_filial():
  df = spark.sql("""
SELECT DISTINCT
       CAST(ID_FILIAL AS BIGINT)    AS ID_FILIAL,
       CAST(ID_MUNICIPIO AS BIGINT) AS ID_MUNICIPIO,
       CNPJ_Masked(NO_CNPJ)         AS CNPJ,
       CONCAT('2000',ID_EMPRESA)   AS ID_FAZENDA
FROM trusted_tezkk0_pims2.filial
""")
  df.createOrReplaceTempView("filial")

# COMMAND ----------

def gera_tabela_regiao_fazenda():
  df = spark.sql("""
SELECT DISTINCT 
       TITLE   AS COD_FAZENDA,
       REGIAO1 AS REGIAO,
       REGIAO2 AS REGIONAL
FROM trusted_sharepointcia.dim_regiaofazendas
""")
  df.createOrReplaceTempView("regiao_fazenda")

# COMMAND ----------

def gera_tabela_saida_pims():
  df = spark.sql("""
  SELECT DISTINCT
       u2.ID_UPNIVEL2                                 AS ID_DOMAIN,
       u2.ID_ORIGEM_DB,
       CONCAT(b.ID_DOMAIN, u2.ID_UPNIVEL2)            AS ID_SEDE,
       b.DESC_BANCO_DADOS                             AS ORIGEM_DB, 
       u2.COD_SEDE,
       u2.DESC_SEDE,
       u.UF,
       r.REGIAO,
       r.REGIONAL,
        m.CIDADE,
        f.CNPJ,
        f.ID_FAZENDA
FROM upnivel2 AS u2
INNER JOIN  upnivel1 AS u1
  ON u2.ID_UPNIVEL1 = u1.ID_UPNIVEL1
INNER JOIN filial AS f
  ON f.ID_FILIAL = u1.ID_FILIAL
INNER JOIN municipio AS m
  ON m.ID_MUNICIPIO = f.ID_MUNICIPIO
INNER JOIN unifederacao as u
  ON u.ID_UNIFEDERACAO = m.ID_UNIFEDERACAO
INNER JOIN DVRY_CADASTROS.tb_banco_dados AS b
  ON u2.ID_ORIGEM_DB = b.ID_DOMAIN
LEFT JOIN regiao_fazenda AS r
  ON r.COD_FAZENDA = u1.COD_FAZENDA""")
  return df

# COMMAND ----------

def gera_tabela_ga_empr():
  df = spark.sql("""
SELECT DISTINCT 
       CAST(COD_EMPR AS BIGINT)   AS COD_EMPR,
       1000 AS ID_ORIGEM_DB,
       SUBSTR(ABV_EMPR,3,2)       AS COD_FAZENDA,
       CONCAT(SUBSTR(ABV_EMPR,3,2),
       0, 
       CASE
         WHEN ABV_EMPR LIKE('%III%')
         THEN 3
         WHEN ABV_EMPR LIKE('%II%')
         THEN 2
         ELSE 1
       END)                       AS ABV_EMPR,
       DSC_EMPR,
       EMP_UF,
       INITCAP(EMP_CIDAD)         AS EMP_CIDAD,
       CNPJ_Masked(TRIM(EMP_CGC)) AS EMP_CGC
FROM trusted_gatec_saf_legado.ga_empr
WHERE LEFT(ABV_EMPR,2) = 'FZ'
""")
  df.createOrReplaceTempView("ga_empr")

# COMMAND ----------

gera_tabela_ga_empr()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ga_empr

# COMMAND ----------

def gera_tabela_saida_gatec():
  df = spark.sql("""
  SELECT DISTINCT
       e.COD_EMPR                                     AS ID_DOMAIN,
       e.ID_ORIGEM_DB,
       CONCAT(b.ID_DOMAIN, e.COD_EMPR)                AS ID_SEDE,
       b.DESC_BANCO_DADOS                             AS ORIGEM_DB, 
       e.ABV_EMPR                                     AS COD_SEDE,
       e.DSC_EMPR                                     AS DESC_SEDE,
       e.EMP_UF                                       AS UF,
       r.REGIAO,
       r.REGIONAL,
       e.EMP_CIDAD                                    AS CIDADE,
       e.EMP_CGC                                      AS CNPJ,
       CONCAT(b.ID_DOMAIN, e.COD_EMPR)                AS ID_FAZENDA
FROM ga_empr AS e
INNER JOIN DVRY_CADASTROS.tb_banco_dados AS b
  ON e.ID_ORIGEM_DB = b.ID_DOMAIN
INNER JOIN regiao_fazenda AS r
  ON r.COD_FAZENDA = e.COD_FAZENDA
""")
  return df

# COMMAND ----------

# DBTITLE 1,Orquestraçao de carga
# geraçao de tabelas tratadas
#pims
gera_tabela_upnivel2()
gera_tabela_upnivel1()
gera_tabela_unifederacao()
gera_tabela_municipio()
gera_tabela_filial()
gera_tabela_upnivel1()

#Sharepoint
gera_tabela_regiao_fazenda()

#gatec
gera_tabela_ga_empr()

# geraçao de tabela de saida
df_pims = gera_tabela_saida_pims()

# se ja esta gravado dados consolidados do legado, tras estes dados prontos, senao, grava
#if not os.path.isdir('/dbfs/mnt/dvryzone-slc/GATEC_SAF_LEGADO/'+table):
df_gatec = gera_tabela_saida_gatec()
#df_gatec.write.format('parquet').mode('overwrite').save('/mnt/dvryzone-slc/GATEC_SAF_LEGADO/'+table)
#
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

