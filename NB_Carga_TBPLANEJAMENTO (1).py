# Databricks notebook source
# MAGIC %md
# MAGIC ### DOMÍNIO planejamento ###
# MAGIC Criação do domínio de PLANEJAMENTO.
# MAGIC 
# MAGIC Data de criação: 29/04/2022 -- Responsável: Luan Borges da Fonseca

# COMMAND ----------

from pyspark.sql.functions import *
import os

# COMMAND ----------

# DBTITLE 1,Criaçao de variaveis
owner = 'DVRY_PRODUCAO'
table = 'PLANEJAMENTO'
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
def gera_tabela_upn3_plan():
  df = spark.sql("""
SELECT DISTINCT
       CAST(ID_UPN3_PLAN AS BIGINT)              AS ID_UPN3_PLAN,
       2000                                      AS ID_ORIGEM_DB,
       CAST(ID_PERIODOSAFRA AS BIGINT)           AS ID_PERIODOSAFRA,
       CAST(ID_UPN3_FISICO AS BIGINT)            AS ID_UPN3_FISICO,
       CD_UPNIVEL3                               AS CD_UPNIVEL3,
       CAST(ID_VARIEDADE AS BIGINT)              AS ID_VARIEDADE,
       CAST(ID_UPNIVEL2 AS BIGINT)               AS ID_UPNIVEL2,
       CAST(ID_CENARIO AS BIGINT)                AS ID_CENARIO,
       CAST(SUM(QT_AREA_PROD) AS DECIMAL(18,6))  AS QT_AREA_PROD,
       TO_DATE(DT_EST_PLANT)                     AS DT_EST_PLANT,
       TO_DATE(DT_ESTIMADA_COLH)                 AS DT_ESTIMADA_COLH,
       CAST(QT_PRODUCAO_ESTIM AS DECIMAL(18,6))  AS QT_PRODUCAO_ESTIM,
       CAST(QT_PROD_ESTIM AS DECIMAL(18,6))      AS QT_PROD_ESTIM
FROM trusted_tezkk0_pims2.upn3_plan
GROUP BY ID_UPN3_PLAN,
         ID_PERIODOSAFRA,
         ID_UPN3_FISICO,
         ID_VARIEDADE,
         ID_UPNIVEL2,
         ID_CENARIO,
         DT_EST_PLANT,
         DT_ESTIMADA_COLH,
         QT_PRODUCAO_ESTIM,
         QT_PROD_ESTIM,
         CD_UPNIVEL3
""")
  df.createOrReplaceTempView("upn3_plan")

# COMMAND ----------

def gera_tabela_cenario():
  df = spark.sql("""
SELECT DISTINCT
       CAST(ID_CENARIO AS BIGINT) AS ID_CENARIO,
       CD_CENARIO
FROM trusted_tezkk0_pims2.Cenario
WHERE DA_CENARIO = 'PLAN'
""")
  df.createOrReplaceTempView("cenario")

# COMMAND ----------

def gera_tabela_periodosafra():
  df = spark.sql("""
SELECT DISTINCT
       CAST(ID_PERIODOSAFRA AS BIGINT) AS ID_PERIODOSAFRA,
       CONCAT('2000',ID_SAFRA,RIGHT(COALESCE(CD_INT_ERP,
           CASE
           WHEN INSTR(DE_PER_SAFRA, '-') > 0 
           THEN SUBSTR(DE_PER_SAFRA FROM INSTR(DE_PER_SAFRA, '-') FOR 2)
           ELSE NULL
         END,
         '-1'),1))                     AS ID_SAFRA,
       CAST(ID_OCUPACAO AS BIGINT)     AS ID_OCUPACAO
FROM trusted_tezkk0_pims2.periodosafra
""")
  df.createOrReplaceTempView("periodosafra")

# COMMAND ----------


def gera_tabela_saida_pims():
  df = spark.sql("""
  SELECT DISTINCT
       u.ID_UPN3_PLAN                                                                                              AS ID_DOMAIN,
       u.ID_ORIGEM_DB,
       CONCAT(d.ID_DOMAIN,u.ID_PERIODOSAFRA,u.ID_UPN3_FISICO,u.ID_VARIEDADE,u.CD_UPNIVEL3)                         AS ID_PLANEJAMENTO_AGRICOLA,
       d.DESC_BANCO_DADOS                                                                                          AS ORIGEM_DB,
       CONCAT(d.ID_DOMAIN,u.ID_UPN3_PLAN)                                                                          AS ID_LAV_LOGICA,
       CONCAT(d.ID_DOMAIN,u.ID_UPN3_FISICO)                                                                        AS ID_LAV_FISICA,
       l.COD_LAVOURA,                  
       p.ID_SAFRA,                  
       s.ANO_AGRICOLA,                  
       s.SAFRINHA,                  
       CONCAT(d.ID_DOMAIN,u.ID_VARIEDADE)                                                                          AS ID_VARIEDADE,
       v.COD_VARIEDADE,                  
       v.CICLO_VARIEDADE,                  
       CONCAT(d.ID_DOMAIN,p.ID_OCUPACAO)                                                                           AS ID_CULTURA,
       c.COD_CULTURA,                  
       c.CULTURA,                  
       c.FATOR_CONVERSAO,                  
       CONCAT(d.ID_DOMAIN,u.ID_UPNIVEL2)                                                                           AS ID_SEDE,
       sd.COD_SEDE,                  
       sd.UF,                  
       sd.REGIAO,                  
       sd.REGIONAL,                  
       f.ID_FAZENDA,                  
       f.SIGLA_FAZENDA,                  
       ce.CD_CENARIO                                                                                                AS COD_SIMULACAO,
       u.QT_AREA_PROD                                                                                               AS AREA_PLANEJADA,
       u.DT_EST_PLANT                                                                                               AS DATA_PLANTIO_PLAN,
       u.DT_ESTIMADA_COLH                                                                                           AS DATA_COLHEITA_PLAN,
       u.QT_PRODUCAO_ESTIM                                                                                          AS PRODUCAO_KG_PLANEJADA,
       u.QT_PROD_ESTIM                                                                                              AS PRODUTIVIDADE_PLANEJADA
FROM upn3_plan AS u
INNER JOIN DVRY_CADASTROS.tb_banco_dados AS d
  ON u.ID_ORIGEM_DB = d.ID_DOMAIN
INNER JOIN periodosafra AS p
  ON p.ID_PERIODOSAFRA = u.ID_PERIODOSAFRA
INNER JOIN cenario AS ce
  ON ce.ID_CENARIO = u.ID_CENARIO
INNER JOIN dvry_producao.LAVOURA_FISICA AS l
  ON l.ID_DOMAIN = u.ID_UPN3_FISICO
  AND l.ID_ORIGEM_DB = u.ID_ORIGEM_DB
INNER JOIN dvry_producao.safra AS s
  ON s.ID_SAFRA = p.ID_SAFRA
  AND s.ID_ORIGEM_DB = u.ID_ORIGEM_DB
INNER JOIN dvry_producao.variedade AS v
  ON v.ID_DOMAIN = u.ID_VARIEDADE
  AND v.ID_ORIGEM_DB = u.ID_ORIGEM_DB
INNER JOIN dvry_producao.cultura AS c
  ON c.ID_DOMAIN = p.ID_OCUPACAO
  AND c.ID_ORIGEM_DB = u.ID_ORIGEM_DB
INNER JOIN dvry_producao.sede AS sd
  ON sd.ID_DOMAIN = u.ID_UPNIVEL2
  AND sd.ID_ORIGEM_DB = u.ID_ORIGEM_DB
INNER JOIN dvry_producao.fazenda AS f
  ON f.ID_FAZENDA = sd.ID_FAZENDA""")
  return df 


# COMMAND ----------

gera_tabela_upn3_plan()
gera_tabela_cenario()
gera_tabela_periodosafra()
df_pims = gera_tabela_saida_pims()

# COMMAND ----------

df_pims.createOrReplaceTempView("df_pims")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM df_pims
# MAGIC 
# MAGIC WHERE SIGLA_FAZENDA =  'PZ'
# MAGIC AND ANO_AGRICOLA = '2021/22'
# MAGIC AND CULTURA = 'Soja'
# MAGIC AND COD_LAVOURA = '119A'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dvry_producao.variedade
# MAGIC where COD_VARIEDADE = 429

# COMMAND ----------

def gera_tabela_ga_prp_divi4():
  df = spark.sql("""
SELECT DISTINCT 
       CAST(ID_DIVI4 AS INT)                                                            AS ID_DIVI4,
       COD_SAFRA,
       1000                                                                             AS ID_ORIGEM_DB,
       CAST(COALESCE(COD_VARIE,'0') AS INT)                                             AS COD_VARIE,
       CAST(ID_DIVI3 AS INT)                                                            AS ID_DIVI3,
       CONCAT(SUBSTRING(COD_SAFRA,0,4),COALESCE(NULLIF(SUBSTRING(COD_SAFRA,9,1),''),1)) AS ID_SAFRA,
       COD_CULTURA,
       CAST(COD_EMPR AS INT)                                                            AS COD_EMPR,
       CAST(COD_SIMULACAO AS INT)                                                       AS COD_SIMULACAO,
       CAST(DV4_AREA AS DECIMAL(18,6))                                                  AS DV4_AREA,
       TO_DATE(DV4_DT_PLANTIO,'yyyyMMddhhmmss')                                         AS DV4_DT_PLANTIO,
       TO_DATE(DV4_DT_COLH,'yyyyMMddhhmmss')                                            AS DV4_DT_COLH,
       CAST(DV4_PROD_EST AS DECIMAL(18,6))                                              AS DV4_PROD_EST,
       CAST(SAF_ANO_SAFRA AS INT)                                                       AS SAF_ANO_SAFRA
FROM trusted_gatec_prp_legado.ga_prp_divi4
""")
  df.createOrReplaceTempView("ga_prp_divi4")

# COMMAND ----------

def gera_tabela_ga_prp_simula():
  df = spark.sql("""
SELECT DISTINCT 
       CAST(SAF_ANO_SAFRA AS INT) AS SAF_ANO_SAFRA,
       CAST(COD_SIMULACAO AS INT) AS COD_SIMULACAO
FROM trusted_gatec_prp_legado.ga_prp_simula
WHERE SIM_ATIVA IN (3,4)
""")
  df.createOrReplaceTempView("ga_prp_simula")

# COMMAND ----------


def gera_tabela_saida_gatec():
  df = spark.sql("""
  SELECT DISTINCT 
       d4.ID_DIVI4                                                                                                 AS ID_DOMAIN,
       d4.ID_ORIGEM_DB,
       CONCAT(d.ID_DOMAIN,REPLACE(REPLACE(d4.COD_SAFRA,'/',''),'-',''),d4.ID_DIVI4,d4.COD_VARIE)                   AS ID_PLANEJAMENTO_AGRICOLA,
       d.DESC_BANCO_DADOS                                                                                          AS ORIGEM_DB,
       CONCAT(d.ID_DOMAIN,d4.ID_DIVI4)                                                                             AS ID_LAV_LOGICA,
       CONCAT(d.ID_DOMAIN,d4.ID_DIVI3)                                                                             AS ID_LAV_FISICA,
       l.COD_LAVOURA,                  
       CONCAT(d.ID_DOMAIN,d4.ID_SAFRA)                                                                             AS ID_SAFRA,                  
       d4.SAF_ANO_SAFRA                                                                                            AS ANO_AGRICOLA,                  
       sf.SAFRINHA,                  
       CONCAT(d.ID_DOMAIN,d4.COD_VARIE)                                                                            AS ID_VARIEDADE,
       v.COD_VARIEDADE,                  
       v.CICLO_VARIEDADE,                  
       CONCAT(d.ID_DOMAIN,d4.COD_CULTURA)                                                                           AS ID_CULTURA,
       c.COD_CULTURA,                  
       c.CULTURA,                  
       c.FATOR_CONVERSAO,                  
       CONCAT(d.ID_DOMAIN,d4.COD_EMPR)                                                                             AS ID_SEDE,
       s.COD_SEDE,                  
       s.UF,                  
       s.REGIAO,                  
       s.REGIONAL,                  
       CONCAT(d.ID_DOMAIN,d4.COD_EMPR)                                                                             AS ID_FAZENDA,                  
       f.SIGLA_FAZENDA,                  
       d4.COD_SIMULACAO                                                                                            AS COD_SIMULACAO,
       d4.DV4_AREA                                                                                                 AS AREA_PLANEJADA,
       d4.DV4_DT_PLANTIO                                                                                           AS DATA_PLANTIO_PLAN,
       d4.DV4_DT_COLH                                                                                              AS DATA_COLHEITA_PLAN,
       CAST(0 AS DECIMAL(18,6))                                                                                    AS PRODUCAO_KG_PLANEJADA,
       d4.DV4_PROD_EST                                                                                             AS PRODUTIVIDADE_PLANEJADA
FROM ga_prp_divi4 AS d4
INNER JOIN ga_prp_simula AS si
    ON si.COD_SIMULACAO = d4.COD_SIMULACAO
    AND si.SAF_ANO_SAFRA = d4.SAF_ANO_SAFRA
INNER JOIN DVRY_CADASTROS.tb_banco_dados AS d
  ON d4.ID_ORIGEM_DB = d.ID_DOMAIN
INNER JOIN dvry_producao.lavoura_fisica AS l
  ON l.ID_LAV_FISICA = CONCAT(d.ID_DOMAIN,d4.ID_DIVI3)
INNER JOIN dvry_producao.variedade AS v
  ON v.ID_VARIEDADE = CONCAT(d.ID_DOMAIN,d4.COD_VARIE)
INNER JOIN dvry_producao.cultura AS c
  ON c.ID_CULTURA = CONCAT(d.ID_DOMAIN,d4.COD_CULTURA)
INNER JOIN dvry_producao.sede AS s
  ON s.ID_SEDE = CONCAT(d.ID_DOMAIN, d4.COD_EMPR)
INNER JOIN dvry_producao.fazenda AS f
  ON f.ID_FAZENDA = CONCAT(d.ID_DOMAIN, d4.COD_EMPR)
INNER JOIN dvry_producao.safra AS sf
  ON sf.ID_SAFRA = CONCAT(d.ID_DOMAIN,d4.ID_SAFRA)
  WHERE d4.SAF_ANO_SAFRA <= 2020""")
  return df 


# COMMAND ----------

# DBTITLE 1,Orquestraçao de carga
# geraçao de tabelas tratadas
#pims
gera_tabela_upn3_plan()
gera_tabela_cenario()
gera_tabela_periodosafra()

#gatec
gera_tabela_ga_prp_divi4()
gera_tabela_ga_prp_simula()

# geraçao de tabela de saida
df_pims = gera_tabela_saida_pims()

# se ja esta gravado dados consolidados do legado, tras estes dados prontos, senao, grava
# if not os.path.isdir('/dbfs/mnt/dvryzone-slc/GATEC_SAF_LEGADO/'+table):
df_gatec = gera_tabela_saida_gatec()
#df_gatec.write.format('parquet').mode('overwrite').save('/mnt/dvryzone-slc/GATEC_SAF_LEGADO/'+table)
#
#df_gatec = spark.read.parquet('/mnt/dvryzone-slc/GATEC_SAF_LEGADO/'+table)
#
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

display(df)

# COMMAND ----------

df.createOrReplaceTempView("df")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM df
