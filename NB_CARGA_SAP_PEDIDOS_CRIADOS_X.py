# Databricks notebook source
# MAGIC %md
# MAGIC ### NB_CARGA_SAP_PEDIDOS_CRIADOS ###
# MAGIC Tabela com informações de pedidos criados do SAP com a visao de suprimentos.
# MAGIC 
# MAGIC 
# MAGIC Data de criação: 15/09/2022 -- Responsavel: Bruno Martins Medeiros

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Criaçao de variaveis
owner = 'DVRY_SUPRIMENTOS'
table = 'PEDIDOS_CRIADOS'
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

def gera_tabela_EKPO():
  df = spark.sql("""
SELECT DISTINCT
       EBELN,
       to_timestamp(cast(cast(CONCAT(CREATIONDATE,CREATIONTIME) as decimal(18,0)) as varchar(18)),'yyyyMMddHHmmss') AS DATA_HORA_CRIACAO,
       HASHCAL_EXISTS AS HASHCAL_EXISTS,
       WERKS,
       EBELP,
       EMATN,
       TXZ01,
       MATKL,
       LOEKZ,
       MENGE,
       MEINS,
       NETPR,
       PEINH,
       BANFN,
       BNFPO,
       BRTWR,
       J_1BNBM,
       MWSKZ,
       KTPNR
FROM TRUSTED_SAP.EKPO
""")
  df.createOrReplaceTempView("EKPO")

# COMMAND ----------

def gera_tabela_EKKO():
  df = spark.sql("""
SELECT DISTINCT
       BSART,
       BSTYP,
       EKGRP,
       GRWCU,
       FRGKE,
       ERNAM,
       ZTERM,
       ZBD1T,
       WKURS,
       INCO1,
       MEMORY,
       LIFNR,
       EBELN
FROM TRUSTED_SAP.EKKO
""")
  df.createOrReplaceTempView("EKKO")

# COMMAND ----------

def gera_tabela_EKET():
  df = spark.sql("""
SELECT DISTINCT
       WEMNG,
       EBELP,
       SLFDT,
       EBELN
FROM TRUSTED_SAP.EKET
""")
  df.createOrReplaceTempView("EKET")

# COMMAND ----------

def gera_tabela_EKKN():
  df = spark.sql("""
SELECT DISTINCT
       KOSTL,
       PRCTR,
       AUFNR,
       SUM(ANLN1 - ANLN2) AS IMOBILIZADO,
       EBELP,
       EBELN
FROM TRUSTED_SAP.EKKN
GROUP BY
       KOSTL,
       PRCTR,
       AUFNR,
       ANLN1,
       ANLN2,
       EBELP,
       EBELN
""")
  df.createOrReplaceTempView("EKKN")

# COMMAND ----------

def gera_tabela_TBFORNECEDOR():
  df = spark.sql("""
SELECT DISTINCT
       FORNECEDOR,
       NOME
FROM DVRY_SAP.TBFORNECEDOR
""")
  df.createOrReplaceTempView("TBFORNECEDOR")

# COMMAND ----------

def gera_tabela_EBAN():
  df = spark.sql("""
SELECT DISTINCT
       BANFN,
       BNFPO,
       FRGGR,
       FRGST,
       FRGKZ,
       BSART
FROM TRUSTED_SAP.EBAN
  """)
  df.createOrReplaceTempView("EBAN")

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

# DBTITLE 1,Dataset de saída
def gera_tabela_saida():
  df = spark.sql(f"""
SELECT DISTINCT
       EKPO.EBELN                                                                                             AS DOC_COMPRAS,
       EKPO.DATA_HORA_CRIACAO                                                                                 AS DATA_DOC,
       EKPO.ORGANIZ_COMPRAS                                                                                   AS ORGC,
       EKPO.WERKS                                                                                             AS CEN,
       EKPO.EBELP                                                                                             AS ITEM,
       EKKO.BSART                                                                                             AS TIPO,
       EKKO.BSTYP                                                                                             AS CTG,
       EKKO.EKGRP                                                                                             AS GCM,
       EKPO.EMATN                                                                                             AS MATERIAL,
       EKPO.TXZ01                                                                                             AS TEXTO_BREVE,
       EKPO.MATKL                                                                                             AS GRPMERCADS,
       EKPO.LOEKZ                                                                                             AS COD_ELIMINACAO,
       EKPO.MENGE                                                                                             AS QTD_PEDIDO,
       EKPO.MEINS                                                                                             AS UMP,
       EKPO.NETPR                                                                                             AS PRECO_LIQ,
       EKPO.PEINH                                                                                             AS POR,
       CAST((((EKPO.MENGE - COALESCE(EKET.WEMNG,0))*EKPO.NETPR)/COALESCE(EKPO.PEINH,1)) AS DECIMAL(18,6))     AS A_FORNECER,
       CAST((EKPO.MENGE - COALESCE(EKET.WEMNG,0)) AS DECIMAL(18,6))                                           AS A_FATURAR,
       EKKO.GRWCU                                                                                             AS MOEDA,
       EKPO.BANFN                                                                                             AS REQUISICAO,
       EKPO.BNFPO                                                                                             AS ITEM_RC,
       EKET.SLFDT                                                                                             AS DT_REMESSA,
       EKKO.FRGKE                                                                                             AS LIB,
       EKKO.ERNAM                                                                                             AS CRIADOR,
       EKKO.ZTERM                                                                                             AS COND_PAGAMENTO,
       EKKO.ZBD1T                                                                                             AS PAGAMENTO_EM,
       EKKO.WKURS                                                                                             AS TAXA_CAMBIO,
       EKKO.INCO1                                                                                             AS INCOTERMS,
       EKKO.MEMORY                                                                                            AS MEMORIZADO,
       EKPO.BRTWR                                                                                             AS VALOR_BRUTO,
       EKPO.J_1BNBM                                                                                           AS NCM,
       EKPO.MWSKZ                                                                                             AS IVA,
       FORN.NOME                                                                                              AS NOME_COMP_FORNECEDOR,
       EKKN.KOSTL                                                                                             AS CENTRO_CUSTO,
       EKKN.PRCTR                                                                                             AS CENTRO_LUCRO,
       EKKN.AUFNR                                                                                             AS ORDEM,
       EKKN.IMOBILIZADO                                                                                       AS IMOBILIZADO,
       EBAN.BSART                                                                                             AS TIPO_RC,
       CONCAT(EKPO.EBELN, EKPO.EBELP)                                                                         AS CHAVE_PEDIDO,
       CONCAT(EKPO.EBELN, EKKO.INCO1)                                                                         AS CHAVE_FRETE
FROM EKPO
INNER JOIN EKKO
  ON EKPO.LPONR = EKKO.LPONR
    AND EKPO.EBELN = EKKO.EBELN
    AND EKPO.KONNR = EKKO.KONNR
INNER JOIN EKET
  ON EKPO.EBELP = EKET.EBELP
LEFT JOIN TBFORNECEDOR AS FORN
  ON EKKO.LIFNR = FORN.FORNECEDOR
INNER JOIN EKKN
  ON EKPO.EBELP = EKKN.EBELP
INNER JOIN EBAN
  ON EKPO.KTPNR = EBAN.KTPNR
""")
  return df

# COMMAND ----------

gera_tabela_EKPO()
gera_tabela_EKKO()
gera_tabela_EKKN()
gera_tabela_EBAN()
gera_tabela_EKET()
gera_tabela_TBFORNECEDOR()
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

gera_tabela_EKPO()
gera_tabela_EKKO()
gera_tabela_EKKN()
gera_tabela_EBAN()
gera_tabela_EKET()
gera_tabela_TBFORNECEDOR()
gera_tabela_CDHDR()
gera_tabela_CDPOS()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC        EKPO.EBELN                                                                                             AS DOC_COMPRAS,
# MAGIC        EKPO.DATA_HORA_CRIACAO                                                                                 AS DATA_DOC,
# MAGIC        EKPO.HASHCAL_EXISTS                                                                                    AS ORGC,
# MAGIC        EKPO.WERKS                                                                                             AS CEN,
# MAGIC        EKPO.EBELP                                                                                             AS ITEM,
# MAGIC        EKKO.BSART                                                                                             AS TIPO,
# MAGIC        EKKO.BSTYP                                                                                             AS CTG,
# MAGIC        EKKO.EKGRP                                                                                             AS GCM,
# MAGIC        EKPO.EMATN                                                                                             AS MATERIAL,
# MAGIC        EKPO.TXZ01                                                                                             AS TEXTO_BREVE,
# MAGIC        EKPO.MATKL                                                                                             AS GRPMERCADS,
# MAGIC        EKPO.LOEKZ                                                                                             AS COD_ELIMINACAO,
# MAGIC        EKPO.MENGE                                                                                             AS QTD_PEDIDO,
# MAGIC        EKPO.MEINS                                                                                             AS UMP,
# MAGIC        EKPO.NETPR                                                                                             AS PRECO_LIQ,
# MAGIC        EKPO.PEINH                                                                                             AS POR,
# MAGIC        CAST((((EKPO.MENGE - COALESCE(EKET.WEMNG,0))*EKPO.NETPR)/COALESCE(EKPO.PEINH,1)) AS DECIMAL(18,6))     AS A_FORNECER,
# MAGIC        CAST((EKPO.MENGE - COALESCE(EKET.WEMNG,0)) AS DECIMAL(18,6))                                           AS A_FATURAR,
# MAGIC        EKKO.GRWCU                                                                                             AS MOEDA,
# MAGIC        EKPO.BANFN                                                                                             AS REQUISICAO,
# MAGIC        EKPO.BNFPO                                                                                             AS ITEM_RC,
# MAGIC        EKET.SLFDT                                                                                             AS DT_REMESSA,
# MAGIC        EKKO.FRGKE                                                                                             AS LIB,
# MAGIC        EKKO.ERNAM                                                                                             AS CRIADOR,
# MAGIC        EKKO.ZTERM                                                                                             AS COND_PAGAMENTO,
# MAGIC        EKKO.ZBD1T                                                                                             AS PAGAMENTO_EM,
# MAGIC        EKKO.WKURS                                                                                             AS TAXA_CAMBIO,
# MAGIC        EKKO.INCO1                                                                                             AS INCOTERMS,
# MAGIC        EKKO.MEMORY                                                                                            AS MEMORIZADO,
# MAGIC        EKPO.BRTWR                                                                                             AS VALOR_BRUTO,
# MAGIC        EKPO.J_1BNBM                                                                                           AS NCM,
# MAGIC        EKPO.MWSKZ                                                                                             AS IVA,
# MAGIC        FORN.NOME                                                                                              AS NOME_COMP_FORNECEDOR, 
# MAGIC        EKKN.KOSTL                                                                                             AS CENTRO_CUSTO,
# MAGIC        EKKN.PRCTR                                                                                             AS CENTRO_LUCRO,
# MAGIC        EKKN.AUFNR                                                                                             AS ORDEM,
# MAGIC        EKKN.IMOBILIZADO                                                                                       AS IMOBILIZADO,
# MAGIC        EBAN.BSART                                                                                             AS TIPO_RC,
# MAGIC        CONCAT(EKPO.EBELN, EKPO.EBELP)                                                                         AS CHAVE_PEDIDO,
# MAGIC        CONCAT(EKPO.EBELN, EKKO.INCO1)                                                                         AS CHAVE_FRETE
# MAGIC FROM EKPO
# MAGIC INNER JOIN EKKO
# MAGIC   ON EKPO.EBELN = EKKO.EBELN
# MAGIC INNER JOIN EKET
# MAGIC   ON EKKO.EBELN = EKET.EBELN
# MAGIC     AND EKPO.EBELP = EKET.EBELP
# MAGIC INNER JOIN EBAN 
# MAGIC   ON EKPO.BANFN = EBAN.BANFN
# MAGIC     AND EKPO.BNFPO = EBAN.BNFPO
# MAGIC INNER JOIN EKKN
# MAGIC   ON EKKO.EBELN = EKKN.EBELN
# MAGIC     AND EKPO.EBELP = EKKN.EBELP
# MAGIC LEFT JOIN TBFORNECEDOR AS FORN
# MAGIC   ON EKKO.LIFNR = FORN.FORNECEDOR
# MAGIC INNER JOIN CDHDR
# MAGIC   ON CDHDR.OBJECTID = EBAN.BANFN
# MAGIC INNER JOIN CDPOS
# MAGIC   ON CDPOS.MANDANT = CDHDR.MANDANT
# MAGIC     AND CDHDR.CHANGENR = CDPOS.CHANGENR
# MAGIC     AND CDPOS.OBJECTCLAS = CDHDR.OBJECTCLAS
# MAGIC     AND CDPOS.OBJECTID = CDHDR.OBJECTID
# MAGIC WHERE EBAN.FRGKZ != 'X'
# MAGIC   AND CDPOS.FNAME = 'FRGKZ'
# MAGIC   AND CDPOS.VALUE_NEW != 'X'
# MAGIC   AND CDHDR.TCODE = 'ME54N'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DVRY_SAP.TBFORNECEDOR

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC FROM EKPO --501738
# MAGIC INNER JOIN EKKO --501109
# MAGIC   ON EKPO.EBELN = EKKO.EBELN
# MAGIC INNER JOIN EKET --501642
# MAGIC   ON EKKO.EBELN = EKET.EBELN
# MAGIC     AND EKPO.EBELP = EKET.EBELP
# MAGIC INNER JOIN EBAN --342137
# MAGIC   ON EKPO.BANFN = EBAN.BANFN
# MAGIC     AND EKPO.BNFPO = EBAN.BNFPO
# MAGIC INNER JOIN EKKN --257214
# MAGIC   ON EKKO.EBELN = EKKN.EBELN
# MAGIC     AND EKPO.EBELP = EKKN.EBELP
# MAGIC LEFT JOIN TBFORNECEDOR AS FORN
# MAGIC   ON EKKO.LIFNR = FORN.FORNECEDOR
# MAGIC WHERE EBAN.FRGKZ != 'X'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trusted_sap.ekkn

# COMMAND ----------

# MAGIC %sql
