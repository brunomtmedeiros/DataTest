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
#spark.sql("CREATE DATABASE IF NOT EXISTS " + owner)

# COMMAND ----------

# DBTITLE 1,Importa funções genéricas
# MAGIC %run "/Engenharia/Functions/Functions_global"

# COMMAND ----------

def gera_tabela_EBAN():
  df = spark.sql("""
SELECT DISTINCT
       MANDT,
       LIFNR,
       EBELN,
       EBELP,
       BADAT,
       BANFN,
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
       BSMNG,
       BMEIN,
       FRGDT,
       BEDAT
      
FROM TRUSTED_SAP.EBAN
""")
  df.createOrReplaceTempView("EBAN")

# COMMAND ----------

# MAGIC %sql
# MAGIC select BEDAT FROM TRUSTED_SAP.EBAN

# COMMAND ----------

def gera_tabela_EBKN():
  df = spark.sql("""
SELECT DISTINCT
       BANFN,
       FISTL,
       BNFPO,
       AUFNR,
       AUFNR,
       SAKTO,
       PRCTR,
       KOSTL
FROM TRUSTED_SAP.EBKN
""")
  df.createOrReplaceTempView("EBKN")

# COMMAND ----------

def gera_tabela_EKKO():
  df = spark.sql("""
SELECT DISTINCT
       MANDT,
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
       MANDT,
       LIFNR,
       NAME1
FROM TRUSTED_SAP.LFA1
""")
  df.createOrReplaceTempView("LFA1")

# COMMAND ----------

# DBTITLE 1,Dataset de saída
def gera_tabela_saida():
  df = spark.sql(f"""
SELECT DISTINCT
       EBA.BEDAT AS DATA_PEDIDO,
       EBA.EBELN AS PEDIDO,
       EBA.EBELP AS ITEM_PEDIDO,
       EBA.BADAT AS DATA_SOLICITACAO,
       EBA.BANFN AS REQUISICAO,
       EBA.BNFPO AS ITEM_REQUISICAO,
       EBA.BSART AS TIPO_DOCUMENTO,
       EBA.MATNR AS MATERIAL,
       EBA.TXZ01 AS TEXTO_BREVE,
       EBA.MENGE AS QUANTIDADE,
       EBA.MEINS AS UM,
       EBA.AFNAM AS REQUISITANTE,
       EBA.MATKL AS GRUPO_MERCADORIA,
       EBA.FISTL AS CENTRO,
       EBA.EKGRP AS GRUPO_COMPRAS,
       EBA.ERNAM AS CRIADO_POR,
       EBA.RLWRT AS VALOR_TOTAL,
       EBA.PEINH AS PRECO_AVALIACAO,
       EBA.LOEKZ AS COD_ELIMINACAO,
       EBA.STATU AS STATUS_PROCESSAMENTO,
       EBA.ESTKZ AS COD_CRIACAO,
       EBA.FRGKZ AS COD_LIBERACAO,
       EBA.FRGST AS ESTRATEGIA_LIBERACAO,
       EBA.ERDAT AS DATA_MODIFICACAO,
       EBA.BEDNR AS NUM_ACOMPANHAMENTO,
       EBA.BADAT AS DATA_REMESSA,
       EBA.EKORG AS ORGANIZACAO_COMPRA,
       EBA.BLCKD AS COD_BLOQUEIO,
       EBA.BLCKT AS TXT_BLOQUEIO,
       EBA.FRGDT AS DATA_APROVACAO,
       EBK.AUFNR AS OS,
       EBK.AUFNR AS ORDEM_SERVICO,
       EBK.SAKTO AS CONTA_RAZAO,
       EBK.PRCTR AS CENTRO_LUCRO,
       EBK.KOSTL AS CENTRO_CUSTO,
       EBA.BSMNG AS QUANTIDADE_PEDIDA,
       SUM(EBA.MENGE - EBA.BSMNG) AS QUANTIDADE_PENDENTE,
       EKK.BSART AS TIPO_PEDIDO_COMPRAS,
       EKK.ERNAM AS USUARIO_PEDIDO,
       EKK.RLWRT AS VALOR_TOTAL_PEDIDO,
       EBA.BMEIN AS VALOR_UNIT_PEDIDO,
       EKK.LIFNR AS BP_FORNECEDOR,
       LFA.NAME1 AS NOME_FORNECEDOR,
       SUM(EBA.FRGDT - EBA.BEDAT) AS CALCULO
       
FROM EBAN AS EBA
INNER JOIN EBKN AS EBK
  ON EBA.BNFPO = EBK.BNFPO
    AND EBA.BANFN = EBK.BANFN
INNER JOIN LFA1 AS LFA
  ON EBA.LIFNR = LFA.LIFNR
INNER JOIN EKKO AS EKK
  ON LFA.LIFNR = EKK.LIFNR
  
GROUP BY 
  EBA.BEDAT,
  EBA.EBELN,
  EBA.EBELP,
  EBA.BADAT,
  EBA.BANFN,
  EBA.BNFPO,
  EBA.BSART,
  EBA.MATNR,
  EBA.TXZ01,
  EBA.MENGE,
  EBA.MEINS,
  EBA.AFNAM,
  EBA.MATKL,
  EBA.FISTL,
  EBA.EKGRP,
  EBA.ERNAM,
  EBA.RLWRT,
  EBA.PEINH,
  EBA.LOEKZ,
  EBA.STATU,
  EBA.ESTKZ,
  EBA.FRGKZ,
  EBA.FRGST,
  EBA.ERDAT,
  EBA.BEDNR,
  EBA.BADAT,
  EBA.EKORG,
  EBA.BLCKD,
  EBA.BLCKT,
  EBA.FRGDT,
  EBK.AUFNR,
  EBK.AUFNR,
  EBK.SAKTO,
  EBK.PRCTR,
  EBK.KOSTL,
  EBA.BSMNG,
  EKK.BSART,
  EKK.ERNAM,
  EKK.RLWRT,
  EBA.BMEIN,
  EKK.LIFNR,
  LFA.NAME1
  
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

df.createOrReplaceTempView("df_count")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as total from df_count
# MAGIC --37979 -LFA1
# MAGIC --193679 -EKKO

# COMMAND ----------

# DBTITLE 1,Orquestraçao de carga
# geraçao de tabelas tratadas
#gera_tabela_MARA()
#gera_tabela_MAKT()
#gera_tabela_MARC()
#
## geraçao de tabela de saida
#df = gera_tabela_saida()
#
##Grava saida de dataframes
#status = False
#x = 0
#while status == False and x <10:
#  status = insere_dados_tabela_populada_dvry(df, owner, table, 'overwrite', 'parquet', pthDestino)
#  x=x+1
#  print(status)
