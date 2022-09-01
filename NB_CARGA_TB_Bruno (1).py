# Databricks notebook source
# MAGIC %md
# MAGIC ### NB_Carga_GECEX_INFO_PROCESSO_EXPORTACAO ###
# MAGIC Tabela de Informações do Processo de exportação
# MAGIC 
# MAGIC 
# MAGIC Data de criação: 27/07/2022 -- Responsavel: Bruno Martins Medeiros

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Criaçao de variaveis
owner = 'DVRY_GECEX'
table = 'INFO_PROCESSO_EXPORTACAO'
pthDestino = pthDestino = '/mnt/dvryzone-slc/' + owner + '/' + table

print('owner ----> '+owner)
print('table ----> '+table)
print('pthDestino ----> '+pthDestino)

# COMMAND ----------

# DBTITLE 1,Criaçao de schema
#spark.sql("CREATE DATABASE IF NOT EXISTS " + owner)

# COMMAND ----------

# DBTITLE 1,Importa funções genéricas
# MAGIC %run "/Shared/Functions/Functions_global"

# COMMAND ----------

# DBTITLE 1,Dataset de saída
def gera_tabela_saida():
  df = spark.sql(f"""
  SELECT 
       A.NUM_PROCESSO                                                  AS PROCESSO,
       CAST(A.COD_TRANSP_INTER AS DECIMAL(18,2))                       AS COD_ARMADOR,
       B.NOM_PESSOA                                                    AS ARMADOR,
       A.NUM_RESERVA                                                   AS BOOKING,
       CAST((SELECT COUNT(B.NUM_CONTAINER_SEQ)  
          FROM TRUSTED_GECEX.EXP_PROCEXP_CONTAI B  
         WHERE A.NUM_PROCESSO = B.NUM_PROCESSO) AS DECIMAL(18,2))      AS QTD_CONTAINER,
       CAST(A.COD_LOCAL_EMBARQUE AS INT)                               AS COD_TERMINAL,
       C.DEN_LOCAL                                                     AS TERMINAL,
       CAST(A.DAT_PREV_SAIDA AS DATE)                                  AS ETA
       
  FROM TRUSTED_GECEX.EXP_PROCEXP A,
       TRUSTED_GECEX.EXP_PESSOA B,
       TRUSTED_GECEX.EXP_LOCAL C
        
  WHERE A.COD_TRANSP_INTER = B.COD_PESSOA
    AND A.COD_LOCAL_EMBARQUE = C.COD_LOCAL
    AND A.NUM_PROCESSO = 'P010036/21';""")
  return df

# COMMAND ----------

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
#  status = insere_dados_tabela_populada_dvry(df, owner, table, 'overwrite', 'parquet', pthDestino)
  x=x+1
  print(status)

