# Databricks notebook source
# MAGIC %md
# MAGIC ### NB_Carga_GECEX_DESP_PREV_PROCESSO_EXPORTACAO ###
# MAGIC Tabela de Despesas previstas do processo de exportação
# MAGIC 
# MAGIC 
# MAGIC Data de criação: 27/07/2022 -- Responsavel: Bruno Martins Medeiros

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Criaçao de variaveis
owner = 'DVRY_GECEX'
table = 'DESP_PREV_PROCESSO_EXPORTACAO'
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
  SELECT DISTINCT 
          EXP_PROCEXP_DES_PV.COD_EMPRESA                                  AS COD_EMPRESA,
          CAST(EXP_PROCEXP_DES_PV.COD_FILIAL AS INT)                      AS COD_FILIAL,
          CAST(EXP_PROCEXP_DES_PV.NUM_SEQUENCIA AS INT)                   AS NUM_SEQUENCIA,
          CAST(EXP_PROCEXP_DES_PV.COD_RECDESP AS INT)                     AS COD_RECDESP,
          FIN_RECDESP.DES_RECDESP                                         AS DES_RECDESP,   
          EXP_PROCEXP_DES_PV.FORMULA                                      AS FORMULA,   
          CAST(EXP_PROCEXP_DES_PV.VAL_PREVISTO AS DECIMAL(18,2))          AS VAL_PREVISTO,
          CAST(EXP_PROCEXP_DES_PV.COD_MOEDA AS INT)                       AS COD_MOEDA,
          EXP_MOEDA.NOM_MOEDA                                             AS NOM_MOEDA,
          EXP_PROCEXP_DES_PV.OBSERVACAO                                   AS OBSERVACAO,
          CAST(EXP_PROCEXP_DES_PV.COD_FAVORECIDO AS DECIMAL(18,2))        AS COD_FAVORECIDO,
          EXP_PESSOA_A.NOM_PESSOA_ABREV                                   AS NOM_PESSOA_ABREV,   
          CAST(EXP_PROCEXP_DES_PV.VAL_AJUSTADO AS DECIMAL(18,2))          AS VAL_AJUSTADO,
          CAST(0 AS DECIMAL(18,2))                                        AS tip_fornecedor,   
          CAST(1 AS DECIMAL(18,2))                                        AS tip_pre_calculo,   
          CAST(EXP_PROCEXP_DES_PV.NUM_FORMULA_INFO AS DECIMAL(18,2))      AS NUM_FORMULA_INFO,
          CAST(EXP_PROCEXP_DES_PV.NUM_LISTA AS INT)                       AS NUM_LISTA,   
          EXP_PROCEXP_DES_PV.NUM_PROCESSO                                 AS NUM_PROCESSO,   
          CAST(0.01 AS DECIMAL(18,2))                                     AS val_complementar,   
          'N'                                                             AS seleciona,   
          CAST(EXP_PROCEXP.COD_EXPORTADOR AS DECIMAL(18,2))               AS COD_EXPORTADOR,
          EXP_DESP_NF_PROCE.NUM_PROCESSO                                  AS NUM_PROCESSO,   
          '          '                                                    AS num_processo,   
          CAST(EXP_PROCEXP_DES_PV.AUTORIZADA AS INT)                      AS AUTORIZADA,
          CAST(0 AS DECIMAL(18,2))                                        AS cancelado,   
          CAST(EXP_PROCEXP_DES_PV.COD_PREST_SERV_RAS AS DECIMAL(18,2))    AS COD_PREST_SERV_RAS,
          EXP_PESSOA_B.NOM_PESSOA_ABREV                                   AS NOM_PESSOA_ABREV

  FROM TRUSTED_GECEX.EXP_PROCEXP_DES_PV AS EXP_PROCEXP_DES_PV
  
  LEFT JOIN TRUSTED_GECEX.EXP_PESSOA AS EXP_PESSOA_A
      ON EXP_PROCEXP_DES_PV.COD_FAVORECIDO = EXP_PESSOA_A.COD_PESSOA
      
  LEFT JOIN TRUSTED_GECEX.EXP_DESP_NF_PROCE AS EXP_DESP_NF_PROCE
      ON EXP_PROCEXP_DES_PV.COD_RECDESP = EXP_DESP_NF_PROCE.COD_RECDESP
        AND EXP_PROCEXP_DES_PV.COD_EMPRESA = EXP_DESP_NF_PROCE.COD_EMPRESA
        AND EXP_PROCEXP_DES_PV.COD_FILIAL = EXP_DESP_NF_PROCE.COD_FILIAL
        AND EXP_PROCEXP_DES_PV.NUM_PROCESSO = EXP_DESP_NF_PROCE.NUM_PROCESSO
      
  LEFT JOIN TRUSTED_GECEX.EXP_PESSOA AS EXP_PESSOA_B
      ON EXP_PROCEXP_DES_PV.COD_PREST_SERV_RAS = EXP_PESSOA_B.COD_PESSOA
      
  INNER JOIN TRUSTED_GECEX.EXP_MOEDA AS EXP_MOEDA
      ON EXP_PROCEXP_DES_PV.COD_MOEDA = EXP_MOEDA.COD_MOEDA
      
  INNER JOIN TRUSTED_GECEX.FIN_RECDESP AS FIN_RECDESP
      ON EXP_PROCEXP_DES_PV.COD_RECDESP = FIN_RECDESP.COD_RECDESP
  
  INNER JOIN TRUSTED_GECEX.EXP_PROCEXP AS EXP_PROCEXP
      ON EXP_PROCEXP_DES_PV.COD_EMPRESA = EXP_PROCEXP.COD_EMPRESA
        AND EXP_PROCEXP_DES_PV.COD_FILIAL = EXP_PROCEXP.COD_FILIAL
        AND EXP_PROCEXP_DES_PV.NUM_PROCESSO = EXP_PROCEXP.NUM_PROCESSO
       
  WHERE ((EXP_PROCEXP_DES_PV.COD_EMPRESA = '01') AND
        (EXP_PROCEXP_DES_PV.COD_FILIAL = 1) AND
        (EXP_PROCEXP_DES_PV.NUM_PROCESSO = 'P010036/21'))
    AND ((exp_procexp_des_pv.num_lista is NULL) OR
        (exp_procexp_des_pv.num_lista is NOT NULL) AND
        (exp_procexp_des_pv.autorizada = 1));""")
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

