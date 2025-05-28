# Databricks notebook source
# MAGIC %md
# MAGIC ## Projeto 1

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/Airline_Data.csv"))
#Buscar o arquivo na pasta  



# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/FileStore/tables/Airline_Data.csv")
display(df)
#Lendo o arquivo csv 

# COMMAND ----------

display( df.where(df.Current.isNull()) )
##Filtrando todos os valores nullos da coluna "Current" 

# COMMAND ----------

df.columns

# COMMAND ----------

#Tratando dados nullos do dataframe 
novo_df =  df.fillna('0', subset=['Current','Future','Historic','Total','Orders','Unit Cost','Total Cost (Current)','Average Age'])  
display(novo_df)

# COMMAND ----------

for col in novo_df.columns:
    novo_df = novo_df.withColumnRenamed(col, col.replace(" ", "_"))
display(novo_df)

# COMMAND ----------

novo_df.columns

# COMMAND ----------

#Criando uma nova Coluna 
from pyspark.sql.functions import concat,lit
novo_df_completo = novo_df.withColumn("Aeronave",concat(novo_df.Parent_Airline  , lit(" - ") ,lit("100%")))
display(novo_df_completo)


        

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS CUBOS;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use CUBOS;
# MAGIC CREATE TABLE IF NOT EXISTS Aeronave
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   'path' '/FileStore/tables/Airline_Data.csv',  -- Caminho para o arquivo no DBFS
# MAGIC   'header' 'true',                               -- Se a primeira linha do arquivo contém cabeçalho
# MAGIC   'inferSchema' 'true'                           -- Inferir automaticamente os tipos de dados das colunas
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC use CUBOS;
# MAGIC select * from Aeronave

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC use Cubos;
# MAGIC --  Consulta de Contatos de Clientes convertendo em um DF
# MAGIC select 
# MAGIC `Parent Airline`,
# MAGIC count(`Parent Airline`) as Count_Aer
# MAGIC from aeronave
# MAGIC group by `Parent Airline`
# MAGIC
# MAGIC -- Usando comando SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Projeto 2
# MAGIC

# COMMAND ----------

df.write \
  .format('parquet') \
  .mode("ignore") \
  .save("/FileStore/tables/Airline_Data.parquet")

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables/Airline_Data.parquet/"))

# COMMAND ----------

#Particionando arquivo parquet 
# Este particionamento  está demorando pois está sendo particionado por Airline","Aircraft Type" , o particionamento é mais eficinete quando é realizado a partição de ano , mes , categoria estado por exemplo quando o particionamento não é eficiente não é muito considerado em termo de performace 
"""
df.write \
    .partitionBy("Airline") \
    .mode("overwrite") \
    .parquet("FileStore/tables/Airline_Data.parquet")
display(df)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Projeto 4

# COMMAND ----------

df = spark.read.option("header", True).csv("dbfs:/FileStore/tables/Airline_Data.csv")
display(df)

# COMMAND ----------

print(df.columns)

# COMMAND ----------

from pyspark.sql.functions import col 
novo_df2 =  df\
    .withColumn("Current",df.Current.cast('integer'))\
    .withColumn("Future",df.Future.cast('integer'))\
    .withColumn("Historic",df.Historic.cast('integer'))\
    .withColumn("Total",df.Total.cast('integer'))\
    .withColumn("Orders",df.Orders.cast('integer'))\
    .withColumn("Average Age",col("Average Age").cast('integer'))   
print(novo_df2)



# COMMAND ----------

##### TESTE
#from pyspark.sql.functions import col 
#novo_df = df.withColumn("Unit Cost",col("Unit Cost").cast("integer"))

# COMMAND ----------

novo_df2 = novo_df2\
    .withColumnRenamed("Parent Airline","Parent_Airline")\
    .withColumnRenamed("Aircraft Type","Aircraft_Type")\
    .withColumnRenamed("Unit Cost","Unit_Cost")\
    .withColumnRenamed("Total Cost (Current)","Total_Cost")\
    .withColumnRenamed("Average Age","Average_Age")
display(novo_df2)

# COMMAND ----------

#  Ler dados de um banco SQL Server com JDBC , fazer agregações com Pyspark e gravar de volta em outra tabela.
display(novo_df2.groupBy("Parent_Airline").sum("Current","Total").orderBy("sum(Current)",ascending=False))

# COMMAND ----------

novo_df2.createOrReplaceTempView("tabela_Aeronave")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabela_Aeronave

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC Parent_Airline AS Companhia_Aerea,
# MAGIC sum(Current) AS Atual,
# MAGIC sum(Total) as Total
# MAGIC from tabela_Aeronave
# MAGIC group by Parent_Airline
# MAGIC order by Atual desc
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Projeto (NOVO_2)

# COMMAND ----------

#Tirando os espaços trim
from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, ltrim, rtrim, col

# Create a Spark session
spark = SparkSession.builder.appName("WhiteSpaceRemoval").getOrCreate()

# Define your data
data = [(1, "ABC    "), (2, "     DEF"), (3, "        GHI    ")]

# Create a DataFrame
df = spark.createDataFrame(data, ["col1", "col2"])

# Show the initial DataFrame
df.show()

# Using withColumn to remove white spaces
df = df.withColumn("col2", rtrim(col("col2")))
df.show()

# COMMAND ----------

novo_df2 = novo_df2.withColumn("Total", rtrim(col("Total")))
display(novo_df2)


# COMMAND ----------

from pyspark.sql.functions import upper 
novo_df3 = novo_df2\
    .withColumn('Parent_Airline', upper(novo_df2['Parent_Airline']))\
    .withColumn('Airline', upper(novo_df2['Airline']))\
    .withColumn('Aircraft_Type', upper(novo_df2['Aircraft_Type'])) 
display(novo_df3)

# COMMAND ----------

print(novo_df3.columns)

# COMMAND ----------

# Utilizando o regexp_replace para substituir na coluna "" o $ por vazio 
from pyspark.sql.functions import regexp_replace , col
novo_df4 = novo_df3\
    .withColumn('Unit_Cost', regexp_replace('Unit_Cost', '$', '')) \
    .withColumn('Total_Cost', regexp_replace('Total_Cost', '$', ''))
display(novo_df4)


# COMMAND ----------

from pyspark.sql.functions import regexp_replace , col
novo_df3 =  novo_df3\
    .withColumn("Unit_Cost", regexp_replace(col("Unit_Cost"), "\\$", ""))\
    .withColumn("Total_Cost", regexp_replace(col("Unit_Cost"), "\\$", ""))  
display(novo_df3)

# COMMAND ----------

#Removendo 100% de linhas iguais em todas as colunas
remov_duplic = novo_df3.drop_duplicates()
display(remov_duplic)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Desafio 2 . Classificação de faixas

# COMMAND ----------

df_new = spark.read.option("header",True).csv('dbfs:/FileStore/tables/Airline_Data.csv')
display(df_new)

# COMMAND ----------

for col_name in df_new.columns:
    df_new = df_new.withColumnRenamed(col_name, col_name.replace(" ", "_"))
display(df_new)

# COMMAND ----------


from pyspark.sql.functions import regexp_replace , col
colunas_valores = ["Unit_Cost","Total_Cost_(Current)"]
for  c in colunas_valores:
    df_new = df_new.withColumn(c , regexp_replace(col(c) , "\\$" ,""))\
                   .withColumn(c,col(c).cast("integer"))
display(df_new)

# COMMAND ----------

df_new =  (
    df_new.withColumn("Current",col("Current").cast('integer'))
    .withColumn("Future",col("Future").cast('integer'))
    .withColumn("Historic",col("Historic").cast('integer'))
    .withColumn("Total",col("Total").cast('integer'))
    .withColumn("Orders",col("Orders").cast('integer'))
    .withColumn("Unit_Cost",col("Unit_Cost").cast('integer'))
)

display(df_new)

# COMMAND ----------

from pyspark.sql.functions import when 
df_new = df_new.withColumn("Classificação_Custo",\
            when(df_new.Unit_Cost < 100 , 'Baixo Custo')\
            .when((df_new.Unit_Cost > 100 ) &  (df_new.Unit_Cost < 300 )  , 'Médio')\
            .when(df_new.Unit_Cost > 300 ,'Alto Custo')
            .otherwise("Sem Custo"))
display(df_new)