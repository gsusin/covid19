from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

pasta_hdfs = "/user/giancarlo/covid19"
spark = SparkSession.builder.appName("Covid19").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("WARN")

covid19_df = spark.read.csv(pasta_hdfs + "/entrada", sep=";", header=True, inferSchema=True)

covid19_df.printSchema()
covid19_df.show(10)

covid19_df = covid19_df.dropna(subset=["municipio"])
covid19_df = covid19_df.select(\
    "regiao",\
    "estado",\
    "municipio",\
    "data",\
    "semanaEpi",\
    "populacaoTCU2019",\
    "casosAcumulado",\
    "obitosAcumulado",\
    "casosNovos",\
    "obitosNovos",\
)

# Últimas estatísticas por município
window_municipio = Window.partitionBy("municipio").orderBy(col("data").desc())
covid19_municipio_df = covid19_df.withColumn("row", row_number().over(window_municipio))
covid19_municipio_df = covid19_municipio_df.where(\
    covid19_municipio_df["row"] == 1\
).drop("row")
covid19_municipio_df = covid19_municipio_df.withColumn(\
    "incidencia_100mil",\
    format_number(\
        covid19_municipio_df["casosAcumulado"]\
        / covid19_municipio_df["populacaoTCU2019"]\
        * 100000,\
        1,\
    ),\
).withColumn(\
    "mortalidade_100mil",\
    format_number(\
        covid19_municipio_df["obitosAcumulado"]\
        / covid19_municipio_df["populacaoTCU2019"]\
        * 100000,\
        1,\
    ),\
)
print("Últimas estatísticas por município:")
covid19_municipio_df.orderBy(col("casosAcumulado").desc()).show(10)
covid19_municipio_df.write.saveAsTable(\
    "default.covid19_municipio",\
    mode="overwrite",\
    partitionBy="municipio",\
)

# Histórico diário do Brasil
covid19_diario_df = covid19_df.groupBy("data").agg(\
    sum("casosAcumulado"), sum("obitosAcumulado"), sum("casosNovos"), sum("obitosNovos")\
)
covid19_diario_df = \
    covid19_diario_df.withColumnRenamed("sum(casosAcumulado)", "casosAcumulado")\
    .withColumnRenamed("sum(obitosAcumulado)", "obitosAcumulado")\
    .withColumnRenamed("sum(casosNovos)", "casosNovos")\
    .withColumnRenamed("sum(obitosNovos)", "obitosNovos")\

print("Histórico diário do Brasil:")
covid19_diario_df.orderBy(col("data").desc()).show(10)
covid19_diario_df.write.parquet(pasta_hdfs + "/saida", compression="snappy")

# Casos e óbitos por estado
covid19_estado_df = covid19_municipio_df.groupBy(["regiao", "estado"]).agg(\
    sum("casosAcumulado"),\
    sum("obitosAcumulado"),\
    sum("casosNovos"),\
    sum("obitosNovos"),\
    sum("populacaoTCU2019"),\
    max("data"),\
)
covid19_estado_df = \
    covid19_estado_df.withColumnRenamed("sum(casosAcumulado)", "casosAcumulado")\
    .withColumnRenamed("sum(obitosAcumulado)", "obitosAcumulado")\
    .withColumnRenamed("sum(casosNovos)", "casosNovos")\
    .withColumnRenamed("sum(obitosNovos)", "obitosNovos")\
    .withColumnRenamed("sum(populacaoTCU2019)", "populacaoTCU2019")\
    .withColumnRenamed("max(data)", "atualizacao")
covid19_estado_df = covid19_estado_df.withColumn(\
    "incidencia_100mil",\
    format_number(\
        covid19_estado_df["casosAcumulado"]\
        / covid19_estado_df["populacaoTCU2019"]\
        * 100000,\
        1,\
    ),\
).withColumn(\
    "mortalidade_100mil",\
    format_number(\
        covid19_estado_df["obitosAcumulado"]\
        / covid19_estado_df["populacaoTCU2019"]\
        * 100000,\
        1,\
    ),\
)

print("Casos e óbitos por estado:")
covid19_estado_df.drop("populacaoTCU2019").drop("casosNovos").drop("obitosNovos")\
    .orderBy(col("regiao")).show(30)

spark.stop()
