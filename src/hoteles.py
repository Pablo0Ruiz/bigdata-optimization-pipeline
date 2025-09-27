# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,desc,sum,round
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType


#configuracion optimizada y sesion de spark
spark = SparkSession.builder\
        .appName('Hoteles')\
        .config('spark.sql.adaptive.enabled','true') \
        .config('spark.sql.adaptive.coalescePartitions.enabled','true')\
        .getOrCreate()



spark.sparkContext.setLogLevel("WARN")



#Lectura de ficheros
ingresos = spark.read.csv(
    "hdfs://namenode:9000/user/spark/input/ingresos.csv", 
    sep=";", header=True, inferSchema=True
    )

hoteles = spark.read.csv(
    "hdfs://namenode:9000/user/spark/input/hoteleseuropa.csv", 
    sep=';', header= True, inferSchema=True
    )



#Cache de los dataFame para mejor performance
ingresos.cache()
hoteles.cache()




#Filtrar los hoteles que pertenezcan a Espana y obtener los ingresos de dichos hoteles

ingresos = ingresos.withColumnRenamed('revenue','dinero_ingresos')

espana = hoteles.filter(col('cc1') == 'es').join(ingresos,on='id',how='inner').select('name','city_hotel','dinero_ingresos')
espana.show(truncate=False)
espana_partitioned = espana.coalesce(2)
espana_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/hoteles-espana",header=True)


#Obtener por pantalla los 100 hoteles con mas ingresos

hoteles100Ingresos = hoteles.join(ingresos,on='id',how='inner').select('name','city_hotel','dinero_ingresos').orderBy(desc('dinero_ingresos')).limit(100)
hoteles100Ingresos.show(truncate=False)
hoteles100Ingresos_partitioned= hoteles100Ingresos.coalesce(2)
hoteles100Ingresos_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/hoteles-100",header=True)

#Obtener por pantalla las 200 ciudades que más ingresos obtuvieron

ciudades200Ingresos = hoteles.join(ingresos,on='id',how='inner').groupBy('city_hotel','name').agg(sum('dinero_ingresos').alias('total_ingresos')).orderBy(desc('total_ingresos')).limit(200)
ciudades200Ingresos.show(truncate=False)
ciudades200Ingresos_partitioned = ciudades200Ingresos.repartition(4,'city_hotel')
ciudades200Ingresos_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/hoteles-ciudade-200",header=True)

#Selecciona los hoteles que no tuvieran ingresos

hotelesSinIngresos = hoteles.join(ingresos,on='id',how='left').filter(ingresos.dinero_ingresos.isNull()).select('name','city_hotel')
hotelesSinIngresos.show(truncate=False)
hotelesSinIngresos_partitioned = hotelesSinIngresos.repartition(2,'city_hotel')
hotelesSinIngresos_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/hoteles-sin-ingresos",header=True)



#View
espana.createOrReplaceTempView('espana')
hoteles100Ingresos.createOrReplaceTempView('hoteles100Ingresos')
ciudades200Ingresos.createOrReplaceTempView('ciudades200Ingresos')
hotelesSinIngresos.createOrReplaceTempView('hotelesSinIngresos')



#Consultas a las view
df1 = spark.sql("SELECT * FROM espana ")
df2 = spark.sql("SELECT * FROM hoteles100Ingresos")
df3 = spark.sql("SELECT * FROM ciudades200Ingresos")
df4 = spark.sql("SELECT * FROM hotelesSinIngresos")


#Mostramos el resultado de las consultas
df1.show()
df2.show()
df3.show()
df4.show()


#Limpiamos el cache para liberar memoria
ingresos.unpersist()
hoteles.unpersist()


#Por ultimo cerramos sesion en spark 
spark.stop()

