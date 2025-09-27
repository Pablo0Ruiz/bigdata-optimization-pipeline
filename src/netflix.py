# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max, min, year, month, to_date, round as round_

#configuracion optimizada y sesion de spark
spark = SparkSession.builder\
        .appName('Netflix')\
        .config('spark.sql.adaptive.enabled','true') \
        .config('spark.sql.adaptive.coalescePartitions.enabled','true')\
        .getOrCreate()




spark.sparkContext.setLogLevel("WARN")



#Lectura de ficheros 
dfNetflix =spark.read.csv("hdfs://namenode:9000/user/spark/input/Netflix_2011_2016.csv",header=True,inferSchema=True)
dfNetflixConversion =spark.read.csv("hdfs://namenode:9000/user/spark/input/USD_EUR_Historical_Data.csv",header=True,inferSchema=True)


#Cache de los dataFame para mejor performance
dfNetflix.cache()
dfNetflixConversion.cache()



#Obtén el dia que las acciones tuvieran el maximo precio
dfMaxHight = dfNetflix.agg(max('High').alias('MaxHigh'))
dfNetflixAccionesAlta = dfNetflix.join(dfMaxHight, dfNetflix["High"] == dfMaxHight["MaxHigh"], "inner").select("Date", round_("High",2).alias('High'))
dfNetflixAccionesAlta.show()
dfNetflixAccionesAlta_partitioned = dfNetflixAccionesAlta.coalesce(1)
dfNetflixAccionesAlta_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-max-acciones",header=True)



#Calcula la media del precio de cierre
dfNetflixMedia = dfNetflix.agg(round_(avg('Close'),2).alias('media_close'))
dfNetflixMedia.show()
dfNetflixMedia_partitioned = dfNetflixMedia.coalesce(1)
dfNetflixMedia_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-media-cierre",header=True)




#Calcula el maximo y minimo de volumen y el dia el que se obtuvo cada uno
dfVolumenMax = dfNetflix.agg(max('Volume').alias('Max_Volume'))
dfNetflixVolumenMax = dfNetflix.join(dfVolumenMax, dfNetflix['Volume'] == dfVolumenMax['Max_Volume'], 'inner').select('Date',col('Volume').alias('Max_Volume'))
dfNetflixVolumenMax.show()
dfNetflixVolumenMax_partitioned = dfNetflixVolumenMax.coalesce(1)
dfNetflixVolumenMax_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-max-volumen",header=True)

dfVolumenMin = dfNetflix.agg(min('Volume').alias('Min_Volume'))
dfNetflixVolumenMin = dfNetflix.join(dfVolumenMin, dfNetflix['Volume'] == dfVolumenMin['Min_Volume']  ,'inner').select('Date',col('Volume').alias('Min_Volume'))
dfNetflixVolumenMin.show()
dfNetflixVolumenMin_partitioned = dfNetflixVolumenMin.coalesce(1)
dfNetflixVolumenMin_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-min-volumen",header=True)



#¿ Cuantos dias el precio de cierre fue mejor de 600$?
dfNetflixCierre600 = dfNetflix.filter(col('Close') >600)
dfNetflixCierre600.show()
dfNetflixCierre600_partitioned = dfNetflixCierre600.repartition(4,'Date')
dfNetflixCierre600_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-mejor-600",header=True)



#¿ Qué porcentaje de dias el precio máximo fue mayor de 500$?
diasHigh500 = dfNetflix.count()
dfNetflixMaximoMayor500 = dfNetflix.filter(col('High') >500).count()
porcentajeHigh500 =( float(dfNetflixMaximoMayor500)/ float(diasHigh500)) * 100
porcentajeHigh500 = round(porcentajeHigh500,2)
dfPorcentaje = spark.createDataFrame([(porcentajeHigh500,)], ['porcentaje_dias_mayor_500'])
dfPorcentaje.show()
dfPorcentaje_partitioned = dfPorcentaje.coalesce(1)
dfPorcentaje_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-porcentaje-mayor-500",header=True)



#Calcula el precio máximo por ano
dfAno = dfNetflix.withColumn('Date',to_date(col('Date'),'yyyy-MM-dd'))
dfNetflixAnoMaximo = dfAno.groupBy(year(col('Date')).alias('Ano')).agg(round_(max('High'),2).alias('Maximo_Precio'))
dfNetflixAnoMaximo.show()
dfNetflixAnoMaximo_partitioned = dfNetflixAnoMaximo.coalesce(1)
dfNetflixAnoMaximo_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-maximo-ano",header=True)



#Calcula el precio medio de cierre para cada mes
dfPrecioCierreMEs = dfAno.groupBy(month(col('Date')).alias('Mes'),year(col('Date')).alias('Ano')).agg(round_(avg('Close'),2).alias('media_close')).orderBy('Ano','Mes')
dfPrecioCierreMEs.show()
dfPrecioCierreMEs_partitioned = dfPrecioCierreMEs.repartition(3,'Mes')
dfPrecioCierreMEs_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-medio-cierre-mes",header=True)



#Contravaloración
#Debes aplicar la conversión del día que corresponda
#Debes aplicar el tipo que corresponda (open, high, low)
#Para los close no hay tipo de cambio, emplea el open

dfContravaloracion = dfNetflixConversion.withColumn('Date',to_date(col('Date'),'MMM dd, yyyy'))
dfContravaloracionNetflix = dfNetflix.withColumn('Date',to_date(col('Date'),'yyyy-MM-dd'))


dfContravaloracion = dfContravaloracion.withColumnRenamed('Open', 'Open_conversion') \
    .withColumnRenamed('High', 'High_conversion') \
    .withColumnRenamed('Low', 'Low_conversion')

dfJoinContravaloracion = dfContravaloracionNetflix.join(dfContravaloracion, on='Date', how='inner')


dfEuro = dfJoinContravaloracion.withColumn('Open_Euro', col('Open') * col('Open_conversion')) \
                               .withColumn('High_Euro', col('High') * col('High_conversion')) \
                               .withColumn('Low_Euro', col('Low') * col('Low_conversion')) \
                               .withColumn('Close_Euro', col('Close') * col('Open_conversion'))



#Cache del dataFame dfEuro para mejor performance
dfEuro.cache()



#Obtén el dia que las acciones tuvieran el maximo precio en EURO
dfEuroHigh = dfEuro.agg(max('High_Euro').alias('Max_High_Euro'))
dfConversionEuroHigh = dfEuro.join(dfEuroHigh, dfEuro["High_Euro"] == dfEuroHigh["Max_High_Euro"], "inner").select("Date", round_("High_Euro",2).alias('High_Euro'))
dfConversionEuroHigh.show()
dfConversionEuroHigh_partitioned = dfConversionEuroHigh.coalesce(1)
dfConversionEuroHigh_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-max-acciones-euro",header=True)



#Calcula la media del precio de cierre EURO
dfEuroMedia = dfEuro.agg(round_(avg('Close_Euro'),2).alias('media_close_euro'))
dfEuroMedia.show()
dfEuroMedia_partitioned = dfEuroMedia.coalesce(1)
dfEuroMedia_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-media-cierre-euro",header=True)



#Calcula el maximo y minimo de volumen y el dia el que se obtuvo cada uno en Euro
dfVolumenMaxEuro = dfEuro.agg(max('Volume').alias('Max_Volume_Euro'))
dfNetflixVolumenMaxEuro = dfEuro.join(dfVolumenMaxEuro, dfEuro['Volume'] == dfVolumenMaxEuro['Max_Volume_Euro'], 'inner').select('Date',col('Volume').alias('Max_Volume_Euro'))
dfNetflixVolumenMaxEuro.show()
dfNetflixVolumenMaxEuro_partitioned = dfNetflixVolumenMaxEuro.coalesce(1)
dfNetflixVolumenMaxEuro_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-max-volumen-euro",header=True)


dfVolumenMinEuro = dfEuro.agg(min('Volume').alias('Min_Volume_Euro'))
dfNetflixVolumenMinEuro = dfEuro.join(dfVolumenMinEuro, dfEuro['Volume'] == dfVolumenMinEuro['Min_Volume_Euro']  ,'inner').select('Date',col('Volume').alias('Min_Volume_Euro'))
dfNetflixVolumenMinEuro.show()
dfNetflixVolumenMinEuro_partitioned = dfNetflixVolumenMinEuro.coalesce(1)
dfNetflixVolumenMinEuro_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-min-volumen-euro",header=True)




#¿ Cuantos dias el precio de cierre fue mejor de 600$?, en Euro
dfNetflixCierre600Euro = dfEuro.filter(col('Close_Euro') >600)
dfNetflixCierre600Euro.show()
dfNetflixCierre600Euro_partitioned = dfNetflixCierre600Euro.repartition(3,'Date')
dfNetflixCierre600Euro_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-mejor-600-euro",header=True)



#¿ Qué porcentaje de dias el precio máximo fue mayor de 500$? , en Euro
diasHigh500Euro = dfEuro.count()
dfNetflixMaximoMayor500Euro = dfEuro.filter(col('High_Euro') >500).count()
porcentajeHigh500Euro =( float(dfNetflixMaximoMayor500Euro)/ float(diasHigh500Euro)) * 100
porcentajeHigh500Euro = round(porcentajeHigh500Euro,2)
dfPorcentajeEuro = spark.createDataFrame([(porcentajeHigh500Euro,)], ['porcentaje_dias_mayor_500_Euro'])
dfPorcentajeEuro.show()
dfPorcentajeEuro_partitioned = dfPorcentajeEuro.coalesce(1)
dfPorcentajeEuro_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-mayor-500-euro",header=True)




#Calcula el precio máximo por ano en Euro , puse el minimo pero no esta en la consigna
dfEuroAnoMaximo = dfEuro.groupBy(year(col('Date')).alias('Ano')) \
                        .agg(round_(max('High_Euro'),2).alias('Maximo_Precio_Euro')) \
                        .orderBy('Ano')
dfEuroAnoMaximo.show()
dfEuroAnoMaximo_partitioned = dfEuroAnoMaximo.coalesce(1)
dfEuroAnoMaximo_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-max-ano-euro",header=True)

dfEuroAnoMinimo = dfEuro.groupBy(year(col('Date')).alias('Ano')) \
                        .agg(round_(min('High_Euro'),2).alias('Minimo_Precio_Euro')) \
                        .orderBy('Ano')
dfEuroAnoMinimo.show()
dfEuroAnoMinimo_partitioned = dfEuroAnoMinimo.coalesce(1)
dfEuroAnoMinimo_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-min-ano-euro",header=True)



#Calcula el precio medio de cierre para cada mes en Euro
dfEuroPrecioCierreMes = dfEuro.groupBy(year(col('Date')).alias('Ano'), month(col('Date')).alias('Mes'))\
                                .agg(round_(avg('Close_Euro'),2).alias('media_close_Euro'))\
                                .orderBy('Ano','Mes')
dfEuroPrecioCierreMes.show()
dfEuroPrecioCierreMes_partitioned = dfEuroPrecioCierreMes.coalesce(3)
dfEuroPrecioCierreMes_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/netflix-medio-cierre-mes-euro",header=True)


#View
dfNetflixAccionesAlta.createOrReplaceTempView('dfNetflixAccionesAlta')
dfNetflixMedia.createOrReplaceTempView('dfNetflixMedia')
dfNetflixVolumenMax.createOrReplaceTempView('dfNetflixVolumenMax')
dfNetflixVolumenMin.createOrReplaceTempView('dfNetflixVolumenMin')
dfNetflixCierre600.createOrReplaceTempView('dfNetflixCierre600')
dfPorcentaje.createOrReplaceTempView('dfPorcentaje')
dfNetflixAnoMaximo.createOrReplaceTempView('dfNetflixAnoMaximo')
dfPrecioCierreMEs.createOrReplaceTempView('dfPrecioCierreMEs')
dfConversionEuroHigh.createOrReplaceTempView('dfConversionEuroHigh')
dfEuroMedia.createOrReplaceTempView('dfEuroMedia')
dfNetflixVolumenMaxEuro.createOrReplaceTempView('dfNetflixVolumenMaxEuro')
dfNetflixVolumenMinEuro.createOrReplaceTempView('dfNetflixVolumenMinEuro')
dfNetflixCierre600Euro.createOrReplaceTempView('dfNetflixCierre600Euro')
dfPorcentajeEuro.createOrReplaceTempView('dfPorcentajeEuro')
dfEuroAnoMaximo.createOrReplaceTempView('dfEuroAnoMaximo')
dfEuroAnoMinimo.createOrReplaceTempView('dfEuroAnoMinimo')
dfEuroPrecioCierreMes.createOrReplaceTempView('dfEuroPrecioCierreMes')



#Consultas a las view
df1 = spark.sql("SELECT * FROM dfEuroPrecioCierreMes ")
df2 = spark.sql("SELECT * FROM dfEuroAnoMinimo")
df3 = spark.sql("SELECT * FROM dfPorcentajeEuro")
df4 = spark.sql("SELECT * FROM dfNetflixVolumenMinEuro")


#Mostramos el resultado de las consultas
df1.show()
df2.show()
df3.show()
df4.show()




#Limpiamos el cache para liberar memoria
dfNetflix.unpersist()
dfNetflixConversion.unpersist()
dfEuro.unpersist()


#Por ultimo cerramos sesion en spark 
spark.stop()
