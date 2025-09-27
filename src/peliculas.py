# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,col,round
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


#configuracion optimizada y sesion de spark
spark = SparkSession.builder\
        .appName('Netflix')\
        .config('spark.sql.adaptive.enabled','true') \
        .config('spark.sql.adaptive.coalescePartitions.enabled','true')\
        .getOrCreate()



spark.sparkContext.setLogLevel("WARN")


#Creacion de Esquemas
userSchema = StructType([
    StructField('idUser',IntegerType(),True),
    StructField('genero',StringType(),True),
    StructField('edad',IntegerType(),True),
    StructField('ocupacion',IntegerType(),True),
    StructField('codigoPostal',StringType(),True)
])

ratingsSchema = StructType([
    StructField('idUser_ratings',IntegerType(),True),
    StructField('idPelicula',IntegerType(),True),
    StructField('rating',IntegerType(),True),
    StructField('tp',IntegerType(),True)
])

moviesSchema = StructType([
    StructField('idMovie',IntegerType(),True),
    StructField('titulo',StringType(),True),
    StructField('genero',StringType(),True)
])

#Lectura de ficheros y mapeo con esquemas predefinidos
dfUser =spark.read.text("hdfs://namenode:9000/user/spark/input/users.dat")
dfRatings =spark.read.text("hdfs://namenode:9000/user/spark/input/ratings.dat")
dfMovies =spark.read.text("hdfs://namenode:9000/user/spark/input/movies.dat")


users = dfUser.rdd.map(lambda x: x[0].split('::')).map(lambda campo: (int(campo[0]),campo[1],int(campo[2]),int(campo[3]),campo[4])).toDF(schema=userSchema)
users.printSchema()


ratings = dfRatings.rdd.map(lambda x: x[0].split('::')).map(lambda campo: (int(campo[0]),int(campo[1]), int(campo[2]),int(campo[3]))).toDF(schema=ratingsSchema)
ratings.printSchema()


movies = dfMovies.rdd.map(lambda x: x[0].split('::')).map(lambda campo: (int(campo[0]),campo[1],campo[2])).toDF(schema=moviesSchema)
movies.printSchema()




#Cache de los dataFame para mejor performance
users.cache()
ratings.cache()
movies.cache()





#Calcula la nota media que ha realizado cada usuario
dfJoinUserRatingsID = users.join(ratings, users.idUser == ratings.idUser_ratings, 'inner')
dfmediaUserRatings = dfJoinUserRatingsID.groupBy('idUser').agg(round(avg('rating'),2).alias('media_rating'))
dfmediaUserRatings.show()
dfmediaUserRatings_partitioned = dfmediaUserRatings.repartition(3,'idUser')
dfmediaUserRatings_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/movies-media-usuario",header=True)



#¿Quien puntua mas alto ? ¿hombres o mujeres?
dfJoinUserRatingsGeneroID = users.join(ratings, users.idUser == ratings.idUser_ratings, 'inner')
dfmediaPorGenero = dfJoinUserRatingsGeneroID.groupBy('genero').agg(round(avg('rating'),2).alias('media_rating'))
dfmediaPorGenero.show()
dfmediaPorGenero_partitioned = dfmediaPorGenero.coalesce(1)
dfmediaPorGenero_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/movies-media-genero",header=True)

#Cuales son las peliculas con mayor puntuacion

dfJoinUserRatingsPeliculaID = movies.join(ratings, movies.idMovie == ratings.idPelicula, 'inner')
dfPeliculaMayorPuntuacion = dfJoinUserRatingsPeliculaID.groupBy('idMovie','titulo').agg(round(avg('rating'),2).alias('media_rating')).orderBy(col('media_rating').desc())
dfPeliculasRankingTop =dfPeliculaMayorPuntuacion.limit(10)
dfPeliculasRankingTop.show()
dfPeliculasRankingTop_partitioned = dfPeliculasRankingTop.coalesce(1)
dfPeliculasRankingTop_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/movies-mayor-puntuacion",header=True)





#View
dfPeliculasRankingTop.createOrReplaceTempView('dfPeliculasRankingTop')
dfmediaPorGenero.createOrReplaceTempView('dfmediaPorGenero')
dfmediaUserRatings.createOrReplaceTempView('dfmediaUserRatings')


#Consultas a las view
df1 = spark.sql("SELECT * FROM dfPeliculasRankingTop ")
df2 = spark.sql("SELECT * FROM dfmediaPorGenero")
df3 = spark.sql("SELECT * FROM dfmediaUserRatings where media_rating <4")

#Mostramos el resultado de las consultas
df1.show()
df2.show()
df3.show()




#Limpiamos el cache para liberar memoria
users.unpersist()
ratings.unpersist()
movies.unpersist()


#Por ultimo cerramos sesion en spark 
spark.stop()



