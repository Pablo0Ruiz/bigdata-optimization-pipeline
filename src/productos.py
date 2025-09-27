# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc,sum,round
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

#configuracion optimizada y sesion de spark
spark = SparkSession.builder \
        .appName('Productos')\
        .config('spark.sql.adaptive.enabled','true') \
        .config('spark.sql.adaptive.coalescePartitions.enabled','true')\
        .getOrCreate()

spark.sparkContext.setLogLevel("WARN")



#Creacion de Esquemas
productosSchema = StructType([
    StructField('idProducto',IntegerType(),True),
    StructField('descripcion',StringType(),True),
    StructField('precio',FloatType(),True),
    StructField('stock',IntegerType(),True)

])
transactionsSchema = StructType([
    StructField('fecha',StringType(),True),
    StructField('hora',StringType(),True),
    StructField('idCliente',IntegerType(),True),
    StructField('producto',IntegerType(),True),
    StructField('numero_items',IntegerType(),True),
    StructField('dinero_total',FloatType(),True)

])



#Lectura de ficheros con esquemas predefinidos
productos = spark.read.csv(
    "hdfs://namenode:9000/user/spark/input/products.txt",
    sep="#",
    header=False,
    schema=productosSchema
)

transactions = spark.read.csv(
    "hdfs://namenode:9000/user/spark/input/transactions.txt",
    sep="#",
    header=False,
    schema=transactionsSchema
)

#Cache de los dataFame para mejor performance
productos.cache()
transactions.cache()


#Obtener el cliente que gasto mas dinero en total.
max_client = transactions.groupBy('idCliente').agg(round(sum('dinero_total'),2).alias('dinero_total_cliente')).orderBy(desc('dinero_total_cliente')).limit(1)
max_client.show()
max_client_partitioned = max_client.coalesce(1)
max_client_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/producto-max-client",header=True)



#Obten un informe de todos los productos vendidos
productosVendidos = transactions.groupBy('producto').agg(sum('numero_items').alias('items_vendidos'),round(sum('dinero_total'),2).alias('cantidad_total'))
productosDetalle = productos.join(productosVendidos, productos.idProducto == productosVendidos.producto, 'inner').select('idProducto','descripcion','items_vendidos','cantidad_total')
productosDetalle.show()
productosDetalle_partitioned = productosDetalle.repartition(4,'idProducto')
productosDetalle_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/productos-vendidos-informe",header=True)



#Lista los productos que nunca han sido vendidos
productosNoVendidos = productos.join(transactions, productos.idProducto == transactions.producto,'left_anti').select('idProducto','descripcion','precio')
productosNoVendidos.show()
productosNoVendidos_partitioned = productosNoVendidos.coalesce(2)
productosNoVendidos_partitioned.write.mode("overwrite").csv("hdfs://namenode:9000/user/spark/output/productos-no-vendidos",header=True)



#Obten las transacciones de los 10 productos con mayor stock
productosStock = productos.orderBy(desc('stock')).limit(10)
Top10Stock = transactions.join(productosStock, transactions.producto == productosStock.idProducto, 'inner').select('fecha','hora','idCliente','producto','numero_items','dinero_total')
Top10Stock.show()
Top10Stock_partitioned = Top10Stock.repartition(3,'producto')
Top10Stock_partitioned.write.mode('overwrite').csv("hdfs://namenode:9000/user/spark/output/productos-10-top-stock",header=True)


#View
max_client.createOrReplaceTempView('max_client')
productosDetalle.createOrReplaceTempView('productosDetalle')
productosNoVendidos.createOrReplaceTempView('productosNoVendidos')
Top10Stock.createOrReplaceTempView('Top10Stock')


#Consultas a las view
df1 = spark.sql("SELECT * FROM max_client ")
df2 = spark.sql("SELECT * FROM productosDetalle")
df3 = spark.sql("SELECT * FROM productosNoVendidos ")
df4 = spark.sql("SELECT * FROM Top10Stock ")

#Mostramos el resultado de las consultas
df1.show()
df2.show()
df3.show()
df4.show()




#Limpiamos el cache para liberar memoria
productos.unpersist()
transactions.unpersist()

#Por ultimo cerramos sesion en spark 
spark.stop()

