# -*- coding: utf-8 -*-

from pyspark import SparkContext

sr = SparkContext()

#Leemos los datasets de productos y transacciones.
productos = sr.textFile("hdfs://namenode:9000/user/spark/input/products.txt")

transactions = sr.textFile("hdfs://namenode:9000/user/spark/input/transactions.txt")

#Obtener el cliente que gasto mas dinero en total.
max_client = transactions.map(lambda x: x.split('#')).map(lambda x: (x[2],float(x[5]))).reduceByKey(lambda a,b: a+b).max(lambda x: x[1])

print(max_client)

print('Cliente: ' + max_client[0] + ' gasto: ' + str(max_client[1]))


#Obten un informe de todos los productos vendidos
productosVendidos = transactions.map(lambda x: x.split('#')).map(lambda x: (x[3], float(x[4]))).reduceByKey(lambda a,b: a+b)
productosDetalle = productos.map(lambda x: x.split('#')).map(lambda x: (x[0],x[1]))

print('rdd2: ',productosDetalle.collect())


rddFinal = productosVendidos.join(productosDetalle).map(lambda x: (x[0],x[1][1],x[1][0]))

for producto in rddFinal.collect():
    print('producto: '+ producto[0] +' descripcion: '+ str(producto[1]) + ' vendidos: '+ str(producto[2]))




#Lista los productos que nunca han sido vendidos

productosNoVendidos = productos.map(lambda x: x.split('#')).map(lambda x: (x[0],(x[1],x[2])))
productosNoVendidosDetalle = transactions.map(lambda x: x.split('#')).map(lambda x: (x[3],float(x[4])))


rddNoVendidos = productosNoVendidos.leftOuterJoin(productosNoVendidosDetalle).filter(lambda x: x[1][1] is None).map(lambda x: (x[0],x[1][0][0],x[1][0][1]))

for noVendidos in rddNoVendidos.collect():
    print('producto no vendido: '+ noVendidos[0] +' descripcion: '+ str(noVendidos[1]) + ' precio: '+ str(noVendidos[2]))


#Obten las transacciones de los 10 productos con mayor stock
productosStock = productos.map(lambda x: x.split('#')).map(lambda x: (x[0],x[3]))

productosStock = productosStock.takeOrdered(10,key=lambda x: -float(x[1]))
productosStock2 = [prod[0] for prod in productosStock]

rdd10Stock = transactions.filter(lambda x: x.split('#')[3] in productosStock2)


for transaccion in rdd10Stock.collect():
    print('transaccion: '+ transaccion)

#Guardar resultado en un fichero en hdfs. 
sr.parallelize([max_client]).saveAsTextFile("hdfs://namenode:9000/user/spark/output/cliente-gasto-mas")

rddFinal.saveAsTextFile("hdfs://namenode:9000/user/spark/output/productos-vendidos")
rddNoVendidos.saveAsTextFile("hdfs://namenode:9000/user/spark/output/productos-no-vendidos")
rdd10Stock.saveAsTextFile("hdfs://namenode:9000/user/spark/output/productos-10-top-stock")






