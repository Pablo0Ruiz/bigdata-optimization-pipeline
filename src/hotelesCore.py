# -*- coding: utf-8 -*-


from pyspark import SparkContext

sr = SparkContext()

#Leemos los datasets de hotelesEuropa y ingresos.

hoteles = sr.textFile("hdfs://namenode:9000/user/spark/input/hoteleseuropa.csv").filter(lambda l: not l.startswith("id;"))
ingresos = sr.textFile("hdfs://namenode:9000/user/spark/input/ingresos.csv").filter(lambda l: not l.startswith("id;"))

#Filtrar los hoteles que pertenezcan a Espana y obtener los ingresos de dichos hoteles

espana = hoteles.map(lambda x: x.split(';')).filter(lambda x: x[5].strip().lower() == 'es').map(lambda x: (x[0],(x[1],x[4])))
espanaIngresos = ingresos.map(lambda x: x.split(';')).filter(lambda x: len(x) > 1 and x[1].strip() != "").map(lambda x: (x[0],int(x[1])))

rddEspana = espana.join(espanaIngresos).map(lambda x: (x[1][0][0],x[1][0][1],x[1][1]))

for hotel in rddEspana.collect():
    nombre = hotel[0].encode('utf-8')
    ciudad = hotel[1].encode('utf-8')
    print('hotel: '+nombre + ' ciudad: '+ ciudad + ' ingresos: '+ str(hotel[2]))

#Obtener por pantalla los 100 hoteles con mas ingresos

hotelesDetalle = hoteles.map(lambda x: x.split(';')).map(lambda x: (x[0],(x[1],x[4])))
hotelesIngresos = ingresos.map(lambda x: x.split(';')).filter(lambda x: len(x) > 1 and x[1].strip() != "").map(lambda x: (x[0],int(x[1])))
rdd100 = hotelesDetalle.join(hotelesIngresos)

hoteles100 = rdd100.takeOrdered(100,key=lambda x: -float(x[1][1]))

for h100 in hoteles100:
    nombre = h100[1][0][0].encode('utf-8')
    ciudad = h100[1][0][1].encode('utf-8')
    print('hotel: '+nombre + ' ciudad: '+ ciudad + ' ingresos: '+ str(h100[1][1]))


#Obtener por pantalla las 200 ciudades que más ingresos obtuvieron

ciudadesDetalle = hoteles.map(lambda x: x.split(';')).map(lambda x: (x[0],x[1]))
ciudadesIngresos = ingresos.map(lambda x: x.split(';')).filter(lambda x: len(x) > 1 and x[1].strip() != "").map(lambda x: (x[0],int(x[1])))

rdd200 = ciudadesDetalle.join(ciudadesIngresos).map(lambda x: (x[1][0],x[1][1])).reduceByKey(lambda a,b: a+b)
ciudades200 = rdd200.takeOrdered(200,key=lambda x: -float(x[1]))

for c200 in ciudades200:
    ciudad = c200[0].encode('utf-8')
    print('ciudad: '+ciudad + ' ingresos: '+ str(c200[1]))





#Selecciona los hoteles que no tuvieran ingresos

hotelesSinIngresosDetalles = hoteles.map(lambda x: x.split(';')).map(lambda x: (x[0],(x[1],x[4])))
hotelesSinIngresos = ingresos.map(lambda x: x.split(';')).map(lambda x: (x[0], int(x[1]) if len(x) > 1 and x[1].strip() != "" else None))

rddSinIngresos = hotelesSinIngresosDetalles.leftOuterJoin(hotelesSinIngresos)
rddSinIngresos = rddSinIngresos.filter(lambda x: x[1][1] is None)

for h100 in rddSinIngresos.collect():
    nombre = h100[1][0][0].encode('utf-8')
    ciudad = h100[1][0][1].encode('utf-8')
    print('hotel: '+nombre + ' ciudad: '+ ciudad + ' ingresos: 0')



