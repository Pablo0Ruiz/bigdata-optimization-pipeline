# 🧠 Big Data Optimization Pipeline

Proyecto de procesamiento y optimización de datos sobre un entorno **Big Data distribuido** con **Apache Spark, Hive y HDFS**, ejecutado dentro de un ecosistema **Dockerizado** que emula un clúster Hadoop completo.

Este repo es una adaptación del entorno `docker-hadoop-spark` de Marcel-Jan, agregando mis propios scripts de procesamiento de datos.

---

## 🚀 Descripción general

El proyecto implementa un **pipeline de lectura, transformación y escritura optimizada de datos** usando PySpark, aplicando técnicas de optimización como:

- **Cache y particionado** de DataFrames para mejorar rendimiento.  
- **Consultas con Spark SQL y Hive** sobre vistas temporales.  
- **Reescritura de resultados en HDFS**, manteniendo distribución de datos.  
- **Uso de coalesce y repartition** para controlar paralelización y tamaño de los archivos de salida.

Es ideal para experimentar con Big Data, procesar grandes volúmenes de datos y aprender buenas prácticas de optimización en Spark.

---

## ⚙️ Estructura del proyecto

bigdata-optimization-pipeline/
├── LICENSE          # MIT para mis scripts nuevos
├── README.md        # Este README
├── src/             # Scripts PySpark creados por mí (MIT)
│   ├── hoteles.py
│   ├── hotelesCore.py
│   ├── netflix.py
│   ├── peliculas.py
│   ├── productos.py
│   └── productosCore.py
├── raw-data/        # Datos de entrada (.csv, .txt, .dat)
├── base/, conf/, datanode/, master/, namenode/, worker/, ... # Contenedores y scripts originales (Apache 2.0)
├── docker-compose.yml
├── Makefile
└── startup.sh / entrypoint.sh / otros scripts del repo original


---


## 🧩 Tecnologías utilizadas

| Componente | Descripción |
|------------|-------------|
| **Apache Spark / PySpark** | Procesamiento distribuido y consultas SQL |
| **HDFS (Hadoop Distributed File System)** | Almacenamiento distribuido de datos |
| **Hive** | Motor SQL sobre datos distribuidos |
| **Docker & Docker Compose** | Orquestación del entorno Hadoop |
| **Python** | Desarrollo de los jobs de procesamiento |
| **Makefile / Bash** | Automatización de tareas de inicio y ejecución |

---

## 🧰 Cómo ejecutar el proyecto

Asegúrate de subir los archivos de raw-data/ a HDFS y los scripts .py al nodo spark-master:/home.

docker compose exec spark-master /spark/bin/spark-submit /home/hoteles.py


Los resultados se generan en:

hdfs://namenode:9000/user/spark/output/

---

📜 Licencias

Los scripts y configuraciones originales provienen del repo docker-hadoop-spark
 y están bajo Apache License 2.0.

Mis scripts nuevos en src/ están bajo MIT License, incluido en el archivo LICENSE de este repositorio.

--- 

📊 Aprendizajes y buenas prácticas aplicadas

Configuración optimizada de Spark con Adaptive Query Execution (AQE).

Uso de cache selectivo y particionado eficiente.

Consultas SQL distribuidas con Hive.

Diseño modular de jobs PySpark reutilizables.

Orquestación de un entorno completo de Big Data con Docker.

---

👨‍💻 Autor

Kenyi Pablo Ruiz Quezada
📧 [kenyi.ruiz22@gmail.com](mailto:kenyi.ruiz22@gmail.com)  
🔗 [LinkedIn](https://rebrand.ly/kenyi-ruiz)
