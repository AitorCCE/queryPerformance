# queryPerformance

Este repositorio forma parte de un TFM en el que se ha creado  una  aplicación  con  Spark  Streaming  que  recibe  datos  a  través  de Apache  Kafka  y, en  base  a  una  configuración, genera  cubos  OLAP  y  los persiste  en ElasticSearch. Por otro lado, los datos "en bruto", seon persistidos en HDFS con formato JSON. Todo ello con el objetivo de acelerar consultas a través del cálculo al vuelo de datos en streaming antes de ser persistidos.
Concretamente realiza mediante Apache Spark operaciones sobre el conjunto de datos almacenado para la extracción de métricas.

## Tecnologías  utilizadas:
*	Apache Spark.
*	Apache Kafka.
*	Apache Hadoop (HDFS).
*	Elastic Search.
*	Scala.
*	JSON.

## Origen de los datos:
Dataset con datos sobre el uso del servicio CitiBike de bicicletas en Nueva York (https://www.citibikenyc.com/system-data)
