import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark._

object queryPerformanceAnalytics extends App {

  Logger.getLogger("org.apache.spark.streaming._").setLevel(Level.DEBUG)

  private val conf = new SparkConf()
    .setAppName("queryPerformanceAnalytics")
    .setMaster("local[2]")
    .set("es.nodes.wan.only", "true")

  val sc = new SparkContext(conf)
  val ss = SparkSession.builder().appName("queryPerformanceAnalytics").config(conf).getOrCreate()
  val sqlContext: SQLContext = ss.sqlContext

  /*
    Métricas tomadas utilizando los listeners de Spark:
    Al lanzar un job de Spark se muestran mensajes adicionales de tipo WARN
    emitidos por el CustomLister con las métricas de ejecución de Spark
  */
  import org.apache.spark.scheduler._
  import org.apache.log4j.LogManager
  val logger = LogManager.getLogger("CustomListener")

  class CustomListener extends SparkListener  {
    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      logger.warn(s"Stage completed, runTime: ${stageCompleted.stageInfo.taskMetrics.executorRunTime}, " +
        s"cpuTime: ${stageCompleted.stageInfo.taskMetrics.executorCpuTime}")}}

  val myListener=new CustomListener
  sc.addSparkListener(myListener)

  // ---------------------------------------------------
  // --------------------   HDFS   ---------------------
  // ---------------------------------------------------

  // Usando DataFrames:
  val df_hdfs =  sqlContext.read.json("hdfs://utad:8020/SparkStreaming/part-00000*")
  df_hdfs.createOrReplaceTempView("df_hdfs")

  // query_1: "Duración media del trayecto por tipo de usuario y género"
  val hdfs_query_1 = ss.sql("SELECT usertype as usertype_hdfs, " +
                                   "gender, " +
                                   "avg(tripduration) as avg_tripduration " +
                            "FROM df_hdfs " +
                            "GROUP BY usertype, gender")
  ss.time(hdfs_query_1.show(hdfs_query_1.count.toInt,false))
    /*
      Resultados:
      - Time taken: 3927 ms
      ---------------------
      - WARN CustomListener: Stage completed, runTime: 242, cpuTime: 169594715
      - WARN CustomListener: Stage completed, runTime: 327, cpuTime: 214288699
      - WARN CustomListener: Stage completed, runTime: 40, cpuTime: 25652784
      - WARN CustomListener: Stage completed, runTime: 52, cpuTime: 14832345
      - WARN CustomListener: Stage completed, runTime: 278, cpuTime: 95834865
      - WARN CustomListener: Stage completed, runTime: 834, cpuTime: 313907020
      - WARN CustomListener: Stage completed, runTime: 647, cpuTime: 210497161
     */

  // query_2: "Número de trayectos realizados por edad de los usuarios"
  val hdfs_query_2 = ss.sql("SELECT birth_year as birth_year_hdfs, " +
                                   "count(birth_year) as num_trips " +
                            "FROM df_hdfs " +
                            "GROUP BY birth_year")
  ss.time(hdfs_query_2.show(hdfs_query_2.count.toInt,false))
    /*
      Resultados:
      - Time taken: 3514 ms
      ---------------------
      - WARN CustomListener: Stage completed, runTime: 207, cpuTime: 143209452
      - WARN CustomListener: Stage completed, runTime: 190, cpuTime: 132561707
      - WARN CustomListener: Stage completed, runTime: 53, cpuTime: 30863644
      - WARN CustomListener: Stage completed, runTime: 32, cpuTime: 12805318
      - WARN CustomListener: Stage completed, runTime: 359, cpuTime: 88705720
      - WARN CustomListener: Stage completed, runTime: 922, cpuTime: 309384105
      - WARN CustomListener: Stage completed, runTime: 535, cpuTime: 200808960
  */

  // query_3: "Número de trayectos que realiza cada bicicleta"
  val hdfs_query_3 = ss.sql("SELECT bikeid as bikeid_hdfs, " +
                                   "count(bikeid) as num_trips " +
                            "FROM df_hdfs " +
                            "GROUP BY bikeid")
  ss.time(hdfs_query_3.show(hdfs_query_3.count.toInt,false))
    /*
      Resultados:
      - Time taken: 3742 ms
      ---------------------
      - WARN CustomListener: Stage completed, runTime: 257, cpuTime: 160742493
      - WARN CustomListener: Stage completed, runTime: 230, cpuTime: 135527036
      - WARN CustomListener: Stage completed, runTime: 41, cpuTime: 31303885
      - WARN CustomListener: Stage completed, runTime: 54, cpuTime: 18018342
      - WARN CustomListener: Stage completed, runTime: 240, cpuTime: 85811277
      - WARN CustomListener: Stage completed, runTime: 1179, cpuTime: 314920430
      - WARN CustomListener: Stage completed, runTime: 526, cpuTime: 205375253
    */


  // Sin usar DataFrames (consulta directa a HDFS):
  /*
  // query_1
  val hdfs_query_1 = ss.sql("select usertype as usertype_hdfs, gender, avg(tripduration) as avg_tripduration from json. `hdfs://utad:8020/SparkStreaming/part-00000*` group by usertype, gender")
  ss.time(hdfs_query_1.show())

  // query_2
  val hdfs_query_2 = ss.sql("select birth_year as birth_year_hdfs, count(birth_year) as num_trips from json.`hdfs://utad:8020/SparkStreaming/part-00000*` group by birth_year")
  ss.time(hdfs_query_2.show())

  // query_3
  val hdfs_query_3 = ss.sql("select bikeid as bikeid_hdfs, count(bikeid) as num_trips from json.`hdfs://utad:8020/SparkStreaming/part-00000*` group by bikeid")
  ss.time(hdfs_query_3.show())
  */

  // ---------------------------------------------------
  // ----------------   ElasticSearch   ----------------
  // ---------------------------------------------------

  // DataFrame + vista temporal con la información agregada de los trayectos
  val df_es_1 = sqlContext
    .read
    .format("org.elasticsearch.spark.sql")
    .option("es.port","9200")
    .option("es.nodes", "localhost")
    .load("spark-streaming-trips/data")
  df_es_1.createOrReplaceTempView("df_es_1")

  // DataFrame + vista temporal con la información agregada de los tipos de usuario
  val df_es_2 = sqlContext
    .read
    .format("org.elasticsearch.spark.sql")
    .option("es.port","9200")
    .option("es.nodes", "localhost")
    .load("spark-streaming-users/data")
  df_es_2.createOrReplaceTempView("df_es_2")

  // DataFrame + vista temporal con la información agregada de las bicicletas
  val df_es_3 = sqlContext
    .read
    .format("org.elasticsearch.spark.sql")
    .option("es.port","9200")
    .option("es.nodes", "localhost")
    .load("spark-streaming-bikes/data")
  df_es_3.createOrReplaceTempView("df_es_3")

  // query_1: "Duración media del trayecto por tipo de usuario y género"
  val df_es_query_1 = ss.sql("SELECT usertype as usertype_es, " +
                                    "gender, " +
                                    "avg(agg_tripduration) as avg_tripduration " +
                             "FROM df_es_1 " +
                             "GROUP BY usertype, gender")
  ss.time(df_es_query_1.show(df_es_query_1.count.toInt,false))
    /*
      Resultados:
      - Time taken: 4768 ms
      ---------------------
      - WARN CustomListener: Stage completed, runTime: 944, cpuTime: 261916127
      - WARN CustomListener: Stage completed, runTime: 57, cpuTime: 28616472
      - WARN CustomListener: Stage completed, runTime: 33, cpuTime: 15113607
      - WARN CustomListener: Stage completed, runTime: 237, cpuTime: 92074621
      - WARN CustomListener: Stage completed, runTime: 955, cpuTime: 329991629
      - WARN CustomListener: Stage completed, runTime: 510, cpuTime: 181558436
    */

  // query_2: "Número de trayectos realizados por edad de los usuarios"
  val df_es_query_2 = ss.sql("SELECT birth_year as birth_year_es, " +
                                    "sum(count) as num_trips " +
                             "FROM df_es_2 " +
                             "GROUP BY birth_year")
  ss.time(df_es_query_2.show(df_es_query_2.count.toInt,false))
    /*
      Resultados:
      - Time taken: 5502 ms
      ---------------------
      - WARN CustomListener: Stage completed, runTime: 1033, cpuTime: 294741153
      - WARN CustomListener: Stage completed, runTime: 149, cpuTime: 34506539
      - WARN CustomListener: Stage completed, runTime: 68, cpuTime: 16705404
      - WARN CustomListener: Stage completed, runTime: 313, cpuTime: 84942320
      - WARN CustomListener: Stage completed, runTime: 1147, cpuTime: 327436134
      - WARN CustomListener: Stage completed, runTime: 534, cpuTime: 186005078
    */

  // query_3: "Número de trayectos que realiza cada bicicleta"
  val df_es_query_3 = ss.sql("SELECT bikeid as bikeid_es, " +
                                    "sum(count) as num_trips " +
                             "FROM df_es_3 " +
                              "GROUP BY bikeid")
  ss.time(df_es_query_3.show(df_es_query_3.count.toInt,false))
    /*
      Resultados:
      - Time taken: 5049 ms
      ---------------------
      - WARN CustomListener: Stage completed, runTime: 896, cpuTime: 281176201
      - WARN CustomListener: Stage completed, runTime: 23, cpuTime: 20791020
      - WARN CustomListener: Stage completed, runTime: 117, cpuTime: 20513432
      - WARN CustomListener: Stage completed, runTime: 336, cpuTime: 93375817
      - WARN CustomListener: Stage completed, runTime: 975, cpuTime: 324721870
      - WARN CustomListener: Stage completed, runTime: 669, cpuTime: 209124674
    */

  // Métricas tomadas utilizando la librería sparkmeasure
  // (partiendo de la consulta sql plana, sin uso de tablas)

  /*
  val es_stageMetrics_q1 = ch.cern.sparkmeasure.StageMetrics(ss)
  es_stageMetrics_q1.runAndMeasure(ss.sql("select usertype, gender, avg(tripduration) from json. `hdfs://utad:8020/SparkStreaming/part-00000*` group by usertype, gender"))

  val es_stageMetrics_q2 = ch.cern.sparkmeasure.StageMetrics(ss)
  es_stageMetrics_q2.runAndMeasure(ss.sql("select birth_year, count(birth_year) from json.`hdfs://utad:8020/SparkStreaming/part-00000*` group by birth_year"))

  val es_stageMetrics_q3 = ch.cern.sparkmeasure.StageMetrics(ss)
  es_stageMetrics_q3.runAndMeasure(ss.sql("select bikeid, count(bikeid) from json.`hdfs://utad:8020/SparkStreaming/part-00000*` group by bikeid"))
  */

  sc.stop()

}