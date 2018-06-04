package utl

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkBase(propMap: Map[String, String], sparkMap: Map[String, String]) {

  def InitSpark(msg: String):SparkSession= {

    val sparkConf = new SparkConf()
    sparkMap foreach { x=> sparkConf.set(x._1,x._2) } // set Spark parameters
    sparkConf.setMaster(if (propMap("master").isEmpty) "local[*]" else propMap("master")) // set Master
    sparkConf.setAppName(msg)

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    spark.sparkContext.setLogLevel(if (propMap("loglevel").isEmpty) "INFO" else propMap("loglevel"))
    spark.sparkContext.hadoopConfiguration.set("fs.igfs.impl", propMap("fs.igfs.impl"))
    spark.sparkContext.hadoopConfiguration.set("fs.hdfs.impl", propMap("fs.hdfs.impl"))
    spark.sparkContext.hadoopConfiguration.set("fs.file.impl", propMap("fs.file.impl"))
    spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", propMap("fs.defaultFS"))

    spark
  }
}
