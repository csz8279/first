package com.dahua.analyse

import com.dahua.util.IfUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util
import scala.collection.JavaConversions.mapAsScalaMap

object ProCityCountV5 {

  def main(args: Array[String]): Unit = {

    // 判断参数。
    if (args.length != 2) {
      println(
        """
          |com.dahua.analyse.ProCityCount
          |缺少参数
          |inputPath
          |outputPath
          """.stripMargin)
      sys.exit()
    }

    // 接收参数
    val Array(inputPath,outputPath) = args
    // 获取SparkSession
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val rdd: RDD[String] = sc.textFile(inputPath)
    val rdd1: RDD[Array[String]] = rdd.map(line => line.split(",", -1)).filter(arr => arr.length >= 85)
    val rdd2: RDD[((String, String), Int)] = rdd1.map(field => {
      val pro = field(24)
      val city = field(25)
      ((pro, city), 1)
    })
    val rdd3: RDD[((String, String), Int)] = rdd2.reduceByKey(_ + _)
    val rdd4: RDD[((String, String), Int)] = rdd3.sortBy(_._2,false,1)

    val pro: Array[String] = rdd4.map(_._1._1).distinct().collect()
    val rdd5: RDD[(String, (String, Int))] = rdd4.map(x => (x._1._1, (x._1._2, x._2)))
    val rdd6: RDD[(String, (String, Int))] = rdd5.partitionBy(new PCPartition(pro))

    IfUtil.exists(sc,outputPath)

    rdd6.saveAsTextFile(outputPath)
//
//    rdd4.foreach(x =>{
//      println(x._1._1+"\t"+x._1._2+"\t"+x._2)
//    })

    spark.stop()
    sc.stop()

  }
  class PCPartition(args: Array[String]) extends Partitioner {
    private val partitionMap: util.HashMap[String, Int] = new util.HashMap[String, Int]()
    var parId = 0
    for (arg <- args) {
      if (!partitionMap.contains(arg)) {
        partitionMap(arg) = parId
        parId += 1
      }
    }

    override def numPartitions: Int = partitionMap.valuesIterator.length

    override def getPartition(key: Any): Int = {
      val keys: String = key.asInstanceOf[String]
      val sub = keys
      partitionMap(sub)
    }
  }
}
