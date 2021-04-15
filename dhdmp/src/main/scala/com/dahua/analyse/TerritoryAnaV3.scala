package com.dahua.analyse

import com.dahua.bean.Log
import com.dahua.util.TerritoryTool
import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object TerritoryAnaV3 {


  def main(args: Array[String]): Unit = {
    // 判断参数。
    if (args.length != 1) {
      println(
        """
          |com.dahua.analyse.ProCityCount
          |缺少参数
          |inputPath
        """.stripMargin)
      sys.exit()
    }

    // 接收参数
    val Array(inputPath) = args
    // 获取SparkSession
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    import spark.implicits._

    // 关闭对象。
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[String] = sc.textFile(inputPath)
    val log: RDD[Log] = rdd.map(_.split(",", -1)).filter(_.length >= 85).map(Log(_))
    val rdd1: RDD[((String, String), List[Double])] = log.map(log => {
      val province: String = log.provincename
      val cityname: String = log.cityname
      val qqs: List[Double] = TerritoryTool.qqsRtp(log.requestmode, log.processnode)
      val jingjia: List[Double] = TerritoryTool.jingjiaRtp(log.iseffective, log.isbilling, log.isbid, log.iswin, log.adorderid)
      val ggzj: List[Double] = TerritoryTool.ggzjRtp(log.requestmode, log.iseffective)
      val mjj: List[Double] = TerritoryTool.mjjRtp(log.requestmode, log.iseffective, log.isbilling)
      val ggcb: List[Double] = TerritoryTool.ggcbRtp(log.iseffective, log.isbilling, log.iswin, log.winprice, log.adpayment)
      ((province, cityname), qqs ++ jingjia ++ ggzj ++ mjj ++ ggcb)
    })
    rdd1.rdd.reduceByKey((list1, list2) => {
      list1.zip(list2).map(t => t._1 + t._2)
    }).foreach(println(_))

    spark.stop()
    sc.stop()
  }

}
