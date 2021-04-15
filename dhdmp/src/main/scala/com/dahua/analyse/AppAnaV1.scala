package com.dahua.analyse

import com.dahua.bean.Log
import com.dahua.util.AppAvaUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object AppAnaV1 {

  def main(args: Array[String]): Unit = {
    // 判断参数。
    if (args.length != 1) {
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
    val Array(inputPath) = args
    // 获取SparkSession
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[Log] = sc.textFile(inputPath).map(_.split(",",-1)).filter(_.length >= 85).map(Log(_))
    val value2: RDD[(String,String)] = rdd.map(line => {
      val imei: String = line.imei
      val mac: String = line.mac
      val idfa: String = line.idfa
      val openudid: String = line.openudid
      val androidid: String = line.androidid
      val adspacetype: Int = line.adspacetype

      val value: String = AppAvaUtil.appAna(imei, mac, idfa, openudid, androidid)

      val value1: String = AppAvaUtil.gglx(adspacetype)
      (value,value1)
    }).filter(x => !x._1.isEmpty)

    val value3: RDD[(String, String)] = value2.reduceByKey((x, y) => {
      var gSum = 0
      val str: String = x.substring(6)
      val str1: String = x.substring(0, 6)

      val str22: String = y.substring(6)
      val str2: String = y.substring(0, 6)

      if (str1.equals(str2)) {
        gSum = str.toInt + str22.toInt
      }
      (str1 + gSum)
    })

   value3.foreach(x =>{
     println(x._1+","+x._2)
   })
  }
}
