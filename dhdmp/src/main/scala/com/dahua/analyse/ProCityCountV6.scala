package com.dahua.analyse

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityCountV6 {

  def main(args: Array[String]): Unit = {
    //判断参数
    if(args.length != 1){
      print(
        """
          |ProCityCount
          |缺少参数
          |inputPath
          |""".stripMargin)
      sys.exit()
    }

    //接受参数
    val Array(inputPath) = args

    //获取SparkSession sparkconf
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val df: DataFrame = spark.read.parquet(inputPath)
    //创建临时视图
    df.createTempView("log")
    //编写sql
    val  sql =
      """
        |select
        |provincename,cityname,
        |sum(case when requestmode = 1 and processnode >= 1 then 1 else 0 end) as ysqq,
        |sum(case when requestmode = 1 and processnode >= 2 then 1 else 0 end) as yxqq
        |from log
        |group by provincename,cityname
        |""".stripMargin

    //执行sql
    spark.sql(sql).show()
    //
    spark.stop()
    sc.stop()

  }

}