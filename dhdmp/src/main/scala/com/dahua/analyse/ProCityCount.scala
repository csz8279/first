package com.dahua.analyse

import com.dahua.util.IfUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityCount {

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
    val Array(inputPath, outputPath) = args
    // 获取SparkSession
    val spark = SparkSession.builder().appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()

    val sc: SparkContext = spark.sparkContext

    // 读取数据源
    val df: DataFrame = spark.read.parquet(inputPath)
    // 创建临时视图
    df.createTempView("log")
    // 编写sql语句
    val sql = "select provincename,cityname,count(*) from log  group by provincename,cityname";
    val procityCount: DataFrame = spark.sql(sql)
    // 输出到json目录。
    // 判断输出路径是否存在。如果存在，就删除
    IfUtil.exists(sc,outputPath)
    procityCount.write.partitionBy("provincename","cityname").json(outputPath)
    // 关闭对象。
    spark.stop()
    sc.stop()

  }

}
