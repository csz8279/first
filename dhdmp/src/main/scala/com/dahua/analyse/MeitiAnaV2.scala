package com.dahua.analyse

import com.dahua.bean.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object MeitiAnaV2 {

  def main(args: Array[String]): Unit = {
    // 判断参数。
    if (args.length != 2) {
      println(
        """
          |com.dahua.analyse.ProCityCount
          |缺少参数
          |inputPath
          |appmapping
        """.stripMargin)
      sys.exit()
    }
    // 接收参数
    val Array(inputPath, appmapping) = args
    // 获取SparkSession
    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    val df: DataFrame = sc.textFile(inputPath).map(_.split(",", -1)).filter(_.length >= 85).map(Log(_)).toDF()

    val rdd: RDD[(String, String)] = sc.textFile(appmapping).map(x => {
      val fields: Array[String] = x.split(":", -1)
      (fields(0), fields(1))
    })
    val df1: DataFrame = rdd.toDF("appid", "appname")

    df.createTempView("appin")
    df1.createTempView("app")


    var sql =
    """
        |select
        |a.appid,
        |a.newname,
        |sum(case when requestmode =1 and processnode >=1 then 1 else 0 end )as ysqq,
        |sum(case when requestmode =1 and processnode >=2 then 1 else 0 end )as yxqq,
        |sum(case when requestmode =1 and processnode = 3 then 1 else 0 end )as ggqq,
        |sum(case when iseffective =1 and isbilling = 1 and isbid =1 and adorderid != 0 then 1 else 0 end )as jjx,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then 1 else 0 end )as jjcgs,
        |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end )as zss,
        |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end )as djs,
        |sum(case when requestmode =2 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjzss,
        |sum(case when requestmode =3 and iseffective =1 and isbilling = 1 then 1 else 0 end )as mjdjs,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (winprice*1.0)/1000 else 0 end )as xiaofei,
        |sum(case when iseffective =1 and isbilling = 1 and iswin =1  then (adpayment*1.0)/1000 else 0 end )as chengben
        |from
        |(select
        |l.*,
        |case when l.appid = z.appid then z.appname else l.appname end as newname
        |from addin l left join app z
        |on l.appid =z.appid) a
        |group by
        |a.appid,
        |a.newname
     """.stripMargin




    //    var sql1 = "select * from app"

    spark.sql(sql).show(10000)
    spark.stop()

  }
}
