package com.dahua.bz2parquet

import com.dahua.bean.Log
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}




object Bz2ParquetV2 {

  def main(args: Array[String]): Unit = {
    /*
     定义参数。
     loginputPath
     logoutputPath
    */
    if (args.length != 2) {
      println(
        """
          |com.dahua.bz2parquet.Bz2Parquet
          |缺少参数
          |loginputPath
          |logoutputPath
        """.stripMargin)
      sys.exit()
    }
    //接受参数
    val Array(longinputPath,longoutputPath) = args
    //需要两个对象
    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //将自定义类注册kryo序列化
    conf.registerKryoClasses(Array(classOf[Log]))

    val spark = SparkSession.builder().config(conf).appName(this.getClass.getSimpleName).master("local[*]").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //读取数据源
    val log: RDD[String] = sc.textFile(longinputPath)
    //对数据进行ETL
    val arr: RDD[Array[String]] = log.map(_.split(",", -1))
    val filter: RDD[Array[String]] = arr.filter(_.length >= 85)
    val logBean: RDD[Log] = filter.map(Log(_))
    val df: DataFrame = spark.createDataFrame(logBean)

    //写出到parquet
    df.write.parquet(longoutputPath)

    spark.stop()
    sc.stop()
  }
}
