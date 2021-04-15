package com.dahua.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

object IfUtil {


  def exists(sc:SparkContext,outputPath:String) ={
    val config: Configuration = sc.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(config)
    val path = new Path(outputPath)
    if(fs.exists(path)){
      fs.delete(path,true)
    }
  }
}
