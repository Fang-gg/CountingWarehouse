package com.fst.common

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

abstract class SparkTool extends Logging with Constant {
  var day_id: String = _
  var month_id: String = _
  def main(args: Array[String]): Unit = {

    /**
     * 获取时间参数
     */
    if (args.length == 0) {
      // 使用Logging中打印日志的方法
      logWarning("请传入时间参数")
    }else{
      day_id = args(0)  // 需要我们自己传进来
      logInfo(s"时间参数:$day_id")
      // 月分区
      month_id=day_id.substring(0,6)
      logInfo("创建Spark环境")
    }


    /**
     * 构建spark环境
     */
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.replace("$", "")) //类名拿到的时候会多一个$,因此要替换掉
      .enableHiveSupport() // 开启Hive支持
      .getOrCreate()
    //调用子类的run方法
    run(spark)
  }

  def run(spark: SparkSession)
}