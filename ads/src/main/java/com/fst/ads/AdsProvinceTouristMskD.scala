package com.fst.ads

import com.fst.common.SparkTool
import com.fst.grid.Geography
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 计算省游客
 * 1、停留时间大于3小时
 * 2、出游距离大于10KM
 */
object AdsProvinceTouristMskD extends SparkTool{
  override def run(spark: SparkSession): Unit = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //1、读取停留表
    val staypoint: DataFrame = spark
      .table(STAYPOINT_TABLE_NAME)
      .where($"day_id" === day_id)

    //2、读取用户画像表
    val usertag: DataFrame = spark
      .table(DIM_USERTAG_TABLE_NAME)
      .where($"month_id" === month_id)

    //3、读取行政区配置表
    val adminCode: DataFrame = spark
      .table(DIM_ADMINCODE_TABLE_NAME)

    // 计算两个网格距离的函数
    val calculateLength: UserDefinedFunction = udf((p1: String, p2: String) => {
      Geography.calculateLength(p1.toLong, p2.toLong)
    })

    staypoint
      //4、关联停留表和行政区配置表 获取省的编号
      .join(adminCode.hint("broadcast"),"county_id")
      //5、计算用户在一个省内的停留时间
      .withColumn("d_stay_time",sum($"duration") over Window.partitionBy($"mdn",$"prov_id"))
      //6、停留时间大于3小时
      .where($"d_stay_time" > 180)
      //7、关联用户画像表获取常住地
      .join(usertag.hint("broadcast"),"mdn")
      //8、计算每个停留点到常住的距离
      .withColumn("distance",calculateLength($"grid_id",$"resi_grid_id"))
      //9、获取一个人在一个省的最远距离
      .withColumn("d_max_distance",max($"distance") over Window.partitionBy($"mdn",$"prov_id"))
      //10、出游距离大于10KM
      .where($"d_max_distance" > 10000)
      //11、整理数据
      .select($"mdn",$"resi_county_id" as "source_county_id",$"prov_id" as "d_province_id",round($"d_stay_time"/60,4),round($"d_max_distance"/1000,4))
      //12、去除重复数据
      .distinct()
      //保存数据
      .write
      .format("csv")
      .option("sep","\t")
      .mode(SaveMode.Overwrite)
      .save(s"${ADS_PROVINCE_TOURIST_PATH}day_id=$day_id")

    //增加分区
    spark.sql(
      s"""
        |
        |alter table $ADS_PROVINCE_TOURIST_TABLE_NAME  add if not exists partition(day_id='$day_id')
        |""".stripMargin)
  }
}
