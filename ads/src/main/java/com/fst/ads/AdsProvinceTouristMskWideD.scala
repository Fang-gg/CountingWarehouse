package com.fst.ads

import com.fst.common.SparkTool
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * 计算省游客宽表
 * 从省游客表、用户画像表，行政区配置表中
 * 提取出指标需要的所有字段构建成宽表
 */
object AdsProvinceTouristMskWideD extends SparkTool {
  override def run(spark: SparkSession): Unit = {

    import spark.implicits._
    import org.apache.spark.sql.functions._

    // 读取省游客表
    val province: DataFrame = spark.table(ADS_PROVINCE_TOURIST_TABLE_NAME).where($"day_id" === day_id)

    // 读取用户画像表
    val usertag: DataFrame = spark.table(DIM_USERTAG_TABLE_NAME).where($"month_id" === month_id)

    // 读取行政区配置表
    val admincode: DataFrame = spark.table(DIM_ADMINCODE_TABLE_NAME)

    // 年龄段
    val ages: Column = when($"age" > 0 and $"age" < 20, "(0,20)")
      .when($"age" >= 20 and $"age" < 25, "[20,25)")
      .when($"age" >= 25 and $"age" < 30, "[25,30)")
      .when($"age" >= 30 and $"age" < 35, "[30,35)")
      .when($"age" >= 35 and $"age" < 40, "[35,40)")
      .when($"age" >= 40 and $"age" < 45, "[40,45)")
      .when($"age" >= 45 and $"age" < 50, "[45,50)")
      .when($"age" >= 50 and $"age" < 55, "[50,55)")
      .when($"age" >= 55 and $"age" < 60, "[55,60)")
      .otherwise("[60,~)")


    //距离分段
    val d_distance_section: Column =
      when($"d_max_distance" >= 10 and $"d_max_distance" < 50, "[10,50)")
        .when($"d_max_distance" >= 50 and $"d_max_distance" < 80, "[50,80)")
        .when($"d_max_distance" >= 80 and $"d_max_distance" < 120, "[80,120)")
        .when($"d_max_distance" >= 120 and $"d_max_distance" < 200, "[120,200)")
        .when($"d_max_distance" >= 200 and $"d_max_distance" < 400, "[200,400)")
        .when($"d_max_distance" >= 400 and $"d_max_distance" < 800, "[400,800)")
        .otherwise("[800,~)")


    //停留时间分段
    val d_stay_time: Column = when($"d_stay_time" >= 3 and $"d_stay_time" < 6, "[3,6)")
      .when($"d_stay_time" >= 6 and $"d_stay_time" < 9, "[6,9)")
      .when($"d_stay_time" >= 9 and $"d_stay_time" < 12, "[6,12)")
      .when($"d_stay_time" >= 12 and $"d_stay_time" < 15, "[12,15)")
      .when($"d_stay_time" >= 15 and $"d_stay_time" < 18, "[15,18)")
      .when($"d_stay_time" >= 18 and $"d_stay_time" < 24, "[18,24)")
      .otherwise("[24,~)")

    // 对多次使用的rdd进行缓存
    admincode.cache()

    // 取出省编号和省名称，去重
    val proIdAndName: Dataset[Row] = admincode.select($"prov_id" as "d_province_id", $"prov_name" as "d_province_name").distinct()

    // 关联用户画像表
    province
      .join(usertag.hint("broadcast"),"mdn")
      // 对年龄进行分段
      .withColumn("age",ages)
      // 对出游距离进行分段
      .withColumn("d_distance_section",d_distance_section)
      // 停留时间分段
      .withColumn("d_stay_time",d_stay_time)
      // 关联行政分区表获取省名
      .join(proIdAndName.hint("broadcast"),"d_province_id")
      // 通过来源地区县关联行政区配置表获取来源的省和市
      .join(admincode.hint("broadcast"),$"source_county_id"===$"county_id")
      // 整理数据
      .select(
        $"mdn",
        $"d_province_name",
        $"city_name" as "o_city_name",
        $"prov_name" as "o_province_name",
        $"number_attr",
        $"d_distance_section",
        $"d_stay_time" ,
        $"gender",
        $"trmnl_brand",
        $"packg" as "pckg_price",
        $"conpot",
        $"age"
      )

     // 保存数据
      .write
      .format("csv")
      .option("sep","\t")
      .mode(SaveMode.Overwrite)
      .save(s"${ADS_PROVINCE_WIDE_PATH}day_id=$day_id")

    // 增加分区
    spark.sql(
      s"""
        |
        |alter table $ADS_PROVINCE_WIDE_TABLE_NAME add if not exists partition(day_id='$day_id')
        |""".stripMargin)
  }
}
