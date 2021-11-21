package com.fst.dim

import com.fst.common.SparkTool
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 处理用户画像表
 */

object DimUsertagMskM extends SparkTool {

  override def run(spark: SparkSession): Unit = {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    spark
      .table(ODS_USERTAG_TABLE_NAME)
      .where($"month_id" === month_id)
      .select(
        md5($"mdn") as "mdn",
        md5($"name") as "name",
        $"gender" as "gender",
        $"age" as "age",
        md5($"id_number") as "id_number",
        $"number_attr",
        $"trmnl_brand",
        $"trmnl_price",
        $"packg",
        $"conpot",
        $"resi_grid_id",
        $"resi_county_id"
      )
      .write
      .format("csv")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(s"${DIM_USERTAG_PATH}month_id=$month_id")

    // 增加分区
    spark.sql(
      s"""
        |
        |alter table $DIM_USERTAG_TABLE_NAME add if not exists partition(month_id='$month_id')
        |""".stripMargin)

  }
}
