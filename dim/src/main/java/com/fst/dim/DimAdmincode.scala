package com.fst.dim

import com.fst.common.SparkTool
import org.apache.spark.sql.SparkSession

/**
 * 行政管理表
 */
object DimAdmincode extends SparkTool{
  override def run(spark: SparkSession): Unit = {

    spark.sql(
        s"""
          |
          |insert overwrite table $DIM_ADMINCODE_TABLE_NAME
          |select * from $ODS_ADMINCODE_TABLE_NAME
          |
          |""".stripMargin)
  }
}
