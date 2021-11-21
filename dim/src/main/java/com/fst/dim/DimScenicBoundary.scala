package com.fst.dim

import com.fst.common.SparkTool
import org.apache.spark.sql.SparkSession

/**
 * 景区管理表
 */
object DimScenicBoundary extends SparkTool{
  override def run(spark: SparkSession): Unit = {

    spark.sql(
      s"""
        |insert overwrite table $DIM_SCENIC_BOUNDARY_TABLE_NAME
        |select * from $ODS_SCENIC_BOUNDARY_TABLE_NAME
        |""".stripMargin)
  }
}
