package com.fst.dwd

import com.fst.common.{Config, SparkTool}
import org.apache.spark.sql.SparkSession

/**
 * 生成位置融合表
 */
object DwdResRegnMergelocationMskD extends SparkTool {
  override def run(spark: SparkSession): Unit = {

    // union all不会去重，因此用union all
    spark.sql(
      s"""
         |
         |insert overwrite table $MERGELOCATION_TABLE_NAME partition(day_id=$day_id)
         |
         |select
         | md5(mdn) as mdn,
         | start_time,
         | county_id,
         | longi,
         | lati,
         | bsid,
         | grid_id,
         | biz_type,
         | event_type,
         | data_source
         |
         |from (
         |(select * from $DDR_TABLE_NAME where day_id = $day_id)
         |union all
         |(select * from $DPI_TABLE_NAME where day_id = $day_id)
         |union all
         |(select * from $WCDR_TABLE_NAME where day_id = $day_id)
         |union all
         |(select * from $OIDD_TABLE_NAME where day_id = $day_id)
         |)
         |""".stripMargin)
  }
}
