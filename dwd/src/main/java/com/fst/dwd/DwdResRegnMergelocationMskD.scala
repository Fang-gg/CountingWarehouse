package com.fst.dwd

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
         |insert overwrite table dwd.dwd_res_regn_mergelocation_msk_d partition(day_id=$day_id)
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
         |(select * from ods.ods_ddr where day_id = $day_id)
         |union all
         |(select * from ods.ods_dpi where day_id = $day_id)
         |union all
         |(select * from ods.ods_wcdr where day_id = $day_id)
         |union all
         |(select * from ods.ods_oidd where day_id = $day_id)
         |)
         |""".stripMargin)
  }
}
