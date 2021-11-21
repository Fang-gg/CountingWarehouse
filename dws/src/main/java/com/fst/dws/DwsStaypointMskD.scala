package com.fst.dws


import com.fst.common.{Config, SparkTool}
import com.fst.grid.Grid
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}

import java.awt.geom.Point2D

/**
 * 生成停留表
 */

object DwsStaypointMskD extends SparkTool {
  override def run(spark: SparkSession): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._




    val getLongi: UserDefinedFunction = udf((grid: String) => {
      // 通过网格编号获取网格中心点的经度
      val point: Point2D.Double = Grid.getCenter(grid.toLong)
      point.getX
    })

    val getLati: UserDefinedFunction = udf((grid: String) => {
      // 通过网格编号获取网格中心点的纬度
      val point: Point2D.Double = Grid.getCenter(grid.toLong)
      point.getY
    })

    // 用DSL写
    spark
      // 读取融合表
      .table(MERGELOCATION_TABLE_NAME)
      // 取出指定分区的数据
      .where($"day_id" === day_id)
      // 取出开始时间和结束时间
      .withColumn("start_date", split($"start_time", ",")(1))
      .withColumn("end_date", split($"start_time", ",")(0))
      // 按照手机号分组，按照时间排序，取 上一条数据的网格编号
      .withColumn("s_grid", lag($"grid_id", 1, "") over Window.partitionBy($"mdn").orderBy($"start_date"))
      // 判断当前网格编号和上一条数据的网格编号是否一致，划分边界
      .withColumn("flag", when($"s_grid" === $"grid_id", 0).otherwise(1))
      // 在同一个组的数据打上同样的标记
      .withColumn("class", sum($"flag") over Window.partitionBy($"mdn").orderBy("start_date"))
      // 将同一个人在同一个网格中的数据分到同一个组
      .groupBy($"mdn", $"county_id", $"grid_id", $"class")
      // 获取用户在网格中第一个点的时间和最后一个点的时间
      .agg(min($"start_date") as "grid_first_time", max($"end_date") as "grid_last_time")
      // 计算用户在网格中的停留时间
      .withColumn("duration", unix_timestamp($"grid_last_time", "yyyyMMddHHmmss") - unix_timestamp($"grid_first_time", "yyyyMMddHHmmss"))
      // 获取网格中心点的经纬度
      .withColumn("longi", getLongi($"grid_id"))
      .withColumn("lati", getLati($"grid_id"))

      // 整理数据
      .select($"mdn", round($"longi", 4) as "longi", round($"lati", 4) as "lati", $"grid_id", $"county_id", round($"duration" / 60, 4), $"grid_first_time", $"grid_last_time")

      // 保存数据
      .write
      .format("csv")
      .option("sep", "\t")
      .mode(SaveMode.Overwrite)
      .save(s"${STAYPOINT_PATH}day_id=" + day_id)

    // 增加分区
    spark.sql(s"alter table $STAYPOINT_TABLE_NAME add if not exists partition(day_id='$day_id') ")
  }
}

