package com.fst.common

/**
 *
 * 放常量 读取配置文件
 *
 */
abstract trait Constant {

  val ODS_USERTAG_TABLE_NAME: String = Config.get("ods.usertag.table.name")
  val DIM_USERTAG_PATH: String = Config.get("dim.usertag.path")
  val DIM_USERTAG_TABLE_NAME: String = Config.get("dim.usertag.table.name")
  val MERGELOCATION_TABLE_NAME: String = Config.get("mergelocation.table.name")
  val DDR_TABLE_NAME: String = Config.get("ddr.table.name")
  val DPI_TABLE_NAME: String = Config.get("dpi.table.name")
  val WCDR_TABLE_NAME: String = Config.get("wcdr.table.name")
  val OIDD_TABLE_NAME: String = Config.get("oidd.table.name")
  val STAYPOINT_PATH: String = Config.get("staypoint.path")
  val STAYPOINT_TABLE_NAME: String = Config.get("staypoint.table.name")
  val ODS_ADMINCODE_TABLE_NAME: String = Config.get("ods.admincode.table.name")
  val DIM_ADMINCODE_TABLE_NAME: String = Config.get("dim.admincode.table.name")
  val ODS_SCENIC_BOUNDARY_TABLE_NAME: String = Config.get("ods.scenic.boundary.table.name")
  val DIM_SCENIC_BOUNDARY_TABLE_NAME: String = Config.get("dim.scenic.boundary.table.name")
  val ADS_PROVINCE_TOURIST_PATH: String = Config.get("ads.province.tourist.path")
  val ADS_PROVINCE_TOURIST_TABLE_NAME: String = Config.get("ads.province.tourist.table.name")

}
