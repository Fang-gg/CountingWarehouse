package com.fst.common

import java.io.InputStream
import java.util.Properties

object Config {

  //加载config.properties配置文件
  //使用类加载器从resources获取一个文件输入流
  val inputStream: InputStream = this
    .getClass
    .getClassLoader
    .getResourceAsStream("config.properties")


  //使用properties读取配置文件

  private val properties: Properties = new Properties()

  properties.load(inputStream)

  def get(key:String):String = {
    properties.getProperty(key)
  }
}
