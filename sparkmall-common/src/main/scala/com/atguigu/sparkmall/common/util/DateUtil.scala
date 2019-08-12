package com.atguigu.sparkmall.common.util

import java.text.SimpleDateFormat
import java.util.Date

object DateUtil {

  def parseStringToDate(s : String,f: String = "yyyy-MM-dd HH:mm:ss") : Date = {

    val format = new SimpleDateFormat(f)

    format.parse(s)

  }

  def parseStringToTimestap(s : String,f : String = "yyyy-MM-dd HH:mm:ss"): Long = {
    parseStringToDate(s,f).getTime
  }

}
