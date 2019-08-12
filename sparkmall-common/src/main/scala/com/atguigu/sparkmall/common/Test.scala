package com.atguigu.sparkmall.common

object Test {

  def main(args: Array[String]): Unit = {
    val s = "1,,,2"

    val strings: Array[String] = s.split(",")

    println("!"+strings(1)+"!")
    println("!"+strings(2)+"!")

  }

}
