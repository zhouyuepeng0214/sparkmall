package com.atguigu.bigdata.sparkmall.offline

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestHive {

  def main(args: Array[String]): Unit = {

    // TODO 0. 准备Spark上下文
    val conf: SparkConf = new SparkConf().setAppName("TestHive").setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    import spark.implicits._

    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",40)))

    val df: DataFrame = rdd.toDF("id","name","age")

    val databaseName = "spark0311"

    spark.sql("use " + databaseName)

    df.write.saveAsTable("user")

    spark.sql("select * from user").show()

    spark.stop()

  }

}
