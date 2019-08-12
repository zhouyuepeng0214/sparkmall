package com.atguigu.bigdata.sparkmall.offline

import java.util.UUID

import com.atguigu.sparkmall.common.model.CategoryTop10
import com.atguigu.sparkmall.common.util.StringUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

object Req1CategoryTop10Application1 {

  def main(args: Array[String]): Unit = {

    // 需求一 ： 获取点击、下单和支付数量排名前 10 的品类

    // TODO 0. 准备Spark上下文
    val conf: SparkConf = new SparkConf().setAppName("Req1CategoryTop10Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 1. 获取原始的用户点击数据
    val rdd: RDD[String] = sc.textFile("input/user_visit_action.csv")

    // TODO 2. 将数据转换结构：( catgory-click, 1 )

    // 创建累加器
    val accumulator = new CategoryAccumulator
    // 注册累加器
    sc.register(accumulator, "accumulator")

    // todo RDD如果没有执行行动算子，那么累加器会出现少加的情况
    /*
    rdd = lineDataRDD.map{
      line => {
          accumulator.add
      }
    }
    accumulator.value
     */

    // todo RDD如果执行多次行动算子，那么累加器会出现多加的情况
    /*
    rdd.count.cache    // todo 加入缓存使下一行的foreach执行算子不再累加
    rdd.foreach{

    }
    accumulator.value
    */

    rdd.foreach {
      line => {
        val datas: Array[String] = line.split(",")
        if (datas(6) != "-1") {
          accumulator.add(datas(6) + "_click")
        } else if (StringUtil.isNotEmpty(datas(8))) {

          val categoryIds: Array[String] = datas(8).split("-")

          categoryIds.map {
            id => {
              accumulator.add(id + "_order")
            }
          }
        } else if (StringUtil.isNotEmpty(datas(10))) {
          val categoryIds: Array[String] = datas(10).split("-")
          categoryIds.map {
            id => {
              accumulator.add(id + "_pay")
            }
          }
        }
      }
    }
    val accumulatorVal: mutable.HashMap[String, Long] = accumulator.value
    val categroupToMap: Map[String, mutable.HashMap[String, Long]] = accumulatorVal.groupBy {
      case (k, sum) => {
        k.split("_")(0)
      }
    }

    val taskId: String = UUID.randomUUID().toString
    val top10: immutable.Iterable[CategoryTop10] = categroupToMap.map {
      case (category, map) => {
        CategoryTop10(taskId,
          category,
          map.getOrElse(category + "_click", 0L),
          map.getOrElse(category + "_order", 0L),
          map.getOrElse(category + "_pay", 0L))
      }
    }

    // TODO 7. 将转换后的数据进行排序(降序)，取前10名
    val result: List[CategoryTop10] = top10.toList.sortWith {
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    }.take(10)

    result.foreach(println)


    // TODO 8. 将结果保存到Mysql中
    //    val driver = "com.mysql.jdbc.Driver"
    //    val url = "jdbc:mysql://hadoop102:3306/exer"
    //    val userName = "root"
    //    val passWd = "123456"
    //
    //    Class.forName(driver)
    //    val connection: Connection = DriverManager.getConnection(url, userName, passWd)
    //    val sql = "insert into category_top10 ( taskId, category_id, click_count, order_count, pay_count ) values (?, ?, ?, ?, ?)"
    //    val statement: PreparedStatement = connection.prepareStatement(sql)
    //
    //    top10Array.foreach{
    //      obj=>{
    //        statement.setObject(1, obj.taskId)
    //        statement.setObject(2, obj.categoryId)
    //        statement.setObject(3, obj.clickCount)
    //        statement.setObject(4, obj.orderCount)
    //        statement.setObject(5, obj.payCount)
    //        statement.executeUpdate()
    //      }
    //    }
    //    statement.close()
    //    connection.close()

    // TODO 9. 释放资源
    sc.stop()

  }
}

//自定义累加器：品类数据统计

class CategoryAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  private var map = new mutable.HashMap[String, Long]()

  override def isZero: Boolean = {
    map.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryAccumulator
  }

  override def reset(): Unit = {
    map.clear()
  }

  override def add(in: String): Unit = {
    map(in) = map.getOrElse(in, 0L) + 1
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    var map1 = map
    var map2 = other.value
    map = map1.foldLeft(map2) {
      (tempMap, kv) => {

        val k: String = kv._1
        val v: Long = kv._2

        tempMap(k) = tempMap.getOrElse(k, 0L) + v

        tempMap
      }
    }


  }

  override def value: mutable.HashMap[String, Long] = {
    map
  }
}

