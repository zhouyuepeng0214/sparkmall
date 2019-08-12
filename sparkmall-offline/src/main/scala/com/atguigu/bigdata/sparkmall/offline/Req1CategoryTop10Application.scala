package com.atguigu.bigdata.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.sparkmall.common.model.CategoryTop10
import com.atguigu.sparkmall.common.util.StringUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req1CategoryTop10Application {

  def main(args: Array[String]): Unit = {

    // 需求一 ： 获取点击、下单和支付数量排名前 10 的品类

    // TODO 0. 准备Spark上下文
    val conf: SparkConf = new SparkConf().setAppName("Req1CategoryTop10Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 1. 获取原始的用户点击数据
    val rdd: RDD[String] = sc.textFile("input/user_visit_action.csv")

    // TODO 2. 将数据转换结构：( catgory-click, 1 )
    val mapRDD: RDD[Array[(String, Long)]] = rdd.map(line => {
      val datas: Array[String] = line.split(",")
      if (datas(6) != "-1") {
        Array((datas(6) + "_click", 1L))
      } else if (StringUtil.isNotEmpty(datas(8))) {

        val categoryIds: Array[String] = datas(8).split("-")

        categoryIds.map {
          id => (id + "_order", 1L)
        }
      } else if (StringUtil.isNotEmpty(datas(10))) {
        val categoryIds: Array[String] = datas(10).split("-")
        categoryIds.map {
          id => (id + "_pay", 1L)
        }
      } else {
        Array(("", 0L))
      }
    })
    val flatMapRDD: RDD[(String, Long)] = mapRDD.flatMap(a=>a)

    val filterRDD: RDD[(String, Long)] = flatMapRDD.filter {
      case (key, v) => {
        StringUtil.isNotEmpty(key)
      }
    }

    // TODO 3. 将转换结构后的数据进行分组聚合:( catgory-click, 1 ) => ( catgory-click, sum)

    val reduceRDD: RDD[(String, Long)] = filterRDD.reduceByKey(_+_)

    // TODO 4. 将聚合后的结果进行结构的转换 ： ( catgory-click, sum) ==> (category, (click. sum))

    val mapRDD1: RDD[(String, (String, Long))] = reduceRDD.map {
      case (key, sum) => {
        val keys: Array[String] = key.split("_")
        (keys(0), (keys(1), sum))
      }
    }

    // TODO 5. 将转换后的结构进行分组：(category, Iterator[(click, sum)])
    val groupRDD: RDD[(String, Iterable[(String, Long)])] = mapRDD1.groupByKey()

    // TODO 6. 将分组后的数据转换为样例类：(category, Iterator[(click, sum)]) => UserVisitAction
    val taskId: String = UUID.randomUUID().toString

    val classRDD: RDD[CategoryTop10] = groupRDD.map {
      case (categoryId, iter) => {
        val map: Map[String, Long] = iter.toMap
        CategoryTop10(taskId, categoryId, map.getOrElse("click", 0L), map.getOrElse("order", 0L),
          map.getOrElse("pay", 0L))
      }
    }
    // TODO 7. 将转换后的数据进行排序(降序)，取前10名
    val collectArray: Array[CategoryTop10] = classRDD.collect()

    val top10Array: Array[CategoryTop10] = collectArray.sortWith {
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
    top10Array.foreach(println)

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
