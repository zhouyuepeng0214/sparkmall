package com.atguigu.bigdata.sparkmall.offline

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.UUID

import com.atguigu.sparkmall.common.model.{CategoryTop10, CategoryTop10SessionTop10, UserVisitAction}
import com.atguigu.sparkmall.common.util.StringUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

object Req2CategoryTop10SessionTop10 {

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
    rdd.count.cache
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

    //result.foreach(println)

    // *************************************************需求二 start********************************************************

    val mapRDD: RDD[UserVisitAction] = rdd.map {
      line => {
        val datas: Array[String] = line.split(",")
        UserVisitAction(
          datas(0),
          datas(1),
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12)
        )
      }
    }

    val ids: List[String] = result.map(line => {
      line.categoryId
    })

    //todo 广播变量
    val broadcastIds: Broadcast[List[String]] = sc.broadcast(ids)

    val filterRDD: RDD[UserVisitAction] = mapRDD.filter {
      line => {
        if (line.click_category_id == -1) {
          false
        } else {
          broadcastIds.value.contains(line.click_category_id.toString)
          /*
          var flg = false
          Breaks.breakable {
            for (top10 <- result) {
              if (line.click_category_id == top10.categoryId) {
                flg = true
                Breaks.break()
              }
            }
          }
          flg
          */
        }
      }
    }

    // todo 数据结构转换 （category-session，1)
    val mapRDD1: RDD[(String, Long)] = filterRDD.map(line => {
      (line.click_category_id + "_" + line.session_id, 1L)
    })

    //todo 4.聚合统计 ：（category-session，1) => （category-session，sum)
    val reduceRDD: RDD[(String, Long)] = mapRDD1.reduceByKey(_ + _)


    //todo 5.聚合统计 ：（category-session，sum) => （category,(session，sum))
    val mapRDD2: RDD[(String, (String, Long))] = reduceRDD.map {
      case (key, v) => {
        val keys: Array[String] = key.split("_")
        (keys(0), (keys(1), v))
      }
    }

    // todo 6. (category,Iterator[(session，sum)])
    val groupRDD: RDD[(String, Iterable[(String, Long)])] = mapRDD2.groupByKey()
    groupRDD.foreach(println)

    //todo 7. 降序取前10
    val resultRDD: RDD[(String, List[(String, Long)])] = groupRDD.mapValues(line => {
      line.toList.sortWith {
        (left, right) => {
          left._2 > right._2
        }
      }.take(10)
    })

    resultRDD.foreach(println)

    // todo 8. 将结果转换为样例类
    val resultMapRDD: RDD[List[CategoryTop10SessionTop10]] = resultRDD.map {
      case (category, list) => {
        list.map {
          case (sessionid, sum) => {
            CategoryTop10SessionTop10(taskId, category, sessionid, sum)
          }
        }
      }
    }
    val realResultRDD: RDD[CategoryTop10SessionTop10] = resultMapRDD.flatMap(list => list)

    //realResultRDD.foreach(println)

    println(realResultRDD.count())

    // *************************************************需求二 end********************************************************


    //TODO 8. 将结果保存到Mysql中

    //todo 错误 序列化
    /*
        val driver = "com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://hadoop102:3306/exer"
        val userName = "root"
        val passWd = "123456"

        Class.forName(driver)
        val connection: Connection = DriverManager.getConnection(url, userName, passWd)
        val sql = "insert into category_top10_session_count ( taskId, categoryId,sessionId,clickCount) values (?, ?, ?, ?)"
        val statement: PreparedStatement = connection.prepareStatement(sql)

        realResultRDD.foreach{
          obj=>{
            statement.setObject(1, obj.taskId)
            statement.setObject(2, obj.categoryId)
            statement.setObject(3, obj.sessionId)
            statement.setObject(4, obj.clickCount)
            statement.executeUpdate()
          }
        }
        statement.close()
        connection.close()
        */
    //todo 效率低，每次都要建立连接
    /*
        realResultRDD.foreach{
          obj=>{
            val driver = "com.mysql.jdbc.Driver"
            val url = "jdbc:mysql://hadoop102:3306/exer"
            val userName = "root"
            val passWd = "123456"

            Class.forName(driver)
            val connection: Connection = DriverManager.getConnection(url, userName, passWd)
            val sql = "insert into category_top10_session_count ( taskId, categoryId,sessionId,clickCount) values (?, ?, ?, ?)"
            val statement: PreparedStatement = connection.prepareStatement(sql)

            statement.setObject(1, obj.taskId)
            statement.setObject(2, obj.categoryId)
            statement.setObject(3, obj.sessionId)
            statement.setObject(4, obj.clickCount)
            statement.executeUpdate()

            statement.close()
            connection.close()
          }
        }
        */

    //todo 效率高
/*
    realResultRDD.foreachPartition(datas => {
      val driver = "com.mysql.jdbc.Driver"
      val url = "jdbc:mysql://hadoop102:3306/exer"
      val userName = "root"
      val passWd = "123456"

      Class.forName(driver)
      val connection: Connection = DriverManager.getConnection(url, userName, passWd)
      val sql = "insert into category_top10_session_count ( taskId, categoryId,sessionId,clickCount) values (?, ?, ?, ?)"
      val statement: PreparedStatement = connection.prepareStatement(sql)

      datas.foreach {
        obj => {
          statement.setObject(1, obj.taskId)
          statement.setObject(2, obj.categoryId)
          statement.setObject(3, obj.sessionId)
          statement.setObject(4, obj.clickCount)
          statement.executeUpdate()
        }
      }
      statement.close()
      connection.close()
    })
    */
    // TODO 9. 释放资源
    sc.stop()

  }
}

