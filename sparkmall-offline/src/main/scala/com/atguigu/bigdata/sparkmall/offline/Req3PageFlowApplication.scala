package com.atguigu.bigdata.sparkmall.offline

import com.atguigu.sparkmall.common.model.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req3PageFlowApplication {
  def main(args: Array[String]): Unit = {

    // todo 创建Spak上下文环境并读取数据

    // TODO 0. 准备Spark上下文
    val conf: SparkConf = new SparkConf().setAppName("Req1CategoryTop10Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // TODO 1. 获取原始的用户点击数据
    val rdd: RDD[String] = sc.textFile("input/user_visit_action.csv")

    val actionRDD: RDD[UserVisitAction] = rdd.map {
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
    actionRDD.cache()

    // todo 2. 分母数据

    // todo 2.1 数据过滤，保留需要数据
    val pageids = List(1,2,3,4,5,6,7)

    val zipPageids: List[String] = pageids.zip(pageids.tail).map {
      case (p1, p2) => {
        p1 + "-" + p2
      }
    }

    val filterRDD: RDD[UserVisitAction] = actionRDD.filter(action => {
      pageids.contains(action.page_id.toInt)
    })

    // todo 2.2 数据结构转换 ：（pageid，1）
    val pageIdToOneRDD: RDD[(Long, Long)] = filterRDD.map {
      line => {
        (line.page_id, 1L)
      }
    }

    // todo 2.3 数据聚合统计 ：（pageid，1） => pageid，sum）
    val pageIdeToSumRDD: RDD[(Long, Long)] = pageIdToOneRDD.reduceByKey(_+_)
    val pageIdToSums: Map[Long, Long] = pageIdeToSumRDD.collect().toMap

    // todo 3 分子数据

    // todo 3.1 根据session分组 （session，Iterator[UserVisitAction]）
    val sessionGroupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(line => line.session_id)

    // todo 3.2 升序排序 （session，List[pageid1,pageid2]）
    val sessionToZipRDD: RDD[(String, List[(String, Long)])] = sessionGroupRDD.mapValues {
      datas => {
        val actions: List[UserVisitAction] = datas.toList.sortWith {
          (left, right) => {
            left.action_time < right.action_time
          }
        }
        val ids: List[Long] = actions.map(line => line.page_id)
        // todo 3.3 拉链效果 （1-2，1） （2-3，1）
        val zipList: List[(Long, Long)] = ids.zip(ids.tail)
        val tuples: List[(String, Long)] = zipList.map {
          case (p1, p2) => {
            (p1 + "-" + p2, 1L)
          }
        }
        tuples
      }
    }
    val zipRDD: RDD[(String, Long)] = sessionToZipRDD.map(line => line._2).flatMap(list => list)

    // todo 3.4 过滤
    val zipFilterRDD: RDD[(String, Long)] = zipRDD.filter {
      case (pageflow, v) => {
        zipPageids.contains(pageflow)
      }
    }

    // todo 3.5  （pageid1,pageid2，sum)
    val pageFlowReduceRDD: RDD[(String, Long)] = zipFilterRDD.reduceByKey(_+_)

    // todo 4 分子数据/分母数据
    pageFlowReduceRDD.foreach{
      case (pageflow,sum) => {
        val p1: String = pageflow.split("-")(0)
        println(pageflow + "=" + sum.toDouble / pageIdToSums.getOrElse(p1.toLong,1L))
      }
    }

    // TODO 9. 释放资源
    sc.stop()


  }

}
