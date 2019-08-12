package com.atguigu.bigdata.sparkmall.offline

import java.text.SimpleDateFormat

import com.atguigu.sparkmall.common.model.UserVisitAction
import com.atguigu.sparkmall.common.util.DateUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Req4PageAvgAccessTimeApplication {
  def main(args: Array[String]): Unit = {

    /*
    使用Spark RDD 统计电商平台每个页面用户的平均停留时间。
     */


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

    //todo 结构转换 （sesson，（pageid，actiontime））

    val rdd1: RDD[(String, (Long, String))] = actionRDD.map(action => {
      (action.session_id, (action.page_id, action.action_time))
    })

    //todo 按session分组（sesson，iterator【（pageid，actiontime）】）
    val groupRDD: RDD[(String, Iterable[(Long, String)])] = rdd1.groupByKey()

    //todo 升序排序

    val mapValRDD: RDD[(String, List[(Long, Long)])] = groupRDD.mapValues(datas => {
      val sortList: List[(Long, String)] = datas.toList.sortWith {
        (left, right) => {
          left._2 < right._2
        }
      }
      //todo 拉链  （（pageid1，actiontime1），（pageid2，actiontime2））
      val zipList: List[((Long, String), (Long, String))] = sortList.zip(sortList.tail)

      // todo 结构转换 （（pageid1，（actiontime1-actiontime2））
      zipList.map {
        case (action1, action2) => {
          val time1: Long = DateUtil.parseStringToTimestap(action1._2)
          val time2: Long = DateUtil.parseStringToTimestap(action2._2)
          (action1._1, time2 - time1)
        }
      }
    })

    val mapRDD: RDD[List[(Long, Long)]] = mapValRDD.map(_._2)

    val flatMapRDD: RDD[(Long, Long)] = mapRDD.flatMap(list => list)

    // todo 结构转换 （（pageid1，iter[（actiontimex）]）
    val groupRDD1: RDD[(Long, Iterable[Long])] = flatMapRDD.groupByKey()

    groupRDD1.foreach{
      case (pageid,timeList) => {
        println("页面" + pageid + "平均停留时间为：" + (timeList.sum / timeList.size))
      }
    }


    // TODO 9. 释放资源
    sc.stop()


  }

}
