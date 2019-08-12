package com.atguigu.sparkmall.common.model

case class UserVisitAction(date: String,
                           user_id: String,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id:String
                          )
case class CategoryTop10 ( taskId:String, categoryId:String, clickCount:Long, orderCount:Long, payCount:Long )

case class CategoryTop10SessionTop10(taskId : String,categoryId : String,sessionId : String,clickCount : Long)