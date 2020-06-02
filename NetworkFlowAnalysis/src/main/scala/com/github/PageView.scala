package com.github

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (C), 1996-2020, 
  * FileName: PageView
  * Author:   yankun
  * Date:     2020/1/30 15:44
  * Description: ${DESCRIPTION}
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  */

//定义输入数据样例类
case class UserBehavior(userId:Long,itemId:Long,catagoryId:Int,behavior: String,timestamp:Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //val streamData = env.readTextFile("D:\\classhome\\Flink\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //用相对路径传入数据源、
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim.toInt,dataArray(3).trim.toString,dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv") //只统计pv操作
      .map(data => ("pv",1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)

    dataStream.print("pv count")

    env.execute("page view count")
  }
}
