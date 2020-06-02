package com.github

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (C), 1996-2020, 
  * FileName: UniqueVistor
  * Author:   yankun
  * Date:     2020/1/30 17:01
  * Description: ${DESCRIPTION}
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  */

case class UvCount(windowEnd:Long,uvCount:Long)

object UniqueVisitor {
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
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())

    dataStream.print("user id ")

    env.execute("unique visitor count")
  }
}
class UvCountByWindow() extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //定义一个Scala Set 用于数据的user ID去重和保存
    var idSet = Set[Long]()
    //把当前窗口所有数据的ID收集到Set中，最后输出set的大小
    for(behavior <- input){
      idSet += behavior.userId
    }
    out.collect(UvCount(window.getEnd,idSet.size))

  }
}