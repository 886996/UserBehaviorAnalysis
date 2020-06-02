package com.github

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (C), 1996-2020, 
  * FileName: AppMarketing
  * Author:   yankun
  * Date:     2020/1/29 22:51
  * Description: ${DESCRIPTION}
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  */
object AppMarketing {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new SimulateEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
        ("dummyKey",1L)
      })
      .keyBy(_._1) //以渠道和行为类型作为key分组
      .timeWindow(Time.hours(1),Time.seconds(10))
      .aggregate(new CountAgg(),new MarketingCountTotal())

    dataStream.print("app market")

    env.execute("app market total")
  }
}

class CountAgg() extends AggregateFunction[(String,Long),Long,Long]{
  override def add(in: (String, Long), acc: Long): Long = acc + 1

  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class MarketingCountTotal() extends WindowFunction[Long,MarketingViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(window.getStart).toString
    val endTs = new Timestamp(window.getEnd).toString
    val count = input.iterator.next()

    out.collect(MarketingViewCount(startTs,endTs,"app marketing","total",count))
  }
}
