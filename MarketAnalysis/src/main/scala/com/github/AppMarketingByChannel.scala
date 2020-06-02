package com.github

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * Copyright (C), 1996-2020, 
  * FileName: AppMarketing
  * Author:   yankun
  * Date:     2020/1/29 21:02
  * Description: ${DESCRIPTION}
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  */

case class MarketingUserBehavior(userId:String,behavior:String,channel:String,timestamp:Long)

//输出结果样例类
case class MarketingViewCount(windowStart:String,windowEnd:String,channel:String,behavior: String,count:Long)

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream = env.addSource(new SimulateEventSource())
        .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
        ((data.channel,data.behavior),1L)
      })
      .keyBy(_._1) //以渠道和行为类型作为key分组
      .timeWindow(Time.hours(1),Time.seconds(10))
      .process(new MarketingCountByChannel())

    dataStream.print("app market count")
    env.execute("app marketing by channel")
  }
}

class SimulateEventSource() extends RichSourceFunction[MarketingUserBehavior]{
  //定义是否运行的标志位，
  var running = true
  //定义用户行为的集合
  val behaviorTypes:Seq[String] = Seq("CLICK","INSTALL","DOWNLOAD","UNINSTALL")
  //定义渠道集合
  val channelSets:Seq[String] = Seq("wechat","weibo","appstore","xiaomistore")
  //定义一个随机数生成器
  val rand:Random = new Random()
  override def cancel(): Unit = running = false

  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    //d定义一个生成数据的上限
    val maxElement = Long.MaxValue
    var count = 0L

    //随机生成所有数据
    while(running && count < maxElement){
      val id = UUID.randomUUID().toString
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSets(rand.nextInt(channelSets.size))
      val ts = System.currentTimeMillis()
      sourceContext.collect(MarketingUserBehavior(id,behavior,channel,ts))

      count += 1
      TimeUnit.MILLISECONDS.sleep(10L)
    }

  }
}

class MarketingCountByChannel() extends ProcessWindowFunction[((String,String),Long),MarketingViewCount,(String,String),TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {
    val startTs = new Timestamp(context.window.getStart).toString
    val endTs = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size

    out.collect(MarketingViewCount(startTs,endTs,channel,behavior,count))

  }
}