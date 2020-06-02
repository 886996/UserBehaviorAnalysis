package com.github

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (C), 1996-2020, 
  * FileName: AdStatisticsByGeo
  * Author:   yankun
  * Date:     2020/1/28 23:09
  * Description: ${DESCRIPTION}
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  */

//输入的广告点击事件样例类
case class  AdClickEvent(userId:Long,adId:Long,province:String,city:String,timestamp:Long)

//根据省份统计输出结果样例类
case class CountByProvice(windowEnd:String,provice:String,count:Long)

//输出黑名单报警信息
case class BlackListWarning(userId:Long,adId:Long,msg:String)

object AdStatisticsByGeo {

  //定义侧输出流的标签
  val blackListOutputTag :OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取数据
    val resource = getClass.getResource("/AdClickLog.csv")
    val adEventStream = env.readTextFile(resource.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).trim.toLong,dataArray(1).trim.toLong,dataArray(2).trim,dataArray(3).trim,dataArray(4).trim.toLong)
       })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //过滤大量刷单的点击量，zi自定义process function
    val fliterBlackListStream = adEventStream.keyBy(data => (data.userId, data.adId))
      .process(new FilterBlackListUser(100))


    //根据省份做分组，开窗聚合
    val adCountStream = fliterBlackListStream.keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())

    fliterBlackListStream.getSideOutput(blackListOutputTag).print("blacklist")
    adCountStream.print()
    env.execute("ads statistic by geo")
  }
  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent]{
    //定义状态，保存当前用户对当前 广告的点击量
    lazy val countState :ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state",classOf[Long]))
    //保存是否发送黑名单的状态
    lazy val isSendBlackList :ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state",classOf[Boolean]))
    //保存定时器发生的时间
    lazy val resetTimer:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resettime-state",classOf[Long]))

    override def processElement(i: AdClickEvent, context: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, collector: Collector[AdClickEvent]): Unit = {
      //取出count状态
      val curCount =countState.value()

      //如果第一次处理,注册定时器,每天零点触发
      if(curCount == 0){
        val ts = (context.timerService().currentProcessingTime()/(1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
        resetTimer.update(ts)
        context.timerService().registerProcessingTimeTimer(ts)
      }
      //判断是否达到上限，如果达到则加入黑名单
      if(curCount >= maxCount){
        //判断是否发送过黑名单，只发送一次
        if(! isSendBlackList.value()){
          isSendBlackList.update(true)
          //输出到侧输出流
          context.output(blackListOutputTag,BlackListWarning(i.userId,i.adId,"Click over" + maxCount + "time today "))
        }
        return
      }
      //计数器状态加一，输出到主流
      countState.update(curCount + 1)
      collector.collect(i)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      //定时器触发，清空状态
      if(timestamp == resetTimer.value()){
        isSendBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }

    }
  }
}

class AdCountAgg() extends AggregateFunction[AdClickEvent,Long,Long]{
  override def add(in: AdClickEvent, acc: Long): Long = acc + 1

  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class AdCountResult() extends WindowFunction[Long,CountByProvice,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvice]): Unit = {
    out.collect(CountByProvice(new Timestamp(window.getEnd).toString,key,input.iterator.next()))
  }
}

