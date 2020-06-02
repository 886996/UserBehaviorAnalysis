package com.github

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (C), 1996-2020, 
  * FileName: LoginFail
  * Author:   yankun
  * Date:     2020/2/10 0:40
  * Description: ${DESCRIPTION}
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  */

//输入的登陆事件样例类
case class LoginEvent(userId:Long,ip:String,evnetType:String,eventTime:Long)
//输出的异常报警信息样例类
case class Warning(userId:Long,firstFailTime:Long,lastFailTime:Long,WarnMsg:String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(t: LoginEvent): Long = t.eventTime * 1000L
      })

    val warningStream = loginEventStream.keyBy(_.userId) //用户id做分组
      .process(new LoginWarning(2))

    warningStream.print()

    env.execute("Login fail massage")
  }
}

class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long,LoginEvent,Warning]{
  //定义状态，保存两秒钟所有失败的事件
  lazy val loginFailState :ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login fail state",classOf[LoginEvent]))
  //
  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {

//    //定义一个loginList
//    val loginFailList = loginFailState.get()
//    //判断类型是否fail，添加fail 的事件到状态
//    if(i.evnetType == "fail"){
//      if(loginFailList.iterator().hasNext){
//        context.timerService().registerEventTimeTimer(i.eventTime * 1000L + 2000L)
//      }
//      loginFailState.add(i)
//    }else{
//      //如果成功，直接清空状态
//      loginFailState.clear()
//    }

    if(i.evnetType == "fail"){
      //失败，判断之前是否登陆失败
      val iter = loginFailState.get().iterator()
      if(iter.hasNext){
        //如果已经又登陆失败事件，就比较事件时间是否在两秒之内
        val firstFail = iter.next()
        if(i.eventTime < firstFail.eventTime + 2){
          //如果两次间隔小于两秒，输出报警
          collector.collect(Warning(i.userId,firstFail.eventTime,i.eventTime,"login fail in 2 seconds "))
        }
        //更新最后一次登陆失败事件，保存在状态里
        loginFailState.clear()
        loginFailState.add(i)
      }else{
        //如果是第一次登陆失败，直接添加到状态
        loginFailState.add(i)
      }
    }else{
      //如果成功，清空状态
      loginFailState.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    //触发定时器的时候，根据状态里的失败个数决定是否输出报警
    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    val iter = loginFailState.get().iterator()
    while(iter.hasNext){
      allLoginFails += iter.next()
    }

    //判断个数
    if(allLoginFails.length >= maxFailTimes){
      out.collect(Warning(allLoginFails.head.userId,allLoginFails.head.eventTime,allLoginFails.last.eventTime,"login fail in 2 seconds for " + allLoginFails.length + "times"))
    }
    //如果成功，清空状态
    loginFailState.clear()
  }

}