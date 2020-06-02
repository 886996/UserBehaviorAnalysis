package com.github

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (C), 1996-2020, 
  * FileName: LoginFailWithCep
  * Author:   yankun
  * Date:     2020/2/12 15:40
  * Description: ${DESCRIPTION}
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  */

//输入的登陆事件样例类
//case class LoginEvent(userId:Long,ip:String,evnetType:String,eventTime:Long)
//输出的异常报警信息样例类
//case class Warning(userId:Long,firstFailTime:Long,lastFailTime:Long,WarnMsg:String)

object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1.读取事件
        val resource = getClass.getResource("/LoginLog.csv")
        val loginEventStream = env.readTextFile(resource.getPath)
          .map(data => {
            val dataArray = data.split(",")
            LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
          })
          .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
            override def extractTimestamp(t: LoginEvent): Long = t.eventTime * 1000L
          })
          .keyBy(_.userId)

    //    //2.定义匹配模式
        val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.evnetType == "fail")
          .next("next").where(_.evnetType == "fail")
          .within(Time.seconds(3))

        //3.在事件流上添加应用，得到一个pattern stream
    val patternStream = CEP.pattern(loginEventStream,loginFailPattern)

    //从ppattern stream中应用sselect function检验匹配事件序列
    val loginFailDataStream = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print()
    env.execute("login fail with cep job")

  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent,Warning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    //从map中按照名称取出对应的事件
    val firstFail = map.get("begin").iterator().next()
    val lastFail = map.get("next").iterator().next()
    Warning(firstFail.userId,firstFail.eventTime,lastFail.eventTime,"login fail !")
//    val firstFail = map.get("begin").iterator().next()
//    val lastFail = map.get("next").iterator().next()
//    Warning( firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail!" )
  }
}
