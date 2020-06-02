
import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (C), 1996-2020, 
  * FileName: OrderTimeout
  * Author:   yankun
  * Date:     2020/2/13 22:11
  * Description: ${DESCRIPTION}
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  */

//输入订单事件的样例类
case class OrderEvent(orderId:Long,eventType:String,txId:String,eventTime:Long)
//定义输出结果
case class OrderResult(orderId:Long,resultMsg:String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //  读取订单数据
    val resoure = getClass.getResource("./OrderLog.csv")

    val orderStream = env.readTextFile(resoure.getPath)
   // val orderStream = env.socketTextStream("nn1.hadoop",7777)
      .map(data =>{
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong,dataArray(1).trim,dataArray(2).trim,dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    //定义一个匹配模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //3.模式应用到stream，得到一个ppapattern stream
    val patternStream = CEP.pattern(orderStream,orderPayPattern)

    //调用select方法，提取事件序列，超时的事件要用报警提示
    val orderTimeOutputTag = new OutputTag[OrderResult]("orderTimeout")

    val resultStream = patternStream.select(orderTimeOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeOutputTag).print("TimeOut")

    env.execute("order timeout job")
  }
}

//自定义超时事件序列处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent,OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeOutOrderId = map.get("begin").iterator().next().orderId
    OrderResult(timeOutOrderId,"timeout")

  }
}

//自定义正常支付s事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = map.get("follow").iterator().next().orderId
    OrderResult(payedOrderId,"payed successfully")

  }
}