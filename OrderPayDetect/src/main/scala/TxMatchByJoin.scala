
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Copyright (C), 1996-2020, 
  * FileName: TxMatchByJoin
  * Author:   yankun
  * Date:     2020/6/2 16:23
  * Description: ${DESCRIPTION}
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  */
object TxMatchByJoin {
  def main(args: Array[String]): Unit = {
 val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取数据
    val resource = getClass.getResource("./OrderLog.csv")
    val orderStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong,dataArray(1).trim,dataArray(2).trim,dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    //读取支付到账流
    val resource1 = getClass.getResource("./ReceiptLog.csv")
    val receiptStream = env.readTextFile(resource1.getPath)
      .map(data =>{
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0).trim,dataArray(1).trim,dataArray(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)


    //join处理
    val processedStream = orderStream.intervalJoin(receiptStream)
      .between(Time.seconds(-5),Time.seconds(5))
      .process(new TxPayMatchByJoin())

    processedStream.print()
    env.execute("tx pay match by join job")
  }
}

class TxPayMatchByJoin() extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
  override def processElement(in1: OrderEvent, in2: ReceiptEvent, context: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    collector.collect((in1,in2))
  }
}
