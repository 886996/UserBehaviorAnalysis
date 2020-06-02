import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * Copyright (C), 1996-2020, 
  * FileName: TxMatchDetect
  * Author:   yankun
  * Date:     2020/2/18 8:05
  * Description: ${DESCRIPTION}
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  */

//定义接受数据流的样例类
case class ReceiptEvent( txId:String,payChannel:String,eventTime:Long)

object TxMatchDetect {
  //定义侧数据流
  val unmatchedPay = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")
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

    //将两条流连接起来，共同处理
    val processedStream = orderStream.connect(receiptStream)
      .process( new TxPayMatch())

    processedStream.print("matched")
    processedStream.getSideOutput(unmatchedPay).print("unmatchedpay")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts")
    env.execute("tx match job")

  }
  class TxPayMatch() extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
    //定义状态保存已经到达的支付订单和事件订单
    lazy val payState : ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay state ",classOf[OrderEvent]))
    lazy val receiptState : ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt state",classOf[ReceiptEvent]))
    //订单支付事件数据处理
    override def processElement1(pay: OrderEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //判断有没有对应的到账事件
      val receipt = receiptState.value()
      if(receipt != null){
        //如果已经有receipt，在主流中输出匹配信息
        collector.collect((pay,receipt))
        receiptState.clear()
      }else{
        //如果还没有到，需要把pay存入状态，注册定时器等待
        payState.update(pay)
        context.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)
      }

    }

    //到账事件处理
    override def processElement2(receipt: ReceiptEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //同样的处理流程
      val pay = payState.value()
      if(pay != null){
        collector.collect((pay,receipt))
        payState.clear()
      }else{
        receiptState.update(receipt)
        context.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //到时间了，如果还没有收到某个事件，输出报警
      if(payState.value() != null){
        //recipt没来,输出pay到输出流
        ctx.output(unmatchedPay,payState.value())
      }
      if(receiptState.value() != null){
        //说明pay没来，
        ctx.output(unmatchedReceipts,receiptState.value())
      }
      payState.clear()
      receiptState.clear()

    }
  }
}
