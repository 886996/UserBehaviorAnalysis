package com.github

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Copyright (C), 1996-2020, 
  * FileName: KafkaPeoducer
  * Author:   yankun
  * Date:     2020/1/28 12:28
  * Description: ${DESCRIPTION}
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  */
object KafkaPeoducer {
  def main(args: Array[String]): Unit = {
    write2Kafka("hotItems")
  }
  def write2Kafka(topic: String): Unit ={

    val properties = new Properties()
    properties.setProperty("bootstrap.servers","nn1.hadoop:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    //定义一个kafka的producer
    val producer = new KafkaProducer[String,String](properties)
    //从文件中读取数据、
    val bufferedSource = io.Source.fromFile("D:\\classhome\\Flink\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for(line <- bufferedSource.getLines()){
      val record = new ProducerRecord[String,String](topic,line)
      producer.send(record)
    }
    producer.close()

  }

}
