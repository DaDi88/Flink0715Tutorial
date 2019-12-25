package com.atguigu

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by DaDi on 2019/12/24
  * kafka生产者,要先监听再生产
  */
object UserBehaviorkafkaProducer {
  def main(args: Array[String]): Unit = {
    writeToKafka("hotitems")
  }
  def writeToKafka(topic: String): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers", "hadoop102:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    val bufferedSource = io.Source.fromFile("D:\\IdeaWorkSpace\\Flink0715Tutorial\\src\\main\\resources\\UserBehavior.csv")
    for (line <- bufferedSource.getLines) {
      val record = new ProducerRecord[String, String](topic, line)
      //发送
      producer.send(record)
    }
    producer.close()
  }
}
