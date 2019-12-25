package com.atguigu

import java.util.Properties
import org.apache.flink.streaming.api.scala._
import com.atguigu.UserBehavior.UserAction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011}
/**
  * Created by DaDi on 2019/12/24
  *kafka消费者作为数据源
  */
object UserBehaviorWithKafka {
  def main(args: Array[String]): Unit = {
//kafka配置信息
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")//监听地址
    properties.setProperty("group.id", "consumer-group")//消费者组
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")//反序列化器
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")//反序列化器
    properties.setProperty("auto.offset.reset", "latest")//offset读取最新位置的
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env
    // source为来自Kafka的数据，这里我们实例化一个消费者，topic为hotitems
      .addSource(new FlinkKafkaConsumer011[String]("hotitems",new SimpleStringSchema(),properties))
      .map(
        line=>{
        val arr = line.split(",")
        UserAction(arr(0),arr(1),arr(2),arr(3),arr(4).toLong*1000)
        }
      )
    stream.print()
    env.execute()
  }
}
