package com.atguigu

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
/**
  * Created by DaDi on 2019/12/21
  */
object LateElement {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.socketTextStream("localhost",port = 9999,'\n')

    val s = stream.map(line => {
      val arr = line.split(" ")
      (arr(0), arr(1).toLong * 1000)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
        override def extractTimestamp(element: (String, Long)): Long = element._2
      })
      .process(new MyLateProcess)

    s.getSideOutput(new OutputTag[String]("late")).print()

    env.execute()
  }

  class MyLateProcess extends ProcessFunction[(String, Long), (String, Long)] {

    val late = new OutputTag[String]("late")

    override def processElement(value: (String, Long),
                                ctx: ProcessFunction[(String, Long), (String, Long)]#Context,
                                out: Collector[(String, Long)]): Unit = {

      if (value._2 < ctx.timerService().currentWatermark()) {
        ctx.output(late, "这个元素迟到了！")
      } else {
        out.collect(value)
      }
    }
  }
}