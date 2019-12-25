

package com.atguigu

import java.util.Calendar

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark

import scala.util.Random

object StreamingTimestamps {

  case class SensorReading(id: String,
                           timestamp: Long,
                           temperature: Double
                          )

  class SensorSource extends RichParallelSourceFunction[SensorReading]{

    // flag: 表示数据源是否还在正常运行
    var running: Boolean = true
    // run()函数连续的发送SensorReading数据，使用SourceContext
    // 需要override
    override def run(ctx: SourceContext[SensorReading]): Unit = {

      // 初始化随机数发生器
      val rand = new Random()

      // 查找当前运行时上下文的任务的索引
      val taskIdx=this.getRuntimeContext.getIndexOfThisSubtask

      // 初始化10个(温度传感器的id, 温度值)元组
      var curFTemp = (1 to 10).map{
        // nextGaussian产生高斯随机数
        i=>("sensor_"+(taskIdx*10 + i),65+(rand.nextGaussian()*20))
      }

      // 无限循环，产生数据流
      while (running) {
        // 更新温度
        curFTemp=curFTemp.map(t=>(t._1,t._2+(rand.nextGaussian()*0.5)))

        // 获取当前时间戳
        val curTime =Calendar.getInstance().getTimeInMillis

        // 发射新的传感器数据, 注意这里srcCtx.collect
        curFTemp.foreach(t=>ctx.collect(SensorReading(t._1,curTime,t._2)))

        // wait for 100 ms
        Thread.sleep(100)
      }
    }
    // override cancel函数
    override def cancel(): Unit = {
      running = false
    }

  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
   // env.setParallelism(1)
    val stream =env.addSource(new SensorSource)

    val value = stream
      .assignTimestampsAndWatermarks(new MyAssigner)
      .keyBy(_.id)
      //.timeWindow(Time.seconds(5))
value.print()

    env.execute()
  }


  class MyAssigner extends AssignerWithPeriodicWatermarks[SensorReading]{
    val bound:Long = 6*1000
    var maxTs:Long = Long.MinValue

    //获取水位线的方法
    override def getCurrentWatermark: Watermark = {
      new Watermark(maxTs-bound)
    }
    //获取时间戳的方法
    override def extractTimestamp(element: SensorReading, l: Long): Long = {
      maxTs = maxTs.max(element.timestamp)
      element.timestamp
    }
  }
//  class MyMapFunction extends MapFunction[SensorReading,String]{
//    override def map(value: SensorReading):String = {
//      value.id
//    }
//
//  }
//  class MyFilterFunction extends FilterFunction[String]{
//    override def filter(value: String): Boolean = {
//      value =="sensor_1"
//    }
//  }
}
