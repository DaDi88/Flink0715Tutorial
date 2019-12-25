

package com.atguigu

import java.util.Calendar


import org.apache.flink.streaming.api.functions.{ProcessFunction}
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.util.Random

object SideOutPutTest {
  case class SensorReading(
                            id: String,
                            timestamp: Long,
                            temperature: Double
                          )
  //需要extend RichParallelSourceFunction,泛型为SensorReading
  class SensorSource extends RichParallelSourceFunction[SensorReading]{

    //flag：indicating whether source is still running  表示数据源是否还在正常运行
    var running:Boolean = true

    //run() 函数连续的发送SensorReading数据，使用SourceContext
    override def run(ctx: SourceContext[SensorReading]): Unit = {

      //初始化随机数发生器
      val rand = new Random()

      //查找当前运行时上下文的任务的索引
      val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

      //初始化10个（温度传感器的id，温度值）元组
      var curFTemp=(1 to 10).map{
        //nextGaussian产生高斯随机数
        i => ("sensor_" + (taskIdx * 10 + i ),65 + (rand.nextGaussian() * 20))
      }

      //无线循环产生数据流
      while(running){

        //更新温度
        curFTemp = curFTemp.map(t => (t._1,t._2 + (rand.nextGaussian() * 0.5 ))  )
        //获取当前时间戳
        val curTime = Calendar.getInstance.getTimeInMillis

        //发射新的传感器数据，注意这里ctx.collect
        curFTemp.foreach( t => ctx.collect(SensorReading(t._1,curTime,t._2)))

        Thread.sleep(100)
      }
    }

    override def cancel(): Unit = {
      running = false
    }
  }

  /**
    * Emitting to Side Outputs(侧输出)
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    env.setParallelism(2)//设置并行度
    //获取流
    //val stream = env.socketTextStream("hadoop102",9999,'\n')
    val stream = env.addSource(new SensorSource)

    //调用一个process function函数,用于对流进行处理(侧)
    val monitoredReadings: DataStream[SensorReading] =stream
      .process(new FreezingMonitor)

    //获取侧输出的内容
    monitoredReadings
      .getSideOutput(new OutputTag[String]("freezing-alarms"))
      .print()


    env.execute()
  }

  class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading]{

    // define a side output tag
    // 定义一个侧输出标签,lazy懒加载只有第一次的时候加载一次，相当于checkpoint
    lazy val freezingAlarmOutput: OutputTag[String] =
    new OutputTag[String]("freezing-alarms")

    override def processElement(r: SensorReading,
                                ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {
      // emit freezing alarm if temperature is below 32F
      if (r.temperature < 32.0) {
       //侧流
        ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${r.id}")
      }
      // forward all readings to the regular output
   //主流
    //  out.collect(r)
    }
  }

}

