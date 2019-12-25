

package com.atguigu
import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import scala.util.Random
import org.apache.flink.streaming.api.scala._

object StreamingJob {
//  case class WordCount(word:String,count:Long){
//
//  }
//  def main(args: Array[String]) {
//1.获取运行时环境
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//1.1设置全局并行度为1
//      env.setParallelism(1)
//2.获取连接
//   val text =  env.socketTextStream("hadoop102",port = 9999,'\n')
//3.具体操作
//    val wordcount = text
//        .flatMap(_.split("\\s"))
//        .map(w=>WordCount(w,1))
//        .keyBy("word")
//        .timeWindow(Time.seconds(10))
//        .sum("count")
//    wordcount.print()
//    env.execute("Flink Streaming Scala API Skeleton")
//  }
//  case class SensorReading(id:String,timestamp:Long,temperature:Double)
//
//  def main(args: Array[String]): Unit = {
//
//      val env = StreamExecutionEnvironment.getExecutionEnvironment
//            env.setParallelism(1)
//      val stream = env.fromCollection(List(
//        SensorReading("1",1,1.1),
//        SensorReading("2",2,9.1),
//        SensorReading("3",3,6.1),
//        SensorReading("4",4,5.1)
//
//
//      ))
//    }
// 传感器id，时间戳，温度
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
    env.setParallelism(1)

    env.addSource(new SensorSource).print()
    env.execute()
  }
}
