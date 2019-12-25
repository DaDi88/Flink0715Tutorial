

package com.atguigu

import java.util.Calendar

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.util.Random

object StreamingProcessFunction {
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

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    env.setParallelism(2)//设置并行度

    //val stream = env.socketTextStream("hadoop102",9999,'\n')
    val stream = env.addSource(new SensorSource)

    stream
      .keyBy(_.id)
      .process(new TempIncreaseAlertFunction)
      .print()

    env.execute()
  }
  //
  class TempIncreaseAlertFunction extends KeyedProcessFunction[String,SensorReading,String]{

    //初始化状态变量
    //惰性求值--调用.process时才加载(初始化一个scala的变量会在内存中生成。
    // 经过设置后可以存储在hdfs等上,宕机重启后会被重新读取不会丢失,指定了变量的名字和类型,序列化到hdfs上)
    lazy val lastTemp:ValueState[Double] = getRuntimeContext.getState(
      new ValueStateDescriptor[Double]("lastTemp",Types.of[Double])
    )

    lazy val currentTimer:ValueState[Long] = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("time",Types.of[Long])
    )

    //输入类型.上下文.输出类型
    //注册时间戳
    override def processElement(r: SensorReading,
                                //#Context--访问一个抽象类的抽象子类必须使用类型投影的方式
                                ctx: KeyedProcessFunction[String, SensorReading, String]#Context,
                                out: Collector[String]): Unit = {
      val prevTemp = lastTemp.value()

      lastTemp.update(r.temperature)

      val curTimerTimestamp = currentTimer.value()
      // 温度下降或者是第一个温度值，删除定时器
      if (prevTemp == 0.0 ||r.temperature<prevTemp){
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        currentTimer.clear()
        // 温度上升且我们并没有设置定时器
      }else if (r.temperature > prevTemp && curTimerTimestamp == 0) {

        // 设置时间戳
        val timerTs = ctx.timerService().currentProcessingTime() + 1000
       //注册定时器
        ctx.timerService().registerProcessingTimeTimer(timerTs)

        // 更新状态变量的值-时间戳
        currentTimer.update(timerTs)
      }
    }

    //定时事件的业务逻辑,做输出
    override def onTimer(ts: Long,
                         ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      out.collect("传感器id为: " + ctx.getCurrentKey + "的传感器温度值已经连续1s上升了。")

      //清除状态变量
      currentTimer.clear()
    }
  }
}

