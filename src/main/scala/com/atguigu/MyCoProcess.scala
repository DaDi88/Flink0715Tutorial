package com.atguigu
import com.atguigu.StreamingProcessFunction.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * CoProcessFunction合并两条流。
  */
object MyCoProcess{

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream1 = env.addSource(new SensorSource)

    //创建固定数据来源--fromElements
    val stream2 = env.fromElements(
      ("sensor_2",1000*2L),
        ("sensor_7",1000*7L)
    )

    //process--自定义的处理规则
    stream1.keyBy(_.id).connect(stream2.keyBy(_._1)).process(new MyCoProcessFunction).print()

    //开始执行
    env.execute()
  }

//1流类型。2流类型。输出类型
  class MyCoProcessFunction extends CoProcessFunction[SensorReading, (String, Long), SensorReading] {
    // 定义状态变量作为开关，自定义开关的名字和类型，只对健控流内部可见，针对每一个key有单独的开关
    lazy val forwardingEnabled = getRuntimeContext.getState(
      new ValueStateDescriptor[Boolean]("filterSwitsh",Types.of[Boolean])
    )

    //定义状态变量储存时间戳，自定义时间戳的名字和类型,他会有一个空的初始值
    lazy val disableTimer = getRuntimeContext.getState(
      new ValueStateDescriptor[Long]("timer", Types.of[Long])
    )

    //操作第1条流
    override def processElement1(value: SensorReading,
                                 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
      // 决定我们是否要将数据继续传下去
      if (forwardingEnabled.value()) {
        out.collect(value)
      }
    }

    //操作第2条流
    override def processElement2(value: (String, Long),
                                 ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#Context,
                                 out: Collector[SensorReading]): Unit = {
      // 允许继续传输数据
      forwardingEnabled.update(true)

      // 获取当前机器时间,加上第2条流的时间
      val timerTimestamp = ctx.timerService().currentProcessingTime() + value._2

      //获取之前定义的时间戳初始值
      val curTimerTimestamp = disableTimer.value()

      //比较新时间与初始值间的大小
      if (timerTimestamp > curTimerTimestamp) {

        // 删除初始的时间戳
        ctx.timerService().deleteProcessingTimeTimer(curTimerTimestamp)
        //将新时间注册为时间戳
        ctx.timerService().registerProcessingTimeTimer(timerTimestamp)

        //更新时间戳
        disableTimer.update(timerTimestamp)
      }
    }

    override def onTimer(ts: Long,
                         ctx: CoProcessFunction[SensorReading, (String, Long), SensorReading]#OnTimerContext,
                         out: Collector[SensorReading]): Unit = {
      // remove all state; forward switch will be false by default
      forwardingEnabled.clear()
      disableTimer.clear()
    }
  }
}