package com.atguigu

import com.atguigu.StreamingProcessFunction.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 获取指定窗口数据的最大值最小值及时间戳
  */
object StreamingMinMaxWindow{

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
   // env.setParallelism(1)
    //获取流
    val stream= env.addSource(new SensorSource)
    stream
      .keyBy(_.id)
      .timeWindow(Time.seconds(5))
      .process(new HighAndLowTempProcessFunction).print()

    env.execute()
  }
    case class MinMaxTemp(id: String, min: Double, max: Double, endTs: Long)
//数据泛型包括[输入流数据类型,模式匹配,key,窗口终止时间]
    class HighAndLowTempProcessFunction extends ProcessWindowFunction[SensorReading, MinMaxTemp, String, TimeWindow]{

      override def process(key: String,
                           ctx: Context,
                           elements: Iterable[SensorReading],
                           out: Collector[MinMaxTemp]): Unit = {
        val temps = elements.map(_.temperature)
        val windowEnd = ctx.window.getEnd

        out.collect(MinMaxTemp(key, temps.min, temps.max, windowEnd))

      }
    }

}