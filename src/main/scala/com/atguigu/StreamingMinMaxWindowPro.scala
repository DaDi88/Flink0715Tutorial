package com.atguigu

import com.atguigu.StreamingProcessFunction.{SensorReading, SensorSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 升级版-降低内存消耗-获取指定窗口数据的最大值最小值及时间戳
  */
object StreamingMinMaxWindowPro{

  case class MinMaxTemp(id: String,
                        min: Double,
                        max: Double,
                        endTs: Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val HighLow = env
      .addSource(new SensorSource)
      .map(r => (r.id, r.temperature, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .aggregate(new AggMinMax, new HighLowTemp)

    HighLow.print()

    env.execute()

  }

  class AggMinMax extends AggregateFunction[(String, Double, Double),
    (String, Double, Double), (String, Double, Double)] {
    override def createAccumulator(): (String, Double, Double) = ("", 0.0, 0.0)
    override def add(value: (String, Double, Double), accumulator: (String, Double, Double)): (String, Double, Double) = {
      (value._1, value._2.min(accumulator._2), value._3.max(accumulator._3))
    }

    override def getResult(accumulator: (String, Double, Double)): (String, Double, Double) = {
      accumulator
    }

    override def merge(a: (String, Double, Double), b: (String, Double, Double)): (String, Double, Double) = {
      (a._1, a._2.min(b._2), a._3.max(b._3))
    }
  }

  class HighLowTemp extends ProcessWindowFunction[(String, Double, Double), MinMaxTemp, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[(String, Double, Double)], out: Collector[MinMaxTemp]): Unit = {
      val e= elements.iterator.next()
      out.collect(MinMaxTemp(key, e._2, e._3, context.window.getEnd))
    }
  }
}