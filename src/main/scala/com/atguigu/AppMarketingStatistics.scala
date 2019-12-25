package com.atguigu

import java.text.SimpleDateFormat
import java.util.Date
import com.atguigu.AppMarketingByChannel.MarketingViewCount
import com.atguigu.SimulatedEventSource.SimulatedEventSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by DaDi on 2019/12/24
  */
object AppMarketingStatistics {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
          .addSource(new SimulatedEventSource)
          .assignAscendingTimestamps(_.timestamp)
          .filter(_.behavior != "UNINSTALL")
          .map(data => {
              ("dummyKey", 1L)
           })
          .keyBy(_._1)
          .timeWindow(Time.hours(1), Time.seconds(1))
          .process(new MarketingCountTotal)
          .print()
    env.execute(getClass.getSimpleName)

  }

  class MarketingCountTotal extends ProcessWindowFunction[(String,Long),MarketingViewCount,String,TimeWindow]{
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Long)],
                         out: Collector[MarketingViewCount]): Unit = {
      val startTs=context.window.getStart
      val endTs=context.window.getEnd
      val count = elements.size
      out.collect(MarketingViewCount(formatTs(startTs), formatTs(endTs), "total", "total", count))
    }
    private def formatTs (ts: Long) = {
      val df = new SimpleDateFormat ("yyyy/MM/dd-HH:mm:ss")
      df.format (new Date (ts) )
    }


  }
}
