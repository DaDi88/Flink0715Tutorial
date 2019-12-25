package com.atguigu

import java.sql.Date
import java.text.SimpleDateFormat

import com.atguigu.SimulatedEventSource.SimulatedEventSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by DaDi on 2019/12/24
  * APP市场推广统计
  * -分渠道统计
  */
object AppMarketingByChannel {
  case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val  stream = env
      .addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
        ((data.channel, data.behavior), 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(1))
      .process(new MarketingCountByChannel)
      .print()
    env.execute(getClass.getSimpleName)
  }
  class MarketingCountByChannel extends ProcessWindowFunction[((String,String),Long),
                                                                MarketingViewCount,
                                                                (String, String),
                                                                TimeWindow
                                                              ]{
    override def process(key: (String, String),
                         context: Context,
                         elements: Iterable[((String, String), Long)],
                         out: Collector[MarketingViewCount]): Unit = {
      val startTs = context.window.getStart
      val endTs = context.window.getEnd
      val channel = key._1
      val behaviorType = key._2
      val count = elements.size
      out.collect(MarketingViewCount(formatTs(startTs), formatTs(endTs), channel, behaviorType, count) )
    }
    private def formatTs (ts: Long) = {
      val df = new SimpleDateFormat ("yyyy/MM/dd-HH:mm:ss")
      df.format (new Date (ts) )
    }
  }
}
