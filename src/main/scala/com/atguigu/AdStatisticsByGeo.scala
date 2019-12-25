package com.atguigu

import java.sql.Date
import java.text.SimpleDateFormat

import com.atguigu.UserBehavior.{Agg, Win}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
  * Created by DaDi on 2019/12/24
  *
  */
object AdStatisticsByGeo {
  case class CountByProvince(windowEnd: String, province: String, count: Long)
  case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .readTextFile("D:\\IdeaWorkSpace\\Flink0715Tutorial\\src\\main\\resources\\AdClickLog.csv")
      .map(line=>{
        val arr = line.split(",")
        AdClickLog(arr(0).toLong,arr(1).toLong,arr(2),arr(3),arr(4).toLong*1000)
      })
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(_.province)
      .timeWindow(Time.minutes(60),Time.seconds(5))
      //在窗口后做聚合-1.使用累加器做累加,2.使用窗口聚合函数
      .aggregate(new CountAgg, new CountResult)
      .print()
    env.execute()
  }

  class CountAgg() extends AggregateFunction[AdClickLog, Long, Long]{
    override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1L
    override def createAccumulator(): Long = 0L
    override def getResult(accumulator: Long): Long = accumulator
    override def merge(a: Long, b: Long): Long = a + b
  }

  class CountResult() extends WindowFunction[Long, CountByProvince, String, TimeWindow]{
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
      //做模式匹配类型，匹配输出结果结构
      out.collect(CountByProvince(formatTs(window.getEnd), key, input.iterator.next()))
    }
    private def formatTs (ts: Long) = {
      val df = new SimpleDateFormat ("yyyy/MM/dd-HH:mm:ss")
      df.format (new Date (ts) )
    }
  }
}

