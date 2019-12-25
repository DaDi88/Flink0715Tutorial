package com.atguigu

import com.atguigu.UserBehavior.UserAction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
/**
  * Created by DaDi on 2019/12/24
  * 浏览量统计,使用set集合占用内存大,可优化使用布隆
  */
object Uv {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .readTextFile("D:\\IdeaWorkSpace\\Flink0715Tutorial\\src\\main\\resources\\UserBehavior.csv")
      .map(line=>{
        val arr = line.split(",")
        UserAction(arr(0),arr(1),arr(2),arr(3),arr(4).toLong*1000)
      })
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior=="pv")
      .timeWindowAll(Time.minutes(60))
      .process(new Mypro)

    stream.print()
    env.execute()
  }
  class Mypro extends ProcessAllWindowFunction[UserAction,(Long,Long),TimeWindow]{
    override def process(context: Context,
                         elements: Iterable[UserAction],
                         out: Collector[(Long, Long)]): Unit = {
      var s:Set[String] = Set()

      for(u<-elements){
          s += u.userId
        }
      out.collect((s.size,context.window.getEnd))


    }
  }
}
