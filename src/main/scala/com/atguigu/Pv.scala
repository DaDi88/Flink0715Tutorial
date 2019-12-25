package com.atguigu
import com.atguigu.UserBehavior.UserAction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * Created by DaDi on 2019/12/24
  */
object Pv {
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
      .filter(_.behavior=="pv")
      .assignAscendingTimestamps(_.ts)
      .map(r=>{
        ("dummy",1)
      })
      .keyBy(_._1)
      .timeWindow(Time.minutes(60))
      .sum(1)
    stream.print()
    env.execute()
      //未keyby时使用
      //.timeWindowAll()
  }
}
