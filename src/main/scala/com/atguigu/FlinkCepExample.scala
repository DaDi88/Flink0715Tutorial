package com.atguigu

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import scala.collection.Map

/**
  * Created by DaDi on 2019/12/23\
  * CEP
  */
object FlinkCepExample {
  case class LoginEvent(userId:String,ipAddr: String,loginStatus:String,ts:String)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream =env.fromElements(
      LoginEvent("1","192.168.0.1","fail","1558430842"),
      LoginEvent("1","192.168.0.2","fail","1558430844"),
      LoginEvent("1","192.168.0.3","fail","1558430845"),
      LoginEvent("4","192.168.0.1","success","1558430846")
    ).assignAscendingTimestamps(_.ts.toLong*1000).keyBy(_.userId)

    val p = Pattern
      .begin[LoginEvent]("x").where(_.loginStatus == "fail")
      .next("y").where(_.loginStatus == "fail")
      .next("z").where(_.loginStatus == "fail")
      .within(Time.seconds(10))

    val patternStream=CEP.pattern(stream,p)
    patternStream.select((p: Map[String,Iterable[LoginEvent]])=>{
      val x= p.getOrElse("x",null).iterator.next()
      val y= p.getOrElse("y",null).iterator.next()
      val z= p.getOrElse("z",null).iterator.next()

      (x.ipAddr,y.ipAddr,z.ipAddr)
    }).print()
    env.execute()
  }
}
