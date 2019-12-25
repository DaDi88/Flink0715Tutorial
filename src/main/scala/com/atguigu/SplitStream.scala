

package com.atguigu

import java.util.Calendar

import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object SplitStream {



  def main(args: Array[String]): Unit = {
    //1.获取运行时环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //1.1设置全局并行度为1，就只是用一个任务槽
    //默认槽数等于核数，指定了并行度后按指定的来
    env.setParallelism(1)
    //2.获取连接
    val stream =  env.fromElements((1,1),(2,2),(3,3))
    //val inputStream:DataStream[(Int,String)]=

   // val flinkTweets = stream.filter(new FilterFilter)
  }

  class FilterFilter extends FilterFunction[String]{
    override def filter(value: String): Boolean = {
      value.contains("flink")
    }
  }

}
