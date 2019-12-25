

package com.atguigu

import com.atguigu.StreamingProcessFunction.{SensorReading, SensorSource}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutPutTest_1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1)

    val stream =env.addSource(new SensorSource)

    val value1 = stream
      .keyBy(_.id)
      .process(new MyKeyedProcessFunction)

   //value1.print()
    value1.getSideOutput(new OutputTag[String]("freezing-alarm")).print()

    env.execute()
  }

  class MyKeyedProcessFunction extends KeyedProcessFunction[String,SensorReading,SensorReading]{
//侧输出的内容
    lazy val freezingAlarmOutPut = new OutputTag[String]("freezing-alarm")

    override def processElement(value: SensorReading,
                                ctx: KeyedProcessFunction[String, SensorReading, SensorReading]#Context,
                                out: Collector[SensorReading]): Unit = {
      if (value.temperature<1000){

        //侧输出的向下游发送操作
        ctx.output(freezingAlarmOutPut,"温度小于32度")
      }
      //out.collect()是主流向下游发送的操作
      //out.collect(value)
    }
  }

}

