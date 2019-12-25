//package com.atguigu
//
//import org.apache.flink.api.common.state.ValueStateDescriptor
//import org.apache.flink.streaming.api.TimeCharacteristic
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector
//
///**
//  * Created by DaDi on 2019/12/21
//  *
//  */
//object AllowedLateTest {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//
//    val stream = env.socketTextStream("hadoop102",port = 9999,'\n')
//
//    val s = stream.map(line => {
//      val arr = line.split(" ")
//      (arr(0), arr(1).toLong * 1000)
//    }).assignTimestampsAndWatermarks(
//      //设定最大延迟时间
//      new BoundedOutOfOrdernessTimestampExtractor[(String, Long)](Time.seconds(5)) {
//              //提取时间戳
//              override def extractTimestamp(element: (String, Long)): Long = element._2
//    })
//      .keyBy(_._1)
//      .timeWindow(Time.seconds(5))
//      .process(new MyAllowedLateProcess)
//    s.print()
//    env.execute()
//  }
//
//  class MyAllowedLateProcess extends ProcessWindowFunction[(String,Long),String,String,TimeWindow]{
//    override def process(key: String,
//                         context: Context,
//                         elements: Iterable[(String, Long)],
//                         out: Collector[String]): Unit = {
//      lazy val isUpdate = getRuntimeContext.getState(
//        new ValueStateDescriptor[Boolean]("update",Boolean)
//      )
//      if(!isUpdate.value()){
//        out.collect("在水位线超过窗口结束时间的时候，窗口第一次闭合计算")
//        isUpdate(true)
//      }else{
//        out.collect("迟到元素来了以后，更新窗口闭合计算的结果")
//      }
//    }
//  }
//}
