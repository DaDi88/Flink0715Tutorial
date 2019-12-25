package com.atguigu


import com.atguigu.StreamingProcessFunction.SensorSource
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * Created by DaDi on 2019/12/21
  * 触发器案例
  */
object TiggerTest1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env
      .addSource(new SensorSource)
      .assignAscendingTimestamps(_.timestamp)
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(5))
      .trigger(new MyTrigger)
      .process(new MyProcess)

    stream.print()
    env.execute()
  }
  class MyProcess extends ProcessWindowFunction[(String,Double),(String,Double,Double,Long),String,TimeWindow]{
    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, Double)],
                         out: Collector[(String, Double, Double, Long)]): Unit = {
      val temps = elements.map(_._2)
      //输出模式匹配
      out.collect((key,temps.min,temps.max,context.window.getEnd))
    }
  }
  class MyTrigger extends Trigger[(String,Double), TimeWindow]{

    override def onElement(element: (String, Double),
                           timestamp: Long, window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = {
      val firstSeen=ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("firstSeen",classOf[Boolean])
      )
      if(!firstSeen.value()){
        val t=ctx.getCurrentWatermark+(1000-(ctx.getCurrentWatermark%1000))
        ctx.registerEventTimeTimer(t)
        ctx.registerEventTimeTimer(window.getEnd)
        firstSeen.update(true)
      }
      TriggerResult.CONTINUE
    }

    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def onEventTime(time: Long,
                             window: TimeWindow,
                             ctx: Trigger.TriggerContext): TriggerResult = {

      if(time>=window.getEnd){
        TriggerResult.FIRE_AND_PURGE
      }else{
        val t=ctx.getCurrentWatermark+(1000-(ctx.getCurrentWatermark%1000))

          ctx.registerEventTimeTimer(t)
          TriggerResult.FIRE

      }

    }

    override def clear(window: TimeWindow,
                       ctx: Trigger.TriggerContext): Unit = {
      val firstSeen=ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean]("firstSeen",classOf[Boolean])
      )
      firstSeen.clear()

    }
  }



}