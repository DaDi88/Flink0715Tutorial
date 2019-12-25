package com.atguigu


import com.atguigu.StreamingJob.{SensorReading, SensorSource}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
  * Created by DaDi on 2019/12/21
  */
object TiggerTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(0)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp

      })
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
      out.collect((key,temps.min,temps.max,context.window.getEnd))
    }
  }
  class MyTrigger extends Trigger[(String,Double), TimeWindow] {
    override def onElement(r: (String,Double),
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = {
      val firstSeen = ctx.getPartitionedState(
        new ValueStateDescriptor[Boolean](
          "firstSeen", classOf[Boolean]
        )
      )
      if (!firstSeen.value()) {
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        ctx.registerEventTimeTimer(t)
        ctx.registerEventTimeTimer(window.getEnd)
        firstSeen.update(true)
      }

      TriggerResult.CONTINUE
    }

    override def onEventTime(
                              timestamp: Long,
                              window: TimeWindow,
                              ctx: Trigger.TriggerContext
                            ): TriggerResult = {
      //如果定时器的被触发的时间戳大于等于窗口最后时间，触发一次计算
      if (timestamp == window.getEnd) {
        TriggerResult.FIRE_AND_PURGE
      }
      //否则注册一下定时器时间,其本质的时间是没有变化的
      else {
        val t = ctx.getCurrentWatermark + (1000 - (ctx.getCurrentWatermark % 1000))
        if (t < window.getEnd) {
          ctx.registerEventTimeTimer(t)
        }
        TriggerResult.FIRE
      }
    }

    override def onProcessingTime(
                                   timestamp: Long,
                                   window: TimeWindow,
                                   ctx: Trigger.TriggerContext
                                 ): TriggerResult = {
      TriggerResult.CONTINUE
    }

    override def clear(
                        window: TimeWindow,
                        ctx: Trigger.TriggerContext
                      ): Unit = {
      val firstSeen: ValueState[Boolean] = ctx
        .getPartitionedState(
          new ValueStateDescriptor[Boolean](
            "firstSeen", classOf[Boolean]
          )
        )
      firstSeen.clear()
    }
  }

}