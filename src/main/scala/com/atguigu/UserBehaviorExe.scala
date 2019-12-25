package com.atguigu


import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Created by DaDi on 2019/12/23
  */
object UserBehaviorExe {
  //模式匹配：用户、商品、种类、行为、时间数据
  case class UserAction (
                          userId:String,
                          itemId:String,
                          categoryId:String,
                          behavior:String,
                          ts:Long)
  case class ItemViewCount(itemId: String,windowEnd:Long,count:Long)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env
      .readTextFile("D:\\IdeaWorkSpace\\Flink0715Tutorial\\src\\main\\resources\\UserBehavior.csv")
      .map(line=>{
        val arr = line.split(",")
        UserAction(arr(0),arr(1),arr(2),arr(3),arr(4).toLong*1000)
      })
      //注册时间
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior=="pv")
      //按key分流
      .keyBy(_.itemId)
      //划分窗口
      .timeWindow(Time.minutes(60),Time.minutes(5))
      //增量聚合+全窗口聚合
      .aggregate(new Agg,new Win)
      //按win的key分区
      .keyBy("windowEnd")
      //process给定取topn函数 new TopNHotItems
      .process(new TopNHotItems(3))
    //输出
    stream.print()
    env.execute()
  }
  //
   class Agg extends AggregateFunction[UserAction,Long,Long]{
    override def createAccumulator(): Long = 0L

    override def add(value: UserAction, accumulator: Long): Long = accumulator+1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

   class Win extends ProcessWindowFunction[Long,ItemViewCount,String,TimeWindow]{

    override def process(key:String, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      out.collect(ItemViewCount(key,context.window.getEnd,elements.iterator.next()))
}
  }

  // 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
  class TopNHotItems(topSize:Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{

    //ToDo：状态函数做检查点的作用，包含标签和类型

    lazy val itemState = getRuntimeContext.getListState{
      new ListStateDescriptor[ItemViewCount]("items",Types.of[ItemViewCount])
    }

    //对每条数据的操作--放在liststate中
    override def processElement(value: ItemViewCount,
                                ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                                out: Collector[String]): Unit = {
      itemState.add(value)
      // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
      // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
      ctx.timerService().registerEventTimeTimer(value.windowEnd+1)
    }
//ToDo 回调函数，当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext,
                         out: Collector[String]): Unit = {
      // 创建在内存中接收状态变量的空间
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()

      import scala.collection.JavaConversions._
      for(i<-itemState.get()){
        //将i放入buffer
        allItems += i
      }
      // 提前清除状态中的数据，释放空间
      itemState.clear()
      // 按照点击量从大到小排序
      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      // 将排名信息格式化成 String, 便于打印
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
      //将Buffer中的sortedItems目录遍历取出,i是索引，currentItem是对象整体
      for (i <- sortedItems.indices) {
        val currentItem: ItemViewCount = sortedItems(i)

        // e.g. No1：商品ID=12224 浏览量=2413
        result
          .append("No")
          .append(i + 1)
          .append(":")
          .append("  商品ID=")
          .append(currentItem.itemId)
          .append("  浏览量=")
          .append(currentItem.count).append("\n")
      }

      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
      Thread.sleep(1000)
      out.collect(result.toString)
    }
  }
}


