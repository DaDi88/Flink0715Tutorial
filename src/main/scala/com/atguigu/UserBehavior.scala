package com.atguigu

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

import scala.collection.mutable.ListBuffer
// Tuple是Flink自己实现的类型
import org.apache.flink.api.java.tuple.{Tuple}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by DaDi on 2019/12/23
  */


object UserBehavior {
  case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)
  case class UserAction(
                         userId: String,
                         itemId: String,
                         categoryId: String,
                         behavior: String,
                         ts: Long
                       )
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream =env
      .readTextFile("D:\\IdeaWorkSpace\\Flink0715Tutorial\\src\\main\\resources\\UserBehavior.csv")
      .map(line=>{
        val arr = line.split(",")
        UserAction(arr(0),arr(1),arr(2),arr(3),arr(4).toLong*1000)
      })
      .assignAscendingTimestamps(_.ts)
      .filter(_.behavior=="pv")
      .keyBy(_.itemId)
      .timeWindow(Time.minutes(60),Time.minutes(5))
      //全窗口聚合--增量聚合全窗口聚合
      .aggregate(new Agg,new Win)
      //需要多个key必须用字符串
      .keyBy("windowEnd")
      .process(new TopNHotItems(3))

    stream.print()
    env.execute()
  }
//增量聚合--输入、累加器、输出类型
class Agg extends AggregateFunction[UserAction, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: UserAction, accumulator: Long): Long = accumulator+1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a+b
}

//窗口全量聚合
class Win extends ProcessWindowFunction[Long,ItemViewCount,String,TimeWindow]{
  override def process(key: String,
                       context: Context,
                       elements: Iterable[Long],
                       out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key.toLong, context.window.getEnd, elements.iterator.next()))
  }
}
// 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串

// Tuple和Tuple1是Flink自己实现的类型
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

//初始化一个ListState保存所有状态,状态保存在磁盘或hdfs上,无法直接计算,需要new Buffer接收后再计算
  lazy val itemState = getRuntimeContext.getListState(
    //ItemViewCount--id/windowend/count
    new ListStateDescriptor[ItemViewCount]("items", Types.of[ItemViewCount])
  )
//对每条数据的操作--放在liststate中
  override def processElement(value: ItemViewCount,
                              ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    // 将每条数据都添加到集合中
    itemState.add(value)
    // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
    // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
    ctx.timerService.registerEventTimeTimer(value.windowEnd + 1)

  }

  override def onTimer(ts: Long,
                       ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    // 创建在内存中接收状态变量的空间
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()

    import scala.collection.JavaConversions._
    //挨个遍历获取数据存到Buffer中
    for (item <- itemState.get) {
      allItems += item
    }
    // 提前清除状态中的数据，释放空间
    itemState.clear()
    // 按照点击量从大到小排序
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    // 将排名信息格式化成 String, 便于打印
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(ts - 1)).append("\n")
    //将Buffer中的sortedItems目录遍历取出
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
