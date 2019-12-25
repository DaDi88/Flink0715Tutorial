package com.atguigu


import com.atguigu.UserBehavior.UserAction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
/**
  * Created by DaDi on 2019/12/24
  * 布隆过滤器求网站独立访客数（UV）的统计
  * 1000w bit = 10M
  */
object UvWithBloomFilter {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //env.setParallelism(1)
    val stream = env
      .readTextFile("D:\\IdeaWorkSpace\\Flink0715Tutorial\\src\\main\\resources\\UserBehavior.csv")
      .map(line=>{
        val arr = line.split(",")
        UserAction(arr(0),arr(1),arr(2),arr(3),arr(4).toLong*1000)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserAction](Time.seconds(0)) {
        override def extractTimestamp(element: UserAction): Long = element.ts
      })
      //如果是升序时间戳可以采用 以下方式，如不清楚则使用上面的方式，自定义一个获取时间戳的方式可以将最大延迟时间设置为0
      //.assignAscendingTimestamps(_.ts)
      .filter(_.behavior=="pv")
      .map(r=>("dummyKey",r.userId))
      .keyBy(_._1)
      .timeWindow(Time.minutes(60))
      .trigger(new MyTrigger)
      .process(new MyProcess)
    stream.print()
    env.execute()
  }
  //自定义触发器
  class MyTrigger extends Trigger[(String,String),TimeWindow]{
    //针对于每个元素做一个聚合并计算
    override def onElement(element: (String, String),
                           timestamp: Long,
                           window: TimeWindow,
                           ctx: Trigger.TriggerContext): TriggerResult = {
      //      if (timestamp >= window.getEnd) {
      //        val jedis = new Jedis("localhost", 6379)
      //        val count = jedis.hget("UvCountHashTable", window.getEnd.toString).toLong
      //        println(count, window.getEnd.toString)
      //      }

      TriggerResult.FIRE_AND_PURGE
    }

    override def onProcessingTime(time: Long,
                                  window: TimeWindow,
                                  ctx: Trigger.TriggerContext): TriggerResult = {
      TriggerResult.CONTINUE
    }

//回调函数,触发时调用
    override def onEventTime(time: Long,
                             window: TimeWindow,
                             ctx: Trigger.TriggerContext): TriggerResult = {
//如果水位线超过窗口最终时间
      if (ctx.getCurrentWatermark >= window.getEnd) {
        val jedis = new Jedis("hadoop102", 6379)
        val key = window.getEnd.toString
        //针对于每个窗口做一个聚合并计算
        TriggerResult.FIRE_AND_PURGE
        //打印出Redis中的数据
        println(key, jedis.hget("UvCountHashTable", key))
      }
      TriggerResult.CONTINUE
    }

    override def clear(window: TimeWindow,
                       ctx: Trigger.TriggerContext): Unit = {

    }
  }

  // 自定义窗口处理函数
  class MyProcess extends ProcessWindowFunction[(String,String),(Long,Long),String,TimeWindow] {
    //状态 ,创建redise连接在此处创建可以减少连接次数
    lazy val jedis = new Jedis("hadoop102", 6379)
    lazy val bloom = new Bloom(1 << 29)

    override def process(key: String,
                         context: Context,
                         elements: Iterable[(String, String)],
                         out: Collector[(Long, Long)]): Unit = {
      //获取窗口最终值作为key
      val storeKey = context.window.getEnd.toString
            var count = 0L
      //取出Redis中存储的k-v
      if (jedis.hget("UvCountHashTable", storeKey) != null) {
              count = jedis.hget("UvCountHashTable", storeKey).toLong
            }
      //每次都会触发窗口闭合故 只有一个值._2为userId
            val userId = elements.last._2
      //通过布隆对象的hash算法计算userId对应的offset(空间位置值)
            val offset = bloom.hash(userId, 61)

//获取storeKey键处存储的字符串的位值
            val isExist = jedis.getbit(storeKey, offset)
//如果0,在Redise中表现为false,如果0那就将他的位值改为1(true)然后将Redise的count+1
      if (!isExist) {
              jedis.setbit(storeKey, offset, true)
//将指定的字段设置为指定的值,UvCountHashTable是表名
              jedis.hset("UvCountHashTable", storeKey, (count + 1).toString)
              out.collect((storeKey.toLong, count + 1))
            }
//            else {
//              out.collect((storeKey.toLong, count))
//            }
    }

  }

//自定义布隆过滤器,内含hash算法
  class Bloom(size:Long) extends Serializable{
    private val cap = size

    def hash(value: String, seed: Int): Long = {
      var result = 0
      for (i <- 0 until value.length) {
        // 最简单的hash算法，每一位字符的ascii码值，乘以seed之后，做叠加
        result = result * seed + value.charAt(i)
      }
      (cap - 1) & result
    }
  }
}
