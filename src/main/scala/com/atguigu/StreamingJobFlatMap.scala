

package com.atguigu
import org.apache.flink.streaming.api.scala._



object StreamingJobFlatMap {

  def main(args: Array[String]) {
      //1.获取运行时环境
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      //1.1设置全局并行度为1，就只是用一个任务槽
     //默认槽数等于核数，指定了并行度后按指定的来
        env.setParallelism(1)
      //2.获取连接
      val stream =  env.socketTextStream("hadoop102",port = 9999,'\n')
      //FlatMap--具体操作
//      stream
//          .flatMap(_.split("\\s"))
//          .map((_,1))
//          .keyBy(0)//groupby
//          .sum("1")
//          .print()
    //map--具体操作


      //4.开始执行
    env.execute()
  }
}
