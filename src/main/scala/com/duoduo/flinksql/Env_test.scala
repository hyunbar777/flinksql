package com.duoduo.flinksql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}

/**
 * Author z
 * Date 2020-11-17 21:59:51
 */
object Env_test {
  def main(args: Array[String]): Unit = {
    //创建老版本的流查询环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val setttings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env,setttings)
  
    //创建老版本的bath查询环境
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv = BatchTableEnvironment.create(batchEnv)
    
    //创建新版本的stream
    val benv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val bSetttings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bTableEnv = StreamTableEnvironment.create(benv,bSetttings)
  
    //创建新版本的batch
    val bbSetttings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val bbTableEnv = TableEnvironment.create(bbSetttings)
  }
}
