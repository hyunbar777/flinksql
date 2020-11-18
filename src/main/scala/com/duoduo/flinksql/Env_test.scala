package com.duoduo.flinksql

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Kafka, OldCsv, Schema}

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
    val tableEnv = StreamTableEnvironment.create(env, setttings)
    
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
    val bTableEnv = StreamTableEnvironment.create(benv, bSetttings)
    
    //创建新版本的batch
    val bbSetttings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val bbTableEnv = TableEnvironment.create(bbSetttings)
    
    
    //从文件中读取数据
    val filePath = "F:\\BigData\\project\\flinksql\\src\\main\\resources\\sensor-data.log"
    tableEnv.connect(
      new FileSystem().path(filePath)
    )
      //定义读取数据之后的格式化方式
      .withFormat(new OldCsv())
      //定义表结构
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("dt", DataTypes.STRING())
        .field("tt", DataTypes.STRING())
      )
      .createTemporaryTable("inputTable")
    //转换成流打印输出
    val sensorTable:Table = tableEnv.from("inputTable")
    sensorTable.toAppendStream[(String,String,String)].print()
    
    env.execute()
    
  }
}
