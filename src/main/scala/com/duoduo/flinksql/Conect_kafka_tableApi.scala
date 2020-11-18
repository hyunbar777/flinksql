package com.duoduo.flinksql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Json, Kafka, Schema}

/**
 * Author z
 * Date 2020-11-18 20:40:26
 */
object Conect_kafka_tableApi {
  def main(args: Array[String]): Unit = {
    //创建老版本的流查询环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val setttings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, setttings)
    
    //读kafka数据
    tableEnv.connect(
      new Kafka()
        .version("0.11")
        .topic("student")
        .property("bootstrap.servers", "hadoop100:9092;hadoop101:9092;hadoop102:9092")
        .property("zookeeper.connect", "hadoop100:2181;hadoop101:2181;hadoop102:2181")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("dt", DataTypes.STRING())
        .field("tt", DataTypes.STRING())
      )
      .createTemporaryTable("kafkaInputTable")
    //转换成流打印输出
    val table: Table = tableEnv.from("kafkaInputTable")
    table.toAppendStream[(String, String, String)].print()
    
    //写入kafka topic中
    tableEnv.connect(
      new Kafka()
        .version("0.11")
        .topic("student_output")
        .property("bootstrap.servers", "hadoop100:9092;hadoop101:9092;hadoop102:9092")
        .property("zookeeper.connect", "hadoop100:2181;hadoop101:2181;hadoop102:2181")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("dt", DataTypes.STRING())
        .field("tt", DataTypes.STRING())
      )
      .createTemporaryTable("kafkaOutputTable")
  
    table.insertInto("kafkaOutputTable")
    
    env.execute()
  }
}
