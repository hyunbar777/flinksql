package com.duoduo.flinksql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._

/**
 * Author z
 * Date 2020-11-18 20:40:26
 */
object Conect_kafka_sql {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val setttings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, setttings)
  
 tableEnv.sqlUpdate(
      """
        |create table kafka_connect_sql(
        |id STRING,
        |dt STRING,
        |tt STRING
        |)with(
        |'connector'='kafka',
        |'connector.topic' = 'kafka_connect_sql',
        |'connector.properties.zookeeper.connect' = 'hadoop100:9092;hadoop101:9092;hadoop102:9092',
        |'connector.properties.bootstrap.servers' = 'hadoop100:2181;hadoop101:2181;hadoop102:2181',
        |'update-mode' = 'append',
        |'format' = 'csv'
        |)
        |""".stripMargin)
    
    val table = tableEnv.from("kafka_connect_sql")
    table.toAppendStream[(String, String, String)].print()
  
  
  
    //table.insertInto("kafkaOutputTable")
    
    env.execute()
  }
}
