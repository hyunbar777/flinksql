package com.duoduo.flinksql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

/**
 * Author z
 * Date 2020-11-16 22:38:48
 */
object SQL_Example1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    
    val inputStream = env
      .readTextFile("F:\\BigData\\project\\flinksql\\src\\main\\resources\\sensor-data.log")
      .map(data=>{
      
        val arr = data.split(",")
       
        Sensor(arr(0),arr(1),arr(2))
        //println(Sensor(arr(0),arr(1).toDouble,arr(2).toLong))
      })

    //创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)
    
    val dataTable = tableEnv.fromDataStream(inputStream)
    //基于sql数据流，转换成一张表
    val resultSqlTable = tableEnv.sqlQuery("select name,dt from "+dataTable)
    //转换成流输出
    val resultStream: DataStream[(String, String)] = resultSqlTable
      .toAppendStream[(String,String)]

    resultStream.print()
    resultSqlTable.printSchema()
    
    env.execute()
    
  }
}

