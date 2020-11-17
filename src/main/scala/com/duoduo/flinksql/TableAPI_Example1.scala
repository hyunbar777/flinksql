package com.duoduo.flinksql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

/**
 * Author z
 * Date 2020-11-16 22:38:48
 */
object TableAPI_Example1 {
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
    //基于数据流，转换成一张表
    val dataTable = tableEnv.fromDataStream(inputStream)
    //调用table api,得到转换结果
    val resultTable = dataTable.select("name,dt").filter("name == 'sensor_1'")
    
    val resultStream:DataStream[(String,String)] = resultTable.toAppendStream[(String,String)]
    
    
    resultStream.print()
    resultTable.printSchema()
    
    
    
    env.execute()
    
  }
}
case class Sensor(name:String,dt:String,id:String)
