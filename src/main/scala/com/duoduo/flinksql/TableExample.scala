package com.duoduo.flinksql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._

/**
 * Author z
 * Date 2020-11-16 22:38:48
 */
object TableExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    
    val inputStream = env
      .readTextFile("F:\\BigData\\project\\flinksql\\src\\main\\resources\\sensor-data.log")
      .map(data=>{
      
        val arr = data.split(",")
       
        Sensor(arr(0),arr(1).toDouble,arr(2).toLong)
        //println(Sensor(arr(0),arr(1).toDouble,arr(2).toLong))
      })

    //创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)
    //基于数据流，转换成一张表
    val dataTable = tableEnv.fromDataStream(inputStream)
    //调用table api,得到转换结果
    val resultTable = dataTable.select("name,dt").filter("name == 'sensor_1'")
    
    val resultStream:DataStream[(String,Double)] = resultTable.toAppendStream[(String,Double)]
    
    
    resultStream.print()
    resultTable.printSchema()
    
    env.execute()
    
  }
}
case class Sensor(name:String,dt:Double,id:Long)
