package com.feixeyes.realetime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by apple on 2017/9/15.
  */
class SocketCosumeDemo {

}

object SocketCosumeDemo {
  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    var sum = 0
    if( newValues !=None && newValues.length != 0){
      sum = newValues.reduce((x,y)=>x+y)
    }
    sum += runningCount.getOrElse(0)
    Some(sum)
  }

  def main(args:Array[String] ){

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkConf().setAppName("socketStreaming")
    val ssc = new StreamingContext(sc, Seconds(3))
    ssc.checkpoint("/Users/apple/test")
    val lines = ssc.socketTextStream("localhost", 9999)

    lines.flatMap( _.split(" "))
      .map(word => (word,1))
      .reduceByKey(_ + _)
      .updateStateByKey(updateFunction)
      .print()
    ssc.start()
    ssc.awaitTermination()


  }
}