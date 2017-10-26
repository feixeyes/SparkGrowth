package com.feixeyes.realetime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by apple on 2017/10/9.
  */
class MultiStreamDemo {

}

object MultiStreamDemo {

  def main(args:Array[String] ){

//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val sc = new SparkConf().setAppName("socketStreaming")
//    val ssc = new StreamingContext(sc, Seconds(5))
//    ssc.checkpoint("/Users/apple/test")
//    val lines = ssc.socketTextStream("localhost", 9999)
//    val stream2 = ssc.socketTextStream("localhost", 9998)
//
//
//    lines.flatMap( _.split(" "))
//      .map(word => (word,1))
//      .reduceByKey(_ + _)
//      .print()
//
//    stream2.flatMap(_.split(" "))
//      .map((_,1L))
//      .reduceByKey(_+_)
//      .print()
//    ssc.start()
//    ssc.awaitTermination()

  }

}