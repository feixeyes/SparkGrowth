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
  def main(args:Array[String] ){

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkConf().setAppName("socketStreaming")
    val ssc = new StreamingContext(sc, Seconds(3))
    val lines = ssc.socketTextStream("localhost", 9999)

    lines.flatMap( _.split(" "))
      .map(word => (word,1))
      .reduceByKey(_ + _)
      .print()
    ssc.start()
    ssc.awaitTermination()


  }
}