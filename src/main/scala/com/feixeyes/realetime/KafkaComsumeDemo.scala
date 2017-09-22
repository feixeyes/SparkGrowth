package com.feixeyes.realetime

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by apple on 2017/9/15.
  */
class KafkaComsumeDemo {

}

object KafkaComsumeDemo {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.ERROR)

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc =  new StreamingContext(sparkConf, Seconds(3))
    //    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
      .map(x=>x._1)
    val words = lines.flatMap(_.split("-"))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKey(_+_)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
