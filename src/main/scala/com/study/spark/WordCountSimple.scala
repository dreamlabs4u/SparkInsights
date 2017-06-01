package com.study.spark

import org.apache.spark._

object WordCountSimple {
  val appName = "spark-word-count"

  def main(args: Array[String]): Unit = {
   assert(
     args.length == 3,
     s"""
        |Application expects exactly 3 arguments provided ${args.length}
        |
        |<master> <input_path> <output_path>
      """.stripMargin
   )

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(args(0))

    // A SparkContext represents the connection to a Spark cluster
    val sc = new SparkContext(conf)
    val inputPath = args(1)
    val outputPath = args(2)

    // RDD 1
    val inputRDD = sc.textFile(inputPath)

    // RDD2
    val wordsRDD = inputRDD.flatMap(_.split("\\s+")) // split words

    // RDD 3
    val tuplesRDD = wordsRDD.map(w => (w, 1))

    // RDD 4
    val reducedRDD = tuplesRDD.reduceByKey(_ + _)

    // Action
    reducedRDD.saveAsTextFile(outputPath)
  }
}