package com.study.spark

import scala.util.Try

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

case class CardData(cardNumber: String, cardHolder: String,lastBillDate: String, extractDate: String)

case class CardAccount(cardNumber: String, accountNumber: String)

case class AccountData(accountNumber: String, cardHolder: String,lastBillDate: String, extractDate: String)

object DataFlow {

  val appName = "spark-data-flow"

    def readCardData(sc: SparkContext, inputPath: String): RDD[CardData] = {
      def toCardData(row: Array[String]) =
        Try{CardData(row(0), row(1), row(2), row(3))}.toOption

      sc
        .textFile(inputPath)
        .flatMap(line => toCardData(line.split(",")))
    }

    def readCardAccount(sc: SparkContext, inputPath: String): RDD[CardAccount] = {
      def toCardAccount(row: Array[String]) =
        Try{CardAccount(row(0), row(1))}.toOption

      sc
        .textFile(inputPath)
        .flatMap(line => toCardAccount(line.split(",")))
    }

  def tranformAndDedup(cardData: RDD[CardData], cardAccount: RDD[CardAccount]): RDD[AccountData] = {
    cardData
      .map(t => (t.cardNumber, t))
      .reduceByKey{case (left, right) => if(left.extractDate > right.extractDate) right else left}
      .join(cardAccount.map(acct => (acct.cardNumber, acct)))
      .mapValues{ case (card, acct) => AccountData(acct.accountNumber, card.cardHolder, card.lastBillDate, card.extractDate)}
      .values
  }

  def persist(outputFile: String, accountData: RDD[AccountData]) =
    accountData.saveAsTextFile(outputFile)


  def main(args: Array[String]): Unit = {
    assert(
      args.length == 4,
      s"""
         |Application expects exactly 3 arguments provided ${args.length}
         |
        |<master> <card_data_input_path> <card_account_input_path> <output_path>
      """.stripMargin
    )

    val conf = new SparkConf()
      .setAppName(appName)
      .setMaster(args(0))
    // A SparkContext represents the connection to a Spark cluster
    val sc = new SparkContext(conf)


    // Read Program Arguments
    val cardDataInputPath = args(1)
    val cardAccountInputPath = args(2)
    val outputPath = args(3)


    // Read Card Data
    val cardData = readCardData(sc, cardDataInputPath)

    // Read Card Account
    val cardAccount = readCardAccount(sc, cardAccountInputPath)

    // transform and dedup
    val accountData = tranformAndDedup(cardData, cardAccount)

    // persist data
    persist(outputPath, accountData)

    sc.stop()
  }
}