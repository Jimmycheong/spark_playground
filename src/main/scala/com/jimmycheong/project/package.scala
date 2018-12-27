package com.jimmycheong

import org.apache.spark.sql.SparkSession


package object project {

  case class Transactions(itemId: Int, quantity: Int)

  case class TransactionsSums(itemId: Int, total: Long)

  case class ItemInfo(itemId: Int, description: String)

  def buildSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .config("spark.master", "local[2]")

      .getOrCreate()
  }

}
