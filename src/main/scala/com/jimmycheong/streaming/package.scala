package com.jimmycheong

import org.apache.spark.sql.SparkSession

package object streaming {
  def buildSparkSession(): SparkSession = {
    SparkSession
      .builder()
      .config("spark.master", "local[3]")
      .config("spark.sql.shuffle.partitions", 5)
      .getOrCreate()
  }
}
