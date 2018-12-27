package com.jimmycheong.project

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class RddTransformer {

  def countTransactionsById(transactions: Dataset[Transactions])(implicit sparkSession: SparkSession): DataFrame = {

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    transactions
      .toDF
      .groupBy("itemId")
      .sum("quantity")
        .select($"itemId", $"sum(quantity)" as "total")
      .withColumn("total", col("total").cast("long"))
  }
}
