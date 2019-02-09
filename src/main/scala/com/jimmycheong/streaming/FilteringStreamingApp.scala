package com.jimmycheong.streaming

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

object FilteringStreamingApp extends App {

  val sparkSession = buildSparkSession()

  val activityDataPath = "/Users/kch31/Development/Spark-The-Definitive-Guide/data/activity-data" // ENTER PATH

  val static = sparkSession.read.json(activityDataPath)
  val dataSchema = static.schema

  val writeTableName = "simple_transform"

  val streaming: DataFrame =
    sparkSession
      .readStream
      .schema(dataSchema)
      .option("maxFilesPerTrigger", 1)
      .json(activityDataPath)

  val simpleTransform =
    streaming
      .withColumn("stairs", expr("gt like '%stairs%'"))
      .where("stairs")
      .where("gt is not null")
      .select("gt", "model", "arrival_time", "creation_time")
      .writeStream
      .queryName("simple_transform")
      .format("memory")
      .outputMode("append") // Using append output mode this time
      .start()

  for (_ <- 1 to 10) {
    sparkSession.sql(s"SELECT * FROM $writeTableName").show()
    Thread.sleep(1000)
  }

  simpleTransform.awaitTermination()


}
