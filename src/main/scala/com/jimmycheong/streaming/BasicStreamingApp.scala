package com.jimmycheong.streaming

import org.apache.spark.sql.DataFrame

object BasicStreamingApp extends App {

  val sparkSession = buildSparkSession()


  val activityDataPath = "" // ENTER PATH

  val static = sparkSession.read.json(activityDataPath)
  val dataSchema = static.schema

  val streaming: DataFrame =
    sparkSession
    .readStream
    .schema(dataSchema)
    .option("maxFilesPerTrigger", 1)
    .json(activityDataPath)

  val activityCounts = streaming.groupBy("gt").count()

  val writeTableName = "activity_counts"

  val activityQuery =
    activityCounts
      .writeStream
      .queryName(writeTableName)
      .format("memory")
      .outputMode("complete")
      .start()

  for (_ <- 1 to 10) {
    sparkSession.sql(s"SELECT * FROM $writeTableName").show()
    Thread.sleep(1000)
  }

  activityQuery.awaitTermination()

}
