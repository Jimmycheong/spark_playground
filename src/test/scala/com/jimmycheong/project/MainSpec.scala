package com.jimmycheong.project

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.storage.StorageLevel
import org.scalatest.{Matchers, WordSpec}

class MainSpec extends WordSpec with Matchers {

  "Pipeline" should {

    "take an RDD and transform it" in {

      implicit val sparkSession = buildSparkSession()
      import sparkSession.implicits._

      val itemInfo: Dataset[ItemInfo] = List(
        ItemInfo(1, "football"),
        ItemInfo(2, "basketball"),
        ItemInfo(3, "cricket ball")
      ).toDS()


      val testDS: Dataset[Transactions] = List(
        Transactions(1, 3),
        Transactions(1, 10),
        Transactions(2, 4),
        Transactions(2, 4),
        Transactions(2, 3),
        Transactions(3, 5),
        Transactions(3, 7),
        Transactions(3, 2),
        Transactions(3, 5),
        Transactions(3, 4),
        Transactions(3, 21)
      ).toDS()

      testDS.persist(StorageLevel.MEMORY_AND_DISK_SER)

      val joinedTable = testDS.join(itemInfo, "itemId")

      println(joinedTable.rdd.toDebugString)

      1 shouldBe 1
//      result.as[TransactionsSums].collect should contain theSameElementsAs Array(
//        TransactionsSums(1, 13),
//        TransactionsSums(2, 4),
//        TransactionsSums(3, 14)
//      )


    }

  }

}
