package com.jimmycheong.project

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

case class Item(ref: Long, quantity: Int)

class PersistanceSpec extends FlatSpec with Matchers with PersistanceFixture {

  "persist" should "improve efficiency" in {

    val sparkSession = buildSparkSession()
    import sparkSession.implicits._

    val lowerBound = 1
    val upperBound = 50
    val numOfRows = 5000000
    val itemInfoDs = createItemInfo(lowerBound, upperBound).toDS()

    val randomList = createRandomListOfInt(lowerBound, upperBound, numOfRows)
    val itemsDs = createItems(randomList).toDS().persist

    val startTime = System.currentTimeMillis()

    val stage_1 = itemsDs.join(itemInfoDs, $"ref" === $"itemId")
    val stage_2 = itemsDs.join(itemInfoDs, $"ref" === $"itemId")
    val stage_3 = itemsDs.join(itemInfoDs, $"ref" === $"itemId")

    val duration = (System.currentTimeMillis() - startTime) / 1000.0

    println(s"Operation took: $duration seconds")


  }
}

trait PersistanceFixture {

  def createItems(numList: List[Int]): List[Item] = numList.map(Item(_, 0))

  def createRandomListOfInt(lowerBound: Int, upperBound: Int, size: Int): List[Int] = {
    for {
      elem <- List.fill(size)(grabRandomNumber(lowerBound, upperBound))
    } yield elem
  }

  def grabRandomNumber(lower: Int, upper: Int): Int = {
    lower + Random.nextInt(upper)
  }

  def createItemInfo(lowerBound: Int, upperBound: Int): List[ItemInfo] = {
     for {
      id <- List.range(lowerBound, upperBound)
      description <- List.fill(upperBound)(Random.nextString(5))
    } yield ItemInfo(id, description)
  }

}