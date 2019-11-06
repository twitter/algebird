package com.twitter.algebird

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable.Queue


object ScanTest {
  type StringScan = Scan[Char, String]

  // technically speaking, these aren't exactly the Free scanner, since that would output a giant tree structure from
  // the whole scan, but that giant tree structure is pretty close to a String.
  val directFreeScan: StringScan = Scan.from(List.empty[Char]) { (char, previousState) =>
    val nextState = char :: previousState
    (nextState.reverse.mkString, nextState)
  }

  val mutableFreeScan: StringScan = Scan.mutable(new Queue[Char]()) {
    (char, previousState) =>
      previousState.enqueue(char)
      previousState.mkString
  }

  val aggregatorFreeScan: StringScan = {
    val aggregator = Aggregator.fromMonoid[List[Char]]

    Scan
      .fromMonoidAggregator(aggregator)
      .composePrepare[Char](c => List(c))
      .andThenPresent(_.mkString)

  }

  val reverseAggregatorFreeScan: StringScan = {
    val aggregator = Aggregator.fromMonoid[List[Char]]

    Scan
      .fromMonoidAggregatorReverse(aggregator)
      .composePrepare[Char](c => List(c))
      .andThenPresent(_.reverse.mkString)

  }


}

class ScanTest extends WordSpec with Matchers with GeneratorDrivenPropertyChecks {
  import ScanTest._

  def freeScanLaws(freeScan: StringScan): Unit =
    forAll(Gen.listOf(Gen.alphaLowerChar)) { inputList =>
      val outputList = freeScan(inputList)

      outputList.length should equal(inputList.length)
      outputList.zipWithIndex
        .foreach {
          case (ithOutput, i) =>
            val expectedOutput = inputList.slice(0, i + 1).mkString
            ithOutput should equal(expectedOutput)
        }
    }

  "freeAggreator laws" should {
    "be obeyed by a direct implementation of the almost-free Scan" in {
      freeScanLaws(directFreeScan)
    }

    "be obeyed by a mutable implementation of the almost-free Scan" in {
      freeScanLaws(mutableFreeScan)
    }

    "be obeyed by an implementation of the almost-free Scan using fromAggregator, composePrepare, and andThenPresent" in {
      freeScanLaws(aggregatorFreeScan)
    }

    "be obeyed by an implementation of the almost-free Scan using fromReverseAggregator, composePrepare, and andThenPresent" in {
      freeScanLaws(reverseAggregatorFreeScan)
    }


  }

}
