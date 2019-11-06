package com.twitter.algebird

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable.Queue


object ScanTest {
  // normal people will use Scan not Scan.Aux, so it's good for most of the tests to be using the more common interface.
  type StringScan = Scan[Char, String]

  // technically speaking, these aren't exactly the Free scanner, since that would output a giant tree structure from
  // the whole scan, but that giant tree structure is pretty close to a String.

  val directFreeScan: Scan.Aux[Char, List[Char], String] = Scan.from(List.empty[Char]) { (char, previousState) =>
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

  val joinWithPosteriorStateFreeScan: StringScan =
    directFreeScan
      .andThenPresent(_ => ())
      .joinWithPosteriorState
      .andThenPresent { case ((), state) => state.reverse.mkString }

  val joinWithPriorStateFreeScan1: StringScan =
    directFreeScan
    .andThenPresent(_ => ())
    .joinWithPriorState
    .joinWithInput
    .andThenPresent{ case (input, (state, ())) => (input::state).mkString.reverse}

  val joinWithPriorStateFreeScan2: StringScan =
    directFreeScan
    .andThenPresent(_ => ())
    .joinWithPriorState
    .join(Scan.identity[Char])
    .andThenPresent{ case ((state, ()) , input) => (input::state).mkString.reverse}

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

    "be obeyed by an implementation of the almost-free Scan using a direct implementation, andThenPresent, and joinWithPosteriorState" in {
      freeScanLaws(joinWithPosteriorStateFreeScan)
    }

    "be obeyed by an implementation of the almost-free Scan using a direct implmeentation, andThenPresent, joinWithPriorState, and joinWithInput" in {
      freeScanLaws(joinWithPriorStateFreeScan1)
    }

    "be obeyed by an implementation of the almost-free Scan using a direct implmeentation, andThenPresent, joinWithPriorState, and join with scan.Identity" in {
      freeScanLaws(joinWithPriorStateFreeScan2)
    }

    "be obeyed by composing the identity scan on either side of a direct-implementation of the almost-free Scan" in {
      freeScanLaws(Scan.identity.compose(directFreeScan))
      freeScanLaws(directFreeScan.compose(Scan.identity))
    }

  }

}
