package com.twitter.algebird

import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.collection.mutable.Queue
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object ScanTest {
  // normal people will use Scan not Scan.Aux, so it's good for most of the tests to be using the more common interface.
  type StringScan = Scan[Char, String]

  // technically speaking, these aren't exactly the Free scanner, since that would output a giant tree structure from
  // the whole scan, but that giant tree structure is pretty close to a String.

  val directFreeScan: Scan.Aux[Char, List[Char], String] = Scan.from(List.empty[Char]) {
    (char, previousState) =>
      val nextState = char :: previousState
      (nextState.reverse.mkString, nextState)
  }

  val mutableFreeScan: StringScan = Scan.mutable(new Queue[Char]()) { (char, previousState) =>
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
      .andThenPresent { case ((state, ()), input) => (input :: state).mkString.reverse }

  val joinWithPriorStateFreeScan2: StringScan =
    directFreeScan
      .andThenPresent(_ => ())
      .joinWithPriorState
      .join(Scan.identity[Char])
      .andThenPresent { case ((state, ()), input) => (input :: state).mkString.reverse }

}

class ScanTest extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {
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

  def zipLaws(scan1: StringScan, scan2: StringScan): Unit =
    forAll(Gen.listOf(Gen.alphaLowerChar), Gen.listOf(Gen.alphaLowerChar)) { (inputList1, inputList2) =>
      val outputList1 = scan1(inputList1)
      val outputList2 = scan2(inputList2)
      val zippedOutput = outputList1.zip(outputList2)

      val zippedScan = scan1.zip(scan2)
      val zippedInput = inputList1.zip(inputList2)
      val zippedScanOutput = zippedScan(zippedInput)

      (zippedOutput should contain).theSameElementsInOrderAs(zippedScanOutput)
    }

  def joinWithIndexLaws(freeScan: StringScan): Unit =
    forAll(Gen.listOf(Gen.alphaLowerChar)) { inputList =>
      val unIndexedOutput = freeScan(inputList)

      val joinedWithIndexOutput = freeScan.joinWithIndex(inputList)
      (unIndexedOutput.zipWithIndex should contain).theSameElementsInOrderAs(joinedWithIndexOutput)
    }

  "an illustrative example without scalacheck" should {
    "work as you'd expect" in {
      val output = directFreeScan(List('a', 'b', 'c'))
      (output should contain).theSameElementsInOrderAs(List("a", "ab", "abc"))
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

  "zipping aggregators" should {
    "obey its laws" in {
      zipLaws(directFreeScan, directFreeScan)
    }
  }

  "joinWithIndex" should {
    "obey its laws" in {
      joinWithIndexLaws(directFreeScan)
    }

    "replaceState" should {
      "behave as you'd expect" in {
        forAll(Gen.listOf(Gen.alphaLowerChar), Gen.listOf(Gen.alphaLowerChar)) { (inputList1, inputList2) =>
          // first we'll run the scan on inputList1 ++ inputList2, which will result in output1 ++ output2.
          // We should be able to replace the initial state of the scan such that just scanning only inputList2
          // will return output2.
          val (_, output2) = directFreeScan(inputList1 ++ inputList2).splitAt(inputList1.length)
          val stateOfScanAfterProcessingList1 = inputList1.reverse
          val scanAfterReplacingState = directFreeScan.replaceState(stateOfScanAfterProcessingList1)
          scanAfterReplacingState(inputList2) should equal(output2)
        }
      }

      "Scan.const" should {
        "behave as you'd expect" in {
          forAll(Gen.alphaLowerChar, Gen.listOf(Gen.alphaLowerChar)) { (const, inputList) =>
            (Scan.const(const)(inputList) should contain)
              .theSameElementsInOrderAs(List.fill(inputList.length)(const))
          }
        }
      }
    }
  }
}
