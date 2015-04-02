package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import org.scalacheck.{ Arbitrary, Gen }
import org.scalatest.Matchers
import org.scalacheck.Prop._

class AbstractAlgebraTest extends CheckProperties with Matchers {

  property("A HasAdditionOperatorAndZero should be able to sum") {
    val monoid = implicitly[HasAdditionOperatorAndZero[Int]]
    forAll { intList: List[Int] =>
      intList.sum == monoid.sum(intList)
    }
  }

  property("A Ring should be able to product") {
    val ring = implicitly[Ring[Int]]
    forAll { intList: List[Int] =>
      intList.product == ring.product(intList)
    }
  }

  property("An OptionHasAdditionOperatorAndZero should be able to sum") {
    val monoid = implicitly[HasAdditionOperatorAndZero[Option[Int]]]

    forAll { intList: List[Option[Int]] =>
      val flattenedList = intList.flatMap(x => x)
      val expectedResult = if (!flattenedList.isEmpty) Some(flattenedList.sum) else None
      expectedResult == monoid.sum(intList)
    }
  }

  property("An OptionHasAdditionOperatorAndZero based on a HasAdditionOperator should be able to sum") {
    val maxHasAdditionOperatorAndZero = implicitly[HasAdditionOperatorAndZero[Option[Max[Int]]]]
    val minHasAdditionOperatorAndZero = implicitly[HasAdditionOperatorAndZero[Option[Min[Int]]]]
    forAll { intList: List[Option[Int]] =>
      val minList = intList.map {
        _ match {
          case Some(x) => Some(Min(x))
          case None => None
        }
      }
      val maxList = intList.map {
        _ match {
          case Some(x) => Some(Max(x))
          case None => None
        }
      }

      val flattenedList = intList.flatMap(x => x)
      val expectedMax = if (!flattenedList.isEmpty) Some(Max(flattenedList.max)) else None
      val expectedMin = if (!flattenedList.isEmpty) Some(Min(flattenedList.min)) else None

      expectedMax == maxHasAdditionOperatorAndZero.sum(maxList) &&
        expectedMin == minHasAdditionOperatorAndZero.sum(minList)

    }
  }

  property("First/Last should work properly") {
    val fsg = implicitly[HasAdditionOperator[First[Int]]]
    val lsg = implicitly[HasAdditionOperator[Last[Int]]]
    forAll { intList: List[Int] =>
      !intList.isEmpty ==> {
        val first = intList.map(First(_)).reduceLeft(fsg.plus _)
        val last = intList.map(Last(_)).reduceLeft(lsg.plus _)

        first == First(intList.head) &&
          last == Last(intList.last)
      }
    }
  }

  property("IndexedSeq should sum") {
    forAll { (lIndexedSeq: IndexedSeq[Int]) =>
      val rIndexedSeq = lIndexedSeq.map { _ => scala.util.Random.nextInt }
      (lIndexedSeq.size == rIndexedSeq.size) ==> {
        val leftBase = lIndexedSeq.map(Max(_))
        val rightBase = rIndexedSeq.map(Max(_))
        val sumBase = (0 until lIndexedSeq.size).map { idx =>
          if (lIndexedSeq(idx) > rIndexedSeq(idx))
            Max(lIndexedSeq(idx))
          else
            Max(rIndexedSeq(idx))
        }
        val remainder = IndexedSeq(Max(-4))

        // equal sized summands

        // when left is bigger
        val left = leftBase ++ remainder

        // when right is bigger
        val right = rightBase ++ remainder

        HasAdditionOperator.plus(left, rightBase) == sumBase ++ remainder &&
          HasAdditionOperator.plus(leftBase, rightBase) == sumBase &&
          HasAdditionOperator.plus(leftBase, right) == sumBase ++ remainder
      }
    }
  }

  property("An ArrayHasAdditionOperatorAndZero should sum") {
    val monoid = new ArrayHasAdditionOperatorAndZero[Int]
    forAll { intList: List[Int] =>
      val (l, r) = intList.splitAt(intList.size / 2)
      val left = l.padTo(math.max(l.size, r.size), 0)
      val right = r.padTo(math.max(l.size, r.size), 0)

      (left, right).zipped.map(_ + _).toArray.deep == monoid.sum(List(l.toArray, r.toArray)).deep
    }
  }

  property("An ArrayGroup should negate") {
    val arrayGroup = new ArrayGroup[Int]
    forAll { intList: List[Int] =>
      intList.map(-1 * _).toArray.deep == arrayGroup.negate(intList.toArray).deep
    }
  }

  property("a user-defined product monoid should work") {
    case class Metrics(count: Int, largestValue: Option[Max[Int]])
    implicit val MetricsHasAdditionOperatorAndZero = HasAdditionOperatorAndZero(Metrics.apply _, Metrics.unapply _)
    implicit val metricsGen = Arbitrary {
      for {
        count <- Gen.choose(0, 10000)
        largest <- Gen.oneOf[Option[Max[Int]]](None, Gen.choose(1, 100).map(n => Some(Max(n))))
      } yield Metrics(count, largest)
    }

    commutativeHasAdditionOperatorAndZeroLaws[Metrics]
  }
}
