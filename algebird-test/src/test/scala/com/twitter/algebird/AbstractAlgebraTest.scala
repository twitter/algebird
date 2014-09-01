package com.twitter.algebird

import org.scalatest._
import org.scalatest.{ DiagrammedAssertions, PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.Prop
import com.twitter.algebird.BaseProperties._
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import scala.Some

class AbstractAlgebraTest extends PropSpec with PropertyChecks with Matchers with DiagrammedAssertions {

  property("A Monoid should be able to sum") {
    val monoid = implicitly[Monoid[Int]]
    forAll { intList: List[Int] =>
      assert(intList.sum == monoid.sum(intList))
    }
  }

  property("A Ring should be able to product") {
    val ring = implicitly[Ring[Int]]
    forAll { intList: List[Int] =>
      assert(intList.product == ring.product(intList))
    }
  }

  property("An OptionMonoid should be able to sum") {
    val monoid = implicitly[Monoid[Option[Int]]]

    forAll { intList: List[Option[Int]] =>
      val flattenedList = intList.flatMap(x => x)
      val expectedResult = if (!flattenedList.isEmpty) Some(flattenedList.sum) else None
      assert(expectedResult == monoid.sum(intList))
    }
  }

  property("An OptionMonoid based on a Semigroup should be able to sum") {
    val maxMonoid = implicitly[Monoid[Option[Max[Int]]]]
    val minMonoid = implicitly[Monoid[Option[Min[Int]]]]
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

      assert(expectedMax == maxMonoid.sum(maxList))
      assert(expectedMin == minMonoid.sum(minList))

    }
  }

  property("First/Last should work properly") {
    val fsg = implicitly[Semigroup[First[Int]]]
    val lsg = implicitly[Semigroup[Last[Int]]]
    forAll { intList: List[Int] =>
      whenever(!intList.isEmpty) {
        val first = intList.map(First(_)).reduceLeft(fsg.plus _)
        val last = intList.map(Last(_)).reduceLeft(lsg.plus _)

        assert(first == First(intList.head))
        assert(last == Last(intList.last))
      }
    }
  }

  property("IndexedSeq should sum") {
    forAll { (lIndexedSeq: IndexedSeq[Int]) =>
      val rIndexedSeq = lIndexedSeq.map { _ => scala.util.Random.nextInt }
      whenever(lIndexedSeq.size == rIndexedSeq.size) {
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
        assert(Semigroup.plus(leftBase, rightBase) == sumBase)

        // when left is bigger
        val left = leftBase ++ remainder
        assert(Semigroup.plus(left, rightBase) == sumBase ++ remainder)

        // when right is bigger
        val right = rightBase ++ remainder
        assert(Semigroup.plus(leftBase, right) == sumBase ++ remainder)
      }
    }
  }

  property("a user-defined product monoid should work") {
    case class Metrics(count: Int, largestValue: Option[Max[Int]])
    implicit val MetricsMonoid = Monoid(Metrics.apply _, Metrics.unapply _)
    implicit val metricsGen = Arbitrary {
      for {
        count <- Gen.choose(0, 10000)
        largest <- Gen.oneOf[Option[Max[Int]]](None, Gen.choose(1, 100).map(n => Some(Max(n))))
      } yield Metrics(count, largest)
    }

    commutativeMonoidLaws[Metrics]
  }
}
