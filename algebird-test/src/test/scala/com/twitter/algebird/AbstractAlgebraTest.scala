package com.twitter.algebird

import com.twitter.algebird.BaseProperties._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.Matchers
import org.scalacheck.Prop._

class AbstractAlgebraTest extends CheckProperties with Matchers {

  property("A Monoid should be able to sum") {
    val monoid = implicitly[Monoid[Int]]
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

  property("An OptionMonoid should be able to sum") {
    val monoid = implicitly[Monoid[Option[Int]]]

    forAll { intList: List[Option[Int]] =>
      val flattenedList = intList.flatMap(x => x)
      val expectedResult =
        if (!flattenedList.isEmpty) Some(flattenedList.sum) else None
      expectedResult == monoid.sum(intList)
    }
  }

  property("An OptionMonoid based on a Semigroup should be able to sum") {
    val maxMonoid = implicitly[Monoid[Option[Max[Int]]]]
    val minMonoid = implicitly[Monoid[Option[Min[Int]]]]
    forAll { intList: List[Option[Int]] =>
      val minList = intList.map {
        _ match {
          case Some(x) => Some(Min(x))
          case None    => None
        }
      }
      val maxList = intList.map {
        _ match {
          case Some(x) => Some(Max(x))
          case None    => None
        }
      }

      val flattenedList = intList.flatMap(x => x)
      val expectedMax =
        if (!flattenedList.isEmpty) Some(Max(flattenedList.max)) else None
      val expectedMin =
        if (!flattenedList.isEmpty) Some(Min(flattenedList.min)) else None

      expectedMax == maxMonoid.sum(maxList) &&
      expectedMin == minMonoid.sum(minList)

    }
  }

  property("IndexedSeq should sum") {
    forAll { (lIndexedSeq: IndexedSeq[Int]) =>
      val rIndexedSeq = lIndexedSeq.map { _ =>
        scala.util.Random.nextInt
      }
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

        Semigroup.plus(left, rightBase) == sumBase ++ remainder &&
        Semigroup.plus(leftBase, rightBase) == sumBase &&
        Semigroup.plus(leftBase, right) == sumBase ++ remainder
      }
    }
  }

  property("An ArrayMonoid should sum") {
    val monoid = new ArrayMonoid[Int]
    forAll { intList: List[Int] =>
      val (l, r) = intList.splitAt(intList.size / 2)
      val left = l.padTo(math.max(l.size, r.size), 0)
      val right = r.padTo(math.max(l.size, r.size), 0)

      (left, right).zipped.map(_ + _).toSeq == monoid
        .sum(List(l.toArray, r.toArray))
        .toSeq
    }
  }

  property("An ArrayGroup should negate") {
    val arrayGroup = new ArrayGroup[Int]
    forAll { intList: List[Int] =>
      intList.map(-1 * _).toSeq == arrayGroup
        .negate(intList.toArray)
        .toSeq
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
