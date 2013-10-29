package com.twitter.algebird

import org.specs2.mutable._
import org.scalacheck._
import org.specs2.ScalaCheck
import org.scalacheck.Prop
import com.twitter.algebird.BaseProperties._
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import scala.Some

class AbstractAlgebraTest extends Specification with ScalaCheck {

  "A Monoid should be able to sum" in {
    val monoid = implicitly[Monoid[Int]]
    val list = List(1,5,6,6,4,5)
    list.sum must be_==(monoid.sum(list))
  }
  "A Ring should be able to product" in {
    val ring = implicitly[Ring[Int]]
    val list = List(1,5,6,6,4,5)
    list.product must be_==(ring.product(list))
  }
  "An OptionMonoid should be able to sum" in {
    val monoid = implicitly[Monoid[Option[Int]]]
    val list = List(Some(1),None,Some(5),Some(-1),Some(7),Some(6))
    list.flatMap(x => x).sum must_== monoid.sum(list).get
  }
  "An OptionMonoid based on a Semigroup should be able to sum" in {
    val maxMonoid = implicitly[Monoid[Option[Max[Int]]]]
    val minMonoid = implicitly[Monoid[Option[Min[Int]]]]
    val list = List(Some(1),None,Some(5),Some(-1),Some(7),Some(6))
    val minList = list.map { _ match {
        case Some(x) => Some(Min(x))
        case None => None
      }
    }
    val maxList = list.map { _ match {
        case Some(x) => Some(Max(x))
        case None => None
      }
    }

    Some(Max(7)) must_== maxMonoid.sum(maxList)
    Some(Min(-1)) must_== minMonoid.sum(minList)
  }
  "First/Last should work properly" in {
    val fsg = implicitly[Semigroup[First[Int]]]
    val lsg = implicitly[Semigroup[Last[Int]]]
    (List(1,2,3,4,5).map { First(_) }).reduceLeft(fsg.plus _) must be_==(First(1))
    (List(1,2,3,4,5).map { Last(_) }).reduceLeft(lsg.plus _) must be_==(Last(5))
  }
  "IndexedSeq should sum" in {
    val leftBase = IndexedSeq(Max(1),Max(2),Max(3))
    val rightBase = IndexedSeq(Max(5),Max(1),Max(3))
    val sumBase = IndexedSeq(Max(5),Max(2),Max(3))
    val remainder = IndexedSeq(Max(-4))

    // equal sized summands
    Semigroup.plus(leftBase, rightBase) must_== sumBase

    // when left is bigger
    val left = leftBase ++ remainder
    Semigroup.plus(left, rightBase) must_== sumBase ++ remainder

    // when right is bigger
    val right = rightBase ++ remainder
    Semigroup.plus(leftBase, right) must_== sumBase ++ remainder
  }


  "a user-defined product monoid should work" in {

    case class Metrics(count: Int, largestValue: Option[Max[Int]])
    implicit val MetricsMonoid = Monoid(Metrics.apply _, Metrics.unapply _)
    implicit val metricsGen = Arbitrary {
        for {
          count <- Gen.choose(0,10000)
          largest <- Gen.oneOf(None, Gen.choose(1, 100).map(n => Some(Max(n))))
        } yield Metrics(count, largest)
      }

    Prop.all(commutativeMonoidLaws[Metrics])
  }
}
