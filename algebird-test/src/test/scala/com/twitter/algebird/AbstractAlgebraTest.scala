package com.twitter.algebird

import org.specs._

class AbstractAlgebraTest extends Specification {
  noDetailedDiffs()
  "A Monoid should be able to sum" in {
    val list = List(1,5,6,6,4,5)
    list.sum must be_==(Monoid.sum[Int](list))
  }
  "A Ring should be able to product" in {
    val list = List(1,5,6,6,4,5)
    list.product must be_==(Ring.product[Int](list))
  }
  "An OptionMonoid should be able to sum" in {
    val list = List(Some(1),None,Some(5),Some(-1),Some(7),Some(6))
    list.flatMap(x => x).sum must_== Monoid.sum[Option[Int]](list).get
  }
  "An OptionMonoid based on a Semigroup should be able to sum" in {
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

    Some(Max(7)) must_== Monoid.sum[Option[Max[Int]]](maxList)
    Some(Min(-1)) must_== Monoid.sum[Option[Min[Int]]](minList)
  }
  "First/Last should work properly" in {
    (List(1,2,3,4,5).map { First(_) }).reduceLeft(Semigroup.plus[First[Int]](_, _)) must be_==(First(1))
    (List(1,2,3,4,5).map { Last(_) }).reduceLeft(Semigroup.plus[Last[Int]](_, _)) must be_==(Last(5))
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

}
