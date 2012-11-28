package com.twitter.algebird

import org.specs._

class AbstractAlgebraTest extends Specification {
  noDetailedDiffs()
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
  "A MaxMonoid should find max" in {
    val monoid = new MaxMonoid[Int]
    val list = List(Some(1),None,Some(5),Some(-1),Some(7),Some(6))
    Some(7) must_== monoid.sum(list)
  }
  "A MinMonoid should find min" in {
    val monoid = new MinMonoid[Int]
    val list = List(Some(1),None,Some(5),Some(-1),Some(7),Some(6))
    Some(-1) must_== monoid.sum(list)
  }
  "A MaxMonoid should find max" in {
    val monoid = new MaxMonoid[Int]
    val list = List(None, None, None)
    None must_== monoid.sum(list)
  }
}
