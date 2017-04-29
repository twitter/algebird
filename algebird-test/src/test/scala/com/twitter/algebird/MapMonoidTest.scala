package com.twitter.algebird

import org.scalacheck.{Arbitrary, Gen, Properties}
import org.scalacheck.Prop._

object MapMonoidTest extends Properties("MapMonoid") {
  import BaseProperties._
  import Monoid._

  property("MapAlgebra.sparseEquiv is correct") =
    forAll { entries: Set[Int] =>
      val map1 = entries.map { i => (i, i) }.toMap
      val map2 = map1.mapValues { -_ }
      val map3 = Monoid.plus(map1, map2)
      map3.size == map1.size && map3.size == map2.size && map3.filterNot { _._2 == 0 }.size == 0
    }
}
