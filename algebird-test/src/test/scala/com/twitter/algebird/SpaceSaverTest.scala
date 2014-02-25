package com.twitter.algebird

import org.specs2.mutable.Specification

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen
import org.scalacheck.Gen.{ frequency, choose, oneOf, containerOf1 }

object SpaceSaverLaws extends Properties("SpaceSaver") {
  import BaseProperties._

  implicit val ssSemigroup = new SpaceSaverSemigroup[Int]

  // limit sizes to 100 to avoid large data structures in tests
  property("SpaceSaver is a Semigroup") = forAll(choose(2, 100)){ capacity =>
    forAll(choose(1, 100)){ range =>

      // need a non-uniform distro
      implicit val ssGenOne: Arbitrary[SSOne[Int]] = Arbitrary {
        for (key <- frequency((1 to range).map{ x => (x * x, x : Gen[Int]) }: _*)) yield SpaceSaver(capacity, key).asInstanceOf[SSOne[Int]]
      }
      implicit val ssGen: Arbitrary[SpaceSaver[Int]] = Arbitrary {
        oneOf(
          arbitrary[SSOne[Int]],
          containerOf1[List, SSOne[Int]](arbitrary[SSOne[Int]]).map(_.reduce(ssSemigroup.plus))
        )
      }

      commutativeSemigroupLawsEq[SpaceSaver[Int]] { (left, right) =>(left consistentWith right) && (right consistentWith left) }
    }
  }
}

class SpaceSaverTest extends Specification {
  "SpaceSaver" should {
    "produce a top 20 with exact bounds" in {
      val gen = frequency((1 to 100).map{ x => (x * x, x: Gen[Int]) }: _*)
      val items = (1 to 1000).map{ x => gen.sample.get }
      val exactCounts = items.groupBy(identity).mapValues( _.size )

      // simulate a distributed system with 10 mappers and 1 reducer
      val sg = new SpaceSaverSemigroup[Int]
      items.grouped(10).map{ _
        .iterator
        .map(SpaceSaver(40, _))
        .reduce(sg.plus)
      }
        .reduce(sg.plus)
        .topK(20)
        .forall{ case (item, approx, guarantee) =>
          // println("item " + item + " : " + approx.min + " <= " + exactCounts(item) + " <= " + approx.max)
          approx ~ exactCounts(item)
        }
    }
  }
}
