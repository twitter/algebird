package com.twitter.algebird

import org.specs2.mutable.Specification

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen
import org.scalacheck.Gen.{ frequency, choose }

object StreamSummaryLaws extends Properties("StreamSummary") {
  import BaseProperties._

  implicit val ssMonoid = new StreamSummaryMonoid[Int]

  // limit sizes to 100 to avoid large data structures in tests
  property("StreamSummary is a Monoid") = forAll(choose(2, 100)){ capacity =>
    forAll(choose(1, 100)){ range =>

      // need a non-uniform distro
      implicit val ssGen: Arbitrary[com.twitter.algebird.StreamSummary[Int]] = Arbitrary {
        for (key <- frequency((1 to range).map{ x => (x * x, x : Gen[Int]) }: _*)) yield StreamSummary(capacity, key)
      }

      commutativeMonoidLawsEq[StreamSummary[Int]] { (left, right) =>(left consistentWith right) && (right consistentWith left) }
    }
  }
}

class StreamSummaryTest extends Specification {
  "StreamSummary" should {
    "produce a top 20 with exact bounds" in {
      val gen = frequency((1 to 100).map{ x => (x * x, x: Gen[Int]) }: _*)
      val items = (1 to 1000).map{ x => gen.sample.get }
      val exactCounts = items.groupBy(identity).mapValues( _.size )

      // simulate a distributed system with 10 mappers and 1 reducer
      val m = new StreamSummaryMonoid[Int]
      items.grouped(10).map{ _
        .iterator
        .map{ StreamSummary(40, _) }
        .fold(m.zero)(m.plus(_, _))
      }
        .fold(m.zero)(m.plus(_, _))
        .topK(20)
        .forall{ case (item, approx, guarantee) =>
          // println("item " + item + " : " + approx.min + " <= " + exactCounts(item) + " <= " + approx.max)
          approx ~ exactCounts(item)
        }
    }
  }
}
