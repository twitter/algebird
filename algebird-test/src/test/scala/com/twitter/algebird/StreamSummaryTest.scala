package com.twitter.algebird

import org.specs2.mutable.Specification

import org.scalacheck.Arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen
import org.scalacheck.Gen.frequency

object StreamSummaryLaws extends Properties("StreamSummary") {
  import BaseProperties._

  implicit val ssMonoid = new StreamSummaryMonoid
  // need a non-uniform distro
  implicit val ssGen: Arbitrary[com.twitter.algebird.StreamSummary[Int]] = Arbitrary {
    for (key: Int <- frequency((1 to 10).map{ x => (x * x, x : Gen[Int]) }: _*)) yield StreamSummary(4, key)
  }

  property("StreamSummary is a Monoid") = commutativeMonoidLawsEq[StreamSummary[Int]] { (left, right) => left consistentWith right }
}

class StreamSummaryTest extends Specification {
  "StreamSummary" should {
    "produce a top 20 with exact bounds" in {
      val gen = Gen.frequency((1 to 100).map{ x => (x * x, x: Gen[Int]) }: _*)
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
