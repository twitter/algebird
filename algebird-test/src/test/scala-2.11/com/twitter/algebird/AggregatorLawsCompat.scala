package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Prop._
import org.scalacheck.Prop
trait AggregatorLawsCompat { self: CheckProperties =>
  implicit def aggregator[A, B, C](implicit
      prepare: Arbitrary[A => B],
      sg: Semigroup[B],
      present: Arbitrary[B => C]
  ): Arbitrary[Aggregator[A, B, C]] = Arbitrary {
    for {
      pp <- prepare.arbitrary
      ps <- present.arbitrary
    } yield new Aggregator[A, B, C] {
      def prepare(a: A) = pp(a)
      def semigroup = sg
      def present(b: B) = ps(b)
    }
  }
  property("Applicative composing two Aggregators is correct") {
    forAll { (in: List[Int], ag1: Aggregator[Int, Set[Int], Int], ag2: Aggregator[Int, Unit, String]) =>
      type AggInt[T] = Aggregator[Int, _, T]
      val c = Applicative.join[AggInt, Int, String](ag1, ag2)
      in.isEmpty || c(in) == ((ag1(in), ag2(in)))
    }
  }
}
