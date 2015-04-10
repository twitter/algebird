package com.twitter.algebird

import org.scalatest._
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Gen, Arbitrary }

class EventuallyRingLaws extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._

  val eventuallyRing = new EventuallyRing[Long, Int](_.asInstanceOf[Long])(_ > 10000)
  val lGen = for (v <- Gen.choose(0L, 1L << 30L)) yield Left(v)
  val rGen = for (v <- Gen.choose(0, 10000)) yield Right(v)

  property("EventuallyRing is a Ring") {
    ringLaws[Either[Long, Int]](eventuallyRing, Arbitrary(Gen.oneOf(lGen, rGen)))
  }

}

// A lossy one:
class EventuallyMonoidLaws extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._

  val eventuallyMonoid = new EventuallyMonoid[Int, String](_.length)(_.length > 100)
  val lGen = for (v <- Gen.choose(0, 1 << 14)) yield Left(v)
  val rGen = for (v <- Gen.alphaStr) yield Right(v)

  property("EventuallyMonoid is a Monoid") {
    monoidLaws[Either[Int, String]](eventuallyMonoid, Arbitrary(Gen.oneOf(lGen, rGen)))
  }

}

class EventuallyTest extends WordSpec with Matchers {

  val eventuallyMonoid = new EventuallyMonoid[Int, String](_.length)(_.length > 100)

  val short = "foo"
  val med = Stream.continually("bar").take(20).mkString("")
  val long = Stream.continually("bell").take(100).mkString("")

  // max batch is 1000
  val listOfRights = Stream.continually[Either[Int, String]](Right(short)).take(1010).toList
  val listOfLefts = Stream.continually[Either[Int, String]](Left(short.length)).take(1010).toList

  "EventuallyMonoid" should {

    "have the right zero" in {
      assert(eventuallyMonoid.zero == Right(""))
    }

    "add R to R" in {
      assert(eventuallyMonoid.plus(Right(short), Right(short)) == Right(short + short))
      assert(eventuallyMonoid.plus(Right(med), Right(med)) == Left(2 * med.length))
      assert(eventuallyMonoid.plus(Right(short), Right(long)) == Left(short.length + long.length))
    }

    "add L to R" in {
      assert(eventuallyMonoid.plus(Left(short.length), Right(short)) == Left(2 * short.length))
      assert(eventuallyMonoid.plus(Left(short.length), Right(long)) == Left(short.length + long.length))
    }

    "add R to L" in {
      assert(eventuallyMonoid.plus(Right(short), Left(short.length)) == Left(2 * short.length))
      assert(eventuallyMonoid.plus(Right(long), Left(short.length)) == Left(short.length + long.length))
    }

    "add L to L" in {
      assert(eventuallyMonoid.plus(Left(short.length), Left(short.length)) == Left(2 * short.length))
      assert(eventuallyMonoid.plus(Left(long.length), Left(short.length)) == Left(short.length + long.length))
    }

    "sumOption L,L,L" in {
      assert(eventuallyMonoid.sum(List(Left(short.length), Left(short.length), Left(short.length))) == Left(3 * short.length))
      assert(eventuallyMonoid.sum(List(Left(long.length), Left(short.length), Left(short.length))) == Left(2 * short.length + long.length))
    }

    "sumOption L,R,L" in {
      assert(eventuallyMonoid.sum(List(Left(short.length), Right(short), Left(short.length))) == Left(3 * short.length))
      assert(eventuallyMonoid.sum(List(Left(long.length), Right(short), Left(short.length))) == Left(2 * short.length + long.length))
    }

    "sumOption R,R,R" in {
      assert(eventuallyMonoid.sum(List(Right(short), Right(short), Right(short))) == Right(short + short + short))
      assert(eventuallyMonoid.sum(List(Right(long), Right(short), Right(short))) == Left(2 * short.length + long.length))
    }

    "sumOption 1010 R, L ,R" in {
      assert(eventuallyMonoid.sum(listOfRights :+ Left(short.length) :+ Right(short)) == Left(1012 * short.length))
    }

    "sumOption 1010 L, R ,L" in {
      assert(eventuallyMonoid.sum(listOfLefts :+ Right(short) :+ Left(short.length)) == Left(1012 * short.length))
    }

  }

}

class EventuallyAggregatorLaws extends PropSpec with PropertyChecks with Matchers {
  implicit def aggregator[A, B, C](implicit prepare: Arbitrary[A => B],
    sg: Semigroup[B],
    present: Arbitrary[B => C]): Arbitrary[Aggregator[A, B, C]] = Arbitrary {
    for {
      pp <- prepare.arbitrary
      ps <- present.arbitrary
    } yield new Aggregator[A, B, C] {
      def prepare(a: A) = pp(a)
      def semigroup = sg
      def present(b: B) = ps(b)
    }
  }

  def eventuallyAggregator(rightAg: Aggregator[Int, Int, Int])(pred: (Int => Boolean)): EventuallyAggregator[Int, Double, Int, String] = {
    new EventuallyAggregator[Int, Double, Int, String] {
      def presentLeft(e: Double) = "Left"

      def convert(o: Int) = o.toDouble
      def mustConvert(o: Int) = pred(o)

      val leftSemigroup = Semigroup.doubleSemigroup
      def rightAggregator = rightAg.andThenPresent{ _ => "Right" }
    }
  }

  def isConvertedCorrectly(s: String, semi: EventuallySemigroup[Double, Int], in: List[Right[Double, Int]]) = {
    val isFromLeft = s == "Left"
    val shouldBeFromLeft = semi.sumOption(in).get.isLeft

    if (shouldBeFromLeft) isFromLeft else !isFromLeft
  }

  property("EventuallyAggregator converts correctly") {
    forAll{ (in: List[Int], pred: (Int => Boolean), rightAg: Aggregator[Int, Int, Int]) =>
      val eventuallyAg = eventuallyAggregator(rightAg)(pred)
      val semi = eventuallyAg.semigroup
      val rightIn = in.map { Right(_) }

      assert(in.isEmpty || isConvertedCorrectly(eventuallyAg(in), semi, rightIn))
    }
  }

}
