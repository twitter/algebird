package com.twitter.algebird

import org.scalatest._
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Gen, Arbitrary }

class EventuallyRingLaws extends CheckProperties {
  import BaseProperties._

  val eventuallyRing = new EventuallyRing[Long, Int](_.asInstanceOf[Long])(_ > 10000)
  val lGen = for (v <- Gen.choose(0L, 1L << 30L)) yield Left(v)
  val rGen = for (v <- Gen.choose(0, 10000)) yield Right(v)

  property("EventuallyRing is a Ring") {
    ringLaws[Either[Long, Int]](eventuallyRing, Arbitrary(Gen.oneOf(lGen, rGen)))
  }

}

// A lossy one:
class EventuallyHasAdditionOperatorAndZeroLaws extends CheckProperties {
  import BaseProperties._

  val eventuallyHasAdditionOperatorAndZero = new EventuallyHasAdditionOperatorAndZero[Int, String](_.length)(_.length > 100)
  val lGen = for (v <- Gen.choose(0, 1 << 14)) yield Left(v)
  val rGen = for (v <- Gen.alphaStr) yield Right(v)

  property("EventuallyHasAdditionOperatorAndZero is a HasAdditionOperatorAndZero") {
    monoidLaws[Either[Int, String]](eventuallyHasAdditionOperatorAndZero, Arbitrary(Gen.oneOf(lGen, rGen)))
  }

}

class EventuallyTest extends WordSpec with Matchers {

  val eventuallyHasAdditionOperatorAndZero = new EventuallyHasAdditionOperatorAndZero[Int, String](_.length)(_.length > 100)

  val short = "foo"
  val med = Stream.continually("bar").take(20).mkString("")
  val long = Stream.continually("bell").take(100).mkString("")

  // max batch is 1000
  val listOfRights = Stream.continually[Either[Int, String]](Right(short)).take(1010).toList
  val listOfLefts = Stream.continually[Either[Int, String]](Left(short.length)).take(1010).toList

  "EventuallyHasAdditionOperatorAndZero" should {

    "have the right zero" in {
      assert(eventuallyHasAdditionOperatorAndZero.zero == Right(""))
    }

    "add R to R" in {
      assert(eventuallyHasAdditionOperatorAndZero.plus(Right(short), Right(short)) == Right(short + short))
      assert(eventuallyHasAdditionOperatorAndZero.plus(Right(med), Right(med)) == Left(2 * med.length))
      assert(eventuallyHasAdditionOperatorAndZero.plus(Right(short), Right(long)) == Left(short.length + long.length))
    }

    "add L to R" in {
      assert(eventuallyHasAdditionOperatorAndZero.plus(Left(short.length), Right(short)) == Left(2 * short.length))
      assert(eventuallyHasAdditionOperatorAndZero.plus(Left(short.length), Right(long)) == Left(short.length + long.length))
    }

    "add R to L" in {
      assert(eventuallyHasAdditionOperatorAndZero.plus(Right(short), Left(short.length)) == Left(2 * short.length))
      assert(eventuallyHasAdditionOperatorAndZero.plus(Right(long), Left(short.length)) == Left(short.length + long.length))
    }

    "add L to L" in {
      assert(eventuallyHasAdditionOperatorAndZero.plus(Left(short.length), Left(short.length)) == Left(2 * short.length))
      assert(eventuallyHasAdditionOperatorAndZero.plus(Left(long.length), Left(short.length)) == Left(short.length + long.length))
    }

    "sumOption L,L,L" in {
      assert(eventuallyHasAdditionOperatorAndZero.sum(List(Left(short.length), Left(short.length), Left(short.length))) == Left(3 * short.length))
      assert(eventuallyHasAdditionOperatorAndZero.sum(List(Left(long.length), Left(short.length), Left(short.length))) == Left(2 * short.length + long.length))
    }

    "sumOption L,R,L" in {
      assert(eventuallyHasAdditionOperatorAndZero.sum(List(Left(short.length), Right(short), Left(short.length))) == Left(3 * short.length))
      assert(eventuallyHasAdditionOperatorAndZero.sum(List(Left(long.length), Right(short), Left(short.length))) == Left(2 * short.length + long.length))
    }

    "sumOption R,R,R" in {
      assert(eventuallyHasAdditionOperatorAndZero.sum(List(Right(short), Right(short), Right(short))) == Right(short + short + short))
      assert(eventuallyHasAdditionOperatorAndZero.sum(List(Right(long), Right(short), Right(short))) == Left(2 * short.length + long.length))
    }

    "sumOption 1010 R, L ,R" in {
      assert(eventuallyHasAdditionOperatorAndZero.sum(listOfRights :+ Left(short.length) :+ Right(short)) == Left(1012 * short.length))
    }

    "sumOption 1010 L, R ,L" in {
      assert(eventuallyHasAdditionOperatorAndZero.sum(listOfLefts :+ Right(short) :+ Left(short.length)) == Left(1012 * short.length))
    }

  }

}
