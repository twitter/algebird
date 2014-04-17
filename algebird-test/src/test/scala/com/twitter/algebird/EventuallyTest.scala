package com.twitter.algebird

import org.scalacheck.{Arbitrary, Properties}
import org.scalacheck.Gen._
import org.specs2.mutable._

object EventuallyRingLaws extends Properties("EventuallyRing") {
  import BaseProperties._

  val eventuallyRing = new EventuallyRing[Long,Int](_.asInstanceOf[Long])(_ > 10000)
  val lGen = for (v <- choose(0L, 1L << 30L)) yield Left(v)
  val rGen = for (v <- choose(0, 10000)) yield Right(v)

  property("EventuallyRing is a Ring") = ringLaws[Either[Long,Int]](eventuallyRing, Arbitrary(oneOf(lGen, rGen)))

}

// A lossy one:
object EventuallyMonoidLaws extends Properties("EventuallyMonoid") {
  import BaseProperties._

  val eventuallyMonoid = new EventuallyMonoid[Int,String](_.length)(_.length > 100)
  val lGen = for (v <- choose(0, 1 << 14)) yield Left(v)
  val rGen = for (v <- alphaStr) yield Right(v)

  property("EventuallyMonoid is a Monoid") = monoidLaws[Either[Int,String]](eventuallyMonoid, Arbitrary(oneOf(lGen, rGen)))

}

class EventuallyTest extends Specification {

  val eventuallyMonoid = new EventuallyMonoid[Int,String](_.length)(_.length > 100)

  val short = "foo"
  val med = Stream.continually("bar").take(20).mkString("")
  val long = Stream.continually("bell").take(100).mkString("")

  "EventuallyMonoid" should {

    "have the right zero" in {
      eventuallyMonoid.zero must be_==(Right(""))
    }

    "add R to R" in {
      eventuallyMonoid.plus(Right(short), Right(short)) must be_==(Right(short + short))
      eventuallyMonoid.plus(Right(med), Right(med)) must be_==(Left(2 * med.length))
      eventuallyMonoid.plus(Right(short), Right(long)) must be_==(Left(short.length + long.length))
    }

    "add L to R" in {
      eventuallyMonoid.plus(Left(short.length), Right(short)) must be_==(Left(2 * short.length))
      eventuallyMonoid.plus(Left(short.length), Right(long)) must be_==(Left(short.length + long.length))
    }

    "add R to L" in {
      eventuallyMonoid.plus(Right(short), Left(short.length)) must be_==(Left(2 * short.length))
      eventuallyMonoid.plus(Right(long), Left(short.length)) must be_==(Left(short.length + long.length))
    }

    "add L to L" in {
      eventuallyMonoid.plus(Left(short.length), Left(short.length)) must be_==(Left(2 * short.length))
      eventuallyMonoid.plus(Left(long.length), Left(short.length)) must be_==(Left(short.length + long.length))
    }

    "sumOption L,L,L" in {
      eventuallyMonoid.sum(List(Left(short.length), Left(short.length), Left(short.length))) must be_==(Left(3 * short.length))
      eventuallyMonoid.sum(List(Left(long.length), Left(short.length), Left(short.length))) must be_==(Left(2 * short.length + long.length))
    }

    "sumOption L,R,L" in {
      eventuallyMonoid.sum(List(Left(short.length), Right(short), Left(short.length))) must be_==(Left(3 * short.length))
      eventuallyMonoid.sum(List(Left(long.length), Right(short), Left(short.length))) must be_==(Left(2 * short.length + long.length))
    }

    "sumOption R,R,R" in {
      eventuallyMonoid.sum(List(Right(short), Right(short), Right(short))) must be_==(Right(short + short + short))
      eventuallyMonoid.sum(List(Right(long), Right(short), Right(short))) must be_==(Left(2 * short.length + long.length))
    }

  }

}
