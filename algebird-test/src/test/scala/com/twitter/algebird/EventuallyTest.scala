package com.twitter.algebird

import org.scalatest._
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Gen, Arbitrary, Prop }

class EventuallyRingLaws extends CheckProperties {
  import BaseProperties._

  val eventuallyRing = new EventuallyRing[Long, Int](_.toLong)(_ > 10000)
  val lGen = for (v <- Gen.choose(0L, 1L << 30L)) yield Left(v)
  val rGen = for (v <- Gen.choose(0, 10000)) yield Right(v)

  property("EventuallyRing is a Ring") {
    ringLaws[Either[Long, Int]](eventuallyRing, Arbitrary(Gen.oneOf(lGen, rGen)))
  }

  property("EventuallyRing is a Ring when we always convert") {
    // This is only lawful if we compare the converted space in general.
    // in practice, for many types this is not needed. Check the laws
    // for your instance to be sure
    val equiv: Equiv[Either[Long, Int]] = Equiv.fromFunction[Either[Long, Int]] {
      case (Right(a), Right(b)) => a == b
      case (Left(a), Left(b)) => a == b
      case (Right(a), Left(b)) => (a.toLong == b)
      case (Left(a), Right(b)) => (a == (b.toLong))
    }
    //ringLaws[Either[Long, Int]](eventuallyRing2, Arbitrary(Gen.oneOf(lGen, rGen)))
    Prop.forAll { (pred: Int => Boolean) =>
      val eventuallyRing2 = new EventuallyRing[Long, Int](_.toLong)(pred)
      // TODO add a ringLawsEquiv
      monoidLawsEquiv[Either[Long, Int]](eventuallyRing2, Arbitrary(Gen.oneOf(lGen, rGen)), equiv)
    }
  }

}

// A lossy one:
class EventuallyMonoidLaws extends CheckProperties {
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

  def eventuallyAggregator(rightAg: Aggregator[Int, List[Int], Int])(pred: (List[Int] => Boolean)): EventuallyAggregator[Int, Double, List[Int], String] = {
    new EventuallyAggregator[Int, Double, List[Int], String] {
      def presentLeft(e: Double) = "Left"

      def convert(o: List[Int]) = o.sum.toDouble
      def mustConvert(o: List[Int]) = pred(o)

      val leftSemigroup = Semigroup.doubleSemigroup
      def rightAggregator = rightAg.andThenPresent{ _ => "Right" }
    }
  }

  property("EventuallyAggregator converts correctly") {
    /**
     * Note, not all middle functions with all mustConvert are lawful.
     * here we are forcing a structure such that a + b >= a, b for the middle type,
     * and the mustConvert is a threshold on some projection of that middle type.
     * You should check the laws for each type you care about.
     *
     * For HLL/Set, which is the common example, this is lawful.
     */
    forAll{ (in: List[Int], thresh: Int, rightAg: Aggregator[Int, List[Int], Int]) =>
      val pred = { x: List[Int] => x.lengthCompare(thresh) > 0 }
      val eventuallyAg = eventuallyAggregator(rightAg)(pred)
      val semi = eventuallyAg.semigroup

      /**
       * Here we just manually implement the eventually logic
       */
      def lift(c: List[Int]) = if (pred(c)) Left(c.sum.toDouble) else Right(c)
      val manual = in.map(rightAg.prepare) match {
        case Nil => None
        case h :: tail =>
          val middle = tail.foldLeft(Right(h): Either[Double, List[Int]]) {
            case (Right(a), b) =>
              lift(rightAg.semigroup.plus(a, b))
            case (Left(a), b) => Left(a + (b.sum.toDouble))
          }
          Some(middle match {
            case Right(_) => "Right"
            case Left(_) => "Left"
          })
      }

      assert(eventuallyAg.applyOption(in) == manual)
    }
  }

}
