package com.twitter.algebird.generic

import com.twitter.algebird.{Monoid, Semigroup}

import shapeless._
import syntax.std.tuple._

import com.twitter.algebird._

import org.scalacheck._, Arbitrary.arbitrary
import org.scalatest._
import org.scalatest.prop._

import Shapeless._

object Check {

  implicit def arbitraryAggregator[X: Cogen, Y: Arbitrary: Cogen: Monoid, Z: Arbitrary]
    : Arbitrary[MonoidAggregator[X, Y, Z]] =
    Arbitrary(
      for {
        f <- arbitrary[X => Y]
        g <- arbitrary[Y => Z]
      } yield
        Aggregator
          .fromMonoid[Y]
          .composePrepare(f)
          .andThenPresent(g))
}

import Check._

abstract class ParallelAggregatorTest[
    A1: Arbitrary: Cogen,
    B1: Arbitrary: Cogen: Monoid,
    C1: Arbitrary,
    A2: Arbitrary: Cogen,
    B2: Arbitrary: Cogen: Monoid,
    C2: Arbitrary
](name: String)
    extends PropSpec
    with Matchers
    with PropertyChecks {

  property(s"$name: parallel aggregator product (allOf)") {
    forAll {
      (a1: MonoidAggregator[A1, B1, C1], a2: MonoidAggregator[A2, B2, C2], axs: List[(A1, A2)]) =>
        val a3 = ParallelAggregators.allOf(a1 :: a2 :: HNil)
        a3(axs.map(_.productElements)) shouldBe (a1 zip a2)(axs).productElements
    }
  }

  property(s"$name: parallel aggregator coproduct (oneOf)") {
    def f(ax: Either[A1, A2]): A1 :+: A2 :+: CNil =
      ax match {
        case Left(a)  => Inl(a)
        case Right(x) => Inr(Inl(x))
      }
    forAll {
      (a1: MonoidAggregator[A1, B1, C1],
       a2: MonoidAggregator[A2, B2, C2],
       axs: List[Either[A1, A2]]) =>
        val a3 = ParallelAggregators.oneOf(a1 :: a2 :: HNil)
        a3(axs.iterator.map(f)) shouldBe (a1 either a2)(axs).productElements
    }
  }
}

class ParallelAggregator_IIISSS
    extends ParallelAggregatorTest[Int, Int, Int, String, String, String]("IIISSS")
class ParallelAggregator_IBSLBS
    extends ParallelAggregatorTest[Int, BigInt, String, Long, BigInt, String]("IBSLBS")

abstract class ApplicativeAggregatorTest[
    A: Arbitrary: Cogen,
    B1: Arbitrary: Cogen: Monoid,
    C1: Arbitrary,
    B2: Arbitrary: Cogen: Monoid,
    C2: Arbitrary
](name: String)
    extends PropSpec
    with Matchers
    with PropertyChecks {

  property(s"$name: applicative aggregator") {
    forAll { (a1: MonoidAggregator[A, B1, C1], a2: MonoidAggregator[A, B2, C2], as: List[A]) =>
      val a3 = ApplicativeAggregators(a1 :: a2 :: HNil)

      val expected =
        if (as.nonEmpty) (a1 join a2)(as).productElements
        else a1.present(Monoid.zero[B1]) :: a2.present(Monoid.zero[B2]) :: HNil

      a3(as) shouldBe expected
    }
  }
}

class ApplicativeAggregator_IBSLL
    extends ApplicativeAggregatorTest[Int, BigInt, String, Long, Long]("IBSLL")

abstract class JoinedAggregatorTest[
    A1: Arbitrary: Cogen,
    B1: Arbitrary: Cogen: Monoid,
    A2: Arbitrary: Cogen,
    B2: Arbitrary: Cogen: Monoid,
    C: Arbitrary: Monoid
](name: String)
    extends PropSpec
    with Matchers
    with PropertyChecks {

  property(s"$name: joined aggregator product (allOf)") {
    forAll {
      (a1: MonoidAggregator[A1, B1, C], a2: MonoidAggregator[A2, B2, C], as: List[(A1, A2)]) =>
        val a3 = JoinedAggregators.allOf(a1 :: a2 :: HNil)
        val got = a3(as.map(_.productElements))
        val expected = Monoid.plus(a1(as.map(_._1)), a2(as.map(_._2)))
        got shouldBe expected
    }
  }

  property(s"$name: joined aggregator product (oneOf)") {
    forAll {
      (a1: MonoidAggregator[A1, B1, C],
       a2: MonoidAggregator[A2, B2, C],
       as: List[Either[A1, A2]]) =>
        val a3 = JoinedAggregators.oneOf(a1 :: a2 :: HNil)
        val got = a3(as.map {
          case Left(a)  => Inl(a)
          case Right(x) => Inr(Inl(x))
        })
        val expected = Monoid.plus(a1(as.collect { case Left(a) => a }), a2(as.collect {
          case Right(x)                                         => x
        }))
        got shouldBe expected
    }
  }
}

class JoinedAggregator_IILLB extends JoinedAggregatorTest[Int, Int, Long, Long, BigInt]("IILLB")

abstract class CombinedAggregatorTest[
    A: Arbitrary: Cogen,
    B1: Arbitrary: Cogen: Monoid,
    B2: Arbitrary: Cogen: Monoid,
    C: Arbitrary: Monoid
](name: String)
    extends PropSpec
    with Matchers
    with PropertyChecks {

  property(s"$name: combined aggregator") {
    forAll { (a1: MonoidAggregator[A, B1, C], a2: MonoidAggregator[A, B2, C], as: List[A]) =>
      val a3 = CombinedAggregators(a1 :: a2 :: HNil)
      a3(as) shouldBe Monoid.plus(a1(as), a2(as))
    }
  }
}

class CombinedAggregator_ILBB extends CombinedAggregatorTest[Int, Long, BigInt, BigInt]("ILBB")

abstract class CinchedAggregatorTest[
    A1: Arbitrary: Cogen,
    B: Arbitrary: Cogen: Monoid,
    C1: Arbitrary,
    A2: Arbitrary: Cogen,
    C2: Arbitrary
](name: String)
    extends PropSpec
    with Matchers
    with PropertyChecks {

  property(s"$name: cinched aggregator product (allOf)") {
    forAll {
      (a1: MonoidAggregator[A1, B, C1], a2: MonoidAggregator[A2, B, C2], as: List[(A1, A2)]) =>
        val a3 = CinchedAggregators.allOf(a1 :: a2 :: HNil)
        val got = a3(as.map(_.productElements))
        val b = Monoid.sum(as.map { case (x1, x2) => Monoid.plus(a1.prepare(x1), a2.prepare(x2)) })
        val expected = a1.present(b) :: a2.present(b) :: HNil
        got shouldBe expected
    }
  }

  property(s"$name: cinched aggregator product (oneOf)") {
    forAll {
      (a1: MonoidAggregator[A1, B, C1],
       a2: MonoidAggregator[A2, B, C2],
       as: List[Either[A1, A2]]) =>
        val a3 = CinchedAggregators.oneOf(a1 :: a2 :: HNil)
        val got = a3(as.map {
          case Left(a)  => Inl(a)
          case Right(x) => Inr(Inl(x))
        })
        val resB = a1.monoid.sum(as.map {
          case Left(x)  => a1.prepare(x)
          case Right(x) => a2.prepare(x)
        })
        val expected = a1.present(resB) :: a2.present(resB) :: HNil
        got shouldBe expected
    }
  }
}

class CinchedAggregator_ISLSB
    extends CinchedAggregatorTest[Int, String, Long, String, BigInt]("ISLSB")

class CinchedAggregator_IIIII extends CinchedAggregatorTest[Int, Int, Int, Int, Int]("IIIII")
