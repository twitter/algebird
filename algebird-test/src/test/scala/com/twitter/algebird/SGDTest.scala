package com.twitter.algebird

import org.scalatest._

import org.scalatest.{ PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.{ Gen, Arbitrary }
import java.lang.AssertionError
import java.util.Arrays

class SGDLaws extends PropSpec with PropertyChecks with Matchers {
  import BaseProperties._

  implicit val sgdMonoid = new SGDMonoid(SGD.constantStep(0.001), SGD.linearGradient)
  val zeroStepMonoid = new SGDMonoid(SGD.constantStep(0.0), SGD.linearGradient)

  val (m, b) = (2.0, 4.0)
  val eps = 1e-3

  val sgdPosGen = for (
    x <- Gen.choose(0.0, 1.0);
    n <- Gen.choose(0.0, 0.001)
  ) yield SGDPos((m * x + b + n, IndexedSeq(x)))

  val sgdWGen = for (
    cnt <- Gen.choose(0L, 100000L);
    m <- Gen.choose(-10.0, 10.0);
    b <- Gen.choose(-10.0, 10.0)
  ) yield SGDWeights(cnt, Vector(m, b))

  val zeroGen = Gen.const(SGDZero)

  implicit val sgdPos = Arbitrary(sgdPosGen)
  implicit val sgdWArb = Arbitrary(sgdWGen)
  implicit val sgdArb: Arbitrary[SGD[(Double, IndexedSeq[Double])]] = Arbitrary {
    Gen.oneOf(sgdWGen, sgdPosGen, zeroGen)
  }

  property("is a Monoid") {
    monoidLaws[SGD[(Double, IndexedSeq[Double])]]
  }

  property("Gradient is zero on the line") {
    forAll { (w: SGDWeights, x: Double) =>
      val m = w.weights(0)
      val b = w.weights(1)
      val y = m * x + b

      assert(y.isInfinity || {
        val pos = (y, IndexedSeq(x))
        val grad = SGD.linearGradient(w.weights, pos)
        (scala.math.abs(grad(0)) < eps) && (scala.math.abs(grad(1)) < eps)
      })
    }
  }

  property("Gradient at x=0 has zero first component") {
    forAll { (w: SGDWeights, y: Double) =>
      assert(SGD.linearGradient(w.weights, (y, IndexedSeq(0.0)))(0) == 0.0)
    }
  }

  property("Zero-step leaves Weights unchanged") {
    forAll {
      (w: SGDWeights, pos: SGDPos[(Double, IndexedSeq[Double])]) =>
        val next = zeroStepMonoid.newWeights(w, pos.pos.head)
        assert(next.weights == w.weights && next.count == (w.count + 1L))
    }
  }

  def minus(x: IndexedSeq[Double], y: IndexedSeq[Double]): IndexedSeq[Double] = {
    x.zip(y).map { case (x: Double, y: Double) => x - y }
  }

  val oneStepMonoid = new SGDMonoid(SGD.constantStep(1.0), SGD.linearGradient)

  property("unit step can be undone by adding gradient") {
    forAll {
      (w: SGDWeights, pos: SGDPos[(Double, IndexedSeq[Double])]) =>
        val next = oneStepMonoid.newWeights(w, pos.pos.head)
        assert(next.weights == minus(w.weights, SGD.linearGradient(w.weights, pos.pos.head)))
    }
  }
}
