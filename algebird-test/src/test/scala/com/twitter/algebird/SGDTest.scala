package com.twitter.algebird

import org.scalacheck.Prop._
import org.scalacheck.{Arbitrary, Gen}

class SGDLaws extends CheckProperties {
  import com.twitter.algebird.BaseProperties._

  implicit val sgdMonoid: SGDMonoid[(Double, IndexedSeq[Double])] =
    new SGDMonoid(SGD.constantStep(0.001), SGD.linearGradient)
  val zeroStepMonoid: SGDMonoid[(Double, IndexedSeq[Double])] = new SGDMonoid(SGD.constantStep(0.0), SGD.linearGradient)

  val (m, b) = (2.0, 4.0)
  val eps: Double = 1e-3

  val sgdPosGen: Gen[SGDPos[(Double, Vector[Double])]] = for {
    x <- Gen.choose(0.0, 1.0)
    n <- Gen.choose(0.0, 0.001)
  } yield SGDPos((m * x + b + n, Vector(x)))

  val sgdWGen: Gen[SGDWeights] = for {
    cnt <- Gen.choose(0L, 100000L)
    m <- Gen.choose(-10.0, 10.0)
    b <- Gen.choose(-10.0, 10.0)
  } yield SGDWeights(cnt, Vector(m, b))

  val zeroGen: Gen[SGDZero.type] = Gen.const(SGDZero)

  implicit val sgdPos: Arbitrary[SGDPos[(Double, Vector[Double])]] = Arbitrary(sgdPosGen)
  implicit val sgdWArb: Arbitrary[SGDWeights] = Arbitrary(sgdWGen)
  implicit val sgdArb: Arbitrary[SGD[(Double, IndexedSeq[Double])]] =
    Arbitrary {
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

      (y.isInfinity || {
        val pos = (y, IndexedSeq(x))
        val grad = SGD.linearGradient(w.weights, pos)
        (scala.math.abs(grad(0)) < eps) && (scala.math.abs(grad(1)) < eps)
      })
    }
  }

  property("Gradient at x=0 has zero first component") {
    forAll((w: SGDWeights, y: Double) => (SGD.linearGradient(w.weights, (y, IndexedSeq(0.0)))(0) == 0.0))
  }

  property("Zero-step leaves Weights unchanged") {
    forAll { (w: SGDWeights, pos: SGDPos[(Double, Vector[Double])]) =>
      val next = zeroStepMonoid.newWeights(w, pos.pos.head)
      (next.weights == w.weights && next.count == (w.count + 1L))
    }
  }

  def minus(x: IndexedSeq[Double], y: IndexedSeq[Double]): IndexedSeq[Double] =
    x.zip(y).map { case (x: Double, y: Double) => x - y }

  val oneStepMonoid: SGDMonoid[(Double, IndexedSeq[Double])] = new SGDMonoid(SGD.constantStep(1.0), SGD.linearGradient)

  property("unit step can be undone by adding gradient") {
    forAll { (w: SGDWeights, pos: SGDPos[(Double, Vector[Double])]) =>
      val next = oneStepMonoid.newWeights(w, pos.pos.head)
      next.weights == minus(w.weights, SGD.linearGradient(w.weights, pos.pos.head))
    }
  }
}
