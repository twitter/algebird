package com.twitter.algebird

import org.specs2.mutable._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen.{choose, oneOf, value}

import java.lang.AssertionError
import java.util.Arrays

object SGDLaws extends Properties("SGD") {
  import BaseProperties._

  implicit val sgdMonoid = new SGDMonoid(SGD.constantStep(0.001), SGD.linearGradient)
  val zeroStepMonoid = new SGDMonoid(SGD.constantStep(0.0), SGD.linearGradient)

  val (m, b) = (2.0, 4.0)
  val eps = 1e-3

  val sgdPosGen = for(
      x <- choose(0.0, 1.0);
      n <- choose(0.0, 0.001)
    ) yield SGDPos((m * x + b + n, IndexedSeq(x)))

  val sgdWGen = for(
      cnt <- choose(0L, 100000L);
      m <- choose(-10.0, 10.0);
      b <- choose(-10.0, 10.0)
    ) yield SGDWeights(cnt, Vector(m,b))

  val zeroGen = value(SGDZero)

  implicit val sgdPos = Arbitrary(sgdPosGen)
  implicit val sgdWArb = Arbitrary(sgdWGen)
  implicit val sgdArb : Arbitrary[SGD[(Double, IndexedSeq[Double])]] = Arbitrary {
    oneOf(sgdWGen, sgdPosGen, zeroGen)
  }

  property("is a Monoid") = monoidLaws[SGD[(Double,IndexedSeq[Double])]]
  property("Gradient is zero on the line") = forAll { (w : SGDWeights, x : Double) =>
    val m = w.weights(0)
    val b = w.weights(1)
    val y = m*x + b

    y.isInfinity || {
      val pos = (y, IndexedSeq(x))
      val grad = SGD.linearGradient(w.weights, pos)
      (scala.math.abs(grad(0)) < eps) && (scala.math.abs(grad(1)) < eps)
    }
  }
  property("Gradient at x=0 has zero first component") = forAll { (w : SGDWeights, y : Double) =>
    SGD.linearGradient(w.weights,(y, IndexedSeq(0.0)))(0) == 0.0
  }
  property("Zero-step leaves Weights unchanged") = forAll {
    (w : SGDWeights, pos : SGDPos[(Double,IndexedSeq[Double])]) =>
      val next = zeroStepMonoid.newWeights(w, pos.pos.head)
      next.weights == w.weights && next.count == (w.count + 1L)
  }
  def minus(x : IndexedSeq[Double], y : IndexedSeq[Double]) : IndexedSeq[Double] = {
    x.zip(y).map { case (x : Double, y : Double) => x-y }
  }
  val oneStepMonoid = new SGDMonoid(SGD.constantStep(1.0), SGD.linearGradient)
  property("unit step can be undone by adding gradient") = forAll {
    (w : SGDWeights, pos : SGDPos[(Double,IndexedSeq[Double])]) =>
      val next = oneStepMonoid.newWeights(w, pos.pos.head)
      next.weights == minus(w.weights, SGD.linearGradient(w.weights, pos.pos.head))
  }
}
