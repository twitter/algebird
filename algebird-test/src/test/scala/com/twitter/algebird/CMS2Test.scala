package com.twitter.algebird

import org.scalatest.{ PropSpec, Matchers, WordSpec }
import org.scalatest.prop.{ GeneratorDrivenPropertyChecks, PropertyChecks }
import org.scalacheck.{ Gen, Arbitrary, Properties }

import CMSHasherImplicits._
import Arbitrary.arbitrary
import org.scalatest.prop.Checkers.check

class Cms2Laws extends PropSpec {

  import BaseProperties._

  val DELTA = 1E-8
  val EPS = 0.005
  val SEED = 1

  implicit def createArbitrary[K: Arbitrary: CMS2.Context]: Arbitrary[CMS2[K]] = {
    val single = arbitrary[K].map(CMS2(_))
    val many = arbitrary[List[(K, Byte)]].map { ks =>
      CMS2.addAllCounts(ks.map { case (k, c) => (k, (c & 255).toLong) })
    }
    Arbitrary(Gen.frequency((1, single), (6, many)))
  }

  def cms2monoidLaw[K: CMS2.Context: Arbitrary] = monoidLaws[CMS2[K]]

  implicit def context[K: CMSHasher]: CMS2.Context[K] = CMS2.Context(EPS, DELTA, SEED)

  property("CMS2[Long] is a Monoid") {
    check(commutativeMonoidLawsEq[CMS2[Long]](Equiv[CMS2[Long]].equiv))
  }
  property("CMS2[String] is a Monoid") {
    check(commutativeMonoidLawsEq[CMS2[String]](Equiv[CMS2[String]].equiv))
  }
  property("CMS2[Long] obeys row invariant") {
    check { (cms: CMS2[Long]) => cms.rowSumInvariant }
  }
  property("CMS2[String] obeys row invariant") {
    check { (cms: CMS2[String]) => cms.rowSumInvariant }
  }
}

class Cms2ApproximateProps[K: CMS2.Context: Arbitrary] extends ApproximateProperty {
  type Exact = Vector[K]
  type Approx = CMS2[K]

  type Input = K
  type Result = Long

  def exactGenerator: Gen[Vector[K]] = Gen.containerOf[Vector, K](arbitrary[K])
  def makeApproximate(e: Exact) = CMS2.addAll(e)
  def inputGenerator(e: Vector[K]): Gen[K] = Gen.oneOf(e)

  def exactResult(vec: Vector[K], key: K) = vec.count(_ == key)
  def approximateResult(cms: CMS2[K], key: K) = cms.frequency(key)
}

class Cms2Props extends ApproximateProperties("CMS2") {
  import ApproximateProperty.toProp
  implicit def ctxt[K: CMSHasher]: CMS2.Context[K] = CMS2.Context(0.01, 0.01)

  implicit def arbCms2Long: Arbitrary[Long] =
    Arbitrary(Gen.choose(Long.MinValue + 1, Long.MaxValue))

  implicit def arbCms2Int: Arbitrary[Int] =
    Arbitrary(Gen.choose(Int.MinValue + 1, Int.MaxValue))

  property("CMS2[Long] works") =
    toProp(new Cms2ApproximateProps[Long], 10, 10, 0.01)

  property("CMS2[String] works") =
    toProp(new Cms2ApproximateProps[String], 10, 10, 0.01)

  property("CMS2[Int] works") =
    toProp(new Cms2ApproximateProps[Int], 10, 10, 0.01)

  implicit def arbBytes: Arbitrary[Bytes] = Arbitrary(arbitrary[Array[Byte]].map(Bytes(_)))

  property("CMS2[Bytes] works") =
    toProp(new Cms2ApproximateProps[Bytes], 10, 10, 0.01)
}
