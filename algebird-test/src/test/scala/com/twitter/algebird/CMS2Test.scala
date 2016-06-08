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

