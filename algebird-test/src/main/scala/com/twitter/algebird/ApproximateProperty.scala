package com.twitter.algebird

import org.scalacheck.Gen

trait ApproximateProperty {
  type Params
  type Exact
  type Approx
  type Input
  type ExactResult
  type ApproximateResult

  def exactGenerator: Gen[Exact]
  def inputGenerator(e: Exact): Gen[Input]

  def makeApproximate(p: Params, e: Exact): Approx

  def exactResult(e: Exact, i: Input): ExactResult
  def approximateResult(a: Approx, i: Input): ApproximateResult

  def claimWithProbability(p: Params, e: ExactResult, a: ApproximateResult): (Boolean, Double)
}

class CmsProperty[K: CMSHasher] extends ApproximateProperty {
  // eps, delta, seed
  case class CmsParams(eps: Double, delta: Double, seed: Int)
  type Params = CmsParams

  type Exact = List[K]
  type Approx = CMS[K]

  type Input = K
  type ExactResult = Long
  type ApproximateResult = Approximate[Long]

  def exactGenerator: Gen[List[K]] = ???
  def inputGenerator(e: List[K]): Gen[K] = ???

  def makeApproximate(p: CmsParams, exact: List[K]) = {
    val cmsMonoid = CMS.monoid(p.eps, p.delta, p.seed)
    cmsMonoid.sum(exact.map(cmsMonoid.create(_)))
  }

  def exactResult(list: List[K], key: K) = list.filter(_ == key).length
  def approximateResult(cms: CMS[K], key: K) = cms.frequency(key)

  def claimWithProbability(p: CmsParams, e: Long, a: Approximate[Long]) =
    (a.boundsContain(e), a.probWithinBounds)
}
