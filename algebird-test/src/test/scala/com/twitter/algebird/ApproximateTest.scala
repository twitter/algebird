package com.twitter.algebird

import org.specs2.mutable._

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Gen.{choose, oneOf, value}
import org.scalacheck.Prop.forAll
import org.scalacheck.Prop

// TODO add tests for scala check that uses a statistical test to check
// that an ApproximateBoolean agrees with the correct Boolean at least as often
// as it is claimed to

object ApproximateLaws extends Properties("Approximate") {
  import BaseProperties._

  implicit val approxGen =
    Arbitrary {
      for (v0 <- choose(0L, (1L << 15) - 2);
        v1 <- choose(v0, (1L << 15) - 1);
        v2 <- choose(v1, (1L << 15))
      ) yield Approximate(v0, v1, v2, 0.9)
    }

  property("is a Monoid") = monoidLaws[Approximate[Long]]
  property("always contain estimate") = forAll {
    (ap1: Approximate[Long], ap2: Approximate[Long]) =>
    ((ap1 + ap2) ~ (ap1.estimate + ap2.estimate)) &&
      ((ap1 * ap2) ~ (ap1.estimate * ap2.estimate)) &&
      ap1 ~ (ap1.estimate) &&
      ap2 ~ (ap2.estimate) &&
      ((ap1 + (ap1.negate)) ~ 0L)
      ((ap2 + (ap2.negate)) ~ 0L)
  }
  def boundsAreOrdered[N](ap: Approximate[N]) = {
    val n = ap.numeric
    n.lteq(ap.min, ap.estimate) && n.lteq(ap.estimate, ap.max)
  }
  property("Addition/Multiplication preserves bounds") = forAll {
    (ap1: Approximate[Long], ap2: Approximate[Long]) =>
      (ap1 + ap2).probWithinBounds <=
        Ordering[Double].min(ap1.probWithinBounds, ap2.probWithinBounds)
      (ap1 * ap2).probWithinBounds <=
        Ordering[Double].min(ap1.probWithinBounds, ap2.probWithinBounds)

      boundsAreOrdered(ap1 * ap2)
      boundsAreOrdered(ap1 + ap2)
  }

  val trueGen = choose(0.0, 1.0).map { ApproximateBoolean(true, _) }
  val falseGen = choose(0.0, 1.0).map { ApproximateBoolean(false, _) }
  implicit val approxArb = Arbitrary(oneOf(trueGen, falseGen,
    value(ApproximateBoolean.exact(true)), value(ApproximateBoolean.exact(false))))

  property("Boolean: &&") = forAll { (a: ApproximateBoolean) =>
    ((a && ApproximateBoolean.exact(false)) == ApproximateBoolean.exact(false)) &&
    // Make sure when it is false, we don't lose precision:
    (a && ApproximateBoolean(false, a.withProb / 2.0)).withProb >= (a.withProb / 2.0)
  }
  property("Boolean: ||") = forAll { (a: ApproximateBoolean) =>
    (a || ApproximateBoolean.exact(true)) == ApproximateBoolean.exact(true) &&
    // Make sure when it is true, we don't lose precision:
    (a || ApproximateBoolean(true, a.withProb / 2.0)).withProb >= (a.withProb / 2.0)
  }
  property("logic works") = forAll { (a: ApproximateBoolean, b: ApproximateBoolean) =>
    (a ^ b).isTrue == (a.isTrue ^ b.isTrue) &&
    (a^b).withProb >= (a.withProb * b.withProb) &&
    (a || b).isTrue == (a.isTrue || b.isTrue) &&
    (a||b).withProb >= (a.withProb * b.withProb) &&
    (a && b).isTrue == (a.isTrue && b.isTrue) &&
    (a&&b).withProb >= (a.withProb * b.withProb) &&
    (a.not).isTrue == (!(a.isTrue)) &&
    (a.not.withProb) == a.withProb
  }
}

class ApproximateTest extends Specification {


  "Approximate" should {
    "Correctly identify exact" in {
      Approximate.exact(1.0) ~ 1.0 must beTrue
      Approximate.exact(1.0).boundsContain(1.0) must beTrue

      Approximate.exact(1.0).boundsContain(1.1) must beFalse
      Approximate.exact(1.0) ~ 1.1 must beFalse
    }
  }
}
