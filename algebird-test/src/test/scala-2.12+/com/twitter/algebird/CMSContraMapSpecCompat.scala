package com.twitter.algebird

import org.scalatestplus.scalacheck.{ScalaCheckDrivenPropertyChecks, ScalaCheckPropertyChecks}
import org.scalacheck.{Arbitrary, Gen}

import scala.util.Random
import CMSHasherImplicits.CMSHasherBigInt
import org.scalatest.matchers.should.Matchers
import org.scalatest.propspec.AnyPropSpec
import org.scalatest.wordspec.AnyWordSpec

trait CMSContraMapSpecCompat { self: AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks =>
  "translates CMSHasher[K] into CMSHasher[L], given a function f: L => K" in {
    // Given a "source" CMSHasher[K]
    val sourceHasher: CMSHasher[String] = CMSHasher.CMSHasherString
    // and a translation function from an unsupported type L (here: Seq[Byte]) to K
    def f(bytes: Seq[Byte]): String = new String(bytes.toArray[Byte], "UTF-8")

    // When we run contramap on a CMSHasher[K] supplying f,
    // then the result should be a CMSHasher[L]...
    val targetHasher: CMSHasher[Seq[Byte]] =
      sourceHasher.contramap((d: Seq[Byte]) => f(d))
    targetHasher shouldBe an[CMSHasher[
      ?
    ]] // Can't test CMSHasher[Seq[Byte]] specifically because of type erasure.

    // ...and hashing should work correctly (this is only a smoke test).
    val a = 4
    val b = 0
    val width = 1234
    val x = Array(113.toByte).toSeq // same as Seq(133.toByte)
    val result = targetHasher.hash(a, b, width)(x)
    val expected = sourceHasher.hash(a, b, width)("q")
    result should be(expected)
    result should be(434)
  }
}
