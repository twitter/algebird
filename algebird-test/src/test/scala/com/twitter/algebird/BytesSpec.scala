package com.twitter.algebird

import org.scalacheck.Gen
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{Matchers, WordSpec}

class BytesSpec extends WordSpec with Matchers with GeneratorDrivenPropertyChecks {

  "has a sane equals function" in {
    val random = new scala.util.Random
    forAll((Gen.choose(1, 4096), "wordLength")) { (wordLength: Int) =>
      // Given two different array instances that have the same content.
      val word = random.nextString(wordLength)
      val array1 = word.getBytes
      val array2 = word.getBytes

      // When wrapping the arrays
      val bytes1 = Bytes(array1)
      val bytes2 = Bytes(array2)

      // Then their wrappers should be equal (cf. http://www.artima.com/lejava/articles/equality.html).
      // Note: We explicitly call `equals` to prevent ScalaTest's equality magic from kicking in.

      // 1. equals is reflexive.
      (bytes1 equals bytes1) should be(true)

      // 2. equals is symmetric.
      (bytes1 equals bytes2) should be(bytes2 equals bytes1)

      // 3. equals is transitive.
      val array3 = word.getBytes
      val bytes3 = Bytes(array3)
      (bytes1 equals bytes3) should (be(bytes1 equals bytes2) and be(bytes2 equals bytes3))

      // 4. equals is consistent.
      // The verification below (combined with the generator-driven test setup) is a naive smoke test.
      (bytes1 equals bytes2) should be(true)
      (bytes1 equals bytes2) should be(true)

      // 5. For any non-null value x, x.equals(null) should return false.
      (bytes1 equals null) should be(false)
      (bytes2 equals null) should be(false)
      (bytes3 equals null) should be(false)
    }
  }

  "has a sane hashCode function" in {
    val random = new scala.util.Random
    forAll((Gen.choose(1, 4096), "wordLength")) { (wordLength: Int) =>
      // Given two Bytes instances that are equal.
      val word = random.nextString(wordLength)
      val array1 = word.getBytes
      val array2 = word.getBytes
      val bytes1 = Bytes(array1)
      val bytes2 = Bytes(array2)
      (bytes1 equals bytes2) should be(true)

      // Then their wrappers should have the same hashCode().
      bytes1.hashCode should be(bytes2.hashCode)
    }
  }

  "provides an Ordering typeclass" in {
    val random = new scala.util.Random
    forAll((Gen.choose(1, 4096), "wordLength")) { (wordLength: Int) =>
      val word = random.nextString(wordLength)
      val array1 = word.getBytes
      val array2 = word.getBytes
      val bytes1 = Bytes(array1)
      val bytes2 = Bytes(array2)
      (bytes1 equals bytes2) should be(true)

      // 1. same Bytes instance (= reflexive).
      // Note: We need to combine two ranges because `bytes1 should be(bytes1)` translates into an equality check.
      bytes1 should (be <= bytes1 and be >= bytes1)

      // 2. two Bytes instances that are equal.
      // Note: We need to combine two ranges because `bytes1 should be(bytes2)` translates into an equality check.
      bytes1 should (be <= bytes2 and be >= bytes2)

      // 3. a Bytes instance and a longer, suffixed Bytes instance.
      val arraySuffixed = (word + "suffixed").getBytes
      val bytesSuffixed = Bytes(arraySuffixed)
      bytes1 should be < bytesSuffixed
      bytesSuffixed should be > bytes1
    }
  }

}