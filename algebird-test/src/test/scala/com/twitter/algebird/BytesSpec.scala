package com.twitter.algebird

import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BytesSpec extends AnyWordSpec with Matchers with ScalaCheckDrivenPropertyChecks {

  "requires non-null arrays" in {
    an[IllegalArgumentException] should be thrownBy Bytes(null)
  }

  "has a sane equals function" in {
    val random = new scala.util.Random
    forAll((Gen.choose(0, 4096), "wordLength")) { (wordLength: Int) =>
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
      bytes1.equals(bytes1) should be(true)

      // 2. equals is symmetric.
      bytes1.equals(bytes2) should be(bytes2.equals(bytes1))

      // 3. equals is transitive.
      val bytes3 = {
        val array3 = word.getBytes
        Bytes(array3)
      }
      bytes1.equals(bytes3) should be(bytes1.equals(bytes2)).and(be(bytes2.equals(bytes3)))

      // 4. equals is consistent.
      // The verification below (combined with the generator-driven test setup) is a naive smoke test.
      bytes1.equals(bytes2) should be(true)
      bytes1.equals(bytes2) should be(true)

      // 5. For any non-null value x, x.equals(null) should return false.
      bytes1.equals(null) should be(false)
      bytes2.equals(null) should be(false)
      bytes3.equals(null) should be(false)

      // 6. equals is false if the contents are different.
      val bytesSuffixed = {
        val arraySuffixed = (word + "suffixed").getBytes
        Bytes(arraySuffixed)
      }
      bytes1.equals(bytesSuffixed) should be(false)

      // 7. equals is false if the types are different.
      bytes1.equals(array1) should be(false)
      bytes1.equals(new AnyRef) should be(false)
    }
  }

  "has a sane hashCode function" in {
    val random = new scala.util.Random
    forAll((Gen.choose(0, 4096), "wordLength")) { (wordLength: Int) =>
      // Given two Bytes instances that are equal.
      val word = random.nextString(wordLength)
      val array1 = word.getBytes
      val array2 = word.getBytes
      val bytes1 = Bytes(array1)
      val bytes2 = Bytes(array2)
      bytes1.equals(bytes2) should be(true)

      // Then their wrappers should have the same hash code.
      bytes1.hashCode should be(bytes2.hashCode)

      // Beyond the standard contract of `hashCode` (which is covered above), we also want small changes to an instance
      // to result in a different hash code.  The verifications below (combined with the generator-driven test setup)
      // are a naive smoke test.
      val appended = Bytes(bytes1.array :+ random.nextInt.toByte)
      bytes1.hashCode shouldNot be(appended.hashCode)

      if (bytes1.array.length > 1) {
        val shortened = Bytes(bytes1.array.tail)
        bytes1.hashCode shouldNot be(shortened.hashCode)
      }
    }
  }

  "provides an Ordering typeclass" in {
    val random = new scala.util.Random
    forAll((Gen.choose(0, 4096), "wordLength")) { (wordLength: Int) =>
      val word = random.nextString(wordLength)
      val array1 = word.getBytes
      val array2 = word.getBytes
      val bytes1 = Bytes(array1)
      val bytes2 = Bytes(array2)
      bytes1.equals(bytes2) should be(true)

      // 1. same Bytes instance (= reflexive).
      // Note: We need to combine two ranges because `bytes1 should be(bytes1)` translates into an equality check.
      bytes1 should (be <= bytes1).and(be >= bytes1)

      // 2. two Bytes instances that are equal.
      // Note: We need to combine two ranges because `bytes1 should be(bytes2)` translates into an equality check.
      bytes1 should (be <= bytes2).and(be >= bytes2)

      // 3. a Bytes instance and a "longer" Bytes instance.
      val suffixed = {
        val arraySuffixed = (word + "suffixed").getBytes
        Bytes(arraySuffixed)
      }
      bytes1 should be < suffixed
      suffixed should be > bytes1

      // 4. a Bytes instances and a "shorter" Bytes instance.
      if (bytes1.array.length > 0) {
        val shortened = Bytes(bytes1.array.dropRight(1))
        bytes1 should be > shortened
        shortened should be < bytes1
      }
    }
  }

  "forbids, by contract, the mutation of the wrapped array but does not prevent it" in {
    // This test case documents that we do not put any safeguards in place to prevent mutation of the wrapped array.
    // Not doing so (e.g. not creating a defensive copy of the array) is a deliberate decision to improve the
    // performance of Bytes.

    // Given a Bytes instance
    val bytes = Bytes(Array(1.toByte))
    val originalHashCode = bytes.hashCode
    // And a copy of the instance
    val copy = bytes.copy()

    // When we mutate the wrapped array
    bytes.array.update(0, 2)

    // Then the new hash code should be different
    bytes.hashCode shouldNot be(originalHashCode)

    // And the copy of the original Bytes instance is NOT a defensive copy of the instance because it wraps the same
    // (mutated) array.
    bytes should be(copy)
  }

}
