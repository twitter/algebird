package com.twitter.algebird

import org.scalatest.FunSuite
import org.scalatest.prop.PropertyChecks

class SemigroupTest extends FunSuite with PropertyChecks {
  test("Semigroup.maybePlus works") {
    forAll { s: String =>
      assert(Semigroup.maybePlus(None, s) == s)
      assert(Semigroup.maybePlus(s, None) == s)
    }
    forAll { (s1: String, s2: String) =>
      assert(Semigroup.maybePlus(Some(s1), s2) == (s1 + s2))
      assert(Semigroup.maybePlus(s1, Some(s2)) == (s1 + s2))
    }
  }
}
