package com.twitter.algebird.lsh.vector

import org.scalatest.{ Matchers, WordSpec }

class VectorMathTests extends WordSpec with Matchers {
  "vector math should handle vectors correctly" in {
    val result = VectorMath.dot(Array(1.0), Array(1.0))
    result should equal (1.0)
  }
}
