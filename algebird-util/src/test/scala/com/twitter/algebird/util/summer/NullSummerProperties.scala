/*
 Copyright 2013 Twitter, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package com.twitter.algebird.util.summer

import com.twitter.algebird.{ MapAlgebra, Semigroup }
import com.twitter.util.{ Future, Await }
import scala.collection.mutable.{ Map => MMap }
import org.scalatest.{ DiagrammedAssertions, PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import Gen._
import Arbitrary._
import org.scalatest.{ DiagrammedAssertions, PropSpec, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalacheck.Prop._
import scala.util.Random
import com.twitter.util.Duration
import Arbitrary.arbitrary

class NullSummerProperties extends PropSpec with PropertyChecks with Matchers with DiagrammedAssertions {
  import AsyncSummerLaws._

  property("Summing with and without the summer should match") =
  forAll { (inputs: List[List[(Int, Long)]]) =>
    val summer = new NullSummer[Int, Long]()
    summingWithAndWithoutSummerShouldMatch(summer, inputs)
  }

}
