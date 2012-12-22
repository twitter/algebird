/*
Copyright 2012 Twitter, Inc.

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

package com.twitter.algebird

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Properties
import org.scalacheck.Prop.forAll
import org.scalacheck.Gen.choose

object SummingQueueTest extends Properties("SummingQueue") {

  val sb = SummingQueue[Int](3)
  property("puts are like sums") = forAll { (items: List[Int]) =>
    Semigroup.sumOption(items) ==
      (Monoid.plus(Monoid.sum(items.map { sb(_) }), sb.flush))
  }
  property("puts return None sometimes") = forAll { (items: List[Int]) =>
    // Should be: true, true, true, false, true, true, true, false
    sb.flush
    val empties = items.map { sb.put(_).isEmpty }
    val correct = Stream
      .continually( Stream(true, true, true, false) )
      .flatMap { s => s }
      .take(empties.size)
      .toList
    empties == correct
  }
}
