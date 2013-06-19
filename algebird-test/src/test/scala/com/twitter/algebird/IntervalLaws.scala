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

package com.twitter.algebird

import org.scalacheck.Properties
import org.scalacheck.Prop._

object IntervalLaws extends Properties("Interval") {
  import Generators._

  property("[x - 1, x) contains x - 1") =
    forAll { x: Int => Interval.leftClosedRightOpen(x - 1, x).contains(x - 1) }

  property("[x - 1, x) does not contain x") =
    forAll { x: Int => ! Interval.leftClosedRightOpen(x - 1, x).contains(x) }

  property("[x, x) is empty") =
    forAll { x : Int => Interval.leftClosedRightOpen(x, x) == Empty[Int]() }

  property("If an intersection contains, both of the intervals contain") =
    forAll { (item: Long, i1: Interval[Long], i2: Interval[Long]) =>
      (i1 && i2).contains(item) == (i1(item) && i2(item))
    }
}
