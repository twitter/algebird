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

import org.scalacheck.Prop.forAll

class SuccessibleProperties extends CheckProperties {
  import com.twitter.algebird.SuccessibleLaws.{successibleLaws => laws}

  property("Int is Successible")(laws[Int])
  property("Long is Successible")(laws[Long])
  property("BigInt is Successible")(laws[BigInt])
  property("Successible.fromNextOrd[Int] is Successible") {
    implicit val succ: Successible[Int] =
      Successible.fromNextOrd[Int](IntegralSuccessible.next(_))
    laws[Int]
  }

  property("optionOrdering treats None as max") {
    val ord = Successible.optionOrdering[Int]
    forAll { (optL: Option[Int], optR: Option[Int]) =>
      (optL, optR) match {
        case (Some(l), Some(r)) =>
          Ordering[Int].compare(l, r) == ord.compare(optL, optR)
        case (None, None) => ord.equiv(optL, optR)
        case (None, _)    => ord.compare(optL, optR) == 1
        case (_, None)    => ord.compare(optL, optR) == -1
      }
    }
  }
}
