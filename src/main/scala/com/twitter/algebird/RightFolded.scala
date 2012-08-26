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

/**
 * This is an associative, but not commutative monoid
 * Also, you must start on the right, with a value, and all subsequent RightFolded must
 * be RightFoldedToFold objects or zero
 *
 * If you add two Folded values together, you always get the one on the left,
 * so this forms a kind of reset of the fold.
 */
object RightFolded {
  def monoid[In,Out](foldfn : (In,Out) => Out) =
    new Monoid[RightFolded[In,Out]] {

    lazy val zero = RightFoldedZero[In,Out]()

    def plus(left : RightFolded[In,Out], right : RightFolded[In,Out]) = (left, right) match {
      case (RightFoldedValue(_), _) => left
      case (RightFoldedZero(), _) => right
      case (RightFoldedToFold(lList), _) => right match {
        case RightFoldedZero() => RightFoldedToFold(lList)
        case RightFoldedValue(vr) => RightFoldedValue(lList.foldRight(vr)(foldfn))
        case RightFoldedToFold(rList) => RightFoldedToFold(lList ++ rList)
      }
    }
  }
}

sealed abstract class RightFolded[In,Out]
case class RightFoldedZero[In,Out]() extends RightFolded[In,Out]
case class RightFoldedValue[In,Out](v : Out) extends RightFolded[In,Out]
case class RightFoldedToFold[In,Out](in : List[In]) extends RightFolded[In,Out]
