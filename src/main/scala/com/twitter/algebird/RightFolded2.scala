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
 * This monoid takes a list of values of type In or Out,
 * and folds to the right all the Ins into Out values, leaving
 * you with a list of Out values, then finally, maps those outs
 * onto Acc, where there is a group, and adds all the Accs up.
 * So, if you have a list:
 * I I I O I O O I O I O
 * the monoid is equivalent to the computation:
 *
 * map(fold(List(I,I,I),O)) + map(fold(List(I),O)) + map(fold(List(),O)) +
 *   map(fold(List(I),O)) + map(fold(List(I),O))
 *
 * This models a version of the map/reduce paradigm, where the fold happens
 * on the mappers for each group on Ins, and then they are mapped to Accs,
 * sent to a single reducer and all the Accs are added up.
 */
object RightFolded2 {
  def monoid[In,Out:Group](foldfn : (In,Out) => Out) =
    new RightFolded2Monoid[In,Out,Out](foldfn, identity _)

  def monoid[In,Out,Acc:Group](trans: (Out) => Acc)(foldfn: (In,Out) => Out) =
    new RightFolded2Monoid[In,Out,Acc](foldfn, trans)
}

class RightFolded2Monoid[In,Out,Acc](foldfn: (In,Out) => Out, accfn: (Out) => Acc)
  (implicit grpAcc: Group[Acc])
  extends Monoid[RightFolded2[In,Out,Acc]] {
  val zero = RightFoldedZero2

  def init(i: Out) = RightFoldedValue2[In,Out,Acc](i, accfn(i), Nil)
  def toFold(v: In) = RightFoldedToFold2(List(v))

  protected def doFold(vals: List[In], init: Out, acc: Acc) : (Out,Acc) = {
    val newV = vals.foldRight(init)(foldfn)
    val delta = grpAcc.plus(accfn(newV), grpAcc.negate(accfn(init)))
    (newV, grpAcc.plus(delta, acc))
  }

  def plus(left: RightFolded2[In,Out,Acc], right : RightFolded2[In,Out,Acc]) = left match {
    case RightFoldedValue2(leftV,leftAcc,leftRvals) => {
      right match {
        case RightFoldedZero2 => left
        case RightFoldedToFold2(in) => RightFoldedValue2(leftV, leftAcc, leftRvals ++ in)
        case RightFoldedValue2(rightV,rightAcc,rightRvals) => {
          if (leftRvals.isEmpty) {
            // This is the case of two initial values next to each other, return the left:
            val newAcc = grpAcc.plus(leftAcc, rightAcc)
            RightFoldedValue2(leftV, newAcc, rightRvals)
          }
          else {
            val (newV, newRightAcc) = doFold(leftRvals, rightV, rightAcc)
            // Now actually add the left value, with the right:
            val newAcc = grpAcc.plus(leftAcc, newRightAcc)
            RightFoldedValue2(leftV, newAcc, rightRvals)
          }
        }
      }
    }
    case RightFoldedZero2 => right
    case RightFoldedToFold2(lList) => right match {
      case RightFoldedZero2 => left
      case RightFoldedToFold2(rList) => RightFoldedToFold2(lList ++ rList)
      case RightFoldedValue2(vr,accr,valsr) => {
        val (newV, newAcc) = doFold(lList, vr, accr)
        RightFoldedValue2(newV, newAcc, valsr)
      }
    }
  }
}

sealed abstract class RightFolded2[+In,+Out,+Acc]
case object RightFoldedZero2 extends RightFolded2[Nothing,Nothing,Nothing]
case class RightFoldedValue2[+In,+Out,+Acc](v: Out, acc: Acc, rvals: List[In]) extends RightFolded2[In,Out,Acc]
case class RightFoldedToFold2[+In](in: List[In]) extends RightFolded2[In,Nothing,Nothing]
