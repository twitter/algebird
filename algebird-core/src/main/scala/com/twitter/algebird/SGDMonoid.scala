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

object SGD {
  /**
   * constructs the gradient for linear regression.
   * the Pos type is (Double, IndexedSeq[Double])
   * note the LAST element in the weights is the constant term.
   * and note that the length of the IndexedSeq in the tuple is
   * one less than the weights (we don't carry the constant term)
   */
  val linearGradient : (IndexedSeq[Double], (Double, IndexedSeq[Double])) => IndexedSeq[Double]
  = { (w, pos) =>
    val (y, xs) = pos
    val xsPlusConst = xs :+ 1.0
    val err = dot(w,xsPlusConst) - y
    // Here is the gradient
    xsPlusConst.map { _ * err }
  }

  def dot(x : IndexedSeq[Double], y : IndexedSeq[Double]) : Double =
    x.view.zip(y).map { case (a: Double, b: Double) => a*b }.sum

  // Here are some step algorithms:
  def constantStep(s: Double) : (Long, IndexedSeq[Double]) => Double = { (_,_) => s }
  // A standard: a/(steps + b)^c
  def countAdaptiveStep(a : Double, b : Double, c : Double = 1.0) :
    (Long, IndexedSeq[Double]) => Double = { (cnt,_) =>
      a/scala.math.pow((cnt + b), c)
    }

  def weightsOf[T](s: SGD[T]) : Option[IndexedSeq[Double]] = {
    s match {
      case SGDWeights(_,w) => Some(w)
      case _ => None
    }
  }
}

sealed abstract class SGD[+Pos]
case object SGDZero extends SGD[Nothing]
object SGDWeights {
  def apply(w : IndexedSeq[Double]) = new SGDWeights(1L, w)
  def average(left : SGDWeights, right : SGDWeights) : SGDWeights = {
    val lc = left.count
    val rc = right.count
    if (rc == 0L) left
    else if (lc == 0L) right
    else {
      val newW = left.weights.view
        .zip(right.weights)
        .map { case (l : Double, r : Double) =>
          (lc * l + rc * r)/((lc + rc).toDouble)
        }
        .toIndexedSeq
      SGDWeights(lc + rc, newW)
    }
  }
}
case class SGDWeights(val count : Long, val weights : IndexedSeq[Double]) extends SGD[Nothing]

object SGDPos {
  def apply[Pos](p: Pos) = new SGDPos(List(p))
}
case class SGDPos[+Pos](val pos: List[Pos]) extends SGD[Pos]

/**
 * Basically a specific implementation of the RightFoldedMonoid
 * gradient is the gradient of the function to be minimized
 * To use this, you need to insert an initial weight SGDWeights
 * before you start adding SGDPos objects. Otherwise you will
 * just be doing list concatenation.
 */
class SGDMonoid[Pos](stepfn : (Long, IndexedSeq[Double]) => Double,
  gradient : (IndexedSeq[Double],Pos) => IndexedSeq[Double])

  extends Monoid[SGD[Pos]] {

  val zero = SGDZero

  def plus(left: SGD[Pos], right: SGD[Pos]): SGD[Pos] = {
    (left, right) match {
      case (_, SGDZero) => left
      case (SGDPos(llps), SGDPos(rlps)) => SGDPos(llps ::: rlps)
      case (rsw@SGDWeights(c,w), SGDPos(p)) => p.foldLeft(rsw) { (cntWeight, pos) =>
          newWeights(cntWeight, pos)
        }
      // TODO make a RightFolded2 which folds A,B => (B,C), and a group on C.
      case _ => right
    }
  }

  def newWeights(sgdW: SGDWeights, p: Pos): SGDWeights = {
    val grad = gradient(sgdW.weights, p)
    val step = stepfn(sgdW.count, grad)
    SGDWeights(sgdW.count + 1L,
      sgdW.weights.view
        .zip(grad)
        .map { case (l : Double, r : Double) => l - step * r }
        .toIndexedSeq)
  }
}
