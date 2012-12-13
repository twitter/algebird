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
 * Represents a container class together with time.
 * Its monoid consists of exponentially scaling the older value and summing with
 * the newer one.
 */
object DecayedVector {
  def buildWithHalflife[C[_]](vector: C[Double], time: Double, halfLife: Double) = {
    DecayedVector(vector, time * scala.math.log(2.0) / halfLife)
  }

  def monoidWithEpsilon[C[_]](eps: Double)
      (implicit vs: VectorSpace[Double, C], metric: Metric[C[Double]]) = new Monoid[DecayedVector[C]] {
    override val zero = DecayedVector(vs.group.zero, 0.0)
    override def plus(left: DecayedVector[C], right: DecayedVector[C]) = {
      if(left.scaledTime <= right.scaledTime) {
        scaledPlus(right, left, eps)
      } else {
        scaledPlus(left, right, eps)
      }
    }
  }

  def forMap[K](m: Map[K, Double], scaledTime: Double) = DecayedVector[({type x[a]=Map[K, a]})#x](m, scaledTime)
  def forMapWithHalflife[K](m: Map[K, Double], time: Double, halfLife: Double) =
    forMap(m, time * scala.math.log(2.0) / halfLife)

  def mapMonoidWithEpsilon[K](eps: Double)
      (implicit vs: VectorSpace[Double, ({type x[a]=Map[K, a]})#x], metric: Metric[Map[K, Double]]) =
    monoidWithEpsilon[({type x[a]=Map[K, a]})#x](eps)

  // This is the default monoid that never thresholds.
  // If you want to set a specific accuracy you need to implicitly override this
  implicit def monoid[F, C[_]](implicit vs: VectorSpace[F, C], metric: Metric[C[F]], ord: Ordering[F]) = monoidWithEpsilon(-1.0)
  implicit def mapMonoid[K]
      (implicit vs: VectorSpace[Double, ({type x[a]=Map[K, a]})#x], metric: Metric[Map[K, Double]]) =
    mapMonoidWithEpsilon(-1.0)

  def scaledPlus[C[_]](newVal: DecayedVector[C], oldVal: DecayedVector[C], eps: Double)
      (implicit vs: VectorSpace[Double, C], metric: Metric[C[Double]]): DecayedVector[C] = {
    implicit val mon: Monoid[C[Double]] = vs.group
    val expFactor = scala.math.exp(oldVal.scaledTime - newVal.scaledTime)
    val newVector = Monoid.plus(newVal.vector, vs.scale(expFactor, oldVal.vector))
    if(eps < 0.0 || Metric.norm(newVector) > eps) {
      DecayedVector(newVector, newVal.scaledTime)
    } else {
      DecayedVector(mon.zero, Double.NegativeInfinity)
    }
  }
}

case class DecayedVector[C[_]](vector: C[Double], scaledTime: Double)
