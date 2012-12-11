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
    DecayedVector(vector, math.pow(2.0, time / halfLife))
  }

  def monoidWithEpsilon[F, C[_]](eps: Double)
      (implicit vs: VectorSpace[F, C], metric: Metric[C[F]], ord: Ordering[F]) = new Monoid[DecayedVector[F, C]] {
    override val zero = DecayedVector(vs.group.zero, vs.field.zero)
    override def plus(left: DecayedVector[F, C], right: DecayedVector[F, C]) = {
      if(ord.compare(left.expTime, right.expTime) <= 0) {
        scaledPlus(right, left, eps)
      } else {
        scaledPlus(left, right, eps)
      }
    }
  }

  def forMap[K,F](m: Map[K, F], exptime: F) = DecayedVector[F, ({type x[a]=Map[K, a]})#x](m, exptime)
  def forMapWithHalflife[K](m: Map[K, Double], time: Double, halfLife: Double) =
    forMap(m, math.pow(2.0, time / halfLife))

  def mapMonoidWithEpsilon[K, F](eps: Double)
      (implicit vs: VectorSpace[F, ({type x[a]=Map[K, a]})#x], metric: Metric[Map[K, F]], ord: Ordering[F]) =
    monoidWithEpsilon[F, ({type x[a]=Map[K, a]})#x](eps)

  // This is the default monoid that never thresholds.
  // If you want to set a specific accuracy you need to implicitly override this
  implicit def monoid[F, C[_]](implicit vs: VectorSpace[F, C], metric: Metric[C[F]], ord: Ordering[F]) = monoidWithEpsilon(-1.0)
  implicit def mapMonoid[K, F]
      (implicit vs: VectorSpace[F, ({type x[a]=Map[K, a]})#x], metric: Metric[Map[K, F]], ord: Ordering[F]) =
    mapMonoidWithEpsilon(-1.0)

  def scaledPlus[F, C[_]](newVal: DecayedVector[F, C], oldVal: DecayedVector[F, C], eps: Double)
      (implicit vs: VectorSpace[F, C], metric: Metric[C[F]]): DecayedVector[F, C] = {
    implicit val mon: Monoid[C[F]] = vs.group
    val expTime = vs.field.div(oldVal.expTime, newVal.expTime)
    val newVector = Monoid.plus(newVal.vector, vs.scale(expTime, oldVal.vector))
    if(eps < 0.0 || Metric.norm(newVector) > eps) {
      DecayedVector(newVector, newVal.expTime)
    } else {
      DecayedVector(mon.zero, vs.field.zero)
    }
  }
}

case class DecayedVector[F, C[_]](vector: C[F], expTime: F)
