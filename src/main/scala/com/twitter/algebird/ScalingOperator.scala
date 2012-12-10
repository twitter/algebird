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

import java.lang.{Integer => JInt, Short => JShort, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBool}

/**
 * This class represents the operation of scaling by a double constant.
 *
 * Includes a bunch of common sense implicits.
 *
 * A ScalingOperator is the scalar multiplication part of a vector space.
 * It is expected to satisfy the following properties:
 * 1. scale(v, 0.0) == Monoid.zero
 * 2. scale(scale(v, b), a) == scale(v, a * b)
 * 3. scale(v, 1.0) == v
 * 4. scale(v, a + b) == plus(scale(v, a), scale(v, b))
 *
 */
object ScalingOperator {
  def scale[T](v: T, scalar: Double)(implicit so: ScalingOperator[T]) = so.scale(v, scalar)
  def from[T](f: (T, Double) => T) = new ScalingOperator[T] {
    def scale(v: T, scalar: Double) = f(v, scalar)
  }
  def withMonoid[T : Monoid](f: (T, Double) => T) = from{ (v: T, scalar: Double) =>
    if(!Monoid.isNonZero[T](v) || scalar == 0.0) {
      Monoid.zero[T]
    } else {
      f(v, scalar)
    }
  }

  // Implicits
  implicit val doubleScaling = withMonoid[Double]((a, b) => a * b)
  implicit val floatScaling = withMonoid[Float]((a, b) => (a * b).toFloat)
  implicit val jDoubleScaling = withMonoid[JDouble]((a, b) => a * b)
  implicit val jFloatScaling = withMonoid[JFloat]((a, b) => (a * b).toFloat.asInstanceOf[JFloat])

  implicit def optionScaling[T : ScalingOperator : Monoid] =
    withMonoid[Option[T]]{(l: Option[T], s: Double) =>
      l.map(scale(_, s))
    }

  implicit def listScaling[T : ScalingOperator : Monoid] =
    withMonoid[List[T]]{(l: List[T], s: Double) =>
      l.map(scale(_, s))
    }

  implicit def indexedSeqScaling[T : ScalingOperator : Monoid] =
    withMonoid[IndexedSeq[T]]{(l: IndexedSeq[T], s: Double) =>
      l.map(scale(_, s))
    }

  implicit def setScaling[T : ScalingOperator : Monoid] =
    withMonoid[Set[T]]{(l: Set[T], s: Double) =>
      l.map(scale(_, s))
    }

  implicit def mapScaling[K, T : ScalingOperator : Monoid] =
    withMonoid[Map[K, T]]{(l: Map[K, T], s: Double) =>
      l.map( el => (el._1, scale(el._2, s)) )
    }
}

trait ScalingOperator[T] {
  def scale(v: T, scalar: Double): T
}
