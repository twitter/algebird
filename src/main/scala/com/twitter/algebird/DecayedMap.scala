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
 * Represents a map whose values decay over time.
 * Similar in spirit to {@link DecayedValue}
 */
object DecayedMap extends java.io.Serializable {

  def build[K](innerMap: Map[K, Double], time: Double, halfLife: Double) = {
    DecayedMap(innerMap, time * scala.math.log(2.0) / halfLife)
  }

  def scale[K](newm: DecayedMap[K], oldm : DecayedMap[K], eps : Double) = {
    val oldScaledMap = oldm.innerMap.map { case(key: K, value: Double) =>
      (key, scala.math.exp(oldm.scaledTime - newm.scaledTime) * value)
    }

    val newMap = Monoid.plus(newm.innerMap, oldScaledMap)
      .filter { case(key: K, value: Double) => value > eps }

    DecayedMap(newMap, newm.scaledTime)
  }

  def monoidWithEpsilon[K](eps : Double) = new Monoid[DecayedMap[K]] {
    override val zero = DecayedMap(Map[K, Double](), Double.NegativeInfinity)
    override def plus(left : DecayedMap[K], right : DecayedMap[K]) = {
      if (left < right) {
        //left is older:
        scale(right, left, eps)
      }
      else {
        // right is older
        scale(left, right, eps)
      }
    }
  }
}

case class DecayedMap[K](innerMap: Map[K, Double], scaledTime: Double) extends Ordered[DecayedMap[K]] {
  def compare(that: DecayedMap[K]): Int = {
    scaledTime.compareTo(that.scaledTime)
  }
}
