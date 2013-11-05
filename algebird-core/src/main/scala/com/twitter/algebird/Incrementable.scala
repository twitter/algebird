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
 * This is a monoid to represent things which increase. Note that it is important
 * that a value after being incremented is always larger than it was before.
 */
trait Incrementable[@specialized(Int,Long,Float,Double) T] extends Monoid[T] {
  def one: T
  def increment(old: T) = plus(old, one)
  def ordering: Ordering[T]
}

object Incrementable {
  implicit val intIncrementable = new Incrementable[Int] {
    val zero = Monoid.intMonoid.zero
    def plus(left: Int, right: Int) = Monoid.intMonoid.plus(left, right)
    val one = 1
    val ordering = Ordering.Int
  }

  implicit val longIncrementable = new Incrementable[Long] {
    val zero = Monoid.longMonoid.zero
    def plus(left: Long, right: Long) = Monoid.longMonoid.plus(left, right)
    val one = 1L
    val ordering = Ordering.Long
  }

  implicit val floatIncrementable = new Incrementable[Float] {
    val zero = Monoid.floatMonoid.zero
    def plus(left: Float, right: Float) = Monoid.floatMonoid.plus(left, right)
    val one = 1.0f
    val ordering = Ordering.Float
  }

  implicit val doubleIncrementable = new Incrementable[Double] {
    val zero = Monoid.doubleMonoid.zero
    def plus(left: Double, right: Double) = Monoid.doubleMonoid.plus(left, right)
    val one = 1.0
    val ordering = Ordering.Double
  }
}
