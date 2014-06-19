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

class HashingTrickMonoid[V: Group](bits: Int, seed: Int = 123456) extends Monoid[AdaptiveVector[V]] {
  val vectorSize = 1 << bits
  val bitMask = vectorSize - 1
  val hash = MurmurHash128(seed)

  val zero = AdaptiveVector.fill[V](vectorSize)(Monoid.zero[V])

  def plus(left: AdaptiveVector[V], right: AdaptiveVector[V]) = Monoid.plus(left, right)

  def init[K <% Array[Byte]](kv: (K, V)): AdaptiveVector[V] = {
    val (long1, long2) = hash(kv._1)
    val index = (long1 & bitMask).toInt
    val isNegative = (long2 & 1) == 1

    val signedValue = if (isNegative) Group.negate(kv._2) else kv._2
    AdaptiveVector.fromMap[V](Map(index -> signedValue), Monoid.zero[V], vectorSize)
  }
}
