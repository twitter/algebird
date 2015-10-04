/*
Copyright 2015 Vinted.com

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

import org.roaringbitmap._

object ExactCountDistinct {
  implicit val monoid = new RoaringBitampMonoid()

  /**
   * Accurate Count-Distinct for integer values (<= 2,147,483,646)
   * Internally, this uses `RoaringBitmap`, a compressed alternative to `BitSet`.
   */
  def accurateDistinctAggregator: MonoidAggregator[Int, RoaringBitmap, Int] = {
    Aggregator.prepareMonoid { value: Int =>
      RoaringBitmap.bitmapOf(value)
    }.andThenPresent(_.getCardinality)
  }
}

class RoaringBitampSemigroup extends Semigroup[RoaringBitmap] {
  def plus(l: RoaringBitmap, r: RoaringBitmap): RoaringBitmap = RoaringBitmap.or(l, r)
}

class RoaringBitampMonoid extends RoaringBitampSemigroup with Monoid[RoaringBitmap] {
  def zero = new RoaringBitmap()
}
