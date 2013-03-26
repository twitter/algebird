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
 * A Sketch Map is a generalized version of the Count-Min Sketch that is an
 * approximation of Map[K, V] that stores reference to top heavy hitters. The
 * Sketch Map can approximate the sums of any summable value that has a monoid.
 */


/**
 * Responsible for creating instances of SketchMap.
 */
class SketchMapMonoid[K, V](width: Int, depth: Int, seed: Int, heavyHittersCount: Int)
                           (implicit serialization: K => Array[Byte], valueOrdering: Ordering[V], monoid: Monoid[V])
extends Monoid[SketchMap[K, V]] {
  /**
   * Hashes an arbitrary key type to one that the Sketch Map can use.
   */
  private case class SketchMapHash(hasher: CMSHash, seed: Int) extends Function1[K, Int] {
    def apply(obj: K): Int = {
      val (first, second) = MurmurHash128(seed)(serialization(obj))
      hasher(first ^ second)
    }
  }

  val hashes: Seq[K => Int] = {
    val r = new scala.util.Random(seed)
    val numHashes = depth
    val numCounters = width
    (0 to (numHashes - 1)).map { _ =>
      SketchMapHash(CMSHash(r.nextInt, 0, numCounters), seed)
    }
  }

  /**
   * All Sketch Maps created with this monoid will have the same parameter configuration.
   */
  val params: SketchMapParams[K] = SketchMapParams[K](hashes, width, depth, heavyHittersCount)

  /**
   * A zero Sketch Map is one with zero elements.
   */
  val zero: SketchMap[K, V] = SketchMap[K, V](params, AdaptiveMatrix.fill(params.depth, params.width)(monoid.zero), Nil, monoid.zero)

  /**
   * We assume the Sketch Map on the left and right use the same hash functions.
   */
  def plus(left: SketchMap[K, V], right: SketchMap[K, V]): SketchMap[K, V] = left ++ right

  /**
   * Create a Sketch Map sketch out of a single key/value pair.
   */
  def create(pair: (K, V)): SketchMap[K, V] = zero + pair

  /**
   * Create a Sketch Map sketch from a sequence of pairs.
   */
  def create(data: Seq[(K, V)]): SketchMap[K, V] = {
    data.foldLeft(zero) { case (acc, (key, value)) =>
      plus(acc, create(key, value))
    }
  }
}


/**
 * Convenience class for holding constant parameters of a Sketch Map.
 */
case class SketchMapParams[K](hashes: Seq[K => Int], width: Int, depth: Int, heavyHittersCount: Int) {
  assert(0 < width, "width must be greater than 0")
  assert(0 < depth, "depth must be greater than 0")
  assert(0 <= heavyHittersCount , "heavyHittersCount must be greater than 0")

  def eps = SketchMap.eps(width)
  def delta = SketchMap.delta(depth)
}


/**
 * Data structure representing an approximation of Map[K, V], where V has an
 * implicit ordering and monoid. This is a more generic version of
 * CountMinSketch.
 *
 * Values are stored in valuesTable, a 2D vector containing aggregated sums of
 * values inserted to the Sketch Map.
 *
 * The data structure stores top non-zero values, called Heavy Hitters. The
 * values are sorted by an implicit reverse ordering for the value, and the
 * number of heavy hitters stored is based on the heavyHittersCount set in
 * params.
 *
 * Use SketchMapMonoid to create instances of this class.
 */

object SketchMap {
  /**
   * Functions to translate between (eps, delta) and (depth, width). The translation is:
   * depth = ceil(ln 1/delta)
   * width = ceil(e / eps)
   */
  def eps(width: Int): Double = scala.math.exp(1.0) / width
  def delta(depth: Int): Double = 1.0 / scala.math.exp(depth)
  def depth(delta: Double): Int = scala.math.ceil(scala.math.log(1.0 / delta)).toInt
  def width(eps: Double): Int = scala.math.ceil(scala.math.exp(1) / eps).toInt

  /**
   * Generates a monoid used to create SketchMap instances. Requires a
   * serialization from K to Array[Byte] for hashing, an ordering for V, and a
   * monoid for V.
   */
  def monoid[K, V](eps: Double, delta: Double, seed: Int, heavyHittersCount: Int)
                  (implicit serialization: K => Array[Byte], valueOrdering: Ordering[V], monoid: Monoid[V]): SketchMapMonoid[K, V] = {
    new SketchMapMonoid(width(eps), depth(delta), seed, heavyHittersCount)(serialization, valueOrdering, monoid)
  }
}

case class SketchMap[K, V](
  val params: SketchMapParams[K],
  val valuesTable: AdaptiveMatrix[V],
  val heavyHitterKeys: List[K],
  val totalValue: V
)(implicit ordering: Ordering[V], monoid: Monoid[V]) extends java.io.Serializable {

  /**
   * Redefine the AdaptiveMatrix Monoid so it takes the implicit Monoid from
   * the SketchMap.
   */
  private implicit val valuesTableMonoid: Monoid[AdaptiveMatrix[V]] = AdaptiveMatrix.monoid[V]

  /**
   * All of the Heavy Hitter frequencies calculated all at once.
   */
  private val heavyHittersMapping: Map[K, V] = calculateHeavyHittersMapping(heavyHitterKeys, valuesTable)

  /**
   * Ordering used to sort keys by its value. We use the reverse implicit
   * ordering on V because we want the hold the "largest" values.
   */
  private implicit val keyValueOrdering: Ordering[K] = Ordering.by[K, V] { heavyHittersMapping(_) } reverse

  /**
   * These are not 100% accurate because of rounding.
   */
  def eps: Double = params.eps
  def delta: Double = params.delta

  /**
   * Returns a sorted list of heavy hitter key/value tuples.
   */
  def heavyHitters: List[(K, V)] = heavyHitterKeys.map { item => (item, heavyHittersMapping(item)) }

  /**
   * Calculates the frequencies for every heavy hitter.
   */
  private def calculateHeavyHittersMapping(keys: Iterable[K], table: AdaptiveMatrix[V]): Map[K, V] = {
    keys.map { item: K => (item, frequency(item, table)) } toMap
  }

  /**
   * Calculates the frequency for a key given a values table.
   */
  private def frequency(key: K, table: AdaptiveMatrix[V]): V = {
    val estimates = table.rowsByColumns.zipWithIndex.map { case (row, i) =>
      row(params.hashes(i)(key))
    }

    estimates.min
  }

  /**
   * Calculates the approximate frequency for any key.
   */
  def frequency(key: K): V = {
    // If the key is a heavy hitter, then use the precalculated heavy hitters mapping.
    // Otherwise, calculate it normally.
    heavyHittersMapping.get(key).getOrElse(frequency(key, valuesTable))
  }

  /**
   * Returns a new Sketch Map with a key value pair added.
   */
  def +(pair: (K, V)): SketchMap[K, V] = {
    val (key, value) = pair

    val newHeavyHitters = key :: heavyHitterKeys
    val newValuesTable = (0 to (params.depth - 1)).foldLeft(valuesTable) { case (table, row) =>
      val pos = (row, params.hashes(row)(key))
      val currValue: V = table.getValue(pos)
      table.updated(pos, Monoid.plus(currValue, value))
    }

    SketchMap(params, newValuesTable, updatedHeavyHitters(newHeavyHitters, newValuesTable), Monoid.plus(totalValue, value))
  }

  /**
   * Returns a new Sketch Map summed with another Sketch Map. These should have
   * the same parameters, and be generated from the same monoid.
   */
  def ++(other: SketchMap[K, V]): SketchMap[K, V] = {
    val newValuesTable = Monoid.plus(valuesTable, other.valuesTable)
    val newHeavyHitters = (heavyHitterKeys ++ other.heavyHitterKeys).distinct

    SketchMap(params, newValuesTable, updatedHeavyHitters(newHeavyHitters, newValuesTable), Monoid.plus(totalValue, other.totalValue))
  }

  /**
   * Returns a new set of sorted and concatenated heavy hitters given an
   * arbitrary list of keys.
   */
  private def updatedHeavyHitters(hitters: Seq[K], table: AdaptiveMatrix[V]): List[K] = {
    val mapping = calculateHeavyHittersMapping(hitters, table)
    val specificOrdering = Ordering.by[K, V] { mapping(_) } reverse

    hitters.sorted(specificOrdering).take(params.heavyHittersCount).toList
  }
}

