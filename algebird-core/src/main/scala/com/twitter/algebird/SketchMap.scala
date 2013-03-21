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
class SketchMapMonoid[K, V](eps: Double, delta: Double, seed: Int, heavyHittersCount: Int)
                           (implicit serialization: K => Array[Byte], valueOrdering: Ordering[V], monoid: Monoid[V])
extends Monoid[SketchMap[K, V]] {

  val hashes: Seq[SketchMapHash[K]] = {
    val r = new scala.util.Random(seed)
    val numHashes = SketchMap.depth(delta)
    val numCounters = SketchMap.width(eps)
    (0 to (numHashes - 1)).map { _ =>
      SketchMapHash[K](CMSHash(r.nextInt, 0, numCounters), seed)
    }
  }

  /**
   * All Sketch Maps created with this monoid will have the same parameter configuration.
   */
  val params: SketchMapParams[K, V] = SketchMapParams[K, V](hashes, eps, delta, heavyHittersCount)

  /**
   * A zero Sketch Map is one with zero elements.
   */
  val zero: SketchMap[K, V] = SketchMap[K, V](params, SketchMapValuesTable[V](params.depth, params.width), Nil, monoid.zero)

  /**
   * We assume the Sketch Map on the left and right use the same hash functions.
   */
  def plus(left: SketchMap[K, V], right: SketchMap[K, V]): SketchMap[K, V] = left ++ right

  /**
   * Create a Sketch Map sketch out of a single key.
   */
  def create(key: K, value: V): SketchMap[K, V] = zero + (key, value)

  /**
   * Create a Sketch Map sketch from a sequence of pairs.
   */
  def create(data: Seq[(K, V)]): SketchMap[K, V] = {
    data.foldLeft(zero) { case (acc, (key, value)) =>
      plus(acc, create(key, value))
    }
  }
}


object SketchMap {
  /**
   * Functions to translate between (eps, delta) and (depth, width). The translation is:
   * depth = ceil(ln 1/delta)
   * width = ceil(e / eps)
   */
  def eps(width: Int) = scala.math.exp(1.0) / width
  def delta(depth: Int) = 1.0 / scala.math.exp(depth)
  def depth(delta: Double) = scala.math.ceil(scala.math.log(1.0 / delta)).toInt
  def width(eps: Double) = scala.math.ceil(scala.math.exp(1) / eps).toInt

  /**
   * Generates a monoid used to create SketchMap instances. Requires a
   * serialization from K to Array[Byte] for hashing, an ordering for V, and a
   * monoid for V.
   */
  def monoid[K, V](eps: Double, delta: Double, seed: Int, heavyHittersCount: Int)
                  (implicit serialization: K => Array[Byte], valueOrdering: Ordering[V], monoid: Monoid[V]): SketchMapMonoid[K, V] = {
    new SketchMapMonoid[K, V](eps, delta, seed, heavyHittersCount)(serialization, valueOrdering, monoid)
  }
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
case class SketchMap[K, V](
  val params: SketchMapParams[K, V],
  val valuesTable: SketchMapValuesTable[V],
  val heavyHitterKeys: List[K],
  val totalValue: V
)(implicit ordering: Ordering[V], monoid: Monoid[V]) extends java.io.Serializable {

  /**
   * All of the Heavy Hitter frequencies calculated all at once.
   */
  private val heavyHittersMapping: Map[K, V] = calculateHeavyHittersMapping(heavyHitterKeys, valuesTable)

  /**
   * Ordering used to sort keys by its value. We use the reverse implicit
   * ordering on V because we want the hold the "biggest" values.
   */
  private implicit val keyValueOrdering = Ordering.by[K, V] { heavyHittersMapping(_) } reverse

  def eps: Double = params.eps
  def delta: Double = params.delta

  /**
   * Returns a sorted list of heavy hitter key/value tuples.
   */
  def heavyHitters: List[(K, V)] = heavyHitterKeys.map { item => (item, heavyHittersMapping(item)) }

  /**
   * Calculates the frequencies for every heavy hitter.
   */
  private def calculateHeavyHittersMapping(keys: Iterable[K], table: SketchMapValuesTable[V]): Map[K, V] = {
    keys.map { item: K => (item, frequency(item, table)) } toMap
  }

  /**
   * Calculates the frequency for a key given a values table.
   */
  private def frequency(key: K, table: SketchMapValuesTable[V]): V = {
    val estimates = table.values.zipWithIndex.map { case (row, i) =>
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
      table + (pos, value)
    }

    SketchMap(params, newValuesTable, updatedHeavyHitters(newHeavyHitters, newValuesTable), Monoid.plus(totalValue, value))
  }

  /**
   * Returns a new Sketch Map summed with another Sketch Map. These should have
   * the same parameters, and be generated from the same monoid.
   */
  def ++(other: SketchMap[K, V]): SketchMap[K, V] = {
    val newValuesTable = valuesTable ++ other.valuesTable
    val newHeavyHitters = (heavyHitterKeys ++ other.heavyHitterKeys).distinct

    SketchMap(params, newValuesTable, updatedHeavyHitters(newHeavyHitters, newValuesTable), Monoid.plus(totalValue, other.totalValue))
  }

  /**
   * Returns a new set of sorted and concatenated heavy hitters given an
   * arbitrary list of keys.
   */
  private def updatedHeavyHitters(hitters: Seq[K], table: SketchMapValuesTable[V]): List[K] = {
    val mapping = calculateHeavyHittersMapping(hitters, table)
    val specificOrdering = Ordering.by[K, V] { mapping(_) } reverse

    hitters.sorted(specificOrdering).take(params.heavyHittersCount).toList
  }
}


/**
 * Convenience class for holding constant parameters of a Sketch Map.
 */
case class SketchMapParams[K, V](hashes: Seq[SketchMapHash[K]], eps: Double, delta: Double, heavyHittersCount: Int) {
  assert(0 < eps && eps < 1, "eps must lie in (0, 1)")
  assert(0 < delta && delta < 1, "delta must lie in (0, 1)")
  assert(0 <= heavyHittersCount , "heavyHittersCount must be greater than 0")

  val depth = SketchMap.depth(delta)
  val width = SketchMap.width(eps)
}


/**
 * Hashes an arbitrary key type to one that the Sketch Map can use.
 */
case class SketchMapHash[T](hasher: CMSHash, seed: Int)
                           (implicit serialization: T => Array[Byte]) extends Function1[T, Int] {
  def apply(obj: T): Int = {
    val hashKey: Long = MurmurHash128(seed)(serialization(obj)) match {
      case (first: Long, second: Long) => (first ^ second)
    }

    hasher(hashKey)
  }
}


/**
 * The 2-dimensional table of values used in the Sketch Map.
 * Each row corresponds to a particular hash function.
 */
object SketchMapValuesTable {
  /**
   * Creates a new SketchMapValuesTable with counts initialized to all zeroes.
   */
  def apply[V](depth: Int, width: Int)(implicit monoid: Monoid[V]): SketchMapValuesTable[V] = {
    SketchMapValuesTable(AdaptiveVector.fill(depth)(AdaptiveVector.fill[V](width)(monoid.zero)))
  }
}

case class SketchMapValuesTable[V](values: AdaptiveVector[AdaptiveVector[V]])(implicit monoid: Monoid[V]) {
  assert(depth > 0, "Table must have at least 1 row.")
  assert(width > 0, "Table must have at least 1 column.")

  def depth: Int = values.size
  def width: Int = values(0).size

  def getValue(pos: (Int, Int)): V = {
    val (row, col) = pos

    assert(row < depth && col < width, "Position must be within the bounds of this table.")

    values(row)(col)
  }

  /**
   * Updates the value of a single cell in the table.
   */
  def +(pos: (Int, Int), value: V): SketchMapValuesTable[V] = {
    val (row, col) = pos
    val currValue: V = getValue(pos)
    val newValues = values.updated(row, values(row).updated(col, Monoid.plus(currValue, value)))

    SketchMapValuesTable[V](newValues)
  }

  /**
   * Adds another values table to this one, through elementwise addition.
   */
  def ++(other: SketchMapValuesTable[V]): SketchMapValuesTable[V] = {
    assert((depth, width) == (other.depth, other.width), "Tables must have the same dimensions.")

    implicit val innerMonoid: Monoid[AdaptiveVector[V]] = AdaptiveVector.monoid[V]
    val vectorMonoid: Monoid[AdaptiveVector[AdaptiveVector[V]]] = AdaptiveVector.monoid[AdaptiveVector[V]]
    val result = vectorMonoid.plus(values, other.values)

    SketchMapValuesTable[V](result)
  }
}

