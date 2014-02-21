/*
Copyright 2014 Twitter, Inc.

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

import scala.collection.breakOut

/**
 * A Sketch Map is a generalized version of the Count-Min Sketch that is an
 * approximation of Map[K, V] that stores reference to top heavy hitters. The
 * Sketch Map can approximate the sums of any summable value that has a monoid.
 */

/**
 * Responsible for creating instances of SketchMap.
 */
class SketchMapMonoid[K, V](val params: SketchMapParams[K])
                            (implicit valueOrdering: Ordering[V], monoid: Monoid[V])
                            extends Monoid[SketchMap[K, V]] {

  /**
   * A zero Sketch Map is one with zero elements.
   */
  val zero: SketchMap[K, V] = SketchMap[K, V](AdaptiveMatrix.monoid.zero, Nil, monoid.zero)

  override def plus(left: SketchMap[K, V], right: SketchMap[K, V]): SketchMap[K, V] = {
    val newValuesTable = Monoid.plus(left.valuesTable, right.valuesTable)
    val newHeavyHitters = left.heavyHitterKeys.toSet ++ right.heavyHitterKeys

    SketchMap(
      newValuesTable,
      params.updatedHeavyHitters(newHeavyHitters.toSeq, newValuesTable),
      Monoid.plus(left.totalValue, right.totalValue))
  }

  /**
   * Create a Sketch Map sketch out of a single key/value pair.
   */
  def create(pair: (K, V)): SketchMap[K, V] = create(Seq(pair))

  /**
   * Create a Sketch Map sketch from a sequence of pairs.
   */
  def create(data: Seq[(K, V)]): SketchMap[K, V] = {
    val heavyHitters = data.map { _._1 }
    val totalValue = Monoid.sum(data.map { _._2 })
    val initTable = AdaptiveMatrix.fill[V](params.depth, params.width)(monoid.zero)
    /* For each row, update the table for each K,V pair */
    val newTable = (0 to (params.depth - 1)).foldLeft(initTable) { case (table, row) =>
      data.foldLeft(table) { case (innerTable, (key, value)) =>
        val pos = (row, params.hashes(row)(key))
        val currValue: V = innerTable.getValue(pos)
        innerTable.updated(pos, Monoid.plus(currValue, value))
      }
    }

    SketchMap(newTable, params.updatedHeavyHitters(heavyHitters, newTable), totalValue)
  }

  /**
   * Calculates the approximate frequency for any key.
   */
  def frequency(sm: SketchMap[K,V], key: K): V =
    // If the key is a heavy hitter, then use the precalculated heavy hitters mapping.
    // Otherwise, calculate it normally.
    heavyHittersMapping(sm).getOrElse(key, params.frequency(key, sm.valuesTable))

  /**
   * All of the Heavy Hitter frequencies calculated all at once.
   */
  private def heavyHittersMapping(sm: SketchMap[K,V]): Map[K, V] =
    params.calculateHeavyHittersMapping(sm.heavyHitterKeys, sm.valuesTable)

  /**
   * Returns a sorted list of heavy hitter key/value tuples.
   */
  def heavyHitters(sm: SketchMap[K,V]) : List[(K, V)] =
    sm.heavyHitterKeys.map { item => (item, heavyHittersMapping(sm).getOrElse(item, monoid.zero)) }
}


/**
 * Convenience class for holding constant parameters of a Sketch Map.
 */
case class SketchMapParams[K](seed: Int, eps: Double, delta: Double, heavyHittersCount: Int)
    (implicit hash: Injection[K, Hash64]) {

  def width = SketchMapParams.width(eps)
  def depth = SketchMapParams.depth(delta)

  assert(0 < width, "width must be greater than 0")
  assert(0 < depth, "depth must be greater than 0")
  assert(0 <= heavyHittersCount , "heavyHittersCount must be greater than 0")

  lazy val hashes: Seq[Injection[K, Hash32]] = {
    val r = new scala.util.Random(seed)
    val numHashes = depth
    val numCounters = width
    (0 to (numHashes - 1)).map { _ => hash andThen Hash.universal(r.nextInt, 0, numCounters) }
  }

  /**
   * Calculates the frequencies for every heavy hitter.
   */
  def calculateHeavyHittersMapping[V:Ordering](keys: Iterable[K], table: AdaptiveMatrix[V]): Map[K, V] =
    keys.map { (item: K) => (item, frequency(item, table)) }(breakOut)

  /**
   * Calculates the frequency for a key given a values table.
   */
  def frequency[V:Ordering](key: K, table: AdaptiveMatrix[V]): V =
    hashes
      .view
      .zip(table.rowsByColumns)
      .map { case (hash, row) => row(hash(key)) }
      .min

  /**
   * Returns a new set of sorted and concatenated heavy hitters given an
   * arbitrary list of keys.
   */
  def updatedHeavyHitters[V:Ordering](hitters: Seq[K], table: AdaptiveMatrix[V]): List[K] = {
    val mapping = calculateHeavyHittersMapping(hitters, table)
    val specificOrdering = Ordering.by[K, V] { mapping(_) } reverse

    hitters.sorted(specificOrdering).take(heavyHittersCount).toList
  }
}

object SketchMapParams {
  /**
   * Functions to translate between (eps, delta) and (depth, width). The translation is:
   * depth = ceil(ln 1/delta)
   * width = ceil(e / eps)
   */
  def eps(width: Int): Double = scala.math.exp(1.0) / width
  def delta(depth: Int): Double = 1.0 / scala.math.exp(depth)
  def width(eps: Double): Int = scala.math.ceil(scala.math.exp(1) / eps).toInt
  def depth(delta: Double): Int = scala.math.ceil(scala.math.log(1.0 / delta)).toInt
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
   * Generates a monoid used to create SketchMap instances. Requires a
   * serialization from K to Array[Byte] for hashing, an ordering for V, and a
   * monoid for V.
   */
  def monoid[K, V](params: SketchMapParams[K])(implicit serialization: K => Array[Byte], valueOrdering: Ordering[V], monoid: Monoid[V]): SketchMapMonoid[K, V] = {
    new SketchMapMonoid(params)(valueOrdering, monoid)
  }

  def aggregator[K, V](params: SketchMapParams[K])
                      (implicit serialization: K => Array[Byte], valueOrdering: Ordering[V], monoid: Monoid[V]): SketchMapAggregator[K, V] = {
    SketchMapAggregator(params, SketchMap.monoid(params))
  }
}

case class SketchMap[K, V](
  val valuesTable: AdaptiveMatrix[V],
  val heavyHitterKeys: List[K],
  val totalValue: V
) extends java.io.Serializable

/**
  * An Aggregator for the SketchMap.
  * Can be created using SketchMap.aggregator
  */
case class SketchMapAggregator[K, V](params: SketchMapParams[K], skmMonoid : SketchMapMonoid[K, V])(implicit valueOrdering: Ordering[V], valueMonoid: Monoid[V]) extends MonoidAggregator[(K, V), SketchMap[K, V], SketchMap[K, V]] {
  val monoid = skmMonoid

  def prepare(value: (K,V)) = monoid.create(value)
  def present(skm: SketchMap[K, V]) = skm
}
