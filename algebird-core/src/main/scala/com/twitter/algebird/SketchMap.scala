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
import com.twitter.algebird.CMSHasherImplicits._
import com.twitter.algebird.matrix.AdaptiveMatrix

/**
 * A Sketch Map is a generalized version of the Count-Min Sketch that is an
 * approximation of Map[K, V] that stores reference to top heavy hitters. The
 * Sketch Map can approximate the sums of any summable value that has a monoid.
 */

/**
 * Hashes an arbitrary key type to one that the Sketch Map can use.
 */
case class SketchMapHash[K](hasher: CMSHash[Long], seed: Int)(implicit serialization: K => Array[Byte]) {
  def apply(obj: K): Int = {
    val (first, second) = MurmurHash128(seed)(serialization(obj))
    hasher(first ^ second)
  }
}

/**
 * Responsible for creating instances of SketchMap.
 */
class SketchMapMonoid[K, V](val params: SketchMapParams[K])(implicit valueOrdering: Ordering[V], monoid: Monoid[V])
  extends Monoid[SketchMap[K, V]] {

  /**
   * A zero Sketch Map is one with zero elements.
   */
  val zero: SketchMap[K, V] = SketchMap(AdaptiveMatrix.fill(params.depth, params.width)(monoid.zero), Nil, monoid.zero)

  override def plus(left: SketchMap[K, V], right: SketchMap[K, V]): SketchMap[K, V] = {
    val newValuesTable = Monoid.plus(left.valuesTable, right.valuesTable)
    val newHeavyHitters = left.heavyHitterKeys.toSet ++ right.heavyHitterKeys

    SketchMap(
      newValuesTable,
      params.updatedHeavyHitters(newHeavyHitters.toSeq, newValuesTable),
      Monoid.plus(left.totalValue, right.totalValue))
  }

  override def sumOption(items: TraversableOnce[SketchMap[K, V]]): Option[SketchMap[K, V]] =
    if (items.isEmpty) None
    else {
      val buffer = scala.collection.mutable.Buffer[SketchMap[K, V]]()
      val maxBuffer = 1000
      def sumBuffer: Unit = {
        val newValuesTable = Monoid.sum(buffer.iterator.map(_.valuesTable))
        val heavyHittersSet = Monoid.sum(buffer.iterator.map(_.heavyHitterKeys.toSet))
        val newtotalValue = Monoid.sum(buffer.iterator.map(_.totalValue))
        buffer.clear()
        buffer += SketchMap(
          newValuesTable,
          params.updatedHeavyHitters(heavyHittersSet.toSeq, newValuesTable),
          newtotalValue)
      }

      items.foreach { sm =>
        if (buffer.size > maxBuffer) sumBuffer
        buffer += sm
      }
      if (buffer.size > 1) sumBuffer //don't bother to sum if there is only one item.
      Some(buffer(0))
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
    val newTable = (0 to (params.depth - 1)).foldLeft(initTable) {
      case (table, row) =>
        data.foldLeft(table) {
          case (innerTable, (key, value)) =>
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
  def frequency(sm: SketchMap[K, V], key: K): V =
    params.frequency(key, sm.valuesTable)

  def frequencyWithHHCache(sm: SketchMap[K, V]): K => V = {
    val hhMap: Map[K, V] = heavyHitters(sm).toMap
    (k: K) => hhMap.getOrElse(k, frequency(sm, k))
  }

  /**
   * Returns a sorted list of heavy hitter key/value tuples.
   */
  def heavyHitters(sm: SketchMap[K, V]): List[(K, V)] =
    sm.heavyHitterKeys.map { item => (item, frequency(sm, item)) }
}

/**
 * Convenience class for holding constant parameters of a Sketch Map.
 */
case class SketchMapParams[K](seed: Int, width: Int, depth: Int, heavyHittersCount: Int)(implicit serialization: K => Array[Byte]) {
  assert(0 < width, "width must be greater than 0")
  assert(0 < depth, "depth must be greater than 0")
  assert(0 <= heavyHittersCount, "heavyHittersCount must be greater than 0")

  lazy val hashes: Seq[K => Int] = {
    val r = new scala.util.Random(seed)
    val numHashes = depth
    val numCounters = width
    (0 to (numHashes - 1)).map { _ =>
      val smhash: SketchMapHash[K] = SketchMapHash(CMSHash[Long](r.nextInt, 0, numCounters), seed)(serialization)
      new (K => Int) { override def apply(k: K) = smhash(k) }
    }
  }

  /**
   * Calculates the frequency for a key given a values table.
   */
  def frequency[V: Ordering](key: K, table: AdaptiveMatrix[V]): V =
    hashes
      .iterator
      .zipWithIndex
      .map { case (hash, row) => table.getValue(row, hash(key)) }
      .min

  /**
   * Returns a new set of sorted and concatenated heavy hitters given an
   * arbitrary list of keys.
   */
  def updatedHeavyHitters[V: Ordering](hitters: Seq[K], table: AdaptiveMatrix[V]): List[K] = {
    val mapping: Map[K, V] = hitters.map(item => (item, frequency(item, table)))(breakOut)
    val specificOrdering = Ordering.by[K, V] { mapping(_) }.reverse
    hitters.sorted(specificOrdering).take(heavyHittersCount).toList
  }
}

object SketchMapParams {
  /**
   * Overloaded apply method for convenience.
   */
  def apply[K](seed: Int, eps: Double, delta: Double, heavyHittersCount: Int)(implicit serialization: K => Array[Byte]): SketchMapParams[K] =
    SketchMapParams[K](seed, width(eps), depth(delta), heavyHittersCount)(serialization)
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
  def monoid[K, V](params: SketchMapParams[K])(implicit valueOrdering: Ordering[V], monoid: Monoid[V]): SketchMapMonoid[K, V] =
    new SketchMapMonoid(params)(valueOrdering, monoid)

  def aggregator[K, V](params: SketchMapParams[K])(implicit valueOrdering: Ordering[V], monoid: Monoid[V]): SketchMapAggregator[K, V] =
    SketchMapAggregator(params, SketchMap.monoid(params))
}

case class SketchMap[K, V](
  val valuesTable: AdaptiveMatrix[V],
  val heavyHitterKeys: List[K],
  val totalValue: V) extends java.io.Serializable

/**
 * An Aggregator for the SketchMap.
 * Can be created using SketchMap.aggregator
 */
case class SketchMapAggregator[K, V](params: SketchMapParams[K], skmMonoid: SketchMapMonoid[K, V])(implicit valueOrdering: Ordering[V], valueMonoid: Monoid[V]) extends MonoidAggregator[(K, V), SketchMap[K, V], SketchMap[K, V]] {
  val monoid = skmMonoid

  def prepare(value: (K, V)) = monoid.create(value)
  def present(skm: SketchMap[K, V]) = skm
}
