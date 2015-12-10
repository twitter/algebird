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

package com.twitter.algebird.maps.prefixsum

import math.Ordering

import scala.collection.SortedMap

import com.twitter.algebird.{ Monoid, MonoidAggregator }

import com.twitter.algebird.maps.redblack.tree._
import com.twitter.algebird.maps.ordered._
import com.twitter.algebird.maps.ordered.tree.DataMap

package tree {
  import com.twitter.algebird.maps.ordered.tree._

  /** Base trait for R/B nodes supporting prefix-sum query */
  trait NodePS[K, V, P] extends NodeMap[K, V] {
    /** Aggregator that defines semantics of prefix sum */
    def prefixAggregator: MonoidAggregator[V, P, P]

    /**
     * Obtain the prefix (cumulative) sum of values <= a given key 'k'.
     * If 'open' is true, sums the open interval for keys strictly < k.
     * If 'k' is not present in the map, then the sum for keys < k is returned.
     */
    final def prefixSum(k: K, open: Boolean = false): P = pfSum(k, prefixAggregator.monoid.zero, open)

    private[tree] def pfSum(k: K, sum: P, open: Boolean): P

    // this has to be public to support injection
    def pfs: P
  }

  /** Leaf node for R/B nodes supporting prefix-sum query */
  trait LNodePS[K, V, P] extends NodePS[K, V, P] with LNodeMap[K, V] {
    final def pfSum(k: K, sum: P, open: Boolean) = sum
    final def pfs = prefixAggregator.monoid.zero
  }

  /** Internal node for R/B nodes supporting prefix-sum query */
  trait INodePS[K, V, P] extends NodePS[K, V, P] with INodeMap[K, V] {
    def lsub: NodePS[K, V, P]
    def rsub: NodePS[K, V, P]

    def prefix: P

    final def pfSum(k: K, sum: P, open: Boolean) =
      if (keyOrdering.lt(k, data.key))
        lsub.pfSum(k, sum, open)
      else if (keyOrdering.gt(k, data.key))
        rsub.pfSum(k, prefixAggregator.append(prefixAggregator.reduce(sum, lsub.pfs), data.value), open)
      else if (open)
        prefixAggregator.reduce(sum, lsub.pfs)
      else
        prefixAggregator.append(prefixAggregator.reduce(sum, lsub.pfs), data.value)

    final def pfs = prefix
  }
}

import tree._

package infra {
  import com.twitter.algebird.maps.ordered.tree.DataMap

  /** Dependency injection for [[PrefixSumMap]].  Supplies implementations of all abstract methods */
  class Inject[K, V, P](val keyOrdering: Ordering[K], val prefixAggregator: MonoidAggregator[V, P, P])
    extends Serializable {
    def iNode(clr: Color, dat: Data[K], ls: Node[K], rs: Node[K]) =
      new Inject[K, V, P](keyOrdering, prefixAggregator) with INodePS[K, V, P] with PrefixSumMap[K, V, P] {
        // INode[K]
        val color = clr
        val lsub = ls.asInstanceOf[NodePS[K, V, P]]
        val rsub = rs.asInstanceOf[NodePS[K, V, P]]
        val data = dat.asInstanceOf[DataMap[K, V]]
        // INodePS[K, V, P]
        val prefix = prefixAggregator.append(prefixAggregator.reduce(lsub.pfs, rsub.pfs), data.value)
      }
  }

}

import infra._

/**
 * An inheritable and mixable trait for adding prefix sum query to ordered maps
 * @tparam K The key type
 * @tparam V The value type
 * @tparam P The prefix sum type
 * @tparam IN The node type of the concrete internal R/B tree subclass
 * @tparam M The map self-type of the concrete map subclass
 */
trait PrefixSumMapLike[K, V, P, IN <: INodePS[K, V, P], M <: PrefixSumMapLike[K, V, P, IN, M] with SortedMap[K, V]]
  extends NodePS[K, V, P] with OrderedMapLike[K, V, IN, M] {

  /**
   * A container of all prefix sums over the stored values.  If 'open' is true,
   * the sums will be for strictly < each key.
   */
  def prefixSums(open: Boolean = false): Iterable[P] = prefixSumsIterator(open).toIterable

  /**
   * Iterate over prefix sums for stored values.  If 'open' is true,
   * the sums will be for strictly < each key.
   */
  def prefixSumsIterator(open: Boolean = false): Iterator[P] = {
    val itr = valuesIterator.scanLeft(prefixAggregator.monoid.zero)((p, e) => prefixAggregator.append(p, e))
    if (open) itr.takeWhile(_ => itr.hasNext) else itr.drop(1)
  }

  /** equivalent to prefixSum of the right-most key */
  def sum: P = this match {
    case n: INodePS[K, V, P] => n.prefix
    case _ => prefixAggregator.monoid.zero
  }
}

sealed trait PrefixSumMap[K, V, P] extends SortedMap[K, V]
  with PrefixSumMapLike[K, V, P, INodePS[K, V, P], PrefixSumMap[K, V, P]] {

  override def empty = PrefixSumMap.key(keyOrdering).value[V].prefix(prefixAggregator)

  override def toString =
    "PrefixSumMap(" +
      iterator.zip(prefixSumsIterator())
      .map(x => s"${x._1._1} -> (${x._1._2}, ${x._2})").mkString(", ") +
      ")"
}

object PrefixSumMap {
  /**
   * Instantiate a new empty PrefixSumMap from key, value and prefix types
   * {{{
   * import com.twitter.algebird.maps.prefixsum._
   *
   * // map strings to integers, using default ordering and standard integer monoid
   * val map1 = PrefixSumMap.key[String].value[Int].prefix(Aggregator.appendMonoid((ps: Int, v: Int) => ps + v))
   * // Use a custom ordering
   * val ord: Ordering[String] = ...
   * val map2 = PrefixSumMap.key(ord).value[Int].prefix(Aggregator.appendMonoid((ps: Int, v: Int) => ps + v))
   * }}}
   */
  def key[K](implicit ord: Ordering[K]): infra.GetValue[K] = infra.GetValue(ord)

  object infra {
    /** Mediating class between key method and value method */
    case class GetValue[K](ord: Ordering[K]) {
      def value[V]: GetPrefix[K, V] = GetPrefix[K, V](ord)
    }

    /** Mediating class between value method and prefix method */
    case class GetPrefix[K, V](ord: Ordering[K]) {
      def prefix[P](implicit agg: MonoidAggregator[V, P, P]): PrefixSumMap[K, V, P] =
        new Inject[K, V, P](ord, agg) with LNodePS[K, V, P] with PrefixSumMap[K, V, P]
    }
  }
}
