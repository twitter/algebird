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

package com.twitter.algebird.maps.ordered

import math.Ordering

import com.twitter.algebird.maps.redblack.tree._

object tree {
  /** Trees that back a map-like object have a value as well as a key */
  trait DataMap[K, V] extends Data[K] {
    val value: V

    override def hashCode = key.hashCode + value.hashCode
    override def equals(that: Any) = that match {
      case data: DataMap[K, V] => this.key.equals(data.key) && this.value.equals(data.value)
      case _ => false
    }
  }

  /**
   * Base class of ordered K/V tree node
   * @tparam K The key type
   * @tparam V The value type
   */
  trait NodeMap[K, V] extends Node[K]

  trait LNodeMap[K, V] extends NodeMap[K, V] with LNode[K]

  trait INodeMap[K, V] extends NodeMap[K, V] with INode[K] {
    val data: DataMap[K, V]
  }
}

import tree._

object infra {
  /** Iterator over internal nodes in a Red Black tree, performing in-order traversal */
  class INodeIterator[K, IN <: INode[K]](node: IN) extends Iterator[IN] {
    // At any point in time, only one iterator is stored, which is important because
    // otherwise we'd instantiate all sub-iterators over the entire tree.  This way iterators
    // get GC'd once they are spent, and only a linear stack is instantiated at any one time.
    private var state = INodeIterator.stateL
    private var itr = itrNext

    def hasNext = itr.hasNext

    def next = {
      val v = itr.next
      if (!itr.hasNext) itr = itrNext
      v
    }

    // Get the next non-empty iterator if it exists, or an empty iterator otherwise
    // Adhere to in-order state transition: left-subtree -> current -> right-subtree 
    private def itrNext = {
      var n = itrState
      while (!n.hasNext && state < INodeIterator.stateR) n = itrState
      n
    }

    // Get the iterator corresponding to next iteration state
    private def itrState = {
      val i = state match {
        case INodeIterator.stateL => INodeIterator.apply[K, IN](node.lsub) // left subtree
        case INodeIterator.stateC => Iterator.single(node) // current node
        case INodeIterator.stateR => INodeIterator.apply[K, IN](node.rsub) // right subtree
        case _ => Iterator.empty
      }
      state += 1
      i
    }
  }

  /** Factory and constants for INodeIterator */
  object INodeIterator {
    // Iteration states corresponding to in-order tree traversal 
    private[infra] val stateL = 1 // iterating over left subtree
    private[infra] val stateC = 2 // current node
    private[infra] val stateR = 3 // iterating over right subtree

    def apply[K, IN <: INode[K]](node: Node[K]) = node match {
      case n: LNode[K] => Iterator.empty
      case _ => new INodeIterator[K, IN](node.asInstanceOf[IN])
    }
  }

  class InjectSet[K](val keyOrdering: Ordering[K]) extends Serializable {
    def iNode(clr: Color, dat: Data[K], ls: Node[K], rs: Node[K]) =
      new InjectSet[K](keyOrdering) with INode[K] with OrderedSet[K] {
        // INode
        val color = clr
        val lsub = ls
        val rsub = rs
        val data = dat
      }
  }

  class InjectMap[K, V](val keyOrdering: Ordering[K]) extends Serializable {
    def iNode(clr: Color, dat: Data[K], ls: Node[K], rs: Node[K]) =
      new InjectMap[K, V](keyOrdering) with INodeMap[K, V] with OrderedMap[K, V] {
        // INode
        val color = clr
        val lsub = ls
        val rsub = rs
        val data = dat.asInstanceOf[DataMap[K, V]]
      }
  }
}

import infra._

/**
 * An inheritable (and mixable) trait representing Ordered container functionality that is
 * backed by a Red/Black tree implemenation.
 * @tparam K The key type
 * @tparam IN The internal node type of the underlying R/B tree subclass
 * @tparam M The container self-type of the concrete map subclass
 */
trait OrderedLike[K, IN <: INode[K], M <: OrderedLike[K, IN, M]] extends Node[K] {

  /** Obtain a new container with key removed */
  def -(k: K) = this.delete(k).asInstanceOf[M]

  /** Get the internal node stored at at key, or None if key is not present */
  def getNode(k: K) = this.node(k).map(_.asInstanceOf[IN])

  /** Returns true if key is present in the container, false otherwise */
  def contains(k: K) = this.node(k).isDefined

  /** A container of underlying nodes, in key order */
  def nodes = nodesIterator.toIterable

  /** Iterator over nodes, in key order */
  def nodesIterator: Iterator[IN] = INodeIterator.apply[K, IN](this)

  /** A container of keys, in key order */
  def keys = keysIterator.toIterable

  /** Iterator over keys, in key order */
  def keysIterator = nodesIterator.map(_.data.key)
}

/**
 * An inheritable (and mixable) trait representing Ordered Set functionality that is
 * backed by a Red/Black tree implemenation.
 * @tparam K The key type
 * @tparam IN The internal node type of the underlying R/B tree subclass
 * @tparam M The map self-type of the concrete map subclass
 */
trait OrderedSetLike[K, IN <: INode[K], M <: OrderedSetLike[K, IN, M]]
  extends OrderedLike[K, IN, M] with Iterable[K] {

  /** Obtain a new container with key inserted */
  def +(k: K) = this.insert(
    new Data[K] {
      val key = k
    }).asInstanceOf[M]

  /** Iterator over keys, in key order */
  def iterator: Iterator[K] = nodesIterator.map(_.data.key)

  override def hashCode = scala.util.hashing.MurmurHash3.orderedHash(nodesIterator.map(_.data))
  override def equals(that: Any) = that match {
    case coll: OrderedSetLike[K, IN, M] => coll.sameElements(this)
    case _ => false
  }
}

/**
 * An inheritable (and mixable) trait representing Ordered Map functionality that is
 * backed by a Red/Black tree implemenation.
 * @tparam K The key type
 * @tparam V The value type
 * @tparam IN The internal node type of the underlying R/B tree subclass
 * @tparam M The map self-type of the concrete map subclass
 */
trait OrderedMapLike[K, V, IN <: INodeMap[K, V], M <: OrderedMapLike[K, V, IN, M]]
  extends NodeMap[K, V] with OrderedLike[K, IN, M] with Iterable[(K, V)] {

  /** Obtain a new map with a (key, val) pair inserted */
  def +(kv: (K, V)) = this.insert(
    new DataMap[K, V] {
      val key = kv._1
      val value = kv._2
    }).asInstanceOf[M]

  /** Get the value stored at a key, or None if key is not present */
  def get(k: K) = this.getNode(k).map(_.data.value)

  /** Iterator over (key,val) pairs, in key order */
  def iterator: Iterator[(K, V)] = nodesIterator.map(n => ((n.data.key, n.data.value)))

  /** Container of values, in key order */
  def values = valuesIterator.toIterable

  /** Iterator over values, in key order */
  def valuesIterator = nodesIterator.map(_.data.value)

  override def hashCode = scala.util.hashing.MurmurHash3.orderedHash(nodesIterator.map(_.data))
  override def equals(that: Any) = that match {
    case coll: OrderedMapLike[K, V, IN, M] => coll.sameElements(this)
    case _ => false
  }
}

sealed trait OrderedSet[K] extends OrderedSetLike[K, INode[K], OrderedSet[K]] {
  override def toString =
    "OrderedSet(" +
      nodesIterator.map(n => s"${n.data.key}").mkString(", ") +
      ")"
}
sealed trait OrderedMap[K, V] extends OrderedMapLike[K, V, INodeMap[K, V], OrderedMap[K, V]] {
  override def toString =
    "OrderedMap(" +
      nodesIterator.map(n => s"${n.data.key} -> ${n.data.value}").mkString(", ") +
      ")"
}

object OrderedSet {
  /**
   * Instantiate a new empty OrderedSet from key and value types
   * {{{
   * import com.twitter.algebird.maps.ordered._
   *
   * // map strings to integers, using default string ordering
   * val set1 = OrderedSet.key[String]
   * // Use a custom ordering
   * val ord: Ordering[String] = ...
   * val map2 = OrderedSet.key(ord)
   * }}}
   */
  def key[K](implicit ord: Ordering[K]): OrderedSet[K] =
    new InjectSet[K](ord) with LNode[K] with OrderedSet[K]
}

object OrderedMap {
  /**
   * Instantiate a new empty OrderedMap from key and value types
   * {{{
   * import com.twitter.algebird.maps.ordered._
   *
   * // map strings to integers, using default string ordering
   * val map1 = OrderedMap.key[String].value[Int]
   * // Use a custom ordering
   * val ord: Ordering[String] = ...
   * val map2 = OrderedMap.key(ord).value[Int]
   * }}}
   */
  def key[K](implicit ord: Ordering[K]) = infra.GetValue(ord)

  object infra {
    /** Mediating class between key method and value method */
    case class GetValue[K](ord: Ordering[K]) {
      def value[V]: OrderedMap[K, V] =
        new InjectMap[K, V](ord) with LNodeMap[K, V] with OrderedMap[K, V]
    }
  }
}
