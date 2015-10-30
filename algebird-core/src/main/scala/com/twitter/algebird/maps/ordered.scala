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
import scala.collection.{ SortedSet, SortedMap }

import scala.collection.immutable.MapLike
import scala.collection.mutable.Builder
import scala.collection.{ SortedSet, SortedMap, SetLike, SortedSetLike, SortedMapLike }

import com.twitter.algebird.maps.redblack.tree._

object tree {
  /** Trees that back a map-like object have a value as well as a key */
  trait DataMap[K, +V] extends Data[K] {
    def value: V

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
  trait NodeMap[K, +V] extends Node[K]

  trait LNodeMap[K, +V] extends NodeMap[K, V] with LNode[K]

  trait INodeMap[K, +V] extends NodeMap[K, V] with INode[K] {
    def data: DataMap[K, V]
  }
}

import tree._

object infra {
  class INodeIterator[IN <: INode[_]] extends Iterator[IN] {
    private[infra] val stack = scala.collection.mutable.Stack.empty[INode[_]]

    def hasNext = !stack.isEmpty
    def next = {
      val r = stack.pop()
      pushLeft(r.rsub)
      r.asInstanceOf[IN]
    }

    private[infra] def pushLeft(n: Node[_]) {
      var t = n
      while (t.isInstanceOf[INode[_]]) {
        val ti = t.asInstanceOf[INode[_]]
        stack.push(ti)
        t = ti.lsub
      }
    }
  }

  object INodeIterator {

    /** An empty node iterator, with a specified type */
    def empty[IN <: INode[_]] = new INodeIterator[IN]

    /** Construct a node iterator from a given tree node */
    def from[IN <: INode[_]](node: Node[_]) = {
      val itr = empty[IN]
      itr.pushLeft(node)
      itr
    }

    /** Construct a node iterator that will start at key */
    def fromKey[K, IN <: INode[_]](key: K, node: Node[K]) = {
      val itr = empty[IN]
      def fill(node: Node[K]) {
        val ord = node.keyOrdering
        node match {
          case n: INode[K] => {
            if (ord.lt(key, n.data.key)) {
              itr.stack.push(n)
              fill(n.lsub)
            } else if (ord.gt(key, n.data.key)) {
              fill(n.rsub)
            } else {
              itr.stack.push(n)
            }
          }
          case _ => ()
        }
      }
      fill(node)
      itr
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
trait OrderedLike[K, +IN <: INode[K], +M <: OrderedLike[K, IN, M]] extends Node[K] {

  /** Get the internal node stored at at key, or None if key is not present */
  def getNode(k: K) = this.node(k).map(_.asInstanceOf[IN])

  /** A container of underlying nodes, in key order */
  def nodes = nodesIterator.toIterable

  /** Iterator over nodes, in key order */
  def nodesIterator: Iterator[IN] = INodeIterator.from[IN](this)

  /** A container of nodes, in key order, having key >= k */
  def nodesFrom(k: K) = nodesIteratorFrom(k).toIterable

  /** Iterator over nodes, in key order, having key >= k */
  def nodesIteratorFrom(k: K): Iterator[IN] = INodeIterator.fromKey[K, IN](k, this)

  def nodesIteratorRange(from: Option[K], until: Option[K]): Iterator[IN] = {
    val fromItr = from.fold(nodesIterator)(k => nodesIteratorFrom(k))
    until.fold(fromItr)(k => fromItr.takeWhile(n => keyOrdering.lt(n.data.key, k)))
  }
}

/**
 * An inheritable (and mixable) trait representing Ordered Set functionality that is
 * backed by a Red/Black tree implemenation.
 * @tparam K The key type
 * @tparam IN The internal node type of the underlying R/B tree subclass
 * @tparam M The map self-type of the concrete map subclass
 */
trait OrderedSetLike[K, IN <: INode[K], M <: OrderedSetLike[K, IN, M] with SortedSet[K]]
  extends OrderedLike[K, IN, M] with SetLike[K, M] with SortedSetLike[K, M] {

  def empty: M

  /** Obtain a new container with key removed */
  def -(k: K) = this.delete(k).asInstanceOf[M]

  /** Returns true if key is present in the container, false otherwise */
  override def contains(k: K) = this.node(k).isDefined

  /** A container of keys, in key order */
  def keys = keysIterator.toIterable

  /** Iterator over keys, in key order */
  def keysIterator = nodesIterator.map(_.data.key)

  /** Obtain a new container with key inserted */
  def +(k: K) = this.insert(
    new Data[K] {
      val key = k
    }).asInstanceOf[M]

  /** Iterator over keys, in key order */
  def iterator: Iterator[K] = nodesIterator.map(_.data.key)

  def keysIteratorFrom(k: K): Iterator[K] = nodesIteratorFrom(k).map(_.data.key)

  def rangeImpl(from: Option[K], until: Option[K]) =
    nodesIteratorRange(from, until).map(_.data.key).foldLeft(empty)((m, e) => m + e)

  override def seq = this.asInstanceOf[M]
  def ordering = keyOrdering

  override def hashCode = scala.util.hashing.MurmurHash3.orderedHash(nodesIterator.map(_.data))
  override def equals(that: Any) = that match {
    case coll: OrderedSetLike[K, IN, M] =>
      coll.nodesIterator.map(_.data).toSeq.sameElements(this.nodesIterator.map(_.data).toSeq)
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
trait OrderedMapLike[K, +V, +IN <: INodeMap[K, V], +M <: OrderedMapLike[K, V, IN, M] with SortedMap[K, V]]
  extends NodeMap[K, V] with OrderedLike[K, IN, M] with SortedMapLike[K, V, M] {

  def empty: M

  def +[V2 >: V](kv2: (K, V2)): M = kv2 match {
    case kv: (K, V) => this.insert(
      new DataMap[K, V] {
        val key = kv._1
        val value = kv._2
      }).asInstanceOf[M]

    // Type-widening insertion causes a couple problems with this tree/map hierarchy
    // (1) Some of the tree variations require Monoid(s) as part of their functionality, and
    // there is no way to widen a Monoid without actually specifying it, so it can't be done
    // via insertion.  In those cases I have to forbid widening.
    // (2) I can push the definition of '+' down into specific subclasses, but it requires me
    // to define type members that encode the correct widened type:
    //   type IN2[V2] <: INodeMap[K, V2]
    //   type M2[V2] <: OrderedMapLike[K, V2, IN2[V2], M2[V2]] with SortedMap[K, V2]
    // In this case, I run into problems when defining an object like TDigestMap, which has no
    // K or V type parameters, because Scala considers a parameter-less variation to be
    // incompatible with the super-class variations.
    // Given (1) and (2), I contend that the least-insane solution is to expose the
    // type-widening signature that SortedMapLike requires, but throw an exception if
    // anybody actually tries to widen the type.  This probably still makes unicorns cry,
    // but I believe it achieves a global minimum of evil.
    // This has a side-benefit of avoiding some annoying type mismatch complaints from Scala
    // caused by its inability to know that M2[V2] is not the same as M.
    case _ => throw new Exception("insertion may not widen OrderedMapLike objects")
  }

  /** Obtain a new container with key removed */
  def -(k: K) = this.delete(k).asInstanceOf[M]

  /** Returns true if key is present in the container, false otherwise */
  override def contains(k: K) = this.node(k).isDefined

  /** A container of keys, in key order */
  override def keys = keysIterator.toIterable

  /** Iterator over keys, in key order */
  override def keysIterator = nodesIterator.map(_.data.key)

  /** Get the value stored at a key, or None if key is not present */
  def get(k: K) = this.getNode(k).map(_.data.value)

  /** Iterator over (key,val) pairs, in key order */
  def iterator: Iterator[(K, V)] = nodesIterator.map(n => ((n.data.key, n.data.value)))

  /** Container of values, in key order */
  override def values = valuesIterator.toIterable

  /** Iterator over values, in key order */
  override def valuesIterator = nodesIterator.map(_.data.value)

  def iteratorFrom(k: K): Iterator[(K, V)] =
    nodesIteratorFrom(k).map(n => (n.data.key, n.data.value))

  def keysIteratorFrom(k: K): Iterator[K] = nodesIteratorFrom(k).map(_.data.key)

  def valuesIteratorFrom(k: K): Iterator[V] = nodesIteratorFrom(k).map(_.data.value)

  def rangeImpl(from: Option[K], until: Option[K]) =
    nodesIteratorRange(from, until).map(n => (n.data.key, n.data.value))
      .foldLeft(empty)((m, e) => m + e)

  override def seq = this.asInstanceOf[M]
  def ordering = keyOrdering

  override protected[this] def newBuilder = new Builder[(K, V), M] {
    var res = empty
    def +=(e: (K, V)) = {
      res = res + e
      this
    }
    def clear() { res = empty }
    def result = res
  }

  override def hashCode = scala.util.hashing.MurmurHash3.orderedHash(nodesIterator.map(_.data))
  override def equals(that: Any) = that match {
    case coll: OrderedMapLike[K, V, IN, M] =>
      coll.nodesIterator.map(_.data).toSeq.sameElements(this.nodesIterator.map(_.data).toSeq)
    case _ => false
  }
}

sealed trait OrderedSet[K] extends SortedSet[K] with OrderedSetLike[K, INode[K], OrderedSet[K]] {

  override def empty = OrderedSet.key(keyOrdering)

  override def toString =
    "OrderedSet(" +
      nodesIterator.map(n => s"${n.data.key}").mkString(", ") +
      ")"
}

sealed trait OrderedMap[K, +V]
  extends SortedMap[K, V]
  with OrderedMapLike[K, V, INodeMap[K, V], OrderedMap[K, V]] {

  override def empty = OrderedMap.key(keyOrdering).value[V]

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
