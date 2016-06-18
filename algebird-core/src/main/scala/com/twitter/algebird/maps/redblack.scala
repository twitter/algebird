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

package com.twitter.algebird.maps.redblack

import math.Ordering

package tree {
  /** The color (red or black) of a node in a Red/Black tree */
  sealed trait Color extends Serializable
  case object R extends Color
  case object B extends Color

  /** Defines the data payload of a tree node */
  trait Data[K] extends Serializable {
    /** The axiomatic unit of data for R/B trees is a key */
    def key: K

    override def hashCode = key.hashCode
    override def equals(that: Any) = that match {
      case data: Data[K] => this.key.equals(data.key)
      case _ => false
    }
  }

  /**
   * Base class of a Red/Black tree node
   * @tparam K The key type
   */
  trait Node[K] extends Serializable {

    /** The ordering that is applied to key values */
    def keyOrdering: Ordering[K]

    /** Instantiate an internal node. */
    protected def iNode(color: Color, d: Data[K], lsub: Node[K], rsub: Node[K]): INode[K]

    /** Insert a node into the tree */
    private[maps] final def insert(d: Data[K]): Node[K] = blacken(ins(d))

    /** Delete the key from the tree */
    private[maps] final def delete(k: K): Node[K] = if (node(k).isDefined) blacken(del(k)) else this

    /** Obtain the node stored at a given key if it exists, None otherwise */
    def node(k: K): Option[INode[K]]

    def nodeMin: Option[INode[K]]
    def nodeMax: Option[INode[K]]

    // internal
    private[tree] def ins(d: Data[K]): Node[K]
    private[tree] def del(k: K): Node[K]

    /** create a new Red node from a key, value, left subtree and right subtree */
    final protected def rNode(d: Data[K], lsub: Node[K], rsub: Node[K]): INode[K] = iNode(R, d, lsub, rsub)

    /** create a new Black node from a key, value, left subtree and right subtree */
    final protected def bNode(d: Data[K], lsub: Node[K], rsub: Node[K]): INode[K] = iNode(B, d, lsub, rsub)

    final protected def blacken(node: Node[K]): Node[K] = node match {
      case n: INode[K] if (n.color == R) => bNode(n.data, n.lsub, n.rsub)
      case n => n
    }
    final protected def redden(node: Node[K]): Node[K] = node match {
      case n: INode[K] => if (n.color == R) n else rNode(n.data, n.lsub, n.rsub)
      case _ => throw new Exception("illegal attempt to make a leaf node red")
    }

    // balance for insertion
    final protected def balance(node: Node[K]) = node match {
      case BNode(dG, RNode(dP, RNode(dC, lC, rC), rP), rG) =>
        rNode(dP, bNode(dC, lC, rC), bNode(dG, rP, rG))
      case BNode(dG, RNode(dP, lP, RNode(dC, lC, rC)), rG) =>
        rNode(dC, bNode(dP, lP, lC), bNode(dG, rC, rG))
      case BNode(dG, lG, RNode(dP, RNode(dC, lC, rC), rP)) =>
        rNode(dC, bNode(dG, lG, lC), bNode(dP, rC, rP))
      case BNode(dG, lG, RNode(dP, lP, RNode(dC, lC, rC))) =>
        rNode(dP, bNode(dG, lG, lP), bNode(dC, lC, rC))
      case _ => node
    }

    final protected def balanceDel(x: Data[K], tl: Node[K], tr: Node[K]) =
      (tl, tr) match {
        case (RNode(y, a, b), RNode(z, c, d)) => rNode(x, bNode(y, a, b), bNode(z, c, d))
        case (RNode(y, RNode(z, a, b), c), d) => rNode(y, bNode(z, a, b), bNode(x, c, d))
        case (RNode(y, a, RNode(z, b, c)), d) => rNode(z, bNode(y, a, b), bNode(x, c, d))
        case (a, RNode(y, b, RNode(z, c, d))) => rNode(y, bNode(x, a, b), bNode(z, c, d))
        case (a, RNode(y, RNode(z, b, c), d)) => rNode(z, bNode(x, a, b), bNode(y, c, d))
        case (a, b) => bNode(x, a, b)
      }

    final protected def balanceLeft(x: Data[K], tl: Node[K], tr: Node[K]) =
      (tl, tr) match {
        case (RNode(y, a, b), c) => rNode(x, bNode(y, a, b), c)
        case (bl, BNode(y, a, b)) => balanceDel(x, bl, rNode(y, a, b))
        case (bl, RNode(y, BNode(z, a, b), c)) =>
          rNode(z, bNode(x, bl, a), balanceDel(y, b, redden(c)))
        case _ => throw new Exception(s"undefined pattern in tree pair: ($tl, $tr)")
      }

    final protected def balanceRight(x: Data[K], tl: Node[K], tr: Node[K]) =
      (tl, tr) match {
        case (a, RNode(y, b, c)) => rNode(x, a, bNode(y, b, c))
        case (BNode(y, a, b), bl) => balanceDel(x, rNode(y, a, b), bl)
        case (RNode(y, a, BNode(z, b, c)), bl) =>
          rNode(z, balanceDel(y, redden(a), b), bNode(x, c, bl))
        case _ => throw new Exception(s"undefined pattern in tree pair: ($tl, $tr)")
      }

    final protected def append(tl: Node[K], tr: Node[K]): Node[K] = (tl, tr) match {
      case (LNode(), n) => n
      case (n, LNode()) => n
      case (RNode(x, a, b), RNode(y, c, d)) => append(b, c) match {
        case RNode(z, bb, cc) => rNode(z, rNode(x, a, bb), rNode(y, cc, d))
        case bc => rNode(x, a, rNode(y, bc, d))
      }
      case (BNode(x, a, b), BNode(y, c, d)) => append(b, c) match {
        case RNode(z, bb, cc) => rNode(z, bNode(x, a, bb), bNode(y, cc, d))
        case bc => balanceLeft(x, a, bNode(y, bc, d))
      }
      case (a, RNode(x, b, c)) => rNode(x, append(a, b), c)
      case (RNode(x, a, b), c) => rNode(x, a, append(b, c))
    }

    // NOTE: the balancing rules for node deletion all assume that the case of deleting a key
    // that is not in the map is addressed elsewhere.  If these balancing functions are applied
    // to a key that isn't present, they will fail destructively and uninformatively.
    final protected def delLeft(node: INode[K], k: K) = node.lsub match {
      case n: INode[K] if (n.color == B) => balanceLeft(node.data, node.lsub.del(k), node.rsub)
      case _ => rNode(node.data, node.lsub.del(k), node.rsub)
    }

    final protected def delRight(node: INode[K], k: K) = node.rsub match {
      case n: INode[K] if (n.color == B) => balanceRight(node.data, node.lsub, node.rsub.del(k))
      case _ => rNode(node.data, node.lsub, node.rsub.del(k))
    }
  }

  /** Represents a leaf node in the Red Black tree system */
  trait LNode[K] extends Node[K] {
    final def node(k: K) = None

    final def nodeMin = None
    final def nodeMax = None

    final def ins(d: Data[K]) = rNode(d, this, this)
    final def del(k: K) = this
  }

  private object LNode {
    def unapply[K](node: LNode[K]): Boolean = true
  }

  /** Represents an internal node (Red or Black) in the Red Black tree system */
  trait INode[K] extends Node[K] {
    /** The Red/Black color of this node */
    def color: Color
    /** Including, but not limited to, the key */
    def data: Data[K]
    /** The left sub-tree */
    def lsub: Node[K]
    /** The right sub-tree */
    def rsub: Node[K]

    final def node(k: K) =
      if (keyOrdering.lt(k, data.key)) lsub.node(k)
      else if (keyOrdering.gt(k, data.key)) rsub.node(k)
      else Some(this)

    final def nodeMin = lsub match {
      case LNode() => Some(this)
      case _ => lsub.nodeMin
    }

    final def nodeMax = rsub match {
      case LNode() => Some(this)
      case _ => rsub.nodeMax
    }

    final def del(k: K) =
      if (keyOrdering.lt(k, data.key)) delLeft(this, k)
      else if (keyOrdering.gt(k, data.key)) delRight(this, k)
      else append(lsub, rsub)

    final def ins(d: Data[K]) =
      if (color == R) {
        if (keyOrdering.lt(d.key, data.key)) rNode(data, lsub.ins(d), rsub)
        else if (keyOrdering.gt(d.key, data.key)) rNode(data, lsub, rsub.ins(d))
        else rNode(d, lsub, rsub)
      } else {
        if (keyOrdering.lt(d.key, data.key)) balance(bNode(data, lsub.ins(d), rsub))
        else if (keyOrdering.gt(d.key, data.key)) balance(bNode(data, lsub, rsub.ins(d)))
        else bNode(d, lsub, rsub)
      }
  }

  private object RNode {
    def unapply[K](node: INode[K]): Option[(Data[K], Node[K], Node[K])] =
      if (node.color == R) Some((node.data, node.lsub, node.rsub)) else None
  }

  private object BNode {
    def unapply[K](node: INode[K]): Option[(Data[K], Node[K], Node[K])] =
      if (node.color == B) Some((node.data, node.lsub, node.rsub)) else None
  }
}
