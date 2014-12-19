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

/**
 * A QTree provides an approximate Map[Double,A:Monoid] suitable for range queries, quantile queries,
 * and combinations of these (for example, if you use a numeric A, you can derive the inter-quartile mean).
 *
 * It is loosely related to the Q-Digest data structure from http://www.cs.virginia.edu/~son/cs851/papers/ucsb.sensys04.pdf,
 * but using an immutable tree structure, and carrying a generalized sum (of type A) at each node instead of just a count.
 *
 * The basic idea is to keep a binary tree, where the root represents the entire range of the input keys,
 * and each child node represents either the lower or upper half of its parent's range. Ranges are constrained to be
 * dyadic intervals (https://en.wikipedia.org/wiki/Interval_(mathematics)#Dyadic_intervals) for ease of merging.
 *
 * To keep the size bounded, the total count carried by any sub-tree must be at least 1/(2^k) of the total
 * count at the root. Any sub-trees that do not meet this criteria have their children pruned and become leaves.
 * (It's important that they not be pruned away entirely, but that we keep a fringe of low-count leaves that can
 * gain weight over time and ultimately split again when warranted).
 *
 * Quantile and range queries both give hard upper and lower bounds; the true result will be somewhere in the range given.
 *
 * Keys must be >= 0.
 */

object QTree {
  def apply[A: Monoid](kv: (Double, A), level: Int = -16): QTree[A] = {
    QTree(math.floor(kv._1 / math.pow(2.0, level)).toLong,
      level,
      1,
      kv._2,
      None,
      None)
  }

  def apply[A: Monoid](kv: (Long, A)): QTree[A] = {
    QTree(kv._1,
      0,
      1,
      kv._2,
      None,
      None)
  }

  /**
   * The common case of wanting a count and sum for the same value
   */
  def apply(k: Long): QTree[Long] = apply(k -> k)
  def apply(k: Double): QTree[Double] = apply(k -> k)
}

class QTreeSemigroup[A: Monoid](k: Int) extends Semigroup[QTree[A]] {
  def plus(left: QTree[A], right: QTree[A]) = left.merge(right).compress(k)
}

case class QTree[A](
  offset: Long, //the range this tree covers is offset*(2^level) ... (offset+1)*(2^level)
  level: Int,
  count: Long, //the total count for this node and all of its children
  sum: A, //the sum at just this node (*not* including its children)
  lowerChild: Option[QTree[A]],
  upperChild: Option[QTree[A]])(implicit monoid: Monoid[A]) {

  require(offset >= 0, "QTree can not accept negative values")

  def range: Double = math.pow(2.0, level)
  def lowerBound: Double = range * offset
  def upperBound: Double = range * (offset + 1)

  private def extendToLevel(n: Int): QTree[A] = {
    if (n <= level)
      this
    else {
      val nextLevel = level + 1
      val nextOffset = offset / 2

      val parent =
        if (offset % 2 == 0)
          QTree[A](nextOffset, nextLevel, count, monoid.zero, Some(this), None)
        else
          QTree[A](nextOffset, nextLevel, count, monoid.zero, None, Some(this))
      parent.extendToLevel(n)
    }
  }

  /**
   *
   * Find the smallest dyadic interval that contains the dyadic interval
   * for this tree's root and the other tree's root, and return its
   * level (that is, the power of 2 for the interval).
   */

  private def commonAncestorLevel(other: QTree[A]) = {
    val minLevel = level.min(other.level)
    val leftOffset = offset << (level - minLevel)
    val rightOffset = other.offset << (other.level - minLevel)
    var offsetDiff = leftOffset ^ rightOffset
    var ancestorLevel = minLevel
    while (offsetDiff > 0) {
      ancestorLevel += 1
      offsetDiff >>= 1
    }
    ancestorLevel.max(level).max(other.level)
  }

  def merge(other: QTree[A]) = {
    val commonAncestor = commonAncestorLevel(other)
    val left = extendToLevel(commonAncestor)
    val right = other.extendToLevel(commonAncestor)
    left.mergeWithPeer(right)
  }

  private def mergeWithPeer(other: QTree[A]): QTree[A] = {
    assert(other.lowerBound == lowerBound, "lowerBound " + other.lowerBound + " != " + lowerBound)
    assert(other.level == level, "level " + other.level + " != " + level)

    copy(count = count + other.count,
      sum = monoid.plus(sum, other.sum),
      lowerChild = mergeOptions(lowerChild, other.lowerChild),
      upperChild = mergeOptions(upperChild, other.upperChild))
  }

  private def mergeOptions(a: Option[QTree[A]], b: Option[QTree[A]]): Option[QTree[A]] = {
    (a, b) match {
      case (Some(qa), Some(qb)) => Some(qa.mergeWithPeer(qb))
      case (None, _) => b
      case (_, None) => a
    }
  }

  def quantileBounds(p: Double): (Double, Double) = {
    val rank = math.floor(count * p).toLong
    (findRankLowerBound(rank).get, findRankUpperBound(rank).get)
  }

  private def findRankLowerBound(rank: Long): Option[Double] = {
    if (rank > count)
      None
    else {
      val childCounts = mapChildrenWithDefault(0L){ _.count }
      val parentCount = count - childCounts._1 - childCounts._2
      lowerChild.flatMap{ _.findRankLowerBound(rank - parentCount) }.orElse {
        val newRank = rank - childCounts._1 - parentCount
        if (newRank <= 0)
          Some(lowerBound)
        else
          upperChild.flatMap{ _.findRankLowerBound(newRank) }
      }
    }
  }

  private def findRankUpperBound(rank: Long): Option[Double] = {
    if (rank > count)
      None
    else {
      lowerChild.flatMap{ _.findRankUpperBound(rank) }.orElse {
        val lowerCount = lowerChild.map{ _.count }.getOrElse(0L)
        upperChild.flatMap{ _.findRankUpperBound(rank - lowerCount) }.orElse(Some(upperBound))
      }
    }
  }

  def rangeSumBounds(from: Double, to: Double): (A, A) = {
    if (from <= lowerBound && to >= upperBound) {
      val s = totalSum
      (s, s)
    } else if (from < upperBound && to >= lowerBound) {
      val ((lower1, upper1), (lower2, upper2)) =
        mapChildrenWithDefault((monoid.zero, monoid.zero)){ _.rangeSumBounds(from, to) }
      (monoid.plus(lower1, lower2),
        monoid.plus(sum, monoid.plus(upper1, upper2)))
    } else {
      (monoid.zero, monoid.zero)
    }
  }

  def rangeCountBounds(from: Double, to: Double): (Long, Long) = {
    if (from <= lowerBound && to >= upperBound) {
      val s = count
      (s, s)
    } else if (from < upperBound && to >= lowerBound) {
      val ((lower1, upper1), (lower2, upper2)) =
        mapChildrenWithDefault((0L, 0L)){ _.rangeCountBounds(from, to) }
      (lower1 + lower2, parentCount + upper1 + upper2)
    } else {
      (0L, 0L)
    }
  }

  def compress(k: Int) = {
    val minCount = count >> k
    prune(minCount)._1.get
  }

  private def prune(minCount: Long): (Option[QTree[A]], Boolean) = {
    val (lowerResult, upperResult) =
      mapChildrenWithDefault[(Option[QTree[A]], Boolean)]((None, false)){ _.prune(minCount) }
    val parent =
      if (lowerResult._2 || lowerResult._2)
        copy(lowerChild = lowerResult._1, upperChild = upperResult._1)
      else
        this
    val (lowerCount, upperCount) = parent.mapChildrenWithDefault(0L){ _.parentCount }

    if (parent.parentCount + lowerCount + upperCount <= minCount) {
      val (lowerSum, upperSum) = parent.mapChildrenWithDefault(monoid.zero){ _.sum }
      val newSum = monoid.plus(parent.sum, monoid.plus(lowerSum, upperSum))
      val (newLowerChild, newUpperChild) =
        parent.mapChildrenWithDefault[Option[QTree[A]]](None) {
          c => Some(c.copy(count = c.count - c.parentCount, sum = monoid.zero))
        }
      val (lowerLeaf, upperLeaf) = parent.mapChildrenWithDefault(true){ _.isLeaf }

      (Some(parent.copy(sum = newSum,
        lowerChild = if (lowerLeaf) None else newLowerChild,
        upperChild = if (upperLeaf) None else newUpperChild)),
        true)
    } else {
      (Some(parent), lowerResult._2 || upperResult._2)
    }
  }

  def size: Int = {
    val childSizes = mapChildrenWithDefault(0){ _.size }
    1 + childSizes._1 + childSizes._2
  }

  def totalSum: A = {
    val childSums = mapChildrenWithDefault(monoid.zero){ _.totalSum }
    monoid.plus(sum, monoid.plus(childSums._1, childSums._2))
  }

  private def mapChildrenWithDefault[T](default: T)(fn: QTree[A] => T): (T, T) = {
    (lowerChild.map(fn).getOrElse(default),
      upperChild.map(fn).getOrElse(default))
  }

  private def parentCount = {
    val childCounts = mapChildrenWithDefault(0L){ _.count }
    count - childCounts._1 - childCounts._2
  }

  private def isLeaf: Boolean = {
    (lowerChild, upperChild) match {
      case (None, None) => true
      case _ => false
    }
  }

  def dump {
    for (i <- (20 to level by -1))
      print(" ")
    print(lowerBound + " - " + upperBound + ": " + count)
    if (lowerChild.isDefined || upperChild.isDefined) {
      print(" (" + parentCount + ")")
    }
    println(" {" + sum + "}")
    lowerChild.foreach{ _.dump }
    upperChild.foreach{ _.dump }
  }

  def interQuartileMean(implicit n: Numeric[A]): (Double, Double) = {
    val (l25, u25) = quantileBounds(0.25)
    val (l75, u75) = quantileBounds(0.75)
    val (ll, lu) = rangeSumBounds(l25, l75)
    val (ul, uu) = rangeSumBounds(u25, u75)
    val (llc, luc) = rangeCountBounds(l25, l75)
    val (ulc, uuc) = rangeCountBounds(u25, u75)

    (n.toDouble(ll) / luc, n.toDouble(uu) / ulc)
  }
}
