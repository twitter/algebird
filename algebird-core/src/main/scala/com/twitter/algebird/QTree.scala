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
  /**
   * level gives a bin size of 2^level. By default the bin size is 1/65536 (level = -16)
   */
  def apply[A](kv: (Double, A), level: Int = -16): QTree[A] =
    QTree(math.floor(kv._1 / math.pow(2.0, level)).toLong,
      level,
      1,
      kv._2,
      None,
      None)

  def apply[A](kv: (Long, A)): QTree[A] =
    QTree(kv._1,
      0,
      1,
      kv._2,
      None,
      None)

  /**
   * The common case of wanting an offset and sum for the same value
   * This is useful if you want to query the mean inside a range later.
   * If you truly just care about the counts/histogram, see the value method.
   */
  def apply(k: Long): QTree[Long] = apply(k -> k)
  /**
   * uses 1/65636 as the bin size, if you want to control that see other apply
   * or value methods.
   *
   * This is useful if you want to query the mean inside a range later.
   * If you truly just care about the counts/histogram, see the value method.
   */
  def apply(k: Double): QTree[Double] = apply(k -> k)

  /**
   * If you are sure you only care about the approximate histogram
   * features of QTree, you can save some space by using QTree[Unit]
   */
  def value(v: Long): QTree[Unit] = apply(v -> ())
  /**
   * If you are sure you only care about the approximate histogram
   * features of QTree, you can save some space by using QTree[Unit]
   * level gives a bin size of 2^level. By default this is 1/65536 (level = -16)
   */
  def value(v: Double, level: Int = -16): QTree[Unit] = apply(v -> (), level)
}

class QTreeSemigroup[A](k: Int)(implicit val underlyingMonoid: Monoid[A]) extends Semigroup[QTree[A]] {
  /** Override this if you want to change how frequently sumOption calls compress */
  def compressBatchSize: Int = 25
  def plus(left: QTree[A], right: QTree[A]) = left.merge(right).compress(k)
  override def sumOption(items: TraversableOnce[QTree[A]]): Option[QTree[A]] = if (items.isEmpty) None
  else {
    // only call compressBatchSize once
    val batchSize = compressBatchSize
    var count = 1 // start at 1, so we only compress after batchSize items
    val iter = items.toIterator
    var result = iter.next // due to not being empty, this does not throw
    while (iter.hasNext) {
      result = result.merge(iter.next)
      count += 1
      if (count % batchSize == 0) {
        result = result.compress(k)
      }
    }
    Some(result.compress(k))
  }
}

case class QTree[A](
  offset: Long, //the range this tree covers is offset*(2^level) ... (offset+1)*(2^level)
  level: Int,
  count: Long, //the total count for this node and all of its children
  sum: A, //the sum at just this node (*not* including its children)
  lowerChild: Option[QTree[A]],
  upperChild: Option[QTree[A]]) {

  require(offset >= 0, "QTree can not accept negative values")

  def range: Double = math.pow(2.0, level)
  def lowerBound: Double = range * offset
  def upperBound: Double = range * (offset + 1)

  private def extendToLevel(n: Int)(implicit monoid: Monoid[A]): QTree[A] = {
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

  /**
   * This merges with another QTree but DOES NOT compress.
   * You should probably never use this and instead use
   * QTreeSemigroup.plus(a, b) or .sumOption. Strongly
   * prefer sumOption if you can, as it is much more efficient
   * due to compressing less frequently.
   */
  def merge(other: QTree[A])(implicit monoid: Monoid[A]): QTree[A] = {
    val commonAncestor = commonAncestorLevel(other)
    val left = extendToLevel(commonAncestor)
    val right = other.extendToLevel(commonAncestor)
    left.mergeWithPeer(right)
  }

  private def mergeWithPeer(other: QTree[A])(implicit monoid: Monoid[A]): QTree[A] = {
    assert(other.lowerBound == lowerBound, "lowerBound " + other.lowerBound + " != " + lowerBound)
    assert(other.level == level, "level " + other.level + " != " + level)

    copy(count = count + other.count,
      sum = monoid.plus(sum, other.sum),
      lowerChild = mergeOptions(lowerChild, other.lowerChild),
      upperChild = mergeOptions(upperChild, other.upperChild))
  }

  private def mergeOptions(a: Option[QTree[A]], b: Option[QTree[A]])(implicit monoid: Monoid[A]): Option[QTree[A]] =
    (a, b) match {
      case (Some(qa), Some(qb)) => Some(qa.mergeWithPeer(qb))
      case (None, right) => right
      case (left, None) => left
    }

  /**
   * give lower and upper bounds respectively of the percentile
   * value given. For instance, quantileBounds(0.5) would give
   * an estimate of the median.
   */
  def quantileBounds(p: Double): (Double, Double) = {
    require(p >= 0.0 && p < 1.0, "The given percentile must be of the form 0 <= p < 1.0")

    val rank = math.floor(count * p).toLong
    // get is safe below, because findRankLowerBound only returns
    // None if rank > count, but due to construction rank <= count
    (findRankLowerBound(rank).get, findRankUpperBound(rank).get)
  }

  private def findRankLowerBound(rank: Long): Option[Double] =
    if (rank > count)
      None
    else {
      val childCounts = mapChildrenWithDefault(0L)(_.count)
      val parentCount = count - childCounts._1 - childCounts._2
      lowerChild.flatMap { _.findRankLowerBound(rank - parentCount) }
        .orElse {
          val newRank = rank - childCounts._1 - parentCount
          if (newRank <= 0)
            Some(lowerBound)
          else
            upperChild.flatMap{ _.findRankLowerBound(newRank) }
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

  /**
   * Get the bounds on the sums within a range (not percentile)
   * This along with the rangeCountBounds can tell you the mean over a range
   */
  def rangeSumBounds(from: Double, to: Double)(implicit monoid: Monoid[A]): (A, A) = {
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

  /**
   * Return upper and lower bounds on the counts that appear in a given range
   */
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

  /**
   * Users should never need to call this if they are adding QTrees using the Semigroup
   * This makes sure no element in the tree has count less than
   * the total count / 2^k. That means after this call there
   * are at most 2^k nodes, but usually fewer.
   */
  def compress(k: Int)(implicit m: Monoid[A]): QTree[A] = {
    val minCount = count >> k
    if ((minCount > 1L) || (count < 1L)) {
      pruneChildren(minCount)
    } else {
      // count > 0, so for all nodes, if minCount <= 1, then count >= minCount
      // so we don't need to traverse
      // this is common when you only add few items together, which happens
      // on map-side aggregation commonly
      this
    }
  }

  // If we don't prune we MUST return this
  private def pruneChildren(minCount: Long)(implicit m: Monoid[A]): QTree[A] =
    if (count < minCount) {
      copy(sum = totalSum, lowerChild = None, upperChild = None)
    } else {
      val newLower = pruneChild(minCount, lowerChild)
      val lowerNotPruned = newLower eq lowerChild
      val newUpper = pruneChild(minCount, upperChild)
      val upperNotPruned = newUpper eq upperChild
      if (lowerNotPruned && upperNotPruned)
        this
      else
        copy(lowerChild = newLower, upperChild = newUpper)
    }

  // If we don't prune we MUST return child
  @inline
  private def pruneChild(minCount: Long,
    child: Option[QTree[A]])(implicit m: Monoid[A]): Option[QTree[A]] = child match {
    case exists @ Some(oldChild) =>
      val newChild = oldChild.pruneChildren(minCount)
      if (newChild eq oldChild) exists // need to pass the same reference if we don't change
      else Some(newChild)
    case n @ None => n // make sure we pass the same ref out
  }

  /**
   * How many total nodes are there in the QTree.
   * Not meaningful for learning statistics, but interesting
   * to estimate serialization size.
   */
  def size: Int = {
    val childSizes = mapChildrenWithDefault(0){ _.size }
    1 + childSizes._1 + childSizes._2
  }

  /**
   * Total sum over the entire tree.
   */
  def totalSum(implicit monoid: Monoid[A]): A = {
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

  /**
   * A debug method that prints the QTree to standard out using print/println
   */
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

  /**
   * This gives you the mean for the middle 50%-ile.
   * This probably only makes sense if the Monoid[A] is
   * equivalent to addition in Numeric[A], which is only
   * used to convert to Double at the end
   */
  def interQuartileMean(implicit n: Numeric[A], m: Monoid[A]): (Double, Double) = {
    val (l25, u25) = quantileBounds(0.25)
    val (l75, u75) = quantileBounds(0.75)
    val (ll, _) = rangeSumBounds(l25, l75)
    val (_, uu) = rangeSumBounds(u25, u75)
    // in the denominator, we chose the opposite to keep the bound:
    val (_, luc) = rangeCountBounds(l25, l75)
    val (ulc, _) = rangeCountBounds(u25, u75)

    (n.toDouble(ll) / luc, n.toDouble(uu) / ulc)
  }
}

trait QTreeAggregatorLike[T] {
  val percentile: Double
  val k: Int
  implicit val num: Numeric[T]

  def prepare(input: T) = QTree(num.toDouble(input))
  val semigroup = new QTreeSemigroup[Double](k)
}

object QTreeAggregator {
  val DefaultK = 9
}

case class QTreeAggregator[T](percentile: Double, k: Int = QTreeAggregator.DefaultK)(implicit val num: Numeric[T])
  extends Aggregator[T, QTree[Double], (Double, Double)]
  with QTreeAggregatorLike[T] {

  def present(qt: QTree[Double]) = qt.quantileBounds(percentile)
}

case class QTreeAggregatorLowerBound[T](percentile: Double, k: Int = QTreeAggregator.DefaultK)(implicit val num: Numeric[T])
  extends Aggregator[T, QTree[Double], Double]
  with QTreeAggregatorLike[T] {

  def present(qt: QTree[Double]) = qt.quantileBounds(percentile)._1
}
