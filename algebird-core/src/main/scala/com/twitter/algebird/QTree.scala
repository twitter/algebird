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

import scala.util.hashing.MurmurHash3

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
  val DefaultLevel: Int = -16
  /**
   * level gives a bin size of 2^level. By default the bin size is 1/65536 (level = -16)
   */
  def apply[A](kv: (Double, A), level: Int = DefaultLevel): QTree[A] = {
    val offset = math.floor(kv._1 / math.pow(2.0, level)).toLong
    require(offset >= 0, "QTree can not accept negative values")

    new QTree(kv._2,
      offset,
      level,
      1,
      null,
      null)
  }

  def apply[A](kv: (Long, A)): QTree[A] = {
    require(kv._1 >= 0, "QTree can not accept negative values")

    new QTree(kv._2,
      kv._1,
      0,
      1,
      null,
      null)
  }

  def apply[A](offset: Long,
    level: Int,
    count: Long,
    sum: A, //the sum at just this node (*not* including its children)
    lowerChild: Option[QTree[A]],
    upperChild: Option[QTree[A]]): QTree[A] = {
    require(offset >= 0, "QTree can not accept negative values")

    new QTree(sum, offset, level, count, lowerChild.orNull, upperChild.orNull)
  }

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
   * End user consumable unapply for QTree
   */
  def unapply[A](qtree: QTree[A]): Option[(Long, Int, Long, A, Option[QTree[A]], Option[QTree[A]])] =
    Some((qtree.offset, qtree.level, qtree.count, qtree.sum, qtree.lowerChild, qtree.upperChild))

  /**
   * If you are sure you only care about the approximate histogram
   * features of QTree, you can save some space by using QTree[Unit]
   */
  def value(v: Long): QTree[Unit] = apply(v -> (()))
  /**
   * If you are sure you only care about the approximate histogram
   * features of QTree, you can save some space by using QTree[Unit]
   * level gives a bin size of 2^level. By default this is 1/65536 (level = -16)
   */
  def value(v: Double, level: Int = DefaultLevel): QTree[Unit] = apply(v -> (()), level)

  private[algebird] def mergePeers[@specialized(Int, Long, Float, Double) A](left: QTree[A], right: QTree[A])(implicit monoid: Monoid[A]): QTree[A] = {
    assert(right.lowerBound == left.lowerBound, "lowerBound " + right.lowerBound + " != " + left.lowerBound)
    assert(right.level == left.level, "level " + right.level + " != " + left.level)

    new QTree[A](monoid.plus(left.sum, right.sum),
      left.offset,
      left.level, left.count + right.count,
      mergeOptions(left.lowerChildNullable, right.lowerChildNullable),
      mergeOptions(left.upperChildNullable, right.upperChildNullable))
  }

  private def mergeOptions[A](aNullable: QTree[A], bNullable: QTree[A])(implicit monoid: Monoid[A]): QTree[A] =
    if (aNullable != null) {
      if (bNullable != null) {
        mergePeers(aNullable, bNullable)
      } else aNullable
    } else bNullable

  private[algebird] val cachedRangeCacheSize: Int = 20
  private[algebird] val cachedRangeLowerBound: Int = cachedRangeCacheSize * -1
  private[algebird] val rangeLut: Array[Double] = (cachedRangeLowerBound until cachedRangeCacheSize).map { level =>
    math.pow(2.0, level)
  }.toArray[Double]
}

class QTreeSemigroup[A](k: Int)(implicit val underlyingMonoid: Monoid[A]) extends Semigroup[QTree[A]] {
  /** Override this if you want to change how frequently sumOption calls compress */
  def compressBatchSize: Int = 50
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

class QTree[@specialized(Int, Long, Float, Double) A] private[algebird] (
  _sum: A, //the sum at just this node (*not* including its children)
  _offset: Long, //the range this tree covers is offset*(2^level) ... (offset+1)*(2^level)
  _level: Int,
  _count: Long, //the total count for this node and all of its children
  _lowerChildNullable: QTree[A],
  _upperChildNullable: QTree[A])
  extends scala.Product6[Long, Int, Long, A, Option[QTree[A]], Option[QTree[A]]] with Serializable {
  import QTree._

  val range: Double =
    if (_level < cachedRangeCacheSize && level > cachedRangeLowerBound)
      rangeLut(_level + cachedRangeCacheSize)
    else
      math.pow(2.0, level)

  def lowerBound: Double = range * _offset
  def upperBound: Double = range * (_offset + 1)

  def lowerChild: Option[QTree[A]] = Option(_lowerChildNullable)
  def upperChild: Option[QTree[A]] = Option(_upperChildNullable)

  def this(offset: Long, level: Int, count: Long, sum: A, lowerChild: Option[QTree[A]], upperChild: Option[QTree[A]]) =
    this(sum, offset, level, count, lowerChild.orNull, upperChild.orNull)

  // Helpers to access the nullable ones from inside the QTree work
  @inline private[algebird] def lowerChildNullable: QTree[A] = _lowerChildNullable
  @inline private[algebird] def upperChildNullable: QTree[A] = _upperChildNullable

  @inline def offset: Long = _offset
  @inline def level: Int = _level
  @inline def count: Long = _count
  @inline def sum: A = _sum

  @inline def _1: Long = _offset
  @inline def _2: Int = _level
  @inline def _3: Long = _count
  @inline def _4: A = _sum
  @inline def _5: Option[QTree[A]] = lowerChild
  @inline def _6: Option[QTree[A]] = upperChild

  override lazy val hashCode: Int = MurmurHash3.productHash(this)

  override def toString: String =
    productIterator.mkString(productPrefix + "(", ",", ")")

  override def equals(other: Any): Boolean =
    other match {
      case r: Product if productArity == r.productArity =>
        productIterator sameElements r.productIterator
      case _ => false
    }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[QTree[A]]

  override def productArity: Int = 6

  @annotation.tailrec
  private[algebird] final def extendToLevel(n: Int)(implicit monoid: Monoid[A]): QTree[A] = {
    if (n <= level)
      this
    else {
      val nextLevel = _level + 1
      val nextOffset = _offset / 2

      // See benchmark in QTreeMicroBenchmark for why do this rather than the single if
      // with 2 calls to QTree[A] in it.
      val l = if (offset % 2 == 0) this else null
      val r = if (offset % 2 == 0) null else this

      val parent =
        new QTree[A](monoid.zero, nextOffset, nextLevel, _count, l, r)

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
    val minLevel = _level.min(other.level)
    val leftOffset = offset << (_level - minLevel)
    val rightOffset = other.offset << (other.level - minLevel)
    var offsetDiff = leftOffset ^ rightOffset
    var ancestorLevel = minLevel
    while (offsetDiff > 0) {
      ancestorLevel += 1
      offsetDiff >>= 1
    }
    ancestorLevel.max(_level).max(other.level)
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
    mergePeers(left, right)
  }

  /**
   * give lower and upper bounds respectively of the percentile
   * value given. For instance, quantileBounds(0.5) would give
   * an estimate of the median.
   */
  def quantileBounds(p: Double): (Double, Double) = {
    require(p >= 0.0 && p <= 1.0, "The given percentile must be of the form 0 <= p <= 1.0")

    // rank is 0-indexed and 0 <= rank < count
    val rank = math.floor(count * p).toLong.min(count - 1)

    findRankBounds(rank)
  }

  /**
   * Precondition: 0 <= rank < count
   */
  private def findRankBounds(rank: Long): (Double, Double) = {
    require(0 <= rank && rank < count)

    val (leftCount, rightCount) = mapChildrenWithDefault(0L)(_.count)
    val parentCount = count - leftCount - rightCount

    if (rank < leftCount) {
      // Note that 0 <= rank < leftCount because of the require above.
      // So leftCount > 0, so lowerChild is not None.
      lowerChild.get.findRankBounds(rank)
    } else if (rank < leftCount + parentCount) {
      // leftCount <= rank < leftCount + parentCount
      (lowerBound, upperBound)
    } else {
      // Note that leftCount + parentCount <= rank < count.
      // So rightCount > 0, so upperChild is not None.
      upperChild.get.findRankBounds(rank - leftCount - parentCount)
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
    val minCount = _count >> k
    if ((minCount > 1L) || (_count < 1L)) {
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
    if (_count < minCount) {
      new QTree[A](totalSum, _offset, _level, _count, null, null)
    } else {
      val newLower = pruneChild(minCount, lowerChildNullable)
      val lowerNotPruned = newLower eq lowerChildNullable
      val newUpper = pruneChild(minCount, upperChildNullable)
      val upperNotPruned = newUpper eq upperChildNullable
      if (lowerNotPruned && upperNotPruned)
        this
      else
        new QTree[A](_sum, _offset, _level, _count, newLower, newUpper)
    }

  // If we don't prune we MUST return child
  @inline
  private def pruneChild(minCount: Long,
    childNullable: QTree[A])(implicit m: Monoid[A]): QTree[A] =
    if (childNullable == null)
      null
    else {
      val newChild = childNullable.pruneChildren(minCount)
      if (newChild eq childNullable) childNullable // need to pass the same reference if we don't change
      else newChild
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
    _count - childCounts._1 - childCounts._2
  }

  /**
   * A debug method that prints the QTree to standard out using print/println
   */
  def dump() {
    for (i <- (20 to _level by -1))
      print(" ")
    print(lowerBound + " - " + upperBound + ": " + _count)
    if (lowerChild.isDefined || upperChild.isDefined) {
      print(" (" + parentCount + ")")
    }
    println(" {" + _sum + "}")
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
  def percentile: Double
  /**
   * This is the depth parameter for the QTreeSemigroup
   */
  def k: Int
  /**
   * We convert T to a Double, then the Double is converted
   * to a Long by using a 2^level bucket size.
   */
  def level: Int = QTree.DefaultLevel
  implicit def num: Numeric[T]
  def prepare(input: T) = QTree.value(num.toDouble(input), level)
  def semigroup = new QTreeSemigroup[Unit](k)
}

object QTreeAggregator {
  val DefaultK = 9
}

/**
 * QTree aggregator is an aggregator that can be used to find the approximate percentile bounds.
 * The items that are iterated over to produce this approximation cannot be negative.
 * Returns an Intersection which represents the bounded approximation.
 */
case class QTreeAggregator[T](percentile: Double, k: Int = QTreeAggregator.DefaultK)(implicit val num: Numeric[T])
  extends Aggregator[T, QTree[Unit], Intersection[InclusiveLower, InclusiveUpper, Double]]
  with QTreeAggregatorLike[T] {

  def present(qt: QTree[Unit]) = {
    val (lower, upper) = qt.quantileBounds(percentile)
    Intersection(InclusiveLower(lower), InclusiveUpper(upper))
  }
}

/**
 * QTreeAggregatorLowerBound is an aggregator that is used to find an appoximate percentile.
 * This is similar to a QTreeAggregator, but is a convenience because instead of returning an Intersection,
 * it instead returns the lower bound of the percentile.
 * Like a QTreeAggregator, the items that are iterated over to produce this approximation cannot be negative.
 */
case class QTreeAggregatorLowerBound[T](percentile: Double, k: Int = QTreeAggregator.DefaultK)(implicit val num: Numeric[T])
  extends Aggregator[T, QTree[Unit], Double]
  with QTreeAggregatorLike[T] {

  def present(qt: QTree[Unit]) = qt.quantileBounds(percentile)._1
}
