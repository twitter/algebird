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

import cats.kernel.CommutativeMonoid
import com.twitter.algebird.CMSInstance.CountsTable

import scala.annotation.tailrec
import scala.util.Random

/**
 * Aggregagtor for AMS
 * */
case class AMSAggregator[K](amsMonoid: AMSMonoid[K]) extends MonoidAggregator[K, AMS[K], AMS[K]] {
  val monoid: AMSMonoid[K] = amsMonoid

  def prepare(value: K): AMS[K] = monoid.create(value)

  def present(cms: AMS[K]): AMS[K] = cms
}

object AMSAggregator {

  def apply[K: CMSHasher](depth: Int, buckets: Int): AMSAggregator[K] =
    AMSAggregator[K](new AMSMonoid[K](depth, buckets))
}

/**
 * AMSMonoid for a better f2 moment vector result. and a better joint between two of them.
 * reference : http://dimacs.rutgers.edu/%7Egraham/pubs/papers/encalgs-ams.pdf
 * and https://www.cs.rutgers.edu/~muthu/ams.c
 *
 * ==Join size estimation==
 *
 * https://people.cs.umass.edu/%7Emcgregor/711S12/sketches1.pdf p. 26
 * part : Comparing AMS and Count-Min sketches for join size estimation.
 *
 * what's join size ?
 * used to  answer to something like :
 * SELECT COUNT(*) FROM F, F’
 *   WHERE F.id = F’.id
 *
 * Count Min Sketch :
 * f ·f' =F2 with error N^2 / depth
 *
 * AMS :
 * f .f' = SQRT( F2(f) F2(f') / depth )
  **/
class AMSMonoid[K: CMSHasher](depth: Int, buckets: Int)
    extends Monoid[AMS[K]]
    with CommutativeMonoid[AMS[K]] {
  val params: AMSParams[K] = AMSParams[K](depth, buckets)

  override def zero: AMS[K] = AMSZero[K](params)

  override def plus(x: AMS[K], y: AMS[K]): AMS[K] = x ++ y

  override def sumOption(iter: TraversableOnce[AMS[K]]): Option[AMS[K]] =
    if (iter.isEmpty) None
    else {
      var sets = 0
      var count = 0L
      var countsTableMonoid = CountsTable[K](depth, buckets)

      @inline def updateCountsTable(pos: (Int, Int), count: Long): CountsTable[K] =
        countsTableMonoid + (pos, count)

      var oneItem: AMSItem[K] = null

      @inline def addItem(it: AMSItem[K]): CountsTable[K] = {
        sets += 1
        oneItem = it
        count += it.totalCount
        (0 until depth).foldLeft(countsTableMonoid) {
          case (table, j) =>
            val hash = params
              .hash(params.randoms.head(j), params.randoms(1)(j), buckets)
              .apply(it.item)
            val mult = AMSFunction.fourwise(
              params.randoms(2)(j),
              params.randoms(3)(j),
              params.randoms(4)(j),
              params.randoms(5)(j),
              hash)
            if ((mult & 1) == 1) {
              table + ((j, hash), it.totalCount)
            } else table + ((j, hash), -it.totalCount)

          case _ => countsTableMonoid
        }
      }
      iter.foreach({
        case AMSZero(_)            => ()
        case it @ AMSItem(_, _, _) => countsTableMonoid = addItem(it)
        case AMSInstances(amsCountTable, _, totalCount) =>
          count += totalCount
          amsCountTable.counts.zipWithIndex.foreach(counts =>
            counts._1.zipWithIndex.foreach(c => {
              sets += 1
              countsTableMonoid = updateCountsTable((counts._2, c._2), c._1)
            }))
      })
      if (sets == 0) Some(zero)
      else if (sets == 1) Some(oneItem)
      else Some(AMSInstances[K](params, countsTableMonoid, count))
    }

  /**
   * Creates a sketch out of a single item.
   */
  def create(item: K): AMS[K] = AMSItem[K](item, 1L, params)

  /**
   * Creates a sketch out of multiple items.
   */
  def create(data: Seq[K]): AMS[K] =
    sum(data.map(AMSItem(_, 1, params)))

}

/**
 * AMS sketch : maintaining a array of counts with all element arriving.
 *
 * AMS is a matrix of d x t counters (d row of length t).
 * - Each row j, a hash function hj(x) -> {1, ..., t} , x in U
 * - A other hash function gj maps element from U  to {-1, +1}
 *
 * */
case class AMSParams[K: CMSHasher](depth: Int, bucket: Int) {
  require(depth > 0 && bucket > 0, "buckets and depth should be positive")

  val randoms: Seq[Seq[Int]] = AMSFunction.generateRandom(depth)

  def hash(a: Int, b: Int, width: Int = Int.MaxValue): CMSHash[K] =
    CMSHash[K](a, b, width)

}

object AMSFunction {
  val fourwiseSize = 6

  def generateRandom(depht: Int): Seq[Seq[Int]] =
    Seq.fill[Seq[Int]](fourwiseSize)(Seq.fill[Int](depht)(Random.nextInt().abs))

  def hashValue[K: CMSHasher](item: K, a: Int, b: Int, width: Int = Int.MaxValue): Int =
    CMSHash[K](a, b, width).apply(item)

  /**
   * To ensure the random element is really random and "pure" see :
   * https://lucatrevisan.wordpress.com/2009/11/12/the-large-deviation-of-fourwise-independent-random-variables/
   * to more details.
   * */
  def fourwise(a: Int, b: Int, c: Int, d: Int, itemHashed: Int): Long = {
    var hash = CMSHash[Int](itemHashed, a, Int.MaxValue).apply(b)
    hash = CMSHash[Int](hash, itemHashed, Int.MaxValue).apply(c)
    hash = CMSHash[Int](hash, itemHashed, Int.MaxValue).apply(d)
    hash
  }

  // TODO : linear in average but ... not the best, median select is better
  def median(raw: Vector[Long]): Long = {
    val (lower, upper) = raw.sortWith(_ < _).splitAt(raw.size / 2)
    if (raw.size % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }

  def generateHash[K: CMSHasher](numHashes: Int, counters: Int): Seq[CMSHash[K]] = {

    @tailrec
    def createHash(buffer: Seq[CMSHash[K]], idx: Int, seed: Int): Seq[CMSHash[K]] =
      if (idx == 0) buffer
      else createHash(buffer :+ CMSHash[K](Random.nextInt(), 0, counters), idx - 1, seed)
    createHash(Seq.empty[CMSHash[K]], numHashes, counters)
  }
}

/**
 * All the method needed for user to take manipulate AMS : metrics and operators.
 * */
trait AMSCounting[K, C[_]] {

  def +(item: K): C[K] = this + (item, 1L)

  def +(item: K, count: Long): C[K]

  def ++(other: C[K]): C[K]

  def f1: Long = totalCount

  def innerProduct(other: C[K]): Approximate[Long]

  def f2: Approximate[Long]

  def frequency(item: K): Approximate[Long]

  def totalCount: Long
}

/**
 * The abstract trait for AMS
 * */
sealed abstract class AMS[A](val params: AMSParams[A]) extends AMSCounting[A, AMS] {
  def depth: Int
  def buckets: Int
}

/**
 * The AMSZero element
 * */
case class AMSZero[A](override val params: AMSParams[A]) extends AMS[A](params) {
  override def depth: Int = 0

  override val totalCount: Long = 0

  override def buckets: Int = 0

  override def innerProduct(other: AMS[A]): Approximate[Long] =
    Approximate.exact(0L)

  override def ++(other: AMS[A]): AMS[A] = other

  override def +(item: A, count: Long): AMS[A] = AMSItem(item, count, params)

  override def frequency(item: A): Approximate[Long] = Approximate.exact(0L)

  override def f2: Approximate[Long] = Approximate.exact(0L)
}

/**
 * An AMS with just one item
 * */
case class AMSItem[A](item: A, override val totalCount: Long, override val params: AMSParams[A])
    extends AMS[A](params) {

  override def depth: Int = params.depth

  override def buckets: Int = params.bucket

  override def innerProduct(other: AMS[A]): Approximate[Long] =
    Approximate.exact(totalCount) * other.frequency(item)

  override def ++(other: AMS[A]): AMS[A] = other match {
    case other: AMSZero[A] => this

    case other: AMSItem[A] =>
      AMSInstances[A](params) + (item, totalCount) + (other.item, other.totalCount)

    case other: AMSInstances[A] => other + (item, totalCount)
  }

  override def +(item: A, count: Long): AMS[A] =
    AMSInstances[A](params) + (this.item, totalCount) + (item, count)

  override def frequency(item: A): Approximate[Long] =
    if (this.item == item) Approximate.exact(1L) else Approximate.exact(0L)

  override def f2: Approximate[Long] = innerProduct(this)
}

/**
 * The Instances AMS algorithm with several values inside.
 * */
case class AMSInstances[A](countsTable: CountsTable[A],
                           override val params: AMSParams[A],
                           override val totalCount: Long)
    extends AMS[A](params) {

  override def depth: Int = params.depth

  override def buckets: Int = params.bucket

  private def compatible(other: AMSInstances[A]): Boolean =
    other.params.depth == depth && other.buckets == buckets && other.params.randoms == params.randoms

  override def innerProduct(other: AMS[A]): Approximate[Long] = other match {
    case other: AMSInstances[A] =>
      require(compatible(other))
      def innerProductAt(rawId: Int): Long =
        (0 until buckets).iterator.map { w =>
          countsTable.getCount((rawId, w)) * other.countsTable.getCount((rawId, w))
        }.sum

      val estimate = (0 until depth).map(innerProductAt)
      if (depth == 1) Approximate.exact(estimate.head)
      else if (depth == 2)
        Approximate.exact((estimate.head + estimate.last) / 2)
      else
        Approximate(0, AMSFunction.median(estimate.toVector), totalCount * other.totalCount, 0.5)

    case _ => other.innerProduct(this)

  }

  override def ++(other: AMS[A]): AMS[A] = other match {

    case other: AMSItem[A] => this + (other.item, other.totalCount)
    case other: AMSZero[A] => this
    case other: AMSInstances[A] =>
      require(other.params.randoms == params.randoms)

      // tcheck integrity here.
      val newCountTable = other.countsTable ++ countsTable
      val newTotalCount = other.totalCount + totalCount
      AMSInstances[A](newCountTable, params, newTotalCount)
  }

  override def +(item: A, count: Long): AMS[A] = {
    require(count >= 0, "cannot add negative count element to AMS Sketch")
    if (count != 0L) {
      val newCountsTable = (0 until depth).foldLeft(countsTable) {
        case (table, j) =>
          val hash = params
            .hash(params.randoms.head(j), params.randoms(1)(j), buckets)
            .apply(item)
          val mult = AMSFunction.fourwise(
            params.randoms(2)(j),
            params.randoms(3)(j),
            params.randoms(4)(j),
            params.randoms(5)(j),
            hash)
          if ((mult & 1) == 1) table + ((j, hash), count)
          else table + ((j, hash), -count)
      }
      AMSInstances(newCountsTable, params, totalCount + count)
    } else this
  }

  /**
   * Determine approximative count of a value.
   * There's not enough doc on it
   * TODO : Create proper approximation.
   * */
  override def frequency(item: A): Approximate[Long] = {
    var estimate = Vector.empty[Long]
    var offset = 0

    for (j <- 1 until params.depth) {
      val hash = params
        .hash(params.randoms.head(j - 1), params.randoms(1)(j - 1), buckets)
        .apply(item)
      val mult = AMSFunction.fourwise(
        params.randoms(2)(j - 1),
        params.randoms(3)(j - 1),
        params.randoms(4)(j - 1),
        params.randoms(5)(j - 1),
        hash)
      if ((mult & 1) == 1)
        estimate = estimate.+:(countsTable.getCount((offset, hash)))
      else estimate = estimate.+:(-countsTable.getCount((offset, hash)))

      offset += 1
    }
    if (params.depth == 1) Approximate.exact(estimate.head)
    else if (params.depth == 2)
      Approximate(estimate(0), (estimate(0) + estimate(1)) / 2, estimate(1), 0.5)
    else {
      Approximate.exact(AMSFunction.median(estimate))
    }
  }

  /**
   * This is much easier and faster than the count min sketch algorithm.
   * */
  override def f2: Approximate[Long] = {

    def f2At(idx: Int): Long =
      (0 until buckets).iterator.map { bucketIndex =>
        countsTable.getCount((idx, bucketIndex)) * countsTable.getCount((idx, bucketIndex))
      }.sum

    val estimate = (1 until depth).map(f2At)

    if (depth == 1) Approximate.exact(estimate(0))
    else if (depth == 2) Approximate.exact((estimate(0) + estimate(1)) / 2)
    else Approximate.exact(AMSFunction.median(estimate.toVector))

  }
}

object AMSInstances {
  def apply[A](params: AMSParams[A]): AMSInstances[A] = {
    val countsTable = CountsTable[A](params.depth, params.bucket)
    new AMSInstances[A](countsTable, params, 0)
  }

  def apply[A](params: AMSParams[A], tables: CountsTable[A], count: Long): AMSInstances[A] =
    new AMSInstances[A](tables, params, count)
}
