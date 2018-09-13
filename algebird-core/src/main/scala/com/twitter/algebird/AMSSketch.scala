
package com.twitter.algebird

import cats.kernel.CommutativeMonoid
import com.twitter.algebird.CMSInstance.CountsTable

import scala.annotation.tailrec
import scala.util.Random

/**
 * AMS sketch : maintaining a array of counts with all element arriving.
 *
 * AMS is a matrix of d x t counters (d row of length t).
 * - Each row j, a hash function hj(x) -> {1, ..., t} , x in U
 * - A other hash function gj maps element from U  to {-1, +1}
 *
 * */
case class AMSParams[K : CMSHasher](depth: Int, bucket: Int) {
  require(depth > 0 && bucket > 0, "buckets and depth should be positive")

  def randoms: Seq[Seq[Int]] = AMSFunction.generateRandom(depth)

  def hash(a : Int, b : Int, width : Int = Int.MaxValue) : CMSHash[K] = CMSHash[K](a, b, width)

}

object AMSFunction {
  val fourwiseSize = 6

  def generateRandom(depht : Int) : Seq[Seq[Int]]= {
    Seq.fill[Seq[Int]](fourwiseSize)(Seq.fill[Int](depht)(Random.nextInt().abs))
  }


  def hashValue[K : CMSHasher](item : K, a : Int, b : Int, width : Int = Int.MaxValue) : Int = {
    CMSHash[K](a, b, width).apply(item)
  }

  def fourwise(a : Int, b : Int, c : Int, d : Int, itemHashed : Int) : Long = {
    val hash1 = CMSHash[Int](itemHashed, a, Int.MaxValue).apply(b)
    val hash2 = CMSHash[Int](hash1, itemHashed, Int.MaxValue).apply(c)
    val hash3 = CMSHash[Int](hash2, itemHashed, Int.MaxValue).apply(d)

    hash3
  }

  def generateHash[K : CMSHasher](numHashes : Int, counters : Int) : Seq[CMSHash[K]] = {

    @tailrec
    def createHash(buffer : Seq[CMSHash[K]], idx : Int, seed : Int): Seq[CMSHash[K]] ={
      if (idx == 0 ) buffer else createHash(buffer:+CMSHash[K](Random.nextInt(), 0, counters), idx - 1, seed)
    }
    createHash(Seq.empty[CMSHash[K]], numHashes, counters)
  }

}

object AMSSketch {
  def apply[A](buckets: Int, depth: Int): AMSSketch = new AMSSketch()
}

class AMSSketch {}

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

class AMSMonoid[K: CMSHasher](depth: Int, buckets: Int)
    extends Monoid[AMS[K]]
    with CommutativeMonoid[AMS[K]] {
  val params = AMSParams[K](depth, buckets )

  override def zero: AMS[K] = AMSZero[K](params)

  override def plus(x: AMS[K], y: AMS[K]): AMS[K] = x ++ y

  /**
   * Creates a sketch out of a single item.
   */
  def create(item: K): AMS[K] = AMSItem[K](item, 1L, params)

  /**
   * Creates a sketch out of multiple items.
   */
  def create(data: Seq[K]): AMS[K] = ???

}

case class AMSZero[A](override val params: AMSParams[A]) extends AMS[A](params) {
  override def depth: Int = 0

  override val totalCount: Long = 0

  override def buckets: Int = 0

  override def innerProduct(other: AMS[A]): Approximate[Long] = ???

  override def ++(other: AMS[A]): AMS[A] = other

  override def +(item: A, count: Long): AMS[A] = AMSItem(item, count, params)

  override def frequency(item: A): Approximate[Long] = Approximate.exact(0L)
}

case class AMSItem[A](item: A, override val totalCount: Long, override val params: AMSParams[A])
    extends AMS[A](params) {

  override def depth: Int = params.depth

  override def buckets: Int = params.bucket

  override def innerProduct(other: AMS[A]): Approximate[Long] = Approximate[Long](0, 0, 0, 0.1)

  override def ++(other: AMS[A]): AMS[A] = other match {
    case other: AMSZero[A] => this

    case other: AMSItem[A] => AMSInstances(params) + (item, totalCount) + (other.item, other.totalCount)

    case other: AMSInstances[A] => other + (item, totalCount)
  }

  override def +(item: A, count: Long): AMS[A] =
    AMSInstances(params) + (this.item, totalCount) + (item, count)

  override def frequency(item: A): Approximate[Long] =
    if (this.item == item) Approximate.exact(1L) else Approximate.exact(0L)
}

case class AMSInstances[A](countsTable: CountsTable[A],
                           override val params: AMSParams[A],
                           override val totalCount: Long)
    extends AMS[A](params) {

  override def depth: Int = params.depth

  override def buckets: Int = params.bucket

    // TODO
  override def innerProduct(other: AMS[A]): Approximate[Long] = Approximate[Long](0, 0, 0, 0.1)

  override def ++(other: AMS[A]): AMS[A] = ???

  override def +(item: A, count: Long): AMS[A] = {
    require(count >= 0, "cannot add negative count element to AMS Sketch")
    if (count != 0L) {
      var offset = 0

      for (j <- 0 until depth) {

        val hash = params.hash(params.randoms.head(j), params.randoms(1)(j), buckets).apply(item)

        val mult = AMSFunction.fourwise(params.randoms(2)(j),
          params.randoms(3)(j),
          params.randoms(4)(j),
          params.randoms(5)(j), hash)

        // TODO : To be changed.
        if ((mult & 1) == 1) countsTable + ((offset, hash), count)
        else countsTable + ((offset, hash), -count)

        offset += 1
      }
      AMSInstances(countsTable, params, totalCount + count)
    } else this
  }

  override def frequency(item: A): Approximate[Long] = {
    for (n <- 0 until params.depth) {}
    Approximate.exact(0L)
  }
}

object AMSInstances {
  def apply[A](params: AMSParams[A]): AMSInstances[A] = {
    val countsTable = CountsTable[A](params.depth, params.bucket)

    new AMSInstances[A](countsTable, params, 0)
  }
}

sealed abstract class AMS[A](val params: AMSParams[A]) extends AMSCounting[A, AMS] {

  def depth: Int

  def buckets: Int

  override val f2: Approximate[Long] = innerProduct(this)
}
