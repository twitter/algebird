package com.twitter.algebird

import algebra.BoundedSemilattice

/**
 * Cuckoo filter for Algebird in a Monoid way of life.
 * referring :
 *  - https://github.com/twitter/algebird/issues/560#issue-185161455
 *  - http://www.eecs.harvard.edu/%7Emichaelm/postscripts/cuckoo-conext2014.pdf
 *  - https://github.com/newhoggy/pico-cuckoo-filter/blob/master/pico-cuckoo-filter/src/main/scala/org/pico/cuckoo/filter/CuckooFilter.scala
 * By nature, this filter isn't commutative
 *
 * */

object CuckooFilter {
// TODO : optimal parameter have to be compute here : see Asymptotic behavior of the original paper.
  def apply[A](bucketSize: Int, fingerPrintSize: LongBitSet, kicks: Int, buckets: Int)(
      implicit hash1: Hash128[A],
      hash2: Hash128[A]): CuckooFilterMonoid[A] = {
    null
  }
}

/**
  * The cuckoo filter monoid
  * */
// TODO : Add the fingerprint size.
case class CuckooFilterMonoid[A](fingerprintBucket: Int, maxKicks: Int = 3, totalBuckets: Int = 256)(
    implicit h1: Hash128[A],
    h2: Hash128[A])
    extends Monoid[CF[A]]
    with BoundedSemilattice[BF[A]] {
  override def zero: CF[A] = ???

  override def plus(x: CF[A], y: CF[A]): CF[A] = ???

  /**
   * Create a cuckoo filter with one item.
   */
  def create(item: A): BF[A] = ???

  /**
   * Create a cuckoo filter with multiple items.
   */
  def create(data: A*): BF[A] = create(data.iterator)

  /**
   * Create a cuckoo filter with multiple items from an iterator
   */
  def create(data: Iterator[A]): BF[A] = ???

}

/**
 * Cuckoo data structure abstract
 */
sealed abstract class CF[A] extends java.io.Serializable {

  def ++(other: CF[A]): CF[A]

  def +(other: A): CF[A]


  def checkAndAdd(item: A): (CF[A], ApproximateBoolean)

  def contains(item: A): ApproximateBoolean = {
    if (maybeContains(item)) {
      // Bloom filter stuff here
    }
    // That's the deal
    ApproximateBoolean.exactFalse
  }

  def maybeContains(item: A): Boolean

  def numBits: Int

  def width: Int

  def density = numBits.toDouble / width

  def size: Approximate[Long]
}

/**
  * Empty cuckoo filter
  * */
case class CFZero[A](fingerPrintBucket : Int, maxKicks : Int = 3, totalBuckets : Int = 256) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def numBits: Int = ???

  override def width: Int = ???

  override def size: Approximate[Long] = Approximate.zero

  override def ++(other: CF[A]): CF[A] = other

  override def +(other: A): CF[A] = new CFItem[A](other, fingerPrintBucket, maxKicks = maxKicks, totalBuckets = totalBuckets)
}

/**
  * One item cuckoo
  * */

case class CFItem[A](item: A,fingerPrintBucket : Int, maxKicks : Int = 3, totalBuckets : Int = 256 ) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???
  override def maybeContains(item: A): Boolean = ???
  override def numBits: Int = ???
  override def width: Int = ???
  override def size: Approximate[Long] = ???
  override def ++(other: CF[A]): CF[A] = CFInstance()
  override def +(other: A): CF[A] = this ++ CFItem[A](other, fingerPrintBucket, maxKicks = maxKicks, totalBuckets = totalBuckets)
}

/**
  * Multiple items cuckoo
  * */

case class CFInstance[A]() extends CF[A] {
  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???
  override def maybeContains(item: A): Boolean = ???
  override def numBits: Int = ???
  override def width: Int = ???
  override def size: Approximate[Long] = ???
  override def ++(other: CF[A]): CF[A] = ???
  override def +(other: A): CF[A] = ???
}


/**
  * The hash class for cuckoo
  * */
case class CFHash[A]()(implicit hash1: Hash128[A], hash2: Hash128[A]) {

  def apply(valueToHash: A)(implicit hash1: Hash128[A], hash2: Hash128[A]): Array[Int] = {
    println(hash1.hash(valueToHash), "hash2 :: " + hash2.hash(valueToHash))
    Array(hash1.hash(valueToHash)._1.toInt, hash2.hash(valueToHash)._1.toInt)
  }



}
// TODO : here is for setting fingerprint size.
case class CFFingerprint[A](){

}