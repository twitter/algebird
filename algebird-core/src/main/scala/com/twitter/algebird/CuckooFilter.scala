package com.twitter.algebird

import algebra.BoundedSemilattice

import scala.annotation.tailrec
import scala.collection.BitSet
import scala.util.Random

/**
  * Cuckoo filter for Algebird in a Monoid way of life.
  * referring :
  *  - https://github.com/twitter/algebird/issues/560#issue-185161455
  *  - http://www.eecs.harvard.edu/%7Emichaelm/postscripts/cuckoo-conext2014.pdf
  *  - https://github.com/irfansharif/cfilter
  * By nature, this filter isn't commutative
  *
  **/


object CuckooFilter {
  // TODO : optimal parameter have to be compute here : see Asymptotic behavior of the original paper.
  def apply[A](bucketSize: Int, fingerPrintSize: LongBitSet, kicks: Int, buckets: Int)(
    implicit hash1: Hash128[A]): CuckooFilterMonoid[A] = {
    null
  }
}

/**
  * The cuckoo filter monoid
  **/
// TODO : Add the fingerprint size.
case class CuckooFilterMonoid[A](fingerprintBucket: Int, maxKicks: Int = 3, totalBuckets: Int = 256)(
  implicit h1: Hash128[A])
  extends Monoid[CF[A]]
    with BoundedSemilattice[CF[A]] {

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

  def bucketBits: Int

  def bucketIndex(hash: Long): Int

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

  def density: Double = numBits.toDouble / width

  val maxKicks = 256

  def size: Approximate[Long]
}

/**
  * Empty cuckoo filter
  **/
case class CFZero[A](fingerPrintBit: Int, fingerPrintBucket: Int, totalBuckets: Int = 256)(implicit hash : Hash128[A]) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def numBits: Int = ???

  override def width: Int = ???

  override def size: Approximate[Long] = Approximate.zero

  override def ++(other: CF[A]): CF[A] = other

  override def +(other: A): CF[A] = new CFItem[A](other, new CFHash[A](), fingerPrintBit, fingerPrintBucket, totalBuckets = totalBuckets)

  override def bucketBits: Int = 0

  override def bucketIndex(hash: Long): Int = 0
}

/**
  * One item cuckoo
  **/

case class CFItem[A](item: A, cFHash: CFHash[A], fingerprintBucket: Int, fingerprintBit: Int, totalBuckets: Int = 256) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def numBits: Int = ??? // cFHash.generateFingerPrint(cFHash(item)(1))

  override def width: Int = ???

  override def size: Approximate[Long] = ???

  override def ++(other: CF[A]): CF[A] = ??? //CFInstance()

  override def +(other: A): CF[A] = this ++ CFItem[A](other, cFHash, fingerprintBucket, fingerprintBit,  totalBuckets)

  override def bucketBits: Int = ???

  override def bucketIndex(hash: Long): Int = ???
}

/**
  * Multiple items cuckoo
  **/

/**
  * cuckooBit = new Array
  *
  * */

case class CFInstance[A](hash: CFHash[A],
                         cuckooBitSet: Array[BitSet],
                         fingerprintBit: Int,
                         fingerprintBucket: Int,
                         totalBuckets: Int)(implicit hashFingerprint : Hash128[A], hashFingerprintRaw : Hash128[Long]) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def numBits: Int = {
    cuckooBitSet.foldLeft(0)((z, b) => z + b.size)
  }

  override def width: Int = ???

  override def size: Approximate[Long] = ???

  override def ++(other: CF[A]): CF[A] = {
    other match {

      case CFZero(_, _, _) => this
    }
  }

  def swapFingerPrint(bucketIndex : Int,  fingerprint : Long): Long = {
    val bucket = cuckooBitSet(bucketIndex)
    if (bucket.isEmpty){
      val randomIndex = Random.nextInt(bucket.size)
      val fingerRandom = bucket.toList(randomIndex)

      cuckooBitSet.update(randomIndex, (bucket - fingerRandom) + fingerprint.toInt)
      return fingerRandom
    }
    fingerprint
  }

  private def insert(elem : A) : Boolean = {
    // TODO : it's wrong
    // TODO: type issue, too much .toInt in my taste
    val fp = new Fingerprint[A]().apply(elem)

    val h = hash(1, elem) % totalBuckets
    val k = (fp ^ h) % totalBuckets
    (insertFingerprint(h.toInt, fp) || insertFingerprint(k.toInt, fp)) || {
      // choose random index to start kick
      var index = if (Random.nextBoolean()) k else h
      for (n <- 0 until maxKicks) {
        val fingerprintKicked = swapFingerPrint(index.toInt, fp)
        index = index ^ hashFingerprintRaw.hash(fingerprintKicked)._1 % totalBuckets
        if (cuckooBitSet(index.toInt).size != fingerprintBucket) {
          cuckooBitSet.update(index.toInt, cuckooBitSet(index.toInt) + fingerprintKicked.toInt)
          true
        }
      }
      false
    }
  }

  override def +(other: A): CFInstance[A] = {
    insert(other)
    new CFInstance[A](hash, cuckooBitSet, fingerprintBit, fingerprintBucket, totalBuckets)
  }


  private def deleteFingerprint(index: Int, fp: Long): Unit = {
    cuckooBitSet.update(index, cuckooBitSet(index) - fp.toInt)
  }

  private def insertFingerprint(index: Int, fp: Long): Boolean = {
    if (cuckooBitSet(index).size < fingerprintBucket) {
      cuckooBitSet.update(index, cuckooBitSet(index) + fp.toInt)
      return true
    }
    false
  }

  override def bucketBits: Int = fingerprintBit * fingerprintBucket

  override def bucketIndex(hash: Long): Int = ???
}

// Let's be generic because the fingerprint have to be hashable
private[algebird] case class Fingerprint[A : Hash128]() {
  def seedFingerprint = 128
  def apply(elem: A)(implicit hash: Hash128[A]): Long ={
    hash.hashWithSeed(seedFingerprint, elem)._1
  }
}


/**
  * The hash class for cuckoo
  **/
private[algebird] case class CFHash[A]()(implicit hash: Hash128[A]) {

  def apply(seed: Long, valueToHash: A): Long = {
    hash.hashWithSeed(seed, valueToHash)._1
  }

}
