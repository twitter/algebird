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

import algebra.BoundedSemilattice
import com.googlecode.javaewah.datastructure.BitSet
import scala.util.Random


/**
  * Cuckoo filter for Algebird in a Monoid way of life.
  * referring :
  *  - https://github.com/twitter/algebird/issues/560#issue-185161455
  *  - http://www.eecs.harvard.edu/%7Emichaelm/postscripts/cuckoo-conext2014.pdf
  *  - https://github.com/irfansharif/cfilter
  * By nature, this filter isn't commutative
  *
  * From the inital paper, there is no problem to consider || fingerprint|| = ln(N) where N = fingerprintPerBucket * totalBucket
  * "" as long as we use reasonably sized buckets, the fingerprint size can remain small. "", we'll use a a 32 bits fingerprint.
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

  override def zero: CF[A] = CFZero(fingerprintBucket)

  override def plus(left: CF[A], right: CF[A]): CF[A] = left ++ right

  /**
    * Create a cuckoo filter with one item.
    */
  def create(item: A): CF[A] = CFItem[A](item, new CFHash[A](), fingerprintBucket, totalBuckets)

  /**
    * Create a cuckoo filter with multiple items.
    */
  def create(data: A*): CF[A] = create(data.iterator)

  /**
    * Create a cuckoo filter with multiple items from an iterator
    */
  def create(data: Iterator[A]): CF[A] = ???

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
      // cuckoo checking here
    }
    // That's the deal
    ApproximateBoolean.exactFalse
  }

  def delete(item: A): Boolean

  def lookup(item: A): Boolean

  def maybeContains(item: A): Boolean

  val maxKicks = 256

  def size: Approximate[Long]
}

/**
  * Empty cuckoo filter
  **/
case class CFZero[A](fingerPrintBucket: Int, totalBuckets: Int = 256)(implicit hash: Hash128[A]) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def size: Approximate[Long] = Approximate.zero

  override def ++(other: CF[A]): CF[A] = other

  override def +(other: A): CF[A] = new CFItem[A](other, new CFHash[A](), fingerPrintBucket, totalBuckets)

  override def lookup(elem: A): Boolean = false

  override def delete(item: A): Boolean = false
}

/**
  * One item cuckoo
  **/

case class CFItem[A](item: A, cFHash: CFHash[A], fingerprintBucket: Int, totalBuckets: Int = 256)(implicit hashFingerprint: Hash128[A], hashFingerprintRaw: Hash128[Int]) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def size: Approximate[Long] = ???

  override def ++(other: CF[A]): CF[A] = {
    CFInstance(cFHash, Array.fill[BitSet](totalBuckets)(new BitSet(fingerprintBucket * 32)), fingerprintBucket, totalBuckets)
  }

  override def +(other: A): CF[A] = this ++ CFItem[A](other, cFHash, fingerprintBucket, totalBuckets)

  override def lookup(elem: A): Boolean = elem == item

  override def delete(item: A): Boolean = false
}

/**
  * Multiple items cuckoo
  **/
case class CFInstance[A](hash: CFHash[A],
                         cuckooBitSet: Array[BitSet],
                         fingerprintBucket: Int,
                         totalBuckets: Int)(implicit hashFingerprint: Hash128[A], hashFingerprintRaw: Hash128[Int]) extends CF[A] {

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def size: Approximate[Long] = ???

  override def ++(other: CF[A]): CF[A] = {
    other match {

      case CFZero(_, _) => this
    }
  }

  def swapRandomFingerprint(bucketIndex: Int, fingerprint: Int): Int = {
    val bucket = cuckooBitSet(bucketIndex)
    if (bucket.cardinality() > 0) {
      var fingerprints: List[Int] = List()
      val it = bucket.intIterator()
      while (it.hasNext)
        fingerprints = fingerprints.::(it.next())
      val randomIndex = Random.nextInt(fingerprints.size)

      deleteFingerprint(bucketIndex, fingerprints(randomIndex))
      insertFingerprint(bucketIndex, fingerprint)

      return fingerprints(randomIndex)
    }
    fingerprint
  }

  private def insert(elem: A): Boolean = {
    val fp = new Fingerprint[A]().apply(elem)
    val h = hash(1, elem) % totalBuckets
    val k = (fp ^ h) % totalBuckets
    insertFingerprint(h.toInt, fp) || insertFingerprint(k.toInt, fp) || {
      // choose random index to start kick
      var index = if (Random.nextBoolean()) k else h
      for (n <- 0 until maxKicks) {
        val fingerprintKicked = swapRandomFingerprint(index.toInt, fp)
        // partial cuckoo hash key
        index = (index ^ hashFingerprint(fingerprintKicked)) % totalBuckets
        if (insertFingerprint(index.toInt, fingerprintKicked)) {
          return true
        }
      }
      return false
    }
  }

  private def hashFingerprint(fp: Int): Int = {
    hashFingerprintRaw.hash(fp)._1.toInt & 0x7fffffff
  }

  override def +(other: A): CFInstance[A] = {
    if (insert(other)) {
      return new CFInstance[A](hash, cuckooBitSet, fingerprintBucket, totalBuckets)
    }
    throw new RuntimeException("can't add element to a full cuckoo filter.")
  }


  private def deleteFingerprint(indexBucket: Int, fp: Int): Boolean = {
    val bucket = cuckooBitSet(indexBucket)
    if (bucket.empty() || isFingerprintInBuck(bucket, fp))
      return false
    val it = bucket.intIterator()
    val bitSet = new BitSet(fingerprintBucket * 64 + 8)
    while (it.hasNext) {
      if (it.next() != fp)
        bitSet.set(it.next())
    }
    cuckooBitSet.update(indexBucket, bitSet)
    true
  }

  private def insertFingerprint(index: Int, fp: Int): Boolean = {
    if (cuckooBitSet(index).cardinality() < fingerprintBucket) {
      cuckooBitSet(index).set(fp)
      return true
    }
    false
  }

  private def isFingerprintInBuck(bucket: BitSet, fp: Int): Boolean = {
    val it = bucket.intIterator()
    while (it.hasNext)
      if (it.next() == fp)
        true
    false
    false
  }

  def hashes(elem: A): (Long, Long, Int) = {
    val fp = new Fingerprint[A]().apply(elem)
    val h = hash(1, elem) % totalBuckets
    val k = (fp ^ h) % totalBuckets
    (h, k, fp)
  }


  override def lookup(elem: A): Boolean = {
    true
  }

  override def delete(elem: A): Boolean = {
    val h = hashes(elem)
    if (deleteFingerprint(h._1.toInt, h._3) || deleteFingerprint(h._2.toInt, h._3))
      return true
    false
  }
}

// Let's be generic because the fingerprint have to be hashable
private[algebird] case class Fingerprint[A: Hash128]() {

  def seedFingerprint = 128

  def apply(elem: A)(implicit hash: Hash128[A]): Int = {
    val hashed = hash.hashWithSeed(seedFingerprint, elem)._1
    math.abs(hashed.toByte)
  }
}


/**
  * The hash class for cuckoo
  **/
private[algebird] case class CFHash[A]()(implicit hash: Hash128[A]) {

  def apply(seed: Long, valueToHash: A): Long = {

    val hashed = hash.hashWithSeed(seed, valueToHash)._1
    math.abs(hashed.toByte).toByte

  }

}
