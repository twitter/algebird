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

  def apply[A](bucketSize : Int, fingerPrintSize : LongBitSet, kicks : Int, buckets : Int ): CuckooFilterMonoid[A] = {
    CuckooFilterMonoid[A]()
  }

}


case class CuckooFilterMonoid[A]()(implicit h1 : Hash128[A], h2 : MurmurHash128) extends Monoid[CF[A]] with BoundedSemilattice[BF[A]]{
  override def zero: CF[A] = ???

  override def plus(x: CF[A], y: CF[A]): CF[A] = ???
}


/**
  * Cuckoo data structure
  */

sealed abstract class CF[A] extends java.io.Serializable {

  def hash1 : Hash128[A]
  def hash2 : CassandraMurmurHash[A]

  def checkAndAdd(item : A) : (CF[A], ApproximateBoolean)

  def contains(item : A) : ApproximateBoolean =  {
    if (maybeContains(item)){
      // Bloom filter stuff here
    }
    // That's the deal
    ApproximateBoolean.exactFalse
  }

  def maybeContains(item : A) : Boolean

  def numBits : Int

  def width : Int

  def density = numBits.toDouble / width

  def size : Approximate[Long]
}

case class CFHash()


case class CFZero[A]() extends CF[A] {
  override def hash1: Hash128[A] = ???

  override def hash2: CassandraMurmurHash[A] = ???

  override def checkAndAdd(item: A): (CF[A], ApproximateBoolean) = ???

  override def maybeContains(item: A): Boolean = ???

  override def numBits: Int = ???

  override def width: Int = ???

  override def size: Approximate[Long] = ???
}