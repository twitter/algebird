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

case class AMSParams[K](hashes : Seq[CMSHash[K]],
                        depth : Int,
                        bucket : Int){
  require(depth > 0 && bucket > 0, "buckets and depth should be positive")
  require(
    hashes.size >= depth,
    s"we require at least $depth hash functions")
}

object AMSFunction  {

  def fourwise[K : CMSHasher](a : Long, b : Long, c : Long, d : Long, x : Long) : Long = {
    1L
  }

  def generateHash[K : CMSHasher](numHashes : Int, counters : Int) : Seq[CMSHash[K]] = {

    @tailrec
    def createHash(buffer : Seq[CMSHash[K]], idx : Int, seed : Int): Seq[CMSHash[K]] ={
      if (idx == 0 ) buffer else createHash(buffer:+CMSHash[K](Random.nextInt(seed), 0, counters), idx - 1, seed)
    }
    createHash(Seq.empty[CMSHash[K]], numHashes, counters)
  }
}


object AMSSketch {
  def apply[A](buckets : Int, depth : Int):  AMSSketch = new AMSSketch()
}

class AMSSketch {


}

trait AMSCounting[K, C[_]] {

  def +(item : K) : C[K]

  def ++(other : C[K]) : C[K]

  def f1 : Long = totalCount

  def innerProduct(other: C[K]): Approximate[Long]

  def f2 : Approximate[Long]

  def totalCount : Long
}

class AMSMonoid[K : CMSHasher](depth : Int, buckets : Int) extends Monoid[AMS[K]] with CommutativeMonoid[AMS[K]] {
  val params  = AMSParams(AMSFunction.generateHash(depth, buckets), depth, buckets)

  override def zero: AMS[K] = AMSZero[K](params)

  override def plus(x: AMS[K], y: AMS[K]): AMS[K] = x ++ y
}

case class AMSZero[A](override val params: AMSParams[A]) extends  AMS[A](params) {
  override def depth: Int = 0

  override val totalCount: Long = 0

  override def buckets: Int = 0


  override def innerProduct(other: AMS[A]): Approximate[Long] = ???

  override def +(item: A): AMS[A] = ???

  override def ++(other: AMS[A]): AMS[A] = ???
}

case class AMSItem[A](item : A,
                 override val totalCount : Long,
                 override val params: AMSParams[A])
  extends AMS[A](params) {

  override def depth: Int = 1

  override def buckets: Int = 1


  override def innerProduct(other: AMS[A]): Approximate[Long] = ???

  override def +(item: A): AMS[A] = ???

  override def ++(other: AMS[A]): AMS[A] = ???
}

case class AMSInstances[A](countsTable: CountsTable[A],
                      override val params: AMSParams[A],
                      override val totalCount: Long)
  extends  AMS[A](params) {

  override def depth: Int = params.depth

  override def buckets: Int = params.bucket


  override def innerProduct(other: AMS[A]): Approximate[Long] = ???

  override def +(item: A): AMS[A] = ???

  override def ++(other: AMS[A]): AMS[A] = ???
}


object AMSInstances {
  def apply[A](params: AMSParams[A]): AMSInstances[A] = {
    val countsTable = CountsTable[A](params.depth, params.bucket)
    new AMSInstances[A](countsTable, params, 0)
  }
}


sealed abstract class AMS[A](val params: AMSParams[A]) extends AMSCounting[A, AMS] {

  def depth : Int

  def buckets : Int

  override val  f2: Approximate[Long] = innerProduct(this)
}

