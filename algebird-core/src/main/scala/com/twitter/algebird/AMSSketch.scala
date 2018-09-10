package com.twitter.algebird

import cats.kernel.CommutativeMonoid

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


object AMSSketch {
  def apply[A](buckets : Int, depth : Int)(implicit hash: Hash128[A]):  AMSSketch = new AMSSketch()
}

class AMSSketch {

}



class AMSSketchMonoid[K : CMSHash[K]]() extends Monoid[CMS[K]] with CommutativeMonoid[CMS[K]] {
  override def zero: CMS[K] = ???

  override def plus(x: CMS[K], y: CMS[K]): CMS[K] = ???
}

class AMSSketchZero[A]() extends  AMSSketchMonoid[A] {
}

object AMSFunction  {
  def fourwise[K : CMSHasher](a : Long, b : Long, c : Long, d : Long, x : Long)(implicit hash128: Hash128[Long]) : Long = {
    1L
  }

  def generateHash[K : CMSHasher](numHashes : Int, counters : Int) : Seq[CMSHash[K]] = {

    @tailrec
    def createHash(buffer : Seq[CMSHash[K]], idx : Int, seed : Int): Seq[CMSHash[K]] ={
      if (idx == 0 ) buffer else createHash(buffer:+CMSHasher[K](Random.nextInt(seed)), idx - 1, seed)
    }



    null
  }

}

sealed abstract class AMS[A] {

}

