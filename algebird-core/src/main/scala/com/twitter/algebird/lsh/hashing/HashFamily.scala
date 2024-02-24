package com.twitter.algebird.lsh.hashing

import com.twitter.algebird.lsh.vector.BaseLshVector

trait Hasher {
  def hash(vector: Array[Double]): Int
}

trait HashFamily {
  val familyId = -1 // Should be unique to each Family.
  def createHasher(hashTableId: Int, hashFunctionId: Int): Hasher
  def combine(hashes: Array[Int]): Int
  def score(keyVec: BaseLshVector, candidateVec: BaseLshVector): Double
}
