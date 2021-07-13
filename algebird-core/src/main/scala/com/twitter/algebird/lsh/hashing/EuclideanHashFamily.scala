package com.twitter.algebird.lsh.hashing

import java.util.Arrays

import com.twitter.algebird.lsh.vector.{ BaseLshVector, VectorMath }

import scala.util.Random

class EuclideanHashFamily(
  radius: Double,
  dimension: Int)
  extends HashFamily with Serializable {
  override val familyId = 1
  override def hashCode = dimension * 1000 + (radius * 10).toInt + familyId

  def createHasher(hashTableId: Int, hashFunctionId: Int): Hasher = {
    new EuclideanHasher(hashTableId, hashFunctionId, radius, dimension)
  }

  def combine(hashes: Array[Int]): Int = Arrays.hashCode(hashes)

  def score(keyVec: BaseLshVector, candidateVec: BaseLshVector): Double =
    VectorMath.dot(keyVec.toDoubleVec, candidateVec.toDoubleVec)
}

class EuclideanHasher(
  hashTableId: Int,
  hashFunctionId: Int,
  radius: Double = 4,
  dimension: Int)
  extends Hasher with Serializable {
  val rand = new Random(hashTableId * 10000 + hashFunctionId)

  val offset = rand.nextInt(radius.toInt)
  val randomProjection = Array.ofDim[Double](dimension)
  for (i <- 0 until dimension)
    randomProjection.update(i, rand.nextGaussian)

  println("Euclidean Hasher: tableId: %d, hashFunctionId: %d, offset: %d, projection: %s",
    hashTableId, hashFunctionId, offset, randomProjection.reduce(_ + _))

  def hash(vector: Array[Double]): Int = {
    scala.math.round((VectorMath.dot(vector, randomProjection) + offset) / radius).toInt
  }
}