package com.twitter.algebird.lsh.vector

/**
 * Vector math functions for LshVectors.
 */
object VectorMath {
  def divide(vector: BaseLshVector, scalar: Double) = { //vectorFn(vector, multiplyFn(scalar, _))
    val returnVec = Array.ofDim[Double](vector.size)
    var idx = 0
    while (idx < vector.size) {
      returnVec(idx) = vector(idx) / scalar
      idx += 1
    }
    LshVector(returnVec)
  }

  def multiply(vector: BaseLshVector, scalar: Double): BaseLshVector = {
    val returnVec = Array.ofDim[Double](vector.size)
    var idx = 0
    while (idx < vector.size) {
      returnVec(idx) = vector(idx) * scalar
      idx += 1
    }
    LshVector(returnVec)
  }

  def vectorDivide(vector1: BaseLshVector, vector2: BaseLshVector): BaseLshVector = {
    val minSize = math.min(vector1.size, vector2.size)
    val returnVec = Array.ofDim[Double](minSize)
    var idx = 0
    while (idx < minSize) {
      returnVec(idx) = if (vector2(idx) == 0.0) {
        0.0
      } else {
        vector1(idx) / vector2(idx)
      }
      idx += 1
    }
    LshVector(returnVec)
  }

  def vectorMultiply(vector1: BaseLshVector, vector2: BaseLshVector): BaseLshVector = {
    val minSize = math.min(vector1.size, vector2.size)
    val returnVec = Array.ofDim[Double](minSize)
    var idx = 0
    while (idx < minSize) {
      returnVec(idx) = vector1(idx) * vector2(idx)
      idx += 1
    }
    LshVector(returnVec)
  }

  def vectorSum(vector1: BaseLshVector, vector2: BaseLshVector): BaseLshVector = {
    val minSize = math.min(vector1.size, vector2.size)
    val returnVec = Array.ofDim[Double](minSize)
    var idx = 0
    while (idx < minSize) {
      returnVec(idx) = vector1(idx) + vector2(idx)
      idx += 1
    }
    LshVector(returnVec)
  }

  def dot[@specialized(Double, Int, Long) T](vector1: Array[T], vector2: Array[T])(implicit num: Numeric[T]): T = {
    val minSize = math.min(vector1.size, vector2.size)
    var sum = num.zero
    var idx = 0
    while (idx < minSize) {
      val times = num.times(vector1(idx), vector2(idx))
      sum = num.plus(sum, times)
      idx += 1
    }
    sum
  }

  def magnitude(vec: BaseLshVector) = { //math.sqrt(vectorFn(vector, square).sum)
    var sum = 0.0
    var idx = 0
    while (idx < vec.size) {
      sum += vec(idx) * vec(idx)
      idx += 1
    }
    math.sqrt(sum)
  }

  def normalize[U <: LshVector](vector: U): LshVector = divide(vector, magnitude(vector))
}
