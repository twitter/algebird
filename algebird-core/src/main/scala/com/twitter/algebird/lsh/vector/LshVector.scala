package com.twitter.algebird.lsh.vector

import com.twitter.algebird.{ ArrayMonoid, Monoid }
import java.util

/**
 * An LshVector must permit the operations listed below.
 */
trait BaseLshVector {
  def size: Int
  def apply(index: Int): Double // Return Vector(index)
  def toDoubleVec: Array[Double]
  override def hashCode: Int = util.Arrays.hashCode(toDoubleVec)
}

/**
 * DoubleLshVector is just a thin wrapper around an Array of Doubles. It's pretty much the most
 * basic LshVector possible.
 * @param vector - The Array of Doubles to wrap.
 */
case class LshVector(vector: Array[Double]) extends BaseLshVector {
  def size = vector.size
  def apply(index: Int) = vector(index)
  def toDoubleVec = vector
}

object LshVector {
  implicit val lshVectorMonoid = LshVectorMonoid
}

object LshVectorMonoid extends Monoid[LshVector] {
  val arrayMonoid = new ArrayMonoid[Double]()
  override def zero = LshVector(arrayMonoid.zero)

  override def plus(left: LshVector, right: LshVector) =
    LshVector(arrayMonoid.plus(left.vector, right.vector))
}