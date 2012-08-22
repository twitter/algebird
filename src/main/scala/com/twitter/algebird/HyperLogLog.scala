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

import scala.collection.BitSet

import java.util.Arrays

/** Implementation of the HyperLogLog approximate counting as a Monoid
 * @link http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 *
 * HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm
 * Philippe Flajolet and Éric Fusy and Olivier Gandouet and Frédéric Meunier
 */
object HyperLogLog {
  val md = java.security.MessageDigest.getInstance("MD5")
  def hash(input : Array[Byte]) : Array[Byte] = md.digest(input)

  implicit def int2Bytes(i : Int) = {
    val buf = new Array[Byte](4)
    java.nio.ByteBuffer
      .wrap(buf)
      .putInt(i)
    buf
  }

  implicit def long2Bytes(i : Long) = {
    val buf = new Array[Byte](8)
    java.nio.ByteBuffer
      .wrap(buf)
      .putLong(i)
    buf
  }

  def twopow(i : Int) : Double = scala.math.pow(2.0, i)

  def bytesToBitSet(in : Array[Byte]) : BitSet = {
    BitSet(in.zipWithIndex.map { bi => (bi._1, bi._2 * 8) }
      .flatMap { byteToIndicator(_) } : _*)
  }
  def byteToIndicator(bi : (Byte,Int)) : Seq[Int] = {
    (0 to 7).flatMap { i =>
      if (((bi._1 >> (7 - i)) & 1) == 1) {
        Vector(bi._2 + i)
      }
      else {
        Vector[Int]()
      }
    }
  }
}

/**
 * These are the individual instances which the Monoid knows how to add
 */
case class HLLInstance(v : Array[Byte]) extends java.io.Serializable {
  lazy val zeroCnt = v.count { _ == 0 }
  lazy val isZero = zeroCnt == v.size

  def +(other : HLLInstance) : HLLInstance = {
    val (larger, smaller) = if (v.size > other.v.size) (v, other.v) else (other.v, v)
    // pad smaller array with 0s at front
    new HLLInstance(larger.reverse
      .zip(smaller.reverse.toStream ++ Stream.continually(0.toByte))
      .map { pair => pair._1 max pair._2 }
      .reverse)
  }

  override def equals(other: Any) = {
    other.isInstanceOf[HLLInstance] && Arrays.equals(v, other.asInstanceOf[HLLInstance].v)
  }

  // Named from the parameter in the paper, probably never useful to anyone
  // except HyperLogLogMonoid
  lazy val z : Double = 1.0 / (v.map { mj => HyperLogLog.twopow(-mj) }.sum)
}

/*
 * Error is about 1.04/sqrt(2^{bits}), so you want something like 12 bits for 1% error
 * which means each HLLInstance is about 2^{12 + 2} = 16kb per instance.
 */
class HyperLogLogMonoid(val bits : Int) extends Monoid[HLLInstance] {
  import HyperLogLog._

  assert(bits > 3, "Use at least 4 bits (2^(bits+2) = bytes consumed)")
  // These parameters come from the paper
  val (alpha, memSize) = bits match {
    case 4 => (0.673, 1 << 4)
    case 5 => (0.697, 1 << 5)
    case 6 => (0.709, 1 << 6)
    case _ => {
      val m = 1 << bits
      (0.7213/(1.0 + 1.079/m), m)
    }
  }

  def apply[T <% Array[Byte]](t : T) = create(t)

  // We are computing j and \rho(w) from the paper,
  // sorry for the name, but it allows someone to compare
  // to the paper
  // extremely low probability rhow (position of the leftmost one bit) is > 127, so we use a Byte to store it
  protected def jRhoW(in : Array[Byte]) : (Int,Byte) = {
    val onBits = HyperLogLog.bytesToBitSet(in)
    (onBits.filter { _ < bits }.map { 1 << _ }.sum,
     (onBits.filter { _ >= bits }.min - bits + 1).toByte)
  }

  val zero : HLLInstance = new HLLInstance(new Array[Byte](memSize))

  def plus(left : HLLInstance, right : HLLInstance) = left + right

  protected val zeroVector = new Array[Byte](memSize)

  def create(example : Array[Byte]) : HLLInstance = {
    val hashed = HyperLogLog.hash(example)
    val (j,rhow) = jRhoW(hashed)
    new HLLInstance(zeroVector.updated(j, rhow))
  }

  private val largeE = HyperLogLog.twopow(32)/30.0
  private val smallE = 5 * memSize / 2.0

  protected def smallEstimate(hi : HLLInstance, e : Double) : Double = {
    val zeroV = hi.zeroCnt
    if (zeroV == 0) {
      e
    }
    else {
      memSize * scala.math.log(memSize.toDouble / zeroV)
    }
  }

  protected lazy val factor = alpha * (memSize.toDouble * memSize.toDouble)
  // Some constant from the algorithm:
  protected val fourBillionSome = HyperLogLog.twopow(32)

  def estimateSize(hi : HLLInstance) : Double = {
    val e = factor * hi.z
    // There are large and small value corrections from the paper
    if(e > largeE) {
      -fourBillionSome * scala.math.log1p(-e/fourBillionSome)
    }
    else if(e <= smallE) {
      smallEstimate(hi, e)
    }
    else {
      e
    }
  }
}
