package com.twitter.algebird

/**
 * The Count-Min sketch uses `d` (aka `depth`) pair-wise independent hash functions drawn from a universal hashing
 * family of the form:
 *
 * `h(x) = [a * x + b (mod p)] (mod m)`
 *
 * As a requirement for using CMS you must provide an implicit `CMSHasher[K]` for the type `K` of the items you want to
 * count.  Algebird ships with several such implicits for commonly used types `K` such as `Long` and `BigInt`.
 *
 * If your type `K` is not supported out of the box, you have two options: 1) You provide a "translation" function to
 * convert items of your (unsupported) type `K` to a supported type such as [[Double]], and then use the `contramap`
 * function of [[CMSHasher]] to create the required `CMSHasher[K]` for your type (see the documentation of `contramap`
 * for an example); 2) You implement a `CMSHasher[K]` from scratch, using the existing CMSHasher implementations as a
 * starting point.
 */
trait CMSHasher[K] extends java.io.Serializable {

  self =>

  /**
   * Returns `a * x + b (mod p) (mod width)`.
   */
  def hash(a: Int, b: Int, width: Int)(x: K): Int

  /**
   * Given `f`, a function from `L` into `K`, creates a `CMSHasher[L]` whose hash function is equivalent to:
   *
   * {{{
   * def hash(a: Int, b: Int, width: Int)(x: L): CMSHasher[L] = CMSHasher[K].hash(a, b, width)(f(x))
   * }}}
   */
  def on[L](f: L => K): CMSHasher[L] = new CMSHasher[L] {
    override def hash(a: Int, b: Int, width: Int)(x: L): Int =
      self.hash(a, b, width)(f(x))
  }

  /**
   * Given `f`, a function from `L` into `K`, creates a `CMSHasher[L]` whose hash function is equivalent to:
   *
   * {{{
   * def hash(a: Int, b: Int, width: Int)(x: L): CMSHasher[L] = CMSHasher[K].hash(a, b, width)(f(x))
   * }}}
   *
   * Be aware that the use of contramap may come at a cost (e.g. increased time) due to the translation calls between
   * `K` and `L`.
   *
   * =Usage=
   *
   * The following example creates a CMSHasher for the unsupported type `K=Double`:
   *
   * {{{
   * def f(d: Double): Array[Byte] = {
   *   val l: Long = java.lang.Double.doubleToLongBits(d)
   *   java.nio.ByteBuffer.allocate(8).putLong(l).array()
   * }
   *
   * implicit val cmsHasherDouble: CMSHasher[Double] = CMSHasherArrayByte.contramap((d: Double) => f(d))
   * }}}
   */
  def contramap[L](f: L => K): CMSHasher[L] = on(f)

}

object CMSHasher {
  implicit object CMSHasherLong extends CMSHasher[Long] {

    override def hash(a: Int, b: Int, width: Int)(x: Long): Int = {
      val unModded: Long = (x * a) + b
      // Apparently a super fast way of computing x mod 2^p-1
      // See page 149 of http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
      // after Proposition 7.
      val modded: Long = (unModded + (unModded >> 32)) & Int.MaxValue
      // Modulo-ing integers is apparently twice as fast as modulo-ing Longs.
      modded.toInt % width
    }

  }

  implicit val cmsHasherShort: CMSHasher[Short] =
    CMSHasherInt.contramap(x => x.toInt)

  implicit object CMSHasherInt extends CMSHasher[Int] {

    override def hash(a: Int, b: Int, width: Int)(x: Int): Int = {
      val unModded: Int = (x * a) + b
      val modded: Long = (unModded + (unModded >> 32)) & Int.MaxValue
      modded.toInt % width
    }
  }

  /**
   * =Implementation details=
   *
   * This hash function is based upon Murmur3.  Note that the original CMS paper requires
   * `d` (depth) pair-wise independent hash functions;  in the specific case of Murmur3 we argue that it is sufficient
   * to pass `d` different seed values to Murmur3 to achieve a similar effect.
   *
   * To seed Murmur3 we use only `a`, which is a randomly drawn `Int` via [[scala.util.Random]] in the CMS code.
   * What is important to note is that we intentionally ignore `b`.  Why?  We need to ensure that we seed Murmur3 with
   * a random value, notably one that is uniformly distributed.  Somewhat surprisingly, combining two random values
   * (such as `a` and `b` in our case) typically worsens the "randomness" of the combination, i.e. the combination is
   * less uniformly distributed as either of its original inputs.  Hence the combination of two random values is
   * discouraged in this context, notably if the two random inputs were generated from the same source anyways, which
   * is the case for us because we use Scala's PRNG only.
   *
   * For further details please refer to the discussion
   * [[http://stackoverflow.com/questions/3956478/understanding-randomness Understanding Randomness]] on
   * StackOverflow.
   *
   * @param a Must be a random value, typically created via [[scala.util.Random]].
   * @param b Ignored by this particular hash function, see the reasoning above for the justification.
   * @param width Width of the CMS counting table, i.e. the width/size of each row in the counting table.
   * @param x Item to be hashed.
   * @return Slot assigned to item `x` in the vector of size `width`, where `x in [0, width)`.
   */
  private[algebird] def hashBytes(a: Int, b: Int, width: Int)(x: Array[Byte]): Int = {
    val _ = b // suppressing unused `b`
    val hash: Int = scala.util.hashing.MurmurHash3.arrayHash(x, a)
    // We only want positive integers for the subsequent modulo.  This method mimics Java's Hashtable
    // implementation.  The Java code uses `0x7FFFFFFF` for the bit-wise AND, which is equal to Int.MaxValue.
    val positiveHash = hash & Int.MaxValue
    positiveHash % width
  }

  // This CMSHasher[String] is newer, and faster, than the old version
  // found in CMSHasherImplicits. Unless you have serialized data that
  // requires the old implementation for correctness, you should be
  // using this instance.
  implicit object CMSHasherString extends CMSHasher[String] {
    override def hash(a: Int, b: Int, width: Int)(x: String): Int =
      (scala.util.hashing.MurmurHash3.stringHash(x, a) & Int.MaxValue) % width
  }

  implicit object CMSHasherBytes extends CMSHasher[Bytes] {
    override def hash(a: Int, b: Int, width: Int)(x: Bytes): Int =
      hashBytes(a, b, width)(x.array)
  }

  implicit object CMSHasherByteArray extends CMSHasher[Array[Byte]] {
    override def hash(a: Int, b: Int, width: Int)(x: Array[Byte]): Int =
      hashBytes(a, b, width)(x)
  }

  // Note: CMSHasher[BigInt] not provided here but in CMSHasherImplicits for legacy support reasons. New hashers
  // should come here.

  implicit object CMSHasherBigDecimal extends CMSHasher[BigDecimal] {
    override def hash(a: Int, b: Int, width: Int)(x: BigDecimal): Int = {

      val uh = scala.util.hashing.MurmurHash3
        .arrayHash(x.underlying.unscaledValue.toByteArray, a)
      val hash = scala.util.hashing.MurmurHash3.productHash((uh, x.scale), a)

      // We only want positive integers for the subsequent modulo.  This method mimics Java's Hashtable
      // implementation.  The Java code uses `0x7FFFFFFF` for the bit-wise AND, which is equal to Int.MaxValue.
      val positiveHash = hash & Int.MaxValue
      positiveHash % width
    }
  }

}
