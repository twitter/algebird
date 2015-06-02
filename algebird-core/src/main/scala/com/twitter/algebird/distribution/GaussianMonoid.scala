package com.twitter.algebird

import com.twitter.algebird.Monoid

/**
 * @mean - Mean
 * @sigma - Standard deviation
 */
case class Gaussian(mean: Double, sigma: Double)


object Gaussian {
  implicit def monoid: Monoid[Gaussian] = GaussianMonoid
}


// To use the OrValMonoid wrap your item in a OrVal object
case class OrVal(get: Boolean) extends AnyVal

object OrVal {
  implicit def monoid: Monoid[OrVal] = OrValMonoid
  def unboxedMonoid: Monoid[Boolean] = new Monoid[Boolean] {
    def zero = false
    def plus(l: Boolean, r: Boolean) = l || r
    override def sumOption(its: TraversableOnce[Boolean]) =
      if (its.isEmpty) None
      else Some(its.exists(identity))
  }
}

/**
 * Boolean OR monoid.
 * plus means logical OR, zero is false.
 */
object OrValMonoid extends Monoid[OrVal] {
  override def zero = OrVal(false)
  override def plus(l: OrVal, r: OrVal) = if (l.get) l else r
  override def sumOption(its: TraversableOnce[OrVal]) =
    if (its.isEmpty) None
    else Some(OrVal(its.exists(_.get)))
}