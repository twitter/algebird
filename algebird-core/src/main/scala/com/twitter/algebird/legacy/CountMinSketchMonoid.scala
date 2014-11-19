package com.twitter.algebird.legacy

import com.twitter.algebird.TopPctCMS

/**
 * Creates instances of type `CountMinSketchMonoid` (which is a type alias for the legacy class of the same name in
 * Algebird versions < 0.8.1).
 *
 * =Deprecated since 0.8.1=
 *
 * Only use this object when transitioning legacy code.  Newer code should use [[TopPctCMS.monoid()]]:
 *
 * {{{
 * import com.twitter.algebird.CMSHasherImplicits._
 * val cmsMonoid = TopPctCMS.monoid[Long](...)
 * }}}
 */
object CountMinSketchMonoid {

  import com.twitter.algebird.CMSHasherImplicits._

  @deprecated(
    "You should use TopPctCMS.monoid[Long]() instead of legacy.CountMinSketchMonoid, and import CMSHasherImplicits._",
    since = "0.8.1")
  def apply(eps: Double, delta: Double, seed: Int, heavyHittersPct: Double = 0.01): CountMinSketchMonoid =
    TopPctCMS.monoid[Long](eps, delta, seed, heavyHittersPct)

}