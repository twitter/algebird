package com.twitter.algebird

package object legacy {

  /**
   * For legacy code (Algebird versions < 0.8.1) that uses CMS we provide this functionally equivalent CMS type, which
   * is a CMS with [[Long]]-based keys.
   *
   * This type is an alias for `TopCMS[Long]` (see [[TopCMS]]).
   *
   * =Usage=
   *
   * You typically create instances of this type via [[CountMinSketchMonoid]].
   */
  @deprecated("You should use TopCMS[Long] instead of legacy.CMS", since = "0.8.1")
  type CMS = TopCMS[Long]

  /**
   * For legacy code (Algebird versions < 0.8.1) that uses CMS we provide this functionally equivalent CMS monoid, which
   * allows you to work with (e.g. create) top-% CMS instances with [[Long]]-based keys.
   *
   * =Usage=
   *
   * You typically create instances of this type via `CountMinSketchMonoid.apply()`, see example below.
   *
   * =Example=
   *
   * {{{
   * import com.twitter.algebird.legacy.CountMinSketchMonoid
   *
   * // Pre-0.8.1 you would have used `new CountMinSketchMonoid(EPS, DELTA, SEED)` (note the `new`).
   * val cmsMonoid: CountMinSketchMonoid = CountMinSketchMonoid(EPS, DELTA, SEED)
   * }}}
   *
   * =Implementation details=
   *
   * This type is an alias for `TopPctCMSMonoid[Long]` (see [[TopPctCMSMonoid]]).
   */
  @deprecated("You should use TopPctCMSMonoid[Long] instead of legacy.CountMinSketchMonoid", since = "0.8.1")
  type CountMinSketchMonoid = TopPctCMSMonoid[Long]

}