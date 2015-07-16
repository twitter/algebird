package com.twitter.algebird.macros

import com.twitter.algebird._
import scala.language.experimental.macros

object caseclass {
  implicit def semigroup[T]: Semigroup[T] = macro SemigroupMacro.caseClassSemigroup[T]
  implicit def monoid[T]: Monoid[T] = macro MonoidMacro.caseClassMonoid[T]
  implicit def group[T]: Group[T] = macro GroupMacro.caseClassGroup[T]
  implicit def ring[T]: Ring[T] = macro RingMacro.caseClassRing[T]
}
