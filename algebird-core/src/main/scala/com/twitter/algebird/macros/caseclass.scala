package com.twitter.algebird.macros

import com.twitter.algebird._
import scala.language.experimental.macros

object caseclass {
  def semigroup[T]: Semigroup[T] =
    macro SemigroupMacro.caseClassSemigroup[T]
  def monoid[T]: Monoid[T] = macro MonoidMacro.caseClassMonoid[T]
  def group[T]: Group[T] = macro GroupMacro.caseClassGroup[T]
  def ring[T]: Ring[T] = macro RingMacro.caseClassRing[T]
}
