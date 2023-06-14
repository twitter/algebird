package com.twitter.algebird.macros

import com.twitter.algebird.*

object caseclass:
  inline final def semigroup[T]: Semigroup[T] = ${ SemigroupMacro.derivedSemigroup[T] }
  inline def monoid[T]: Monoid[T] = ${ MonoidMacro.derivedMonoid[T] }
  inline def group[T]: Group[T] = ${ GroupMacro.derivedGroup[T] }
  inline def ring[T]: Ring[T] = ${ RingMacro.derivedRing[T] }
