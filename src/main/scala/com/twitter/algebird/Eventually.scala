/*
Copyright 2013 Twitter, Inc.

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

// Classes that support algebraic structures with dynamic switching between
// multiple representations.  In the case of Semigroup, we specify
// - Two Semigroups L and R
// - A Semigroup homomorphism convert: R => L
// - A conditional mustConvert: R => Boolean
// Then we get a Semigroup[Either[L,R]], where:
//   Left(x)  + Left(y)  = Left(x+y)
//   Left(x)  + Right(y) = Left(x+convert(y))
//   Right(x) + Left(y)  = Left(convert(x)+y)
//   Right(x) + Right(y) = Left(convert(x+y)) if mustConvert(x+y)
//                         Right(x+y) otherwise.
// EventuallyMonoid, EventuallyGroup, and EventuallyRing are defined analogously,
// with the contract that convert respect the appropriate structure.

class EventuallySemigroup[L,R](convert: R => L)(mustConvert: R => Boolean)
  (implicit lSemigroup: Semigroup[L], rSemigroup: Semigroup[R]) extends Semigroup[Either[L,R]] {

  override def plus(x: Either[L,R], y: Either[L,R]) = {
    x match {
      case Left(xl) => y match {
        case Left(yl) => Left(Semigroup.plus(xl, yl))
        case Right(yr) => Left(Semigroup.plus(xl, convert(yr)))
      }
      case Right(xr) => y match {
        case Left(yl) => Left(Semigroup.plus(convert(xr), yl))
        case Right(yr) => conditionallyConvert(Semigroup.plus(xr, yr))
      }
    }
  }

  def conditionallyConvert(r: R) = {
    if (mustConvert(r)) {
      Left(convert(r))
    } else {
      Right(r)
    }
  }

}

class EventuallyMonoid[L,R](convert: R => L)(mustConvert: R => Boolean)
  (implicit lMonoid: Monoid[L], rMonoid: Monoid[R]) extends EventuallySemigroup[L,R](convert)(mustConvert)
  with Monoid[Either[L,R]] {

  override def zero = Right(Monoid.zero[R])

}

class EventuallyGroup[L,R](convert: R => L)(mustConvert: R => Boolean)
  (implicit lGroup: Group[L], rGroup: Group[R]) extends EventuallyMonoid[L,R](convert)(mustConvert)
  with Group[Either[L,R]] {

  override def negate(x: Either[L,R]) = {
    x match {
      case Left(xl) => Left(Group.negate(xl))
      case Right(xr) => conditionallyConvert(Group.negate(xr))
    }
  }

}

class EventuallyRing[L,R](convert: R => L)(mustConvert: R => Boolean)
  (implicit lRing: Ring[L], rRing: Ring[R]) extends EventuallyGroup[L,R](convert)(mustConvert)
  with Ring[Either[L,R]] {

  override def one = conditionallyConvert(Ring.one[R])

  override def times(x: Either[L,R], y: Either[L,R]) = {
    x match {
      case Left(xl) => y match {
        case Left(yl) => Left(Ring.times(xl, yl))
        case Right(yr) => Left(Ring.times(xl, convert(yr)))
      }
      case Right(xr) => y match {
        case Left(yl) => Left(Ring.times(convert(xr), yl))
        case Right(yr) => conditionallyConvert(Ring.times(xr, yr))
      }
    }
  }

}
