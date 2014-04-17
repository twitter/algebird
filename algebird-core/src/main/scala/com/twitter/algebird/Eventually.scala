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
        case Left(yl) => left(Semigroup.plus(xl, yl))
        case Right(yr) => left(Semigroup.plus(xl, convert(yr)))
      }
      case Right(xr) => y match {
        case Left(yl) => left(Semigroup.plus(convert(xr), yl))
        case Right(yr) => conditionallyConvert(Semigroup.plus(xr, yr))
      }
    }
  }

  override def sumOption(iter: TraversableOnce[Either[L,R]]): Option[Either[L,R]] = {
    import scala.collection.mutable.ArrayBuffer
    iter.foldLeft[Either[ArrayBuffer[L], ArrayBuffer[R]]](Right(new ArrayBuffer[R]))((acc, v) => {
      // turns the list of either into an either of lists
      acc match {
        case Left(al) => v match {
          case Left(vl) => al += vl
          case Right(vr) => al += convert(vr)
        }; acc // left stays left we just add to the buffer and convert if needed
        case Right(ar) => v match {
          case Left(vl) => Left(ar.map(convert(_)) += vl) // one left value => the right list needs to be converted
          case Right(vr) => ar += vr; acc // otherwise stays right, just add to the buffer
        }
      }
    }) match { // finally apply sumOption accordingly
       case Left(bl) => lSemigroup.sumOption(bl).map(left(_))
       case Right(br) => rSemigroup.sumOption(br).map(conditionallyConvert(_)) // and optionally convert
    }
  }

  def conditionallyConvert(r: R) = {
    if (mustConvert(r)) {
      left(convert(r))
    } else {
      Right(r)
    }
  }

  // Overriden by EventuallyGroup to ensure that the group laws are obeyed.
  def left(l: L): Either[L,R] = Left(l)

}

class EventuallyMonoid[L,R](convert: R => L)(mustConvert: R => Boolean)
  (implicit lSemigroup: Semigroup[L], rMonoid: Monoid[R]) extends EventuallySemigroup[L,R](convert)(mustConvert)
  with Monoid[Either[L,R]] {

  override def zero = Right(Monoid.zero[R])

}

class EventuallyGroup[L,R](convert: R => L)(mustConvert: R => Boolean)
  (implicit lGroup: Group[L], rGroup: Group[R]) extends EventuallyMonoid[L,R](convert)(mustConvert)
  with Group[Either[L,R]] {

  override def negate(x: Either[L,R]) = {
    x match {
      case Left(xl) => left(Group.negate(xl))
      case Right(xr) => Right(Group.negate(xr))
    }
  }

  override def left(l: L) = if (Monoid.isNonZero(l)) Left(l) else zero

}

class EventuallyRing[L,R](convert: R => L)(mustConvert: R => Boolean)
  (implicit lRing: Ring[L], rRing: Ring[R]) extends EventuallyGroup[L,R](convert)(mustConvert)
  with Ring[Either[L,R]] {

  override def one = Right(Ring.one[R])

  override def times(x: Either[L,R], y: Either[L,R]) = {
    x match {
      case Left(xl) => y match {
        case Left(yl) => left(Ring.times(xl, yl))
        case Right(yr) => left(Ring.times(xl, convert(yr)))
      }
      case Right(xr) => y match {
        case Left(yl) => left(Ring.times(convert(xr), yl))
        case Right(yr) => conditionallyConvert(Ring.times(xr, yr))
      }
    }
  }

}
