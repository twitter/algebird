package com.twitter.algebird

// Classes that support algebraic structures with dynamic switching between
// multiple representations.  In the case of Semigroup, we specify
// - Two Semigroups L and R
// - A Semigroup homomorphism convert: L => R
// - A conditional mustConvert: L => Boolean
// Then we get a Semigroup[Either[L,R]], where:
//   Right(x) + Right(y) = Right(x+y)
//   Left(x)  + Right(y) = Right(convert(x)+y)
//   Right(x) + Left(y)  = Right(x+convert(y))
//   Left(x)  + Left(y)  = Right(convert(x+y)) if mustConvert(x+y)
//                         Left(x+y) otherwise.
// EventuallyMonoid, EventuallyGroup, and EventuallyRing are defined analogously,
// with the contract that convert respect the appropriate structure.

class EventuallySemigroup[L,R](convert: L => R)(mustConvert: L => Boolean)
  (implicit lSemigroup: Semigroup[L], rSemigroup: Semigroup[R]) extends Semigroup[Either[L,R]] {

  override def plus(x: Either[L,R], y: Either[L,R]) = {
    x match {
      case Left(xl) => y match {
        case Left(yl) => conditionallyConvert(Semigroup.plus(xl, yl))
        case Right(yr) => Right(Semigroup.plus(convert(xl), yr))
      }
      case Right(xr) => y match {
        case Left(yl) => Right(Semigroup.plus(xr, convert(yl)))
        case Right(yr) => Right(Semigroup.plus(xr, yr))
      }
    }
  }

  def conditionallyConvert(l: L) = {
    if (mustConvert(l)) {
      Right(convert(l))
    } else {
      Left(l)
    }
  }

}

class EventuallyMonoid[L,R](convert: L => R)(mustConvert: L => Boolean)
  (implicit lMonoid: Monoid[L], rMonoid: Monoid[R]) extends EventuallySemigroup[L,R](convert)(mustConvert)
  with Monoid[Either[L,R]] {

  override def zero = Left(Monoid.zero[L])

}

class EventuallyGroup[L,R](convert: L => R)(mustConvert: L => Boolean)
  (implicit lGroup: Group[L], rGroup: Group[R]) extends EventuallyMonoid[L,R](convert)(mustConvert)
  with Group[Either[L,R]] {

  override def negate(x: Either[L,R]) = {
    x match {
      case Left(xl) => conditionallyConvert(Group.negate(xl))
      case Right(xr) => Right(Group.negate(xr))
    }
  }

}

class EventuallyRing[L,R](convert: L => R)(mustConvert: L => Boolean)
  (implicit lRing: Ring[L], rRing: Ring[R]) extends EventuallyGroup[L,R](convert)(mustConvert)
  with Ring[Either[L,R]] {

  override def one = conditionallyConvert(Ring.one[L])

  override def times(x: Either[L,R], y: Either[L,R]) = {
    x match {
      case Left(xl) => y match {
        case Left(yl) => conditionallyConvert(Ring.times(xl, yl))
        case Right(yr) => Right(Ring.times(convert(xl), yr))
      }
      case Right(xr) => y match {
        case Left(yl) => Right(Ring.times(xr, convert(yl)))
        case Right(yr) => Right(Ring.times(xr, yr))
      }
    }
  }

}
