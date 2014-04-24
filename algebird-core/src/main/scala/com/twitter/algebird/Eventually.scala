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

/**
 * Classes that support algebraic structures with dynamic switching between
 * two representations, the original type O and the eventual type E.
 * In the case of Semigroup, we specify
 * - Two Semigroups eventualSemigroup and originalSemigroup
 * - A Semigroup homomorphism convert: O => E
 * - A conditional mustConvert: O => Boolean
 * Then we get a Semigroup[Either[E,O]], where:
 *   Left(x)  + Left(y)  = Left(x+y)
 *   Left(x)  + Right(y) = Left(x+convert(y))
 *   Right(x) + Left(y)  = Left(convert(x)+y)
 *   Right(x) + Right(y) = Left(convert(x+y)) if mustConvert(x+y)
 *                         Right(x+y) otherwise.
 * EventuallyMonoid, EventuallyGroup, and EventuallyRing are defined analogously,
 * with the contract that convert respect the appropriate structure.
 *
 * @param E eventual type
 * @param O original type
 */
class EventuallySemigroup[E, O](convert: O => E)(mustConvert: O => Boolean)
  (implicit eventualSemigroup: Semigroup[E], originalSemigroup: Semigroup[O]) extends Semigroup[Either[E, O]] {

  override def plus(x: Either[E, O], y: Either[E, O]) = {
    (x, y) match {
      case (Left(xe), Left(ye)) => left(Semigroup.plus(xe, ye))
      case (Left(xe), Right(yo)) => left(Semigroup.plus(xe, convert(yo)))
      case (Right(xo), Left(ye)) => left(Semigroup.plus(convert(xo), ye))
      case (Right(xo), Right(yo)) => conditionallyConvert(Semigroup.plus(xo, yo))
    }
  }

  /** stops on the first Left */
  private class RightIterator(val i: BufferedIterator[Either[E, O]]) extends Iterator[O] {

    override def hasNext = i.hasNext && i.head.isRight

    override def next = i.next match {
      case Right(v) => v
      case v@_ => throw new IllegalStateException("Called next when hasNext was false: " + v)
    }
  }

  /** converts Rights to Lefts */
  private class LeftIterator(val i: Iterator[Either[E, O]]) extends Iterator[E] {

    override def hasNext = i.hasNext

    override def next = i.next match {
      case Right(v) => convert(v)
      case Left(v) => v
    }
  }

  override def sumOption(iter: TraversableOnce[Either[E, O]]): Option[Either[E, O]] = {
    val i = iter.toIterator.buffered
    val ri = new RightIterator(i)
    val rs: Option[Either[E, O]] = if (ri.hasNext) {
      var result = ri.next
      var convertResult = false
      // If we encounter a Left(E) or mustConvert returns true on a plus result we must move to sumOption on E
      while (ri.hasNext && !convertResult) {
        result = Semigroup.plus(result, ri.next)
        convertResult = mustConvert(result)
      }
      Some(if (convertResult) left(convert(result)) else Right(result))
    } else None

    // run sumOption of E on the remainder
    val ls = Semigroup.sumOption(new LeftIterator(i))

    // add up the two computed sums
    (ls, rs) match {
      case (None, _) => rs
      case (Some(l), None) => Some(left(l))
      case (Some(l), Some(Right(r))) => Some(left(Semigroup.plus(convert(r), l)))
      case (Some(l), Some(Left(r))) => Some(left(Semigroup.plus(r, l)))
    }
  }

  protected def conditionallyConvert(o: O): Either[E, O] = {
    if (mustConvert(o)) {
      left(convert(o))
    } else {
      Right(o)
    }
  }

  // Overriden by EventuallyGroup to ensure that the group laws are obeyed.
  protected def left(e: E): Either[E, O] = Left(e)

}

/**
 * @see EventuallySemigroup
 */
class EventuallyMonoid[E, O](convert: O => E)(mustConvert: O => Boolean)
  (implicit lSemigroup: Semigroup[E], rMonoid: Monoid[O]) extends EventuallySemigroup[E, O](convert)(mustConvert)
  with Monoid[Either[E, O]] {

  override def zero = Right(Monoid.zero[O])

}

/**
 * @see EventuallySemigroup
 */
class EventuallyGroup[E, O](convert: O => E)(mustConvert: O => Boolean)
  (implicit lGroup: Group[E], rGroup: Group[O]) extends EventuallyMonoid[E, O](convert)(mustConvert)
  with Group[Either[E, O]] {

  override def negate(x: Either[E, O]) = {
    x match {
      case Left(xe) => left(Group.negate(xe))
      case Right(xo) => Right(Group.negate(xo))
    }
  }

  override def left(e: E) = if (Monoid.isNonZero(e)) Left(e) else zero

}

/**
 * @see EventuallySemigroup
 */
class EventuallyRing[E, O](convert: O => E)(mustConvert: O => Boolean)
  (implicit lRing: Ring[E], rRing: Ring[O]) extends EventuallyGroup[E, O](convert)(mustConvert)
  with Ring[Either[E, O]] {

  override def one = Right(Ring.one[O])

  override def times(x: Either[E, O], y: Either[E, O]) = {
    (x, y) match {
      case (Left(xe), Left(ye)) => left(Ring.times(xe, ye))
      case (Left(xe), Right(yo)) => left(Ring.times(xe, convert(yo)))
      case (Right(xo), Left(ye)) => left(Ring.times(convert(xo), ye))
      case (Right(xo), Right(yo)) => conditionallyConvert(Ring.times(xo, yo))
    }
  }

}
