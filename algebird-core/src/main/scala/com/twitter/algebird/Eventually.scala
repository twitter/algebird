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
 * In the case of HasAdditionOperator, we specify
 * - Two HasAdditionOperators eventualHasAdditionOperator and originalHasAdditionOperator
 * - A HasAdditionOperator homomorphism convert: O => E
 * - A conditional mustConvert: O => Boolean
 * Then we get a HasAdditionOperator[Either[E,O]], where:
 *   Left(x)  + Left(y)  = Left(x+y)
 *   Left(x)  + Right(y) = Left(x+convert(y))
 *   Right(x) + Left(y)  = Left(convert(x)+y)
 *   Right(x) + Right(y) = Left(convert(x+y)) if mustConvert(x+y)
 *                         Right(x+y) otherwise.
 * EventuallyHasAdditionOperatorAndZero, EventuallyGroup, and EventuallyRing are defined analogously,
 * with the contract that convert respect the appropriate structure.
 *
 * @param E eventual type
 * @param O original type
 */
class EventuallyHasAdditionOperator[E, O](convert: O => E)(mustConvert: O => Boolean)(implicit eventualHasAdditionOperator: HasAdditionOperator[E], originalHasAdditionOperator: HasAdditionOperator[O]) extends HasAdditionOperator[Either[E, O]] {

  import scala.collection.mutable.Buffer

  override def plus(x: Either[E, O], y: Either[E, O]) = {
    x match {
      case Left(xe) => y match {
        case Left(ye) => left(HasAdditionOperator.plus(xe, ye))
        case Right(yo) => left(HasAdditionOperator.plus(xe, convert(yo)))
      }
      case Right(xo) => y match {
        case Left(ye) => left(HasAdditionOperator.plus(convert(xo), ye))
        case Right(yo) => conditionallyConvert(HasAdditionOperator.plus(xo, yo))
      }
    }
  }

  private val maxBuffer = 1000

  /**
   * used to avoid materializing the entire input in memory
   */
  private[this] final def checkSize[T: HasAdditionOperator](buffer: Buffer[T]) {
    if (buffer.size > maxBuffer) {
      val sum = HasAdditionOperator.sumOption(buffer)
      buffer.clear
      sum.foreach(buffer += _)
    }
  }

  override def sumOption(iter: TraversableOnce[Either[E, O]]): Option[Either[E, O]] = {
    iter.foldLeft[Either[Buffer[E], Buffer[O]]] (Right(Buffer[O]())) ((buffer, v) => {

      def addToEventualBuffer(buffer: Buffer[E], value: Either[E, O]): Unit =
        value match {
          case Left(ve) => buffer += ve
          case Right(vo) => buffer += convert(vo)
        }

      def toEventualBuffer(buffer: Buffer[O]): Buffer[E] = {
        val newBuffer = Buffer[E]()
        HasAdditionOperator.sumOption(buffer).foreach((sum) => newBuffer += convert(sum))
        newBuffer
      }

      // turns the list of either into an either of lists
      buffer match {
        case Left(be) => {
          checkSize(be)
          addToEventualBuffer(be, v)
          buffer // left stays left we just add to the buffer and convert if needed
        }
        case Right(bo) => {
          checkSize(bo)
          v match {
            // one Left eventual value => the right list needs to be converted
            case Left(ve) => Left(toEventualBuffer(bo) += ve)
            // otherwise stays Right, just add to the buffer
            case Right(vo) => {
              bo += vo
              buffer
            }
          }
        }
      }
    }) match { // finally apply sumOption accordingly
      case Left(be) => HasAdditionOperator.sumOption(be).map(left(_))
      case Right(bo) => HasAdditionOperator.sumOption(bo).map(conditionallyConvert(_)) // and optionally convert
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
 * @see EventuallyHasAdditionOperator
 */
class EventuallyHasAdditionOperatorAndZero[E, O](convert: O => E)(mustConvert: O => Boolean)(implicit lHasAdditionOperator: HasAdditionOperator[E], rHasAdditionOperatorAndZero: HasAdditionOperatorAndZero[O]) extends EventuallyHasAdditionOperator[E, O](convert)(mustConvert)
  with HasAdditionOperatorAndZero[Either[E, O]] {

  override def zero = Right(HasAdditionOperatorAndZero.zero[O])

}

/**
 * @see EventuallyHasAdditionOperator
 */
class EventuallyGroup[E, O](convert: O => E)(mustConvert: O => Boolean)(implicit lGroup: Group[E], rGroup: Group[O]) extends EventuallyHasAdditionOperatorAndZero[E, O](convert)(mustConvert)
  with Group[Either[E, O]] {

  override def negate(x: Either[E, O]) = {
    x match {
      case Left(xe) => left(Group.negate(xe))
      case Right(xo) => Right(Group.negate(xo))
    }
  }

  override def left(e: E) = if (HasAdditionOperatorAndZero.isNonZero(e)) Left(e) else zero

}

/**
 * @see EventuallyHasAdditionOperator
 */
class EventuallyRing[E, O](convert: O => E)(mustConvert: O => Boolean)(implicit lRing: Ring[E], rRing: Ring[O]) extends EventuallyGroup[E, O](convert)(mustConvert)
  with Ring[Either[E, O]] {

  override def one = Right(Ring.one[O])

  override def times(x: Either[E, O], y: Either[E, O]) = {
    x match {
      case Left(xe) => y match {
        case Left(ye) => left(Ring.times(xe, ye))
        case Right(yo) => left(Ring.times(xe, convert(yo)))
      }
      case Right(xo) => y match {
        case Left(ye) => left(Ring.times(convert(xo), ye))
        case Right(yo) => conditionallyConvert(Ring.times(xo, yo))
      }
    }
  }

}
