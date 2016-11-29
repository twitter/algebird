/*
 Copyright 2014 Twitter, Inc.

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
 * This is an operation that when it is completely defined,
 * it is associative.
 */
trait PartialSemigroup[T] extends java.io.Serializable {
  /**
   * This is a non-total semigroup.
   * If all (a + b) + c, and a + (b + c)
   * are defined, then they are equal.
   * If any are undefined, all bets are
   * off.
   */
  def tryPlus(left: T, right: T): Option[T]
  /**
   * If we can plus, do so, else return left
   * Useful for keeping values and doing
   * partial merges (this a key-value store
   * where you update if possible).
   * Note that this is not generally associative.
   */
  def plusOrLeft(left: T, right: T): T =
    tryPlus(left, right).getOrElse(left)
}

object PartialSemigroup {
  implicit def fromSemigroup[T: Semigroup]: PartialSemigroup[T] = new PartialSemigroup[T] {
    def tryPlus(left: T, right: T) = Some(Semigroup.plus(left, right))
  }
  def tryPlus[T](left: T, right: T)(implicit psg: PartialSemigroup[T]): Option[T] =
    psg.tryPlus(left, right)

  def plusOrLeft[T](left: T, right: T)(implicit psg: PartialSemigroup[T]): T =
    psg.plusOrLeft(left, right)
}
