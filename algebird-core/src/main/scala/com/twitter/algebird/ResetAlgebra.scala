/*
Copyright 2012 Twitter, Inc.

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
 * Used to represent cases where we need to periodically reset
 *  a +  b =   a + b
 * |a +  b = |(a + b)
 *  a + |b = |b
 * |a + |b = |b
 */
sealed trait ResetState[+A] { def get: A }
case class SetValue[+A](get: A) extends ResetState[A]
case class ResetValue[+A](get: A) extends ResetState[A]

object ResetState {
  implicit def monoid[A: Monoid]: Monoid[ResetState[A]] =
    new ResetStateMonoid[A]
}

class ResetStateMonoid[A](implicit monoid: Monoid[A]) extends Monoid[ResetState[A]] {
  def zero = SetValue(monoid.zero)
  def plus(left: ResetState[A], right: ResetState[A]) =
    (left, right) match {
      case (SetValue(l), SetValue(r))   => SetValue(monoid.plus(l, r))
      case (ResetValue(l), SetValue(r)) => ResetValue(monoid.plus(l, r))
      case (_, ResetValue(_))           => right
    }
}
