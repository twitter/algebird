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

// To use the MaxSemigroup wrap your item in a Max object
case class Max[+T](get: T)

class MaxSemigroup[T](implicit ord: Ordering[T]) extends Semigroup[Max[T]] {
  def plus(l: Max[T], r: Max[T]) =
    if(ord.gteq(l.get, r.get)) l else r
}

// To use the MinSemigroup wrap your item in a Min object
case class Min[+T](get: T)

class MinSemigroup[T](implicit ord: Ordering[T]) extends Semigroup[Min[T]] {
  def plus(l: Min[T], r: Min[T]) =
    if(ord.lteq(l.get, r.get)) l else r
}

object First {
  implicit def semigroup[T]: Semigroup[First[T]] = new FirstSemigroup[T]
}
case class First[+T](get: T)
class FirstSemigroup[T] extends Semigroup[First[T]] {
  def plus(l: First[T], r: First[T]) = l
}

object Last {
  implicit def semigroup[T]: Semigroup[Last[T]] = new LastSemigroup[T]
}
case class Last[+T](get: T)
class LastSemigroup[T] extends Semigroup[Last[T]] {
  def plus(l: Last[T], r: Last[T]) = r
}

