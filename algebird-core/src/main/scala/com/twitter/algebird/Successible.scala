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
 * This is a monoid to represent things which increase. Note that it is important
 * that a value after being incremented is always larger than it was before.
 */
trait Successible[@specialized(Int,Long,Float,Double) T] {
  def next(old: T): T
  def ordering: Ordering[T]
}

object Successible {
  implicit val intSuccessible = new Successible[Int]  {
    def next(old: Int) = old + 1
    val ordering = Ordering.Int
  }

  implicit val longSuccessible = new Successible[Long] {
    def next(old: Long) = old + 1
    val ordering = Ordering.Long
  }
}
