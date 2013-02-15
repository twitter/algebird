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
case class Max[@specialized(Int,Long,Float,Double) +T](get: T)

object Max {
  implicit def semigroup[T](implicit ord:Ordering[T]) =
    Semigroup.from[Max[T]] { (l,r) => if(ord.gteq(l.get, r.get)) l else r }
}

// To use the MinSemigroup wrap your item in a Min object
case class Min[@specialized(Int,Long,Float,Double) +T](get: T)

object Min {
  implicit def semigroup[T](implicit ord:Ordering[T]) =
    Semigroup.from[Min[T]] { (l,r) => if(ord.lteq(l.get, r.get)) l else r }
}

// Not ordered by type, but ordered by order in which we see them:

case class First[@specialized(Int,Long,Float,Double) +T](get: T)
object First {
  implicit def semigroup[T] = Semigroup.from[First[T]] { (l,r) => l }
}

case class Last[@specialized(Int,Long,Float,Double) +T](get: T)
object Last {
  implicit def semigroup[T] = Semigroup.from[Last[T]] { (l,r) => r }
}
