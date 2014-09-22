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
 * Represents functions of the kind:
 * f(x) = slope * x + intercept
 */
case class AffineFunction[R](slope: R, intercept: R) extends java.io.Serializable {
  def toFn(implicit ring: Ring[R]): Function1[R, R] = { x => this.apply(x)(ring) }
  def apply(x: R)(implicit ring: Ring[R]) = ring.plus(ring.times(slope, x), intercept)
}

/**
 * This feeds the value in on the LEFT!!!  This may seem counter intuitive, but
 * with this approach, a stream/iterator which is summed will have the same output
 * as applying the function one at a time in order to the input.
 * If we did the "lexigraphically correct" thing, which might be (f+g)(x) = f(g(x))
 * then we would wind up reversing the list in the sum.
 * (f1 + f2)(x) = f2(f1(x)) so that:
 * listOfFn.foldLeft(x) { (v, fn) => fn(v) } = (Monoid.sum(listOfFn))(x)
 */
class AffineFunctionMonoid[R](implicit ring: Ring[R]) extends Monoid[AffineFunction[R]] {
  lazy val zero = AffineFunction[R](ring.one, ring.zero)
  def plus(f: AffineFunction[R], g: AffineFunction[R]) = {
    // (f+g)(x) = g(f(x))
    // g(f(x)) = g(a*x+b) = c*(a*x+b) + d = (c*a)*x + (c*b + d)
    val newSlope = ring.times(g.slope, f.slope)
    val newIntercept = ring.plus(ring.times(g.slope, f.intercept), g.intercept)
    AffineFunction(newSlope, newIntercept)
  }
}

object AffineFunction extends java.io.Serializable {
  implicit def monoid[R: Ring]: Monoid[AffineFunction[R]] = new AffineFunctionMonoid[R]
}
