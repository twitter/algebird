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

package com.twitter.algebird.matchers

object seq {
  import org.scalatest.matchers.{ Matcher, MatchResult }

  class EqSeqMatcher[E](ref: TraversableOnce[E]) extends Matcher[TraversableOnce[E]] {
    private def elist(s: Seq[E]) = "(" + s.mkString(", ") + ")"
    def apply(left: TraversableOnce[E]) = {
      val (ls, rs) = (left.toSeq, ref.toSeq)
      MatchResult(
        ls.equals(rs),
        s"Seq${elist(ls)} did not match Seq${elist(rs)}",
        "sequences matched")
    }
  }

  def beEqSeq[E](ref: TraversableOnce[E]) = new EqSeqMatcher(ref)

  class EqNumSeqMatcher(ref: TraversableOnce[Double], eps: Double) extends Matcher[TraversableOnce[Double]] {
    private def elist(s: Seq[Double]) = "(" + s.mkString(", ") + ")"
    def apply(left: TraversableOnce[Double]) = {
      val (ls, rs) = (left.toSeq, ref.toSeq)
      MatchResult(
        ls.zip(rs).map(p => math.abs(p._1 - p._2)).max < eps,
        s"Seq${elist(ls)} did not match Seq${elist(rs)}",
        "sequences matched")
    }
  }

  def beEqNumSeq[N](ref: TraversableOnce[N], eps: Double)(implicit td: N => Double) =
    new EqNumSeqMatcher(ref.map(td), eps)
}
