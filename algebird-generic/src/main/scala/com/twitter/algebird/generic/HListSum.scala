package com.twitter.algebird.generic

import shapeless._
import shapeless.ops.hlist.ToList
import com.twitter.algebird._

object HListSum {
  def sum[L <: HList, T](hlist: L)(implicit toList: ToList[L, T], m: Monoid[T]): T =
    m.sum(toList(hlist))

  def sum1[L <: HList, T](cons: T :: L)(implicit toList: ToList[L, T], s: Semigroup[T]): T =
    // this get can never fail because there is at least one item
    s.sumOption(cons.head :: toList(cons.tail)).get
}
