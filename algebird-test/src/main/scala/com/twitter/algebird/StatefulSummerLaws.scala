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

object StatefulSummerLaws {
  import BaseProperties.isNonZero

  // Law1:
  def zeroEquiv[V:Semigroup:Equiv](v0: Option[V], v1: Option[V]): Boolean = {
    val v0NonZero = v0.exists(isNonZero(_))
    val v1NonZero = v1.exists(isNonZero(_))
    if (!(v0NonZero||v1NonZero)) {
      // neither are non-zero
      true
    }
    else {
      (for(a <- v0; b <- v1; eq = Equiv[V].equiv(a,b)) yield eq).getOrElse(false)
    }
  }
  def sumIsPreserved[V:Semigroup:Equiv](summer: StatefulSummer[V], items: Iterable[V]): Boolean = {
    summer.flush
    val sg = Semigroup.sumOption(items)
    val wsummer = Monoid.plus(Monoid.sum(items.map { summer.put(_) }.filter { _.isDefined }), summer.flush)
    zeroEquiv(sg, wsummer) && summer.isFlushed
  }
  // Law 2:
  def isFlushedIsConsistent[V](summer: StatefulSummer[V], items: Iterable[V]): Boolean = {
    items.forall { v =>
      summer.put(v)
      (summer.isFlushed == summer.flush.isEmpty) &&
        // Now flush should empty
        summer.isFlushed &&
        summer.flush.isEmpty
    }
  }
}
