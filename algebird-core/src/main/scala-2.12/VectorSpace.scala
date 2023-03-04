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

import scala.annotation.implicitNotFound

/**
 * This class represents a vector space. For the required properties see:
 *
 * http://en.wikipedia.org/wiki/Vector_space#Definition
 */
object VectorSpace extends VectorSpaceOps with Implicits

sealed trait VectorSpaceOps {
  def scale[F, C[_]](v: F, c: C[F])(implicit vs: VectorSpace[F, C]): C[F] =
    vs.scale(v, c)
  def from[F, C[_]](scaleFn: (F, C[F]) => C[F])(implicit r: Ring[F], cGroup: Group[C[F]]): VectorSpace[F, C] =
    new VectorSpace[F, C] {
      override def ring: Ring[F] = r
      override def group: Group[C[F]] = cGroup
      override def scale(v: F, c: C[F]): C[F] =
        if (r.isNonZero(v)) scaleFn(v, c) else cGroup.zero
    }
}
private object VectorSpaceOps extends VectorSpaceOps

sealed trait Implicits extends LowPrioImpicits {
  implicit def indexedSeqSpace[T: Ring]: VectorSpace[T, IndexedSeq] =
    VectorSpaceOps.from[T, IndexedSeq]((s, seq) => seq.map(Ring.times(s, _)))
}

sealed trait LowPrioImpicits {
  implicit def mapSpace[K, T: Ring]: VectorSpace[T, Map[K, _]] =
    VectorSpaceOps.from[T, Map[K, _]] { (s, m) =>
      m.transform { case (_, v) => Ring.times(s, v) }
    }
}

@implicitNotFound(msg = "Cannot find VectorSpace type class for Container: ${C} and Ring: ${F}")
trait VectorSpace[F, C[_]] extends java.io.Serializable {
  implicit def ring: Ring[F]
  def field: Ring[F] = ring // this is for compatibility with older versions
  implicit def group: Group[C[F]]
  def scale(v: F, c: C[F]): C[F]
}
