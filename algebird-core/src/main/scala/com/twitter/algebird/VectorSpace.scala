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

import java.lang.{Integer => JInt, Short => JShort, Long => JLong, Float => JFloat, Double => JDouble, Boolean => JBool}

import scala.annotation.implicitNotFound
/**
 * This class represents a vector space. For the required properties see:
 *
 * http://en.wikipedia.org/wiki/Vector_space#Definition
 *
 */
object VectorSpace {
  def scale[F, C[_]](v: F, c: C[F])(implicit vs: VectorSpace[F, C]): C[F] = vs.scale(v, c)
  def from[F, C[_]](scaleFn: (F, C[F]) => C[F])(implicit fField: Field[F], cGroup: Group[C[F]]) = new VectorSpace[F, C] {
    def field = fField
    def group = cGroup
    def scale(v: F, c: C[F]) = if(field.isNonZero(v)) scaleFn(v, c) else cGroup.zero
  }

  // Implicits
  implicit def indexedSeqSpace[T : Field] =
    from[T, IndexedSeq]{(s, seq) => seq.map(Ring.times(s, _)) }

  implicit def mapSpace[K, T : Field] =
    from[T, ({type x[a]=Map[K, a]})#x] { (s, m) => m.mapValues(Ring.times(s, _))}

  // TODO: add implicits for java lists, arrays, and options
}

@implicitNotFound(msg = "Cannot find VectorSpace type class for Container: ${C} and Field: ${F}")
trait VectorSpace[F, C[_]] extends java.io.Serializable {
  implicit def field: Field[F]
  implicit def group: Group[C[F]]
  def scale(v: F, c: C[F]): C[F]
}
