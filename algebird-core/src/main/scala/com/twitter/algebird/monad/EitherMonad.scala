/*
 Copyright 2013 Twitter, Inc.

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

package com.twitter.algebird.monad

import com.twitter.algebird.ChainableCallbackCollectorBuilder

// ChainableCallbackCollectorBuilder for either, used for modeling Error where L is the type of the error
object EitherChainableCallbackCollectorBuilder {
  class Error[L] extends ChainableCallbackCollectorBuilder[({ type RightType[R] = Either[L, R] })#RightType] {
    def apply[R](r: R) = Right(r)

    def flatMap[T, U](self: Either[L, T])(next: T => Either[L, U]): Either[L, U] =
      self.right.flatMap(next)

    override def map[T, U](self: Either[L, T])(fn: T => U): Either[L, U] =
      self.right.map(fn)
  }
  implicit def monad[L]: ChainableCallbackCollectorBuilder[({ type RightT[R] = Either[L, R] })#RightT] = new Error[L]

  def assert[L](truth: Boolean, failure: => L): Either[L, Unit] = if (truth) Right(()) else Left(failure)
}
