/*
Copyright 2016 GoDaddy, Inc.

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

case class Identity[T](get: T)

object IdentityMonad extends Monad[Identity] {
  override def map[T, U](m: Identity[T])(fn: T => U): Identity[U] =
    Identity(fn(m.get))

  override def apply[T](v: T): Identity[T] =
    Identity(v)

  override def join[T, U](mt: Identity[T], mu: Identity[U]): Identity[(T, U)] =
    Identity((mt.get, mu.get))

  override def flatMap[T, U](m: Identity[T])(fn: T => Identity[U]): Identity[U] =
    fn(m.get)

  override def sequence[T](ms: Seq[Identity[T]]): Identity[Seq[T]] =
    Identity(ms.map(_.get))

  override def joinWith[T, U, V](mt: Identity[T], mu: Identity[U])(fn: (T, U) => V): Identity[V] =
    Identity(fn(mt.get, mu.get))

  override def join[T1, T2, T3](
      m1: Identity[T1],
      m2: Identity[T2],
      m3: Identity[T3]
  ): Identity[(T1, T2, T3)] =
    Identity((m1.get, m2.get, m3.get))

  override def join[T1, T2, T3, T4](
      m1: Identity[T1],
      m2: Identity[T2],
      m3: Identity[T3],
      m4: Identity[T4]
  ): Identity[(T1, T2, T3, T4)] =
    Identity((m1.get, m2.get, m3.get, m4.get))

  override def join[T1, T2, T3, T4, T5](
      m1: Identity[T1],
      m2: Identity[T2],
      m3: Identity[T3],
      m4: Identity[T4],
      m5: Identity[T5]
  ): Identity[(T1, T2, T3, T4, T5)] =
    Identity((m1.get, m2.get, m3.get, m4.get, m5.get))
}

object Identity {
  implicit def identityFunctor: Functor[Identity] =
    IdentityMonad

  implicit def identityApplicative: Applicative[Identity] =
    IdentityMonad

  implicit def identityMonad: Monad[Identity] =
    IdentityMonad
}
