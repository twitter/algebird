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
import scala.util.{Failure, Success, Try}
import scala.concurrent.{ExecutionContext, Future}
import collection.GenTraversable

/**
 * Simple implementation of a Monad type-class.
 * Subclasses only need to override apply and flatMap, but they should override map,
 * join, joinWith, and sequence if there are better implementations.
 *
 * Laws Monads must follow:
 * identities:
 *  flatMap(apply(x))(fn) == fn(x)
 *  flatMap(m)(apply _) == m
 * associativity on flatMap (you can either flatMap f first, or f to g:
 *  flatMap(flatMap(m)(f))(g) == flatMap(m) { x => flatMap(f(x))(g) }
 */
@implicitNotFound(msg = "Cannot find Monad type class for ${M}")
trait Monad[M[_]] extends Applicative[M] {
  def flatMap[T, U](m: M[T])(fn: (T) => M[U]): M[U]
  override def map[T, U](m: M[T])(fn: (T) => U): M[U] =
    flatMap(m)((t: T) => apply(fn(t)))
  override def join[T, U](mt: M[T], mu: M[U]): M[(T, U)] =
    flatMap(mt)((t: T) => map(mu)((u: U) => (t, u)))
}

/**
 * For use from Java/minimizing code bloat in scala
 */
abstract class AbstractMonad[M[_]] extends Monad[M]

/**
 * Follows the type-class pattern for the Monad trait
 */
object Monad {

  /** Get the Monad for a type, e.g: Monad[List] */
  def apply[M[_]](implicit monad: Monad[M]): Monad[M] = monad
  def flatMap[M[_], T, U](m: M[T])(fn: (T) => M[U])(implicit monad: Monad[M]): M[U] =
    monad.flatMap(m)(fn)
  def map[M[_], T, U](m: M[T])(fn: (T) => U)(implicit monad: Monad[M]): M[U] =
    monad.map(m)(fn)
  def foldM[M[_], T, U](acc: T, xs: GenTraversable[U])(fn: (T, U) => M[T])(implicit monad: Monad[M]): M[T] =
    if (xs.isEmpty)
      monad.apply(acc)
    else
      monad.flatMap(fn(acc, xs.head)) { t: T => foldM(t, xs.tail)(fn) }

  // Some instances of the Monad typeclass (case for a macro):
  implicit val list: Monad[List] = new Monad[List] {
    override def apply[T](v: T) = List(v);
    override def flatMap[T, U](m: List[T])(fn: (T) => List[U]): List[U] = m.flatMap(fn)
  }
  implicit val option: Monad[Option] = new Monad[Option] {
    override def apply[T](v: T): Option[T] = Option(v);
    override def flatMap[T, U](m: Option[T])(fn: (T) => Option[U]): Option[U] = m.flatMap(fn)
  }
  implicit val some: Monad[Some] = new Monad[Some] {
    override def apply[T](v: T): Some[T] = Some(v);
    override def flatMap[T, U](m: Some[T])(fn: (T) => Some[U]): Some[U] = fn(m.get)
  }
  implicit val vector: Monad[Vector] = new Monad[Vector] {
    override def apply[T](v: T) = Vector(v);
    override def flatMap[T, U](m: Vector[T])(fn: (T) => Vector[U]): Vector[U] = m.flatMap(fn)
  }
  implicit val set: Monad[Set] = new Monad[Set] {
    override def apply[T](v: T) = Set(v);
    override def flatMap[T, U](m: Set[T])(fn: (T) => Set[U]): Set[U] = m.flatMap(fn)
  }
  implicit val seq: Monad[Seq] = new Monad[Seq] {
    override def apply[T](v: T) = Seq(v);
    override def flatMap[T, U](m: Seq[T])(fn: (T) => Seq[U]): Seq[U] = m.flatMap(fn)
  }
  implicit val indexedseq: Monad[IndexedSeq] = new Monad[IndexedSeq] {
    override def apply[T](v: T) = IndexedSeq(v);
    override def flatMap[T, U](m: IndexedSeq[T])(fn: (T) => IndexedSeq[U]): IndexedSeq[U] =
      m.flatMap(fn)
  }
  implicit val scalaTry: Monad[Try] = new Monad[Try] {
    override def apply[T](t: T): Try[T] = Success(t)
    override def flatMap[T, U](t: Try[T])(fn: T => Try[U]): Try[U] = t.flatMap(fn)
    override def map[T, U](t: Try[T])(fn: T => U): Try[U] = t.map(fn)
    override def join[T, U](t: Try[T], u: Try[U]): Try[(T, U)] =
      // make the common case fast:
      if (t.isSuccess && u.isSuccess) Success((t.get, u.get))
      else
        (t, u) match {
          case (Failure(e), _) => Failure(e)
          case (_, Failure(e)) => Failure(e)
          // One must be a failure due to being in the else branch
          case (_, _) =>
            sys.error("unreachable, but the compiler can't see that")
        }
  }
  implicit def scalaFuture(implicit ec: ExecutionContext): Monad[Future] =
    new Monad[Future] {
      override def apply[T](t: T): Future[T] = Future.successful(t)
      override def flatMap[T, U](t: Future[T])(fn: T => Future[U]): Future[U] =
        t.flatMap(fn)
      override def map[T, U](t: Future[T])(fn: T => U): Future[U] = t.map(fn)
      override def join[T, U](t: Future[T], u: Future[U]): Future[(T, U)] =
        t.zip(u)
      override def sequence[T](fs: Seq[Future[T]]): Future[Seq[T]] =
        Future.sequence(fs)
    }

  // Set up the syntax magic (allow .pure[Int] syntax and flatMap in for):
  // import Monad.{pureOp, operators} to get
  implicit def pureOp[A](a: A): PureOp[A] = new PureOp(a)
  implicit def operators[A, M[_]](m: M[A])(implicit monad: Monad[M]): MonadOperators[A, M] =
    new MonadOperators(m)(monad)
}

/**
 * This enrichment allows us to use our Monad instances in for expressions:
 * if (import Monad._) has been done
 */
class MonadOperators[A, M[_]](m: M[A])(implicit monad: Monad[M]) extends ApplicativeOperators[A, M](m) {
  def flatMap[U](fn: (A) => M[U]): M[U] = monad.flatMap(m)(fn)
}
