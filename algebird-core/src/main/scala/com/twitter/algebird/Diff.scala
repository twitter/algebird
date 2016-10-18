/*
Copyright 2016 Twitter, Inc.

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
 * `Diff` is a class that represents changes applied to a set. It is
 * in fact a Set[T] => Set[T], but doesn't extend Function1 since
 * that brings in a pack of methods that we don't necessarily want.
 */
sealed abstract case class Diff[T] private (add: Set[T], remove: Set[T]) { self =>
  def +(t: T): Diff[T] = Diff(add + t, remove - t)
  def -(t: T): Diff[T] = Diff(add - t, remove + t)
  def ++(ts: Iterable[T]): Diff[T] = Diff(add ++ ts, remove -- ts)
  def --(ts: Iterable[T]): Diff[T] = Diff(add -- ts, remove ++ ts)
  def merge(other: Diff[T]): Diff[T] = {
    val newAdd = (add ++ other.add) -- other.remove
    val newRem = (remove -- other.add) ++ other.remove
    Diff(newAdd, newRem)
  }

  /**
   * Applies the contained diff to the supplied set.
   */
  def apply(previous: Set[T]): Set[T] = previous ++ add -- remove

  /**
   * Returns a diff that, if applied to a set, undoes the effects of
   * this diff.
   */
  def invert: Diff[T] = Diff(remove, add)

  /**
   * Same as apply, but fails to None if the diff's removal set has
   * any items that aren't present in `previous`.
   *
   * Returns Some(_) if and only if invert will undo.
   */
  def strictApply(previous: Set[T]): Option[Set[T]] =
    if (remove.subsetOf(previous) && Diff.areDisjoint(add, previous)) {
      Some(apply(previous))
    } else None
}

object Diff {
  /**
   * Keeping this constructor private prevents creation of ad-hoc,
   * invalid `Diff` instances. `Diff`s must be created by
   * construction with the supplied helper methods below.
   */
  private[Diff] def apply[T](add: Set[T], remove: Set[T]): Diff[T] = new Diff[T](add, remove) {}

  implicit def monoid[T]: Monoid[Diff[T]] = Monoid.from(Diff.empty[T])(_ merge _)

  private def areDisjoint[T](a: Set[T], b: Set[T]): Boolean =
    if (a.size > b.size) areDisjoint(b, a)
    else !a.exists(b)

  def add[T](t: T): Diff[T] = Diff(Set(t), Set.empty)
  def remove[T](t: T): Diff[T] = Diff(Set.empty, Set(t))
  def addAll[T](ts: Set[T]): Diff[T] = Diff(ts, Set.empty)
  def removeAll[T](ts: Set[T]): Diff[T] = Diff(Set.empty, ts)
  def empty[T]: Diff[T] = Diff(Set.empty, Set.empty)

  /**
   * Tracks the changes between the old and new set in a Diff[T]
   * instance. The law that diffs preserve is:
   *
   * {{{
   * val diff = Diff.of(a, b)
   * b == diff(a)
   * }}}
   */
  def of[T](oldSet: Set[T], newSet: Set[T]): Diff[T] =
    Diff(newSet &~ oldSet, oldSet &~ newSet)
}
