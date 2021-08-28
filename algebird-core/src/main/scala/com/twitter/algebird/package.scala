/*
 Copyright 2014 Twitter, Inc.

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

package com.twitter

package object algebird {

  /**
   * TODO remove these in scala 2.11 and use the standard there. these are here to avoid massive bloat around
   * these classes https://github.com/twitter/algebird/issues/263
   */
  private[algebird] abstract class AbstractIterable[T] extends Iterable[T]
  private[algebird] abstract class AbstractIterator[T] extends Iterator[T]

  /**
   * To keep code using algebird.Field compiling, we export algebra Field
   */
  type Field[V] = algebra.ring.Field[V]
  val Field = algebra.ring.Field // scalafix:ok
}
