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

import com.twitter.algebird.CheckProperties
import org.scalacheck.Prop._

class MonadInstanceLaws extends CheckProperties {

  // Mutually recursive functions
  def ping(todo: Int, acc: Int): Trampoline[Int] =
    if (todo <= 0) Trampoline(acc) else Trampoline.call(pong(todo - 1, acc + 1))

  def pong(todo: Int, acc: Int): Trampoline[Int] =
    if (todo <= 0) Trampoline(acc) else Trampoline.call(ping(todo - 1, acc + 1))

  property("Trampoline should run without stackoverflow") {
    forAll { (b: Int) =>
      val bsmall = b % 1000000
      ping(bsmall, 0).get == (bsmall.max(0))
    }
  }

  property("get/swap") {
    forAll { (i: Int) =>
      val fn = for {
        start <- StateWithError.getState[Int]
        oldState <- StateWithError.swapState[Int](start * 2)
      } yield oldState

      fn(i) == Right((2 * i, i))
    }
  }

  property("State behaves correctly") {
    forAll { (in: Int, head: Long, fns: List[(Int) => Either[String, (Int, Long)]]) =>
      val mons = fns.map { StateWithError(_) }
      val init =
        StateWithError.const[Int, Long](head): StateWithError[Int, String, Long]
      val comp = mons.foldLeft(init) { (old, fn) =>
        old.flatMap { x =>
          fn
        } // just bind
      }
      comp(in) == (fns
        .foldLeft(Right((in, head)): Either[String, (Int, Long)]) { (oldState, fn) =>
          oldState.right.flatMap { case (s, v) => fn(s) }
        })
    }
  }

  class MutableBox(var item: Int) {
    def inc(v: Int) = item += v
  }

  property("Reader behaves correctly") {
    forAll { (initialEnv: Int, fns: List[(Int) => Int]) =>
      // Set up the mutable variable to feed to the readers:
      val m1 = new MutableBox(initialEnv)
      val m2 = new MutableBox(initialEnv)

      val readers = fns.map { fn =>
        Reader { (m: MutableBox) =>
          val toInc = fn(m.item)
          m.inc(toInc)
        }
      }
      // Now apply them all:
      val bigReader =
        readers.foldLeft(Reader.const(()): Reader[MutableBox, Unit]) { (oldr, thisR) =>
          oldr.flatMap { x =>
            thisR
          } // just sequence them
        }
      // apply:
      bigReader(m1)

      // This should be the same as this loop:
      fns.foreach { fn =>
        m2.inc(fn(m2.item))
      }
      m1.item == m2.item
    }
  }

}
