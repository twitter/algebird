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

package com.twitter.algebird

class FunctorProperties extends CheckProperties {
  import com.twitter.algebird.FunctorLaws._
  import com.twitter.algebird.Monad._ // for Functor instances
  import com.twitter.algebird.MonadLaws._ // for Arbitrary instances

  property("list") {
    functorLaws[List, Int, String, Long]()
  }

  property("option") {
    functorLaws[Option, Int, String, Long]()
  }

  property("indexedseq") {
    functorLaws[IndexedSeq, Int, String, Long]()
  }

  property("vector") {
    functorLaws[Vector, Int, String, Long]()
  }

  property("set") {
    functorLaws[Set, Int, String, Long]()
  }

  property("seq") {
    functorLaws[Seq, Int, String, Long]()
  }

  property("identity") {
    functorLaws[Identity, Int, String, Long]()
  }
}
