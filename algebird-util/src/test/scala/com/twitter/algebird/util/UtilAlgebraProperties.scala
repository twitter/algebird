/*
 * Copyright 2013 Twitter Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.algebird.util

import com.twitter.algebird.CheckProperties
import com.twitter.algebird.ChainableCallbackCollectorBuilderLaws.monadLaws
import com.twitter.util.{ Await, Future, Return, Try }
import org.scalacheck.Arbitrary

class UtilAlgebraProperties extends CheckProperties {
  import com.twitter.algebird.util.UtilAlgebras._

  def toOption[T](f: Future[T]): Option[T] =
    try {
      Some(Await.result(f))
    } catch {
      case _: Exception => None
    }

  implicit def futureA[T: Arbitrary]: Arbitrary[Future[T]] =
    Arbitrary {
      Arbitrary.arbitrary[T].map { l => Future.value(l) }
    }

  implicit def returnA[T: Arbitrary]: Arbitrary[Try[T]] =
    Arbitrary {
      Arbitrary.arbitrary[T].map { l => Return(l) }
    }

  property("futureChainableCallbackCollectorBuilder") {
    monadLaws[Future, Int, String, Long] { (f1, f2) =>
      toOption(f1) == toOption(f2)
    }
  }

  property("tryChainableCallbackCollectorBuilder") {
    monadLaws[Try, Int, String, Long]()
  }

}
