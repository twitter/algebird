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
package com.twitter.algebird.util

import com.twitter.algebird._
import org.scalacheck.Prop._

class PromiseLinkMonoidProperties extends CheckProperties {
  property("associative") {
    def makeTunnel(seed: Int) = PromiseLink.toPromiseLink(seed)
    def collapseFinalValues(finalTunnel: PromiseLink[Int], tunnels: Seq[PromiseLink[Int]], toFeed: Int) = {
      finalTunnel.completeWithStartingValue(toFeed)
      finalTunnel.promise +: tunnels.map { _.promise }
    }

    TunnelMonoidProperties.testTunnelMonoid(identity, makeTunnel, collapseFinalValues)
  }
}
