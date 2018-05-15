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
package scalacheck

import org.scalacheck.{Arbitrary, Gen}

trait ExpHistGen {
  import ExpHist.{Bucket, Config, Timestamp}

  implicit val genTimestamp: Gen[Timestamp] =
    Gen.posNum[Long].map(Timestamp(_))

  implicit val genBucket: Gen[Bucket] = for {
    count <- Gen.posNum[Long]
    timestamp <- genTimestamp
  } yield Bucket(count - 1L, timestamp)

  implicit val genConfig: Gen[Config] = for {
    k <- Gen.posNum[Short]
    windowSize <- Gen.posNum[Long]
  } yield Config(1 / k.toDouble, windowSize)

  implicit val genExpHist: Gen[ExpHist] =
    for {
      buckets <- Gen.containerOf[Vector, Bucket](genBucket)
      conf <- genConfig
    } yield ExpHist.empty(conf).addAll(buckets)
}

object ExpHistGenerators extends ExpHistGen

trait ExpHistArb {
  import ExpHist.{Bucket, Config, Timestamp}
  import ExpHistGenerators._

  implicit val arbTs: Arbitrary[Timestamp] = Arbitrary(genTimestamp)
  implicit val arbBucket: Arbitrary[Bucket] = Arbitrary(genBucket)
  implicit val arbConfig: Arbitrary[Config] = Arbitrary(genConfig)
  implicit val arbExpHist: Arbitrary[ExpHist] = Arbitrary(genExpHist)
}
