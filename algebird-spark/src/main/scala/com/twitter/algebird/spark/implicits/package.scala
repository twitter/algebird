package com.twitter.algebird.spark

package object implicits {

import com.twitter.algebird.BF

import com.twitter.algebird.BFZero

import java.util.PriorityQueue

import com.twitter.algebird.BloomFilterMonoid

import org.apache.spark.sql.Encoder

  import scala.reflect.ClassTag
  implicit def kryoPriorityQueueEncoder[A](implicit ct: ClassTag[PriorityQueue[A]]): Encoder[PriorityQueue[A]] =
    org.apache.spark.sql.Encoders.kryo[PriorityQueue[A]](ct)

  implicit def kryoTuplePriorityQueueEncoder[A, B](implicit ct: ClassTag[(B, PriorityQueue[A])]): Encoder[(B, PriorityQueue[A])] =
    org.apache.spark.sql.Encoders.kryo[(B, PriorityQueue[A])](ct)  

  implicit def kryoBloomFilterMonoidEncoder[A](implicit ct: ClassTag[BloomFilterMonoid[A]]): Encoder[BloomFilterMonoid[A]] =
    org.apache.spark.sql.Encoders.kryo[BloomFilterMonoid[A]](ct)

  implicit def kryoBFZeroEncoder[A](implicit ct: ClassTag[BFZero[A]]): Encoder[BFZero[A]] =
    org.apache.spark.sql.Encoders.kryo[BFZero[A]](ct)

  implicit def kryoBFEncoder[A](implicit ct: ClassTag[BF[A]]): Encoder[BF[A]] = org.apache.spark.sql.Encoders.kryo[BF[A]](ct)
}
