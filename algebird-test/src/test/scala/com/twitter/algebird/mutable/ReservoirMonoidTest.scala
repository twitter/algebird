package com.twitter.algebird.mutable

import com.twitter.algebird.CheckProperties
import com.twitter.algebird.scalacheck.Distribution.{forAllSampled, uniform}
import org.scalacheck.Gen

import scala.util.Random

class ReservoirMonoidTest extends CheckProperties {
  val rng = new Random()
  implicit val randomSupplier: () => Random = () => rng

  property("adding empty is no-op") {
    val mon: ReservoirMonoid[Int] = new ReservoirMonoid(1)
    val a = mon.zero.accept(1, rng)
    val b = mon.zero.accept(1, rng)
    val z = mon.zero
    (mon.plus(a, z) == a) &&
    (mon.plus(z, b) == b)
  }

  property("plus produces correct distribution") {
    forAllSampled(10000, Gen.choose(1, 20))(n => uniform(2 * n)) { n =>
      val mon: ReservoirMonoid[Int] = new ReservoirMonoid(n)
      val a = (0 until n).foldLeft(mon.zero)((r, x) => r.accept(x, rng))
      val b = (n until 2 * n).foldLeft(mon.zero)((r, x) => r.accept(x, rng))

      val c = mon.plus(a, b)
      c.toSeq(rng.nextInt(c.size))
    }
  }
}
