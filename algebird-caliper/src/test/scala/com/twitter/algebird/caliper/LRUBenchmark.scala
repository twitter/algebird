package com.twitter.algebird.caliper

import com.twitter.algebird.mutable.LRU

import com.google.caliper.{ Param, SimpleBenchmark }

import java.util.{ LinkedHashMap, Map }

import scala.collection.JavaConverters._

class JavaLRU[K, V](capacity: Int) {
  private class LHM extends LinkedHashMap[K, V](capacity) {
    var lastEvicted: (K, V) = _
    override def removeEldestEntry(item: Map.Entry[K, V]) =
      if ((size > capacity)) { lastEvicted = (item.getKey, item.getValue); true }
      else false
  }
  private val cache = new LHM
  def update(k: K)(fn: Option[V] => V): Option[(K, V)] = {
    val newV = fn(Option(cache.get(k)))
    cache.put(k, newV)
    val res = Option(cache.lastEvicted)
    cache.lastEvicted = null
    res
  }
  def clear: IndexedSeq[(K, V)] = {
    val res = cache.entrySet.iterator.asScala.map { entry =>
      (entry.getKey, entry.getValue)
    }.toIndexedSeq
    cache.clear
    res
  }
}

class JavaLruBenchmark extends SimpleBenchmark {
  var jru: JavaLRU[Int, Int] = _

  @Param(Array("10000", "1000000"))
  val capacity: Int = 0

  @Param(Array("100", "10000", "1000000"))
  val numInputKeys: Int = 0

  @Param(Array("10000", "1000000", "10000000"))
  val numInputItems: Int = 0

  var inputData: Iterator[(Int, Int)] = _

  val rng = new java.util.Random(3)

  override def setUp {
    jru = new JavaLRU(capacity)

    inputData = (0 until numInputItems).iterator.map { _ =>
      (rng.nextInt(numInputKeys), rng.nextInt)
    }
  }

  def timeJaveLRU(reps: Int): Int = {
    var dummy = 0
    while (dummy < reps) {
      inputData.foreach {
        case (k, v) =>
          jru.update(k) {
            case None => v
            case Some(old) => old + v
          }
      }
      dummy += 1
    }
    dummy
  }
}

class LruBenchmark extends SimpleBenchmark {
  var lru: LRU[Int, Int] = _

  @Param(Array("10000", "1000000"))
  val capacity: Int = 0

  @Param(Array("100", "10000", "1000000"))
  val numInputKeys: Int = 0

  @Param(Array("10000", "1000000", "10000000"))
  val numInputItems: Int = 0

  var inputData: Iterator[(Int, Int)] = _

  val rng = new java.util.Random(3)

  override def setUp {
    lru = new LRU(capacity)

    inputData = (0 until numInputItems).iterator.map { _ =>
      (rng.nextInt(numInputKeys), rng.nextInt)
    }
  }

  def timeLRU(reps: Int): Int = {
    var dummy = 0
    while (dummy < reps) {
      inputData.foreach {
        case (k, v) =>
          lru.update(k) {
            case None => v
            case Some(old) => old + v
          }
      }
      dummy += 1
    }
    dummy
  }
}
