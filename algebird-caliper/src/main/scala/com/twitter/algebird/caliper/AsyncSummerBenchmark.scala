package com.twitter.algebird.caliper

import com.twitter.algebird._
import com.twitter.util.Duration
import scala.util.Random
import com.twitter.bijection._
import com.twitter.algebird.util.summer._
import com.twitter.util.{Await, Future}
import com.twitter.util.{Future, Await, Duration, FuturePool}

import java.util.concurrent.Executors
import com.twitter.algebird.util._
import com.google.caliper.{Param, SimpleBenchmark}
import com.twitter.algebird.HyperLogLogMonoid

import java.nio.ByteBuffer

import Math._

class AsyncSummerBenchmark extends SimpleBenchmark {
  val flushFrequency = FlushFrequency(Duration.fromMilliseconds(100))
  val memoryFlushPercent = MemoryFlushPercent(80.0f)
  val executor = Executors.newFixedThreadPool(4)
  val workPool = FuturePool(executor)

  implicit val hllMonoid = new HyperLogLogMonoid(24)

  def genSummers[K, V: Semigroup] = Map(
    "AsyncListSum" -> new AsyncListSum[K, V](bufferSize, flushFrequency, memoryFlushPercent, workPool),
    "AsyncMapSum" -> new AsyncMapSum[K, V](bufferSize, flushFrequency, memoryFlushPercent, workPool),
    "SyncSummingQueue" -> new SyncSummingQueue[K, V](bufferSize, flushFrequency, memoryFlushPercent),
    "NullSummer" -> new NullSummer[K, V]()
    )

  var summers: Map[String, AsyncSummer[(Long, HLL), Map[Long, HLL]]] = _

  def hll[T](t: T)(implicit monoid: HyperLogLogMonoid, inj: Injection[T, Array[Byte]]): HLL = monoid.create(inj(t))

  @Param(Array("10", "100", "1000", "10000"))
  val numInputKeys: Int = 0

  @Param(Array("10", "1000", "10000", "100000"))
  val numInputItems: Int = 0

  @Param(Array("100", "200", "500", "1000"))
  val buffSizeInt: Int = 0

  var inputItems: IndexedSeq[IndexedSeq[(Long, HLL)]] = _
  var batchCount: Int = _
  var bufferSize: BufferSize = _

  def calcNumHeavyKeys(numInputKeys: Int): Int = {
    val ratio = numInputKeys/(numInputKeys.toDouble + 50)
    val count = (ratio * 100).toInt
    val heavyHitterCount = min(count, numInputKeys/10)
    println("Producing %d heavy hitter keys".format(heavyHitterCount))
    heavyHitterCount
  }

  override def setUp {
    val heavyKeys = (0 until calcNumHeavyKeys(numInputKeys)).toSet
    val heavyKeysIndexedSeq = heavyKeys.toIndexedSeq
    val rnd = new Random(3)

    bufferSize = BufferSize(buffSizeInt)
    summers = genSummers[Long, HLL]

    val inputData: IndexedSeq[(Long, HLL)] = (0L until numInputItems).map {inputKey =>
      val pos = rnd.nextInt(10)
      val k = if(pos < 8) { // 80% chance of hitting a heavy hitter
        heavyKeysIndexedSeq(rnd.nextInt(heavyKeysIndexedSeq.size))
      } else {
        var kCandidate = rnd.nextInt(numInputKeys)
        while(heavyKeys.contains(kCandidate)) {
          kCandidate = rnd.nextInt(numInputKeys)
        }
        kCandidate
      }
      (k.toLong, hll(Random.nextLong))
    }.toIndexedSeq

    inputItems = inputData.grouped(bufferSize.v/4).toIndexedSeq // materialize this
    batchCount = inputItems.size
  }


  def fn(summer: AsyncSummer[(Long, HLL), Map[Long, HLL]]) {
    val batch = Random.nextInt(batchCount)
    Await.result(summer.addAll(inputItems(batch)))
  }

  def doBench(name: String, reps: Int) = {
    val summer = summers(name)
    var dummy = 0
    while (dummy < reps) {
      fn(summer)
      dummy += 1
    }
    dummy
  }

  def timeAsyncListSum(reps: Int): Int = doBench("AsyncListSum", reps)
  def timeAsyncMapSum(reps: Int): Int = doBench("AsyncMapSum", reps)
  def timeSyncSummingQueue(reps: Int): Int = doBench("SyncSummingQueue", reps)
  def timeNullSummer(reps: Int): Int = doBench("NullSummer", reps)
}
