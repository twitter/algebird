package com.twitter.algebird.benchmark

import java.lang.Math._
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong

import org.openjdk.jmh.annotations._
import com.twitter.algebird.{HyperLogLogMonoid, _}
import com.twitter.algebird.util.summer._
import com.twitter.bijection._
import com.twitter.util.{Await, Duration, FuturePool}

import scala.util.Random

object AsyncSummerBenchmark {
  val flushFrequency = FlushFrequency(Duration.fromMilliseconds(100))
  val memoryFlushPercent = MemoryFlushPercent(80.0f)
  val executor = Executors.newFixedThreadPool(4)
  val workPool = FuturePool(executor)
  implicit val hllMonoid = new HyperLogLogMonoid(24)

  def hll[T](t: T)(implicit monoid: HyperLogLogMonoid, inj: Injection[T, Array[Byte]]): HLL =
    monoid.create(inj(t))

  @State(Scope.Benchmark)
  class SummerState {

    var asyncNonCompactListSum: AsyncSummer[(Long, HLL), Map[Long, HLL]] = _
    var asyncCompactListSum: AsyncSummer[(Long, HLL), Map[Long, HLL]] = _
    var asyncMapSum: AsyncSummer[(Long, HLL), Map[Long, HLL]] = _
    var syncSummingQueue: AsyncSummer[(Long, HLL), Map[Long, HLL]] = _
    var nullSummer: AsyncSummer[(Long, HLL), Map[Long, HLL]] = _

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
      val ratio = numInputKeys / (numInputKeys.toDouble + 50)
      val count = (ratio * 100).toInt
      val heavyHitterCount = min(count, numInputKeys / 10)
      println("Producing %d heavy hitter keys".format(heavyHitterCount))
      heavyHitterCount
    }

    @Setup(Level.Trial)
    def setup(): Unit = {
      val heavyKeys = (0 until calcNumHeavyKeys(numInputKeys)).toSet
      val heavyKeysIndexedSeq = heavyKeys.toIndexedSeq
      val rnd = new Random(3)

      bufferSize = BufferSize(buffSizeInt)
      nullSummer = new NullSummer[Long, HLL](Counter("tuplesIn"), Counter("tuplesOut"))
      asyncNonCompactListSum = new AsyncListSum[Long, HLL](
        bufferSize,
        flushFrequency,
        memoryFlushPercent,
        Counter("memory"),
        Counter("timeOut"),
        Counter("insertOp"),
        Counter("insertFails"),
        Counter("size"),
        Counter("tuplesIn"),
        Counter("tuplesOut"),
        workPool,
        Compact(false),
        CompactionSize(0)
      )
      asyncCompactListSum = new AsyncListSum[Long, HLL](
        bufferSize,
        flushFrequency,
        memoryFlushPercent,
        Counter("memory"),
        Counter("timeOut"),
        Counter("insertOp"),
        Counter("insertFails"),
        Counter("size"),
        Counter("tuplesIn"),
        Counter("tuplesOut"),
        workPool,
        Compact(true),
        CompactionSize(500)
      )
      asyncMapSum = new AsyncMapSum[Long, HLL](
        bufferSize,
        flushFrequency,
        memoryFlushPercent,
        Counter("memory"),
        Counter("timeOut"),
        Counter("insertOp"),
        Counter("tuplesOut"),
        Counter("size"),
        workPool
      )
      syncSummingQueue = new SyncSummingQueue[Long, HLL](
        bufferSize,
        flushFrequency,
        memoryFlushPercent,
        Counter("memory"),
        Counter("timeOut"),
        Counter("size"),
        Counter("puts"),
        Counter("tuplesIn"),
        Counter("tuplesOut")
      )

      val inputData: IndexedSeq[(Long, HLL)] = (0L until numInputItems).map { _ =>
        val pos = rnd.nextInt(10)
        val k = if (pos < 8) { // 80% chance of hitting a heavy hitter
          heavyKeysIndexedSeq(rnd.nextInt(heavyKeysIndexedSeq.size))
        } else {
          var kCandidate = rnd.nextInt(numInputKeys)
          while (heavyKeys.contains(kCandidate)) {
            kCandidate = rnd.nextInt(numInputKeys)
          }
          kCandidate
        }
        (k.toLong, hll(Random.nextLong))
      }.toIndexedSeq

      inputItems = inputData.grouped(bufferSize.v / 4).toIndexedSeq // materialize this
      batchCount = inputItems.size
    }
  }

}
class AsyncSummerBenchmark {
  import AsyncSummerBenchmark._

  @inline
  def fn(state: SummerState, summer: AsyncSummer[(Long, HLL), Map[Long, HLL]]) = {
    val batch = Random.nextInt(state.batchCount)
    Await.result(summer.addAll(state.inputItems(batch)))
  }

  def timeAsyncNonCompactListSum(state: SummerState) =
    fn(state, state.asyncNonCompactListSum)
  def timeAsyncCompactListSum(state: SummerState) =
    fn(state, state.asyncCompactListSum)
  def timeAsyncMapSum(state: SummerState) = fn(state, state.asyncMapSum)
  def timeSyncSummingQueue(state: SummerState) =
    fn(state, state.syncSummingQueue)
  def timeNullSummer(state: SummerState) = fn(state, state.nullSummer)

}

case class Counter(name: String) extends Incrementor {
  private val counter = new AtomicLong()

  override def incr(): Unit = counter.incrementAndGet()

  override def incrBy(amount: Long): Unit = counter.addAndGet(amount)

  def size = counter.get()

  override def toString: String = s"$name: size:$size"
}
