package com.twitter.algebird
package benchmark

import org.openjdk.jmh.annotations._

object BloomFilterDistanceBenchmark {

  def toSparse[A](bf: BF[A]): BFSparse[A] = bf match {
    case BFZero(hashes, width) => BFSparse(hashes, RichCBitSet(), width)
    case BFItem(item, hashes, width) =>
      BFSparse(hashes, RichCBitSet.fromArray(hashes(item)), width)
    case bfs @ BFSparse(_, _, _) => bfs
    case BFInstance(hashes, bitset, width) =>
      BFSparse(hashes, RichCBitSet.fromBitSet(bitset), width)
  }

  def toDense[A](bf: BF[A]): BFInstance[A] = bf match {
    case BFZero(hashes, width) => BFInstance.empty[A](hashes, width)
    case BFItem(item, hashes, width) =>
      val bs = LongBitSet.empty(width)
      bs += hashes(item)
      BFInstance(hashes, bs.toBitSetNoCopy, width)
    case bfs @ BFSparse(_, _, _)   => bfs.dense
    case bfi @ BFInstance(_, _, _) => bfi
  }

  @State(Scope.Benchmark)
  class BloomFilterState {

    val nbrOfElements: Int = 1000
    val falsePositiveRate: Double = 0.01

    def randomElements: Seq[String] =
      BloomFilterCreateBenchmark.createRandomString(nbrOfElements, 10)

    val emptyBF1: BF[String] =
      BloomFilter[String](nbrOfElements, falsePositiveRate).zero
    val emptyBF2: BF[String] =
      BloomFilter[String](nbrOfElements, falsePositiveRate).zero

    val sparseBF1: BF[String] =
      toSparse(
        BloomFilter[String](nbrOfElements, falsePositiveRate)
          .create(randomElements: _*)
      )
    val sparesBF2: BF[String] =
      toSparse(
        BloomFilter[String](nbrOfElements, falsePositiveRate)
          .create(randomElements: _*)
      )

    val denseBF1: BF[String] = toDense(
      BloomFilter[String](nbrOfElements, falsePositiveRate)
        .create(randomElements: _*)
    )
    val denseBF2: BF[String] = toDense(
      BloomFilter[String](nbrOfElements, falsePositiveRate)
        .create(randomElements: _*)
    )

  }
}

class BloomFilterDistanceBenchmark {

  import BloomFilterDistanceBenchmark._

  @Benchmark
  def distanceOfEmptyVsEmpty(bloomFilterState: BloomFilterState): Int =
    bloomFilterState.emptyBF1.hammingDistance(bloomFilterState.emptyBF2)

  @Benchmark
  def distanceOfEmptyVsSparse(bloomFilterState: BloomFilterState): Int =
    bloomFilterState.emptyBF1.hammingDistance(bloomFilterState.sparseBF1)

  @Benchmark
  def distanceOfEmptyVsDense(bloomFilterState: BloomFilterState): Int =
    bloomFilterState.emptyBF1.hammingDistance(bloomFilterState.denseBF1)

  @Benchmark
  def distanceOfSparseVsSparse(bloomFilterState: BloomFilterState): Int =
    bloomFilterState.sparseBF1.hammingDistance(bloomFilterState.sparesBF2)

  @Benchmark
  def distanceOfSparseVsDense(bloomFilterState: BloomFilterState): Int =
    bloomFilterState.sparseBF1.hammingDistance(bloomFilterState.denseBF1)

  @Benchmark
  def distanceOfDenseVsDense(bloomFilterState: BloomFilterState): Int =
    bloomFilterState.denseBF1.hammingDistance(bloomFilterState.denseBF1)

}
