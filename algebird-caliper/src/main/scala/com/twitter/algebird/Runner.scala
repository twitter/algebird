package com.twiter.algebird.caliper

/** Adapted from @link https://github.com/sirthias/scala-benchmarking-template/blob/master/src/main/scala/org/example/Runner.scala 
 */
import com.google.caliper.{Benchmark, Runner => CaliperRunner}
import com.google.common.collect.ObjectArrays.concat

import java.io.PrintWriter

object Runner {

  import com.twitter.algebird.caliper._

  def main(args: Array[String]) {
    val outWriter = new PrintWriter(System.out, true)
    val errWriter = new PrintWriter(System.err, true)
    /* We should probably call to the Caliper command line
    tool if we want to scale up? */
    CaliperRunner.main(classOf[HllBatchCreateBenchmark], args)
  }

}

