package com.twiter.algebird.caliper

/** Adapted from @link https://github.com/sirthias/scala-benchmarking-template/blob/master/src/main/scala/org/example/Runner.scala
 */
import com.google.caliper.runner.CaliperMain
import com.google.common.collect.ObjectArrays.concat

import java.io.PrintWriter

object Runner {

  def main(args: Array[String]) {
    val outWriter = new PrintWriter(System.out, true)
    val errWriter = new PrintWriter(System.err, true)
    /* We should probably call to the Caliper command line
    tool if we want to scale up? */
    CaliperMain.exitlessMain(args, outWriter, errWriter)
  }

}

