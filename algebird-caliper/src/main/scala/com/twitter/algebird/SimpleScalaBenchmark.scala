package com.twitter.algebird.caliper

/* Stolen from {@link https://github.com/sirthias/scala-benchmarking-template/blob/master/src/main/scala/org/example/SimpleScalaBenchmark.scala } */

import com.google.caliper.SimpleBenchmark

trait SimpleScalaBenchmark extends SimpleBenchmark {

  // helper method to keep the actual benchmarking methods a bit cleaner
  // your code snippet should always return a value that cannot be "optimized away"
  def repeat[@specialized A](reps: Int)(snippet: => A) = {
    val zero = 0.asInstanceOf[A] // looks weird but does what it should: init w/ default value in a fully generic way
    var i = 0
    var result = zero
    while (i < reps) {
      val res = snippet 
      if (res != zero) result = res // make result depend on the benchmarking snippet result 
      i = i + 1
    }
    result
  }

}
