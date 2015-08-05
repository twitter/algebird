[jmh](http://openjdk.java.net/projects/code-tools/jmh/)-based Benchmarks for Algebird data structures.

# Usage

Run the following commands from the top-level Algebird directory:

    $ ./sbt   # <<< enter sbt REPL
    > project algebird-benchmark

Now you can run the following commands from within the sbt REPL:

    # List available benchmarks
    > run -l

    # Run a particular benchmark
    > run .*HLLBenchmark.*

    # Run all benchmarks (apparently this is broken, see https://github.com/softprops/cappi/issues/1)
    > run .*

You can find further details in the [sbt-jmh](https://github.com/ktoso/sbt-jmh) documentation, which is the sbt plugin
we use to run the jmh benchmarks.

Example output for [CMSBenchmark](src/main/scala/com/twitter/algebird/benchmark/CMSBenchmark.scala):

Running:
 3 Iterations
 3 Warmups per trial
 1 forks
 1 threads
 eps of 0.005 (benchmark parameter setting)
    > run -i 3 -wi 3 -f1 -t1 -p eps=0.005 com.twitter.algebird.benchmark.CMSBenchmark.*
    ..... (other benchmark warmup/run info)
[info] # JMH 1.10.3 (released 14 days ago)
[info] # VM version: JDK 1.7.0_40, VM 24.0-b56
[info] # VM invoker: /Library/Java/JavaVirtualMachines/jdk1.7.0_40.jdk/Contents/Home/jre/bin/java
[info] # VM options: <none>
[info] # Warmup: 3 iterations, 1 s each
[info] # Measurement: 3 iterations, 1 s each
[info] # Timeout: 10 min per iteration
[info] # Threads: 1 thread, will synchronize iterations
[info] # Benchmark mode: Throughput, ops/time
[info] # Benchmark: com.twitter.algebird.benchmark.CMSBenchmark.timePlusOfRandom2048BitNumbersWithStringCms
[info] # Parameters: (delta = 0.0000001, eps = 0.005, heavyHittersPct = 0.2, maxBits = 2048, operations = 100)
[info]
[info] # Run progress: 75.00% complete, ETA 00:00:06
[info] # Fork: 1 of 1
[info] # Warmup Iteration   1: Created 101 input records for String
[info] Created 101 input records for BigInt
[info] 66.056 ops/s
[info] # Warmup Iteration   2: 102.034 ops/s
[info] # Warmup Iteration   3: 116.003 ops/s
[info] Iteration   1: 109.730 ops/s
[info] Iteration   2: 112.937 ops/s
[info] Iteration   3: 98.425 ops/s
[info]
[info]
[info] Result "timePlusOfRandom2048BitNumbersWithStringCms":
[info]   107.031 ±(99.9%) 139.073 ops/s [Average]
[info]   (min, avg, max) = (98.425, 107.031, 112.937), stdev = 7.623
[info]   CI (99.9%): [≈ 0, 246.103] (assumes normal distribution)
[info]
[info]
[info] # Run complete. Total time: 00:00:26
[info]
[info] Benchmark                                                   (delta)  (eps)  (heavyHittersPct)  (maxBits)  (operations)   Mode  Cnt     Score      Error  Units
[info] CMSBenchmark.timePlusOfFirstHundredIntegersWithBigIntCms  0.0000001  0.005                0.2       2048           100  thrpt    3  1485.284 ± 1039.117  ops/s
[info] CMSBenchmark.timePlusOfFirstHundredIntegersWithLongCms    0.0000001  0.005                0.2       2048           100  thrpt    3  1768.006 ± 2623.229  ops/s
[info] CMSBenchmark.timePlusOfRandom2048BitNumbersWithBigIntCms  0.0000001  0.005                0.2       2048           100  thrpt    3   106.443 ±  201.605  ops/s
[info] CMSBenchmark.timePlusOfRandom2048BitNumbersWithStringCms  0.0000001  0.005                0.2       2048           100  thrpt    3   107.031 ±  139.073  ops/s
