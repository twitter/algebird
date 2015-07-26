[Caliper](https://code.google.com/p/caliper/)-based Benchmarks for Algebird data structures.

# Usage

Run the following commands from the top-level Algebird directory:

    $ ./sbt   # <<< enter sbt REPL
    > project algebird-caliper

Now you can run the following commands from within the sbt REPL:

    # List available benchmarks
    > show cappi::benchmarks

    # Run a particular benchmark
    > cappi::benchmarkOnly com.twitter.algebird.caliper.HLLBenchmark

    # Debug a particular benchmark (shows e.g. number of repetitions that will be run)
    > cappi::benchmarkOnly --debug com.twitter.algebird.caliper.HLLBenchmark

    # Run all benchmarks (apparently this is broken, see https://github.com/softprops/cappi/issues/1)
    > cappi::benchmarks

You can find further details in the [cappi](https://github.com/softprops/cappi) documentation, which is the sbt plugin
we use to run the caliper benchmarks.

Example output for [CMSBenchmark](src/test/scala/com/twitter/algebird/caliper/CMSBenchmark.scala):

    > cappi::benchmarkOnly com.twitter.algebird.caliper.CMSBenchmark
    [info] Running com.google.caliper.Runner com.twitter.algebird.caliper.CMSBenchmark
    [info]  0% Scenario{vm=java, trial=0, benchmark=PlusOfFirstHundredIntegersWithLongCms, delta=0.0000001, eps=0.1, heavyHittersPct=0.2, maxBits=2048, operations=100} 292576.31 ns; σ=1271.12 ns @ 3 trials
    [info] 17% Scenario{vm=java, trial=0, benchmark=PlusOfFirstHundredIntegersWithBigIntCms, delta=0.0000001, eps=0.1, heavyHittersPct=0.2, maxBits=2048, operations=100} 830195.29 ns; σ=7349.10 ns @ 3 trials
    [info] 33% Scenario{vm=java, trial=0, benchmark=PlusOfRandom2048BitNumbersWithBigIntCms, delta=0.0000001, eps=0.1, heavyHittersPct=0.2, maxBits=2048, operations=100} 3362751.81 ns; σ=104683.16 ns @ 10 trials
    [info] 50% Scenario{vm=java, trial=0, benchmark=PlusOfFirstHundredIntegersWithLongCms, delta=0.0000001, eps=0.005, heavyHittersPct=0.2, maxBits=2048, operations=100} 384133.61 ns; σ=41211.47 ns @ 10 trials
    [info] 67% Scenario{vm=java, trial=0, benchmark=PlusOfFirstHundredIntegersWithBigIntCms, delta=0.0000001, eps=0.005, heavyHittersPct=0.2, maxBits=2048, operations=100} 1018308.55 ns; σ=43285.12 ns @ 10 trials
    [info] 83% Scenario{vm=java, trial=0, benchmark=PlusOfRandom2048BitNumbersWithBigIntCms, delta=0.0000001, eps=0.005, heavyHittersPct=0.2, maxBits=2048, operations=100} 3610991.09 ns; σ=195033.95 ns @ 10 trials
    [info]
    [info]                               benchmark   eps   us linear runtime
    [info]   PlusOfFirstHundredIntegersWithLongCms   0.1  293 ==
    [info]   PlusOfFirstHundredIntegersWithLongCms 0.005  384 ===
    [info] PlusOfFirstHundredIntegersWithBigIntCms   0.1  830 ======
    [info] PlusOfFirstHundredIntegersWithBigIntCms 0.005 1018 ========
    [info] PlusOfRandom2048BitNumbersWithBigIntCms   0.1 3363 ===========================
    [info] PlusOfRandom2048BitNumbersWithBigIntCms 0.005 3611 ==============================
    [info]
    [info] vm: java
    [info] trial: 0
    [info] delta: 0.0000001
    [info] heavyHittersPct: 0.2
    [info] maxBits: 2048
    [info] operations: 100
    [success] Total time: 74 s, completed Oct 12, 2014 2:36:04 PM
