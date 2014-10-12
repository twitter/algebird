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
