# Phenix Challenge

This is my take at Carrefour's [Phenix Challenge](https://github.com/Carrefour-Group/phenix-challenge), just for fun !

Here is an [overview of the algorithm](ALGORITHM.md) implemented.

And here is a description of the [challenge](CHALLENGE.md) itself

## Test

`sbt test`

## Run With SBT

`sbt "run data results 2017-05-14"`

## Package

`sbt dist`

The zipped package is located under `target/universal/phenix-challenge-<VERSION>.zip`

Once unzipped, you can run the program with a limited heap size like this:

`phenix-challenge data results 2017-05-14 -J-Xmx512m`

## Load Test

You can generate a big amount of data with the script under `generator`. Change the hardcoded values in `generator/generator.scala` if needed then run:

`sbt run`

And voila: gigabytes of data are generated under `generator/data/`.

## Performance

On my computer, it take 4685 seconds (or 1h18m) to process 129GB of data :
- CPU : Intel(R) Core(TM) i7-6560U CPU @ 2.20GHz
- RAM : capped at 512MB
- stores = 3000
- transactions per day = 1 million
- references = 500 000
- days = 7

## Possible Improvements

- There are probably a lot of untested corner cases to iron out.
- Paralellize computations (if disk access is not the bottleneck)
- The current implementation uses more temporary files than necessary when merging each day's aggregate.
- Some intermediary results could be memoized (see `combineByProduct`).
