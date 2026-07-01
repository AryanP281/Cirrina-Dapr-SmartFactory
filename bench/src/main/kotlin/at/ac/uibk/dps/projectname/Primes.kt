package at.ac.uibk.dps.projectname

import java.util.concurrent.TimeUnit
import kotlinx.benchmark.*
import org.openjdk.jmh.annotations.Fork

@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
open class PrimesBenchmark {
  @Benchmark
  fun isPrime_true_early(): Boolean {
    return Primes.isPrime(5L)
  }

  @Benchmark
  fun isPrime_true_late(): Boolean {
    return Primes.isPrime(7919L)
  }

  @Benchmark
  fun isPrime_false_composite(): Boolean {
    return Primes.isPrime(7921L)
  }

  @Benchmark
  fun isPrime_false_pseudoprime(): Boolean {
    return Primes.isPrime(341L)
  }

  @Benchmark
  fun approximate_first_50(): List<Long> {
    return Primes.approximate().take(50).toList()
  }

  @Benchmark
  fun approximate_first_1000(): List<Long> {
    return Primes.approximate().take(1000).toList()
  }
}
